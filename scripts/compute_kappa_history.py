"""Compute the daily Cohen's κ time-series for IMD-vs-SHRAM forecast agreement.

Reads every archived IMD district warning bulletin and pairs it with the
*closest-in-time* SHRAM forecast archive. For each pair, computes κ across
every (IMD definition × SHRAM definition × MET level × forecast day) combo.
Appends the results to comparisons/kappa_history.csv.

Rerunning is idempotent — it only computes pairs not already in the CSV.

Run from the dashboard root:
    python3 scripts/compute_kappa_history.py
"""

from __future__ import annotations

import csv
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
IMD_ARCHIVE_DIR = DASHBOARD_ROOT / "reference_history" / "imd_districtwarning"
SHRAM_ARCHIVE_DIR = DASHBOARD_ROOT / "reference_history" / "shram_forecast"
OUT_DIR = DASHBOARD_ROOT / "comparisons"
OUT_CSV = OUT_DIR / "kappa_history.csv"

# Pairing tolerance: an IMD archive within ±N hours of a SHRAM archive
PAIR_TOLERANCE_HOURS = 12

# IMD definitions: same 9 cumulative rows the dashboard's forecast-κ tab uses
IMD_DEFS = []
for name, code in [("Heat Wave", 9), ("Hot Day", 10), ("Warm Night", 11)]:
    for tname, min_tier in [("Watch+", 2), ("Alert+", 3), ("Warning", 4)]:
        IMD_DEFS.append((f"{name} {tname}", code, min_tier))

SHRAM_THRESHOLDS = [("Z>=4", 4), ("Z>=5", 5), ("Z>=6", 6)]
MET_LEVELS = [3, 4, 5, 6]
FC_DAYS = [1, 2, 3, 4, 5]

STAMP_RE = re.compile(r"_(\d{8}T\d{6}Z)")


@dataclass
class Archive:
    path: Path
    stamp: datetime


def parse_stamp(path: Path) -> datetime | None:
    m = STAMP_RE.search(path.name)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def list_archives(d: Path) -> list[Archive]:
    if not d.exists():
        return []
    out = []
    for f in sorted(d.glob("*.json")):
        s = parse_stamp(f)
        if s:
            out.append(Archive(f, s))
    return out


def pair_archives(imd_list: list[Archive], shram_list: list[Archive]) -> list[tuple[Archive, Archive]]:
    """For each IMD archive, find the closest SHRAM archive within tolerance."""
    pairs = []
    for imd in imd_list:
        best = None
        best_dt = None
        for sh in shram_list:
            delta = abs((sh.stamp - imd.stamp).total_seconds()) / 3600.0
            if delta > PAIR_TOLERANCE_HOURS:
                continue
            if best is None or delta < best_dt:
                best = sh
                best_dt = delta
        if best:
            pairs.append((imd, best))
    return pairs


def cohen_kappa(a: int, b: int, c: int, d: int) -> float | None:
    n = a + b + c + d
    if n == 0:
        return None
    p_obs = (a + d) / n
    p_exp = (((a + b) * (a + c)) + ((c + d) * (b + d))) / (n * n)
    if p_exp == 1:
        return None
    return (p_obs - p_exp) / (1 - p_exp)


def load_imd(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def load_shram(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def _norm_name(s: str | None) -> str:
    """Strip, upper-case, replace underscores+multi-spaces with single spaces."""
    if not s:
        return ""
    s = s.replace("_", " ").strip().upper()
    return re.sub(r"\s+", " ", s)


def index_imd_by_district_date(imd: dict) -> dict:
    """Return { district_normalized: {day_num: {codes, tier, lat, lon}} } for days 1..5.

    IMD's `state` field is often blank, so we key on district name alone
    (normalized) and rely on lat/lon as a tiebreaker if names collide.
    """
    out = {}
    for d in imd.get("districts", []):
        name = _norm_name(d.get("district"))
        if not name:
            continue
        days = {}
        for i, day in enumerate(d.get("days", [])[:5], start=1):
            days[i] = {
                "codes": list(day.get("codes") or []),
                "tier": day.get("color_idx") or 1,
            }
        # Multiple districts can share a name across states (e.g., "AURANGABAD"
        # exists in both Maharashtra and Bihar). Bucket by name → list.
        out.setdefault(name, []).append({
            "lat": d.get("lat"),
            "lon": d.get("lon"),
            "days": days,
        })
    return out


def index_shram_by_district_date(shram: dict) -> dict:
    """Return { district_normalized: [ { lat, lon, by_date: {date: {met: peak}} } ... ] }."""
    out = {}
    for state_key, state_block in (shram.get("states") or {}).items():
        sub = state_block.get("districts") or {}
        for dist_key, pt in sub.items():
            name = _norm_name(dist_key)
            if not name:
                continue
            by_date = {}
            for day in pt.get("forecast") or []:
                pk = day.get("peak_zone_by_met") or {}
                by_date[day.get("date")] = {
                    int(m.replace("met", "")): pk.get(m, {}).get("sun")
                    for m in pk.keys()
                }
            out.setdefault(name, []).append({
                "lat": pt.get("lat"),
                "lon": pt.get("lon"),
                "by_date": by_date,
            })
    return out


def _haversine_km(la1, lo1, la2, lo2):
    import math
    if None in (la1, lo1, la2, lo2):
        return float("inf")
    R = 6371
    la1, la2 = math.radians(la1), math.radians(la2)
    dla = la2 - la1
    dlo = math.radians(lo2 - lo1)
    a = math.sin(dla/2)**2 + math.cos(la1)*math.cos(la2)*math.sin(dlo/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def best_shram_match(imd_entry: dict, shram_candidates: list[dict]) -> dict | None:
    """Among same-name SHRAM districts, return the geographically closest."""
    if not shram_candidates:
        return None
    if len(shram_candidates) == 1:
        return shram_candidates[0]
    return min(
        shram_candidates,
        key=lambda s: _haversine_km(imd_entry.get("lat"), imd_entry.get("lon"),
                                     s.get("lat"), s.get("lon"))
    )


def kappa_for_pair(imd_path: Path, shram_path: Path) -> list[dict]:
    imd = load_imd(imd_path)
    shram = load_shram(shram_path)
    imd_idx = index_imd_by_district_date(imd)
    shram_idx = index_shram_by_district_date(shram)

    bulletin_date_str = imd.get("districts", [{}])[0].get("date") if imd.get("districts") else None
    try:
        bulletin_date = datetime.strptime(bulletin_date_str, "%Y-%m-%d").date()
    except Exception:
        bulletin_date = parse_stamp(imd_path).date()

    # IMD's `Day_1` semantically refers to "tomorrow when issued by morning"
    # or "today when issued by evening." Rather than fight that, we anchor on
    # the SHRAM forecast's date stream: SHRAM forecast[0].date is its own
    # generation date. We pair IMD's `Day_N` with the SHRAM forecast day whose
    # date matches `bulletin_date + (N-1)` IF that date exists in SHRAM, ELSE
    # `bulletin_date + N` (covers the evening-bulletin case where Day_1 =
    # tomorrow). Fallback uses the bulletin_date offset.
    from datetime import timedelta

    # Find the earliest date present in SHRAM's forecast
    shram_dates_all = set()
    for entries in shram_idx.values():
        for e in entries:
            shram_dates_all.update(e["by_date"].keys())
    shram_first_date = min(shram_dates_all) if shram_dates_all else None

    rows = []
    for day in FC_DAYS:
        # Try two candidate target dates and pick whichever is in SHRAM
        cand1 = (bulletin_date + timedelta(days=day - 1)).isoformat()
        cand2 = (bulletin_date + timedelta(days=day)).isoformat()
        if cand1 in shram_dates_all:
            target_date = cand1
        elif cand2 in shram_dates_all:
            target_date = cand2
        else:
            target_date = cand1   # will produce empty rows but recorded for debugging
        for met in MET_LEVELS:
            for def_label, def_code, def_min_tier in IMD_DEFS:
                for shram_label, shram_min_zone in SHRAM_THRESHOLDS:
                    a = b = c = d = 0
                    for name, imd_entries in imd_idx.items():
                        shram_candidates = shram_idx.get(name)
                        if not shram_candidates:
                            continue
                        for imd_entry in imd_entries:
                            info = imd_entry["days"].get(day)
                            if not info:
                                continue
                            imd_alert = def_code in info["codes"] and info["tier"] >= def_min_tier
                            sh = best_shram_match(imd_entry, shram_candidates)
                            if not sh:
                                continue
                            peak = (sh["by_date"].get(target_date) or {}).get(met)
                            if peak is None:
                                continue
                            shram_alert = peak >= shram_min_zone
                            if imd_alert and shram_alert:
                                a += 1
                            elif imd_alert and not shram_alert:
                                b += 1
                            elif not imd_alert and shram_alert:
                                c += 1
                            else:
                                d += 1
                    k = cohen_kappa(a, b, c, d)
                    rows.append({
                        "imd_archive_utc": parse_stamp(imd_path).isoformat(),
                        "shram_archive_utc": parse_stamp(shram_path).isoformat(),
                        "bulletin_date": bulletin_date.isoformat(),
                        "target_date": target_date,
                        "fc_day": day,
                        "met": met,
                        "imd_def": def_label,
                        "imd_code": def_code,
                        "imd_min_tier": def_min_tier,
                        "shram_def": shram_label,
                        "shram_min_zone": shram_min_zone,
                        "a_both_alert": a,
                        "b_imd_only": b,
                        "c_shram_only": c,
                        "d_both_none": d,
                        "n": a + b + c + d,
                        "kappa": k if k is not None else "",
                    })
    return rows


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    imd_archives = list_archives(IMD_ARCHIVE_DIR)
    shram_archives = list_archives(SHRAM_ARCHIVE_DIR)
    logger.info("IMD archives:   %d", len(imd_archives))
    logger.info("SHRAM archives: %d", len(shram_archives))

    pairs = pair_archives(imd_archives, shram_archives)
    logger.info("paired IMD×SHRAM snapshots within ±%dh: %d", PAIR_TOLERANCE_HOURS, len(pairs))

    # Load any existing rows to avoid recomputing
    existing_keys = set()
    if OUT_CSV.exists():
        with OUT_CSV.open() as f:
            for row in csv.DictReader(f):
                existing_keys.add((row.get("imd_archive_utc"), row.get("shram_archive_utc")))

    new_rows = []
    for imd_a, sh_a in pairs:
        key = (imd_a.stamp.isoformat(), sh_a.stamp.isoformat())
        if key in existing_keys:
            continue
        logger.info("computing κ for IMD=%s ↔ SHRAM=%s",
                    imd_a.path.name, sh_a.path.name)
        try:
            new_rows.extend(kappa_for_pair(imd_a.path, sh_a.path))
        except Exception as e:
            logger.warning("skipping pair (%s, %s): %s", imd_a.path.name, sh_a.path.name, e)

    if not new_rows:
        logger.info("no new rows to add — already up to date")
        return 0

    # Write/append
    fieldnames = list(new_rows[0].keys())
    mode = "a" if OUT_CSV.exists() else "w"
    with OUT_CSV.open(mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if mode == "w":
            writer.writeheader()
        writer.writerows(new_rows)
    logger.info("wrote %d new rows → %s", len(new_rows), OUT_CSV)
    return 0


if __name__ == "__main__":
    sys.exit(main())
