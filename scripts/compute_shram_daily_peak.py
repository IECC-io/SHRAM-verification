"""Compute SHRAM daily peak zone per station.

Reads `shram_stations_history/by_station/<station>/<year>.csv` and produces a
daily roll-up: max zone_hard reached during each calendar day, per station.

Output is written as JSON for the dashboard to read:
    reference_history/shram_daily_peak.json

This is the "apples-to-apples" temporal granularity for comparing against IMD's
daily heatwave declaration: both are statements about the day, not a moment.

Run from the dashboard root:
    python3 scripts/compute_shram_daily_peak.py
    python3 scripts/compute_shram_daily_peak.py --date 2026-04-29
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
STATION_HISTORY = DASHBOARD_ROOT / "shram_stations_history" / "by_station"
STATION_INDEX = DASHBOARD_ROOT / "shram_stations_history" / "imd_stations.json"
OUT_PATH = DASHBOARD_ROOT / "reference_history" / "shram_daily_peak.json"

ZONE_RE = re.compile(r"Zone\s*(\d+)", re.IGNORECASE)


def parse_zone(s: str) -> int | None:
    if not s:
        return None
    m = ZONE_RE.match(s.strip())
    return int(m.group(1)) if m else None


def parse_ts(s: str) -> datetime | None:
    """Parse 'YYYY-MM-DD HH:MM:SS' (IST) into a naive datetime."""
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return None


def load_station_index() -> dict[str, dict]:
    """Map station_slug → {lat, lon, station, district, state}."""
    if not STATION_INDEX.exists():
        return {}
    with STATION_INDEX.open() as f:
        payload = json.load(f)
    out = {}
    for s in payload.get("stations", []):
        # The dashboard stores station slugs as lowercased + underscored versions
        # of the station name. Match the convention used by `slice_shram_by_station.py`.
        name = s.get("station") or ""
        slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
        if not slug:
            continue
        out[slug] = {
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "station": name,
            "district": s.get("district"),
            "state": s.get("state"),
        }
    return out


def compute_peak_for_station(csv_path: Path, target_date: date) -> dict | None:
    """Find the max zone_hard observed at this station on target_date.

    Returns None if the file has no rows for that date.
    """
    target_str = target_date.isoformat()
    max_zone_hard = 0
    max_zone_light = 0
    n_obs = 0
    max_ta = None
    max_rh = None
    state = district = station = ""
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_str = row.get("logged_at_ist") or ""
            # Fast string-prefix check before paying for parsing
            if not ts_str.startswith(target_str):
                continue
            zone_hard = parse_zone(row.get("zone_hard") or "")
            zone_light = parse_zone(row.get("zone_light") or "")
            if zone_hard is None and zone_light is None:
                continue
            n_obs += 1
            if zone_hard and zone_hard > max_zone_hard:
                max_zone_hard = zone_hard
            if zone_light and zone_light > max_zone_light:
                max_zone_light = zone_light
            try:
                ta = float(row.get("Ta_C") or "")
                if max_ta is None or ta > max_ta:
                    max_ta = ta
            except (TypeError, ValueError):
                pass
            try:
                rh = float(row.get("RH_pct") or "")
                if max_rh is None or rh > max_rh:
                    max_rh = rh
            except (TypeError, ValueError):
                pass
            state = state or row.get("state") or ""
            district = district or row.get("district") or ""
            station = station or row.get("station") or ""
    if n_obs == 0:
        return None
    return {
        "max_zone_hard": max_zone_hard,
        "max_zone_light": max_zone_light,
        "max_ta": max_ta,
        "max_rh": max_rh,
        "n_obs": n_obs,
        "state": state,
        "district": district,
        "station": station,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="ISO date (YYYY-MM-DD). Defaults to today IST.",
                    default=None)
    args = ap.parse_args()

    if args.date:
        target = date.fromisoformat(args.date)
    else:
        # IST = UTC+5:30
        target = (datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)).date()
    logger.info("computing daily peak for %s (IST)", target)

    if not STATION_HISTORY.exists():
        logger.error("station history not found at %s", STATION_HISTORY)
        return 1

    index = load_station_index()
    logger.info("loaded coordinates for %d stations from index", len(index))

    points = []
    n_skipped = 0
    n_no_index = 0
    for station_dir in sorted(STATION_HISTORY.iterdir()):
        if not station_dir.is_dir():
            continue
        slug = station_dir.name
        # Pick the CSV for the target year (handle year boundaries)
        candidates = list(station_dir.glob(f"{target.year}.csv"))
        if not candidates:
            n_skipped += 1
            continue
        peak = compute_peak_for_station(candidates[0], target)
        if peak is None:
            n_skipped += 1
            continue
        # Resolve coords from the station index
        idx_entry = index.get(slug)
        if not idx_entry:
            # Try a looser match: strip trailing tokens like "_kvk", "_amfu" etc.
            n_no_index += 1
            continue
        if idx_entry.get("lat") is None or idx_entry.get("lon") is None:
            n_no_index += 1
            continue
        points.append({
            "slug": slug,
            "lat": idx_entry["lat"],
            "lon": idx_entry["lon"],
            "station": peak["station"] or idx_entry.get("station"),
            "district": peak["district"] or idx_entry.get("district"),
            "state": peak["state"] or idx_entry.get("state"),
            "max_zone_hard": peak["max_zone_hard"],
            "max_zone_light": peak["max_zone_light"],
            "max_ta": peak["max_ta"],
            "max_rh": peak["max_rh"],
            "n_obs": peak["n_obs"],
        })

    payload = {
        "label": "SHRAM daily peak zone",
        "target_date_ist": target.isoformat(),
        "computed_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_stations": len(points),
        "n_skipped_no_data": n_skipped,
        "n_skipped_no_coords": n_no_index,
        "n_zone_hard_5plus": sum(1 for p in points if p["max_zone_hard"] >= 5),
        "n_zone_hard_6": sum(1 for p in points if p["max_zone_hard"] == 6),
        "points": points,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info(
        "wrote %d stations (%d skipped no-data, %d skipped no-coords) to %s",
        len(points), n_skipped, n_no_index, OUT_PATH,
    )
    logger.info(
        "%d stations reached zone_hard >= 5, %d reached zone_hard = 6",
        payload["n_zone_hard_5plus"], payload["n_zone_hard_6"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
