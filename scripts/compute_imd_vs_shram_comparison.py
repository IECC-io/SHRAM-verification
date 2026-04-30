"""Cross-join IMD heatwave declarations with SHRAM daily peak zones.

Reads the two latest pre-computed JSONs:
    reference_history/imd_heatwave_latest.json
    reference_history/shram_daily_peak.json

For each IMD heatwave-feed station, finds the nearest SHRAM daily-peak station
within MAX_KM, and emits one row per pairing. Appends results to:

    <data_repo>/comparisons/imd_vs_shram_daily.csv
    <data_repo>/comparisons/imd_vs_shram_summary.csv

The "data repo" path is configurable via --data-repo (default ../SHRAM-verification-data).

Run from the dashboard root:
    python3 scripts/compute_imd_vs_shram_comparison.py
    python3 scripts/compute_imd_vs_shram_comparison.py --data-repo /path/to/data
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
IMD_PATH = DASHBOARD_ROOT / "reference_history" / "imd_heatwave_latest.json"
SHRAM_PATH = DASHBOARD_ROOT / "reference_history" / "shram_daily_peak.json"

MAX_KM = 25.0  # nearest-neighbor join radius — same as the dashboard's diff/R² tabs

DAILY_FIELDS = [
    "run_date",
    "fetched_at_utc",
    "imd_station",
    "imd_station_code",
    "imd_station_type",
    "imd_state",
    "imd_district",
    "imd_lat",
    "imd_lon",
    "imd_hw_status",
    "imd_hw_label",
    "imd_tmax_d1",
    "imd_rh_d1",
    "nearest_shram_station",
    "nearest_shram_district",
    "nearest_shram_state",
    "nearest_shram_km",
    "shram_max_zone_hard",
    "shram_max_zone_light",
    "shram_max_ta",
    "shram_max_rh",
    "shram_n_obs",
    "agreement",
]

SUMMARY_FIELDS = [
    "run_date",
    "fetched_at_utc",
    "n_imd_total",
    "n_imd_heatwave",
    "n_imd_severe",
    "n_shram_total",
    "n_shram_zone5_plus",
    "n_shram_zone6",
    "n_matched_within_25km",
    "n_match_dangerous",
    "n_match_safe",
    "n_imd_only",
    "n_shram_only",
    "n_no_shram_data",
    "recall_imd_hw",
    "binary_agreement",
]


def km_between(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Cheap great-circle distance, kilometers."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def nearest(target: dict, candidates: list[dict], max_km: float) -> tuple[dict | None, float | None]:
    best, best_km = None, math.inf
    for c in candidates:
        if c.get("lat") is None or c.get("lon") is None:
            continue
        k = km_between(target["lat"], target["lon"], c["lat"], c["lon"])
        if k < best_km:
            best, best_km = c, k
    if best is None or best_km > max_km:
        return None, None
    return best, best_km


def classify_agreement(imd_status: int, shram_zone) -> str:
    """4-state agreement label, plus a no-data fallback."""
    if shram_zone is None or not isinstance(shram_zone, (int, float)) or shram_zone <= 0:
        return "no_shram_data"
    imd_hw = imd_status in (1, 2)
    shram_hw = shram_zone >= 5
    if imd_hw and shram_hw:
        return "match_dangerous"
    if not imd_hw and not shram_hw:
        return "match_safe"
    if imd_hw and not shram_hw:
        return "imd_only"
    return "shram_only"


def append_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    """Append rows; write header if file doesn't exist yet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-repo", default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
                    help="Path to the SHRAM-verification-data clone where CSVs are appended.")
    args = ap.parse_args()
    data_repo = Path(args.data_repo)

    if not IMD_PATH.exists():
        logger.error("IMD heatwave file not found at %s — run fetch_imd_heatwave.py first", IMD_PATH)
        return 1
    if not SHRAM_PATH.exists():
        logger.error("SHRAM daily peak file not found at %s — run compute_shram_daily_peak.py first", SHRAM_PATH)
        return 1
    if not data_repo.exists():
        logger.error("data repo not found at %s", data_repo)
        return 1

    with IMD_PATH.open() as f:
        imd_payload = json.load(f)
    with SHRAM_PATH.open() as f:
        shram_payload = json.load(f)

    fetched_at = imd_payload.get("fetched_at_utc") or datetime.now(timezone.utc).isoformat()
    run_date = shram_payload.get("target_date_ist") or datetime.now(timezone.utc).date().isoformat()
    imd_points = imd_payload.get("points") or []
    shram_points = shram_payload.get("points") or []

    logger.info("joining %d IMD HW points × %d SHRAM peak stations (run_date=%s)",
                len(imd_points), len(shram_points), run_date)

    rows = []
    counts = {"match_dangerous": 0, "match_safe": 0, "imd_only": 0, "shram_only": 0, "no_shram_data": 0}
    n_matched = 0
    n_imd_hw = 0
    n_imd_severe = 0
    n_tp = 0  # IMD HW + SHRAM HW
    n_fn = 0  # IMD HW + SHRAM safe
    n_agree = 0
    n_disagree = 0

    for ip in imd_points:
        if ip.get("lat") is None or ip.get("lon") is None:
            continue
        if ip.get("hw_status") == 1:
            n_imd_hw += 1
        if ip.get("hw_status") == 2:
            n_imd_severe += 1
        sp, km = nearest(ip, shram_points, MAX_KM)
        zone = sp["max_zone_hard"] if sp else None
        agreement = classify_agreement(ip.get("hw_status") or 0, zone)
        counts[agreement] += 1
        if sp:
            n_matched += 1
        # Recall counter (denominator = IMD-flagged events)
        if ip.get("hw_status") in (1, 2):
            if zone is not None and zone >= 5:
                n_tp += 1
            else:
                n_fn += 1
        # Binary agreement counter
        if zone is not None:
            imd_hw = ip.get("hw_status") in (1, 2)
            shram_hw = zone >= 5
            if imd_hw == shram_hw:
                n_agree += 1
            else:
                n_disagree += 1
        rows.append({
            "run_date": run_date,
            "fetched_at_utc": fetched_at,
            "imd_station": ip.get("name"),
            "imd_station_code": ip.get("station_code"),
            "imd_station_type": ip.get("station_type"),
            "imd_state": "",
            "imd_district": "",
            "imd_lat": ip.get("lat"),
            "imd_lon": ip.get("lon"),
            "imd_hw_status": ip.get("hw_status"),
            "imd_hw_label": ip.get("hw_label"),
            "imd_tmax_d1": ip.get("Ta"),
            "imd_rh_d1": ip.get("RH"),
            "nearest_shram_station": sp.get("station") if sp else "",
            "nearest_shram_district": sp.get("district") if sp else "",
            "nearest_shram_state": sp.get("state") if sp else "",
            "nearest_shram_km": round(km, 2) if km is not None else "",
            "shram_max_zone_hard": sp.get("max_zone_hard") if sp else "",
            "shram_max_zone_light": sp.get("max_zone_light") if sp else "",
            "shram_max_ta": sp.get("max_ta") if sp else "",
            "shram_max_rh": sp.get("max_rh") if sp else "",
            "shram_n_obs": sp.get("n_obs") if sp else "",
            "agreement": agreement,
        })

    # Append per-station rows
    daily_csv = data_repo / "comparisons" / "imd_vs_shram_daily.csv"
    append_csv(daily_csv, DAILY_FIELDS, rows)
    logger.info("appended %d rows to %s", len(rows), daily_csv)

    # Append daily summary row
    recall = (n_tp / (n_tp + n_fn)) if (n_tp + n_fn) > 0 else None
    binary_agree = (n_agree / (n_agree + n_disagree)) if (n_agree + n_disagree) > 0 else None
    summary_row = {
        "run_date": run_date,
        "fetched_at_utc": fetched_at,
        "n_imd_total": len(imd_points),
        "n_imd_heatwave": n_imd_hw,
        "n_imd_severe": n_imd_severe,
        "n_shram_total": len(shram_points),
        "n_shram_zone5_plus": shram_payload.get("n_zone_hard_5plus"),
        "n_shram_zone6": shram_payload.get("n_zone_hard_6"),
        "n_matched_within_25km": n_matched,
        "n_match_dangerous": counts["match_dangerous"],
        "n_match_safe": counts["match_safe"],
        "n_imd_only": counts["imd_only"],
        "n_shram_only": counts["shram_only"],
        "n_no_shram_data": counts["no_shram_data"],
        "recall_imd_hw": round(recall, 4) if recall is not None else "",
        "binary_agreement": round(binary_agree, 4) if binary_agree is not None else "",
    }
    summary_csv = data_repo / "comparisons" / "imd_vs_shram_summary.csv"
    append_csv(summary_csv, SUMMARY_FIELDS, [summary_row])
    logger.info("appended 1 summary row to %s", summary_csv)
    logger.info(
        "summary: %d IMD HW, %d severe, %d matched, %d match-dangerous, %d match-safe, "
        "%d IMD-only, %d SHRAM-only, recall=%s, agreement=%s",
        n_imd_hw, n_imd_severe, n_matched,
        counts["match_dangerous"], counts["match_safe"],
        counts["imd_only"], counts["shram_only"],
        f"{recall:.3f}" if recall is not None else "—",
        f"{binary_agree:.3f}" if binary_agree is not None else "—",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
