"""Build a slim daily time-series JSON for the dashboard's Time-series tab.

District-to-district comparison: both the IMD and SHRAM series count
*districts*, not stations or grid points. Reads the per-district daily CSV:

    comparisons/district_2pm_snapshot.csv

which has, for each district per day, the worst IMD alert (aws_worst_status)
and the worst SHRAM zone at each MET × sun combination. We tally districts
per category per day.

Emits reference_history/timeseries_daily.json in the dashboard repo.
Idempotent — rebuilds the whole series each run, so back-corrections in the
CSV propagate.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = DASHBOARD_ROOT / "reference_history" / "timeseries_daily.json"


def _int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def read_csv_rows(path: Path) -> list[dict]:
    if not path.exists():
        logger.warning("missing %s — skipping", path)
        return []
    with path.open() as f:
        return list(csv.DictReader(f))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-repo",
                    default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
                    help="Path to SHRAM-verification-data clone.")
    args = ap.parse_args()
    data_repo = Path(args.data_repo)
    if not data_repo.exists():
        logger.error("data repo not found at %s", data_repo)
        return 1

    district_rows = read_csv_rows(data_repo / "comparisons" / "district_2pm_snapshot.csv")

    # Tally districts per category per day. Both IMD and SHRAM count DISTRICTS,
    # so the series are directly comparable (district-to-district).
    # Each district counted once per day (CSV already has one row per district
    # per snapshot).
    by_date: dict[str, dict] = {}

    def bump(date, key):
        e = by_date.setdefault(date, {
            "date": date,
            "n_districts": 0,
            "imd_heatwave": 0, "imd_severe": 0,
            "shram_met5_zone5": 0, "shram_met5_zone6": 0,
            "shram_met6_zone5": 0, "shram_met6_zone6": 0,
        })
        e[key] += 1

    seen_district_day = set()
    for r in district_rows:
        d = r.get("snapshot_date_ist")
        if not d:
            continue
        # Guard against duplicate district rows within a day
        dkey = (d, r.get("district"), r.get("state"))
        if dkey in seen_district_day:
            continue
        seen_district_day.add(dkey)

        by_date.setdefault(d, {
            "date": d, "n_districts": 0,
            "imd_heatwave": 0, "imd_severe": 0,
            "shram_met5_zone5": 0, "shram_met5_zone6": 0,
            "shram_met6_zone5": 0, "shram_met6_zone6": 0,
        })
        by_date[d]["n_districts"] += 1

        aws_status = _int(r.get("aws_worst_status")) or 0
        if aws_status == 2:
            by_date[d]["imd_severe"] += 1
        elif aws_status == 1:
            by_date[d]["imd_heatwave"] += 1

        if _int(r.get("zone_met5_sun")) == 5: by_date[d]["shram_met5_zone5"] += 1
        if _int(r.get("zone_met5_sun")) == 6: by_date[d]["shram_met5_zone6"] += 1
        if _int(r.get("zone_met6_sun")) == 5: by_date[d]["shram_met6_zone5"] += 1
        if _int(r.get("zone_met6_sun")) == 6: by_date[d]["shram_met6_zone6"] += 1

    series = [by_date[d] for d in sorted(by_date)]

    payload = {
        "label": "SHRAM dashboard daily district counts",
        "note": ("Each entry is one 14:30 IST snapshot. District-to-district: "
                 "imd_* count districts whose worst nearby AWS station was "
                 "HW/severe; shram_* count districts at MET 5/6 (sun) zone 5/6."),
        "n_days": len(series),
        "series": series,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=1))
    logger.info("wrote %s (%d days)", OUT_PATH, len(series))
    return 0


if __name__ == "__main__":
    sys.exit(main())
