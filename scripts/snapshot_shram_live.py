"""Daily SHRAM live-grid snapshot for IECC-io/SHRAM-verification-data.

At 14:30 IST (~09:00 UTC) — pulls https://shram.info/grid_data.json, summarizes
the per-grid-point zones at MET 5 and MET 6 (sun scenario, which is what the
dashboard's panels B and C show), and appends two CSVs to the sibling data repo:

  comparisons/shram_snapshot_daily.csv     (one row per grid point per day)
  comparisons/shram_snapshot_summary.csv   (one row per day, all-India totals)

This is independent of the IMD pipeline — if the IMD fetch is broken, this
still runs.

Run from the dashboard root:
    python3 scripts/snapshot_shram_live.py
    python3 scripts/snapshot_shram_live.py --data-repo /path/to/data
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import ssl
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
SHRAM_URL = "https://shram.info/grid_data.json"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

DAILY_FIELDS = [
    "snapshot_date_ist",
    "snapshot_time_ist",
    "lat",
    "lon",
    "district",
    "state",
    "temp_c",
    "rh_pct",
    "sw_wm2",
    # All combinations of MET 3-6 × {shade, sun}
    "zone_met3_shade", "zone_met3_sun",
    "zone_met4_shade", "zone_met4_sun",
    "zone_met5_shade", "zone_met5_sun",
    "zone_met6_shade", "zone_met6_sun",
    "ehi_met3_shade", "ehi_met3_sun",
    "ehi_met4_shade", "ehi_met4_sun",
    "ehi_met5_shade", "ehi_met5_sun",
    "ehi_met6_shade", "ehi_met6_sun",
]

SUMMARY_FIELDS = [
    "snapshot_date_ist",
    "snapshot_time_ist",
    "n_points",
    "n_met5_zone4",
    "n_met5_zone5",
    "n_met5_zone6",
    "n_met6_zone4",
    "n_met6_zone5",
    "n_met6_zone6",
    "pct_met5_zone5_plus",
    "pct_met6_zone5_plus",
    "data_quality",
    "api_failures",
    "remaining_gaps",
    "is_nighttime",
]


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "shram-verification-snapshot"})
    with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
        return json.loads(r.read())


def append_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
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
    ap.add_argument(
        "--data-repo",
        default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
        help="Path to the SHRAM-verification-data clone where CSVs are appended.",
    )
    args = ap.parse_args()
    data_repo = Path(args.data_repo)
    if not data_repo.exists():
        logger.error("data repo not found at %s", data_repo)
        return 1

    logger.info("fetching SHRAM live grid: %s", SHRAM_URL)
    payload = fetch_json(SHRAM_URL)
    meta = payload.get("metadata", {})
    points = payload.get("points", [])
    logger.info("  %d points, generated %s",
                len(points), meta.get("generated_at_ist"))

    # Use SHRAM's own generated_at as the snapshot stamp (more reliable than
    # local clock — pinpoints what data we actually captured).
    gen_at = meta.get("generated_at") or datetime.now(timezone.utc).isoformat()
    try:
        dt = datetime.fromisoformat(gen_at)
        snapshot_date = dt.date().isoformat()
        snapshot_time = dt.strftime("%H:%M:%S")
    except ValueError:
        snapshot_date = datetime.now(timezone(timedelta(hours=5, minutes=30))).date().isoformat()
        snapshot_time = ""

    rows: list[dict] = []
    counts_met5: Counter = Counter()
    counts_met6: Counter = Counter()

    for pt in points:
        data = pt.get("data") or {}
        row = {
            "snapshot_date_ist": snapshot_date,
            "snapshot_time_ist": snapshot_time,
            "lat": pt.get("lat"),
            "lon": pt.get("lon"),
            "district": pt.get("district") or "",
            "state": pt.get("state") or "",
            "temp_c": pt.get("temp"),
            "rh_pct": pt.get("rh"),
            "sw_wm2": pt.get("sw"),
        }
        for met in (3, 4, 5, 6):
            for sun in ("shade", "sun"):
                cell = (data.get(f"met{met}") or {}).get(sun) or {}
                row[f"zone_met{met}_{sun}"] = cell.get("zone")
                row[f"ehi_met{met}_{sun}"] = cell.get("ehi")
        rows.append(row)
        # Summary buckets keyed off the sun scenario (matches dashboard panels B/C)
        counts_met5[row["zone_met5_sun"]] += 1
        counts_met6[row["zone_met6_sun"]] += 1

    n = len(rows) or 1
    n_met5_5plus = counts_met5[5] + counts_met5[6]
    n_met6_5plus = counts_met6[5] + counts_met6[6]
    summary_row = {
        "snapshot_date_ist": snapshot_date,
        "snapshot_time_ist": snapshot_time,
        "n_points": len(rows),
        "n_met5_zone4": counts_met5[4],
        "n_met5_zone5": counts_met5[5],
        "n_met5_zone6": counts_met5[6],
        "n_met6_zone4": counts_met6[4],
        "n_met6_zone5": counts_met6[5],
        "n_met6_zone6": counts_met6[6],
        "pct_met5_zone5_plus": round(100.0 * n_met5_5plus / n, 2),
        "pct_met6_zone5_plus": round(100.0 * n_met6_5plus / n, 2),
        "data_quality": meta.get("data_quality") or "",
        "api_failures": meta.get("api_failures") or 0,
        "remaining_gaps": meta.get("remaining_gaps") or 0,
        "is_nighttime": meta.get("is_nighttime", False),
    }

    daily_csv = data_repo / "comparisons" / "shram_snapshot_daily.csv"
    summary_csv = data_repo / "comparisons" / "shram_snapshot_summary.csv"
    append_csv(daily_csv, DAILY_FIELDS, rows)
    append_csv(summary_csv, SUMMARY_FIELDS, [summary_row])

    logger.info("appended %d rows to %s", len(rows), daily_csv)
    logger.info("appended 1 summary row to %s", summary_csv)

    # Also publish a slim dashboard payload so compare.html can render Panel B/C
    # from the frozen 14:30 snapshot instead of refetching shram.info live.
    # Just the per-grid-point zones — the dashboard's existing aggregation
    # (aggregateShramByMet) will roll up to districts on the client side.
    slim_payload = {
        "label": "SHRAM 14:30 IST snapshot",
        "source_url": SHRAM_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot_date_ist": snapshot_date,
        "snapshot_time_ist": snapshot_time,
        "metadata": meta,
        "points": [
            {
                "lat": r["lat"], "lon": r["lon"],
                "district": r["district"], "state": r["state"],
                "temp": r["temp_c"], "rh": r["rh_pct"], "sw": r["sw_wm2"],
                # nest in the same shape grid_data.json uses so the dashboard's
                # aggregateShramByMet helper works unchanged
                "data": {
                    f"met{m}": {
                        s: {
                            "zone": r.get(f"zone_met{m}_{s}"),
                            "ehi":  r.get(f"ehi_met{m}_{s}"),
                        }
                        for s in ("shade", "sun")
                    }
                    for m in (3, 4, 5, 6)
                },
            }
            for r in rows
        ],
    }
    slim_out = DASHBOARD_ROOT / "reference_history" / "shram_2pm_snapshot_latest.json"
    slim_out.parent.mkdir(parents=True, exist_ok=True)
    slim_out.write_text(json.dumps(slim_payload))
    logger.info("wrote dashboard payload %s (%d points)",
                slim_out, len(slim_payload["points"]))

    # Also write a date-stamped archive copy so the Yesterday tab can read
    # the prior day's snapshot. File naming: shram_2pm_<YYYY-MM-DD>.json
    archive_dir = DASHBOARD_ROOT / "reference_history" / "shram_2pm_snapshot"
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"shram_2pm_{snapshot_date}.json"
    archive_path.write_text(json.dumps(slim_payload))
    logger.info("archived date-stamped copy: %s", archive_path)
    logger.info(
        "summary: %d points · MET5 Z5+: %d (%.1f%%) · MET6 Z5+: %d (%.1f%%) · night=%s",
        len(rows), n_met5_5plus, summary_row["pct_met5_zone5_plus"],
        n_met6_5plus, summary_row["pct_met6_zone5_plus"],
        summary_row["is_nighttime"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
