"""Populate the 14:30 IST SHRAM snapshot from the published forecast feed.

The 14:30 IST cron may not have fired yet today (or may have missed past days
during deploy). This script reads https://shram.info/weather_logs/forecast_7day.json,
which already contains hourly per-district forecasts for the current day,
and extracts the 14:00 IST hour into the dashboard snapshot payload shape.

Two modes:
    --today          Write a fresh reference_history/shram_2pm_snapshot_latest.json
                     using today's 14:00 IST hour from the live forecast feed.
                     Useful for backfilling immediately on deploy.

    --backfill-csv   Append rows to <data-repo>/comparisons/shram_snapshot_daily.csv
                     for every (state.capital + district) point at 14:00 IST.
                     Useful to populate historical CSV before the cron starts.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import ssl
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
SHRAM_FORECAST_URL = "https://shram.info/weather_logs/forecast_7day.json"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

# Same field list as snapshot_shram_live.py so the CSV is append-compatible
DAILY_FIELDS = [
    "snapshot_date_ist", "snapshot_time_ist",
    "lat", "lon", "district", "state",
    "temp_c", "rh_pct", "sw_wm2",
    "zone_met3_shade", "zone_met3_sun",
    "zone_met4_shade", "zone_met4_sun",
    "zone_met5_shade", "zone_met5_sun",
    "zone_met6_shade", "zone_met6_sun",
    "ehi_met3_shade", "ehi_met3_sun",
    "ehi_met4_shade", "ehi_met4_sun",
    "ehi_met5_shade", "ehi_met5_sun",
    "ehi_met6_shade", "ehi_met6_sun",
]


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "shram-2pm-populator"})
    with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
        return json.loads(r.read())


def extract_2pm_points(forecast_payload: dict, target_date: str | None = None) -> tuple[list[dict], str]:
    """Walk the forecast tree, pulling the 14:00 IST hour for each location.

    Returns (rows, snapshot_date_ist). If target_date is None, uses the first
    day in each district's forecast (which is "today" per the feed).
    """
    rows: list[dict] = []
    detected_date = None
    states = forecast_payload.get("states", {})

    for state_name, sv in states.items():
        # Capital
        caps = sv.get("capital")
        if caps:
            rows.extend(_rows_for_location(
                caps, state_name=state_name,
                district_name=caps.get("name") or "",
                target_date=target_date,
            ))

        # Districts
        districts = sv.get("districts", {}) or {}
        for d_name, dv in districts.items():
            rows.extend(_rows_for_location(
                dv, state_name=state_name,
                district_name=d_name,
                target_date=target_date,
            ))

    if rows:
        detected_date = rows[0]["snapshot_date_ist"]
    return rows, detected_date or ""


def _rows_for_location(loc: dict, state_name: str, district_name: str,
                       target_date: str | None) -> list[dict]:
    """For one (capital or district), pull the 14:00 IST hour."""
    lat = loc.get("lat")
    lon = loc.get("lon")
    fc = loc.get("forecast") or []
    out: list[dict] = []

    for entry in fc:
        date_str = entry.get("date")
        if not date_str:
            continue
        if target_date and date_str != target_date:
            continue
        hours = entry.get("hours") or []
        # Find the 14:00 hour
        h14 = next((h for h in hours if h.get("time", "").endswith(" 14:00")), None)
        if h14 is None:
            continue
        data = h14.get("data") or {}
        row = {
            "snapshot_date_ist": date_str,
            "snapshot_time_ist": "14:00:00",
            "lat": lat,
            "lon": lon,
            "district": district_name,
            "state": state_name,
            "temp_c": h14.get("temp_c"),
            "rh_pct": h14.get("humidity"),
            "sw_wm2": h14.get("sw"),
        }
        for met in (3, 4, 5, 6):
            for sun in ("shade", "sun"):
                cell = (data.get(f"met{met}") or {}).get(sun) or {}
                row[f"zone_met{met}_{sun}"] = cell.get("zone")
                row[f"ehi_met{met}_{sun}"] = cell.get("ehi")
        out.append(row)
        if not target_date:
            # Only take the first day (today) when not explicitly targeting
            break

    return out


def build_dashboard_payload(rows: list[dict], snapshot_date: str, source_url: str) -> dict:
    return {
        "label": "SHRAM 14:00 IST snapshot (extracted from forecast feed)",
        "source_url": source_url,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot_date_ist": snapshot_date,
        "snapshot_time_ist": "14:00:00",
        "metadata": {"point_count": len(rows), "source": "forecast_7day.json"},
        "points": [
            {
                "lat": r["lat"], "lon": r["lon"],
                "district": r["district"], "state": r["state"],
                "temp": r["temp_c"], "rh": r["rh_pct"], "sw": r["sw_wm2"],
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


def append_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--today", action="store_true",
                    help="Write reference_history/shram_2pm_snapshot_latest.json from today's 14:00 IST hour.")
    ap.add_argument("--backfill-csv", action="store_true",
                    help="Append today's 14:00 IST rows to the daily-snapshot CSV in the data repo.")
    ap.add_argument("--data-repo",
                    default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
                    help="Path to SHRAM-verification-data clone (used with --backfill-csv).")
    args = ap.parse_args()
    if not (args.today or args.backfill_csv):
        ap.error("specify --today and/or --backfill-csv")

    logger.info("fetching SHRAM forecast feed")
    payload = fetch_json(SHRAM_FORECAST_URL)
    logger.info("  generated_at_ist: %s", payload.get("metadata", {}).get("generated_at_ist"))

    rows, snap_date = extract_2pm_points(payload, target_date=None)
    logger.info("  extracted %d (district+capital) rows for %s 14:00 IST",
                len(rows), snap_date)

    if args.today:
        out = DASHBOARD_ROOT / "reference_history" / "shram_2pm_snapshot_latest.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        payload_out = build_dashboard_payload(rows, snap_date, SHRAM_FORECAST_URL)
        out.write_text(json.dumps(payload_out))
        logger.info("wrote dashboard payload %s (%d points)", out, len(payload_out["points"]))
        # Date-stamped archive so the Yesterday tab can read prior days
        archive_dir = DASHBOARD_ROOT / "reference_history" / "shram_2pm_snapshot"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"shram_2pm_{snap_date}.json"
        archive_path.write_text(json.dumps(payload_out))
        logger.info("archived %s", archive_path)

    if args.backfill_csv:
        data_repo = Path(args.data_repo)
        if not data_repo.exists():
            logger.error("data repo not found at %s", data_repo)
            return 1
        out_csv = data_repo / "comparisons" / "shram_snapshot_daily.csv"
        append_csv(out_csv, DAILY_FIELDS, rows)
        logger.info("appended %d rows to %s", len(rows), out_csv)

    return 0


if __name__ == "__main__":
    sys.exit(main())
