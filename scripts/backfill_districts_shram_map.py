"""Backfill shram_map_history for every Indian district in shram's india_districts.json.

Uses Open-Meteo's archive API at each district's centroid (lat, lon) to reconstruct
what shram's live grid would have shown for the district over the last N days.

Output:
    shram_map_history/by_district/{state_slug}__{district_slug}/{year}.csv
        logged_at_ist, Ta_C, RH_pct, source_lat, source_lon, notes

Plus index.json with all districts and lat/lons for the dashboard's geocoder.

Usage:
    python scripts/backfill_districts_shram_map.py --days 90
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
HISTORY_DIR = ROOT / "shram_map_history" / "by_district"
DISTRICTS_URL = "https://shram.info/india_districts.json"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

SLIM_COLS = ["logged_at_ist", "Ta_C", "RH_pct", "source_lat", "source_lon", "notes"]


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _utc_to_ist_str(iso_utc):
    dt = datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc)
    return (dt + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def _fetch(lat, lon, start, end):
    try:
        r = requests.get(ARCHIVE_URL, params={
            "latitude": lat, "longitude": lon,
            "start_date": start, "end_date": end,
            "hourly": "temperature_2m,relative_humidity_2m",
            "timezone": "UTC",
        }, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("archive failed for %s,%s: %s", lat, lon, exc)
        return None


def _split_by_year(rows):
    out = {}
    for r in rows:
        try:
            yr = int(r["logged_at_ist"][:4])
        except (KeyError, ValueError):
            continue
        out.setdefault(yr, []).append(r)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--limit", type=int, help="Stop after N districts (debug)")
    args = p.parse_args()

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)

    logger.info("fetching districts from %s", DISTRICTS_URL)
    districts_payload = requests.get(DISTRICTS_URL, timeout=30).json()
    targets = []
    for state, sv in districts_payload.get("states", {}).items():
        for district, dv in (sv.get("districts") or {}).items():
            if not isinstance(dv, dict): continue
            lat = dv.get("lat"); lon = dv.get("lon")
            if lat is None or lon is None: continue
            targets.append({
                "state": state, "district": district,
                "lat": float(lat), "lon": float(lon),
                "slug": f"{slugify(state)}__{slugify(district)}",
            })
    logger.info("loaded %d districts", len(targets))
    if args.limit:
        targets = targets[:args.limit]
        logger.info("limited to %d", len(targets))

    index = {"start_date": start.isoformat(), "end_date": end.isoformat(),
             "n_districts": len(targets), "districts": []}
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    for i, t in enumerate(targets, start=1):
        payload = _fetch(t["lat"], t["lon"], start.isoformat(), end.isoformat())
        if not payload:
            continue
        h = payload.get("hourly") or {}
        times = h.get("time", []) or []
        temps = h.get("temperature_2m", []) or []
        rhs = h.get("relative_humidity_2m", []) or []
        rows = []
        for ts, ta, rh in zip(times, temps, rhs):
            if ta is None and rh is None: continue
            rows.append({
                "logged_at_ist": _utc_to_ist_str(ts),
                "Ta_C": "" if ta is None else round(ta, 2),
                "RH_pct": "" if rh is None else round(rh, 1),
                "source_lat": t["lat"], "source_lon": t["lon"],
                "notes": "open-meteo archive",
            })
        n_total = 0
        for yr, rr in _split_by_year(rows).items():
            path = HISTORY_DIR / t["slug"] / f"{yr}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            rr.sort(key=lambda r: r["logged_at_ist"])
            with path.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=SLIM_COLS)
                w.writeheader(); w.writerows(rr)
            n_total += len(rr)
        index["districts"].append({**t, "n_rows": n_total})
        if i % 25 == 0 or i == len(targets):
            logger.info("  progress %d/%d (latest: %s = %d rows)", i, len(targets), t["slug"], n_total)
        time.sleep(0.3)

    with (HISTORY_DIR / "index.json").open("w") as f:
        json.dump(index, f, indent=2)
    logger.info("done: %d districts × %d days written to %s", len(targets), args.days, HISTORY_DIR)


if __name__ == "__main__":
    sys.exit(main())
