"""Slice shram's weekly weather_logs CSVs into one file per (station, year).

Output:
    shram_stations_history/by_station/{station_slug}/{year}.csv
        logged_at_ist, state, district, station, Ta_C, RH_pct, zone_light, zone_hard

Plus:
    shram_stations_history/by_station/index.json
        list of {station, slug, state, district, lat, lon, years, n_rows}

`station_slug` is a filesystem-safe lowercase ascii name (e.g. "Bhubaneshwar Airport"
-> "bhubaneshwar_airport"). Multiple stations with the same name in different
districts are disambiguated with district suffix.

Usage:
    python scripts/slice_shram_by_station.py --shram-dir /tmp/SHRAM-v2/weather_logs
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from collections import defaultdict
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "shram_stations_history" / "by_station"

SLIM_COLS = ["logged_at_ist", "state", "district", "station", "Ta_C", "RH_pct", "zone_light", "zone_hard"]


def slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return s or "unknown"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--shram-dir", type=Path,
                   default=Path("/tmp/SHRAM-v2/weather_logs"),
                   help="Directory containing india_weather_*_week*.csv")
    args = p.parse_args()

    if not args.shram_dir.exists():
        logger.error("shram dir not found: %s", args.shram_dir)
        return 1

    # Collect rows by (station_key, year) -> list of slim dicts
    # station_key = (state, district, station) — needed to disambiguate same-named stations
    bucket: dict[tuple, list[dict]] = defaultdict(list)
    files = sorted(args.shram_dir.glob("india_weather_*_week*.csv"))
    logger.info("found %d weekly CSVs in %s", len(files), args.shram_dir)

    for fp in files:
        with fp.open() as f:
            reader = csv.DictReader(f)
            for r in reader:
                state = (r.get("STATE") or "").strip()
                district = (r.get("DISTRICT") or "").strip()
                station = (r.get("STATION") or "").strip()
                logged = (r.get("LOGGED_AT (IST)") or "").strip()
                if not state or not district or not station or not logged:
                    continue
                try:
                    year = int(logged[:4])
                except ValueError:
                    continue
                key = (state, district, station, year)
                bucket[key].append({
                    "logged_at_ist": logged,
                    "state": state,
                    "district": district,
                    "station": station,
                    "Ta_C": (r.get("TEMP") or "").strip(),
                    "RH_pct": (r.get("RH") or "").strip(),
                    "zone_light": (r.get("Light Work Heat Stress Zone") or "").strip(),
                    "zone_hard": (r.get("Hard Labor Heat Stress Zone") or "").strip(),
                })
        logger.info("processed %s (running buckets: %d)", fp.name, len(bucket))

    # Build slug map and write per-(slug, year) CSVs.
    # If a station name collides across different districts, suffix with district slug.
    station_to_districts: dict[str, set[tuple]] = defaultdict(set)
    for (state, district, station, _year) in bucket.keys():
        station_to_districts[station].add((state, district))

    def _slug_for(state, district, station):
        slug = slugify(station)
        if len(station_to_districts[station]) > 1:
            slug = f"{slug}__{slugify(district)}"
        return slug

    index_meta: dict[str, dict] = {}
    for (state, district, station, year), rows in bucket.items():
        slug = _slug_for(state, district, station)
        rows.sort(key=lambda r: r["logged_at_ist"])
        out_path = OUT_DIR / slug / f"{year}.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=SLIM_COLS)
            w.writeheader()
            w.writerows(rows)
        meta = index_meta.setdefault(slug, {
            "station": station, "slug": slug,
            "state": state, "district": district,
            "years": [], "n_rows": 0,
        })
        meta["years"].append(year)
        meta["n_rows"] += len(rows)

    for m in index_meta.values():
        m["years"] = sorted(set(m["years"]))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with (OUT_DIR / "index.json").open("w") as f:
        json.dump({
            "n_stations": len(index_meta),
            "stations": sorted(index_meta.values(), key=lambda m: (m["state"], m["district"], m["station"])),
        }, f, indent=2)

    logger.info("wrote %d stations across %d (station, year) files", len(index_meta), len(bucket))
    return 0


if __name__ == "__main__":
    sys.exit(main())
