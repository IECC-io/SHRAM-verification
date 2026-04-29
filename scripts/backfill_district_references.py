"""Backfill the 7 Open-Meteo reference sources for all ~723 Indian districts.

Reads district list from shram's india_districts.json, runs the same Open-Meteo
fetch logic as backfill_references.py, but at every district centroid.

Output:
    reference_history/by_district/{source}/{state_slug}__{district_slug}/{year}.csv

Skips NASA POWER per user instruction.

Usage:
    python scripts/backfill_district_references.py --days 90
"""
from __future__ import annotations

import argparse
import csv
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
OUT_DIR = ROOT / "reference_history" / "by_district"
DISTRICTS_URL = "https://shram.info/india_districts.json"

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
FCST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"

OM_SOURCES = {
    "open_meteo":              {"api": ARCHIVE_API, "model": None},  # ERA5 analysis
    "open_meteo_ecmwf":        {"api": FCST_API,    "model": "ecmwf_ifs025"},
    "open_meteo_ecmwf_hres":   {"api": FCST_API,    "model": "ecmwf_ifs"},
    "open_meteo_gfs":          {"api": FCST_API,    "model": "ncep_gfs013"},
    "open_meteo_gfs_graphcast":{"api": FCST_API,    "model": "ncep_gfs_graphcast025"},
    "open_meteo_dwd_icon":     {"api": FCST_API,    "model": "icon_global"},
    "open_meteo_ukmo":         {"api": FCST_API,    "model": "ukmo_global_deterministic_10km"},
}

SLIM_COLS = ["logged_at_ist", "Ta_C", "RH_pct", "source_lat", "source_lon", "model"]
REQUEST_SLEEP = 0.4


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")


def _utc_to_ist_str(iso_utc):
    dt = datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc)
    return (dt + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def _fetch(api, lat, lon, start, end, model):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start, "end_date": end,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "UTC",
    }
    if model:
        params["models"] = model
    try:
        r = requests.get(api, params=params, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("fetch failed (model=%s, %s,%s): %s", model, lat, lon, exc)
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


def _backfill_one(source_label, conf, district, start, end):
    payload = _fetch(conf["api"], district["lat"], district["lon"], start, end, conf["model"])
    if not payload:
        return 0
    h = payload.get("hourly") or {}
    times = h.get("time", []) or []
    temps = h.get("temperature_2m", []) or []
    rhs = h.get("relative_humidity_2m", []) or []
    rows = []
    for t, ta, rh in zip(times, temps, rhs):
        if ta is None and rh is None:
            continue
        rows.append({
            "logged_at_ist": _utc_to_ist_str(t),
            "Ta_C": "" if ta is None else round(float(ta), 2),
            "RH_pct": "" if rh is None else round(float(rh), 1),
            "source_lat": district["lat"],
            "source_lon": district["lon"],
            "model": conf["model"] or "ERA5_analysis",
        })
    n_total = 0
    for yr, rr in _split_by_year(rows).items():
        path = OUT_DIR / source_label / district["slug"] / f"{yr}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        rr.sort(key=lambda r: r["logged_at_ist"])
        with path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=SLIM_COLS)
            w.writeheader()
            w.writerows(rr)
        n_total += len(rr)
    return n_total


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--limit", type=int, help="Debug: cap to N districts")
    p.add_argument("--sources", nargs="+", choices=list(OM_SOURCES.keys()))
    p.add_argument("--resume", action="store_true",
                   help="Skip {source}/{slug} where the year file already exists")
    args = p.parse_args()

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)

    payload = requests.get(DISTRICTS_URL, timeout=30).json()
    districts = []
    for state, sv in payload.get("states", {}).items():
        for district, dv in (sv.get("districts") or {}).items():
            if not isinstance(dv, dict):
                continue
            lat = dv.get("lat"); lon = dv.get("lon")
            if lat is None or lon is None:
                continue
            districts.append({
                "state": state, "district": district,
                "lat": float(lat), "lon": float(lon),
                "slug": f"{slugify(state)}__{slugify(district)}",
            })
    if args.limit:
        districts = districts[:args.limit]
    logger.info("loaded %d districts; range %s → %s", len(districts), start, end)

    sources_to_run = args.sources or list(OM_SOURCES.keys())
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for label in sources_to_run:
        conf = OM_SOURCES[label]
        logger.info("=== source: %s (model=%s) ===", label, conf["model"])
        n_done_session = 0
        n_skipped = 0
        for i, d in enumerate(districts, start=1):
            year_path = OUT_DIR / label / d["slug"] / f"{end.year}.csv"
            if args.resume and year_path.exists():
                n_skipped += 1
                continue
            try:
                rows = _backfill_one(label, conf, d, start.isoformat(), end.isoformat())
            except Exception as exc:
                logger.error("  fail %s: %s", d["slug"], exc)
                continue
            n_done_session += 1
            time.sleep(REQUEST_SLEEP)
            if i % 50 == 0:
                logger.info("  progress %d/%d (skipped %d)", i, len(districts), n_skipped)
        logger.info("source %s done: %d districts, %d skipped",
                    label, n_done_session, n_skipped)


if __name__ == "__main__":
    sys.exit(main())
