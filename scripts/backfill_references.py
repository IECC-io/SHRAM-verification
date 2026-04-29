"""Backfill historical reference values for the 12 monitored cities.

For each Open-Meteo model variant (best_match, ECMWF, GFS, ICON, UKMO, GraphCast)
and NASA POWER, pull hourly Ta + RH from the relevant archive endpoint over the
last N days and write per-(city, year) slim CSVs.

Output:
    reference_history/{source}/{city}/{year}.csv
        logged_at_ist, Ta_C, RH_pct, source_lat, source_lon, model

Open-Meteo's archive (`archive-api`) returns ERA5-derived analysis values when no
`models=` is passed. To get a per-model historical view, we hit the
`historical-forecast-api` endpoint with `models=...`. That returns the past
forecast for each model.

Usage:
    python scripts/backfill_references.py --days 90
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from fetch_sources import load_cities

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CITIES_PATH = ROOT / "config" / "cities.csv"
OUT_DIR = ROOT / "reference_history"

FCST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
NASA_POWER_API = "https://power.larc.nasa.gov/api/temporal/hourly/point"

OM_SOURCES = {
    "open_meteo":              {"api": ARCHIVE_API, "model": None},  # ERA5-derived analysis
    "open_meteo_ecmwf":        {"api": FCST_API,    "model": "ecmwf_ifs025"},
    "open_meteo_ecmwf_hres":   {"api": FCST_API,    "model": "ecmwf_ifs"},
    "open_meteo_gfs":          {"api": FCST_API,    "model": "ncep_gfs013"},
    "open_meteo_gfs_graphcast":{"api": FCST_API,    "model": "ncep_gfs_graphcast025"},
    "open_meteo_dwd_icon":     {"api": FCST_API,    "model": "icon_global"},
    "open_meteo_ukmo":         {"api": FCST_API,    "model": "ukmo_global_deterministic_10km"},
}

SLIM_COLS = ["logged_at_ist", "Ta_C", "RH_pct", "source_lat", "source_lon", "model"]


def _utc_to_ist_str(iso_utc: str) -> str:
    dt = datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc)
    return (dt + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_om(api: str, lat: float, lon: float, start: str, end: str, model: str | None):
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
        logger.warning("OM fetch failed (%s, model=%s, %s,%s): %s", api, model, lat, lon, exc)
        return None


def _fetch_nasa(lat: float, lon: float, start: str, end: str):
    params = {
        "parameters": "T2M,RH2M",
        "community": "RE",
        "latitude": lat, "longitude": lon,
        "start": start.replace("-", ""), "end": end.replace("-", ""),
        "format": "JSON",
        "time-standard": "UTC",
    }
    try:
        r = requests.get(NASA_POWER_API, params=params, timeout=120)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.warning("NASA POWER fetch failed for %s,%s: %s", lat, lon, exc)
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


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows, key=lambda r: r["logged_at_ist"])
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SLIM_COLS)
        w.writeheader()
        w.writerows(rows)


def _backfill_om(city, label, conf, start, end):
    payload = _fetch_om(conf["api"], city.lat, city.lon, start, end, conf["model"])
    if not payload:
        return
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
            "Ta_C": "" if ta is None else round(ta, 2),
            "RH_pct": "" if rh is None else round(rh, 1),
            "source_lat": city.lat,
            "source_lon": city.lon,
            "model": conf["model"] or "ERA5_analysis",
        })
    by_year = _split_by_year(rows)
    for yr, rr in by_year.items():
        path = OUT_DIR / label / city.name / f"{yr}.csv"
        _write_csv(path, rr)
    n_total = sum(len(v) for v in by_year.values())
    logger.info("  %s %-30s +%d rows", city.name, label, n_total)


def _backfill_nasa(city, start, end):
    payload = _fetch_nasa(city.lat, city.lon, start, end)
    if not payload:
        return
    params = (payload.get("properties") or {}).get("parameter") or {}
    t2m = params.get("T2M") or {}
    rh2m = params.get("RH2M") or {}
    rows = []
    for k in sorted(t2m.keys()):
        ta = t2m.get(k); rh = rh2m.get(k)
        if (ta is None or ta < -900) and (rh is None or rh < -900):
            continue
        # NASA keys are YYYYMMDDHH UTC
        try:
            dt = datetime.strptime(k, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        ist = dt + timedelta(hours=5, minutes=30)
        rows.append({
            "logged_at_ist": ist.strftime("%Y-%m-%d %H:%M:%S"),
            "Ta_C": "" if ta is None or ta < -900 else round(ta, 2),
            "RH_pct": "" if rh is None or rh < -900 else round(rh, 1),
            "source_lat": city.lat,
            "source_lon": city.lon,
            "model": "NASA_POWER",
        })
    by_year = _split_by_year(rows)
    for yr, rr in by_year.items():
        path = OUT_DIR / "nasa_power" / city.name / f"{yr}.csv"
        _write_csv(path, rr)
    n_total = sum(len(v) for v in by_year.values())
    logger.info("  %s %-30s +%d rows", city.name, "nasa_power", n_total)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=90)
    args = p.parse_args()
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)
    cities = load_cities(CITIES_PATH)
    logger.info("backfill range %s → %s for %d cities", start, end, len(cities))

    for c in cities:
        logger.info("=== %s ===", c.name)
        for label, conf in OM_SOURCES.items():
            _backfill_om(c, label, conf, start.isoformat(), end.isoformat())
            time.sleep(0.4)
        _backfill_nasa(c, start.isoformat(), end.isoformat())
        time.sleep(0.4)
    logger.info("done.")


if __name__ == "__main__":
    sys.exit(main())
