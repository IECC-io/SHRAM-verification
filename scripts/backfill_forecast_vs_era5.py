"""Compare Open-Meteo forecasts (~nowcast) against ERA5 reanalysis at past hours.

Pulls two parallel time series for each city, last N days:
    - historical-forecast-api  → what the forecast model said for that hour (nowcast lead)
    - archive-api              → ERA5 reanalysis truth for that hour

Joins on logged_at_ist; computes per-hour errors. Writes:

    forecast_history/{city}/{year}.csv         logged_at_ist, Ta_C, RH_pct
    forecast_error_history/{city}/{year}.csv   logged_at_ist, fcst_Ta, era5_Ta, dTa, fcst_RH, era5_RH, dRH
    forecast_error_history/summary.json        per-city stats (bias, MAE, RMSE, threshold rates)

Usage:
    python scripts/backfill_forecast_vs_era5.py --days 90
"""
from __future__ import annotations

import argparse
import csv
import json
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
FCST_DIR = ROOT / "forecast_history"
ERR_DIR = ROOT / "forecast_error_history"

FCST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"

FCST_COLS = ["logged_at_ist", "Ta_C", "RH_pct"]
ERR_COLS = ["logged_at_ist", "fcst_Ta", "era5_Ta", "dTa", "fcst_RH", "era5_RH", "dRH"]


def _utc_to_ist_str(iso_utc: str) -> str:
    dt = datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc)
    ist = dt + timedelta(hours=5, minutes=30)
    return ist.strftime("%Y-%m-%d %H:%M:%S")


def _fetch_hourly(api_url: str, lat: float, lon: float, start: str, end: str,
                  model: str | None = None) -> dict | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "UTC",
    }
    if model:
        params["models"] = model
    try:
        resp = requests.get(api_url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("%s fetch failed for %s,%s: %s", api_url, lat, lon, exc)
        return None


def _hourly_to_dict(payload: dict) -> dict[str, tuple[float | None, float | None]]:
    """Return mapping: ist_timestamp_str -> (Ta_C, RH_pct)."""
    out: dict[str, tuple] = {}
    h = (payload or {}).get("hourly") or {}
    times = h.get("time") or []
    temps = h.get("temperature_2m") or []
    rhs = h.get("relative_humidity_2m") or []
    for t, ta, rh in zip(times, temps, rhs):
        out[_utc_to_ist_str(t)] = (ta, rh)
    return out


def _split_by_year(rows):
    out: dict[int, list[dict]] = {}
    for r in rows:
        try:
            yr = int(r["logged_at_ist"][:4])
        except (KeyError, ValueError):
            continue
        out.setdefault(yr, []).append(r)
    return out


def _write_csv(path: Path, cols: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows, key=lambda r: r["logged_at_ist"])
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def _summary_for_city(rows: list[dict]) -> dict:
    """Compute bias/MAE/RMSE and threshold rates for one city."""
    def _stats(deltas: list[float]) -> dict:
        if not deltas:
            return {"n": 0}
        n = len(deltas)
        bias = sum(deltas) / n
        mae = sum(abs(d) for d in deltas) / n
        rmse = (sum(d * d for d in deltas) / n) ** 0.5
        return {"n": n, "bias": round(bias, 3), "mae": round(mae, 3), "rmse": round(rmse, 3)}

    dTa = [float(r["dTa"]) for r in rows if r.get("dTa") not in ("", None)]
    dRH = [float(r["dRH"]) for r in rows if r.get("dRH") not in ("", None)]
    s_ta = _stats(dTa)
    s_rh = _stats(dRH)
    if dTa:
        s_ta["pct_over_2C"] = round(100 * sum(1 for d in dTa if abs(d) > 2) / len(dTa), 2)
        s_ta["pct_over_3C"] = round(100 * sum(1 for d in dTa if abs(d) > 3) / len(dTa), 2)
    if dRH:
        s_rh["pct_over_10"] = round(100 * sum(1 for d in dRH if abs(d) > 10) / len(dRH), 2)
        s_rh["pct_over_20"] = round(100 * sum(1 for d in dRH if abs(d) > 20) / len(dRH), 2)
    return {"Ta": s_ta, "RH": s_rh}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=90)
    args = p.parse_args()

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=args.days)
    logger.info("range: %s → %s (%d days)", start_date, end_date, args.days)

    cities = load_cities(CITIES_PATH)
    summary: dict = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "lead_time": "nowcast (most recent forecast available)",
        "forecast_model": "ecmwf_ifs025",
        "truth_source": "ERA5 (Open-Meteo archive API)",
        "cities": {},
    }

    total_rows = 0
    for c in cities:
        logger.info("=== %s (%.2f, %.2f) ===", c.name, c.lat, c.lon)

        fcst_payload = _fetch_hourly(
            FCST_API, c.lat, c.lon, start_date.isoformat(), end_date.isoformat(),
            model="ecmwf_ifs025",
        )
        time.sleep(0.5)
        era5_payload = _fetch_hourly(
            ARCHIVE_API, c.lat, c.lon, start_date.isoformat(), end_date.isoformat()
        )
        time.sleep(0.5)
        if not fcst_payload or not era5_payload:
            logger.warning("  skipped: missing payload")
            continue

        fcst = _hourly_to_dict(fcst_payload)
        era5 = _hourly_to_dict(era5_payload)

        # Forecast history rows
        fcst_rows = []
        for ts, (ta, rh) in fcst.items():
            fcst_rows.append({
                "logged_at_ist": ts,
                "Ta_C": "" if ta is None else round(ta, 2),
                "RH_pct": "" if rh is None else round(rh, 1),
            })

        # Joined error rows: only hours present in both feeds
        err_rows = []
        for ts, (fta, frh) in fcst.items():
            if ts not in era5:
                continue
            eta, erh = era5[ts]
            row = {
                "logged_at_ist": ts,
                "fcst_Ta": "" if fta is None else round(fta, 2),
                "era5_Ta": "" if eta is None else round(eta, 2),
                "dTa": "" if (fta is None or eta is None) else round(fta - eta, 2),
                "fcst_RH": "" if frh is None else round(frh, 1),
                "era5_RH": "" if erh is None else round(erh, 1),
                "dRH": "" if (frh is None or erh is None) else round(frh - erh, 1),
            }
            err_rows.append(row)

        # Write per-year CSVs
        for yr, rr in _split_by_year(fcst_rows).items():
            _write_csv(FCST_DIR / c.name / f"{yr}.csv", FCST_COLS, rr)
        for yr, rr in _split_by_year(err_rows).items():
            _write_csv(ERR_DIR / c.name / f"{yr}.csv", ERR_COLS, rr)
            total_rows += len(rr)

        summary["cities"][c.name] = _summary_for_city(err_rows)
        s = summary["cities"][c.name]
        logger.info(
            "  Ta: bias=%+.2f MAE=%.2f RMSE=%.2f n=%d  |  RH: bias=%+.1f MAE=%.1f RMSE=%.1f n=%d",
            s["Ta"].get("bias", 0), s["Ta"].get("mae", 0), s["Ta"].get("rmse", 0), s["Ta"].get("n", 0),
            s["RH"].get("bias", 0), s["RH"].get("mae", 0), s["RH"].get("rmse", 0), s["RH"].get("n", 0),
        )

    (ERR_DIR).mkdir(parents=True, exist_ok=True)
    with (ERR_DIR / "summary.json").open("w") as f:
        json.dump(summary, f, indent=2)
    logger.info("done. wrote %d error rows; summary at %s", total_rows, ERR_DIR / "summary.json")


if __name__ == "__main__":
    sys.exit(main())
