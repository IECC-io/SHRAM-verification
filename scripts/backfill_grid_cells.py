"""Backfill historical Ta/RH for every shram grid cell from multiple sources.

Pulls all 4,642 cells from shram's live grid_data.json, then for each cell hits
each Open-Meteo archive endpoint over the last N days. Output is per-(source, year)
Parquet, written to a sibling repo (SHRAM-verification-data) for size reasons.

Output:
    {OUT_ROOT}/{source}/{year}.parquet
        cell_lat, cell_lon, ts_utc (ms epoch), Ta_C, RH_pct, model

Plus an index file:
    {OUT_ROOT}/grid_cells.parquet
        cell_lat, cell_lon, district, state

Usage:
    python scripts/backfill_grid_cells.py --days 90 --out-root /tmp/SHRAM-verification-data
    python scripts/backfill_grid_cells.py --days 90 --resume   # skip cells already done
    python scripts/backfill_grid_cells.py --days 90 --limit 50 # debug, do first 50 cells

Politeness:
    - 0.4 s sleep between requests
    - On HTTP 429, exponential backoff up to 5 min, then continue

Resume strategy:
    - Within each source, completed cells are tracked in {OUT_ROOT}/{source}/_done.txt
    - If interrupted, --resume skips cells already in that file
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_ROOT = Path("/tmp/SHRAM-verification-data")

SHRAM_GRID_URL = "https://shram.info/grid_data.json"
ARCHIVE_API = "https://archive-api.open-meteo.com/v1/archive"
FCST_API = "https://historical-forecast-api.open-meteo.com/v1/forecast"

SOURCES = {
    "open_meteo":              {"api": ARCHIVE_API, "model": None},
    "open_meteo_ecmwf":        {"api": FCST_API,    "model": "ecmwf_ifs025"},
    "open_meteo_ecmwf_hres":   {"api": FCST_API,    "model": "ecmwf_ifs"},
    "open_meteo_gfs":          {"api": FCST_API,    "model": "ncep_gfs013"},
    "open_meteo_gfs_graphcast":{"api": FCST_API,    "model": "ncep_gfs_graphcast025"},
    "open_meteo_dwd_icon":     {"api": FCST_API,    "model": "icon_global"},
    "open_meteo_ukmo":         {"api": FCST_API,    "model": "ukmo_global_deterministic_10km"},
}

REQUEST_SLEEP = 0.4
MAX_BACKOFF = 300


def fetch_with_backoff(url, params):
    backoff = 5
    while True:
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                logger.warning("HTTP 429 — sleeping %ds", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in (502, 503, 504):
                logger.warning("HTTP %d — sleeping %ds", e.response.status_code, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            raise
        except (requests.ConnectionError, requests.Timeout) as e:
            logger.warning("network error %s — sleeping %ds", e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


def fetch_cells():
    payload = requests.get(SHRAM_GRID_URL, timeout=30).json()
    cells = []
    for p in payload.get("points", []):
        lat = p.get("lat"); lon = p.get("lon")
        if lat is None or lon is None: continue
        cells.append({
            "cell_lat": float(lat),
            "cell_lon": float(lon),
            "district": p.get("district"),
            "state": p.get("state"),
        })
    return cells


def fetch_one(api_url, model, lat, lon, start, end):
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": start, "end_date": end,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "UTC",
    }
    if model:
        params["models"] = model
    payload = fetch_with_backoff(api_url, params)
    h = payload.get("hourly") or {}
    times = h.get("time", []) or []
    temps = h.get("temperature_2m", []) or []
    rhs = h.get("relative_humidity_2m", []) or []
    rows = []
    for t, ta, rh in zip(times, temps, rhs):
        if ta is None and rh is None: continue
        ts_utc = pd.Timestamp(t, tz="UTC").value // 10**6  # ms epoch
        rows.append({
            "cell_lat": lat, "cell_lon": lon,
            "ts_utc": ts_utc,
            "Ta_C": None if ta is None else round(float(ta), 2),
            "RH_pct": None if rh is None else round(float(rh), 1),
            "model": model or "ERA5_analysis",
        })
    return rows


def append_parquet(rows, path: Path):
    """Append `rows` to `path` (per-source-per-year). Reads existing, concats, dedupes."""
    if not rows:
        return 0
    new_df = pd.DataFrame(rows)
    if path.exists():
        old_df = pd.read_parquet(path)
        df = pd.concat([old_df, new_df], ignore_index=True)
        df.drop_duplicates(subset=["cell_lat", "cell_lon", "ts_utc"], keep="last", inplace=True)
    else:
        df = new_df
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, compression="zstd", index=False)
    return len(df)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    p.add_argument("--limit", type=int, help="Stop after N cells (debug)")
    p.add_argument("--sources", nargs="+", choices=list(SOURCES.keys()),
                   help="Only run these sources (default: all)")
    p.add_argument("--resume", action="store_true",
                   help="Skip cells already in {source}/_done.txt")
    p.add_argument("--flush-every", type=int, default=200,
                   help="Append to parquet every N cells (default 200)")
    args = p.parse_args()

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=args.days)
    logger.info("range %s → %s (%d days)", start, end, args.days)

    cells = fetch_cells()
    logger.info("loaded %d cells from shram grid", len(cells))
    if args.limit:
        cells = cells[:args.limit]
        logger.info("limited to %d", len(cells))

    # Write the cell index once
    idx_path = args.out_root / "grid_cells.parquet"
    idx_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cells).to_parquet(idx_path, compression="zstd", index=False)
    logger.info("wrote cell index: %s", idx_path)

    sources_to_run = args.sources or list(SOURCES.keys())
    for source_label in sources_to_run:
        conf = SOURCES[source_label]
        logger.info("=== source: %s (model=%s) ===", source_label, conf["model"])
        src_dir = args.out_root / source_label
        src_dir.mkdir(parents=True, exist_ok=True)
        done_path = src_dir / "_done.txt"
        done = set()
        if args.resume and done_path.exists():
            done = set(done_path.read_text().splitlines())
            logger.info("resume: skipping %d cells already done", len(done))

        # Buffer rows by year to amortize parquet writes
        buffers: dict[int, list[dict]] = {}
        n_done_session = 0
        n_skipped = 0
        for i, c in enumerate(cells, start=1):
            cell_key = f"{c['cell_lat']:.4f},{c['cell_lon']:.4f}"
            if cell_key in done:
                n_skipped += 1
                continue
            try:
                rows = fetch_one(conf["api"], conf["model"], c["cell_lat"], c["cell_lon"],
                                 start.isoformat(), end.isoformat())
            except Exception as exc:
                logger.error("  fetch failed for cell %s: %s", cell_key, exc)
                continue
            for r in rows:
                yr = pd.Timestamp(r["ts_utc"], unit="ms", tz="UTC").year
                buffers.setdefault(yr, []).append(r)
            done.add(cell_key)
            n_done_session += 1
            time.sleep(REQUEST_SLEEP)

            if n_done_session % args.flush_every == 0:
                for yr, rr in buffers.items():
                    n = append_parquet(rr, src_dir / f"{yr}.parquet")
                    logger.info("  flush at cell %d/%d  year %d -> %d total rows",
                                i, len(cells), yr, n)
                buffers.clear()
                done_path.write_text("\n".join(sorted(done)))
                logger.info("  progress %d/%d cells (skipped %d)", i, len(cells), n_skipped)

        # Final flush
        for yr, rr in buffers.items():
            n = append_parquet(rr, src_dir / f"{yr}.parquet")
            logger.info("  final flush year %d -> %d total rows", yr, n)
        done_path.write_text("\n".join(sorted(done)))
        logger.info("source %s complete: %d cells done this session, %d skipped",
                    source_label, n_done_session, n_skipped)


if __name__ == "__main__":
    sys.exit(main())
