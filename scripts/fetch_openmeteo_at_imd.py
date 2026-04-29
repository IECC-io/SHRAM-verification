"""Fetch Open-Meteo current weather at every IMD AWS station location.

Reads `shram_stations_history/imd_stations.json` (the cached IMD station list with
lat/lon and the most recent IMD-reported temp_c) and queries the Open-Meteo
default best_match endpoint at each station's coordinates.

Open-Meteo accepts batched queries (comma-separated lat/lon lists, up to ~100
locations per request), so 751 stations resolve in ~8 batches.

Output schema matches what the dashboard's R² tab expects from a "source":
    { "label", "generated_at_utc", "points": [{lat, lon, Ta, RH, name}] }

Run from the dashboard root:
    python3 scripts/fetch_openmeteo_at_imd.py
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
IMD_CACHE = DASHBOARD_ROOT / "shram_stations_history" / "imd_stations.json"
OUT_PATH = DASHBOARD_ROOT / "reference_history" / "openmeteo_at_imd_stations.json"

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
BATCH_SIZE = 100
REQUEST_TIMEOUT = 60
SLEEP_BETWEEN_BATCHES = 1.0  # be polite to the public API
RETRY_SLEEP = 65  # Open-Meteo rate-limits at the minute mark; sleep past it
MAX_RETRIES = 3


def load_imd_stations() -> list[dict]:
    with IMD_CACHE.open() as f:
        payload = json.load(f)
    stations = payload.get("stations", [])
    # Drop stations with bad coordinates so we don't poison the batch
    clean = [
        s for s in stations
        if isinstance(s.get("lat"), (int, float))
        and isinstance(s.get("lon"), (int, float))
        and -90 <= s["lat"] <= 90
        and -180 <= s["lon"] <= 180
    ]
    dropped = len(stations) - len(clean)
    if dropped:
        logger.warning("dropped %d stations with bad coordinates", dropped)
    return clean


def fetch_batch(stations: list[dict]) -> list[dict]:
    """Fetch Open-Meteo current Ta/RH for a batch of stations."""
    lats = ",".join(f"{s['lat']:.4f}" for s in stations)
    lons = ",".join(f"{s['lon']:.4f}" for s in stations)
    params = {
        "latitude": lats,
        "longitude": lons,
        "current": "temperature_2m,relative_humidity_2m",
        "timezone": "UTC",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    # Single-location response is a dict; multi-location is a list.
    if isinstance(data, dict):
        data = [data]
    if len(data) != len(stations):
        raise RuntimeError(f"expected {len(stations)} results, got {len(data)}")

    out = []
    for st, result in zip(stations, data):
        cur = (result or {}).get("current") or {}
        ta = cur.get("temperature_2m")
        rh = cur.get("relative_humidity_2m")
        name = f"OM @ {st.get('station', '?')} ({st.get('district', '?')})"
        out.append({
            "lat": st["lat"],
            "lon": st["lon"],
            "Ta": float(ta) if ta is not None else None,
            "RH": float(rh) if rh is not None else None,
            "name": name,
            "station": st.get("station"),
            "district": st.get("district"),
            "state": st.get("state"),
        })
    return out


def main() -> int:
    if not IMD_CACHE.exists():
        logger.error("IMD cache not found at %s", IMD_CACHE)
        return 1

    stations = load_imd_stations()
    logger.info("fetching Open-Meteo for %d IMD stations", len(stations))

    points: list[dict] = []
    for i in range(0, len(stations), BATCH_SIZE):
        batch = stations[i : i + BATCH_SIZE]
        success = False
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                batch_points = fetch_batch(batch)
                points.extend(batch_points)
                logger.info("batch %d/%d done (%d points)",
                            i // BATCH_SIZE + 1,
                            (len(stations) + BATCH_SIZE - 1) // BATCH_SIZE,
                            len(batch_points))
                success = True
                break
            except requests.HTTPError as exc:
                last_exc = exc
                if exc.response is not None and exc.response.status_code == 429:
                    logger.warning("rate-limited on batch starting at %d; sleeping %ds (attempt %d/%d)",
                                   i, RETRY_SLEEP, attempt + 1, MAX_RETRIES)
                    time.sleep(RETRY_SLEEP)
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                logger.error("batch starting at %d failed: %s", i, exc)
                break
        if not success:
            for st in batch:
                points.append({
                    "lat": st["lat"],
                    "lon": st["lon"],
                    "Ta": None,
                    "RH": None,
                    "name": f"OM @ {st.get('station', '?')}",
                    "station": st.get("station"),
                    "district": st.get("district"),
                    "state": st.get("state"),
                    "error": str(last_exc),
                })
        time.sleep(SLEEP_BETWEEN_BATCHES)

    payload = {
        "label": "Open-Meteo (at IMD stations)",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_total": len(points),
        "n_with_ta": sum(1 for p in points if p["Ta"] is not None),
        "n_with_rh": sum(1 for p in points if p["RH"] is not None),
        "points": points,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote %d points (%d with Ta, %d with RH) to %s",
                payload["n_total"], payload["n_with_ta"], payload["n_with_rh"], OUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
