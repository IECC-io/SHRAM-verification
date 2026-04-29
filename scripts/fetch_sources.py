"""Fetch Ta / RH for a list of cities from multiple reference sources.

Each fetcher returns a list of dicts with schema:
    { "source", "city", "lat", "lon", "timestamp_utc", "Ta_C", "RH_pct", "notes" }

All functions are expected to be resilient: on failure, return [] and log, do not raise.
"""

from __future__ import annotations

import csv
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 20
SHRAM_ALERTS_URL = "https://iecc-io.github.io/SHRAM/weather_logs/latest_alerts.json"
SHRAM_GRID_URL = "https://shram.info/grid_data.json"


@dataclass
class City:
    name: str
    state: str
    lat: float
    lon: float
    shram_district: str
    shram_station: str
    notes: str


def load_cities(path: Path) -> list[City]:
    cities: list[City] = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            cities.append(
                City(
                    name=row["name"],
                    state=row["state"],
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    shram_district=row.get("shram_district", ""),
                    shram_station=row.get("shram_station", ""),
                    notes=row.get("notes", ""),
                )
            )
    return cities


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def fetch_shram_map(cities: list[City]) -> list[dict]:
    """Snapshot the nearest grid cell from shram's live zone map (grid_data.json).

    This is what users actually see on the live map. Source label: `shram_map`.
    """
    try:
        resp = requests.get(SHRAM_GRID_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("shram_map fetch failed: %s", exc)
        return []

    points = payload.get("points", [])
    grid_lat_step = payload.get("metadata", {}).get("resolution_deg", 0.25)

    out: list[dict] = []
    ts = _now_iso()
    for c in cities:
        # Find nearest grid cell by squared lat/lon distance (cheap and good enough at 0.25°).
        best = None
        best_d2 = None
        for p in points:
            try:
                dlat = p["lat"] - c.lat
                dlon = p["lon"] - c.lon
            except (KeyError, TypeError):
                continue
            d2 = dlat * dlat + dlon * dlon
            if best_d2 is None or d2 < best_d2:
                best_d2 = d2
                best = p
        if not best:
            out.append(_row("shram_map", c, ts, None, None, notes="no grid points"))
            continue
        zone_met4_shade = (
            best.get("data", {}).get("met4", {}).get("shade", {}).get("zone")
        )
        zone_met4_sun = (
            best.get("data", {}).get("met4", {}).get("sun", {}).get("zone")
        )
        out.append(
            _row(
                "shram_map",
                c,
                ts,
                _to_float(best.get("temp")),
                _to_float(best.get("rh")),
                notes=json.dumps({
                    "cell_lat": best.get("lat"),
                    "cell_lon": best.get("lon"),
                    "zone_met4_shade": zone_met4_shade,
                    "zone_met4_sun": zone_met4_sun,
                    "resolution_deg": grid_lat_step,
                }),
            )
        )
    return out


def fetch_shram_stations(cities: list[City]) -> list[dict]:
    """Pull shram's current alerts JSON once, then attach each city's row.

    This is the station-anchored alerts product (mix of IMD + Open-Meteo gap-fill).
    Source label: `shram_stations`.
    """
    try:
        resp = requests.get(SHRAM_ALERTS_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("shram fetch failed: %s", exc)
        return []

    alerts = payload.get("alerts", [])
    # Index by (state_lower, district_lower) and station for quick lookup
    by_station: dict[tuple[str, str], dict] = {}
    for a in alerts:
        key = (
            str(a.get("STATE", "")).strip().lower(),
            str(a.get("DISTRICT", "")).strip().lower(),
        )
        by_station[key] = a

    out: list[dict] = []
    ts = _now_iso()
    for c in cities:
        if not c.shram_district:
            out.append(
                _row("shram_stations", c, ts, None, None, notes="no matching shram district")
            )
            continue
        key = (c.state.lower(), c.shram_district.lower())
        match = by_station.get(key)
        if not match:
            out.append(_row("shram_stations", c, ts, None, None, notes="district not in alerts"))
            continue
        out.append(
            _row(
                "shram_stations",
                c,
                ts,
                _to_float(match.get("TEMP")),
                _to_float(match.get("RH")),
                notes=json.dumps(
                    {
                        "zone_light": match.get("Light Work Heat Stress Zone"),
                        "zone_hard": match.get("Hard Labor Heat Stress Zone"),
                        "station": match.get("STATION"),
                    }
                ),
            )
        )
    return out


def _fetch_open_meteo_variant(cities: list[City], source_label: str, models: str | None) -> list[dict]:
    out: list[dict] = []
    ts = _now_iso()
    for c in cities:
        try:
            params = {
                "latitude": c.lat,
                "longitude": c.lon,
                "current": "temperature_2m,relative_humidity_2m",
                "timezone": "UTC",
            }
            if models:
                params["models"] = models
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            cur = resp.json().get("current", {})
            out.append(
                _row(
                    source_label,
                    c,
                    ts,
                    _to_float(cur.get("temperature_2m")),
                    _to_float(cur.get("relative_humidity_2m")),
                    notes=f"model={models}" if models else "",
                )
            )
        except Exception as exc:
            logger.warning("%s failed for %s: %s", source_label, c.name, exc)
            out.append(_row(source_label, c, ts, None, None, notes=f"error: {exc}"))
        time.sleep(0.2)
    return out


def fetch_open_meteo(cities: list[City]) -> list[dict]:
    """Open-Meteo's default best_match blend."""
    return _fetch_open_meteo_variant(cities, "open_meteo", None)


def fetch_open_meteo_ecmwf(cities: list[City]) -> list[dict]:
    """ECMWF IFS only (forces a single physical model rather than a blend)."""
    return _fetch_open_meteo_variant(cities, "open_meteo_ecmwf", "ecmwf_ifs025")


def fetch_open_meteo_gfs(cities: list[City]) -> list[dict]:
    """NOAA GFS only — independent of ECMWF, useful for diff-map cross-check."""
    return _fetch_open_meteo_variant(cities, "open_meteo_gfs", "ncep_gfs013")


def fetch_open_meteo_ecmwf_hres(cities: list[City]) -> list[dict]:
    """ECMWF IFS — Open-Meteo's main ECMWF endpoint (label clarifies it's the headline model)."""
    return _fetch_open_meteo_variant(cities, "open_meteo_ecmwf_hres", "ecmwf_ifs")


def fetch_open_meteo_gfs_graphcast(cities: list[City]) -> list[dict]:
    """NOAA × DeepMind GraphCast — neural global model. Returns Ta only (no RH exposed)."""
    return _fetch_open_meteo_variant(cities, "open_meteo_gfs_graphcast", "ncep_gfs_graphcast025")


def fetch_open_meteo_dwd_icon(cities: list[City]) -> list[dict]:
    """DWD ICON global — independent European model, often competitive with ECMWF."""
    return _fetch_open_meteo_variant(cities, "open_meteo_dwd_icon", "icon_global")


def fetch_open_meteo_ukmo(cities: list[City]) -> list[dict]:
    """UK Met Office global — third independent European voice."""
    return _fetch_open_meteo_variant(cities, "open_meteo_ukmo", "ukmo_global_deterministic_10km")


def fetch_nasa_power(cities: list[City]) -> list[dict]:
    """NASA POWER hourly — last available hour. ~3-day lag, used for backfill."""
    out: list[dict] = []
    ts = _now_iso()
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    for c in cities:
        try:
            resp = requests.get(
                "https://power.larc.nasa.gov/api/temporal/hourly/point",
                params={
                    "parameters": "T2M,RH2M",
                    "community": "RE",
                    "latitude": c.lat,
                    "longitude": c.lon,
                    "start": today,
                    "end": today,
                    "format": "JSON",
                    "time-standard": "UTC",
                },
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            params = resp.json().get("properties", {}).get("parameter", {})
            t2m = params.get("T2M", {})
            rh = params.get("RH2M", {})
            # Pick latest non-sentinel (-999) hour
            ta_val = rh_val = None
            latest_key = None
            for k in sorted(t2m.keys()):
                if t2m.get(k, -999) > -900 and rh.get(k, -999) > -900:
                    ta_val = t2m[k]
                    rh_val = rh[k]
                    latest_key = k
            out.append(
                _row(
                    "nasa_power",
                    c,
                    ts,
                    _to_float(ta_val),
                    _to_float(rh_val),
                    notes=f"hour_utc={latest_key}" if latest_key else "no_data",
                )
            )
        except Exception as exc:
            logger.warning("nasa_power failed for %s: %s", c.name, exc)
            out.append(_row("nasa_power", c, ts, None, None, notes=f"error: {exc}"))
        time.sleep(0.5)
    return out


def fetch_imd_aws(cities: list[City]) -> list[dict]:
    """Hit the same IMD endpoint shram uses. Returns station-level obs where available."""
    out: list[dict] = []
    ts = _now_iso()
    try:
        temp_resp = requests.get(
            "http://aws.imd.gov.in:8091/AWS/hometemp.php",
            params={"a": "60", "b": "ALL_STATE"},
            timeout=REQUEST_TIMEOUT,
        )
        rh_resp = requests.get(
            "http://aws.imd.gov.in:8091/AWS/homerh.php",
            params={"a": "60", "b": "ALL_STATE"},
            timeout=REQUEST_TIMEOUT,
        )
        temp_resp.raise_for_status()
        rh_resp.raise_for_status()
        temp_rows = temp_resp.json() if temp_resp.text.strip().startswith(("[", "{")) else []
        rh_rows = rh_resp.json() if rh_resp.text.strip().startswith(("[", "{")) else []
    except Exception as exc:
        logger.warning("imd fetch failed: %s", exc)
        for c in cities:
            out.append(_row("imd_aws", c, ts, None, None, notes=f"error: {exc}"))
        return out

    # IMD returns a JSON array of comma-separated strings, not objects. Schema observed:
    #   temp: "lat,lon,type,STATE,DISTRICT,STATION,TEMP,date,time,code"
    #   rh:   same shape, RH in the value column
    def _index(rows):
        idx = {}
        for r in rows if isinstance(rows, list) else []:
            if not isinstance(r, str):
                continue
            parts = [p.strip() for p in r.split(",")]
            if len(parts) < 7:
                continue
            district = parts[4].lower()
            station = parts[5].lower()
            value = parts[6]
            idx[(district, station)] = value
        return idx

    temp_idx = _index(temp_rows)
    rh_idx = _index(rh_rows)

    for c in cities:
        if not c.shram_station or not c.shram_district:
            out.append(_row("imd_aws", c, ts, None, None, notes="no imd station configured"))
            continue
        key = (c.shram_district.upper().replace(" ", "_").lower(),
               c.shram_station.upper().replace(" ", "_").lower())
        ta = _to_float(temp_idx.get(key))
        rh = _to_float(rh_idx.get(key))
        out.append(
            _row(
                "imd_aws",
                c,
                ts,
                ta,
                rh,
                notes=f"station={c.shram_station}",
            )
        )
    return out


def _row(source, city: City, ts, ta, rh, notes=""):
    return {
        "timestamp_utc": ts,
        "source": source,
        "city": city.name,
        "state": city.state,
        "lat": city.lat,
        "lon": city.lon,
        "Ta_C": ta,
        "RH_pct": rh,
        "notes": notes,
    }


def _to_float(x):
    if x is None or x == "":
        return None
    try:
        v = float(x)
        if v < -100 or v > 1000:
            return None
        return v
    except (TypeError, ValueError):
        return None


def fetch_all(cities: list[City]) -> Iterable[dict]:
    for fetcher in (
        fetch_shram_map,
        fetch_shram_stations,
        fetch_open_meteo,
        fetch_open_meteo_ecmwf,
        fetch_open_meteo_ecmwf_hres,
        fetch_open_meteo_gfs,
        fetch_open_meteo_gfs_graphcast,
        fetch_open_meteo_dwd_icon,
        fetch_open_meteo_ukmo,
        fetch_nasa_power,
        fetch_imd_aws,
    ):
        try:
            yield from fetcher(cities)
        except Exception as exc:
            logger.exception("fetcher %s raised: %s", fetcher.__name__, exc)
