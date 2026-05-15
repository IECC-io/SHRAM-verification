"""Fetch IMD's official heatwave forecast feed.

Pulls https://dss.imd.gov.in/dwr_img/GIS/HW_Status_Forecast.json — IMD's daily
GeoJSON of station-level heatwave declarations and 7-day forecasts. Saves a
date-stamped archive copy plus a `latest.json` symlink-equivalent that the
dashboard reads.

Schema per feature (the parts we care about):
    properties.Latitude, properties.Longitude
    properties.Stat_Code, properties.Stations  (Plain/Hilly)
    properties.D1F_Mx_Tem, ..., properties.D7_Mx_Temp
    properties.D1F_HW ... D7_HW    (0 = none, 1 = heatwave, 2 = severe heatwave)
    properties.D1_RH_0830, D1_RH_1730

Run from the dashboard root:
    python3 scripts/fetch_imd_heatwave.py

Cite as: India Meteorological Department, Ministry of Earth Sciences.
Data feed: https://dss.imd.gov.in/dwr_img/GIS/heatwave.html
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = DASHBOARD_ROOT / "reference_history" / "imd_station_heatwave"
LATEST_PATH = DASHBOARD_ROOT / "reference_history" / "imd_station_heatwave_latest.json"

FEED_URL = "https://dss.imd.gov.in/dwr_img/GIS/HW_Status_Forecast.json"
REQUEST_TIMEOUT = 60


def normalize(raw: dict) -> dict:
    """Reshape the raw GeoJSON into a station-level payload retaining the
    fields we care about for verification:
      - observed Tmax today (D1_Mx_Temp) + departure from normal (D1_Mx_Dep)
      - observed Tmin today (D1_Mn_Temp) + departure (D1_Mn_Dep)
      - previous-day observed Tmax (PD_Mx_Temp) + departure (PD_Mx_Dep)
      - RH morning + evening (D1_RH_0830, D1_RH_1730)
      - today's heatwave flag (D1_HW)              ← observed-status
      - tomorrow's heatwave forecast (D1F_HW)      ← forecast
      - 7-day forecast trajectory of (Tmax, Tmin, HW)
    """
    points = []
    for feat in raw.get("features", []):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])

        # IMD's feed has no `D1_HW` field. Compute today's HW status ourselves
        # from observed Tmax + departure-from-normal + station type, using
        # IMD's documented heatwave criteria (Mausam guidance).
        station_type = props.get("Stations")
        tmax = _fnum(props.get("D1_Mx_Temp"))
        dep = _fnum(props.get("D1_Mx_Dep"))
        hw_today_obs = compute_hw_status(tmax, dep, station_type)
        hw_tomorrow_fc = _safe_int(props.get("D1F_HW"))

        points.append({
            "lat": lat,
            "lon": lon,
            "station_code": props.get("Stat_Code"),
            "station_type": props.get("Stations"),
            "name": f"IMD {props.get('Stat_Code', '?')} ({props.get('Stations', '?')})",

            # Today (observed)
            "today_tmax": _fnum(props.get("D1_Mx_Temp")),
            "today_tmax_dep": _fnum(props.get("D1_Mx_Dep")),
            "today_tmin": _fnum(props.get("D1_Mn_Temp")),
            "today_tmin_dep": _fnum(props.get("D1_Mn_Dep")),
            "today_rh_0830": _fnum(props.get("D1_RH_0830")),
            "today_rh_1730": _fnum(props.get("D1_RH_1730")),
            "today_rain_24h": _fnum(props.get("Pt_24_Rain")),
            "today_hw_status": hw_today_obs,
            "today_hw_label": _hw_label(hw_today_obs),

            # Yesterday (observed)
            "prev_tmax": _fnum(props.get("PD_Mx_Temp")),
            "prev_tmax_dep": _fnum(props.get("PD_Mx_Dep")),
            "prev_rh_1730": _fnum(props.get("PD_RH_1730")),

            # Tomorrow (forecast — Day 1 in IMD parlance is tomorrow when issued
            # by the morning bulletin)
            "fc_tmax_tomorrow": _fnum(props.get("D1F_Mx_Tem")),
            "fc_tmin_tomorrow": _fnum(props.get("D1F_Mn_Tem")),
            "fc_weather_tomorrow": props.get("D1F_Weathr"),
            "fc_hw_tomorrow": hw_tomorrow_fc,
            "fc_hw_label_tomorrow": _hw_label(hw_tomorrow_fc),

            # 7-day trajectory (forecast Tmax/Tmin/HW per day)
            "forecast_trajectory": {
                f"D{i}": {
                    "max": _fnum(props.get(f"D{i}_Mx_Temp")),
                    "min": _fnum(props.get(f"D{i}_Mn_Temp")),
                    "hw": _safe_int(props.get(f"D{i}_HW")),
                }
                for i in range(1, 8)
            },
        })

    n_today_hw = sum(1 for p in points if p["today_hw_status"] == 1)
    n_today_severe = sum(1 for p in points if p["today_hw_status"] == 2)
    n_fc_hw = sum(1 for p in points if p["fc_hw_tomorrow"] == 1)

    return {
        "label": "IMD station heatwave bulletin (observed today + forecast tomorrow)",
        "source_url": FEED_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_total": len(points),
        "n_today_heatwave": n_today_hw,
        "n_today_severe": n_today_severe,
        "n_fc_heatwave_tomorrow": n_fc_hw,
        "points": points,
    }


def _hw_label(status: int) -> str:
    return "Severe heatwave" if status == 2 else "Heatwave" if status == 1 else "Normal"


# IMD heatwave classification, applied to observed Tmax + departure from normal
# per station type.
# Reference: https://mausam.imd.gov.in/responsive/heatwave_guidance.php
#   Heat Wave is declared when:
#     - Plain station:   Tmax >= 40°C AND departure >= 4.5°C, OR Tmax >= 45°C
#     - Coastal station: Tmax >= 37°C AND departure >= 4.5°C, OR Tmax >= 45°C
#     - Hilly station:   Tmax >= 30°C AND departure >= 4.5°C
#   Severe Heat Wave when departure >= 6.5°C (any region) OR Tmax >= 47°C absolute.
THRESHOLDS = {
    "Plain":   {"tmax_min": 40.0, "absolute_hw": 45.0},
    "Coastal": {"tmax_min": 37.0, "absolute_hw": 45.0},
    "Hilly":   {"tmax_min": 30.0, "absolute_hw": None},   # no absolute trigger for hilly
}


def compute_hw_status(tmax, dep, station_type):
    """Return 0 (none), 1 (HW), or 2 (severe HW) per IMD criteria.

    Returns 0 if any input is missing or station_type is unrecognized.
    """
    if tmax is None or station_type not in THRESHOLDS:
        return 0
    thr = THRESHOLDS[station_type]
    # Absolute triggers (any region)
    if tmax >= 47.0:
        return 2
    if thr["absolute_hw"] is not None and tmax >= thr["absolute_hw"]:
        return 1
    # Departure-based criteria
    if dep is None:
        return 0
    if tmax >= thr["tmax_min"] and dep >= 6.5:
        return 2
    if tmax >= thr["tmax_min"] and dep >= 4.5:
        return 1
    return 0


def _fnum(v):
    try:
        if v is None or v == "" or v == "NIL":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def main() -> int:
    logger.info("fetching %s", FEED_URL)
    # IMD's TLS chain has had intermediate-cert hiccups historically; verify=False
    # is acceptable here because the feed is public and content-integrity isn't
    # security-critical. If you want stricter verification, install certifi or
    # the IMD root cert on the host.
    resp = requests.get(FEED_URL, timeout=REQUEST_TIMEOUT, verify=False)
    resp.raise_for_status()
    raw = resp.json()
    payload = normalize(raw)

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = ARCHIVE_DIR / f"imd_station_heatwave_{stamp}.json"
    with archive_path.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("archived %d stations (today HW=%d severe=%d; tomorrow forecast HW=%d) → %s",
                payload["n_total"],
                payload["n_today_heatwave"], payload["n_today_severe"],
                payload["n_fc_heatwave_tomorrow"], archive_path)

    with LATEST_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote dashboard-facing copy to %s", LATEST_PATH)
    return 0


if __name__ == "__main__":
    # Suppress the InsecureRequestWarning from verify=False above.
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sys.exit(main())
