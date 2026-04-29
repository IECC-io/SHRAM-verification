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
ARCHIVE_DIR = DASHBOARD_ROOT / "reference_history" / "imd_heatwave"
LATEST_PATH = DASHBOARD_ROOT / "reference_history" / "imd_heatwave_latest.json"

FEED_URL = "https://dss.imd.gov.in/dwr_img/GIS/HW_Status_Forecast.json"
REQUEST_TIMEOUT = 60


def normalize(raw: dict) -> dict:
    """Reshape the raw GeoJSON into the dashboard's source format."""
    points = []
    for feat in raw.get("features", []):
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])
        # Day-1 forecast HW status drives the dashboard's "current" view.
        # The page itself uses D1F_HW (forecast for today/tomorrow window).
        hw_status = props.get("D1F_HW")
        if hw_status is None:
            hw_status = props.get("D1_HW") or 0
        try:
            hw_status = int(hw_status)
        except (TypeError, ValueError):
            hw_status = 0
        points.append({
            "lat": lat,
            "lon": lon,
            # Map IMD max-temp forecast to the dashboard's "Ta" channel
            "Ta": _fnum(props.get("D1F_Mx_Tem")) or _fnum(props.get("D1_Mx_Temp")),
            "RH": _fnum(props.get("D1_RH_1730")) or _fnum(props.get("D1_RH_0830")),
            "hw_status": hw_status,
            "hw_label": (
                "Severe heatwave" if hw_status == 2
                else "Heatwave" if hw_status == 1
                else "Normal"
            ),
            "station_code": props.get("Stat_Code"),
            "station_type": props.get("Stations"),
            "name": f"IMD {props.get('Stat_Code', '?')} ({props.get('Stations', '?')})",
            # Keep the raw forecast trajectory so we can visualize lead time later
            "forecast": {
                f"D{i}": {
                    "max": _fnum(props.get(f"D{i}_Mx_Temp")),
                    "min": _fnum(props.get(f"D{i}_Mn_Temp")),
                    "hw": _safe_int(props.get(f"D{i}_HW")),
                }
                for i in range(1, 8)
            },
        })
    return {
        "label": "IMD heatwave forecast (official)",
        "source_url": FEED_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_total": len(points),
        "n_heatwave": sum(1 for p in points if p["hw_status"] == 1),
        "n_severe": sum(1 for p in points if p["hw_status"] == 2),
        "n_with_ta": sum(1 for p in points if p["Ta"] is not None),
        "points": points,
    }


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
    archive_path = ARCHIVE_DIR / f"imd_heatwave_{stamp}.json"
    with archive_path.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("archived %d stations (%d heatwave, %d severe) to %s",
                payload["n_total"], payload["n_heatwave"], payload["n_severe"], archive_path)

    with LATEST_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote dashboard-facing copy to %s", LATEST_PATH)
    return 0


if __name__ == "__main__":
    # Suppress the InsecureRequestWarning from verify=False above.
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sys.exit(main())
