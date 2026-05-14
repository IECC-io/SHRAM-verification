"""Fetch IMD's official district-level warning feed.

Replaces the older station-level heatwave feed (HW_Status_Forecast.json) with
IMD's district-level warning GeoJSON, which covers all ~764 Indian districts
with polygon geometry and a 5-day forecast of warning codes.

Data source (public, no API key required):
    https://reactjs.imd.gov.in/geoserver/wfs
        ?service=WFS
        &version=1.1.0
        &request=GetFeature
        &typename=imd:district_warnings_india
        &srsname=EPSG:4326
        &outputFormat=application/json

This is the same WFS endpoint that powers
https://mausam.imd.gov.in/responsive/districtWiseWarningGIS.php

Warning-code legend (from the GIS page's JS) — Day_N is a comma-separated list:
    1  No Warning
    2  Heavy Rain
    3  Heavy Snow
    4  Thunderstorms & Lightning
    5  Hailstorm
    6  Dust Storm
    7  Dust Raising Winds
    8  Strong Surface Winds
    9  Heat Wave         ← heat-relevant
    10 Hot Day            ← heat-relevant
    11 Warm Night         ← heat-relevant
    12 Cold Wave
    13 Cold Day
    14 Ground Frost
    15 Fog
    16 Very Heavy Rain
    17 Extremely Heavy Rain

Day{N}_Color encodes severity (1 Green / 2 Yellow / 3 Orange / 4 Red — IMD's
standard impact-based color code).

Outputs:
    reference_history/imd_districtwarning/{stamp}.json   — full payload (archive)
    reference_history/imd_districtwarning_latest.json    — dashboard-facing copy
    reference_history/imd_heatwave_latest.json           — back-compat shim:
        a points-style payload derived from district centroids, so the existing
        dashboard map continues to render until the frontend is updated.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
REF_DIR = DASHBOARD_ROOT / "reference_history"
ARCHIVE_DIR = REF_DIR / "imd_districtwarning"
LATEST_FULL = REF_DIR / "imd_districtwarning_latest.json"
LATEST_POINTS_COMPAT = REF_DIR / "imd_heatwave_latest.json"

FEED_URL = "https://reactjs.imd.gov.in/geoserver/wfs"
FEED_PARAMS = {
    "service": "WFS",
    "version": "1.1.0",
    "request": "GetFeature",
    "typename": "imd:district_warnings_india",
    "srsname": "EPSG:4326",
    "outputFormat": "application/json",
}
REQUEST_TIMEOUT = 90

HEAT_CODES = {9, 10, 11}            # Heat Wave / Hot Day / Warm Night
HW_ONLY_CODE = 9                     # Heat Wave alone (most severe heat category)

CATEGORY = {
    1:  "No Warning",
    2:  "Heavy Rain",
    3:  "Heavy Snow",
    4:  "Thunderstorms",
    5:  "Hailstorm",
    6:  "Dust Storm",
    7:  "Dust Raising Winds",
    8:  "Strong Surface Winds",
    9:  "Heat Wave",
    10: "Hot Day",
    11: "Warm Night",
    12: "Cold Wave",
    13: "Cold Day",
    14: "Ground Frost",
    15: "Fog",
    16: "Very Heavy Rain",
    17: "Extremely Heavy Rain",
}

COLOR_TIER = {
    1: "Green",   # No warning / Be aware
    2: "Yellow",  # Watch
    3: "Orange",  # Alert
    4: "Red",     # Warning
}


def _parse_codes(raw: str | None) -> list[int]:
    if not raw:
        return []
    out = []
    for piece in str(raw).split(","):
        piece = piece.strip()
        if not piece:
            continue
        try:
            out.append(int(piece))
        except ValueError:
            continue
    return out


def _safe_int(v, default=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _polygon_centroid(geom: dict) -> tuple[float | None, float | None]:
    """Cheap polygon centroid via averaging vertex coords. Good enough for
    point-overlay back-compat — not for area-weighted calculations."""
    try:
        coords = geom.get("coordinates")
        if not coords:
            return None, None
        # Flatten all rings of all polygons
        pts = []
        if geom.get("type") == "Polygon":
            for ring in coords:
                pts.extend(ring)
        elif geom.get("type") == "MultiPolygon":
            for poly in coords:
                for ring in poly:
                    pts.extend(ring)
        if not pts:
            return None, None
        lon = sum(p[0] for p in pts) / len(pts)
        lat = sum(p[1] for p in pts) / len(pts)
        return lat, lon
    except Exception:
        return None, None


def normalize(raw: dict) -> dict:
    """Reshape the WFS GeoJSON into a dashboard-friendly payload.

    Returns a dict with:
      label, source_url, fetched_at_utc
      n_total, n_heatwave_today, n_heat_any_5d
      districts: list[dict]   per-district summary + 5-day forecast
      geojson: dict           the original GeoJSON (polygons retained)
    """
    districts = []
    for feat in raw.get("features", []):
        p = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        # IMD's lat/lon fields are often 0 — fall back to centroid
        lat = _safe_int(p.get("lat"), 0)
        lon = _safe_int(p.get("lon"), 0)
        if not lat or not lon:
            clat, clon = _polygon_centroid(geom)
            lat = clat if clat is not None else None
            lon = clon if clon is not None else None

        days = []
        for di in range(1, 6):
            codes = _parse_codes(p.get(f"Day_{di}"))
            color_idx = _safe_int(p.get(f"Day{di}_Color"), 1)
            heat_codes_today = [c for c in codes if c in HEAT_CODES]
            days.append({
                "day": di,
                "codes": codes,
                "labels": [CATEGORY.get(c, f"Code {c}") for c in codes],
                "color_idx": color_idx,
                "color_tier": COLOR_TIER.get(color_idx, "?"),
                "text": (p.get(f"Day{di}_text") or "").strip() or None,
                "has_heatwave": HW_ONLY_CODE in codes,
                "has_any_heat": bool(heat_codes_today),
            })

        district_entry = {
            "id": p.get("ID") or p.get("id"),
            "district": p.get("District"),
            "state": p.get("state"),
            "subdivision": p.get("sub"),
            "mc": p.get("mc"),
            "rmc": p.get("rmc"),
            "date": p.get("Date"),
            "updated_at": p.get("updated_at"),
            "lat": lat,
            "lon": lon,
            "days": days,
            "today_heatwave": days[0]["has_heatwave"],
            "today_any_heat": days[0]["has_any_heat"],
            "today_color": days[0]["color_idx"],
        }
        districts.append(district_entry)

    n_heatwave_today = sum(1 for d in districts if d["today_heatwave"])
    n_any_heat_today = sum(1 for d in districts if d["today_any_heat"])
    n_heat_any_5d = sum(
        1 for d in districts
        if any(day["has_any_heat"] for day in d["days"])
    )

    return {
        "label": "IMD district warnings (official)",
        "source_url": f"{FEED_URL}?{','.join(f'{k}={v}' for k,v in FEED_PARAMS.items())}",
        "viz_url": "https://mausam.imd.gov.in/responsive/districtWiseWarningGIS.php",
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "n_total": len(districts),
        "n_heatwave_today": n_heatwave_today,
        "n_any_heat_today": n_any_heat_today,
        "n_heat_any_5d": n_heat_any_5d,
        "districts": districts,
        "geojson": raw,
    }


def points_compat(payload: dict) -> dict:
    """Emit a points-shaped payload matching the old fetch_imd_heatwave.py
    output, so the existing dashboard's map layer keeps rendering until the
    frontend is updated to use polygons.

    hw_status mapping: 0 = no heat, 1 = Hot Day or Warm Night, 2 = Heat Wave.
    """
    points = []
    for d in payload["districts"]:
        today = d["days"][0]
        codes_today = set(today["codes"])
        if HW_ONLY_CODE in codes_today:
            hw_status = 2 if d["today_color"] >= 3 else 1
            hw_label = "Severe heatwave" if d["today_color"] >= 3 else "Heatwave"
        elif codes_today & HEAT_CODES:
            hw_status = 1
            hw_label = ", ".join(CATEGORY.get(c, "?") for c in codes_today
                                 if c in HEAT_CODES)
        else:
            hw_status = 0
            hw_label = "Normal"

        if d["lat"] is None or d["lon"] is None:
            continue

        points.append({
            "lat": d["lat"],
            "lon": d["lon"],
            "Ta": None,   # IMD district feed does not include Tmax
            "RH": None,
            "hw_status": hw_status,
            "hw_label": hw_label,
            "station_code": d["id"],
            "station_type": "District",
            "name": f"IMD district: {d['district']} ({d['state']})",
            "forecast": {
                f"D{day['day']}": {
                    "max": None,
                    "min": None,
                    "hw": (
                        2 if HW_ONLY_CODE in day["codes"] and day["color_idx"] >= 3
                        else 1 if (set(day["codes"]) & HEAT_CODES)
                        else 0
                    ),
                }
                for day in d["days"]
            },
        })

    return {
        "label": "IMD district warnings (heat subset, derived from districtwarning feed)",
        "source_url": payload["source_url"],
        "fetched_at_utc": payload["fetched_at_utc"],
        "n_total": len(points),
        "n_heatwave": sum(1 for p in points if p["hw_status"] == 1),
        "n_severe":   sum(1 for p in points if p["hw_status"] == 2),
        "n_with_ta":  0,
        "points": points,
    }


def main() -> int:
    logger.info("fetching %s", FEED_URL)
    resp = requests.get(FEED_URL, params=FEED_PARAMS,
                        timeout=REQUEST_TIMEOUT, verify=False)
    resp.raise_for_status()
    raw = resp.json()
    payload = normalize(raw)

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = ARCHIVE_DIR / f"imd_districtwarning_{stamp}.json"
    with archive_path.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info(
        "archived %d districts (today: %d heatwave / %d any-heat; 5d: %d) → %s",
        payload["n_total"], payload["n_heatwave_today"],
        payload["n_any_heat_today"], payload["n_heat_any_5d"], archive_path,
    )

    with LATEST_FULL.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote dashboard-facing full copy → %s", LATEST_FULL)

    compat = points_compat(payload)
    with LATEST_POINTS_COMPAT.open("w") as f:
        json.dump(compat, f, indent=2)
    logger.info("wrote points back-compat copy → %s (%d heat points)",
                LATEST_POINTS_COMPAT,
                compat["n_heatwave"] + compat["n_severe"])

    return 0


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    sys.exit(main())
