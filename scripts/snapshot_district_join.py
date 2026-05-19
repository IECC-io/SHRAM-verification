"""District-level daily snapshot joining AWS alerts with SHRAM zones.

For each Indian district polygon at 14:30 IST:
  - Worst AWS alert from stations within 30 km
  - SHRAM zone for MET {3,4,5,6} × {shade,sun} aggregated to the worst zone in
    grid points inside the district

Reads from:
  - reference_history/aws_2pm_snapshot_latest.json (written by snapshot_aws_2pm.py)
  - https://shram.info/grid_data.json  (live grid)
  - reference_history/imd_districtwarning_geojson.json  (district polygons)

Appends to:
  - <data-repo>/comparisons/district_2pm_snapshot.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import ssl
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
AWS_2PM_LATEST = DASHBOARD_ROOT / "reference_history" / "aws_2pm_snapshot_latest.json"
DISTRICTS_GEOJSON = DASHBOARD_ROOT / "reference_history" / "imd_districtwarning_geojson.json"
SHRAM_URL = "https://shram.info/grid_data.json"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

NEAR_KM = 30

FIELDS = [
    "snapshot_date_ist", "snapshot_time_ist",
    "district", "state",
    "centroid_lat", "centroid_lon",
    "aws_worst_status", "aws_worst_label", "aws_n_stations_30km",
    # Worst zone per MET × sun condition across all SHRAM grid points in the district
    "shram_n_pts",
    "zone_met3_shade", "zone_met3_sun",
    "zone_met4_shade", "zone_met4_sun",
    "zone_met5_shade", "zone_met5_sun",
    "zone_met6_shade", "zone_met6_sun",
]


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "shram-verification-district-snapshot"})
    with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
        return json.loads(r.read())


def haversine_km(la1, lo1, la2, lo2):
    R = 6371.0
    r = math.radians
    dla, dlo = r(la2 - la1), r(lo2 - lo1)
    a = math.sin(dla / 2) ** 2 + math.cos(r(la1)) * math.cos(r(la2)) * math.sin(dlo / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def polygon_bbox(geom: dict) -> tuple[float, float, float, float] | None:
    """Compute (minlon, minlat, maxlon, maxlat) for a Polygon or MultiPolygon."""
    coords = geom.get("coordinates") or []
    typ = geom.get("type")
    pts: list[tuple[float, float]] = []
    if typ == "Polygon":
        for ring in coords:
            pts.extend(ring)
    elif typ == "MultiPolygon":
        for poly in coords:
            for ring in poly:
                pts.extend(ring)
    if not pts:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return (min(xs), min(ys), max(xs), max(ys))


def point_in_polygon(lon: float, lat: float, geom: dict) -> bool:
    """Ray-casting point-in-polygon for Polygon or MultiPolygon. Lon/lat order."""
    typ = geom.get("type")
    coords = geom.get("coordinates") or []
    rings = []
    if typ == "Polygon":
        rings = coords
    elif typ == "MultiPolygon":
        for poly in coords:
            rings.extend(poly)
    inside = False
    for ring in rings:
        n = len(ring)
        if n < 3:
            continue
        j = n - 1
        for i in range(n):
            xi, yi = ring[i][0], ring[i][1]
            xj, yj = ring[j][0], ring[j][1]
            intersect = ((yi > lat) != (yj > lat)) and \
                        (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi)
            if intersect:
                inside = not inside
            j = i
    return inside


def append_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data-repo",
        default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
        help="Path to the SHRAM-verification-data clone where CSV is appended.",
    )
    args = ap.parse_args()
    data_repo = Path(args.data_repo)
    if not data_repo.exists():
        logger.error("data repo not found at %s", data_repo)
        return 1
    if not AWS_2PM_LATEST.exists():
        logger.error("AWS snapshot not found at %s — run snapshot_aws_2pm.py first", AWS_2PM_LATEST)
        return 1
    if not DISTRICTS_GEOJSON.exists():
        logger.error("districts geojson not found at %s", DISTRICTS_GEOJSON)
        return 1

    logger.info("loading AWS 2 PM snapshot")
    with AWS_2PM_LATEST.open() as f:
        aws_payload = json.load(f)
    aws_points = aws_payload.get("points", [])
    snapshot_date = aws_payload.get("snapshot_date_ist") or ""
    snapshot_time = aws_payload.get("snapshot_time_ist") or ""

    logger.info("loading districts geojson")
    with DISTRICTS_GEOJSON.open() as f:
        districts_payload = json.load(f)
    districts = (districts_payload.get("geojson") or districts_payload).get("features", [])
    logger.info("  %d districts", len(districts))

    logger.info("fetching SHRAM live grid")
    shram = fetch_json(SHRAM_URL)
    shram_pts = shram.get("points", [])
    logger.info("  %d SHRAM grid points", len(shram_pts))

    # Pre-index SHRAM points by 1° lat/lon buckets for fast PIP candidate lookup
    shram_buckets: dict[tuple[int, int], list[dict]] = {}
    for pt in shram_pts:
        la, lo = pt.get("lat"), pt.get("lon")
        if la is None or lo is None:
            continue
        shram_buckets.setdefault((int(la), int(lo)), []).append(pt)

    rows = []
    for feat in districts:
        geom = feat.get("geometry")
        props = feat.get("properties") or {}
        if not geom:
            continue
        bbox = polygon_bbox(geom)
        if not bbox:
            continue
        minlon, minlat, maxlon, maxlat = bbox
        cx, cy = (minlon + maxlon) / 2, (minlat + maxlat) / 2

        # Worst AWS alert within NEAR_KM of district centroid
        worst_aws_status, worst_aws_label, n_aws = 0, "Normal", 0
        for pt in aws_points:
            la, lo = pt.get("lat"), pt.get("lon")
            if la is None or lo is None:
                continue
            # Cheap bbox prune
            if la < minlat - 0.5 or la > maxlat + 0.5:
                continue
            if lo < minlon - 0.5 or lo > maxlon + 0.5:
                continue
            if haversine_km(cy, cx, la, lo) > NEAR_KM:
                continue
            n_aws += 1
            s = pt.get("alert_status") or 0
            if s > worst_aws_status:
                worst_aws_status = s
                worst_aws_label = pt.get("alert_label") or "Heatwave"

        # Worst SHRAM zone per MET × sun across grid points inside the polygon
        worst_zones: dict[str, int] = {f"zone_met{m}_{s}": 0
                                       for m in (3, 4, 5, 6) for s in ("shade", "sun")}
        n_shram = 0
        # Only check grid points in 1° buckets overlapping the bbox
        for la_b in range(int(minlat), int(maxlat) + 1):
            for lo_b in range(int(minlon), int(maxlon) + 1):
                for pt in shram_buckets.get((la_b, lo_b), []):
                    la, lo = pt["lat"], pt["lon"]
                    if not (minlat <= la <= maxlat and minlon <= lo <= maxlon):
                        continue
                    if not point_in_polygon(lo, la, geom):
                        continue
                    n_shram += 1
                    data = pt.get("data") or {}
                    for met in (3, 4, 5, 6):
                        for sun in ("shade", "sun"):
                            cell = (data.get(f"met{met}") or {}).get(sun) or {}
                            z = cell.get("zone") or 0
                            key = f"zone_met{met}_{sun}"
                            if z > worst_zones[key]:
                                worst_zones[key] = z

        if n_aws == 0 and n_shram == 0:
            continue   # district had no nearby AWS station AND no SHRAM grid points

        row = {
            "snapshot_date_ist": snapshot_date,
            "snapshot_time_ist": snapshot_time,
            "district": props.get("District") or "",
            "state": props.get("state") or "",
            "centroid_lat": round(cy, 4),
            "centroid_lon": round(cx, 4),
            "aws_worst_status": worst_aws_status,
            "aws_worst_label": worst_aws_label,
            "aws_n_stations_30km": n_aws,
            "shram_n_pts": n_shram,
            **worst_zones,
        }
        rows.append(row)

    out_csv = data_repo / "comparisons" / "district_2pm_snapshot.csv"
    append_csv(out_csv, FIELDS, rows)
    logger.info("appended %d district rows to %s", len(rows), out_csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
