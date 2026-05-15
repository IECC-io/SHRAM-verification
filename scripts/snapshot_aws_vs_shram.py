"""Snapshot: IMD AWS observed temp/RH vs SHRAM (Open-Meteo) live grid.

Pairs each fresh IMD AWS station with the nearest SHRAM grid point and writes
a JSON snapshot the dashboard reads. Same logic as /tmp/aws_vs_shram_snapshot.py
but persisted to reference_history/ for the dashboard.

Run from the dashboard root:
    python3 scripts/snapshot_aws_vs_shram.py
"""

from __future__ import annotations
import json
import math
import ssl
import statistics
import sys
import urllib.request
import logging
from datetime import datetime, timezone
from pathlib import Path

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = DASHBOARD_ROOT / "reference_history" / "aws_vs_shram_snapshot.json"

AWS_URL = ("https://reactjs.imd.gov.in/geoserver/wfs?service=WFS&version=1.1.0"
           "&request=GetFeature&typename=imd:aws_data_layer"
           "&srsname=EPSG:4326&outputFormat=application/json")
SHRAM_URL = "https://shram.info/grid_data.json"
FRESH_HOURS = 2
NEAR_KM = 30


def _fnum(v):
    try:
        if v is None or v == "" or v == "NULL":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def haversine_km(la1, lo1, la2, lo2):
    R = 6371
    r = math.radians
    dla = r(la2 - la1); dlo = r(lo2 - lo1)
    a = math.sin(dla / 2)**2 + math.cos(r(la1)) * math.cos(r(la2)) * math.sin(dlo / 2)**2
    return 2 * R * math.asin(math.sqrt(a))


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "shram-verification"})
    with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
        return json.loads(r.read())


def main() -> int:
    logger.info("fetching AWS layer")
    aws = fetch_json(AWS_URL)
    aws_features = aws.get("features", [])
    logger.info("  %d AWS records", len(aws_features))

    logger.info("fetching SHRAM grid")
    shram = fetch_json(SHRAM_URL)
    shram_pts = shram.get("points", [])
    shram_meta = shram.get("metadata", {})
    logger.info("  %d SHRAM grid points, generated %s",
                len(shram_pts), shram_meta.get("generated_at_ist"))

    # Build SHRAM bucket index (0.5° buckets)
    buckets = {}
    for pt in shram_pts:
        lat, lon = pt.get("lat"), pt.get("lon")
        if lat is None or lon is None:
            continue
        k = (round(lat * 2), round(lon * 2))
        buckets.setdefault(k, []).append(pt)

    def nearest_shram(lat, lon):
        best, best_d = None, float("inf")
        for dy in (-1, 0, 1):
            for dx in (-1, 0, 1):
                k = (round(lat * 2) + dy, round(lon * 2) + dx)
                for pt in buckets.get(k, []):
                    d = haversine_km(lat, lon, pt["lat"], pt["lon"])
                    if d < best_d:
                        best, best_d = pt, d
        return best, best_d

    # Filter AWS to fresh stations with usable readings
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    pairs = []
    n_fresh = 0
    for f in aws_features:
        p = f["properties"] or {}
        ut = p.get("update_time")
        if not ut:
            continue
        try:
            t = datetime.strptime(ut, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        age_h = (now - t).total_seconds() / 3600
        if age_h > FRESH_HOURS:
            continue
        temp = _fnum(p.get("temp"))
        if temp is None:
            continue
        n_fresh += 1
        rh = _fnum(p.get("rh"))
        coords = (f.get("geometry") or {}).get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])

        sh, dist = nearest_shram(lat, lon)
        if sh is None or dist > NEAR_KM:
            continue
        sh_temp = sh.get("temp")
        sh_rh = sh.get("rh")
        pairs.append({
            "station": p.get("station"),
            "call_sign": p.get("call_sign"),
            "lat": lat, "lon": lon,
            "aws_temp": temp,
            "aws_rh": rh,
            "aws_update_utc": ut,
            "aws_age_h": round(age_h, 2),
            "shram_lat": sh.get("lat"), "shram_lon": sh.get("lon"),
            "dist_km": round(dist, 2),
            "shram_temp": sh_temp,
            "shram_rh": sh_rh,
            "shram_sw": sh.get("sw"),
            "shram_zone_met6_sun": (sh.get("data") or {}).get("met6", {}).get("sun", {}).get("zone"),
        })

    # Aggregate stats on temp & RH
    temp_pairs = [(p["aws_temp"], p["shram_temp"]) for p in pairs if p["shram_temp"] is not None]
    rh_pairs = [(p["aws_rh"], p["shram_rh"]) for p in pairs
                if p["aws_rh"] is not None and p["shram_rh"] is not None]

    def stats(pair_list):
        if not pair_list:
            return None
        diffs = [s - a for a, s in pair_list]
        ma = statistics.mean(a for a, _ in pair_list)
        ms = statistics.mean(s for _, s in pair_list)
        n = len(pair_list)
        sd = statistics.stdev(diffs) if n > 1 else 0
        mae = statistics.mean(abs(d) for d in diffs)
        # Pearson r
        num = sum((a - ma) * (s - ms) for a, s in pair_list)
        denom = math.sqrt(sum((a - ma)**2 for a, _ in pair_list)
                          * sum((s - ms)**2 for _, s in pair_list))
        r = (num / denom) if denom > 0 else None
        return {
            "n": n,
            "mean_diff": statistics.mean(diffs),
            "median_diff": statistics.median(diffs),
            "stdev_diff": sd,
            "mae": mae,
            "min_diff": min(diffs),
            "max_diff": max(diffs),
            "pearson_r": r,
            "r_squared": (r * r) if r is not None else None,
        }

    payload = {
        "label": "AWS observed (IMD) vs SHRAM modeled (Open-Meteo) — snapshot comparison",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "shram_generated_at_ist": shram_meta.get("generated_at_ist"),
        "shram_is_nighttime": shram_meta.get("is_nighttime"),
        "fresh_hours_window": FRESH_HOURS,
        "max_pair_distance_km": NEAR_KM,
        "n_aws_total": len(aws_features),
        "n_aws_fresh_with_temp": n_fresh,
        "n_paired": len(pairs),
        "temperature_stats": stats(temp_pairs),
        "rh_stats": stats(rh_pairs),
        "pairs": pairs,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUT_PATH.open("w") as f:
        json.dump(payload, f, indent=2)
    logger.info("wrote %d pairs → %s", len(pairs), OUT_PATH)
    if payload["temperature_stats"]:
        t = payload["temperature_stats"]
        logger.info("temp: n=%d, mean_diff=%+.2f, MAE=%.2f, R²=%.3f",
                    t["n"], t["mean_diff"], t["mae"], t["r_squared"])
    if payload["rh_stats"]:
        r = payload["rh_stats"]
        logger.info("rh: n=%d, mean_diff=%+.2f, MAE=%.2f, R²=%.3f",
                    r["n"], r["mean_diff"], r["mae"], r["r_squared"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
