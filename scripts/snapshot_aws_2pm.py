"""Daily 2 PM IST snapshot of IMD AWS stations with computed heatwave alerts.

Pipeline:
  1. Fetch the IMD AWS layer (https://reactjs.imd.gov.in geoserver) at 14:30 IST.
  2. Filter to stations whose latest update_time is today's IST date AND
     within a recency window of the snapshot time.
  3. Cross-reference each AWS station against the 285 classified IMD heatwave
     stations (reference_history/imd_station_heatwave_latest.json) — inherit
     the nearest one's station_type (Plain / Coastal / Hilly).
  4. Apply IMD thresholds to AWS `temp` (current reading, near-2-PM):
        Plain   ≥40°C → Heatwave
        Coastal ≥37°C → Heatwave
        Hilly   ≥30°C → Heatwave
        any     ≥45°C → Severe Heatwave
  5. Write two outputs to the sibling SHRAM-verification-data repo:
        comparisons/aws_2pm_snapshot.csv      (one row per station per day)
        comparisons/aws_2pm_summary.csv       (one row per day, aggregates)
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
AWS_URL = ("https://reactjs.imd.gov.in/geoserver/wfs?service=WFS&version=1.1.0"
           "&request=GetFeature&typename=imd:aws_data_layer"
           "&srsname=EPSG:4326&outputFormat=application/json")
TYPED_STATIONS = DASHBOARD_ROOT / "reference_history" / "imd_station_heatwave_latest.json"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

IST = timezone(timedelta(hours=5, minutes=30))

# Thresholds (°C) applied to current AWS temp
HW_THRESHOLD = {"Plain": 40.0, "Coastal": 37.0, "Hilly": 30.0}
SEVERE_THRESHOLD = 45.0

# Freshness window: only count AWS stations whose update_time is today (IST)
# AND no older than this many hours from the snapshot run-time. Wider tolerance
# than the live snapshot since AWS reports can lag.
MAX_AGE_HOURS = 6


DAILY_FIELDS = [
    "snapshot_date_ist", "snapshot_time_ist",
    "station_id", "station_name", "call_sign", "lat", "lon",
    "station_type", "inherited_from_km",
    "aws_update_time_utc", "aws_age_hours",
    "temp_c", "rh_pct", "tmax_today_c", "tmin_today_c",
    "alert_status", "alert_label",
]

SUMMARY_FIELDS = [
    "snapshot_date_ist", "snapshot_time_ist",
    "n_aws_total", "n_aws_fresh",
    "n_heatwave", "n_severe", "n_normal",
    "n_plain", "n_coastal", "n_hilly",
    "max_temp_c", "median_temp_c",
]


def _fnum(v):
    try:
        if v is None or v == "" or v == "NULL":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def haversine_km(la1, lo1, la2, lo2):
    R = 6371.0
    r = math.radians
    dla, dlo = r(la2 - la1), r(lo2 - lo1)
    a = math.sin(dla / 2) ** 2 + math.cos(r(la1)) * math.cos(r(la2)) * math.sin(dlo / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "shram-verification-snapshot"})
    with urllib.request.urlopen(req, timeout=120, context=CTX) as r:
        return json.loads(r.read())


def parse_aws_time(s: str | None) -> datetime | None:
    if not s:
        return None
    # Format: "2026-05-19 17:29:04" — assumed UTC per IMD convention
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def classify(temp_c: float | None, station_type: str | None) -> tuple[int, str]:
    """Return (status, label). 0 = Normal, 1 = HW, 2 = Severe."""
    if temp_c is None or station_type not in HW_THRESHOLD:
        return 0, "Normal"
    if temp_c >= SEVERE_THRESHOLD:
        return 2, "Severe Heatwave"
    if temp_c >= HW_THRESHOLD[station_type]:
        return 1, "Heatwave"
    return 0, "Normal"


def build_typed_index() -> list[dict]:
    """Load the 285 classified heatwave stations and keep only those with a type."""
    with TYPED_STATIONS.open() as f:
        d = json.load(f)
    out = [
        {"lat": p["lat"], "lon": p["lon"], "type": p["station_type"]}
        for p in d.get("points", [])
        if p.get("station_type") in HW_THRESHOLD
    ]
    logger.info("typed stations loaded: %d", len(out))
    return out


def nearest_type(lat: float, lon: float, typed: list[dict]) -> tuple[str, float]:
    best_t, best_km = None, math.inf
    for t in typed:
        k = haversine_km(lat, lon, t["lat"], t["lon"])
        if k < best_km:
            best_t, best_km = t["type"], k
    return best_t, best_km


def append_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data-repo",
        default=str(DASHBOARD_ROOT.parent / "SHRAM-verification-data"),
        help="Path to the SHRAM-verification-data clone where CSVs are appended.",
    )
    args = ap.parse_args()
    data_repo = Path(args.data_repo)
    if not data_repo.exists():
        logger.error("data repo not found at %s", data_repo)
        return 1
    if not TYPED_STATIONS.exists():
        logger.error("typed stations file not found at %s", TYPED_STATIONS)
        return 1

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    snapshot_date = now_ist.date().isoformat()
    snapshot_time = now_ist.strftime("%H:%M:%S")
    logger.info("snapshot at %s IST (%s UTC)", now_ist.isoformat(), now_utc.isoformat())

    logger.info("fetching AWS layer")
    aws = fetch_json(AWS_URL)
    features = aws.get("features", [])
    logger.info("  %d AWS records returned", len(features))

    typed = build_typed_index()

    rows: list[dict] = []
    n_fresh = 0
    n_hw = n_sev = n_normal = 0
    n_plain = n_coastal = n_hilly = 0
    temps: list[float] = []

    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if len(coords) < 2:
            continue
        lon, lat = float(coords[0]), float(coords[1])

        aws_time = parse_aws_time(props.get("update_time"))
        if aws_time is None:
            continue
        age_hours = (now_utc - aws_time).total_seconds() / 3600.0
        # Only count stations that reported recently AND share today's IST date
        aws_ist_date = aws_time.astimezone(IST).date().isoformat()
        if age_hours > MAX_AGE_HOURS or aws_ist_date != snapshot_date:
            continue
        n_fresh += 1

        temp_c = _fnum(props.get("temp"))
        rh = _fnum(props.get("rh"))
        tmax = _fnum(props.get("temp_max"))
        tmin = _fnum(props.get("temp_min"))

        st_type, inherited_km = nearest_type(lat, lon, typed)
        if st_type == "Plain":
            n_plain += 1
        elif st_type == "Coastal":
            n_coastal += 1
        elif st_type == "Hilly":
            n_hilly += 1

        status, label = classify(temp_c, st_type)
        if status == 2:
            n_sev += 1
        elif status == 1:
            n_hw += 1
        else:
            n_normal += 1

        if temp_c is not None:
            temps.append(temp_c)

        rows.append({
            "snapshot_date_ist": snapshot_date,
            "snapshot_time_ist": snapshot_time,
            "station_id": props.get("station_id") or "",
            "station_name": props.get("station") or "",
            "call_sign": props.get("call_sign") or "",
            "lat": lat,
            "lon": lon,
            "station_type": st_type or "",
            "inherited_from_km": round(inherited_km, 1) if inherited_km is not math.inf else "",
            "aws_update_time_utc": props.get("update_time") or "",
            "aws_age_hours": round(age_hours, 2),
            "temp_c": temp_c,
            "rh_pct": rh,
            "tmax_today_c": tmax,
            "tmin_today_c": tmin,
            "alert_status": status,
            "alert_label": label,
        })

    logger.info("  fresh AWS stations: %d (within %dh of snapshot and same IST date)",
                n_fresh, MAX_AGE_HOURS)
    logger.info("  type inheritance: %d Plain, %d Coastal, %d Hilly",
                n_plain, n_coastal, n_hilly)
    logger.info("  alerts: %d Heatwave, %d Severe, %d Normal", n_hw, n_sev, n_normal)

    temps_sorted = sorted(temps)
    summary_row = {
        "snapshot_date_ist": snapshot_date,
        "snapshot_time_ist": snapshot_time,
        "n_aws_total": len(features),
        "n_aws_fresh": n_fresh,
        "n_heatwave": n_hw,
        "n_severe": n_sev,
        "n_normal": n_normal,
        "n_plain": n_plain,
        "n_coastal": n_coastal,
        "n_hilly": n_hilly,
        "max_temp_c": max(temps) if temps else "",
        "median_temp_c": (
            round(temps_sorted[len(temps_sorted) // 2], 2) if temps_sorted else ""
        ),
    }

    daily_csv = data_repo / "comparisons" / "aws_2pm_snapshot.csv"
    summary_csv = data_repo / "comparisons" / "aws_2pm_summary.csv"
    append_csv(daily_csv, DAILY_FIELDS, rows)
    append_csv(summary_csv, SUMMARY_FIELDS, [summary_row])
    logger.info("appended %d rows to %s", len(rows), daily_csv)
    logger.info("appended 1 summary row to %s", summary_csv)

    # Also publish a slim JSON the dashboard can consume directly without
    # parsing the big CSV. Same structure as imd_station_heatwave_latest.json
    # so the existing UI patterns work.
    out_json = DASHBOARD_ROOT / "reference_history" / "aws_2pm_snapshot_latest.json"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps({
        "label": "IMD AWS 14:30 IST snapshot",
        "source_url": AWS_URL,
        "fetched_at_utc": now_utc.isoformat(),
        "snapshot_date_ist": snapshot_date,
        "snapshot_time_ist": snapshot_time,
        "n_total": len(features),
        "n_fresh": n_fresh,
        "n_heatwave": n_hw,
        "n_severe": n_sev,
        "points": rows,
    }, indent=1))
    logger.info("wrote dashboard payload %s", out_json)
    return 0


if __name__ == "__main__":
    sys.exit(main())
