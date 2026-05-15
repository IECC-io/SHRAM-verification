"""Archive shram.info's 7-day forecast for historical comparison.

Pulls https://shram.info/weather_logs/forecast_7day.json and saves a
date-stamped copy under reference_history/shram_forecast/, so the dashboard
can later compute IMD-vs-SHRAM agreement κ as a time-series.

Run from the dashboard root:
    python3 scripts/archive_shram_forecast.py

Schema notes (relevant fields for κ analysis):
    metadata.generated_at_ist           — when SHRAM produced this forecast
    states.<state>.districts.<dist>.forecast[i].date          — date of day i
    states.<state>.districts.<dist>.forecast[i].hours[h].data.met<N>.<shade|sun>.zone
"""

from __future__ import annotations

import json
import logging
import sys
import urllib.request
import urllib3
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DASHBOARD_ROOT = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = DASHBOARD_ROOT / "reference_history" / "shram_forecast"
LATEST_PATH = DASHBOARD_ROOT / "reference_history" / "shram_forecast_latest.json"

FEED_URL = "https://shram.info/weather_logs/forecast_7day.json"
REQUEST_TIMEOUT = 90


def slim(payload: dict) -> dict:
    """Strip per-hour data, keep per-day MET zone peaks only. The full hourly
    file is ~10 MB; the slimmed per-day version is ~200 KB. Archived files use
    this slim form to keep the repo manageable."""
    slimmed_states = {}
    for state_key, state_block in (payload.get("states") or {}).items():
        slim_state = {}
        for kind in ("capital", "districts"):
            sub = state_block.get(kind)
            if not sub:
                continue
            if kind == "capital":
                slim_state["capital"] = _slim_point(sub)
            else:
                slim_state["districts"] = {k: _slim_point(v) for k, v in sub.items()}
        slimmed_states[state_key] = slim_state
    return {
        "label": "SHRAM 7-day forecast (slimmed: per-day MET zone peaks)",
        "source_url": FEED_URL,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "metadata": payload.get("metadata"),
        "states": slimmed_states,
    }


def _slim_point(pt: dict) -> dict:
    slim_forecast = []
    for day_block in pt.get("forecast") or []:
        # Per MET level and (shade/sun), compute the day's peak zone
        peaks = {}
        max_temp = -float("inf")
        max_rh = -float("inf")
        for h in day_block.get("hours") or []:
            data = h.get("data") or {}
            for met_key, met_block in data.items():
                if not met_key.startswith("met"):
                    continue
                for sun_key in ("shade", "sun"):
                    sb = met_block.get(sun_key) or {}
                    z = sb.get("zone")
                    if z is None:
                        continue
                    key = (met_key, sun_key)
                    if peaks.get(key, 0) < z:
                        peaks[key] = z
            t = h.get("temp_c")
            if t is not None and t > max_temp:
                max_temp = t
            r = h.get("humidity")
            if r is not None and r > max_rh:
                max_rh = r
        peak_by_met = {}
        for (mk, sk), v in peaks.items():
            peak_by_met.setdefault(mk, {})[sk] = v
        slim_forecast.append({
            "date": day_block.get("date"),
            "peak_zone_by_met": peak_by_met,
            "max_temp_c": max_temp if max_temp != -float("inf") else None,
            "max_humidity": max_rh if max_rh != -float("inf") else None,
        })
    return {
        "lat": pt.get("lat"),
        "lon": pt.get("lon"),
        "name": pt.get("name"),
        "forecast": slim_forecast,
    }


def main() -> int:
    logger.info("fetching %s", FEED_URL)
    req = urllib.request.Request(FEED_URL, headers={"User-Agent": "shram-verification"})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        payload = json.loads(resp.read())
    slimmed = slim(payload)

    n_states = len(slimmed["states"])
    n_districts = sum(len(s.get("districts") or {}) for s in slimmed["states"].values())
    logger.info("slimmed: %d states, %d districts", n_states, n_districts)

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_path = ARCHIVE_DIR / f"shram_forecast_{stamp}.json"
    with archive_path.open("w") as f:
        json.dump(slimmed, f, indent=2)
    logger.info("archived → %s", archive_path)

    with LATEST_PATH.open("w") as f:
        json.dump(slimmed, f, indent=2)
    logger.info("latest copy → %s", LATEST_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
