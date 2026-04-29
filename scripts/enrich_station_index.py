"""Add lat/lon to shram_stations_history/by_station/index.json by joining against
the IMD station cache.

The slicer leaves out coordinates because shram's weekly CSVs don't carry them.
The IMD cache (cache_imd_stations.py output) does. Match by station name.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT / "shram_stations_history" / "by_station" / "index.json"
IMD_CACHE_PATH = ROOT / "shram_stations_history" / "imd_stations.json"


def norm(s):
    return "".join(c.lower() for c in (s or "") if c.isalnum())


def main():
    if not INDEX_PATH.exists():
        logger.error("index missing: %s", INDEX_PATH)
        return 1
    if not IMD_CACHE_PATH.exists():
        logger.error("imd cache missing: %s — run cache_imd_stations.py first", IMD_CACHE_PATH)
        return 1

    idx = json.loads(INDEX_PATH.read_text())
    imd = json.loads(IMD_CACHE_PATH.read_text())

    by_station: dict[str, dict] = {}
    by_state_district: dict[tuple, list[dict]] = {}
    for s in imd.get("stations", []):
        key = norm(s.get("station"))
        by_station[key] = s
        sd = (norm(s.get("state")), norm(s.get("district")))
        by_state_district.setdefault(sd, []).append(s)

    n_matched = 0
    n_unmatched = 0
    for st in idx.get("stations", []):
        # First try exact station-name match
        m = by_station.get(norm(st.get("station")))
        # Fall back to (state, district) — pick first match
        if not m:
            cands = by_state_district.get((norm(st.get("state")), norm(st.get("district"))), [])
            m = cands[0] if cands else None
        if m:
            st["lat"] = m.get("lat")
            st["lon"] = m.get("lon")
            n_matched += 1
        else:
            st["lat"] = None
            st["lon"] = None
            n_unmatched += 1

    INDEX_PATH.write_text(json.dumps(idx, indent=2))
    logger.info("enriched %d / %d stations with lat/lon (unmatched: %d)",
                n_matched, n_matched + n_unmatched, n_unmatched)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
