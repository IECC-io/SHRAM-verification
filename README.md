# SHRAM Verification Dashboard

Browser-based dashboard for comparing SHRAM (heat-stress alerting tool) outputs against
external reference sources, including weather forecasts and India Meteorological Department
(IMD) ground-truth observations and official heatwave declarations.

## Tabs

1. **Lookup** — Point-in-time snapshot comparison: what shram.info reported vs. what other
   reference sources said at the same hour and place.
2. **Source map** — Map of where SHRAM grid points sit vs. where IMD AWS stations
   physically exist; shows sensor density.
3. **Difference map** — Side-by-side maps: source A, source B, and a nearest-neighbor
   difference layer (within 25 km).
4. **R² analysis** — Scatter plot + regression statistics (R², RMSE, MAE, bias) between
   any two sources, computed on nearest-neighbor pairs.
5. **SHRAM vs IMD heatwave** — Confusion matrix and per-station drill-down comparing
   IMD's official heatwave declarations against SHRAM's EHI-N* zone classifications.

## Data sources

| Source | What it is | Update cadence |
|---|---|---|
| `shram_map` | SHRAM live grid output (Open-Meteo best_match at 0.25° cells) | Hourly |
| `imd_aws` | IMD Automatic Weather Station observations | ~5 min (cached daily) |
| `imd_heatwave` | IMD official heatwave forecast feed (GeoJSON) | Twice daily |
| `open_meteo*` | Open-Meteo current weather (best_match + per-model variants) | Per snapshot |
| `nasa_power` | NASA POWER hourly reanalysis | ~3 day lag |

## Data attribution

- **IMD heatwave forecast feed**: India Meteorological Department, Ministry of Earth
  Sciences. Source: <https://dss.imd.gov.in/dwr_img/GIS/heatwave.html>.
- **IMD AWS observations**: India Meteorological Department.
- **ERA5**: Hersbach et al., ECMWF.
- **Open-Meteo**: <https://open-meteo.com>.
- **NASA POWER**: NASA Langley Research Center.

## Scripts

Located under `scripts/`. Each is idempotent and writes its output to a known path the
dashboard reads at runtime.

- `fetch_sources.py` — Pulls Ta/RH for the 12 monitored cities from every reference source
  and writes the latest snapshot.
- `fetch_openmeteo_at_imd.py` — Queries Open-Meteo at the exact lat/lon of every IMD AWS
  station (~751 stations, ~8 batched API calls) and writes
  `reference_history/openmeteo_at_imd_stations.json`. Powers the R² analysis tab.
- `fetch_imd_heatwave.py` — Pulls IMD's official `HW_Status_Forecast.json` feed, archives
  a date-stamped copy, and writes `reference_history/imd_heatwave_latest.json`. Powers the
  SHRAM vs IMD heatwave tab.

## Running locally

```bash
# 1. Install dependencies (only needed if you want to run the fetchers)
pip install -r requirements.txt

# 2. Refresh the data the dashboard reads
python3 scripts/fetch_imd_heatwave.py
python3 scripts/fetch_openmeteo_at_imd.py

# 3. Serve the dashboard (file:// won't work because of CORS on local fetches)
python3 -m http.server 8765
# Then open http://localhost:8765/
```

## Historical archives

Long-running historical archives (`reference_history/by_district/`,
`shram_map_history/`, `shram_stations_history/by_station/`, etc.) are kept in the
sibling repo [SHRAM-verification-data](https://github.com/IECC-io/SHRAM-verification-data)
to keep this repo small. The dashboard reads only the small "latest" files committed here.
