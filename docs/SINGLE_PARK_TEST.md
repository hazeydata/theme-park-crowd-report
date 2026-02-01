# Single-Park Pipeline Test

During development, running the full pipeline for all parks can take many hours (especially training). You can limit the run to **one park** so the pipeline completes in a reasonable time and you can verify the full flow end-to-end.

## Usage

```bash
# Full pipeline for one park only (training, forecast, WTI)
./scripts/run_daily_pipeline.sh --park MK

# Or run steps individually with --park
python scripts/train_batch_entities.py --park MK --workers 2 --output-base "$output_base"
python scripts/generate_forecast.py --park MK --output-base "$output_base"
python scripts/calculate_wti.py --park MK --output-base "$output_base"
```

**Park code** must match the entity code prefix (e.g. `MK` for Magic Kingdom, `EP` for EPCOT, `AK` for Animal Kingdom, `HS` for Hollywood Studios). Use 2-letter codes; for Tokyo use `TDL` or `TDS`. **TL** (Typhoon Lagoon) and **BB** (Blizzard Beach) are water parks and are out of scope for single-park testing—ignore them when choosing a park.

## Which park has the fewest entities?

Entity count per park depends on your entity index (built from your data). To list entity counts by park, **excluding water parks (TL, BB)**:

```bash
# Replace with your output_base path, e.g. from config/config.json
output_base="/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report"

# 2-letter park prefix; exclude water parks TL and BB
sqlite3 "$output_base/state/entity_index.sqlite" \
  "SELECT substr(entity_code,1,2) AS park, count(*) AS n FROM entity_index
   WHERE substr(entity_code,1,2) NOT IN ('TL','BB') GROUP BY 1 ORDER BY n;"

# If you have Tokyo (TDL, TDS), count by 3-char prefix for those: substr(entity_code,1,3) IN ('TDL','TDS')
```

Pick the park with the smallest `n` from the list (MK, EP, HS, AK, DL, CA, etc.; Tokyo TDL/TDS if present).

## What still runs for all data

- **ETL**, **dimension fetches**, **posted aggregates**, and **wait time DB report** are **not** limited by `--park`; they run over all input data as usual. Only **training**, **forecast**, and **WTI** are restricted to the given park when you pass `--park`.

## Restarting Dropbox after the run

If the pipeline stopped Dropbox (because `output_base` is under Dropbox), start it again manually when the run is done, e.g.:

```bash
dropbox start
# or from the desktop / menu
```
