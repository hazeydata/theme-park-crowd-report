# TODO / Pinned Reminders

## Queue-Times: Staging + Morning Merge (DONE)

**Implemented**: The queue-times scraper writes to **`staging/queue_times/YYYY-MM/{park}_{date}.csv`** only. Fact_tables stay **static for modelling**. The **morning ETL** (S3 run) merges **yesterday's** staging into `fact_tables/clean` at the start of each run, then deletes those staged files. The scraper runs continuously (`--interval`); staging is also available for **live use** (e.g. Twitch/YouTube).

---

## Queue-Times: Hours Filter, 5‑min Loop, ThemeParkQueueTimes_5min (DONE)

**Implemented**: [get_wait_times_from_queue_times.py](src/get_wait_times_from_queue_times.py) loads `dimparkhours` and only calls the API when a park is in-window (open−90 to close+90 in park TZ, 6am rule). [run_queue_times_loop.ps1](scripts/run_queue_times_loop.ps1) default interval 300s. [register_scheduled_tasks.ps1](scripts/register_scheduled_tasks.ps1) registers **ThemeParkQueueTimes_5min** at log on. Use `--no-hours-filter` to disable.

---

## Output Layout and Atomic Dimension Writes (DONE)

**Output layout**: One `output_base` from `config/config.json`; one `output_base/logs/`. See [src/utils/paths.py](src/utils/paths.py), [OUTPUT_LAYOUT_REVIEW.md](OUTPUT_LAYOUT_REVIEW.md). **Atomic dimension writes**: All six dimension scripts write to `{name}.csv.tmp` then `os.replace()` to target; unlink `.tmp` on error.

---

## Queue-Times: Unmapped Attractions (DONE)

**Implemented**: [scripts/report_queue_times_unmapped.py](scripts/report_queue_times_unmapped.py) fetches queue-times feed, left-joins to `config/queue_times_entity_mapping.csv`, and writes unattributed `(park_code, queue_times_id, queue_times_name, last_seen)` to `reports/queue_times_unmapped.csv` for review. `last_seen` is the report run date (YYYY-MM-DD).

---

## Queue-Times: Stale `observed_at` (DONE)

**Implemented**: `observed_at` is taken from queue-times `last_updated`; the API can return stale timestamps. [get_wait_times_from_queue_times.py](src/get_wait_times_from_queue_times.py) logs a warning when `observed_at` is more than 24h older than fetch time (`STALE_OBSERVED_AT_THRESHOLD_HOURS`), with up to 3 sample rows. Doc: [docs/STALE_OBSERVED_AT.md](docs/STALE_OBSERVED_AT.md).

---

## Fact Table Schema Doc (DONE)

**Implemented**: [docs/SCHEMA.md](docs/SCHEMA.md) defines fact table columns (`entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes`), `observed_at` semantics and 6am rule, `wait_time_type`/ranges, sources (S3, queue-times), and path layout. README links to it.

---

## Entity Metadata Index (DONE)

**Implemented**: Entity metadata index (`state/entity_index.sqlite`) tracks per-entity metadata (latest observation date, latest timestamp, row counts, last modeled timestamp) to enable efficient modeling workflows. Updates incrementally when ETL writes new CSVs. Provides functions to query entities needing re-modeling and load entity data selectively (only reads relevant park's CSVs). See [docs/ENTITY_INDEX.md](docs/ENTITY_INDEX.md). Scripts: [src/build_entity_index.py](src/build_entity_index.py) (build/rebuild from all CSVs), [scripts/inspect_entity_index.py](scripts/inspect_entity_index.py) (query and inspect).

---

## Feature Engineering Module (DONE)

**Implemented**: [src/processors/features.py](src/processors/features.py) adds modeling features to fact rows:
  - `pred_mins_since_6am`: Minutes since 6am from `observed_at` (6am rule: if hour < 6, add 1440)
  - `pred_dategroupid`: Join to `dimdategroupid` on `park_date`
  - `pred_season`, `pred_season_year`: Join to `dimseason` on `park_date`
  - `wgt_geo_decay`: Geometric decay weight `0.5^(days_since_observed / 730)` for training
  - `park_date`: Operational date (6am rule, uses timezone from `observed_at`)
  - `park_code`: Derived from `entity_code` prefix
  - `observed_wait_time`: Target variable (from `wait_time_minutes`)

**Usage**: `add_features(df, output_base)` takes fact rows and returns feature-rich DataFrame ready for encoding and modeling.

**Park Hours**: `add_park_hours` implemented with versioned table support (dimparkhours_with_donor.csv).

---

## Encoding Module (DONE)

**Implemented**: [src/processors/encoding.py](src/processors/encoding.py) converts categorical features to numeric format for ML models.

**Features**:
  - **Label Encoding** (default): Maps categories to integers (0, 1, 2, ...). Recommended for tree-based models (XGBoost, LightGBM).
  - **One-Hot Encoding**: Creates binary columns for each category. Useful for linear models or when categories have no ordinal meaning.
  - **Mappings Storage**: Saves encoding mappings to `state/encoding_mappings.json` for consistent encoding during inference.
  - **Unknown Handling**: Configurable handling of unknown values ("error", "ignore", or "encode").

**Categorical Features Encoded**:
  - `pred_dategroupid`: Date group ID
  - `pred_season`: Season name
  - `pred_season_year`: Season year
  - `park_code`: Park code
  - `entity_code`: Entity/attraction code

**Usage**:
  ```python
  from processors.encoding import encode_features
  
  # After adding features
  df_features = add_features(df, output_base)
  
  # Encode categorical features
  df_encoded, mappings = encode_features(
      df_features,
      output_base,
      strategy="label",  # or "one_hot"
  )
  ```

**Future**: Target encoding support (for mean-based encoding with regularization).

---

## Training Module (DONE)

**Implemented**: [src/processors/training.py](src/processors/training.py) trains XGBoost models to predict ACTUAL wait times.

**Features**:
  - **Two models**: 
    - **With-POSTED**: ACTUAL ~ POSTED + features (for backfill and live inference)
    - **Without-POSTED**: ACTUAL ~ features only (for forecast)
  - **Chronological split**: Train/val/test by park_date to avoid temporal leakage
  - **XGBoost**: Gradient boosted trees with early stopping
  - **Evaluation metrics**: MAE, RMSE, MAPE, R², correlation
  - **Model persistence**: Saves models and metadata to `models/{entity_code}/`
  - **Entity index integration**: Marks entities as modeled after training

**Usage**:
  ```python
  from processors.training import train_entity_model
  
  # After features and encoding
  models, metrics = train_entity_model(
      df_encoded,
      entity_code,
      output_base,
      train_ratio=0.7,
      val_ratio=0.15,
  )
  ```

**Training script**: [scripts/train_entity_model.py](scripts/train_entity_model.py)
  - Loads entity data
  - Adds features
  - Encodes categoricals
  - Trains both models
  - Marks entity as modeled

**Future**: Hyperparameter tuning, quantile regression, SHAP analysis.

---

## Predicted POSTED Module (DONE)

**Implemented**: [src/processors/posted_aggregates.py](src/processors/posted_aggregates.py) generates predicted POSTED from historical aggregates.

**Purpose**: Predicted POSTED enables:
  1. **Live comparison**: "We predicted POSTED = X, we observe POSTED = Y"
  2. **Building trust**: Show accuracy of predictions in real-time
  3. **Live streaming content**: Watch predictions perform in real-time

**Features**:
  - **Aggregation**: (entity_code, dategroupid, hour) → median POSTED
  - **Fallback strategy**: If exact match not found, tries:
    1. (entity, dategroupid) → median across hours
    2. (entity, hour) → median across dategroupids
    3. (entity) → median across all
    4. (park_code, hour) → park-level
  - **Storage**: Saves to `aggregates/posted_aggregates.parquet` for fast lookup
  - **Batch generation**: Get predicted POSTED for all hours of a day

**Usage**:
  ```python
  from processors.posted_aggregates import get_predicted_posted
  
  # Get predicted POSTED for a specific time
  predicted = get_predicted_posted(
      entity_code="MK101",
      park_date=date(2026, 6, 15),
      hour=14,  # 2 PM
      output_base=output_base,
  )
  ```

**Build script**: [scripts/build_posted_aggregates.py](scripts/build_posted_aggregates.py)
  - Scans all historical fact tables
  - Aggregates POSTED by (entity, dategroupid, hour)
  - Saves to Parquet for fast lookup

**Forecast script**: [scripts/generate_forecast.py](scripts/generate_forecast.py)
  - Generates predicted ACTUAL (from without-POSTED model) and predicted POSTED (from aggregates)
  - For future dates (tomorrow to +2 years)
  - At 5-minute resolution for all park operating hours
  - Output: `curves/forecast/{entity_code}_{park_date}.csv`

**Backfill script**: [scripts/generate_backfill.py](scripts/generate_backfill.py)
  - Generates historical ACTUAL curves using with-POSTED model
  - For past dates (specified date range)
  - Uses observed ACTUAL when available, otherwise imputed from POSTED
  - At 5-minute resolution for all park operating hours
  - Output: `curves/backfill/{entity_code}_{park_date}.csv` (columns: entity_code, park_date, time_slot, actual, source)

**WTI script**: [scripts/calculate_wti.py](scripts/calculate_wti.py)
  - Calculates Wait Time Index (WTI) for each (park, park_date, time_slot)
  - WTI = mean(actual) over all entities where actual is not null (closed)
  - Uses backfill curves (historical) and forecast curves (future)
  - Output: `wti/wti.parquet` and `wti/wti.csv` (columns: park_code, park_date, time_slot, wti, n_entities, min_actual, max_actual)

---

## Next Steps (from attraction-io alignment)

See [docs/ATTRACTION_IO_ALIGNMENT.md](docs/ATTRACTION_IO_ALIGNMENT.md) for the legacy pipeline summary and full mapping. For modeling, ACTUAL curves, forecast, live inference, and WTI: [docs/MODELING_AND_WTI_METHODOLOGY.md](docs/MODELING_AND_WTI_METHODOLOGY.md).

**Suggested order:**

1. **Entity-grouped fact** — Build `fact_tables/by_entity/{entity}.csv` (or Parquet) from park-date CSVs. Unblocks BI, `latest_obs_report`, and a single Parquet if we want to feed Julia.
2. **`latest_obs_report.csv`** — `entity_code`, `latest_observation_date` (max park_date per entity). Needed for attraction-io and for “who to model” logic.
3. **Python feature module** — `add_mins_since_6am`, `add_dategroupid`, `add_season`, `add_geometric_decay` (from `observed_at`/park_date + dims); then `add_park_hours` (needs dimparkhours → donor-style bridge).
4. **Parquet + S3 (optional)** — One `wait_time_fact_table.parquet` + `latest_obs_report.csv` to `s3://.../fact_tables/` if we keep running attraction-io’s Julia pipeline.

---

## Tomorrow: Verify wait_time_db_report.md auto-update

**Check**: After the scheduled task runs (ThemeParkWaitTimeReport_530am at 5:30 AM), verify that `reports/wait_time_db_report.md` was updated with today's date. The report should show the latest data including any new park-date CSVs from the 5am ETL run.

**If not updated**: Check Task Scheduler to see if the task ran successfully, check logs for errors.

---

*Add new items below as needed.*
