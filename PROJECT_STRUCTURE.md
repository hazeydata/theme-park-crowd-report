# Project Structure

This document describes the folder structure and organization of the Theme Park Crowd Report project.

## Directory Structure

```
theme-park-crowd-report/
├── src/                              # Main source code package
│   ├── __init__.py
│   ├── get_tp_wait_time_data_from_s3.py   # Main ETL (S3 wait times)
│   ├── get_wait_times_from_queue_times.py # Live queue-times.com fetcher → staging
│   ├── get_entity_table_from_s3.py
│   ├── get_park_hours_from_s3.py
│   ├── get_events_from_s3.py
│   ├── get_metatable_from_s3.py
│   ├── build_dimdategroupid.py
│   ├── build_dimseason.py
│   ├── build_entity_index.py         # Entity index (state/entity_index.sqlite)
│   ├── build_park_hours_donor.py
│   ├── clean_*.py                    # Dimension cleaners
│   ├── migrate_park_hours_to_versioned.py
│   ├── inspect_dimension_tables.py
│   ├── parsers/
│   │   └── wait_time_parsers.py      # Standby and fastpass parsers
│   ├── processors/                  # Modeling, aggregates, features, entity index
│   │   ├── encoding.py
│   │   ├── entity_index.py
│   │   ├── features.py
│   │   ├── training.py
│   │   ├── posted_aggregates.py
│   │   └── park_hours_versioning.py
│   └── utils/
│       ├── file_identification.py
│       ├── paths.py                 # get_output_base from config
│       ├── pipeline_status.py       # pipeline_status.json helpers
│       └── entity_names.py
│
├── scripts/                          # Entrypoints and automation
│   ├── README.md
│   ├── run_daily_pipeline.sh         # Master: ETL → dimensions → aggregates → report → training → forecast → WTI
│   ├── run_etl.sh, run_dimension_fetches.sh, run_queue_times_loop.sh
│   ├── install_cron.sh, install_queue_times_service.sh
│   ├── queue-times-loop.service
│   ├── build_posted_aggregates.py, train_batch_entities.py, train_entity_model.py
│   ├── generate_forecast.py, generate_backfill.py, calculate_wti.py
│   ├── report_wait_time_db.py, validate_wait_times.py, check_prerequisites.py
│   ├── update_pipeline_status.py
│   └── ... (see scripts/README.md)
│
├── dashboard/                       # Pipeline status dashboard (Dash)
│   ├── app.py
│   └── README.md
│
├── config/
│   ├── README.md
│   ├── config.example.json          # Template (Windows/generic)
│   └── config.linux.example.json    # Template for Linux
│
├── docs/                            # Schema, methodology, setup
│   ├── SCHEMA.md, SCHEMAS.md
│   ├── PIPELINE_STATE.md            # Current setup (paths, cron, queue-times)
│   ├── ENTITY_INDEX.md, MODELING_AND_WTI_METHODOLOGY.md
│   ├── LINUX_SETUP.md, REFRESH_READINESS.md
│   └── ... (see README.md “See Also”)
│
├── tests/
│   ├── __init__.py
│   └── test_entity_index.py
│
├── temp/                            # Temporary files (gitignored)
│   └── README.md
│
├── web/                             # Static site (Figma → HTML)
│   ├── index.html, styles.css
│   └── FIGMA_TO_HTML.md, DEPLOY.md
│
├── julia/                           # Optional Julia app (separate)
│   └── README.md
│
├── requirements.txt
├── README.md
├── PROJECT_STRUCTURE.md
├── CHANGES.md
├── LINUX_CRON_SETUP.md
├── CONNECTION_ERROR_FIX.md
├── CONCURRENT_EXECUTION_FIX.md
├── OUTPUT_LAYOUT_REVIEW.md
└── REFACTORING_NOTES.md
```

## Directory Purposes

### `src/` - Main Source Code

Contains all application logic organized into modules:

- **`get_tp_wait_time_data_from_s3.py`**: Main ETL script that orchestrates the entire pipeline
  - Lists S3 files (standby + fastpass)
  - Classifies file types (Standby, New Fastpass, Old Fastpass)
  - Filters to new/changed only (incremental); skips old repeatedly-failed files
  - Routes to appropriate parsers
  - Deduplicates via SQLite; writes CSV per (park, date)
  - Uses process lock to prevent concurrent runs

- **`get_entity_table_from_s3.py`**: Fetches entity dimension data from S3
  - Downloads `current_*_entities.csv` from `export/entities/` (dlr, tdr, uor, ush, wdw)
  - Combines with union of columns; normalizes `land` column
  - Writes `dimension_tables/dimentity.csv` under output base

- **`get_park_hours_from_s3.py`**: Fetches park-hours dimension data from S3
  - Downloads `{prop}_park_hours.csv` from `export/park_hours/` (dlr, tdr, uor, ush, wdw)
  - Combines with union of columns; writes `dimension_tables/dimparkhours.csv` under output base

- **`get_events_from_s3.py`**: Fetches events dimension data from S3
  - Downloads `current_event_days.csv` and `current_events.csv` from `export/events/`
  - Writes `dimension_tables/dimeventdays.csv` (events by day) and `dimension_tables/dimevents.csv` (event lookup)

- **`get_metatable_from_s3.py`**: Fetches metatable (park-day metadata: EMH, parades, closures) from S3
  - Downloads `current_metatable.csv` from `export/metatable/`
  - Writes `dimension_tables/dimmetatable.csv` under output base

- **`build_dimdategroupid.py`**: Builds date-group dimension table locally (no S3)
  - Date spine 2005-01-01 through today + 2 years; "today" = Eastern park_day (6 AM rule)
  - Holiday codes/names (Easter, MLK, Thanksgiving, NJC, PMP/PMM, etc.) and `date_group_id`
  - Writes `dimension_tables/dimdategroupid.csv` under output base. Adapted from legacy Julia dimDate, dimHolidays, dimDateGroupID.

- **`build_dimseason.py`**: Builds season dimension table from dimdategroupid (no S3)
  - Reads `dimension_tables/dimdategroupid.csv`; assigns `season` and `season_year` from `date_group_id` patterns (CHRISTMAS_PEAK, holiday carry, Presidents+Mardi Gras combined window, seasonal buckets)
  - Writes `dimension_tables/dimseason.csv` under output base. Depends on dimdategroupid. Adapted from legacy Julia `run_dimSeason.jl`.
  
- **`parsers/wait_time_parsers.py`**: Modular parsers for different data formats
  - `parse_standby_chunk()`: Parses standby wait time data
  - `parse_fastpass_stream()`: Parses fastpass/priority data (handles old and new formats)
  - Ported from proven Julia codebase for accuracy
  
- **`get_wait_times_from_queue_times.py`**: Fetches live wait times from queue-times.com API; writes to `staging/queue_times/`; morning ETL merges into fact_tables.

- **`parsers/wait_time_parsers.py`**: Standby and fastpass parsers (ported from Julia).

- **`processors/`**: Modeling and data prep — `entity_index.py` (state/entity_index.sqlite), `training.py` (XGBoost/mean models), `posted_aggregates.py` (hourly POSTED aggregates), `features.py`, `encoding.py`, `park_hours_versioning.py`.

- **`utils/file_identification.py`**: File type classifier. **`utils/paths.py`**: `get_output_base()` from config. **`utils/pipeline_status.py`**: Helpers for `state/pipeline_status.json`.

**Why modular**: Separates concerns, makes testing easier, allows updating parsers and processors independently.

### `scripts/` - Entrypoints and Automation

Pipeline runners, reports, and scheduling: `run_daily_pipeline.sh` (master), `run_etl.sh`, `run_dimension_fetches.sh`, `run_queue_times_loop.sh`, `install_cron.sh`, `install_queue_times_service.sh`, `build_posted_aggregates.py`, `train_batch_entities.py`, `generate_forecast.py`, `calculate_wti.py`, `report_wait_time_db.py`, `validate_wait_times.py`, and others. See [scripts/README.md](scripts/README.md).

### `dashboard/` - Pipeline Status Dashboard

Single-page Dash app: pipeline step status, queue-times job, entities table. Refreshes every 5 minutes. See [dashboard/README.md](dashboard/README.md).

### `config/` - Configuration

- **`config.example.json`** / **`config.linux.example.json`**: Templates; copy to `config.json` (gitignored) and set `output_base`. All pipeline scripts use this for data and logs.
- **`queue_times_entity_mapping.csv`**: Maps queue-times.com ride IDs to TouringPlans `entity_code` (if present).

### `tests/` - Test Suite

Unit and integration tests (e.g. `test_entity_index.py`). Run from project root.

### `temp/` - Temporary Files

Temporary files directory (gitignored). Can be cleaned up between runs.

### `output/` - Optional Dev Output

**Note**: Production runs use a single **output_base** from `config/config.json` (typically Dropbox). The 5am/7am ETL, 6am dimension fetch, and queue-times fetcher all write to that output_base. The `output/` folder here is for **local dev only** (e.g. `--output-base=./output`). It is gitignored.

The output structure under an output_base is:
```
output_base/
├── fact_tables/clean/YYYY-MM/    # CSV files by park and date
├── dimension_tables/             # dimentity, dimparkhours, dimdategroupid, dimseason, etc.
├── staging/queue_times/          # Queue-times fetcher staging (merged by morning ETL)
├── state/                        # dedupe.sqlite, entity_index.sqlite, pipeline_status.json, lock files
├── aggregates/                   # posted_aggregates.parquet
├── models/                       # Per-entity XGBoost/mean models
├── curves/forecast/              # Forecast curves
├── reports/                      # wait_time_db_report.md, etc.
├── validation/                   # validate_wait_times.py JSON reports
└── logs/                         # All pipeline logs
```

**Why separate from code**: Keeps data separate from source code; one output_base gives one `logs/` and one set of dimension_tables.

### `docs/` - Schema and contract docs

- **SCHEMA.md**, **SCHEMAS.md**: Fact table columns, dimension schemas, column naming.
- **PIPELINE_STATE.md**: Current setup (paths, cron, queue-times, dashboard).
- **ENTITY_INDEX.md**, **MODELING_AND_WTI_METHODOLOGY.md**: Entity index and modeling.
- **LINUX_SETUP.md**, **REFRESH_READINESS.md**: Linux setup and refresh order.
- Others: ATTRACTION_IO_ALIGNMENT, LEGACY_PIPELINE_CRITICAL_REVIEW, PARK_HOURS_VERSIONING, STALE_OBSERVED_AT, etc. See [README.md](README.md) “See Also”.

## File Organization Principles

### Why This Structure?

1. **Separation of Concerns**: Code, data, config, and logs are separated
2. **Modularity**: Parsers and utilities are in separate modules
3. **Scalability**: Easy to add new parsers or utilities
4. **Maintainability**: Clear organization makes it easy to find and update code
5. **Git Hygiene**: Data and logs are gitignored, only code is tracked

### Import Structure

The main script imports from submodules:
```python
from parsers import parse_standby_chunk, parse_fastpass_stream
from utils import get_wait_time_filetype
```

**Why**: Keeps imports clean and makes dependencies explicit.

## Usage Notes

- `temp/` and `output/` (optional dev output) are gitignored; production data and logs live under **output_base** from config.
- The `src/` directory is a Python package — run scripts from project root so imports resolve.
- Current “where we are” (Linux, cron, queue-times, dashboard): [docs/PIPELINE_STATE.md](docs/PIPELINE_STATE.md).
