# Theme Park Wait Time Data Pipeline

A Python ETL pipeline that processes theme park wait time data from AWS S3, transforming it into clean CSV fact tables organized by park and date.

## What This Does

This pipeline:
1. **Reads wait time data** from S3 (standby wait times and fastpass/priority data)
2. **Classifies file types** automatically (Standby, New Fastpass, Old Fastpass)
3. **Parses the data** using modular parsers (ported from proven Julia code)
4. **Deduplicates** rows using a persistent SQLite database
5. **Derives park codes and dates** from the data
6. **Writes clean CSV files** organized by park and date: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv`

**Fact table schema** (columns, `observed_at`, `wait_time_type`, sources): [docs/SCHEMA.md](docs/SCHEMA.md).  
**Legacy pipeline (attraction-io) alignment** and next steps: [docs/ATTRACTION_IO_ALIGNMENT.md](docs/ATTRACTION_IO_ALIGNMENT.md).  
**Critical review** of the legacy pipeline (efficiency, workflow, quality) and how we improve: [docs/LEGACY_PIPELINE_CRITICAL_REVIEW.md](docs/LEGACY_PIPELINE_CRITICAL_REVIEW.md).  
**Modeling and WTI methodology** (ACTUAL curves, forecast, live inference, Wait Time Index): [docs/MODELING_AND_WTI_METHODOLOGY.md](docs/MODELING_AND_WTI_METHODOLOGY.md).

## Project Structure

```
theme-park-crowd-report/
├── src/                          # Main source code
│   ├── get_tp_wait_time_data_from_s3.py  # Main ETL script
│   ├── parsers/                  # Modular data parsers
│   │   └── wait_time_parsers.py  # Standby and fastpass parsers
│   └── utils/                     # Utility functions
│       ├── file_identification.py # File type classifier
│       └── paths.py               # get_output_base (config/config.json)
├── config/                        # config.example.json; copy to config.json for output_base
├── output/                        # Optional dev output (gitignored); production uses output_base
└── ...                           # fact_tables, dimension_tables, state, logs live under output_base
```

See [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for detailed folder organization.

## Setup

### 1. Create and Activate Virtual Environment

We use a virtual environment to isolate Python dependencies:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**Why**: Keeps project dependencies separate from system Python, prevents conflicts.

### 2. Install Dependencies

```powershell
pip install -r requirements.txt
```

**Required packages**:
- `boto3`: AWS S3 access
- `pandas`: Data processing
- `pydantic`: Data validation (if needed)

### 3. (Optional) Output base

Copy `config/config.example.json` to `config/config.json` and set `output_base` to your pipeline output folder (e.g. your Dropbox path). If you skip this, the default path is used. See [config/README.md](config/README.md).

### 4. Configure AWS Credentials

The script needs AWS credentials to access S3. Configure one of these:

- **AWS CLI**: Run `aws configure`
- **Environment variables**: Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- **IAM role**: If running on EC2

**Why**: S3 requires authentication to read files.

## Usage

### Daily Scheduled Run (Recommended)

Runs incrementally, only processing new files since last run:

```powershell
python src/get_tp_wait_time_data_from_s3.py
```

**What happens**:
- Checks which files have already been processed (stored in `state/processed_files.json`)
- Only processes new files from S3
- Appends to existing CSV files if they already exist
- Updates the processed files list

**Why incremental**: Much faster for daily runs - only processes new data instead of everything.

### Full Rebuild

Processes all files from scratch, ignoring what's been processed before:

```powershell
python src/get_tp_wait_time_data_from_s3.py --full-rebuild
```

**When to use**:
- First time setup
- After fixing bugs in parsing logic
- When you need to reprocess everything

**Why**: Sometimes you need to rebuild everything with updated logic.

### Custom Options

```powershell
# Process specific properties only
python src/get_tp_wait_time_data_from_s3.py --props wdw,dlr

# Use a different output directory
python src/get_tp_wait_time_data_from_s3.py --output-base "D:\Custom\Path"

# Adjust chunk size (for memory management)
python src/get_tp_wait_time_data_from_s3.py --chunksize 500000
```

### Entity table (dimEntity)

Fetches entity dimension data from S3 and writes a single master table:

```powershell
python src/get_entity_table_from_s3.py
python src/get_entity_table_from_s3.py --output-base "D:\Custom\Path"
```

**What it does**: Downloads `current_*_entities.csv` from `s3://touringplans_stats/export/entities/` (properties: dlr, tdr, uor, ush, wdw — includes Epic Universe / EU via uor), combines them with a union of columns, normalizes the `land` column, and writes `dimension_tables/dimentity.csv` under the output base. Uses the same S3 bucket and AWS credentials as the wait-time ETL. Adapted from legacy Julia `run_dimEntity.jl`.

### Queue-Times.com (live wait times)

Fetches **live** wait times from the queue-times.com API. The scraper writes to **`staging/queue_times/YYYY-MM/{park}_{date}.csv`** only (not `fact_tables`), so fact_tables stay **static for modelling**. The **morning ETL** (S3 run) merges **yesterday's** staging into `fact_tables/clean` at the start of each run, then deletes those staged files. Staging is also available for **live use** (e.g. Twitch/YouTube). Uses `config/queue_times_entity_mapping.csv`; deduplicates via SQLite.

- **One-shot**: `python src/get_wait_times_from_queue_times.py`
- **Continuous (constantly runs)**: `--interval SECS` loops fetch → write to staging → sleep:
  ```powershell
  python src/get_wait_times_from_queue_times.py --interval 300
  ```
  Or: `powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1` (default 5 min). Stop with Ctrl+C. For 24/7, register a Windows task “At log on” that runs `run_queue_times_loop.ps1`. Run from **PowerShell or Command Prompt** (not Cursor’s terminal) if your IDE forces a local proxy. If the loop was stopped by closing the window, delete `state/processing_queue_times.lock` under your output base before starting again.

### Park hours (dimParkHours)

Fetches park-hours dimension data from S3 and writes a single master table:

```powershell
python src/get_park_hours_from_s3.py
python src/get_park_hours_from_s3.py --output-base "D:\Custom\Path"
```

**What it does**: Downloads `{prop}_park_hours.csv` from `s3://touringplans_stats/export/park_hours/` (dlr, tdr, uor, ush, wdw), combines them with a union of columns, and writes `dimension_tables/dimparkhours.csv` under the output base. Same S3 bucket and AWS credentials as the wait-time ETL.

### Events (dimEventDays, dimEvents)

Fetches events dimension data from S3 and writes two tables:

```powershell
python src/get_events_from_s3.py
python src/get_events_from_s3.py --output-base "D:\Custom\Path"
```

**What it does**: Downloads `current_event_days.csv` (events by day using event codes) and `current_events.csv` (lookup table for event codes) from `s3://touringplans_stats/export/events/`, and writes `dimension_tables/dimeventdays.csv` and `dimension_tables/dimevents.csv` under the output base. Same S3 bucket and AWS credentials as the wait-time ETL. Other files in export/events/ (e.g. event_days_*.csv, events_*.csv) are ignored.

### Date group ID (dimDateGroupID)

Builds the date-group dimension table locally (no S3):

```powershell
python src/build_dimdategroupid.py
python src/build_dimdategroupid.py --output-base "D:\Custom\Path"
```

**What it does**: Builds a combined dimension table with date spine (2005-01-01 through today + 2 years), holiday codes/names (Easter, MLK, Thanksgiving, etc.), and `date_group_id` for modeling and cohort analysis. Uses "today" = Eastern park_day (6 AM rule). Writes `dimension_tables/dimdategroupid.csv` under the output base. Adapted from legacy Julia `run_dimDate.jl`, `run_dimHolidays.jl`, `run_dimDateGroupID.jl`; always overwrites.

### Metatable (dimMetatable)

Fetches park-day metadata (extra magic hours, parades, closures, etc.) from S3:

```powershell
python src/get_metatable_from_s3.py
python src/get_metatable_from_s3.py --output-base "D:\Custom\Path"
```

**What it does**: Downloads `current_metatable.csv` from `s3://touringplans_stats/export/metatable/` and writes `dimension_tables/dimmetatable.csv` under the output base. No transformation. Adapted from legacy Julia `run_dimMetatable.jl`.

### Season (dimSeason)

Builds season labels from dimdategroupid (holidays, carry, Presidents+Mardi Gras, seasonal buckets):

```powershell
python src/build_dimseason.py
python src/build_dimseason.py --output-base "D:\Custom\Path"
```

**What it does**: Reads `dimension_tables/dimdategroupid.csv`, assigns `season` and `season_year` from `date_group_id` patterns (CHRISTMAS_PEAK, holiday carry, Presidents+Mardi Gras combined window, AFTER_EASTER/BEFORE_EASTER, SPRING/SUMMER/AUTUMN/WINTER). Writes `dimension_tables/dimseason.csv`. Depends on dimdategroupid; run `build_dimdategroupid` first. Adapted from legacy Julia `run_dimSeason.jl`; always overwrites.

### Data Cleaning

All dimension tables are cleaned and standardized for consistency:

```powershell
# Clean all dimension tables
python src/clean_all_dimensions.py

# Or clean individually
python src/clean_dimentity.py
python src/clean_dimparkhours.py
python src/clean_dimeventdays.py
python src/clean_dimevents.py
python src/clean_dimmetatable.py
```

**What cleaning does:**
- **Standardizes column names** (e.g., `code` → `entity_code`, `date` → `park_date`, `park` → `park_code`)
- **Applies defaults** (`opened_on` = park opening date if blank, `extinct_on` = 2099-01-01 if blank)
- **Converts data types** (booleans, dates formatted as YYYY-MM-DD)
- **Trims strings** and converts empty strings to NULL
- **Validates ranges** (numeric columns)

**Inspection tool:**
```powershell
python src/inspect_dimension_tables.py
```

Shows column names, types, null counts, and sample values for all dimension tables.

**Documentation:**
- **Schema definitions**: [docs/SCHEMAS.md](docs/SCHEMAS.md)
- **Column naming standard**: [docs/COLUMN_NAMING_STANDARD.md](docs/COLUMN_NAMING_STANDARD.md)
- **Cleaning plan**: [docs/DATA_CLEANING_PLAN.md](docs/DATA_CLEANING_PLAN.md)

## Output Structure

The pipeline writes to a single **output_base** (from `config/config.json` or the default path). The script creates organized CSV files under that directory:

```
output_base/
├── fact_tables/
│   └── clean/
│       └── YYYY-MM/                    # Organized by year-month (S3 ETL + yesterday's queue-times merged at morning run)
│           ├── mk_2024-01-15.csv       # One file per park per day
│           ├── epcot_2024-01-15.csv
│           └── hs_2024-01-16.csv
├── staging/
│   └── queue_times/                    # Queue-times scraper output (merged into fact_tables/clean by morning ETL; also for live use)
│       └── YYYY-MM/
│           └── {park}_{date}.csv
├── dimension_tables/
│   ├── dimentity.csv                   # Entity table; src/get_entity_table_from_s3.py
│   ├── dimparkhours.csv                # Park hours; src/get_park_hours_from_s3.py
│   ├── dimeventdays.csv                # Events by day; src/get_events_from_s3.py
│   ├── dimevents.csv                   # Event lookup; src/get_events_from_s3.py
│   ├── dimmetatable.csv                # Park-day metadata (EMH, parades, closures); src/get_metatable_from_s3.py
│   ├── dimdategroupid.csv              # Date + holidays + date_group_id; src/build_dimdategroupid.py
│   └── dimseason.csv                   # Season + season_year; src/build_dimseason.py
├── samples/
│   └── YYYY-MM/
│       └── wait_time_fact_table_sample.csv  # Random sample for testing
├── state/
│   ├── dedupe.sqlite                   # Persistent deduplication database
│   ├── processed_files.json            # Tracks which S3 files have been processed
│   ├── failed_files.json               # Tracks failed files (skip old + repeatedly-failed)
│   └── processing.lock                 # Prevents multiple simultaneous runs
└── logs/
    ├── get_tp_wait_time_data_*.log
    ├── get_entity_table_*.log
    ├── get_park_hours_*.log
    ├── get_events_*.log
    ├── get_metatable_*.log
    ├── build_dimdategroupid_*.log
    └── build_dimseason_*.log
```

### CSV File Format

Each CSV file contains 4 columns:
- `entity_code`: Attraction/entity identifier (e.g., "MK101", "EP09")
- `observed_at`: ISO timestamp with timezone offset (e.g., "2024-01-15T10:30:00-05:00")
- `wait_time_type`: One of "POSTED", "ACTUAL", or "PRIORITY"
- `wait_time_minutes`: Wait time in minutes (8888 for PRIORITY sellout)

**Why this structure**: 
- One file per park per day makes it easy to work with specific dates/parks
- Organized by year-month keeps directories manageable
- CSV format is widely compatible and easy to inspect

## How It Works

### 1. File Discovery

The script lists all CSV files in S3 for the specified properties:
- Standby files: `export/wait_times/{prop}/`
- Fastpass files: `export/fastpass_times/{prop}/`

**Why**: We need to know what files exist before processing them.

### 2. File Type Classification

Each file is classified using the filename:
- **Standby**: Contains "wait_times" in the path
- **New Fastpass**: Contains "fastpass_times" and doesn't match old patterns
- **Old Fastpass**: Contains "fastpass_times" and matches old date patterns (2012-2019)

**Why**: Different file formats need different parsers. Classification ensures we use the right parser.

### 3. Parsing

Each file type has its own parser (in `src/parsers/wait_time_parsers.py`):

- **Standby parser**: 
  - Reads POSTED and ACTUAL wait times
  - Filters out rows where both are missing
  - Splits into separate rows (one for POSTED, one for ACTUAL)
  
- **Fastpass parser**:
  - Handles both old and new formats automatically
  - Detects sold-out (FWINHR >= 8000) and sets wait_time_minutes = 8888
  - Calculates wait time as difference between return time and observed time

**Why modular parsers**: Easier to test, maintain, and update individual parsers without affecting others.

### 4. Deduplication

Uses a SQLite database with a primary key on `(entity_code, observed_at, wait_time_type, wait_time_minutes)`.

**Why SQLite**: 
- Fast lookups for duplicate detection
- Persistent across runs (deduplication works across multiple days)
- Simple, no external dependencies

### 5. Park and Date Derivation

- **Park code**: Extracted from entity_code prefix (e.g., "MK101" → "mk")
- **Park date**: Derived from observed_at using 6 AM rule (if hour < 6, it's previous day's operations)

**Why 6 AM rule**: Theme parks consider operations to start at 6 AM. Times before 6 AM belong to the previous operational day.

### 6. CSV Writing

Groups data by (park, park_date) and writes to separate CSV files:
- Path: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv`
- Appends if file exists (for multiple S3 files with same park/date)
- Sorts by observed_at before writing

**Why grouping**: Makes it easy to work with data for specific parks and dates.

## Error Handling

### Connection Errors

The script automatically retries on connection errors:
- **Retry attempts**: 3 attempts per file
- **Backoff**: Waits 1s, 2s, 4s between retries (exponential backoff)
- **S3 client**: Configured with adaptive retry mode and longer timeouts

**Why retries**: Network connections can be unstable. Retries handle transient errors automatically.

### Failed Files

Files that fail (parse errors, connection errors, etc.):
- Are logged with error details
- Are NOT marked as processed
- Are recorded in `state/failed_files.json` (failure count, last attempt)
- Will be retried on the next run **unless** they are old and have failed ≥3 times

**Skip old repeatedly-failed**: If a file has failed ≥3 times and its S3 LastModified is older than 600 days, we skip it on future runs. Tunables: `FAILED_SKIP_THRESHOLD`, `OLD_FILE_DAYS` in the main script.

**Why**: Ensures no data is lost for transient failures. Old, unparseable files (e.g. some 2014 fastpass) are skipped instead of retried forever.

## Scheduling

### Configured Tasks (5 AM, 6 AM, 7 AM Eastern)

Three Windows scheduled tasks run **daily**:

| Task | Time | Purpose |
|------|------|---------|
| **ThemeParkWaitTimeETL_5am** | 5:00 AM Eastern | Primary wait-time ETL run |
| **ThemeParkDimensionFetch_6am** | 6:00 AM Eastern | Fetches entity, park hours, events, metatable from S3; builds dimdategroupid, dimseason → dimension_tables |
| **ThemeParkWaitTimeETL_7am** | 7:00 AM Eastern | Backup ETL (e.g. if 5 AM didn’t run or S3 updates were late) |

The **process lock** (`state/processing.lock`) ensures the 7 AM ETL run does not overlap the 5 AM run. The 6 AM dimension fetch runs `scripts/run_dimension_fetches.ps1`, which invokes `get_entity_table_from_s3.py`, `get_park_hours_from_s3.py`, `get_events_from_s3.py`, `get_metatable_from_s3.py`, `build_dimdategroupid.py`, and `build_dimseason.py` in sequence. It writes to the same **output_base** as the ETL (from `config/config.json` or default): `dimension_tables/` and `logs/` under that path. It does not use the lock.

**Register the tasks** (run once, or after changes):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/register_scheduled_tasks.ps1
```

The script uses `C:\Python314\python.exe` and project root `d:\GitHub\hazeydata\theme-park-crowd-report`. Edit `scripts/register_scheduled_tasks.ps1` if your Python path or project root differ.

**View or edit**: Open **Task Scheduler** (`taskschd.msc`) → Task Scheduler Library → `ThemeParkWaitTimeETL_5am` / `ThemeParkDimensionFetch_6am` / `ThemeParkWaitTimeETL_7am`.

**Time zone**: Tasks use the **system local time**. Set Windows to Eastern Time so 5:00, 6:00, and 7:00 AM are Eastern.

### Manual Setup (Windows Task Scheduler)

1. Open Task Scheduler → Create Basic Task
2. Trigger: Daily at desired time (e.g. 5:00 AM)
3. Action: Start a program
   - Program: `C:\Python314\python.exe` (or your `python.exe` path)
   - Arguments: `src/get_tp_wait_time_data_from_s3.py`
   - Start in: `d:\GitHub\hazeydata\theme-park-crowd-report`

## Monitoring

### Log Files

Check **`output_base/logs/`** for detailed processing logs (output_base from `config/config.json` or the default path):
- Each run creates a new log file with timestamp
- Logs include: files processed, rows written, errors encountered
- Useful for debugging and monitoring

### State Files

- **`state/processed_files.json`**: Lists all successfully processed S3 files (key → last_modified)
- **`state/failed_files.json`**: Tracks files that failed (parse/connection errors). Old, repeatedly-failed files are skipped.
- **`state/dedupe.sqlite`**: Deduplication database (grows over time)
- **`state/processing.lock`**: Prevents multiple instances from running at once

**Why track state**: Enables incremental processing, prevents duplicate work, and avoids retrying files that cannot be parsed.

### Validation

Run `scripts/validate_wait_times.py` to check fact table CSVs for schema issues, invalid `wait_time_minutes` ranges, and outliers (e.g. POSTED/ACTUAL ≥ 300). Reports are written to `validation/` under the output base. **Exit 1** if any invalid rows; **0** otherwise. See [scripts/README.md](scripts/README.md#validate_wait_timespy) for options.

### Wait Time DB Report

Run `scripts/report_wait_time_db.py` for an **easily consumable Markdown report** of what's in the fact table:

- **Summary**: date range, parks, park-day count, total rows
- **By park**: file count, row count, date range per park
- **Recent coverage**: grid of last N days × parks (✓/— or row counts)

Report path: `reports/wait_time_db_report.md` under the output base (overwritten each run). Use `--quick` to skip row counts for faster daily checks on slow paths. See [scripts/README.md](scripts/README.md#report_wait_time_dbpy).

## Troubleshooting

### Script Fails Mid-Run

**Solution**: Just run it again. Files that succeeded are already tracked, so it will resume from where it left off.

**Why this works**: The processed files list is saved after each successful file.

### Need to Reprocess a File

**Solution**: Use `--full-rebuild` to reprocess everything, or manually remove the file from `state/processed_files.json`.

### Connection Errors Persist

**Solution**: 
- Check network connectivity
- Verify AWS credentials
- Check S3 bucket permissions
- Files will be retried automatically on next run

### Dedupe Database Gets Large

**Solution**: The database will grow over time but should remain manageable. If needed, delete `state/dedupe.sqlite` and run with `--full-rebuild` (note: this will allow duplicates until the database is rebuilt).

## Data Schema

### Output CSV Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `entity_code` | string | Attraction identifier (uppercase) | "MK101", "EP09" |
| `observed_at` | string | ISO timestamp with timezone | "2024-01-15T10:30:00-05:00" |
| `wait_time_type` | string | Type of wait time | "POSTED", "ACTUAL", "PRIORITY" |
| `wait_time_minutes` | int64 | Wait time in minutes | 30, 45, 8888 (soldout) |

### Wait Time Types

- **POSTED**: Posted wait time (what the sign says)
- **ACTUAL**: Actual wait time (from user reports)
- **PRIORITY**: Fastpass/Genie+ return window wait time

### Special Values

- **8888 minutes**: Indicates PRIORITY is sold out (no return window available)

## Performance

- **First run**: Processes all files (may take hours depending on data volume)
- **Daily runs**: Only processes new files (typically much faster)
- **Memory**: Uses chunked processing (250k rows default) to manage memory
- **Deduplication**: SQLite database grows over time but remains efficient

## See Also

- [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md): Detailed folder organization
- [CHANGES.md](CHANGES.md): Change log and migration notes
- [CONNECTION_ERROR_FIX.md](CONNECTION_ERROR_FIX.md): Connection error handling and retries
- [CONCURRENT_EXECUTION_FIX.md](CONCURRENT_EXECUTION_FIX.md): Process lock and concurrent run prevention
