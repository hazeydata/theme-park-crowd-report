# Project Structure

This document describes the folder structure and organization of the Theme Park Crowd Report project.

## Directory Structure

```
theme-park-crowd-report/
├── src/                              # Main source code package
│   ├── __init__.py                   # Package marker
│   ├── get_tp_wait_time_data_from_s3.py  # Main ETL script
│   ├── get_entity_table_from_s3.py   # Entity dimension table from S3
│   ├── get_park_hours_from_s3.py     # Park hours dimension table from S3
│   ├── get_events_from_s3.py         # Events dimension tables from S3
│   ├── get_metatable_from_s3.py      # Metatable (park-day metadata) from S3
│   ├── build_dimdategroupid.py       # Date + holidays + date_group_id (built locally)
│   ├── build_dimseason.py            # Season + season_year from dimdategroupid (built locally)
│   ├── parsers/                      # Data parsers for different formats
│   │   ├── __init__.py
│   │   └── wait_time_parsers.py      # Standby and fastpass parsers
│   ├── processors/                   # Data processing modules (reserved for future use)
│   │   └── __init__.py
│   └── utils/                         # Utility functions
│       ├── __init__.py
│       └── file_identification.py    # File type classifier
│
├── scripts/                           # Standalone executable scripts (currently empty)
│   └── README.md
│
├── tests/                             # Test suite (currently empty)
│   └── __init__.py
│
├── config/                            # Configuration files
│   ├── README.md
│   ├── config.example.json           # Example configuration template
│   └── README.md
│
├── data/                              # Data directories (gitignored)
│   ├── README.md
│   ├── raw/                          # Raw data files (not currently used)
│   └── processed/                    # Processed data files (not currently used)
│
├── work/                              # Working directory for intermediate files (gitignored)
│   └── README.md
│
├── temp/                              # Temporary files directory (gitignored)
│   └── README.md
│
├── output/                            # Final output files directory (gitignored)
│   └── README.md
│
├── logs/                              # Log files directory (gitignored)
│   └── README.md
│
├── venv/                              # Python virtual environment (gitignored)
├── requirements.txt                  # Python package dependencies
├── .gitignore                        # Git ignore rules
├── README.md                         # Main project documentation
├── PROJECT_STRUCTURE.md              # This file
├── CHANGES.md                        # Change log
├── CONNECTION_ERROR_FIX.md           # Connection error handling
└── CONCURRENT_EXECUTION_FIX.md       # Process lock and concurrent run prevention
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
  
- **`utils/file_identification.py`**: File type classifier
  - `get_wait_time_filetype()`: Determines if file is Standby, New Fastpass, Old Fastpass, or Unknown
  - Uses filename patterns to classify (ported from Julia)

**Why modular**: Separates concerns, makes testing easier, allows updating parsers independently.

### `scripts/` - Standalone Scripts

Reserved for standalone executable scripts that can be run independently. Currently empty but available for future utility scripts.

**Why separate**: Keeps main application code separate from utility scripts.

### `tests/` - Test Suite

Reserved for unit tests, integration tests, and test fixtures. Currently empty but structure is in place for future testing.

**Why**: Testing ensures code quality and prevents regressions.

### `config/` - Configuration Files

Contains configuration templates and examples:

- **`config.example.json`**: Example configuration file (if needed in future)
- Configuration is currently handled via command-line arguments

**Why**: Centralizes configuration management.

### `data/` - Data Storage

Directories for raw and processed data files. Currently not used (data is streamed directly from S3), but available for future use.

- **`raw/`**: Would store raw data files if downloaded locally
- **`processed/`**: Would store intermediate processed files if needed

**Why**: Provides structure for future data storage needs.

### `work/` - Working Directory

Temporary working files during processing. Can be cleaned up between runs.

**Why**: Keeps temporary files organized and separate from output.

### `temp/` - Temporary Files

Temporary files directory. Can be cleaned up between runs.

**Why**: Provides a dedicated space for truly temporary files.

### `output/` - Output Directory

**Note**: The **6 AM dimension fetch** (`run_dimension_fetches.ps1`) writes to `output/`: `output/dimension_tables/`, `output/logs/`. The wait-time ETL uses a configurable output base (default: Dropbox) unless `--output-base` points here.

The output structure under an output base (e.g. `output/` or Dropbox) is:
```
output_base/
├── fact_tables/clean/YYYY-MM/    # CSV files by park and date
├── dimension_tables/             # dimentity, dimparkhours, dimeventdays, dimevents, dimmetatable, dimdategroupid, dimseason
├── samples/YYYY-MM/              # Sample CSV files
├── state/                        # dedupe.sqlite, processed_files.json, failed_files.json, processing.lock
└── logs/                         # wait-time ETL, entity, park hours, events, metatable, build_dimdategroupid, build_dimseason
```

**Why separate from code**: Keeps data separate from source code, makes it easier to manage large datasets.

### `logs/` - Application Logs

Processing logs, error logs, and debug information. Each run creates a timestamped log file.

**Why**: Essential for monitoring scheduled jobs and debugging issues.

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

- All data directories (`data/`, `work/`, `temp/`, `output/`, `logs/`) are gitignored
- README files in these directories are tracked to document their purpose
- The `src/` directory is a Python package - modules can be imported
- Scripts should be run from the project root directory
