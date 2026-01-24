# Output Directory

**Note**: The **6 AM dimension fetch** (`run_dimension_fetches.ps1`) writes to this `output/` folder: `output/dimension_tables/`, `output/logs/`. The wait-time ETL uses `--output-base` (default: Dropbox) unless overridden.

## Actual Output Location

The pipeline writes output to the location specified by `--output-base`. Dimension fetches use **project root `output/`**; the ETL defaults to a Dropbox location.

The output structure is:
```
output_base/
├── fact_tables/
│   └── clean/
│       └── YYYY-MM/              # Organized by year-month
│           ├── mk_2024-01-15.csv  # One file per park per date
│           └── epcot_2024-01-16.csv
├── dimension_tables/
│   ├── dimentity.csv             # Entity table; src/get_entity_table_from_s3.py
│   ├── dimparkhours.csv          # Park hours; src/get_park_hours_from_s3.py
│   ├── dimeventdays.csv          # Events by day; src/get_events_from_s3.py
│   ├── dimevents.csv             # Event lookup; src/get_events_from_s3.py
│   ├── dimmetatable.csv          # Park-day metadata (EMH, parades, closures); src/get_metatable_from_s3.py
│   ├── dimdategroupid.csv        # Date + holidays + date_group_id; src/build_dimdategroupid.py
│   └── dimseason.csv             # Season + season_year; src/build_dimseason.py
├── samples/
│   └── YYYY-MM/
│       └── wait_time_fact_table_sample.csv
├── state/
│   ├── dedupe.sqlite
│   ├── processed_files.json
│   ├── failed_files.json
│   └── processing.lock
├── validation/
│   └── validate_wait_times_*.json   # From scripts/validate_wait_times.py
├── reports/
│   └── wait_time_db_report.md       # Wait time DB summary; scripts/report_wait_time_db.py
└── logs/
    ├── get_tp_wait_time_data_*.log
    ├── get_entity_table_*.log
    ├── get_park_hours_*.log
    ├── get_events_*.log
    ├── get_metatable_*.log
    ├── build_dimdategroupid_*.log
    └── build_dimseason_*.log
```

## Why This Directory?

The **6 AM dimension fetch** uses `output/` as its output base so dimension tables and logs live under `output/dimension_tables/` and `output/logs/`. The wait-time ETL defaults to Dropbox; use `--output-base` to point it at `output/` instead.

## Using This Directory

If you want to use this directory as the output location:

```powershell
python src/get_tp_wait_time_data_from_s3.py --output-base "D:\GitHub\hazeydata\theme-park-crowd-report\output"
```

## Note

This directory is gitignored, so output files are not tracked in version control. This is intentional - data files can be large and should not be in the repository.
