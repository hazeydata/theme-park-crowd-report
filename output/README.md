# Output Directory

**Note**: This directory is currently not used. The actual output goes to the configured output base directory.

## Actual Output Location

The pipeline writes output to the location specified by `--output-base` (default: Dropbox location).

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
│   └── dimparkhours.csv          # Park hours; src/get_park_hours_from_s3.py
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
    └── get_park_hours_*.log
```

## Why Not This Directory?

The output location is configurable and defaults to a Dropbox location for easy access and backup. This `output/` directory is kept for potential future use or if you want to use it as the output base.

## Using This Directory

If you want to use this directory as the output location:

```powershell
python src/get_tp_wait_time_data_from_s3.py --output-base "D:\GitHub\hazeydata\theme-park-crowd-report\output"
```

## Note

This directory is gitignored, so output files are not tracked in version control. This is intentional - data files can be large and should not be in the repository.
