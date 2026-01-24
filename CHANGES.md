# Change Log

This document tracks significant changes to the Theme Park Wait Time Data Pipeline.

## Recent Changes

### Script Documentation and Wrap-Up

**What changed**:
- Main ETL script (`get_tp_wait_time_data_from_s3.py`) fully documented with module docstring, section headers, and step-by-step comments (STEP 1–12 in `main()`)
- README, CHANGES, PROJECT_STRUCTURE, output/logs READMEs, and CONNECTION_ERROR_FIX updated for `failed_files.json`, `processing.lock`, and skip–old–repeatedly-failed behavior
- No temporary scripts present; `scripts/`, `temp/`, `work/` contain only READMEs

**Why**: Easier for new readers to understand each step; docs stay in sync with current behavior.

### Modular Refactoring (Current Version)

**What Changed**:
- Refactored into modular structure with separate parsers
- Ported proven Julia parsing logic to Python
- Added file type classification
- Changed output from single Parquet file to individual CSV files per park/date
- Added connection error retry logic

**Why**:
- Modular parsers are easier to test and maintain
- Julia logic was proven to work correctly
- CSV files per park/date are easier to work with than one large file
- Retry logic handles network issues automatically

**Key Files**:
- `src/parsers/wait_time_parsers.py`: New modular parsers
- `src/utils/file_identification.py`: File type classifier
- `src/get_tp_wait_time_data_from_s3.py`: Refactored main script

### Output Structure Change

**Before**: Single Parquet file per month
```
fact_tables/YYYY-MM/wait_time_fact_table.parquet
```

**After**: Individual CSV files per park and date
```
fact_tables/clean/YYYY-MM/mk_2024-01-15.csv
fact_tables/clean/YYYY-MM/epcot_2024-01-15.csv
```

**Why**: 
- CSV files are easier to inspect and work with
- One file per park/date makes it easy to work with specific data
- Appending to existing files handles multiple S3 files for same park/date

### Parser Improvements

**Old Fastpass Parser**:
- Fixed date parsing (was producing incorrect years like 2813)
- Now correctly reads headerless format (matches Julia: `header=false, skipto=2`)
- Handles sold-out detection correctly (FWINHR >= 8000 → 8888 minutes)

**New Fastpass Parser**:
- Added sold-out handling (was filtering them out, now keeps with 8888 minutes)
- Matches Julia logic exactly

**Standby Parser**:
- Verified to match Julia logic
- Correctly filters rows where both posted and actual are missing

**Why**: Ensures data accuracy and matches proven Julia implementation.

### Connection Error Handling

**Added**:
- Automatic retry logic with exponential backoff
- S3 client configured with adaptive retry mode
- Longer timeouts for large files
- Failed files are not marked as processed (will retry on next run)

**Why**: Network connections can be unstable. Retries handle transient errors automatically.

### Skip Old Repeatedly-Failed Files

**Added**:
- `state/failed_files.json` tracks files that fail (parse errors, connection errors, etc.)
- If a file has failed ≥3 times **and** its S3 last-modified is older than 600 days, we skip it on future runs
- Successfully processing a file clears its failure tracking

**Why**: Some files (e.g. 2014 Old Fastpass) cannot be parsed successfully. Retrying them every run wastes time. Old, repeatedly-failed files are skipped instead.

**Tunables**: `FAILED_SKIP_THRESHOLD` (default 3), `OLD_FILE_DAYS` (default 600) in `get_tp_wait_time_data_from_s3.py`.

## Previous Versions

### Incremental Processing (Earlier Version)

**What Changed**:
- Added incremental processing (only processes new files)
- Added persistent deduplication database
- Added processed files tracking
- Added file-based logging

**Why**:
- Much faster for daily runs
- Deduplication works across multiple runs
- Better monitoring and debugging

## Migration Notes

### From Old Version

If you have existing output from an older version:

1. **First run**: Use `--full-rebuild` to process everything with new logic
2. **Output location**: Defaults to Dropbox location, or specify with `--output-base`
3. **File format**: Old Parquet files are replaced with CSV files
4. **Structure**: New structure is `fact_tables/clean/YYYY-MM/{park}_{date}.csv`

### Reprocessing Files

- **Single file**: Remove from `state/processed_files.json` and run normally
- **All files**: Use `--full-rebuild` flag
- **Specific properties**: Use `--props wdw,dlr` to process only certain properties

## Performance Improvements

- **Chunked processing**: Processes files in chunks (250k rows default) to manage memory
- **Incremental runs**: Only processes new files, much faster for daily runs
- **SQLite deduplication**: Fast lookups, persistent across runs
- **Connection retries**: Handles transient network errors automatically

## Known Issues and Solutions

### Old Repeatedly-Failed Files Skipped

**Behavior**: Files that fail ≥3 times and are older than 600 days are skipped on future runs.

**To retry**: Remove the file key from `state/failed_files.json` and run normally.

### Connection Errors

**Issue**: Connection errors when reading large files from S3

**Solution**: Automatic retry logic with exponential backoff. Files that fail will be retried on next run.

### Large Dedupe Database

**Issue**: SQLite database grows over time

**Solution**: Database should remain manageable. If needed, delete `state/dedupe.sqlite` and run with `--full-rebuild` (note: this will allow duplicates until database is rebuilt).

## Future Improvements

Potential enhancements for future versions:
- Unit tests for parsers
- Data validation and quality checks
- Performance monitoring and metrics
- Support for additional data sources
