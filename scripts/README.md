# Scripts Directory

Standalone utility scripts that can be run independently of the main pipeline.

## Current Scripts

### `register_scheduled_tasks.ps1`

Registers the **5 AM**, **6 AM**, and **7 AM Eastern** Windows scheduled tasks.

**Run once** (or after changing Python path / project root):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/register_scheduled_tasks.ps1
```

Creates:
- **ThemeParkWaitTimeETL_5am** — Daily at 5:00 AM (wait-time ETL, primary)
- **ThemeParkDimensionFetch_6am** — Daily at 6:00 AM (entity, park hours, events from S3)
- **ThemeParkWaitTimeETL_7am** — Daily at 7:00 AM (wait-time ETL, backup)

See [README.md](../README.md#scheduling) for details.

### `run_dimension_fetches.ps1`

Runs the three dimension-table fetch scripts in sequence: `get_entity_table_from_s3.py`, `get_park_hours_from_s3.py`, `get_events_from_s3.py`. Uses default `--output-base` (Dropbox). Invoked by **ThemeParkDimensionFetch_6am**; can also be run manually:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_dimension_fetches.ps1
```

Exits with an error if any script fails.

### `validate_wait_times.py`

Validates wait time fact table CSVs under `fact_tables/clean/`:

- **Schema**: Required columns (`entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes`), valid `wait_time_type` values.
- **Ranges**:
  - **POSTED / ACTUAL**: 0–1000 (invalid outside); **outlier** if ≥ 300.
  - **PRIORITY**: -100–2000 or 8888 (sold out); **outlier** if &lt; -100 or &gt; 2000 and ≠ 8888.

Writes a JSON report to `validation/validate_wait_times_YYYYMMDD_HHMMSS.json` under the output base. Exits **1** if any invalid rows; **0** otherwise. Outliers are flagged but do not fail the run.

**Run**:

```powershell
python scripts/validate_wait_times.py
python scripts/validate_wait_times.py --lookback-days 14
python scripts/validate_wait_times.py --all
python scripts/validate_wait_times.py --output-base "D:\Path" --report validation/custom.json
```

**Options**: `--output-base`, `--lookback-days` (default 7), `--all`, `--report`.

### `report_wait_time_db.py`

Produces an **easily consumable Markdown report** of what's in the wait time fact table. Scans `fact_tables/clean/YYYY-MM/{park}_{date}.csv` (same layout as ETL output) and writes a report with:

1. **Summary** — Date range, parks, park-day (file) count, total rows (or “—” with `--quick`)
2. **By park** — Per-park: file count, row count (or omitted with `--quick`), date range
3. **Recent coverage** — Grid: last N days × parks. Cells show ✓ (file exists) / — (no file) with `--quick`, or row counts otherwise.

Report path: `reports/wait_time_db_report.md` under the output base. **Overwritten each run** so you can always open the same file for daily or ad-hoc checks.

**When to use `--quick`**: Skip row counts; grid shows ✓/— only. Faster on slow or remote paths (e.g. Dropbox). Use for quick daily coverage checks.

**Run**:

```powershell
python scripts/report_wait_time_db.py
python scripts/report_wait_time_db.py --quick --lookback-days 7
python scripts/report_wait_time_db.py --output-base "D:\Path" --report reports/db_report.md
```

**Options**: `--output-base`, `--report`, `--lookback-days` (default 14), `--quick`.

## Potential Future Scripts

Examples of scripts that might go here:
- Data validation scripts
- One-off data migration scripts
- Reporting and analysis scripts
- Maintenance utilities

## Running Scripts

Scripts in this directory can be run directly:

```powershell
python scripts/script_name.py
```

Or with the virtual environment activated:

```powershell
.\venv\Scripts\Activate.ps1
python scripts/script_name.py
```

## Note

Scripts here should be standalone and not depend on the main pipeline structure. They can import from `src/` if needed, but should be runnable independently.
