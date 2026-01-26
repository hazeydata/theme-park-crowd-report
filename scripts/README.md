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
- **ThemeParkDimensionFetch_6am** — Daily at 6:00 AM (entity, park hours, events, metatable from S3; build dimdategroupid, dimseason)
- **ThemeParkWaitTimeETL_7am** — Daily at 7:00 AM (wait-time ETL, backup)

See [README.md](../README.md#scheduling) for details.

### `run_dimension_fetches.ps1`

Runs the dimension-table fetch scripts and builds in sequence: `get_entity_table_from_s3.py`, `get_park_hours_from_s3.py`, `get_events_from_s3.py`, `get_metatable_from_s3.py`, `build_dimdategroupid.py`, `build_dimseason.py` (metatable from S3; dimdategroupid and dimseason built locally). Writes to the same **output_base** as the ETL (from `config/config.json` or default): `dimension_tables/` and `logs/` under that path. Invoked by **ThemeParkDimensionFetch_6am**; can also be run manually:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_dimension_fetches.ps1
```

Exits with an error if any script fails.

### Queue-Times.com fetcher and `run_queue_times_loop.ps1`

**`src/get_wait_times_from_queue_times.py`** fetches live wait times from the queue-times.com API, maps ride IDs to TouringPlans `entity_code` via `config/queue_times_entity_mapping.csv`, deduplicates with SQLite, and **writes to `staging/queue_times/YYYY-MM/{park}_{date}.csv`** (not `fact_tables`). The **morning ETL** (S3 run) merges **yesterday's** staging into `fact_tables/clean` at the start of each run. Staging is also used for live content (e.g. Twitch/YouTube).

- **One-shot** (single fetch and write to staging):
  ```powershell
  python src/get_wait_times_from_queue_times.py
  python src/get_wait_times_from_queue_times.py --output-base "D:\Path\output" --park-ids 6,5,7,8
  ```
- **Continuous (constantly runs)**: `--interval SECS` runs a loop: fetch → write to staging → sleep → repeat. Stop with Ctrl+C.
  ```powershell
  python src/get_wait_times_from_queue_times.py --interval 600
  ```

**`run_queue_times_loop.ps1`** is a thin wrapper that runs the fetcher with `--interval 300` (5 minutes) by default. It uses the same **output_base** as the rest of the pipeline (from `config/config.json` or default) unless you pass `-OutputBase`. You can run it in a console or register a Windows task “At log on” / “At startup” to keep it running across reboots. The fetcher uses **dimparkhours** (`opening_time` / `closing_time` when present) to only scrape parks in-window (open−90 to close+90 in park TZ); use `--no-hours-filter` to disable. The fetcher always bypasses HTTP/HTTPS proxy; run the loop from **PowerShell or Command Prompt** (not Cursor's terminal) if your IDE forces a local proxy.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1
powershell -ExecutionPolicy Bypass -File scripts/run_queue_times_loop.ps1 -IntervalSeconds 300 -OutputBase "D:\Path\output"
```

**Troubleshooting:** If the loop was stopped by closing the window, delete `state/processing_queue_times.lock` under your output base before starting again. Check `output_base/logs/get_tp_wait_time_data_*.log` for errors.

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

### `report_queue_times_unmapped.py`

Lists **queue-times.com attractions that have no row** in `config/queue_times_entity_mapping.csv`. Fetches parks and `queue_times` from the API, collects all rides in mapped parks, left-joins to the mapping, and writes unattributed `(park_code, queue_times_id, queue_times_name, last_seen)` to `reports/queue_times_unmapped.csv` for review. `last_seen` is the report run date (YYYY-MM-DD).

**Run** (requires network for queue-times.com API):

```powershell
python scripts/report_queue_times_unmapped.py
python scripts/report_queue_times_unmapped.py --output-base "D:\Path" --report reports/unmapped.csv
```

**Options**: `--output-base`, `--report` (default: `output_base/reports/queue_times_unmapped.csv`).

### `build_entity_index.py`

**Location**: `src/build_entity_index.py` (not in scripts/, but a utility script)

Builds or rebuilds the **entity metadata index** (`state/entity_index.sqlite`) by scanning all fact table CSVs in `fact_tables/clean/`. The index tracks per-entity metadata (latest observation date, latest timestamp, row counts) to enable efficient modeling workflows. Useful for initial index creation or rebuilding after corruption.

**Run**:

```powershell
# Build index from all CSVs
python src/build_entity_index.py

# Rebuild (delete existing and start fresh)
python src/build_entity_index.py --rebuild

# Custom output base
python src/build_entity_index.py --output-base "D:\Path"
```

**Note**: The index updates automatically during ETL runs, so a full rebuild is only needed for initial creation or recovery. See [docs/ENTITY_INDEX.md](../docs/ENTITY_INDEX.md).

### `inspect_entity_index.py`

Inspects the entity metadata index to show entities, their latest observations, and which ones need re-modeling.

**Run**:

```powershell
# Show all entities (first 20)
python scripts/inspect_entity_index.py

# Show entities needing modeling
python scripts/inspect_entity_index.py --needing-modeling

# Custom limit
python scripts/inspect_entity_index.py --limit 50
```

**Options**: `--output-base`, `--needing-modeling` (show only entities needing re-modeling), `--limit` (default: 20).

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
