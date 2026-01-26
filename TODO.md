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

## Next Steps (from attraction-io alignment)

See [docs/ATTRACTION_IO_ALIGNMENT.md](docs/ATTRACTION_IO_ALIGNMENT.md) for the legacy pipeline summary and full mapping. For modeling, ACTUAL curves, forecast, live inference, and WTI: [docs/MODELING_AND_WTI_METHODOLOGY.md](docs/MODELING_AND_WTI_METHODOLOGY.md).

**Suggested order:**

1. **Entity-grouped fact** — Build `fact_tables/by_entity/{entity}.csv` (or Parquet) from park-date CSVs. Unblocks BI, `latest_obs_report`, and a single Parquet if we want to feed Julia.
2. **`latest_obs_report.csv`** — `entity_code`, `latest_observation_date` (max park_date per entity). Needed for attraction-io and for “who to model” logic.
3. **Python feature module** — `add_mins_since_6am`, `add_dategroupid`, `add_season`, `add_geometric_decay` (from `observed_at`/park_date + dims); then `add_park_hours` (needs dimparkhours → donor-style bridge).
4. **Parquet + S3 (optional)** — One `wait_time_fact_table.parquet` + `latest_obs_report.csv` to `s3://.../fact_tables/` if we keep running attraction-io’s Julia pipeline.

---

*Add new items below as needed.*
