# Entity Metadata Index

## Purpose

The **entity metadata index** is a SQLite database that tracks per-entity metadata to enable efficient modeling workflows:

- **Find entities with new observations** (need re-modeling) without scanning all fact CSVs
- **Load entity data selectively** (only read CSVs for the entity's park)
- **Avoid full scans** of fact tables when determining what to model

## Design Rationale

### Why an index instead of entity-grouped CSVs?

1. **No duplicate storage**: Keep park-date CSVs as the source of truth; index is small metadata only
2. **Incremental updates**: Index updates automatically when ETL writes new CSVs (no rebuild needed)
3. **Fast queries**: SQLite index is small and fast; can query "which entities have new data?" instantly
4. **Selective loading**: Know which park an entity belongs to (from `entity_code` prefix), so only scan that park's CSVs

### Why SQLite?

- **Small size**: Only metadata (entity_code, dates, counts) - not the full data
- **Fast queries**: Indexed columns for `latest_observed_at` and `last_modeled_at`
- **Incremental**: Updates happen during ETL writes (no separate rebuild step)
- **Self-contained**: Single file, easy to backup/restore

### Why track `last_modeled_at`?

Enables the modeling workflow:
1. Query index: `SELECT entity_code WHERE latest_observed_at > last_modeled_at`
2. For each entity, load data, add features, train model
3. Mark as modeled: `UPDATE entity_index SET last_modeled_at = now() WHERE entity_code = ?`

This avoids re-modeling entities that haven't changed.

## Schema

```sql
CREATE TABLE entity_index (
    entity_code TEXT PRIMARY KEY,
    latest_park_date TEXT NOT NULL,      -- YYYY-MM-DD, max date with observations
    latest_observed_at TEXT NOT NULL,   -- ISO 8601 timestamp, max observed_at
    row_count INTEGER DEFAULT 0,         -- Total rows for this entity (optional, for stats)
    last_modeled_at TEXT,                -- ISO 8601 timestamp, when we last ran modeling
    first_seen_at TEXT NOT NULL,        -- ISO 8601 timestamp, when first added to index
    updated_at TEXT NOT NULL             -- ISO 8601 timestamp, last update
);

CREATE INDEX idx_latest_observed_at ON entity_index(latest_observed_at);
CREATE INDEX idx_last_modeled_at ON entity_index(last_modeled_at);
```

## How It Works

### 1. Incremental Updates (During ETL)

When `write_grouped_csvs()` writes new fact CSVs:
- Aggregates per entity: `max(park_date)`, `max(observed_at)`, `count(*)`
- Updates index: inserts new entities or updates existing ones
- Happens automatically - no manual step needed

**Integration points:**
- `src/get_tp_wait_time_data_from_s3.py`: Updates index when writing S3 data
- `merge_yesterday_queue_times()`: Updates index when merging queue-times staging

### 2. Entity Loading (Selective CSV Reading)

Since each entity belongs to exactly one park (derived from `entity_code` prefix):
- Derive park: `MK101` → `mk`
- Scan only: `fact_tables/clean/YYYY-MM/mk_*.csv`
- Filter rows: `entity_code == "MK101"`
- Result: All observations for that entity, sorted by `observed_at`

**Why this is efficient:**
- Don't scan all parks' CSVs
- Don't load full CSVs into memory (can chunk if needed)
- Only read what's needed for modeling

### 3. Modeling Workflow

```python
from processors.entity_index import (
    get_entities_needing_modeling,
    load_entity_data,
    mark_entity_modeled,
)

# Find entities with new observations
entities = get_entities_needing_modeling(index_db_path, min_age_hours=24)

for entity_code, latest_observed_at, last_modeled_at in entities:
    # Load entity data (selective CSV reading)
    df = load_entity_data(entity_code, output_base, index_db_path)
    
    # Add features, train model, etc.
    # ...
    
    # Mark as modeled
    mark_entity_modeled(entity_code, index_db_path)
```

## Building/Rebuilding the Index

If the index gets out of sync or needs initial creation:

```bash
# Build from all existing CSVs
python src/build_entity_index.py

# Rebuild (delete existing and start fresh)
python src/build_entity_index.py --rebuild
```

This scans all CSVs in `fact_tables/clean/` and builds the index. After this, incremental updates take over.

## Location

- **Index DB**: `state/entity_index.sqlite` (under `output_base`)
- **Module**: `src/processors/entity_index.py`
- **Builder script**: `src/build_entity_index.py`

## Benefits

1. **No full scans**: Query index to find entities needing modeling
2. **Selective loading**: Only read CSVs for relevant parks
3. **Incremental**: Updates happen during ETL (no separate step)
4. **Small**: Index is metadata only, not duplicate data
5. **Fast**: SQLite with indexes on query columns
6. **Self-maintaining**: Updates automatically when data is written

## Future Optimizations

- **Date range hints**: Track date ranges per entity to skip irrelevant CSVs entirely
- **Parquet cache**: Cache frequently-modeled entities as Parquet for faster loading
- **Parallel loading**: Load multiple entities in parallel

## See Also

- [ATTRACTION_IO_ALIGNMENT.md](ATTRACTION_IO_ALIGNMENT.md): How this fits into the modeling workflow
- [TODO.md](../TODO.md): Next steps (feature module, modeling)
