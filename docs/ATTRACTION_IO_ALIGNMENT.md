# Attraction-IO Legacy Pipeline Alignment

This document summarizes the **attraction-io** pipeline ([disneystatswhiz/attraction-io](https://github.com/disneystatswhiz/attraction-io)), maps it to **theme-park-crowd-report**, and suggests goals and next steps.

---

## 1. What We Can See in Attraction-IO

### Repo and branches

- **main**: README describes `fact_table_sync`; `src/fact_table/` is **not** present on `main`.
- **fact_table_sync**: Used on EC2. **Fact input** comes from S3 rather than local Python:
  - `run_raw_wait_sync.jl` downloads:
    - `s3://touringplans_stats/stats_work/fact_tables/wait_time_fact_table.parquet` → `input/wait_times/wait_time_fact_table.parquet`
    - `s3://touringplans_stats/stats_work/fact_tables/latest_obs_report.csv` → `input/wait_times/latest_obs_report.csv`
  - `DATA_FACT = Parquet.read_parquet(...)` is the in-memory fact table.
- The README’s **Step 0** (Python `report.py`, `update.py`, `latest.py` in `src/fact_table/`) that *produces* the Parquet and `latest_obs_report.csv` is **not** in the public repo (likely another repo, or `fact_table_sync` expects them to be built and uploaded elsewhere).

### Pipeline flow (fact_table_sync)

| Step | What | Where |
|------|------|-------|
| **Setup** | `main_setup.jl` | dim (dimDate, dimEntity, dimParkHours, dimEvents, dimHolidays, dimMetatable, dimDateGroupID, dimSeason), donor (donorParkHours), **run_raw_wait_sync.jl** (sync Parquet + latest_obs_report from S3, load `DATA_FACT`) |
| **Jobs** | `run_jobs.jl` | Cleans input/output/temp/work, runs `main_setup`, reads `latest_obs_report.csv` (`entity_code`, `latest_observation_date`), selects entities with `latest_observation_date` in last 2 days, loops `run_entity(code; data_fact=DATA_FACT)` |
| **Per entity** | `main_runner.jl` | run_set_attraction → run_sync → **run_wait_time_ingestion** (filter `DATA_FACT` to entity, `wait_time_minutes` → `observed_wait_time`, write `work/{ENTITY}/wait_times/wait_times.csv`) → run_futuredates → **run_features** → run_premodelling → run_encodefeatures → run_trainer → run_predictions → calendar (dailyavgs, thresholds, levels, observed_dailyavgs) → reporting |

### Fact table schema (what Julia expects)

From `run_wait_time_ingestion.jl` and `run_features.jl`:

- **Required**: `entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes` (renamed to `observed_wait_time`).
- **Derived in features**: `park_day_id` (Date, 6am rule: if hour &lt; 6 → previous calendar day), `park_code`, `property_code`.
- **Joins**: `park_day_id` + `park_code` → donor park hours; `park_day_id` → dimdategroupid (`date_group_id` → `pred_dategroupid`); `park_day_id` → dimseason (`pred_season`, `pred_season_year`).

### Feature engineering (`run_features.jl` + `features.jl`)

| Feature | Source | Notes |
|---------|--------|-------|
| `pred_mins_since_6am` | `add_mins_since_6am(observed_at)` | `(hour−6)*60+min`; if &lt; 0, add 1440. |
| `pred_mins_since_park_open` | `add_park_hours` | `(observed_at - opening_time)` in minutes; needs `donorparkhours` (park_day_id, park_code) with `opening_time`, `closing_time`, `emh_*`. |
| `pred_park_open_hour`, `pred_park_close_hour`, `pred_park_hours_open`, `pred_emh_morning`, `pred_emh_evening` | `add_park_hours` | From donor park hours join. |
| `pred_dategroupid` | `add_dategroupid` | Join to `dimdategroupid` on `park_day_id`. |
| `pred_season`, `pred_season_year` | `add_season` | Join to `dimseason` on `park_day_id`. |
| `wgt_geo_decay` | `add_geometric_decay` | `0.5^(days_since_observed / 730)`. |
| **Target** | | `observed_wait_time` (from `wait_time_minutes`). |

### `latest_obs_report.csv`

- **Columns**: `entity_code`, `latest_observation_date` (Date).
- **Role**: `run_jobs.jl` selects entities whose `latest_observation_date` is within the last 2 days to decide which entities to run.

### Attraction-IO TODO.md (from main)

- Imputation for POSTED, ACTUAL, PRIORITY; refactor for new wait-time types.
- Edge cases: late/missing/empty inputs; MK44 8–11pm averages; avoid full re-runs on same input.
- Optional: alerts, summary reports, dashboard.

---

## 2. Theme-Park-Crowd-Report vs Attraction-IO

### Fact table

| | theme-park-crowd-report | attraction-io |
|--|--------------------------|---------------|
| **Layout** | `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv` (one file per park-date) | One `wait_time_fact_table.parquet` (all parks/dates) |
| **Columns** | `entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes` | Same 4; Julia adds `observed_wait_time`, `park_day_id`, `park_code`, `property_code` |
| **Park date** | 6am rule in **park TZ** (`derive_park_date` in S3 ETL) | 6am rule on `observed_at` **in its timezone** (`get_park_day_id`) |

Our 4 columns match. `park_day_id` = park date (Date); we can derive it from `observed_at` with the 6am rule in park TZ. `park_code` from `entity_code` prefix; `property_code` from a small map (wdw, dlr, uor, ush, tdr).

### Dimensions

| attraction-io | theme-park-crowd-report | Alignment |
|---------------|--------------------------|-----------|
| **dimdategroupid** (park_day_id → date_group_id) | **dimdategroupid** (park_date, date_group_id, …) | `park_date` = `park_day_id` (calendar date). Join on date. |
| **dimseason** (park_day_id → season, season_year) | **dimseason** (park_date, season, season_year) | Same; join on `park_date` = `park_day_id`. |
| **donorparkhours** (park_day_id, park_code, opening_time, closing_time, emh_*, opening_hour, closing_hour, hours_open) | **dimparkhours** (from S3: date, park, open, close, emh, …) | Same idea; column names and structure differ. A **bridge** (or renaming) would map (park_date, park_code) + open/close/emh → donor-style columns. |
| dimEntity, dimParkHours, dimEvents, dimHolidays, dimMetatable | dimentity, dimparkhours, dimeventdays, dimevents, dimmetatable; dimdategroupid includes holiday-style logic | Conceptually aligned; naming and layout differ. |

### Gaps

1. **No `latest_obs_report.csv`**  
   We do not produce it. It can be derived from our fact CSVs: for each `entity_code`, `latest_observation_date` = max(park_date) with data.

2. **No single Parquet (or entity-centric) fact**  
   - Julia expects one Parquet or an entity-filtered view.  
   - We have many park-date CSVs. Options: (a) **entity-grouped** `fact_tables/by_entity/{entity}.csv`, or (b) **one Parquet** (and optionally `latest_obs_report.csv`) under `fact_tables/` to mimic attraction-io’s S3 input.

3. **Park-hours for feature joins**  
   - `add_park_hours` needs (park_day_id, park_code) and donor-style columns (opening_time, closing_time, emh, opening_hour, closing_hour, hours_open).  
   - Our `dimparkhours` has date, park, open, close, emh; we’d need a small **donor builder** or adapter from `dimparkhours` → (park_date, park_code) + those columns (and timezone handling for `opening_time`/`closing_time` if Julia expects ZonedDateTime).

4. **`property_code`**  
   - We can add it when building features: e.g. from `entity_code` → park → property (wdw, dlr, uor, ush, tdr), same as `utils.jl` in attraction-io.

---

## 3. Goals and Next Steps

### A. Feed attraction-io (or a Julia re-run) as-is

- **Build** from our fact CSVs:
  - `fact_tables/wait_time_fact_table.parquet` (all 4 columns; optionally add `park_date`, `park_code`, `property_code` if we want to precompute).
  - `fact_tables/latest_obs_report.csv` (`entity_code`, `latest_observation_date`).
- **Publish** to `s3://touringplans_stats/stats_work/fact_tables/` (or equivalent) so `run_raw_wait_sync.jl` can pull them.  
- **Bridge** `dimparkhours` → donor-style (park_date, park_code, opening_time, closing_time, emh_*, opening_hour, closing_hour, hours_open) if we want to run `add_park_hours` in Julia without changing our dim.

### B. Entity-grouped fact for BI and features (no Julia)

- **Build** `fact_tables/by_entity/{entity_code}.csv` (or Parquet) from `fact_tables/clean/YYYY-MM/{park}_{date}.csv`.
- **Use for**: BI, coverage reports, and as input to a **Python feature layer** that replicates `add_mins_since_6am`, `add_park_hours`, `add_dategroupid`, `add_season`, `add_geometric_decay` using our dims.

### C. Python feature layer (theme-park-crowd-report)

- **Implement** (in `src/` or `processors/`):
  - `add_mins_since_6am`
  - `add_park_hours` (using `dimparkhours` or a donor-style view)
  - `add_dategroupid` (join to `dimdategroupid` on park_date)
  - `add_season` (join to `dimseason` on park_date)
  - `add_geometric_decay`
- **Input**: fact rows (from park-date CSVs or entity-grouped).  
- **Output**: rows with `pred_*`, `wgt_geo_decay`, `observed_wait_time` (and any IDs we need for modelling).

### D. Modelling (later)

- **Target**: `observed_wait_time` (from `wait_time_minutes`), likely for **ACTUAL** (and possibly POSTED); use of POSTED as a predictor TBD.
- **Predictors**: the `pred_*` and `wgt_geo_decay` above; optionally POSTED series.
- **Placement**: in theme-park-crowd-report (Python) or by feeding prepared Parquet + `latest_obs_report` into attraction-io’s Julia stack.

### Suggested order (for theme-park-crowd-report)

1. **Entity-grouped fact** (`fact_tables/by_entity/` or similar) — unblocks BI, `latest_obs_report`, and any single-table/Parquet build.
2. **`latest_obs_report.csv`** — from entity-grouped or from a full scan of park-date CSVs; needed for attraction-io and for “who to model” logic.
3. **Python feature module** — `add_mins_since_6am`, `add_dategroupid`, `add_season`, `add_geometric_decay` (these only need `observed_at`, `park_date`, and our dims). Then `add_park_hours` once we have a dimparkhours → donor bridge or equivalent.
4. **Parquet + S3 (optional)** — if the goal is to keep running attraction-io’s Julia pipeline unchanged; else we can stop at (3) and do modelling in Python.

---

## 4. References

- [attraction-io](https://github.com/disneystatswhiz/attraction-io) — main; README and `fact_table_sync` branch.
- **Critical review** of the legacy pipeline and how we improve: [docs/LEGACY_PIPELINE_CRITICAL_REVIEW.md](LEGACY_PIPELINE_CRITICAL_REVIEW.md).
- **fact_table_sync**: `run_raw_wait_sync.jl`, `run_jobs.jl`, `main_setup.jl`, `main_runner.jl`, `run_wait_time_ingestion.jl`, `run_features.jl`, `features.jl`, `utils.jl` (get_park_day_id, get_timezone_for_park).
- **theme-park-crowd-report**: [docs/SCHEMA.md](SCHEMA.md), [README.md](../README.md), [TODO.md](../TODO.md), `derive_park_date` and `get_park_code` in `get_tp_wait_time_data_from_s3.py`, `build_dimdategroupid.py`, `build_dimseason.py`, `get_park_hours_from_s3.py`, `get_wait_times_from_queue_times.py` (dimparkhours for hours filter).
