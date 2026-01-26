# Legacy Pipeline (Attraction-IO): Critical Review

A production-readiness review of [attraction-io](https://github.com/disneystatswhiz/attraction-io) (fact_table_sync) to inform improvements in **theme-park-crowd-report**. We adopt its concepts but improve efficiency, workflow, quality, and documentation.

---

## 1. What the Legacy Pipeline Does (Complete Overview)

### 1.1 High-level flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  OFF-REPO: Step 0 (Python) — report.py, update.py, latest.py                │
│  Builds: wait_time_fact_table.parquet, latest_obs_report.csv → S3           │
│  (Not in public repo; EC2 pipeline.sh calls src/fact_table/main.py)         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  run_jobs.jl (scheduler)                                                    │
│  1. ensure_clean_dirs!(input, output, temp, work)  ← wipes everything       │
│  2. main_setup.jl (dims, donor, run_raw_wait_sync)                          │
│  3. load latest_obs_report.csv → entities with latest_obs in last 2 days    │
│  4. for each entity: run_entity(code; data_fact=DATA_FACT)  [sequential]    │
│  5. exit(1) if any run_entity fails                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
          ┌─────────────────────────────┴─────────────────────────────┐
          ▼                                                           ▼
┌──────────────────────┐                              ┌──────────────────────────┐
│  main_setup.jl       │                              │  main_runner.jl          │
│  - Skip if           │                              │  (per entity)            │
│    dimEntity mtime   │                              │  - set_attraction, sync  │
│    is today          │                              │  - wait_time_ingestion   │
│  - run_dimDate       │                              │  - futuredates, features │
│  - run_dimEntity     │                              │  - premodelling          │
│  - run_dimParkHours  │                              │  - encode, train, predict│
│  - run_dimEvents     │                              │  - calendar (dailyavgs,  │
│  - run_dimHolidays   │                              │    thresholds, levels)   │
│  - run_dimMetatable  │                              │  - reporting             │
│  - run_dimDateGroupID│                              │  - cleanup_folders (odd) │
│  - run_dimSeason     │                              └──────────────────────────┘
│  - run_donorParkHours│
│  - run_raw_wait_sync │  ← downloads Parquet + latest_obs from S3, loads DATA_FACT
└──────────────────────┘
```

### 1.2 Data and dimensions

- **Fact**: One `wait_time_fact_table.parquet` on S3, produced off-repo. `run_raw_wait_sync` downloads to `input/wait_times/`, loads into `DATA_FACT` (in-memory `DataFrame`). Expected columns: `entity_code`, `observed_at`, `wait_time_type`, `wait_time_minutes`.
- **latest_obs_report.csv**: `entity_code`, `latest_observation_date`. Used to select entities whose latest observation is within the last 2 days (hardcoded `FRESHNESS_WINDOW_DAYS=2`). Timezone for “today”: `America/Toronto` in `run_jobs.jl`.
- **Dimensions**: dimDate, dimEntity, dimParkHours, dimEvents, dimHolidays, dimMetatable, dimDateGroupID, dimSeason. Built in `main_setup` from S3 syncs or local logic; written to `work/_dim/` (LOC_DIM). Example: `run_dimEntity` syncs `s3://.../entities/` → `input/entities/`, globs `current_*_entities.csv`, combines, writes `LOC_DIM/dimentity.csv`, then **uploads** back to S3.
- **Donor**: `run_donorParkHours` produces `donorparkhours.csv` (used by `add_park_hours` in features). Separate from dimParkHours; column shape differs (opening_time, closing_time, emh_*, opening_hour, closing_hour, hours_open).

### 1.3 Per-entity (main_runner)

Each `run_entity` does (via `include()` of many scripts):

1. **Preprocessing**: set_attraction (from dimEntity), run_sync, **wait_time_ingestion** (filter `DATA_FACT` to entity, `wait_time_minutes` → `observed_wait_time`, write `work/{ENTITY}/wait_times/wait_times.csv`), run_futuredates.
2. **Features**: `run_features.jl` — add_mins_since_6am, add_park_hours (join donor on park_day_id+park_code), add_dategroupid, add_season, add_geometric_decay. Writes `work/.../features.csv`.
3. **Modelling**: run_encodefeatures, run_trainer, run_predictions (XGBoost.jl).
4. **Calendar**: dailyavgs, thresholds, assign_levels, observed_dailyavgs.
5. **Reporting**: descriptives, accuracy, daily_wait_time_curve.
6. **Cleanup**: `cleanup_folders(ATTRACTION.code)` only if `ATTRACTION.code != ENTITY_CODE[]` — easy to misread; cleans by pattern in work/output.

### 1.4 Orchestration and scheduling

- **EC2 `pipeline.sh`**: git pull (fact_table_sync), run `python3 src/fact_table/main.py` (Step 0 — **absent in public repo**), upload log to S3, `shutdown -h now`. **No** `run_jobs.jl` here; Julia is presumably a separate cron or manual.
- **run_jobs.jl**: Assumes `main_setup` has already run in the same process (it `Base.include`’s it). No direct integration with pipeline.sh in the public tree.

### 1.5 S3 and I/O

- **download_file_from_s3**: Always overwrites (the “skip if exists” is commented out). Parquet and latest_obs_report are re-downloaded every run.
- **Dimensions**: Mix of S3 sync → local build → write to `work/_dim` (and for dimEntity, upload back to S3). No atomic write pattern (direct `CSV.write` to target).
- **Fact**: Loaded fully into memory. For large Parquet, this can be a memory and GC bottleneck.

---

## 2. Critical Review: Efficiency

| Issue | Severity | Description |
|-------|----------|-------------|
| **Work dirs wiped every run** | High | `ensure_clean_dirs!` deletes `input`, `output`, `temp`, `work` at the start of **every** run. All dimensions, donor, and fact must be re-fetched and rebuilt. No incremental reuse. |
| **Fact table fully in memory** | Medium | `DATA_FACT = load_wait_time_fact_table()` loads the entire Parquet. Scales poorly with years of data; no streaming or partition-by-entity. |
| **Entity loop is strictly sequential** | Medium | `for code in entities; run_entity(code); sleep(0.2); end`. No `@sync`/`@async` or `pmap`. TODO claims “parallel job launching works” but the main path is sequential. |
| **Re-downloads on every run** | Medium | `download_file_from_s3` does not skip when local file exists. Parquet and latest_obs_report are re-downloaded even when unchanged. |
| **Re-include of scripts** | Low | Each `run_entity` does `include(joinpath(ROOT, "src", "data", "run_*.jl"))` for many files. Scripts re-read from disk and re-execute; not true modules. Minor cost. |
| **Duplicate park-hours logic** | Medium | dimParkHours and donorParkHours both hold park-hours-like data; donor has a different schema for `add_park_hours`. Two sources of truth and two sync/build steps. |
| **No incremental fact** | High | The design assumes one monolithic Parquet. There is no notion of “only new rows” for the modelling step; the off-repo Step 0 may do incrementality, but it’s opaque. |
| **Hardcoded timezone** | Low | `TZ_LOCAL = America/Toronto` for “today” and freshness. Fine for a single deployment; brittle if run in other regions. |

**Efficiency — Summary:** The “nuke working dirs and rebuild everything” approach is simple but expensive. It does not scale well with more entities or longer history. Full in-memory fact and sequential entity runs limit throughput.

---

## 3. Critical Review: Workflow

| Issue | Severity | Description |
|-------|----------|-------------|
| **Step 0 lives outside repo** | High | Parquet and latest_obs_report are produced by `src/fact_table/main.py` (report, update, latest), which is **not** in the public repo. Reproducibility and change control for the fact table are unclear. |
| **pipeline.sh vs run_jobs** | High | `pipeline.sh` runs only Step 0 (Python) then shuts down. `run_jobs.jl` runs setup + entities but is not invoked by `pipeline.sh`. The documented “Step 0 → 1 → 2” is split across different entrypoints; easy to run one without the other. |
| **Brittle “skip setup if today”** | Medium | main_setup skips if `dimEntity` exists and its mtime is “today”. If dimEntity is updated mid-day, or if the run crosses midnight, behavior can be surprising. Also, `ensure_clean_dirs!` wipes `input` before main_setup, so on a fresh run input is empty—the “skip” only helps when run_jobs is invoked twice in one day without the clean. |
| **ensure_clean_dirs and main_setup ordering** | High | Cleaning `input` at the start removes any prior dim or fact inputs. Then main_setup must re-sync everything. The “skip if today” in main_setup is based on dimEntity in `LOC_DIM` (under `work/_dim`), but `work` is in the cleaned list. So after clean, `work` is gone—the skip would not see an existing dimEntity. The skip only works if `ensure_clean_dirs!` does not delete `work` or if `LOC_DIM` is elsewhere. (In utility_setup, `LOC_DIM = work/_dim`.) So the first run after clean always rebuilds; the “skip” only applies to a **second** call to main_setup in the same process without an intermediate clean. Convoluted. |
| **Heavy global state** | Medium | `Main.DATA_FACT`, `Main.ATTRACTION`, `ENTITY_CODE[]`, and includes that pollute `Main` make control flow and testing harder. |
| **Cleanup logic** | Low | `if ATTRACTION.code != ENTITY_CODE[]` then `cleanup_folders(ATTRACTION.code)`. This avoids deleting the current entity’s output but is easy to misread; also `cleanup_folders` is by pattern (e.g. `"ak07"`) and can be broad. |
| **No idempotency / “don’t re-run on same input”** | Medium | TODO: “Add/fix code so that full modelling doesn’t re-run with same input data.” Entities are re-run whenever they fall in the 2-day freshness window; no hash or stamp of inputs to short-circuit. |
| **Single exit(1) on first entity failure** | Low | One failed entity fails the whole run. No partial-success notion; no retries. |
| **No process or file lock** | Medium | If two `run_jobs` or two EC2 jobs overlap, they can overwrite shared inputs/outputs. pipeline.sh does not mention a lock. |

**Workflow — Summary:** The split between Step 0 (off-repo / pipeline.sh) and Step 1–2 (run_jobs), combined with “clean everything then rebuild” and a subtle “skip setup if today,” makes the end-to-end workflow hard to reason about and to operate reliably.

---

## 4. Critical Review: Quality

| Issue | Severity | Description |
|-------|----------|-------------|
| **No canonical schema doc** | High | There is no single document that defines the fact table columns, types, semantics, or the 6am rule. Same for dimensions and donor. This makes validation and evolution harder. |
| **No validation before modelling** | High | There is no step that checks fact (or derived) rows for schema, ranges, or duplicate keys before encode/train. Bad data can fail deep in the stack or produce silent nonsense. |
| **get_park_day_id and timezones** | Medium | `get_park_day_id` uses `Dates.hour(dt_col)` on a ZonedDateTime—so it uses the observation’s stored timezone. If `observed_at` is ever stored in different TZs or as UTC without consistent conversion, 6am-rollover can be wrong for some parks. Our `derive_park_date(observed_at, park_tz)` is explicit and clearer. |
| **Dimension writes are not atomic** | Medium | Example: `CSV.write(joinpath(LOC_DIM, "dimentity.csv"), df_all_entities)`. A reader (or a downstream step) can see a partially written file on crash or interrupt. |
| **Imputation and wait-type handling incomplete** | Medium | TODO: “Finalize/improve imputation logic for POSTED, ACTUAL, and PRIORITY”; “Refactor pipeline to support new wait time types.” Production use of 8888, missing, or mixed types is not fully specified. |
| **Edge cases not automated** | Medium | TODO: “Late-arriving files,” “Missing or corrupted input files,” “Empty or all-missing entity data.” No automated handling or clear fail-fast. |
| **Logging and observability** | Medium | Many `@info`/`@warn` are commented out. pipeline.sh only uploads a single log file to S3; no structured logs, no run IDs, no metrics. |
| **Tests** | High | No visible unit or integration tests in the repo. `test_runner.jl` exists but is separate; coverage of dims, features, or run_jobs is unknown. |
| **run_wait_time_ingestion output** | Low | Writes `wait_times.csv`; `run_features` reads `future.csv`. So something (e.g. run_futuredates or run_sync) must produce `future.csv` in between. The contract between these steps is implied, not documented. |

**Quality — Summary:** Missing schema docs, no validation layer, non-atomic dimension writes, and partial handling of wait types and edge cases reduce trust and make production support harder. Logging and tests are light.

---

## 5. How We Improve in Theme-Park-Crowd-Report

We keep the **concepts** (fact + dimensions, 6am rule, park_day_id, dategroupid, season, park hours, features, freshness-based entity selection) and improve on each dimension.

### 5.1 Efficiency

| Legacy | theme-park-crowd-report improvement |
|--------|-------------------------------------|
| Wipe input/output/temp/work every run | **No wipe**. We keep `fact_tables/clean`, `dimension_tables`, `staging`, `logs`. Only clear true scratch (e.g. `temp/`) if needed. ETL and dims are incremental where possible. |
| Full fact in memory | **Park-date (or entity) files**. We write `fact_tables/clean/YYYY-MM/{park}_{date}.csv`. Downstream can stream or filter by entity/date without loading everything. For a future “single Parquet” or entity views, we build from these, not the other way around. |
| Sequential entity loop | **Keep sequential for now**; design feature/model layers so we can add parallelism or out-of-core later without changing the fact schema. |
| Re-download every time | **Incremental S3 pulls**. We only fetch new/changed files (e.g. by listing and comparing to state or by file dates). For dims we already use “fetch when needed” and atomic overwrites. |
| Two park-hours tables | **One dimparkhours**. We use a single `dimparkhours` from S3. A feature step can derive donor-style columns if needed, or we extend `dimparkhours` in a documented way. |

### 5.2 Workflow

| Legacy | theme-park-crowd-report improvement |
|--------|-------------------------------------|
| Step 0 off-repo; pipeline.sh vs run_jobs | **Single repo, clear entrypoints**. ETL (S3 + queue-times merge), dimension fetch, queue-times loop, and (later) feature/model runs are all in this repo. Scripts and `register_scheduled_tasks.ps1` make “what runs when” explicit. |
| ensure_clean_dirs + “skip if today” | **No daily wipe**. Dims and fact are additive/overwrite by design. A “run dimensions” or “run ETL” step is idempotent without deleting entire dirs. |
| Heavy globals, many includes | **Config and explicit params**. We use `config/config.json` (e.g. `output_base`), and we pass paths and options into functions. Fewer globals, easier to test. |
| No “don’t re-run on same input” | **Later**: add checksums or “last built” stamps for feature/model outputs so we can skip when inputs are unchanged. |
| No lock | **Processing lock** (e.g. `state/processing.lock`) for ETL and any job that must not overlap. We already use this for the 5am/7am ETL. |

### 5.3 Quality

| Legacy | theme-park-crowd-report improvement |
|--------|-------------------------------------|
| No schema doc | **docs/SCHEMA.md** for the fact table: columns, types, `observed_at` and 6am rule, `wait_time_type` and ranges, sources. We extend with dimension and feature contracts as we add them. |
| No validation before modelling | **validate_wait_times.py** before any feature/model run. Fails the run on invalid rows; reports outliers. We keep and extend it. |
| get_park_day_id vs TZ | **derive_park_date(observed_at, park_tz)** in ETL and in any feature code. Park TZ is explicit; we get park from `entity_code` and a small map. |
| Non-atomic dimension writes | **Atomic writes**: write to `{name}.csv.tmp`, then `os.replace` to `{name}.csv`; on error, unlink `.tmp`. All dimension scripts follow this. |
| Wait-type and edge-case TODOs | **Document** in SCHEMA and in code: 8888, missing, and valid ranges. **Validate** in `validate_wait_times`. We handle “no in-window parks” in queue-times by exiting cleanly; we can add more edge-case handling as we hit them. |
| Logging | **Structured logs** under `output_base/logs/` with timestamps and script names. We avoid commenting out log lines; we keep a single `output_base` so logs are in one place. |
| Tests | **Add tests** for parsers, `derive_park_date`, `get_park_code`, and validators as we touch them. We don’t need to reach legacy’s surface area at once. |

### 5.4 Documentation

| Legacy | theme-park-crowd-report improvement |
|--------|-------------------------------------|
| README describes fact_table_sync but code and pipeline.sh disagree | **README, PROJECT_STRUCTURE, config/README** describe what runs, where output lives, and how to configure. We keep them in sync with the code. |
| No alignment between fact schema and Julia’s expectations | **docs/ATTRACTION_IO_ALIGNMENT.md** maps legacy to our schema and dimensions and lists gaps. **docs/LEGACY_PIPELINE_CRITICAL_REVIEW.md** (this doc) records what we change and why. |
| Implied contracts between run_* scripts | **docs/SCHEMA.md** and module docstrings. For features, we will add a short “Inputs/Outputs” section when we implement that layer. |

---

## 6. Summary Table

| Dimension   | Legacy (attraction-io)                                                     | theme-park-crowd-report direction                                                                 |
|------------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| **Efficiency** | Wipe dirs; full fact in memory; sequential; re-download; two park-hours   | Keep dirs; park-date (and later entity) files; incremental S3; one dimparkhours; room to parallelize later |
| **Workflow**  | Step 0 off-repo; pipeline.sh vs run_jobs; clean+skip; globals; no lock   | One repo; clear entrypoints; no daily wipe; config and params; processing lock                    |
| **Quality**   | No schema doc; no validation; non-atomic dims; TZ implicit; weak tests  | SCHEMA.md; validate_wait_times; atomic dim writes; explicit park TZ; tests for core pieces        |
| **Documentation** | README vs code; no alignment doc; implied contracts                     | README, PROJECT_STRUCTURE, config/README; ATTRACTION_IO_ALIGNMENT; LEGACY_PIPELINE_CRITICAL_REVIEW |

---

## 7. References

- [attraction-io](https://github.com/disneystatswhiz/attraction-io) (main, fact_table_sync)
- fact_table_sync: `run_jobs.jl`, `main_setup.jl`, `main_runner.jl`, `run_raw_wait_sync.jl`, `run_wait_time_ingestion.jl`, `run_features.jl`, `features.jl`, `utils.jl` (get_park_day_id), `s3utils.jl`, `run_dimEntity.jl`, `pipeline.sh`
- [docs/ATTRACTION_IO_ALIGNMENT.md](ATTRACTION_IO_ALIGNMENT.md) — schema and goal mapping
- [docs/SCHEMA.md](SCHEMA.md) — fact table canonical definition
- theme-park-crowd-report: `get_tp_wait_time_data_from_s3.py`, `get_wait_times_from_queue_times.py`, `build_dimdategroupid.py`, `build_dimseason.py`, `get_park_hours_from_s3.py`, `validate_wait_times.py`, `paths.py`, `register_scheduled_tasks.ps1`
