# Expert Review: GitHub vs Dropbox Folder Structure

**Purpose:** Unify where the pipeline writes (one `output_base`), eliminate duplicate `logs/` and split outputs between GitHub vs Dropbox, and make the layout configurable and maintainable.

**Status:** Implemented. This is a historical design doc. For current layout see **config/README.md** and **docs/PIPELINE_STATE.md**.

---

## 1. Principles (Data Pipeline Best Practice)

| Principle | Application |
|-----------|-------------|
| **Single source of truth** | One `output_base` for all pipeline outputs (facts, dimensions, state, samples, reports, logs). No “which folder?” for logs or dimension_tables. |
| **Code vs data split** | **GitHub repo** = code, config examples, tests, lightweight scratch dirs. **No** large data, no logs, no run-specific state in the repo. |
| **Canonical run location** | **Dropbox** = canonical “production” output: shared with team, backed up, and where downstream tools (e.g. Tableau, R, Excel) expect to read. |
| **Config over hardcoding** | `output_base` comes from `config/config.json` so scheduled tasks and ad‑hoc runs use the same path across machines (e.g. different Dropbox roots: `D:\Dropbox (TouringPlans.com)\...` vs `D:\TouringPlans.com Dropbox\...`). |
| **One `logs/`** | Logs live only under `output_base/logs/`. No repo-root `logs/` and no `output/logs` vs `Dropbox/.../logs` split for the same runs. |

---

## 2. What Belongs Where

### GitHub (`D:\GitHub\hazeydata\theme-park-crowd-report`)

| Path | Purpose | Tracked? |
|------|---------|----------|
| `src/` | Application and pipeline code | Yes |
| `scripts/` | Entrypoints: `run_dimension_fetches.ps1`, `register_scheduled_tasks.ps1`, `validate_wait_times.py`, `report_wait_time_db.py`, etc. | Yes |
| `config/` | `config.example.json` (template), `config/README.md`; `queue_times_entity_mapping.csv` if used | Example + README yes; `config.json` no |
| `tests/` | Unit and integration tests | Yes |
| `data/` | Placeholder for future raw/processed; not used by current pipeline | README only |
| `work/`, `temp/` | Scratch / intermediate; can be local-only | README only, contents gitignored |
| `output/` | **Optional dev target only.** Documented as `--output-base=./output` for local runs. Production does **not** write here. | README only, contents gitignored |

**Not in GitHub (remove from layout / .gitignore logic):**

- Repo-root `logs/` — **removed.** Logs exist only as `output_base/logs/`. No `logs/` at project root.

---

### Dropbox (or other `output_base` from config)

**Path:** `output_base` from `config/config.json` (e.g.  
`D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report` or `D:\TouringPlans.com Dropbox\fred hazelton\stats team\pipeline\hazeydata\theme-park-crowd-report`).

| Path under `output_base` | Written by | Purpose |
|--------------------------|------------|---------|
| `fact_tables/clean/YYYY-MM/` | 5am/7am ETL | Wait-time fact CSVs (large, 10k+ files) |
| `dimension_tables/` | 6am dimension fetch | dimentity, dimparkhours, dimeventdays, dimevents, dimmetatable, dimdategroupid, dimseason |
| `state/` | 5am/7am ETL | dedupe.sqlite, processed_files.json, failed_files.json, processing.lock |
| `staging/queue_times/` | queue-times fetcher | Staged queue-times CSVs before merge into fact_tables |
| `samples/YYYY-MM/` | 5am/7am ETL | Sample fact CSVs |
| `validation/` | `validate_wait_times.py` | Validation JSONs |
| `reports/` | `report_wait_time_db.py` | wait_time_db_report.md |
| **`logs/`** | All Python pipelines + dimension scripts | **Single** log directory for ETL, dimension fetches, queue-times, etc. |

---

## 3. Resolving the Current Split

### Before (current)

| Component | Output base | Logs |
|-----------|-------------|------|
| 5am/7am ETL | Dropbox (hardcoded in Python) | `output_base/logs/` (Dropbox) |
| 6am dimension fetch | **GitHub `output/`** (hardcoded in `run_dimension_fetches.ps1`) | `output/logs/` (GitHub) |
| Queue-times loop | GitHub `output/` (default in PS1) | `output/logs/` (GitHub) |

So: two output bases (GitHub `output/` and Dropbox) and two log trees (`output/logs/` and Dropbox `logs/`).

### After (recommended)

| Component | Output base | Logs |
|-----------|-------------|------|
| 5am/7am ETL | **config (default Dropbox)** | `output_base/logs/` |
| 6am dimension fetch | **config (same)** | `output_base/logs/` |
| Queue-times loop | **config (same)** | `output_base/logs/` |
| `validate_wait_times`, `report_wait_time_db` | `--output-base` (should match config for production) | N/A (reports under `output_base/`) |

- **Single `output_base`** ⇒ one `dimension_tables/`, one `state/`, one **`logs/`**.
- **No** `output/logs` in GitHub used by production. `output/` remains an optional dev target only.

---

## 4. Duplicate `logs/` and `output/logs`

- **Recommendation: do not keep two.**  
  - `output_base/logs/` is the only logs directory.  
  - Repo-root `logs/` is **removed** from the intended layout and from `.gitignore`’s `!logs/README.md` (there is no `logs/` at repo root).  
  - `output/logs/` exists only when you use `--output-base=./output` for **local dev**; it is not used by production.

- **Cleanup:**  
  - Remove any references to a top-level `logs/` in `PROJECT_STRUCTURE.md` and `README.md`.  
  - In `.gitignore`: drop `logs/**` and `!logs/README.md` if we never intend a repo-root `logs/`. Optionally keep `logs/` ignored in case someone creates it by mistake.  
  - After switching the 6am job to `output_base` from config, you can delete or archive `output/logs/` and `output/dimension_tables/` in the GitHub project if you no longer need them for dev.

---

## 5. Config as Single Source for `output_base`

- **`config/config.json`** (gitignored, copy from `config.example.json`):  
  - `output_base`: absolute path to the canonical output (e.g. your Dropbox path).

- **Fallback:**  
  - If `config/config.json` is missing or has no `output_base`, use the same default as today:  
    `D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report`.  
  - Users on different Dropbox layouts set `output_base` in `config.json` to their actual path.

- **Who reads it:**  
  - **Python:** `get_tp_wait_time_data_from_s3.py`, `get_wait_times_from_queue_times.py`, and any script that has an `--output-base` default (e.g. `validate_wait_times`, `report_wait_time_db`, dimension scripts when run stand‑alone) — via a small `get_output_base()` in `src/utils/paths.py` that reads `config/config.json` and falls back to the default.  
  - **PowerShell:** `run_dimension_fetches.ps1` and `run_queue_times_loop.ps1` — read `config/config.json` and set `$OutputBase` (or equivalent); if missing, use the same default.

- **Scheduled tasks:**  
  - 5am/7am: Python’s default `--output-base` comes from `get_output_base()` → config.  
  - 6am: `run_dimension_fetches.ps1` passes `--output-base $OutputBase` from config.  
  - No changes needed to `register_scheduled_tasks.ps1` beyond ensuring `config.json` exists on the machine that runs the jobs.

---

## 6. Summary of Conventions

| Topic | Convention |
|-------|------------|
| **Output base** | One `output_base` from `config/config.json`; fallback to Dropbox default. |
| **GitHub `output/`** | Optional `--output-base=./output` for local dev; production does not write here. |
| **Logs** | Only `output_base/logs/`. No repo-root `logs/`; no second log tree for production. |
| **Dropbox** | Canonical production output: facts, dimensions, state, samples, reports, logs. |
| **GitHub** | Code, config examples, tests, `data/`/`work/`/`temp/` placeholders, and `output/` as a dev option. |

---

## 7. Implementation Checklist ✅

- [x] Add `config/config.json` to `.gitignore`.
- [x] Add `src/utils/paths.py` with `get_output_base()` (read config, else default).
- [x] `get_tp_wait_time_data_from_s3.py`: default `--output-base` from `get_output_base()`.
- [x] `run_dimension_fetches.ps1`: set `$OutputBase` from `config/config.json`; fallback to same default.
- [x] `run_queue_times_loop.ps1`: default `$OutputBase` from config when not passed.
- [x] `get_wait_times_from_queue_times.py`: default `--output-base` from `get_output_base()`.
- [x] Dimension scripts (get_entity, get_park_hours, get_events, get_metatable, build_dimdategroupid, build_dimseason): default from `get_output_base()` when run without `--output-base` (e.g. standalone).
- [x] `validate_wait_times.py`, `report_wait_time_db.py`: optional default `--output-base` from `get_output_base()` for consistency.
- [x] Remove repo-root `logs/` from `PROJECT_STRUCTURE.md` and `README.md`; document only `output_base/logs/`.
- [x] Simplify `.gitignore`: remove `logs/**` and `!logs/README.md` (or leave `logs/` ignored; do not track `logs/README.md`).
- [x] Update `output/README.md`: production uses config (Dropbox); `output/` is for dev only.
- [x] Update `config/README.md` and `config.example.json` so `output_base` is the single source for the pipeline output path.
