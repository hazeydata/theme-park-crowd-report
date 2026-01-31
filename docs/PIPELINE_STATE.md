# Pipeline State — Where We Are

Single reference for the current Theme Park pipeline setup (Linux, user **fred**). Updated when config, cron, or services change.

**When you change** config paths, cron, queue-times, or dashboard setup, **update this file.** (A project rule reminds the AI to keep it in sync.)

---

## 1. Current setup (summary)

| Item | Value |
|------|--------|
| **User** | fred |
| **Repo** | `/home/fred/Desktop/theme-park-crowd-report` |
| **Output base (data & logs)** | `/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report` |
| **Cron** | Single daily run at **6:00 AM Eastern** (`run_daily_pipeline.sh`) |
| **Queue-times** | Continuous loop (every 5 min); **systemd service**, starts on boot |
| **Dropbox** | Synced under fred’s home: `~/TouringPlans.com Dropbox/` |

---

## 2. Config

- **File:** `config/config.json`
- **Important key:** `output_base` — all pipeline data and logs go under this path.
- **Current value:**  
  `/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report`
- **AWS:** Scripts and cron use `~/.aws/credentials` and `~/.aws/config` (needed for S3 ETL and dimension fetches).

---

## 3. What runs when

### 3.1 Daily pipeline (cron, 6:00 AM Eastern)

- **What:** One cron job runs `scripts/run_daily_pipeline.sh`.
- **Order:** ETL (incremental) → Dimension fetches → Posted aggregates → Wait time DB report → Batch training → Forecast → WTI.
- **Runs as:** fred (your crontab).
- **Log:** `output_base/logs/daily_pipeline_YYYY-MM-DD.log`

### 3.2 Queue-times loop (systemd, on boot + always)

- **What:** Fetches wait times from queue-times.com every 5 minutes; writes to `output_base/staging/queue_times/`. Morning ETL later merges staging into `fact_tables/clean`.
- **Service:** `queue-times-loop.service` (user fred, project dir = repo path).
- **Starts:** Automatically on boot if you ran `sudo bash scripts/install_queue_times_service.sh`.
- **Log (systemd):** `sudo journalctl -u queue-times-loop -f`  
  Optional file log: `output_base/logs/queue_times_loop.log` if started manually with redirect.

---

## 4. Key paths

All under **output_base** unless noted.

| Path | Purpose |
|------|--------|
| `output_base/` | Root for all pipeline data (see config) |
| `output_base/logs/` | Pipeline logs (daily pipeline, queue_times_loop, etc.) |
| `output_base/dimension_tables/` | dimentity, dimparkhours, dimdategroupid, dimseason, etc. |
| `output_base/fact_tables/clean/` | Cleaned wait time fact CSVs (by date) |
| `output_base/staging/queue_times/` | Queue-times fetcher output (before ETL merge) |
| `output_base/aggregates/` | posted_aggregates.parquet (for forecast) |
| `output_base/models/` | Per-entity XGBoost (or mean) models |
| `output_base/curves/forecast/` | Forecast curves (actual/posted predicted) |
| `output_base/state/` | entity_index.sqlite, encoding_mappings.json, lock files, etc. |
| `output_base/reports/` | wait_time_db_report.md, etc. |

---

## 5. Commands reference

### Cron (daily pipeline)

```bash
# View cron
crontab -l

# Install single 6 AM daily pipeline (current setup)
bash scripts/install_cron.sh --daily-master

# Remove cron
bash scripts/install_cron.sh --remove

# Preview what would be installed
bash scripts/install_cron.sh --show
```

### Manual daily run

```bash
cd /home/fred/Desktop/theme-park-crowd-report
./scripts/run_daily_pipeline.sh
# Or in background with log:
nohup ./scripts/run_daily_pipeline.sh >> "output_base/logs/daily_pipeline_$(date +%Y-%m-%d).log" 2>&1 &
```

### Queue-times (systemd)

```bash
# Install and enable on boot (run once; needs sudo)
sudo bash scripts/install_queue_times_service.sh

# Status
sudo systemctl status queue-times-loop

# Logs (live)
sudo journalctl -u queue-times-loop -f

# Stop
sudo systemctl stop queue-times-loop

# Remove service and disable on boot
sudo bash scripts/install_queue_times_service.sh --remove
```

### Queue-times (manual, no systemd)

```bash
cd /home/fred/Desktop/theme-park-crowd-report
nohup bash scripts/run_queue_times_loop.sh --interval 300 >> "output_base/logs/queue_times_loop.log" 2>&1 &
```

### Prerequisites / quick check

```bash
python scripts/check_prerequisites.py
```

### Dashboard (pipeline + queue-times + entities)

Single-page status dashboard (Python Dash). Refreshes every 5 minutes. Optional Basic Auth for sharing (e.g. with wilma).

```bash
# Run (no auth)
python dashboard/app.py

# Run with auth (share URL + credentials)
DASH_USER=admin DASH_PASSWORD=your-secret python dashboard/app.py
```

- **URL:** http://localhost:8050 or http://\<this-machine-ip\>:8050 (binds to 0.0.0.0)
- **Data:** Reads `output_base/state/pipeline_status.json` (written by daily pipeline and train_batch_entities) and `output_base/state/entity_index.sqlite`; checks queue-times process via `pgrep`
- See **dashboard/README.md** for details

---

## 6. Other docs

- **docs/DAILY_DOCUMENTATION_REVIEW.md** — End-of-day checklist to keep PIPELINE_STATE, README, and key docs in sync.
- **LINUX_CRON_SETUP.md** — Cron options (five separate jobs vs single daily master), queue-times service, log paths.
- **docs/REFRESH_READINESS.md** — Full refresh order, what’s in/out of cron, common gaps.
- **scripts/README.md** — All scripts (run_daily_pipeline.sh, install_cron.sh, install_queue_times_service.sh, etc.).

---

## 7. Changes from default

- **Cron:** We use the **single daily master** at 6 AM (not the five separate jobs).
- **Output base:** Set to **fred’s Dropbox** path under home (not `/media/fred/...`).
- **Queue-times:** Configured as a **systemd service** for user fred, starts on boot; unit file and install script are in `scripts/`.
- **Wilma:** No theme-park cron or queue-times under wilma; everything runs as **fred**.
