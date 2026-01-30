# Linux Cron Setup - Theme Park Pipeline

**Current “where we are” summary:** See **docs/PIPELINE_STATE.md** (config, paths, cron, queue-times, commands).

---

## Two ways to schedule

1. **Five separate cron jobs** (default) – ETL, report, dimensions, backup ETL, and training at different times.
2. **Single daily master** – One cron job at 6:00 AM that runs `scripts/run_daily_pipeline.sh` (ETL → dimensions → posted aggregates → report → training → forecast → WTI).

To install the **single daily pipeline**:
```bash
bash scripts/install_cron.sh --daily-master
```

To install the **five separate jobs** (default):
```bash
bash scripts/install_cron.sh
```

---

## Installed Daily Tasks (Five-Job Setup)

All times are Eastern (system timezone: America/Toronto = EST/EDT).

### 1. **5:00 AM Eastern** - Main ETL (Incremental)
- **Script**: `scripts/run_etl.sh`
- **What it does**: Runs incremental S3 wait time ETL (`src/get_tp_wait_time_data_from_s3.py`)
- **Log**: `logs/cron_etl_5am.log`

### 2. **5:30 AM Eastern** - Wait Time DB Report
- **Script**: `scripts/report_wait_time_db.py --quick --lookback-days 14`
- **What it does**: Generates daily wait time database report (`reports/wait_time_db_report.md`)
- **Log**: `logs/cron_report_530am.log`

### 3. **6:00 AM Eastern** - Dimension Fetches
- **Script**: `scripts/run_dimension_fetches.sh`
- **What it does**: 
  - Fetches from S3: entity, park hours, events, metatable
  - Builds locally: `dimdategroupid.csv`, `dimseason.csv`
- **Log**: `logs/cron_dimensions_6am.log`

### 4. **7:00 AM Eastern** - Secondary ETL (Backup)
- **Script**: `scripts/run_etl.sh`
- **What it does**: Backup ETL run (if 5 AM didn't run or S3 updates were late)
- **Log**: `logs/cron_etl_7am.log`

### 5. **8:00 AM Eastern** - Batch Training
- **Script**: `scripts/train_batch_entities.py --min-age-hours 24`
- **What it does**: Trains XGBoost models for entities that need modeling (from entity index). Only trains entities whose latest data is at least 24 hours old.
- **Log**: `logs/cron_training_8am.log`

## Weekly Tasks (Skipped - Will Set Up on Mac Mini Next Week)

- **Sunday 6:30 AM**: Posted accuracy report (`scripts/report_posted_accuracy.py`)
- **Sunday 7:00 AM**: Log cleanup (`scripts/cleanup_logs.py --days 30 --keep-recent 10`)

## Single Daily Pipeline (run_daily_pipeline.sh)

When installed with `install_cron.sh --daily-master`, one cron job runs at **6:00 AM Eastern**:

**Script:** `scripts/run_daily_pipeline.sh`

**Order:** ETL (incremental) → Dimension fetches → Posted aggregates → Wait time DB report → Batch training → Forecast → WTI

**Log:** `logs/daily_pipeline_YYYY-MM-DD.log` (same day’s runs append)

**Manual run:**
```bash
./scripts/run_daily_pipeline.sh
```
Options: `--no-stop-on-error`, `--skip-etl`, `--skip-dimensions`, `--skip-aggregates`, `--skip-report`, `--skip-training`, `--skip-forecast`, `--skip-wti`

---

## Queue-Times Loop (Continuous Process)

The queue-times fetcher runs continuously (every 5 minutes) and is **NOT** a cron job. It needs to run as a background process or systemd service.

**To start manually (from repo):**
```bash
cd /home/fred/Desktop/theme-park-crowd-report
nohup bash scripts/run_queue_times_loop.sh --interval 300 >> "output_base/logs/queue_times_loop.log" 2>&1 &
```
(Replace `output_base` with the path from `config/config.json`, or run from repo and the script uses config.)

**To set up as systemd service (starts on boot):**
```bash
sudo bash scripts/install_queue_times_service.sh
```
This copies `scripts/queue-times-loop.service` to `/etc/systemd/system/`, enables it, and starts it. The service is configured for user **fred** and project path `/home/fred/Desktop/theme-park-crowd-report`. To remove: `sudo bash scripts/install_queue_times_service.sh --remove`.

## Management Commands

**View installed cron jobs:**
```bash
crontab -l | grep -A 5 "theme-park-crowd-report"
```

**Remove all cron jobs:**
```bash
bash scripts/install_cron.sh --remove
```

**Reinstall cron jobs:**
```bash
bash scripts/install_cron.sh
```

**View what would be installed:**
```bash
bash scripts/install_cron.sh --show
```

## Logs

All cron and pipeline logs are written to **output_base/logs/** (path from `config/config.json`). Current value:

`/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report/logs/`

- **Single daily pipeline:** `daily_pipeline_YYYY-MM-DD.log`
- **Five-job setup (if used):** `cron_etl_5am.log`, `cron_report_530am.log`, `cron_dimensions_6am.log`, `cron_etl_7am.log`, `cron_training_8am.log`
- **Queue-times (if started with redirect):** `queue_times_loop.log`

## Notes

- All scripts include `export PATH="$HOME/.local/bin:$PATH"` to ensure pip-installed packages are found
- System timezone is America/Toronto (Eastern), so cron times match Eastern time
- Weekly tasks will be set up on Mac mini next week
- Queue-times loop should be started manually or via systemd service (not cron)
