# Linux Cron Setup - Theme Park Pipeline

## Installed Daily Tasks (Starting Tomorrow)

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

## Weekly Tasks (Skipped - Will Set Up on Mac Mini Next Week)

- **Sunday 6:30 AM**: Posted accuracy report (`scripts/report_posted_accuracy.py`)
- **Sunday 7:00 AM**: Log cleanup (`scripts/cleanup_logs.py --days 30 --keep-recent 10`)

## Queue-Times Loop (Continuous Process)

The queue-times fetcher runs continuously (every 5 minutes) and is **NOT** a cron job. It needs to run as a background process or systemd service.

**To start manually:**
```bash
cd /home/fred/Desktop/theme-park-crowd-report
nohup bash scripts/run_queue_times_loop.sh --interval 300 > /tmp/queue_times_loop.log 2>&1 &
```

**To set up as systemd service** (optional):
1. Edit `scripts/queue-times-loop.service` with correct paths
2. Copy to `/etc/systemd/system/`
3. `sudo systemctl daemon-reload`
4. `sudo systemctl enable queue-times-loop`
5. `sudo systemctl start queue-times-loop`

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

All cron job logs are written to:
```
/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report/logs/
```

- `cron_etl_5am.log` - 5 AM ETL run
- `cron_report_530am.log` - 5:30 AM report
- `cron_dimensions_6am.log` - 6 AM dimension fetches
- `cron_etl_7am.log` - 7 AM backup ETL

## Notes

- All scripts include `export PATH="$HOME/.local/bin:$PATH"` to ensure pip-installed packages are found
- System timezone is America/Toronto (Eastern), so cron times match Eastern time
- Weekly tasks will be set up on Mac mini next week
- Queue-times loop should be started manually or via systemd service (not cron)
