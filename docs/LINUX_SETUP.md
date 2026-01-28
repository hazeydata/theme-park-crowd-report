# Linux Setup Guide

This guide covers running the Theme Park Crowd Report pipeline on Linux.

## Quick Start

```bash
# Clone the repo
git clone https://github.com/hazeydata/theme-park-crowd-report.git
cd theme-park-crowd-report

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure output path
cp config/config.linux.example.json config/config.json
# Edit config/config.json with your preferred output path

# Configure AWS credentials
aws configure
# Or set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

# Test dimension fetch
python3 src/get_entity_table_from_s3.py

# Test main ETL
python3 src/get_tp_wait_time_data_from_s3.py
```

## Directory Structure

The pipeline writes to `output_base` (from config/config.json):

```
/home/user/hazeydata/pipeline/
├── fact_tables/
│   └── clean/
│       └── YYYY-MM/
│           └── {park}_{date}.csv
├── staging/
│   └── queue_times/
├── dimension_tables/
├── state/
└── logs/
```

## Bash Scripts

Linux equivalents of the PowerShell scripts:

| Script | Purpose |
|--------|---------|
| `scripts/run_etl.sh` | Main ETL (equivalent to running the Python directly) |
| `scripts/run_dimension_fetches.sh` | Fetch all dimension tables |
| `scripts/run_queue_times_loop.sh` | Continuous Queue-Times.com fetcher |
| `scripts/install_cron.sh` | Install scheduled cron jobs |
| `scripts/common.sh` | Shared functions (sourced by other scripts) |

### Examples

```bash
# Run ETL manually
./scripts/run_etl.sh

# Run with custom output path
./scripts/run_etl.sh --output-base /path/to/output

# Full rebuild (reprocess everything)
./scripts/run_etl.sh --full-rebuild

# Fetch all dimension tables
./scripts/run_dimension_fetches.sh

# Start queue-times loop (runs until Ctrl+C)
./scripts/run_queue_times_loop.sh

# Custom interval (10 minutes)
./scripts/run_queue_times_loop.sh --interval 600
```

## Scheduled Tasks (Cron)

Install cron jobs to run the pipeline automatically:

```bash
# Preview what will be installed
./scripts/install_cron.sh --show

# Install cron jobs
./scripts/install_cron.sh

# View installed jobs
crontab -l

# Remove jobs
./scripts/install_cron.sh --remove
```

### Default Schedule

| Time (ET) | Task |
|-----------|------|
| 5:00 AM | Main ETL (incremental) |
| 5:30 AM | Wait time DB report |
| 6:00 AM | Dimension fetches |
| 7:00 AM | Secondary ETL (backup) |
| Sunday 6:30 AM | Posted accuracy report |
| Sunday 7:00 AM | Log cleanup |

**Note:** Times are in system timezone. If your server isn't Eastern time:
```bash
# Option 1: Set system timezone
sudo timedatectl set-timezone America/New_York

# Option 2: Edit cron entries to use TZ prefix
# TZ=America/New_York is prepended to each command
```

## Queue-Times Loop as a Service

For 24/7 operation, run the queue-times fetcher as a systemd service:

```bash
# Copy service file
sudo cp scripts/queue-times-loop.service /etc/systemd/system/

# Edit paths in the service file
sudo nano /etc/systemd/system/queue-times-loop.service

# Enable and start
sudo systemctl daemon-reload
sudo systemctl enable queue-times-loop
sudo systemctl start queue-times-loop

# Check status
sudo systemctl status queue-times-loop

# View logs
sudo journalctl -u queue-times-loop -f
```

## AWS Credentials

The pipeline needs AWS credentials to access S3. Options:

1. **AWS CLI config** (recommended):
   ```bash
   aws configure
   # Enter your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
   ```

2. **Environment variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   ```

3. **IAM role** (if running on EC2):
   - Attach a role with S3 read permissions to the instance

## Troubleshooting

### Python not found
```bash
# Check Python installation
which python3
python3 --version

# If using pyenv or conda, activate the environment first
```

### Permission denied on scripts
```bash
chmod +x scripts/*.sh
```

### AWS credentials not working
```bash
# Test AWS access
aws s3 ls s3://touringplans_stats/export/ --max-items 5
```

### Cron jobs not running
```bash
# Check cron service
sudo systemctl status cron

# Check logs
grep CRON /var/log/syslog
```

## Differences from Windows

| Windows | Linux |
|---------|-------|
| `.\venv\Scripts\Activate.ps1` | `source venv/bin/activate` |
| Task Scheduler | cron / systemd |
| `D:\path\to\output` | `/home/user/path/to/output` |
| PowerShell scripts (`.ps1`) | Bash scripts (`.sh`) |
