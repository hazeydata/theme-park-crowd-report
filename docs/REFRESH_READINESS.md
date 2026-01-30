# Pipeline Refresh Readiness

**Current “where we are” summary:** See **docs/PIPELINE_STATE.md** (config, paths, cron, queue-times, commands).

Quick checklist for running a full refresh (ETL → dimensions → aggregates → training → forecast → WTI).

## Master script (single daily run)

**`scripts/run_daily_pipeline.sh`** runs the full pipeline in order:

1. ETL (incremental)  
2. Dimension fetches (entity, park hours, events, metatable + dimdategroupid, dimseason)  
3. Posted aggregates  
4. Wait time DB report  
5. Batch training  
6. Forecast  
7. WTI  

```bash
./scripts/run_daily_pipeline.sh
```

Options: `--output-base PATH`, `--no-stop-on-error` (continue on step failure), `--skip-etl`, `--skip-dimensions`, `--skip-aggregates`, `--skip-report`, `--skip-training`, `--skip-forecast`, `--skip-wti`.

**Cron (one job):** Run once daily at 6:00 AM ET:

```bash
bash scripts/install_cron.sh --daily-master
```

See LINUX_CRON_SETUP.md for details.

## Config

- **output_base**: Set in `config/config.json` (e.g. Dropbox path). All scripts use this for data/logs.
- **AWS**: Credentials in `~/.aws/credentials` and `~/.aws/config` so cron and scripts can access S3.

## What’s in Cron (Daily)

| Time (ET) | Task | Script |
|-----------|------|--------|
| 5:00 AM | Main ETL (incremental) | `run_etl.sh` |
| 5:30 AM | Wait time DB report | `report_wait_time_db.py --quick --lookback-days 14` |
| 6:00 AM | Dimension fetches + build dimdategroupid, dimseason | `run_dimension_fetches.sh` |
| 7:00 AM | Backup ETL | `run_etl.sh` |
| 8:00 AM | Batch training (entities needing modeling) | `train_batch_entities.py --min-age-hours 24` |

These run from **project root** and write to **output_base** (from config).

## Not in Cron (Run Manually or One-Off)

1. **Queue-times loop** – Continuous 5‑minute fetches. Start once (e.g. after boot):
   ```bash
   cd "<output_base>/.."   # or repo root if same)
   nohup bash scripts/run_queue_times_loop.sh --interval 300 > /tmp/queue_times_loop.log 2>&1 &
   ```

2. **Posted aggregates** – Required for forecasting. Run **after** dimensions (so dimdategroupid exists) and **before** forecast:
   ```bash
   python scripts/build_posted_aggregates.py
   ```
   - Writes `output_base/aggregates/posted_aggregates.parquet`.
   - If this previously failed with “0 rows”, check logs; first exception is now logged at WARNING.

3. **Forecast** – Run after **posted aggregates** and **models** exist:
   ```bash
   python scripts/generate_forecast.py
   ```
   - Optional: `scripts/start_forecast_when_ready.sh` waits for `posted_aggregates.parquet` then starts forecast.

4. **WTI** – Run after **forecast** (or backfill) curves exist:
   ```bash
   python scripts/calculate_wti.py
   ```

5. **Weekly (optional)** – Posted accuracy report, log cleanup; currently noted for Mac mini.

## Refresh Order (Full Manual Run)

1. ETL: `scripts/run_etl.sh`
2. Dimensions: `scripts/run_dimension_fetches.sh`
3. Posted aggregates: `python scripts/build_posted_aggregates.py`
4. Training (if needed): `python scripts/train_batch_entities.py --min-age-hours 24`
5. Forecast: `python scripts/generate_forecast.py`
6. WTI: `python scripts/calculate_wti.py`

Or use **prerequisites check** before forecast/WTI:

```bash
python scripts/check_prerequisites.py
```

## Common Gaps

- **Posted aggregates missing** – Forecast will log “No aggregates available”. Run `build_posted_aggregates.py` and re-run forecast.
- **Training failure for one entity** – Batch script now suggests checking `logs/train_entity_model_*.log` for that entity when stderr is empty.
- **Queue-times not updating** – Ensure the queue-times loop is running (manual or systemd); it is not installed as a cron job.
