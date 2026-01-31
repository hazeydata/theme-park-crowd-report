# Pipeline status dashboard

Single-page dashboard: pipeline step status, queue-times job, entities table (this run + obs counts, wait types). Refreshes every 5 minutes.

## Run

From project root:

```bash
python dashboard/app.py
```

- **URL:** http://localhost:8050 (or http://\<this-machine-ip\>:8050 from another device)
- **Bind:** `0.0.0.0:8050` so it’s reachable on the LAN

## Optional auth (for sharing with wilma)

Set username and password; the app will use HTTP Basic Auth:

```bash
DASH_USER=admin DASH_PASSWORD=your-secret python dashboard/app.py
```

Share the URL and credentials with the person you grant access to.

## Data

- **Pipeline / training:** `output_base/state/pipeline_status.json` (written by `run_daily_pipeline.sh` and `train_batch_entities.py`)
- **Entity stats:** `output_base/state/entity_index.sqlite` (row counts, ACTUAL/POSTED/PRIORITY, latest date)
- **Queue-times:** Dashboard checks if the `get_wait_times_from_queue_times` process is running (`pgrep`)

## Dependencies

`dash`, `dash-auth` (see project `requirements.txt`). Install with:

```bash
pip install -r requirements.txt
```
