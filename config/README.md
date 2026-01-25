# Configuration

This directory contains configuration files and templates for the Theme Park Wait Time Data Pipeline.

## Queue-Times Lookup

**`queue_times_entity_mapping.csv`** — Master lookup from queue-times.com ride IDs to TouringPlans `entity_code`. Used by `src/get_wait_times_from_queue_times.py`. Columns: `entity_code`, `park_code`, `queue_times_id`, `queue_times_name`, `touringplans_name`. Supplemental build scripts and workflow notes live in `temp/` (see `temp/QUEUE_TIMES_MAPPING_README.md`).

## Output Base (config.json)

**`config.json`** (copy from `config.example.json`) is the **single source for the pipeline output path**. It is gitignored.

- **`output_base`**: Absolute path where the pipeline writes: `fact_tables/`, `dimension_tables/`, `state/`, `logs/`, etc.  
  Example: `D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report`  
  (On some machines Dropbox may be `D:\TouringPlans.com Dropbox\...` — set `output_base` to your actual path.)

**Setup**:

1. Copy `config.example.json` to `config.json`.
2. Set `output_base` to your chosen output folder (e.g. your Dropbox pipeline folder).

If `config.json` is missing or has no `output_base`, the code falls back to the same default used in `config.example.json`. All of the following use this: 5am/7am ETL, 6am dimension fetch, `run_queue_times_loop.ps1`, `validate_wait_times.py`, `report_wait_time_db.py`, and the dimension/queue-times Python scripts when run without `--output-base`.

## Configuration Methods

### Command-Line Override

You can override the output base for a single run:

```powershell
python src/get_tp_wait_time_data_from_s3.py --props wdw,dlr --output-base "D:\Path"
```

### Environment Variables

AWS credentials can be configured via environment variables:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`

**Why**: Standard AWS authentication method, works with AWS CLI and other tools.

### AWS Credentials File

AWS credentials can be stored in `~/.aws/credentials`:

```
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

**Why**: Secure way to store credentials, used by AWS CLI and boto3.

## Example

See `config.example.json` for the template. Copy it to `config.json` and set `output_base` to your output folder.
