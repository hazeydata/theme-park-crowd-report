# Scripts Directory

This directory is reserved for standalone utility scripts that can be run independently of the main pipeline.

## Current Usage

**Currently empty**: The main pipeline script is in `src/get_tp_wait_time_data_from_s3.py`.

## Potential Future Scripts

Examples of scripts that might go here:
- Data validation scripts
- One-off data migration scripts
- Reporting and analysis scripts
- Maintenance utilities

## Running Scripts

Scripts in this directory can be run directly:

```powershell
python scripts/script_name.py
```

Or with the virtual environment activated:

```powershell
.\venv\Scripts\Activate.ps1
python scripts/script_name.py
```

## Note

Scripts here should be standalone and not depend on the main pipeline structure. They can import from `src/` if needed, but should be runnable independently.
