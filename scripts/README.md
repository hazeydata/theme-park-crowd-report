# Scripts Directory

Standalone utility scripts that can be run independently of the main pipeline.

## Current Scripts

### `register_scheduled_tasks.ps1`

Registers the **5 AM** and **7 AM Eastern** Windows scheduled tasks for the Theme Park Wait Time ETL.

**Run once** (or after changing Python path / project root):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/register_scheduled_tasks.ps1
```

Creates:
- **ThemeParkWaitTimeETL_5am** — Daily at 5:00 AM (primary)
- **ThemeParkWaitTimeETL_7am** — Daily at 7:00 AM (backup)

See [README.md](../README.md#scheduling) for details.

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
