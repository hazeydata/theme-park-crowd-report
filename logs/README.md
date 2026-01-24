# Logs Directory

This directory contains processing logs from pipeline runs.

## What's Stored Here

Each run creates a new log file with a timestamp:
- **Wait-time ETL**: `get_tp_wait_time_data_YYYYMMDD_HHMMSS.log`
- **Entity table**: `get_entity_table_YYYYMMDD_HHMMSS.log`
- **Park hours**: `get_park_hours_YYYYMMDD_HHMMSS.log`
- Example: `get_tp_wait_time_data_20240122_143201.log`

## Log Contents

Logs include:
- **Start/end times**: When the run started and completed
- **Files processed**: Which S3 files were processed (or skipped as old repeatedly-failed)
- **Rows written**: How many rows were written to output files
- **Errors**: Any errors encountered (with full stack traces)
- **Statistics**: File type breakdown, processing summary, skipped counts

## Why Logs Are Important

- **Monitoring**: Check if scheduled jobs are running successfully
- **Debugging**: Identify problematic files or parsing issues
- **Auditing**: Track what data has been processed
- **Performance**: See how long processing takes

## Log Rotation

Logs are not automatically rotated. You may want to:
- Archive old logs periodically
- Delete logs older than a certain date
- Keep only the most recent N logs

## Note

This directory is gitignored, so log files are not tracked in version control. This is intentional - logs can be large and contain sensitive information.
