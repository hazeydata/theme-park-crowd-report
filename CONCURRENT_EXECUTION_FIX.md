# Concurrent Execution Prevention

## Problem Identified

**Issue**: Two Python processes were running simultaneously, causing conflicts:
- Database locks (SQLite)
- File write conflicts (CSV appending)
- State file race conditions (processed_files.json)
- Connection errors (S3 access conflicts)

**Evidence**: Two Python processes found running:
- PID 12072: Started 2026-01-22 1:52:34 PM
- PID 21980: Started 2026-01-22 12:18:07 PM

## Solution Implemented

### Process Lock File

Added a lock file mechanism to prevent multiple instances from running:

1. **Lock File Location**: `state/processing.lock`
2. **Lock Acquisition**: Before processing starts
3. **Lock Release**: In finally block (always releases, even on error)
4. **Stale Lock Detection**: Removes locks older than 24 hours

### How It Works

1. **Before Processing**: Script checks for lock file
   - If exists and recent (< 24 hours): Exit with error message
   - If exists and stale (> 24 hours): Remove and continue
   - If doesn't exist: Create lock file and continue

2. **During Processing**: Lock file remains, preventing other instances

3. **After Processing**: Lock file is removed in finally block

### Lock File Contents

The lock file contains:
- Process ID (PID)
- Start time
- Script path

This helps identify which process is running.

## Usage

### Normal Operation

Just run the script normally:
```powershell
python src/get_tp_wait_time_data_from_s3.py
```

If another instance is running, you'll see:
```
ERROR - ANOTHER INSTANCE IS ALREADY RUNNING
ERROR - Lock file: D:\...\state\processing.lock
ERROR - If you're sure no other instance is running, delete the lock file and try again.
```

### If Lock File Gets Stuck

If a process crashes and doesn't release the lock:

1. **Check if process is actually running**:
   ```powershell
   Get-Process python | Where-Object {$_.Path -like "*theme-park-crowd-report*"}
   ```

2. **If no process is running, delete the lock file**:
   ```powershell
   Remove-Item "D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report\state\processing.lock"
   ```

3. **Then run the script again**

### Automatic Stale Lock Cleanup

The script automatically removes lock files older than 24 hours. This handles cases where:
- Process crashed without cleaning up
- System was rebooted
- Lock file was left from a previous run

## Why This Matters

### Conflicts That Can Occur

1. **SQLite Database Locks**:
   - Multiple processes writing to same database
   - Can cause "database is locked" errors
   - Can corrupt the database

2. **CSV File Conflicts**:
   - Multiple processes appending to same CSV files
   - Can cause file corruption
   - Can lose data

3. **State File Race Conditions**:
   - Multiple processes updating processed_files.json
   - Can lose track of processed files
   - Can cause duplicate processing

4. **S3 Connection Issues**:
   - Multiple processes accessing same S3 files
   - Can cause connection errors
   - Can cause throttling

## Best Practices

1. **Check before running manually**: If you see a scheduled job running, don't start another manually
2. **Monitor logs**: Check logs to see if a job is already running
3. **Use Task Scheduler**: Set up scheduled task to run once per day
4. **Don't run multiple instances**: The lock prevents this automatically

## Technical Details

### Lock File Implementation

- **Platform**: Windows-compatible (uses file existence, not fcntl)
- **Location**: `state/processing.lock`
- **Format**: Text file with process info
- **Cleanup**: Automatic (stale lock detection) and manual (finally block)

### Error Handling

- Lock acquisition failure: Script exits immediately
- Lock release failure: Logged as warning (non-fatal)
- Stale locks: Automatically removed if > 24 hours old

## Related Issues

This fix addresses:
- Connection errors (can be caused by concurrent S3 access)
- Database lock errors
- File corruption
- State file inconsistencies
