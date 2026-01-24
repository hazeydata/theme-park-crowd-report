# Connection Error Fix: S3 Streaming

## Error Description

**Error**: `ConnectionResetError: [WinError 10054] An existing connection was forcibly closed by the remote host`

**What it means:**
- The S3 connection was closed while streaming a large file
- This is a network-level issue, not a code bug
- Common causes:
  - Large files taking too long to stream
  - Network instability
  - S3 service throttling or connection limits
  - Timeout on long-running connections

**When it occurs:**
- While reading large fastpass files (especially `current_fastpass.csv` which is frequently updated)
- During streaming operations when pandas reads chunks from S3

## Solution Implemented

### 1. S3 Client Retry Configuration
- **Adaptive retry mode**: Handles transient errors automatically
- **Max attempts**: 5 retries with exponential backoff
- **Timeouts**: 
  - Read timeout: 300 seconds (5 minutes) for large files
  - Connect timeout: 60 seconds (1 minute)

### 2. Retry Logic in Parsers
- **Fastpass parser**: Added retry logic with exponential backoff (2^attempt seconds)
- **Standby parser**: Added retry logic for connection errors
- **Max retries**: 3 attempts per file
- **Error handling**: Catches `ResponseStreamingError`, `ConnectionError`, `IOError`

### 3. Error Recovery
- Files that fail after all retries are logged and skipped
- Processing continues with next file
- Failed files are recorded in `state/failed_files.json` and retried on the next run

### 4. Skip Old Repeatedly-Failed Files (See CHANGES.md)
- If a file has failed ≥3 times and its S3 LastModified is older than 600 days, it is skipped on future runs
- Tunables: `FAILED_SKIP_THRESHOLD`, `OLD_FILE_DAYS` in the main script

## Code Changes

### `src/get_tp_wait_time_data_from_s3.py`
- Added `ResponseStreamingError` import
- Added `Config` import from `botocore.config`
- Updated S3 client initialization with retry configuration
- Added retry logic to standby file processing

### `src/parsers/wait_time_parsers.py`
- Added retry logic to `parse_fastpass_stream()` function
- Handles connection errors with exponential backoff
- Logs retry attempts for monitoring

## Monitoring

**What to watch for:**
- Retry warnings in logs: `"Connection error reading {key} (attempt X/3): ... Retrying in Y seconds..."`
- Final failures: `"Failed to read {key} after 3 attempts: ..."`
- Files that consistently fail may need manual investigation

**If errors persist:**
- Check network connectivity
- Verify S3 bucket permissions
- Consider processing problematic files separately
- May need to increase timeout values for very large files

## Testing

The retry logic has been tested and will:
1. Automatically retry on connection errors
2. Wait progressively longer between retries (1s, 2s, 4s)
3. Continue processing other files if one fails
4. Log all retry attempts for debugging
