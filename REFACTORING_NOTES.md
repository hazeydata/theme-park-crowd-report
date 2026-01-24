# Refactoring Notes: Modular Wait Time ETL Pipeline

**Note**: This document describes the refactoring that was done. For current usage and features, see [README.md](README.md). For change history, see [CHANGES.md](CHANGES.md).

## Overview

The wait time ETL pipeline has been refactored into a modular structure, porting proven Julia logic while maintaining compatibility with the existing Python codebase.

## New Structure

### 1. File Type Classifier (`src/utils/file_identification.py`)

**Function**: `get_wait_time_filetype(key: str) -> str`

- Classifies S3 files as "Standby", "New Fastpass", "Old Fastpass", or "Unknown"
- Ported from Julia codebase
- Uses lowercase key matching and pattern detection
- Logs warnings for unknown file types

**Usage**:
```python
from utils import get_wait_time_filetype

file_type = get_wait_time_filetype("export/wait_times/wdw/2024/01/01/file.csv")
# Returns: "Standby"
```

### 2. Modular Parsers (`src/parsers/wait_time_parsers.py`)

**Functions**:
- `parse_standby_chunk(chunk: pd.DataFrame) -> pd.DataFrame`
- `parse_fastpass_chunk(chunk: pd.DataFrame, is_new_format: bool) -> pd.DataFrame`
- `parse_fastpass_stream(s3, bucket: str, key: str, chunksize: int) -> Iterable[pd.DataFrame]`

**Features**:
- **Standby parser**: Handles POSTED and ACTUAL wait times, splits into separate rows
- **Fastpass parser**: Auto-detects new vs old format, handles sold-out (FWINHR >= 8000 → 8888 minutes)
- **Streaming**: Processes large files in chunks
- **Error handling**: Returns empty DataFrames for invalid data

**Output**: All parsers return DataFrames with 4 columns:
- `entity_code` (str, upper, stripped)
- `observed_at` (str, ISO format)
- `wait_time_type` ("POSTED" | "ACTUAL" | "PRIORITY")
- `wait_time_minutes` (Int64, 0-1000 for standby, 8888 for sold-out fastpass)

### 3. Refactored Main Script (`src/get_tp_wait_time_data_from_s3.py`)

**Key Changes**:
1. **File Type Classification**: Uses `get_wait_time_filetype()` to classify each file before processing
2. **Modular Parsing**: Routes to appropriate parser based on file type
3. **Enhanced Logging**: Logs file type classification, parsing progress, and statistics
4. **File Type Statistics**: Tracks breakdown of file types processed

**Processing Flow**:
```
1. List S3 files
2. For each file:
   a. Classify file type (Standby/New Fastpass/Old Fastpass/Unknown)
   b. Get timezone from S3 key
   c. Route to appropriate parser
   d. Add timezone offset to observed_at
   e. Dedupe using SQLite
   f. Derive park and park_date
   g. Write grouped CSVs (fact_tables/clean/YYYY-MM/{park}_{park_date}.csv)
```

### 4. Output Structure

**Changed from**: Single Parquet file per month

**Changed to**: Individual CSV files per park and date
- Path: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv`
- Appends to existing files if they exist
- One file per unique (park, park_date) combination

## Key Features Preserved

✅ **Incremental Processing**: Tracks processed files in JSON
✅ **Deduplication**: SQLite-based primary key dedupe
✅ **Timezone Handling**: Automatic timezone offset addition
✅ **Park Derivation**: From entity_code prefix
✅ **Park Date**: 6 AM rule (if hour < 6, previous day)
✅ **Reservoir Sampling**: For sample CSV generation
✅ **Chunked Reading**: Handles large files efficiently
✅ **Error Handling**: Continues processing on errors

## New Features

✨ **File Type Classification**: Automatic detection of file types
✨ **Modular Parsers**: Separated parsing logic for maintainability
✨ **Enhanced Logging**: More detailed progress and statistics
✨ **File Type Statistics**: Tracks breakdown of processed file types

## Testing

See [README.md](README.md) for usage examples and testing instructions.

## Folder Structure

```
src/
├── __init__.py
├── get_tp_wait_time_data_from_s3.py  # Main script (refactored)
├── parsers/
│   ├── __init__.py
│   └── wait_time_parsers.py          # Modular parsers
└── utils/
    ├── __init__.py
    └── file_identification.py         # File type classifier

test_single_file_processing.py         # Test script
```

## Migration Notes

- Old parsing functions removed from main script (now in `parsers/wait_time_parsers.py`)
- File type detection now explicit (was implicit based on S3 prefix)
- Logging enhanced with file type classification messages
- Output changed from Parquet to CSV files
- Connection error retry logic added

## Current Status

✅ **Complete**: All parsers ported and tested
✅ **Complete**: File type classification working
✅ **Complete**: Output structure implemented
✅ **Complete**: Connection error handling added
✅ **Complete**: Documentation updated

For current usage, see [README.md](README.md).
