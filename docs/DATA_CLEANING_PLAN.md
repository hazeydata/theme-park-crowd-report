# Data Cleaning Plan

This document outlines the plan for cleaning all dimension and fact tables to meet our schema standards.

## Goals

1. **All cells filled** (use NULL for truly missing/unknown, not empty strings)
2. **Defaults applied** where it makes sense (e.g., `opened_on` = park opening date, `extinct_on` = 2099-01-01)
3. **ISO8601 datetimes** with timezone offset for all datetime columns
4. **Consistent data types** (dates, times, booleans, numbers)
5. **Validated constraints** (enums, ranges, formats)

## What We've Created

### 1. Schema Documentation
- **`docs/SCHEMAS.md`** - Complete schema definitions for all tables
  - Column names, types, nullable, defaults
  - Cleaning rules for each column
  - Data type standards (ISO8601, booleans, etc.)

### 2. Inspection Tool
- **`src/inspect_dimension_tables.py`** - Inspect actual columns in dimension tables
  - Shows column names, types, null counts, sample values
  - Identifies empty strings that should be NULL
  - Use this first to see what we're actually working with

### 3. Cleaning Scripts (Complete)
- **`src/clean_dimentity.py`** - Clean dimentity.csv ✅
- **`src/clean_dimparkhours.py`** - Clean dimparkhours.csv ✅
- **`src/clean_dimeventdays.py`** - Clean dimeventdays.csv ✅
- **`src/clean_dimevents.py`** - Clean dimevents.csv ✅
- **`src/clean_dimmetatable.py`** - Clean dimmetatable.csv ✅
- **`src/clean_all_dimensions.py`** - Master script to run all cleaning scripts ✅
- **`src/clean_fact_tables.py`** - Not created (fact tables are already validated by `validate_wait_times.py`)

## Next Steps

### Step 1: Inspect Current Data
Run the inspection tool to see what columns actually exist:

```powershell
python src/inspect_dimension_tables.py
```

This will show:
- Actual column names (may differ from expected)
- Data types
- Null counts
- Sample values
- Empty strings that need cleaning

### Step 2: Update Schemas
Based on inspection results, update `docs/SCHEMAS.md` with:
- Actual column names found
- Any columns not in our expected list
- Any expected columns missing

### Step 3: Create Remaining Cleaning Scripts
Create cleaning scripts for:
1. `dimparkhours.csv` - Clean dates, times, booleans
2. `dimeventdays.csv` - Clean dates, times, codes
3. `dimevents.csv` - Clean codes, names, booleans
4. `dimmetatable.csv` - Clean dates, booleans, strings

**Template pattern** (from `clean_dimentity.py`):
- Read CSV
- Apply cleaning rules (trim, case, defaults, type conversion)
- Write cleaned CSV (atomic with `.tmp`)

### Step 4: Test Cleaning Scripts
Run each cleaning script on a sample or backup:

```powershell
# Backup first
Copy-Item dimension_tables\dimentity.csv dimension_tables\dimentity.csv.backup

# Clean
python src/clean_dimentity.py

# Verify
python src/inspect_dimension_tables.py
```

### Step 5: Apply Cleaning to All Tables
Once scripts are tested, run all cleaning scripts:

```powershell
python src/clean_dimentity.py
python src/clean_dimparkhours.py
python src/clean_dimeventdays.py
python src/clean_dimevents.py
python src/clean_dimmetatable.py
```

### Step 6: Validate Cleaned Data
After cleaning, validate:
- No empty strings (all converted to NULL)
- All required columns present and non-null
- Data types correct
- Constraints satisfied
- Defaults applied

## Cleaning Rules Summary

### Strings
- **Trim** leading/trailing whitespace
- **Case conversion**:
  - Codes/abbreviations: Uppercase (MK, POSTED)
  - Property codes: Lowercase (wdw, dlr)
  - Names: Title case preferred
- **Empty strings → NULL**

### Dates
- **Format**: `YYYY-MM-DD` (ISO 8601)
- **Parse** various formats if needed
- **Defaults**:
  - `opened_on`: Park opening date if blank
  - `extinct_on`: `2099-01-01` if blank (far future = still open)

### Datetimes
- **Format**: `YYYY-MM-DDTHH:MM:SS±HH:MM` (ISO 8601 with timezone)
- **No `Z`**: Always use explicit offset
- **Validate** format

### Times
- **Format**: `HH:MM:SS` or `HH:MM` (24-hour)
- **NULL** if unknown

### Booleans
- **Format**: `true` or `false` (lowercase)
- **Convert** from strings ("true"/"false", "yes"/"no", "1"/"0")
- **Default**: `false` (not NULL) unless truly unknown

### Numbers
- **Validate ranges** (e.g., wait times, heights)
- **NULL** if invalid (negative heights, non-positive durations)

## Park Opening Dates (for defaults)

| Park Code | Opening Date |
|-----------|--------------|
| MK | 1971-10-01 |
| EP | 1982-10-01 |
| HS | 1989-05-01 |
| AK | 1998-04-22 |
| DL | 1955-07-17 |
| CA | 2001-02-08 |
| TDL | 1983-04-15 |
| TDS | 2001-09-04 |
| UF | 1990-06-07 |
| IA | 1999-05-28 |
| EU | 2025-06-01 |
| USH | 1964-07-15 |

## Notes

- **dimdategroupid.csv** and **dimseason.csv** are built clean from code, so no cleaning needed
- **Fact tables** are already validated by `validate_wait_times.py`, but we could add a cleaning script if needed
- All cleaning scripts use **atomic writes** (`.tmp` file then `os.replace()`) to avoid corruption
- Cleaning scripts **overwrite** the original files (backup first if needed)

## Resolved

1. ✅ **Actual column names**: Inspected and documented in `docs/SCHEMAS.md`
2. ✅ **Missing columns**: Documented which columns are optional/nullable
3. ✅ **Extra columns**: All columns from S3 are preserved (e.g., dimmetatable has 490 columns)
4. ✅ **Date formats**: Standardized to YYYY-MM-DD format
5. ✅ **Boolean formats**: Converted to proper boolean type where appropriate

## Status

- ✅ Schema documentation created
- ✅ Inspection tool created
- ✅ All cleaning scripts created and tested
- ✅ All dimension tables cleaned and standardized
- ✅ Column naming standardized across all tables
- ✅ Defaults applied (opened_on, extinct_on)
- ✅ Data types validated and converted
- ✅ Empty strings converted to NULL

**All dimension tables are now cleaned and ready for use.**
