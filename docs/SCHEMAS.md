# Complete Schema Documentation

Comprehensive schema definitions for all fact and dimension tables, including data types, constraints, defaults, and cleaning rules.

**Principles:**
- **ISO8601 datetimes with timezone offset** for all datetime columns
- **All cells filled** in general (use null for truly missing/unknown)
- **Defaults** when it makes sense (e.g., `opened_on` = park opening date if blank, `extinct_on` = 2099-01-01 if blank)
- **Consistent naming**: snake_case for columns, lowercase for codes/abbreviations
- **Data types**: Use appropriate types (date, datetime, int, float, str, bool)

---

## Fact Tables

### fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv

**Path**: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv` under `output_base`

**Columns** (exactly 4):

| Column | Type | Nullable | Description | Constraints |
|--------|------|----------|-------------|-------------|
| `entity_code` | str | NO | Attraction identifier, uppercase (e.g. `MK101`, `EP09`). Prefix maps to park. | Uppercase, matches pattern `[A-Z]{2}\d+` |
| `observed_at` | str (ISO8601) | NO | Timestamp of observation, **ISO 8601 with timezone offset** (e.g. `2024-01-15T10:30:00-05:00`). | Format: `YYYY-MM-DDTHH:MM:SS±HH:MM` (no `Z`, explicit offset) |
| `wait_time_type` | str | NO | One of `POSTED`, `ACTUAL`, `PRIORITY`. | Must be one of: `POSTED`, `ACTUAL`, `PRIORITY` |
| `wait_time_minutes` | int | NO | Wait time in minutes. For `PRIORITY`, `8888` = sold out. | POSTED/ACTUAL: 0-1000; PRIORITY: -100 to 2000 or 8888 |

**Cleaning Rules:**
- `entity_code`: Uppercase, trim whitespace
- `observed_at`: Validate ISO8601 format, ensure timezone offset present
- `wait_time_type`: Uppercase, validate enum
- `wait_time_minutes`: Validate ranges per type; flag outliers (≥300 for POSTED/ACTUAL, < -100 or > 2000 and ≠ 8888 for PRIORITY)

**See**: [SCHEMA.md](SCHEMA.md) for detailed fact table documentation.

---

## Dimension Tables

### dimension_tables/dimentity.csv

**Source**: Combined from S3 `export/entities/current_*_entities.csv` (dlr, tdr, uor, ush, wdw)

**Expected Columns** (from S3; may vary by property):

| Column | Type | Nullable | Default | Description | Cleaning Rules |
|--------|------|----------|---------|-------------|----------------|
| `entity_code` | str | NO | - | Attraction identifier (e.g. `MK101`, `EP09`). Primary key. **Renamed from `code`.** | Uppercase, trim, validate pattern |
| `entity_name` | str | NO | - | Full name of the attraction. **Renamed from `name`.** | Trim, title case preferred |
| `park_code` | str | NO | - | Park abbreviation (MK, EP, HS, AK, DL, CA, IA, UF, EU, USH, TDL, TDS). **Extracted from `entity_code` prefix if not present.** | Uppercase, validate enum |
| `property_code` | str | YES | NULL | Property abbreviation (wdw, dlr, tdr, uor, ush). **May not be present in source data.** | Lowercase, validate enum, NULL if missing |
| `land` | str | YES | NULL | Land/area within park (e.g. "Fantasyland", "Tomorrowland") | Trim, NULL if missing |
| `opened_on` | date | NO | Park opening date | Date attraction opened. Default: park opening date if blank. Format: `YYYY-MM-DD` |
| `extinct_on` | date | NO | `2099-01-01` | Date attraction closed/permanently closed. Default: `2099-01-01` if blank (far future = still open). Format: `YYYY-MM-DD` |
| `attraction_type` | str | YES | NULL | Type of attraction (ride, show, meet, etc.) | Trim, NULL if missing |
| `fastpass_available` | bool | YES | `false` | Whether Fastpass/Lightning Lane available | Convert to bool, default `false` |
| `priority_available` | bool | YES | `false` | Whether priority access available | Convert to bool, default `false` |
| `height_requirement_inches` | float | YES | NULL | Height requirement in inches | NULL if missing, validate ≥ 0 |
| `duration_minutes` | float | YES | NULL | Typical duration in minutes | NULL if missing, validate > 0 |
| `thrill_level` | str | YES | NULL | Thrill level (gentle, moderate, high, etc.) | Trim, NULL if missing |

**Park Opening Dates** (for `opened_on` default):
- WDW Magic Kingdom: `1971-10-01`
- WDW EPCOT: `1982-10-01`
- WDW Hollywood Studios: `1989-05-01`
- WDW Animal Kingdom: `1998-04-22`
- DLR Disneyland: `1955-07-17`
- DLR California Adventure: `2001-02-08`
- TDR Tokyo Disneyland: `1983-04-15`
- TDR Tokyo DisneySea: `2001-09-04`
- UOR Universal Studios Florida: `1990-06-07`
- UOR Islands of Adventure: `1999-05-28`
- UOR Epic Universe: `2025-06-01` (future)
- USH Universal Studios Hollywood: `1964-07-15`

**Cleaning Script**: `src/clean_dimentity.py`

---

### dimension_tables/dimparkhours.csv

**Source**: Combined from S3 `export/park_hours/{prop}_park_hours.csv` (dlr, tdr, uor, ush, wdw)

**Expected Columns** (from S3; may vary):

| Column | Type | Nullable | Default | Description | Cleaning Rules |
|--------|------|----------|---------|-------------|----------------|
| `park_date` | date | NO | - | Park operational date. Format: `YYYY-MM-DD` | Validate date, ensure all dates present |
| `park_code` | str | NO | - | Park abbreviation (MK, EP, HS, AK, DL, CA, IA, UF, EU, USH, TDL, TDS) | Uppercase, validate enum |
| `property_code` | str | NO | - | Property abbreviation (wdw, dlr, tdr, uor, ush) | Lowercase, validate enum |
| `opening_time` | time | NO | - | Park opening time. Format: `HH:MM:SS` or `HH:MM` | Validate time format, ensure present |
| `closing_time` | time | NO | - | Park closing time. Format: `HH:MM:SS` or `HH:MM` | Validate time format, ensure present |
| `emh_morning` | bool | YES | `false` | Extra Magic Hours morning | Convert to bool, default `false` |
| `emh_evening` | bool | YES | `false` | Extra Magic Hours evening | Convert to bool, default `false` |
| `special_event` | str | YES | NULL | Special event name if applicable | Trim, NULL if missing |
| `notes` | str | YES | NULL | Additional notes | Trim, NULL if missing |

**Cleaning Rules:**
- Ensure all dates in range are present (no gaps)
- `opening_time` and `closing_time`: Validate format, ensure `closing_time` > `opening_time` (or handle overnight)
- Boolean columns: Convert strings ("true"/"false", "yes"/"no", "1"/"0") to bool

**Cleaning Script**: `src/clean_dimparkhours.py`

---

### dimension_tables/dimeventdays.csv

**Source**: S3 `export/events/current_event_days.csv`

**Expected Columns**:

| Column | Type | Nullable | Default | Description | Cleaning Rules |
|--------|------|----------|---------|-------------|----------------|
| `park_date` | date | NO | - | Date of event. Format: `YYYY-MM-DD`. **Renamed from `date`.** | Validate date |
| `park_code` | str | YES | NULL | Park abbreviation (MK, EP, HS, AK, etc.). **Renamed from `park_abbreviation`. Currently 100% null in source data.** | Uppercase, validate enum, NULL if missing |
| `event_abbreviation` | str | NO | - | Event code (e.g. "MNSSHP", "MVMCP") | Uppercase, trim |
| `event_opening_time` | time | YES | NULL | Event start time. Format: `HH:MM:SS` or `HH:MM` | Validate time format, NULL if missing |
| `event_closing_time` | time | YES | NULL | Event end time. Format: `HH:MM:SS` or `HH:MM` | Validate time format, NULL if missing |

**Cleaning Rules:**
- `park_date`: Validate date format
- `park_abbreviation`: Uppercase, validate against known parks
- `event_abbreviation`: Uppercase, trim
- Times: Validate format, ensure `event_closing_time` > `event_opening_time` if both present

**Cleaning Script**: `src/clean_dimeventdays.py`

---

### dimension_tables/dimevents.csv

**Source**: S3 `export/events/current_events.csv`

**Expected Columns**:

| Column | Type | Nullable | Default | Description | Cleaning Rules |
|--------|------|----------|---------|-------------|----------------|
| `property_code` | str | NO | - | Property abbreviation (wdw, dlr, tdr, uor, ush). **Renamed from `property_abbrev`.** | Lowercase, validate enum |
| `event_abbreviation` | str | NO | - | Event code (e.g. "MNSSHP", "MVMCP"). Primary key with property. | Uppercase, trim |
| `event_code` | str | YES | NULL | Alternative event code | Uppercase, trim, NULL if missing |
| `event_name` | str | NO | - | Full event name | Trim, title case preferred |
| `event_hard_ticket` | bool | YES | `false` | Whether event requires separate ticket | Convert to bool, default `false` |

**Cleaning Rules:**
- `property_abbrev`: Lowercase, validate enum
- `event_abbreviation`: Uppercase, trim
- `event_name`: Trim, ensure present
- `event_hard_ticket`: Convert to bool

**Cleaning Script**: `src/clean_dimevents.py`

---

### dimension_tables/dimmetatable.csv

**Source**: S3 `export/metatable/current_metatable.csv`

**Expected Columns** (from S3; structure may vary):

| Column | Type | Nullable | Default | Description | Cleaning Rules |
|--------|------|----------|---------|-------------|----------------|
| `park_date` | date | NO | - | Park operational date. Format: `YYYY-MM-DD` | Validate date |
| `park_code` | str | NO | - | Park abbreviation (MK, EP, HS, AK, etc.) | Uppercase, validate enum |
| `property_code` | str | NO | - | Property abbreviation (wdw, dlr, tdr, uor, ush) | Lowercase, validate enum |
| `extra_magic_hours_morning` | bool | YES | `false` | Extra Magic Hours morning | Convert to bool, default `false` |
| `extra_magic_hours_evening` | bool | YES | `false` | Extra Magic Hours evening | Convert to bool, default `false` |
| `parade` | str | YES | NULL | Parade name if scheduled | Trim, NULL if missing |
| `fireworks` | str | YES | NULL | Fireworks show name if scheduled | Trim, NULL if missing |
| `special_event` | str | YES | NULL | Special event name | Trim, NULL if missing |
| `closure_reason` | str | YES | NULL | Reason for park closure if applicable | Trim, NULL if missing |
| `notes` | str | YES | NULL | Additional notes | Trim, NULL if missing |

**Cleaning Rules:**
- Ensure all dates in range are present (no gaps)
- Boolean columns: Convert to bool, default `false`
- String columns: Trim, NULL if empty/missing

**Cleaning Script**: `src/clean_dimmetatable.py`

---

### dimension_tables/dimdategroupid.csv

**Source**: Built locally by `build_dimdategroupid.py`

**Columns** (all non-nullable unless noted):

| Column | Type | Nullable | Description | Constraints |
|--------|------|----------|-------------|-------------|
| `park_date` | date | NO | Park operational date. Format: `YYYY-MM-DD` | Date range: 2005-01-01 to today + 2 years |
| `year` | int | NO | Year (e.g. 2024) | 2005 ≤ year ≤ today.year + 2 |
| `month` | int | NO | Month (1-12) | 1 ≤ month ≤ 12 |
| `day` | int | NO | Day of month (1-31) | 1 ≤ day ≤ 31 |
| `day_of_week` | int | NO | Day of week (Mon=1, Sun=7) | 1 ≤ day_of_week ≤ 7 |
| `quarter` | int | NO | Quarter (1-4) | 1 ≤ quarter ≤ 4 |
| `week_of_year` | int | NO | ISO week of year (1-53) | 1 ≤ week_of_year ≤ 53 |
| `day_of_year` | int | NO | Day of year (1-366) | 1 ≤ day_of_year ≤ 366 |
| `month_name` | str | NO | Full month name (e.g. "January") | One of 12 month names |
| `month_mmm` | str | NO | Abbreviated month (e.g. "Jan") | One of 12 abbreviations |
| `month_m` | str | NO | Single letter month (J, F, M, A, M, J, J, A, S, O, N, D) | Single uppercase letter |
| `day_of_week_name` | str | NO | Full day name (e.g. "Monday") | One of 7 day names |
| `day_of_week_ddd` | str | NO | Abbreviated day (e.g. "Mon") | One of 7 abbreviations |
| `day_of_week_d` | str | NO | Single letter day (M, T, W, T, F, S, S) | Single uppercase letter |
| `month_year_mmm_yyyy` | str | NO | Month-year label (e.g. "Jan-2024") | Format: `MMM-YYYY` |
| `quarter_year_q_yyyy` | str | NO | Quarter-year label (e.g. "Q1-2024") | Format: `Q{1-4}-YYYY` |
| `year_yy` | str | NO | Two-digit year with apostrophe (e.g. "'24") | Format: `'YY` |
| `cur_day_offset` | int | NO | Days offset from today (negative = past, positive = future) | Integer |
| `cur_month_offset` | int | NO | Months offset from today | Integer |
| `cur_quarter_offset` | int | NO | Quarters offset from today | Integer |
| `cur_year_offset` | int | NO | Years offset from today | Integer |
| `future_date` | str | NO | "Future" or "Past" | One of: "Future", "Past" |
| `ytd_flag` | bool | NO | Year-to-date flag (true if day_of_year ≤ today's day_of_year) | Boolean |
| `mtd_flag` | bool | NO | Month-to-date flag (true if day ≤ today's day) | Boolean |
| `output_file_label` | str | NO | Output file label (e.g. "2024_01JAN") | Format: `YYYY_MMMMM` |
| `holidaycode` | str | NO | Holiday code (e.g. "NYD", "THK", "NONE") | Uppercase, "NONE" if no holiday |
| `holidayname` | str | NO | Holiday name (e.g. "New Year's Day", "None") | "None" if no holiday |
| `date_group_id` | str | NO | Date group identifier for modeling (e.g. "JAN_WEEK1_MON", "THANKSGIVING") | Uppercase, always present |

**Cleaning Rules:**
- All columns are computed/derived, so no cleaning needed (already clean)
- Validate date range and computed values match expectations
- Ensure `holidaycode` = "NONE" and `holidayname` = "None" for non-holidays

**No cleaning script needed** (built clean from code)

---

### dimension_tables/dimseason.csv

**Source**: Built locally by `build_dimseason.py` from `dimdategroupid.csv`

**Columns** (all non-nullable):

| Column | Type | Nullable | Description | Constraints |
|--------|------|----------|-------------|-------------|
| `park_date` | date | NO | Park operational date. Format: `YYYY-MM-DD` | Must match `dimdategroupid.park_date` |
| `season` | str | NO | Season label (e.g. "CHRISTMAS", "EASTER", "SUMMER") | Uppercase, always present |
| `season_year` | str | NO | Season with year (e.g. "CHRISTMAS_2023", "SUMMER_2024") | Format: `{SEASON}_{YYYY}` |

**Cleaning Rules:**
- All columns are computed/derived, so no cleaning needed (already clean)
- Validate `season` is one of known values
- Validate `season_year` format

**No cleaning script needed** (built clean from code)

---

## Data Type Standards

### Dates
- **Format**: `YYYY-MM-DD` (ISO 8601 date)
- **Example**: `2024-01-15`
- **Null**: Use `NULL` (empty string not allowed for dates)

### Datetimes
- **Format**: `YYYY-MM-DDTHH:MM:SS±HH:MM` (ISO 8601 with timezone offset)
- **Example**: `2024-01-15T10:30:00-05:00`
- **No `Z`**: Always use explicit offset (e.g. `-05:00`, `+09:00`)
- **Null**: Use `NULL` (empty string not allowed for datetimes)

### Times
- **Format**: `HH:MM:SS` or `HH:MM` (24-hour format)
- **Example**: `09:00:00` or `09:00`
- **Null**: Use `NULL` if time is unknown

### Booleans
- **Format**: `true` or `false` (lowercase)
- **CSV**: May be stored as `1`/`0` or `true`/`false` strings; convert to proper bool
- **Null**: Use `false` as default (not NULL) unless truly unknown

### Strings
- **Trim**: Remove leading/trailing whitespace
- **Case**: 
  - Codes/abbreviations: Uppercase (e.g. `MK`, `POSTED`)
  - Property codes: Lowercase (e.g. `wdw`, `dlr`)
  - Names: Title case preferred (e.g. "Magic Kingdom")
- **Null**: Use `NULL` (empty string converted to NULL)

### Integers/Floats
- **Null**: Use `NULL` if value is unknown
- **Validation**: Check ranges (e.g. wait times, heights)

---

## Cleaning Scripts

All cleaning scripts follow this pattern:
1. Read CSV from `dimension_tables/`
2. Apply cleaning rules (defaults, type conversion, trimming, validation)
3. Write cleaned CSV back to `dimension_tables/` (atomic write with `.tmp`)

**Scripts:**
- `src/clean_dimentity.py` - Clean dimentity.csv
- `src/clean_dimparkhours.py` - Clean dimparkhours.csv
- `src/clean_dimeventdays.py` - Clean dimeventdays.csv
- `src/clean_dimevents.py` - Clean dimevents.csv
- `src/clean_dimmetatable.py` - Clean dimmetatable.csv
- `src/clean_fact_tables.py` - Clean all fact table CSVs (optional, fact tables are already validated)

**Usage:**
```powershell
python src/clean_dimentity.py
python src/clean_dimentity.py --output-base "D:\Path"
```

---

## Validation

After cleaning, validate:
1. **No empty strings** (use NULL instead)
2. **All required columns present** and non-null
3. **Data types correct** (dates parse, numbers are numeric, booleans are bool)
4. **Constraints satisfied** (enums, ranges, formats)
5. **Defaults applied** (opened_on, extinct_on, etc.)

---

## Next Steps

1. **Run dimension fetches** to get current data
2. **Inspect actual columns** in downloaded CSVs (may differ from expected)
3. **Update schemas** based on actual columns
4. **Implement cleaning scripts** with actual column names
5. **Test cleaning** on sample data
6. **Apply cleaning** to all dimension tables
7. **Validate** cleaned data
