# Column Naming Standard

**Principle**: "Call things what they are" - use descriptive, clear names even if longer.

## Core Standards

### Dates
- **`park_date`** - Park operational date (YYYY-MM-DD)
  - NOT: `date`, `park_day`, `park_day_id`
- **`observed_at`** - Timestamp of observation (ISO8601 with timezone)
- **`opened_on`** - Date attraction/entity opened
- **`extinct_on`** - Date attraction/entity closed

### Codes and Identifiers
- **`entity_code`** - Attraction/entity identifier (e.g., "MK101", "EP09")
  - NOT: `code`, `attraction_code`
- **`park_code`** - Park abbreviation (e.g., "MK", "EP", "HS", "AK", "UF")
  - NOT: `park`, `park_abbreviation`, `park_abbrev`
- **`property_code`** - Property abbreviation (e.g., "wdw", "dlr", "uor")
  - NOT: `property`, `property_abbrev`
- **`event_code`** - Event code (e.g., "WDW028")
- **`event_abbreviation`** - Event abbreviation (e.g., "MNSSHP", "MVMCP")
- **`date_group_id`** - Date group identifier for modeling

### Names
- **`entity_name`** - Full name of attraction/entity (e.g., "Space Mountain")
  - NOT: `name`
- **`park_name`** - Full name of park (e.g., "Magic Kingdom")
- **`event_name`** - Full name of event (e.g., "Mickey's Not-So-Scary Halloween Party")
- **`land`** - Land/area within park (e.g., "Fantasyland")

### Times
- **`opening_time`** - Park/attraction opening time (ISO8601 with timezone)
- **`closing_time`** - Park/attraction closing time (ISO8601 with timezone)
- **`event_opening_time`** - Event opening time (ISO8601 with timezone)
- **`event_closing_time`** - Event closing time (ISO8601 with timezone)

### Other Common Columns
- **`wait_time_type`** - Type of wait time (POSTED, ACTUAL, PRIORITY)
- **`wait_time_minutes`** - Wait time in minutes
- **`season`** - Season label (e.g., "CHRISTMAS", "EASTER")
- **`season_year`** - Season with year (e.g., "CHRISTMAS_2023")

## Naming Patterns

### Suffixes
- **`_code`** - Abbreviation/identifier (uppercase for codes, lowercase for properties)
- **`_name`** - Full descriptive name
- **`_id`** - Numeric identifier
- **`_date`** - Date (YYYY-MM-DD)
- **`_at`** - Timestamp (ISO8601 with timezone)
- **`_on`** - Date (YYYY-MM-DD) for events (opened_on, extinct_on)
- **`_time`** - Time or datetime (ISO8601 with timezone)
- **`_minutes`** - Duration in minutes
- **`_flag`** - Boolean flag
- **`_available`** - Boolean availability

### Prefixes
- **`park_`** - Related to park (park_date, park_code, park_name)
- **`event_`** - Related to event (event_code, event_name, event_opening_time)
- **`entity_`** - Related to entity/attraction (entity_code, entity_name)

## Table-Specific Standards

### dimentity
- `code` → `entity_code`
- `name` → `entity_name`
- Extract `park_code` from `entity_code` prefix if not present

### dimparkhours
- `park` → `park_code`
- `date` → `park_date`

### dimeventdays
- `date` → `park_date`
- `park_abbreviation` → `park_code`

### dimevents
- `property_abbrev` → `property_code`
- (Other columns already follow standard)

## Implementation

All cleaning scripts should:
1. Rename columns to match this standard
2. Apply cleaning rules
3. Ensure consistency across all tables
