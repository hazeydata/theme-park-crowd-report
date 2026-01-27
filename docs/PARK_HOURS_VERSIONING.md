# Park Hours Versioning System

## Problem Statement

1. **Limited future data**: Park hours are only known for a limited number of days ahead (varies by park, typically 30-180 days)
2. **Hours change over time**: Official hours announced early often change as the date approaches (e.g., Easter hours extended from 11pm to 1am)
3. **Forecasting needs**: We need to predict 2 years into the future, requiring park hours for all dates
4. **Previous approach**: Donor day imputation filled gaps but didn't handle versioning or change probability

## Proposed Solution: Versioned Park Hours

### Core Concept

Maintain **multiple versions** of park hours for each `(park_code, park_date)` combination:
- **Official**: Currently announced hours (from S3/dimparkhours)
- **Predicted**: Forecasted hours (from historical patterns, donor days, ML)
- **Historical**: Past actual hours (for training/validation)
- **Probability**: Likelihood that official hours will change

### Schema Design

#### `dimension_tables/dimparkhours_with_donor.csv`

Primary table with versioned park hours:

| Column | Type | Description |
|--------|------|-------------|
| `park_date` | date | Park operational date (YYYY-MM-DD) |
| `park_code` | str | Park code (MK, EP, HS, etc.) |
| `version_type` | str | `official`, `predicted`, `historical`, `final` |
| `version_id` | str | Unique version identifier (e.g., `official_2026-01-15`, `predicted_v1_2026-01-15`) |
| `source` | str | Source of hours (`s3_sync`, `donor_imputation`, `ml_forecast`, `manual_override`) |
| `created_at` | timestamp | When this version was created |
| `valid_from` | timestamp | When this version becomes valid (for temporal queries) |
| `valid_until` | timestamp | When this version expires (NULL for current) |
| `opening_time` | time | Park opening time (HH:MM:SS) |
| `closing_time` | time | Park closing time (HH:MM:SS) |
| `emh_morning` | bool | Morning Extra Magic Hours |
| `emh_evening` | bool | Evening Extra Magic Hours |
| `confidence` | float | Confidence score (0.0-1.0) for predicted versions |
| `change_probability` | float | Probability that official hours will change (0.0-1.0) |
| `notes` | str | Optional notes (e.g., "Extended for Easter", "Donor: 2024-04-15") |

**Primary key**: `(park_date, park_code, version_id)`

**Indexes**:
- `(park_date, park_code, version_type, valid_until)` - for querying current version
- `(park_date, park_code, created_at)` - for version history

#### `dimension_tables/dimparkhours_metadata.csv`

Metadata about versioning rules and change patterns:

| Column | Type | Description |
|--------|------|-------------|
| `park_code` | str | Park code |
| `days_ahead_known` | int | Typical days ahead official hours are known |
| `change_probability_base` | float | Base probability of hours changing (by days_before_date) |
| `donor_priority_rules` | json | Rules for selecting donor days (e.g., same dategroupid, same season) |
| `ml_model_version` | str | Version of ML model used for predictions |
| `last_updated` | timestamp | When metadata was last updated |

### Version Types

#### 1. **Official** (`version_type = 'official'`)
- **Source**: S3 sync from `get_park_hours_from_s3.py`
- **Valid from**: When synced from S3
- **Valid until**: When superseded by newer official hours or when date passes
- **Change probability**: Calculated based on:
  - Days until park_date
  - Historical change patterns (e.g., Easter hours change 80% of the time if >30 days out)
  - dategroupid (holidays more likely to change)
  - Season (peak season hours more stable)

#### 2. **Predicted** (`version_type = 'predicted'`)
- **Source**: ML forecast or donor day imputation
- **Valid from**: When created
- **Valid until**: When official hours become available or superseded
- **Confidence**: Based on:
  - Similarity to donor day
  - Historical accuracy of predictions
  - Days until park_date (further out = lower confidence)

#### 3. **Historical** (`version_type = 'historical'`)
- **Source**: Actual hours from past dates (for training)
- **Valid from**: Date of the park_date
- **Valid until**: NULL (permanent record)
- **Use**: Training models, validating predictions

#### 4. **Final** (`version_type = 'final'`)
- **Source**: Official hours that have been finalized (date has passed or within 7 days)
- **Valid from**: When marked as final
- **Valid until**: NULL
- **Use**: Authoritative record for past dates

### Query Logic

#### For Feature Engineering (Current Date)

```python
def get_park_hours_for_date(park_date: date, park_code: str, as_of: datetime) -> dict:
    """
    Get park hours for a given date, using the best available version as of 'as_of'.
    
    Priority:
    1. Official (if valid and not expired)
    2. Predicted (if no official and date is future)
    3. Historical (if date is past)
    """
    # Query: WHERE park_date = ? AND park_code = ? 
    #   AND (valid_from <= as_of AND (valid_until IS NULL OR valid_until > as_of))
    # ORDER BY version_type priority, created_at DESC
    # LIMIT 1
```

#### For Forecasting (Future Dates)

```python
def get_park_hours_for_forecast(park_date: date, park_code: str) -> dict:
    """
    Get park hours for forecasting (future dates).
    
    Returns:
    - official: If available
    - predicted: If no official (with confidence)
    - change_probability: Likelihood official will change
    """
    # Query official first
    # If not available, query predicted with highest confidence
    # Calculate change_probability based on metadata rules
```

### Implementation Plan

#### Phase 1: Schema and Basic Versioning

1. **Create versioned table schema**
   - Add `dimparkhours_with_donor.csv` with columns above
   - Migration script to convert existing `dimparkhours` to versioned format
   - Mark all existing as `version_type='official'`, `valid_from=now()`, `valid_until=NULL`
   - Keep original `dimparkhours.csv` for backward compatibility

2. **Update `get_park_hours_from_s3.py`**
   - When syncing, create new `official` versions
   - Mark old official versions as `valid_until=now()` when superseded
   - Detect changes: compare new hours to previous official for same (park_date, park_code)

3. **Update `add_park_hours()` in features.py**
   - Query versioned table instead of flat dimparkhours
   - Use `get_park_hours_for_date()` logic
   - Handle missing hours gracefully (use predicted or null features)

#### Phase 2: Donor Day Imputation (Improved)

1. **Build donor day selection logic**
   - **Primary criteria**: Same `dategroupid` (most important - holidays behave similarly)
   - **Secondary criteria**: Same `park_code`, same day-of-week (optional tiebreaker)
   - **Recency weighting**: More recent dates weighted higher
     - Formula: `recency_weight = 1.0 / (1.0 + days_ago / 365.0)` 
     - Recent year gets weight ~1.0, older gets lower weight
   - **Scoring**: `score = (dategroupid_match ? 1.0 : 0.0) × recency_weight`
   - Select donor day with highest score
   - Create `predicted` versions from best donor day

2. **Fill gaps for future dates**
   - For dates > `days_ahead_known`: create predicted versions
   - Store `source='donor_imputation'`, `confidence` score, and `donor_park_date` in notes
   - Confidence = score (0.0-1.0) based on match quality
   - Update as official hours become available (mark predicted as `valid_until=now()`)

#### Phase 3: Change Probability Model

1. **Build change probability calculator**
   - Features: days_until_date, dategroupid, season, park_code, historical_change_rate
   - Train on historical data: did official hours change between announcement and date?
   - Output: `change_probability` for each official version

2. **Use in forecasting**
   - When forecasting, include `change_probability` in metadata
   - Allow models to account for uncertainty in park hours

#### Phase 4: ML-Based Predictions (Optional - Evaluate First)

1. **Track performance metrics**
   - Compare predicted (donor) vs actual official hours when they arrive
   - Metrics: MAE for opening/closing times, accuracy for EMH flags
   - Store in metadata table for analysis

2. **Evaluate ML feasibility**
   - If donor imputation accuracy is high (>90%), ML may not be worth complexity
   - If accuracy is low, consider ML model:
     - Features: dategroupid, season, day_of_week, park_code, historical patterns
     - Target: opening_time, closing_time, emh flags
     - Create `predicted` versions with `source='ml_forecast'`
   - Compare: ML vs donor day accuracy, choose best method

### Integration with Feature Engineering

```python
# In add_park_hours()
def add_park_hours(
    df: pd.DataFrame,
    dimparkhours_versioned: Optional[pd.DataFrame],
    as_of: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add park hours features using versioned park hours.
    
    Args:
        df: DataFrame with park_date, park_code, observed_at
        dimparkhours_versioned: Versioned park hours table
        as_of: Timestamp for version selection (default: now)
        logger: Optional logger
    
    Returns:
        DataFrame with park hours features + version metadata
    """
    if as_of is None:
        as_of = datetime.now(ZoneInfo("UTC"))
    
    # Query best version for each (park_date, park_code)
    # Join to df
    # Calculate features
    # Add version metadata: version_type, confidence, change_probability
```

### Benefits

1. **Temporal accuracy**: Use correct hours for the time period being modeled
2. **Uncertainty tracking**: Know when hours are likely to change
3. **Historical record**: Track how hours changed over time
4. **Flexible forecasting**: Support multiple prediction methods
5. **Audit trail**: Full history of version changes

### Design Decisions

1. **Version retention**: Keep all versions indefinitely (full audit trail)
2. **Change detection**: Track every change, including EMH schedules. Compare: opening_time, closing_time, emh_morning, emh_evening
3. **Confidence thresholds**: Use predicted if confidence > 0.7, else null (configurable)
4. **Storage**: CSV is sufficient. Estimated size: ~26,000 rows (2 years × 12 parks × 3 versions avg) = ~2-3 MB. Pandas can handle this efficiently with proper indexing in memory.
5. **Donor selection**: Primary criteria = same dategroupid. Weight more recent dates higher. Score = similarity_score × recency_weight
6. **ML model**: Track performance but may not be worth it. Start with donor imputation, evaluate ML later.

### Next Steps

1. Review and refine this design
2. Implement Phase 1 (schema + basic versioning)
3. Test with existing dimparkhours data
4. Update feature engineering to use versioned table
5. Implement Phase 2 (donor imputation)
6. Build change probability model (Phase 3)
