"""
Generate Backfill Curves (Historical ACTUAL)

================================================================================
PURPOSE
================================================================================
Generates historical ACTUAL wait time curves for past dates at 5-minute
resolution. Uses the with-POSTED model to impute ACTUAL from POSTED when
observed ACTUAL is not available.

Output:
  - curves/backfill/{entity_code}_{park_date}.csv
  - Columns: entity_code, park_date, time_slot, actual, source (observed|imputed)
  - actual is null where closed

================================================================================
USAGE
================================================================================
  python scripts/generate_backfill.py
  python scripts/generate_backfill.py --entity MK101 --start-date 2025-01-01 --end-date 2025-12-31
  python scripts/generate_backfill.py --output-base "D:\\Path" --max-entities 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.encoding import encode_features, load_encoding_mappings
from processors.entity_index import get_all_entities
from processors.features import PARK_TIMEZONE_MAP, add_features, load_dims
from processors.park_hours_versioning import get_park_hours_for_date, load_versioned_table
from processors.training import load_model
from utils.paths import get_output_base

try:
    import xgboost as xgb
except ImportError:
    xgb = None


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(output_base: Path) -> logging.Logger:
    """Set up logging to file and console."""
    log_dir = output_base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"generate_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to: {log_file}")
    return logger


# =============================================================================
# HELPER FUNCTIONS (reused from forecast)
# =============================================================================

def generate_time_slots(
    park_open_time: str,
    park_close_time: str,
) -> list[tuple[int, int, str]]:
    """Generate 5-minute time slots for park operating hours."""
    open_hour, open_min = map(int, park_open_time.split(":"))
    close_hour, close_min = map(int, park_close_time.split(":"))
    
    slots = []
    current_hour = open_hour
    current_min = open_min
    
    # Handle overnight (close < open)
    if close_hour < open_hour or (close_hour == open_hour and close_min < open_min):
        # First part: open to 23:55
        while current_hour < 24:
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            slots.append((current_hour, current_min, time_slot))
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
                if current_hour >= 24:
                    break
        
        # Second part: 00:00 to close
        current_hour = 0
        current_min = 0
        while current_hour < close_hour or (current_hour == close_hour and current_min <= close_min):
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            slots.append((current_hour, current_min, time_slot))
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    else:
        # Normal day: open to close
        while current_hour < close_hour or (current_hour == close_hour and current_min <= close_min):
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            slots.append((current_hour, current_min, time_slot))
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    
    return slots


def build_features_for_time_slot(
    entity_code: str,
    park_date: date,
    hour: int,
    minute: int,
    park_code: str,
    park_timezone: str,
    dims: dict,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """Build features for a single time slot (for model prediction)."""
    # Create observed_at timestamp for this time slot
    tz = ZoneInfo(park_timezone)
    observed_at = datetime(
        park_date.year,
        park_date.month,
        park_date.day,
        hour,
        minute,
        0,
        tzinfo=tz,
    )
    
    observed_at_str = observed_at.isoformat()
    
    # Create synthetic fact row
    fact_row = pd.DataFrame({
        "entity_code": [entity_code],
        "observed_at": [observed_at_str],
        "wait_time_type": ["ACTUAL"],
        "wait_time_minutes": [None],
    })
    
    # Add features
    df_features = add_features(
        fact_row,
        output_base,
        dims=dims,
        logger=logger,
    )
    
    return df_features


# =============================================================================
# LOAD HISTORICAL DATA
# =============================================================================

def load_historical_data_for_date(
    entity_code: str,
    park_date: date,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Load historical fact data for a specific entity and date.
    
    Returns:
        DataFrame with columns: entity_code, observed_at, wait_time_type, wait_time_minutes
        Filtered to the specified park_date
    """
    park_code = entity_code[:2] if len(entity_code) >= 2 else "MK"
    
    # Build expected CSV path
    date_str = park_date.strftime("%Y-%m-%d")
    month_dir = park_date.strftime("%Y-%m")
    csv_path = output_base / "fact_tables" / "clean" / month_dir / f"{park_code}_{date_str}.csv"
    
    if not csv_path.exists():
        if logger:
            logger.debug(f"No fact table for {entity_code} {park_date}: {csv_path}")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
    
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        if "entity_code" not in df.columns:
            return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
        
        # Filter to this entity
        entity_df = df[df["entity_code"] == entity_code].copy()
        
        if logger:
            logger.debug(f"Loaded {len(entity_df)} rows for {entity_code} {park_date}")
        
        return entity_df[["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]]
        
    except Exception as e:
        if logger:
            logger.warning(f"Error reading {csv_path}: {e}")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])


def aggregate_to_5min_slots(
    df: pd.DataFrame,
    park_date: date,
    park_timezone: str,
    time_slots: list[tuple[int, int, str]],
    logger: Optional[logging.Logger] = None,
) -> dict[str, dict[str, Optional[float]]]:
    """
    Aggregate historical fact data to 5-minute time slots.
    
    For each time slot, finds the closest POSTED and ACTUAL observations.
    Uses forward-fill for POSTED (last known value) and keeps observed ACTUAL.
    
    Returns:
        Dict: {time_slot: {"posted": value or None, "actual": value or None}}
    """
    if df.empty:
        return {slot[2]: {"posted": None, "actual": None} for slot in time_slots}
    
    tz = ZoneInfo(park_timezone)
    
    # Parse observed_at to datetime
    df = df.copy()
    df["dt"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df = df[df["dt"].notna()].copy()
    
    if df.empty:
        return {slot[2]: {"posted": None, "actual": None} for slot in time_slots}
    
    # Separate POSTED and ACTUAL
    df_posted = df[df["wait_time_type"] == "POSTED"].copy()
    df_actual = df[df["wait_time_type"] == "ACTUAL"].copy()
    
    # Create time slot datetimes
    slot_datetimes = {}
    for hour, minute, time_slot in time_slots:
        slot_dt = datetime(
            park_date.year,
            park_date.month,
            park_date.day,
            hour,
            minute,
            0,
            tzinfo=tz,
        )
        slot_datetimes[time_slot] = slot_dt
    
    # Aggregate to slots
    result = {}
    for time_slot, slot_dt in slot_datetimes.items():
        # Find closest POSTED (within 10 minutes, forward-fill)
        posted_value = None
        if not df_posted.empty:
            df_posted["time_diff"] = (df_posted["dt"] - slot_dt).abs()
            # Prefer observations at or after the slot (forward-fill)
            df_posted_after = df_posted[df_posted["dt"] >= slot_dt]
            if not df_posted_after.empty:
                closest = df_posted_after.loc[df_posted_after["time_diff"].idxmin()]
                if closest["time_diff"] <= timedelta(minutes=10):
                    posted_value = closest["wait_time_minutes"]
            else:
                # Fallback: closest before (backward-fill)
                closest = df_posted.loc[df_posted["time_diff"].idxmin()]
                if closest["time_diff"] <= timedelta(minutes=15):
                    posted_value = closest["wait_time_minutes"]
        
        # Find closest ACTUAL (within 5 minutes, exact match preferred)
        actual_value = None
        if not df_actual.empty:
            df_actual["time_diff"] = (df_actual["dt"] - slot_dt).abs()
            closest = df_actual.loc[df_actual["time_diff"].idxmin()]
            if closest["time_diff"] <= timedelta(minutes=5):
                actual_value = closest["wait_time_minutes"]
        
        result[time_slot] = {
            "posted": posted_value if pd.notna(posted_value) else None,
            "actual": actual_value if pd.notna(actual_value) else None,
        }
    
    return result


# =============================================================================
# PREDICTION
# =============================================================================

def predict_actual_with_posted(
    df_features: pd.DataFrame,
    posted_value: Optional[float],
    entity_code: str,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Optional[float]:
    """
    Predict ACTUAL wait time using the with-POSTED model.
    
    Args:
        df_features: DataFrame with features (one row)
        posted_value: POSTED wait time (minutes) - required for with-POSTED model
        entity_code: Entity code
        output_base: Pipeline output base directory
        logger: Optional logger
    
    Returns:
        Predicted ACTUAL wait time (minutes), or None if model not found or POSTED missing
    """
    if xgb is None:
        if logger:
            logger.error("XGBoost not installed")
        return None
    
    if posted_value is None:
        # Can't use with-POSTED model without POSTED
        return None
    
    try:
        # Load model and metadata
        model, metadata = load_model(
            entity_code,
            output_base,
            model_type="with_posted",
        )
        
        # Get feature columns from metadata
        feature_cols = metadata.get("feature_columns", [])
        if not feature_cols:
            if logger:
                logger.warning(f"No feature columns in metadata for {entity_code}")
            return None
        
        # Add POSTED to features
        df_features = df_features.copy()
        df_features["posted_wait_time"] = posted_value
        
        # Select and prepare features
        available_features = [col for col in feature_cols if col in df_features.columns]
        missing_features = [col for col in feature_cols if col not in df_features.columns]
        
        if missing_features and logger:
            logger.debug(f"Missing features for {entity_code}: {missing_features}")
        
        if not available_features:
            if logger:
                logger.warning(f"No available features for {entity_code}")
            return None
        
        X = df_features[available_features].copy()
        
        # Fill missing features with 0
        for col in feature_cols:
            if col not in X.columns:
                X[col] = 0
        
        # Reorder to match training order
        X = X[feature_cols]
        
        # Convert boolean to int
        for col in X.columns:
            if X[col].dtype == bool:
                X[col] = X[col].astype(int)
        
        # Fill nulls
        X = X.fillna(0)
        
        # Predict
        prediction = model.predict(X)[0]
        
        # Ensure non-negative
        prediction = max(0.0, float(prediction))
        
        return prediction
        
    except FileNotFoundError:
        if logger:
            logger.debug(f"Model not found for {entity_code} (with-POSTED)")
        return None
    except Exception as e:
        if logger:
            logger.error(f"Error predicting for {entity_code}: {e}")
        return None


# =============================================================================
# BACKFILL GENERATION
# =============================================================================

def generate_backfill_for_entity_date(
    entity_code: str,
    park_date: date,
    output_base: Path,
    dims: dict,
    encoding_mappings: Optional[dict],
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """
    Generate backfill for a single entity and date.
    
    Returns:
        DataFrame with columns: entity_code, park_date, time_slot, actual, source
        Returns None if park hours not available
    """
    # Get park code
    park_code = entity_code[:2] if len(entity_code) >= 2 else "MK"
    park_code_lower = park_code.lower()
    park_timezone = PARK_TIMEZONE_MAP.get(park_code_lower, "America/New_York")
    
    # Get park hours
    versioned_df = load_versioned_table(output_base)
    if versioned_df is None:
        if logger:
            logger.warning(f"Versioned park hours table not found for {entity_code} {park_date}")
        return None
    
    hours = get_park_hours_for_date(
        park_date,
        park_code,
        versioned_df,
        as_of=datetime.now(ZoneInfo("UTC")),
        logger=logger,
    )
    
    if not hours:
        if logger:
            logger.debug(f"No park hours for {park_code} {park_date}")
        return None
    
    park_open_time = hours.get("opening_time", "09:00")
    park_close_time = hours.get("closing_time", "22:00")
    
    # Generate time slots
    time_slots = generate_time_slots(park_open_time, park_close_time)
    
    if not time_slots:
        if logger:
            logger.warning(f"No time slots generated for {entity_code} {park_date}")
        return None
    
    # Load historical data
    df_historical = load_historical_data_for_date(
        entity_code,
        park_date,
        output_base,
        logger,
    )
    
    # Aggregate to 5-minute slots
    slot_data = aggregate_to_5min_slots(
        df_historical,
        park_date,
        park_timezone,
        time_slots,
        logger,
    )
    
    # Generate predictions for each time slot
    results = []
    for hour, minute, time_slot in time_slots:
        slot_info = slot_data.get(time_slot, {"posted": None, "actual": None})
        observed_actual = slot_info["actual"]
        posted_value = slot_info["posted"]
        
        # If we have observed ACTUAL, use it
        if observed_actual is not None:
            results.append({
                "entity_code": entity_code,
                "park_date": park_date,
                "time_slot": time_slot,
                "actual": observed_actual,
                "source": "observed",
            })
        else:
            # Try to impute using with-POSTED model
            if posted_value is not None:
                # Build features
                df_features = build_features_for_time_slot(
                    entity_code,
                    park_date,
                    hour,
                    minute,
                    park_code,
                    park_timezone,
                    dims,
                    output_base,
                    logger,
                )
                
                # Encode features
                if encoding_mappings:
                    df_encoded, _ = encode_features(
                        df_features,
                        output_base,
                        strategy=encoding_mappings.get("strategy", "label"),
                        mappings=encoding_mappings,
                    )
                else:
                    df_encoded, _ = encode_features(
                        df_features,
                        output_base,
                        strategy="label",
                    )
                
                # Predict ACTUAL
                imputed_actual = predict_actual_with_posted(
                    df_encoded,
                    posted_value,
                    entity_code,
                    output_base,
                    logger,
                )
                
                if imputed_actual is not None:
                    results.append({
                        "entity_code": entity_code,
                        "park_date": park_date,
                        "time_slot": time_slot,
                        "actual": imputed_actual,
                        "source": "imputed",
                    })
                else:
                    # No model or prediction failed - set to null
                    results.append({
                        "entity_code": entity_code,
                        "park_date": park_date,
                        "time_slot": time_slot,
                        "actual": None,
                        "source": "imputed",  # Still mark as imputed attempt
                    })
            else:
                # No POSTED available - set to null
                results.append({
                    "entity_code": entity_code,
                    "park_date": park_date,
                    "time_slot": time_slot,
                    "actual": None,
                    "source": "imputed",  # No data available
                })
    
    if not results:
        return None
    
    return pd.DataFrame(results)


def save_backfill_curve(
    df: pd.DataFrame,
    entity_code: str,
    park_date: date,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Path:
    """Save backfill curve to CSV."""
    curves_dir = output_base / "curves" / "backfill"
    curves_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{entity_code}_{park_date.strftime('%Y-%m-%d')}.csv"
    filepath = curves_dir / filename
    
    df.to_csv(filepath, index=False)
    
    if logger:
        logger.debug(f"Saved backfill: {filepath} ({len(df)} time slots)")
    
    return filepath


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate backfill curves for historical dates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--output-base",
        type=str,
        help="Pipeline output base directory (default: from config/config.json)",
    )
    
    parser.add_argument(
        "--entity",
        type=str,
        help="Generate backfill for specific entity only (default: all entities)",
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date (YYYY-MM-DD)",
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date (YYYY-MM-DD)",
    )
    
    parser.add_argument(
        "--max-entities",
        type=int,
        help="Limit number of entities to process (for testing)",
    )
    
    parser.add_argument(
        "--max-dates",
        type=int,
        help="Limit number of dates per entity (for testing)",
    )
    
    args = parser.parse_args()
    
    # Get output base
    if args.output_base:
        base = Path(args.output_base)
    else:
        base = get_output_base()
    
    # Set up logging
    logger = setup_logging(base)
    logger.info("Backfill Generation")
    logger.info(f"Output base: {base}")
    
    # Parse dates
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    
    if start_date > end_date:
        logger.error("Start date must be before end date")
        sys.exit(1)
    
    if start_date >= date.today():
        logger.warning("Start date is today or in the future. Backfill is for historical dates.")
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Load dimensions
    logger.info("Loading dimension tables...")
    dims = load_dims(base, logger)
    
    # Load encoding mappings
    logger.info("Loading encoding mappings...")
    encoding_mappings = load_encoding_mappings(base, logger)
    
    # Get entities
    if args.entity:
        entities = [args.entity]
        logger.info(f"Processing single entity: {args.entity}")
    else:
        logger.info("Loading entities from entity index...")
        index_db = base / "state" / "entity_index.sqlite"
        if not index_db.exists():
            logger.error(f"Entity index not found: {index_db}")
            logger.error("Run build_entity_index.py first")
            sys.exit(1)
        
        all_entities_df = get_all_entities(index_db)
        if all_entities_df.empty:
            logger.error("No entities found in entity index")
            sys.exit(1)
        
        all_entities = all_entities_df["entity_code"].tolist()
        if args.max_entities:
            entities = all_entities[:args.max_entities]
            logger.info(f"Limited to {len(entities)} entities (of {len(all_entities)} total)")
        else:
            entities = all_entities
            logger.info(f"Processing {len(entities)} entities")
    
    # Generate date range
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    
    if args.max_dates:
        dates = dates[:args.max_dates]
        logger.info(f"Limited to {len(dates)} dates per entity")
    
    logger.info(f"Generating backfill for {len(entities)} entities × {len(dates)} dates = {len(entities) * len(dates)} entity-dates")
    
    # Process each entity-date
    total_processed = 0
    total_saved = 0
    total_failed = 0
    
    for entity_code in entities:
        logger.info(f"Processing entity: {entity_code}")
        
        entity_dates_processed = 0
        entity_dates_saved = 0
        
        for park_date in dates:
            try:
                df_backfill = generate_backfill_for_entity_date(
                    entity_code,
                    park_date,
                    base,
                    dims,
                    encoding_mappings,
                    logger,
                )
                
                if df_backfill is not None and len(df_backfill) > 0:
                    save_backfill_curve(df_backfill, entity_code, park_date, base, logger)
                    entity_dates_saved += 1
                    total_saved += 1
                else:
                    total_failed += 1
                
                entity_dates_processed += 1
                total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing {entity_code} {park_date}: {e}", exc_info=True)
                total_failed += 1
                total_processed += 1
        
        logger.info(f"  Entity {entity_code}: {entity_dates_saved}/{entity_dates_processed} dates saved")
    
    logger.info("")
    logger.info("Backfill generation complete")
    logger.info(f"  Total processed: {total_processed}")
    logger.info(f"  Total saved: {total_saved}")
    logger.info(f"  Total failed: {total_failed}")
    logger.info(f"  Backfill curves: {base / 'curves' / 'backfill'}")


if __name__ == "__main__":
    main()
