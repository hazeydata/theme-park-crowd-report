"""
Generate Forecast Curves

================================================================================
PURPOSE
================================================================================
Generates predicted ACTUAL and POSTED wait times for future dates (tomorrow to
+2 years) at 5-minute resolution. This is the forecast stage of the modeling
pipeline.

Output:
  - curves/forecast/{entity_code}_{park_date}.csv
  - Columns: entity_code, park_date, time_slot, actual_predicted, posted_predicted

================================================================================
USAGE
================================================================================
  python scripts/generate_forecast.py
  python scripts/generate_forecast.py --entity MK101 --start-date 2026-01-26 --end-date 2026-12-31
  python scripts/generate_forecast.py --output-base "D:\\Path" --max-entities 10
"""

from __future__ import annotations

import argparse
import json
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
from processors.posted_aggregates import get_predicted_posted_5min_slots, load_posted_aggregates
from processors.training import load_model
from utils.entity_names import format_entity_display
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
    
    log_file = log_dir / f"generate_forecast_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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
# FORECAST GENERATION
# =============================================================================

def generate_time_slots(
    park_open_time: str,
    park_close_time: str,
) -> list[tuple[int, int, str]]:
    """
    Generate 5-minute time slots for park operating hours.
    
    Args:
        park_open_time: Opening time (HH:MM format, e.g., "09:00")
        park_close_time: Closing time (HH:MM format, e.g., "22:00")
    
    Returns:
        List of (hour, minute, time_slot_str) tuples
    """
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
    park_hours: Optional[dict] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Build features for a single time slot (for model prediction).
    
    Creates a synthetic fact row with observed_at set to the time slot,
    then runs add_features to get all feature columns.
    
    Args:
        entity_code: Entity code
        park_date: Park date
        hour: Hour (0-23)
        minute: Minute (0-59)
        park_code: Park code
        park_timezone: Park timezone (e.g., "America/New_York")
        dims: Dimension tables dict
        output_base: Pipeline output base directory
        park_hours: Optional dict with opening_time, closing_time (HH:MM format)
        logger: Optional logger
    
    Returns:
        DataFrame with one row containing all features
    """
    # Create observed_at timestamp for this time slot
    # Use park's local timezone
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
    
    # Convert to ISO 8601 string
    observed_at_str = observed_at.isoformat()
    
    # Create synthetic fact row
    fact_row = pd.DataFrame({
        "entity_code": [entity_code],
        "observed_at": [observed_at_str],
        "wait_time_type": ["ACTUAL"],  # Doesn't matter for features
        "wait_time_minutes": [None],  # No target for forecast
    })
    
    # Add features
    df_features = add_features(
        fact_row,
        output_base,
        logger=logger,
        include_park_hours=True,  # Try to include park hours
    )
    
    # If park hours features are missing (None), calculate them from provided hours
    # This happens when dimparkhours.csv doesn't exist
    needs_park_hours = False
    if "pred_park_open_hour" in df_features.columns:
        first_val = df_features["pred_park_open_hour"].iloc[0]
        needs_park_hours = (first_val is None or pd.isna(first_val))
    
    if park_hours and needs_park_hours:
        if logger:
            logger.debug(f"Calculating park hours features from provided hours: {park_hours}")
        # Parse hours from park_hours dict
        opening_time = park_hours.get("opening_time", "09:00")
        closing_time = park_hours.get("closing_time", "22:00")
        open_hour, open_min = map(int, opening_time.split(":"))
        close_hour, close_min = map(int, closing_time.split(":"))
        
        # Calculate mins_since_park_open
        observed_dt = pd.to_datetime(df_features["observed_at"], errors="coerce")
        park_date_dt = pd.Timestamp(park_date).tz_localize(tz)
        opening_dt = park_date_dt.replace(hour=open_hour, minute=open_min)
        mins_since_open = (observed_dt - opening_dt).dt.total_seconds() / 60.0
        
        # Handle negative (before park open) - can be negative, that's OK
        df_features["pred_mins_since_park_open"] = mins_since_open.astype("Float64")
        df_features["pred_park_open_hour"] = open_hour
        df_features["pred_park_close_hour"] = close_hour
        df_features["pred_park_hours_open"] = float((close_hour - open_hour) % 24)
        df_features["pred_emh_morning"] = bool(park_hours.get("emh_morning", False))
        df_features["pred_emh_evening"] = bool(park_hours.get("emh_evening", False))
        
        if logger:
            logger.debug(f"Set park hours: open={open_hour}:{open_min:02d}, close={close_hour}:{close_min:02d}, mins_since_open={mins_since_open.iloc[0]:.1f}")
    
    return df_features


def predict_actual_for_time_slot(
    df_features: Optional[pd.DataFrame],
    entity_code: str,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Optional[float]:
    """
    Predict ACTUAL wait time for a time slot using the without-POSTED model.
    
    Args:
        df_features: DataFrame with features (one row)
        entity_code: Entity code
        output_base: Pipeline output base directory
        logger: Optional logger
    
    Returns:
        Predicted ACTUAL wait time (minutes), or None if model not found
    """
    if xgb is None:
        if logger:
            logger.error("XGBoost not installed")
        return None
    
    try:
        # Load model and metadata
        model, metadata = load_model(
            entity_code,
            output_base,
            model_type="without_posted",
        )
        
        # Check if this is a mean model (for entities with < 1000 observations)
        if model is None and metadata.get("model_type") == "mean":
            mean_wait_time = metadata.get("mean_wait_time", 0.0)
            if logger:
                logger.debug(f"Using mean model for {entity_code}: {mean_wait_time:.2f} minutes")
            return max(0.0, float(mean_wait_time))
        
        # XGBoost model path - requires features
        if model is None:
            if logger:
                logger.debug(f"Model not found for {entity_code} (without-POSTED)")
            return None
        
        if df_features is None:
            if logger:
                logger.warning(f"Features required for XGBoost model but df_features is None")
            return None
        
        # Get feature columns from metadata (uses "feature_names" key)
        feature_cols = metadata.get("feature_names", metadata.get("feature_columns", []))
        if not feature_cols:
            if logger:
                logger.warning(f"No feature columns in metadata for {entity_code}")
            return None
        
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
        
        # Fill missing features with 0 (for features not in this row)
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
        
        # Predict - XGBoost models saved with feature names need numpy array, not DataFrame
        prediction = model.predict(X.values)[0]
        
        # Ensure non-negative
        prediction = max(0.0, float(prediction))
        
        return prediction
        
    except FileNotFoundError:
        if logger:
            logger.debug(f"Model not found for {entity_code} (without-POSTED)")
        return None
    except Exception as e:
        if logger:
            logger.error(f"Error predicting for {entity_code}: {e}")
        return None


def generate_forecast_for_entity_date(
    entity_code: str,
    park_date: date,
    output_base: Path,
    dims: dict,
    aggregates: Optional[pd.DataFrame],
    encoding_mappings: Optional[dict],
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """
    Generate forecast for a single entity and date.
    
    Args:
        entity_code: Entity code
        park_date: Park date
        output_base: Pipeline output base directory
        dims: Dimension tables dict
        aggregates: Posted aggregates DataFrame (if None, loads from file)
        encoding_mappings: Encoding mappings (if None, loads from file)
        logger: Optional logger
    
    Returns:
        DataFrame with columns: entity_code, park_date, time_slot, actual_predicted, posted_predicted
        Returns None if park hours not available or model not found
    """
    # Get park code from entity code
    park_code = entity_code[:2] if len(entity_code) >= 2 else "MK"
    
    # Get park timezone
    park_code_lower = park_code.lower()
    park_timezone = PARK_TIMEZONE_MAP.get(park_code_lower, "America/New_York")
    
    # Get park hours (try versioned table first, fallback to regular dimparkhours)
    versioned_df = load_versioned_table(output_base)
    hours = None
    
    if versioned_df is not None:
        # Use versioned table
        hours = get_park_hours_for_date(
            park_date,
            park_code,
            versioned_df,
            as_of=datetime.now(ZoneInfo("UTC")),
            logger=logger,
        )
    
    if hours is None:
        # Fallback to regular dimparkhours.csv
        if logger:
            logger.debug(f"Versioned park hours not available, using regular dimparkhours.csv")
        
        dimparkhours_path = output_base / "dimension_tables" / "dimparkhours.csv"
        if not dimparkhours_path.exists():
            if logger:
                logger.warning(f"Park hours table not found for {entity_code} {park_date}, using default hours (09:00-22:00)")
            # Use default hours as fallback to allow testing
            hours = {
                "opening_time": "09:00",
                "closing_time": "22:00",
                "emh_morning": False,
                "emh_evening": False,
            }
        else:
            try:
                dimparkhours = pd.read_csv(dimparkhours_path, low_memory=False)
                park_date_str = park_date.strftime("%Y-%m-%d")
                park_code_upper = park_code.upper()
                
                # Find date and park columns (dimparkhours uses "date" and "park")
                date_col = None
                for col in ["date", "park_date", "park_day_id"]:
                    if col in dimparkhours.columns:
                        date_col = col
                        break
                
                park_col = None
                for col in ["park", "park_code", "code"]:
                    if col in dimparkhours.columns:
                        park_col = col
                        break
                
                if not date_col or not park_col:
                    if logger:
                        logger.warning(f"dimparkhours.csv missing required columns (date, park), using default hours")
                    # Use default hours as fallback
                    hours = {
                        "opening_time": "09:00",
                        "closing_time": "22:00",
                        "emh_morning": False,
                        "emh_evening": False,
                    }
                else:
                    # Find matching row
                    mask = (
                        (pd.to_datetime(dimparkhours[date_col], errors="coerce").dt.strftime("%Y-%m-%d") == park_date_str) &
                        (dimparkhours[park_col].astype(str).str.upper() == park_code_upper)
                    )
                    matching = dimparkhours[mask]
                    
                    if matching.empty:
                        if logger:
                            logger.warning(f"No park hours for {park_code} {park_date}, using default hours (09:00-22:00)")
                        # Use default hours as fallback
                        hours = {
                            "opening_time": "09:00",
                            "closing_time": "22:00",
                            "emh_morning": False,
                            "emh_evening": False,
                        }
                    else:
                        # Use first match
                        row = matching.iloc[0]
                        
                        # Extract time strings (handle ISO8601 format)
                        opening_time_raw = row.get("opening_time", "09:00")
                        closing_time_raw = row.get("closing_time", "22:00")
                        
                        # Parse ISO8601 datetime strings to extract HH:MM
                        def extract_time(time_val):
                            if pd.isna(time_val):
                                return "09:00"
                            time_str = str(time_val)
                            if "T" in time_str:
                                # ISO8601 format: extract HH:MM from "2004-01-01T08:00:00-08:00"
                                parts = time_str.split("T")
                                if len(parts) > 1:
                                    time_part = parts[1].split("-")[0].split("+")[0]
                                    return time_part[:5]  # HH:MM
                            elif ":" in time_str:
                                return time_str[:5]  # Already HH:MM
                            return "09:00"  # Default
                        
                        hours = {
                            "opening_time": extract_time(opening_time_raw),
                            "closing_time": extract_time(closing_time_raw),
                            "emh_morning": bool(row.get("emh_morning", False)),
                            "emh_evening": bool(row.get("emh_evening", False)),
                        }
            except Exception as e:
                if logger:
                    logger.warning(f"Error reading dimparkhours.csv: {e}, using default hours (09:00-22:00)")
                # Use default hours as fallback
                hours = {
                    "opening_time": "09:00",
                    "closing_time": "22:00",
                    "emh_morning": False,
                    "emh_evening": False,
                }
    
    if not hours:
        if logger:
            logger.warning(f"No park hours for {park_code} {park_date}, using default hours (09:00-22:00)")
        # Use default hours as fallback to allow testing
        hours = {
            "opening_time": "09:00",
            "closing_time": "22:00",
            "emh_morning": False,
            "emh_evening": False,
        }
    
    # Extract time strings (handle ISO8601 or HH:MM format)
    park_open_time = hours.get("opening_time", "09:00")
    park_close_time = hours.get("closing_time", "22:00")
    
    # If ISO8601 format, extract HH:MM
    if "T" in str(park_open_time):
        park_open_time = str(park_open_time).split("T")[-1].split(" ")[-1][:5]
    if "T" in str(park_close_time):
        park_close_time = str(park_close_time).split("T")[-1].split(" ")[-1][:5]
    
    # Generate time slots
    time_slots = generate_time_slots(park_open_time, park_close_time)
    
    if not time_slots:
        if logger:
            logger.warning(f"No time slots generated for {entity_code} {park_date}")
        return None
    
    # Get predicted POSTED for all time slots
    posted_df = get_predicted_posted_5min_slots(
        entity_code,
        park_date,
        park_open_time=park_open_time,
        park_close_time=park_close_time,
        aggregates=aggregates,
        output_base=output_base,
        logger=logger,
    )
    
    # Create lookup dict: time_slot -> posted_predicted
    posted_lookup = dict(zip(posted_df["time_slot"], posted_df["posted_predicted"]))
    
    # Generate predictions for each time slot
    results = []
    for hour, minute, time_slot in time_slots:
        # Get predicted POSTED
        posted_predicted = posted_lookup.get(time_slot)
        
        # Check if this is a mean model (skip feature building/encoding if so)
        try:
            from processors.training import load_model
            _, metadata_check = load_model(
                entity_code,
                output_base,
                model_type="without_posted",
            )
            is_mean_model = metadata_check.get("model_type") == "mean"
        except (FileNotFoundError, Exception):
            is_mean_model = False
        
        if is_mean_model:
            # For mean models, we can predict directly without features/encoding
            actual_predicted = predict_actual_for_time_slot(
                None,  # No features needed for mean model
                entity_code,
                output_base,
                logger,
            )
        else:
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
                park_hours=hours,  # Pass park hours so features can be calculated
                logger=logger,
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
            actual_predicted = predict_actual_for_time_slot(
                df_encoded,
                entity_code,
                output_base,
                logger,
            )
        
        # Round predictions to match real-world display conventions
        # ACTUAL: round to nearest integer (whole minutes) - matches how actual wait times are reported
        if actual_predicted is not None and pd.notna(actual_predicted):
            actual_predicted = int(round(actual_predicted))
        
        # POSTED: round to nearest 5 minutes - matches how posted wait times are displayed on signs/apps
        if posted_predicted is not None and pd.notna(posted_predicted):
            posted_predicted = round(posted_predicted / 5.0) * 5.0
        
        # Only add if we have at least one prediction
        if actual_predicted is not None or posted_predicted is not None:
            results.append({
                "entity_code": entity_code,
                "park_date": park_date,
                "time_slot": time_slot,
                "actual_predicted": actual_predicted,
                "posted_predicted": posted_predicted,
            })
    
    if not results:
        return None
    
    return pd.DataFrame(results)


def save_forecast_curve(
    df: pd.DataFrame,
    entity_code: str,
    park_date: date,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Path:
    """
    Save forecast curve to CSV.
    
    Args:
        df: Forecast DataFrame
        entity_code: Entity code
        park_date: Park date
        output_base: Pipeline output base directory
        logger: Optional logger
    
    Returns:
        Path to saved file
    """
    curves_dir = output_base / "curves" / "forecast"
    curves_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{entity_code}_{park_date.strftime('%Y-%m-%d')}.csv"
    filepath = curves_dir / filename
    
    df.to_csv(filepath, index=False)
    
    if logger:
        logger.debug(f"Saved forecast: {filepath} ({len(df)} time slots)")
    
    return filepath


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate forecast curves for future dates",
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
        help="Generate forecast for specific entity only (default: all entities)",
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, default: tomorrow)",
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD, default: +2 years from start)",
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
    logger.info("Forecast Generation")
    logger.info(f"Output base: {base}")
    
    # Parse dates
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = date.today() + timedelta(days=1)  # Tomorrow
    
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        end_date = start_date + timedelta(days=365 * 2)  # +2 years
    
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Load dimensions
    logger.info("Loading dimension tables...")
    dims = load_dims(base, logger)
    
    # Load posted aggregates
    logger.info("Loading posted aggregates...")
    aggregates = load_posted_aggregates(base, logger)
    
    # Load encoding mappings
    logger.info("Loading encoding mappings...")
    encoding_mappings = load_encoding_mappings(base, logger)
    
    # Get entities
    if args.entity:
        entities = [args.entity]
        entity_display = format_entity_display(args.entity, base)
        logger.info(f"Processing single entity: {entity_display}")
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
    
    logger.info(f"Generating forecasts for {len(entities)} entities × {len(dates)} dates = {len(entities) * len(dates)} entity-dates")
    
    # Process each entity-date
    total_processed = 0
    total_saved = 0
    total_failed = 0
    
    for entity_code in entities:
        entity_display = format_entity_display(entity_code, base)
        logger.info(f"Processing entity: {entity_display}")
        
        entity_dates_processed = 0
        entity_dates_saved = 0
        
        for park_date in dates:
            try:
                df_forecast = generate_forecast_for_entity_date(
                    entity_code,
                    park_date,
                    base,
                    dims,
                    aggregates,
                    encoding_mappings,
                    logger,
                )
                
                if df_forecast is not None and len(df_forecast) > 0:
                    save_forecast_curve(df_forecast, entity_code, park_date, base, logger)
                    entity_dates_saved += 1
                    total_saved += 1
                else:
                    total_failed += 1
                
                entity_dates_processed += 1
                total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing {entity_display} {park_date}: {e}", exc_info=True)
                total_failed += 1
                total_processed += 1
        
        logger.info(f"  Entity {entity_display}: {entity_dates_saved}/{entity_dates_processed} dates saved")
    
    logger.info("")
    logger.info("Forecast generation complete")
    logger.info(f"  Total processed: {total_processed}")
    logger.info(f"  Total saved: {total_saved}")
    logger.info(f"  Total failed: {total_failed}")
    logger.info(f"  Forecast curves: {base / 'curves' / 'forecast'}")


if __name__ == "__main__":
    main()
