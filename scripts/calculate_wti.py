"""
Calculate Wait Time Index (WTI)

================================================================================
PURPOSE
================================================================================
Calculates Wait Time Index (WTI) for each (park, park_date) by averaging ACTUAL
wait times across all entities at 5-minute resolution.

WTI = mean(actual) over (entity, time_slot) where actual is not null (closed)

Uses:
  - Backfill curves (observed or imputed ACTUAL) for historical dates
  - Forecast curves (predicted ACTUAL) for future dates

Output:
  - wti/wti.parquet
  - Columns: park_code, park_date, time_slot, wti, n_entities, min_actual, max_actual

================================================================================
USAGE
================================================================================
  python scripts/calculate_wti.py
  python scripts/calculate_wti.py --start-date 2025-01-01 --end-date 2026-12-31
  python scripts/calculate_wti.py --output-base "D:\\Path" --park MK
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

from processors.entity_index import get_all_entities
from processors.park_hours_versioning import get_park_hours_for_date, load_versioned_table
from utils.paths import get_output_base


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(output_base: Path) -> logging.Logger:
    """Set up logging to file and console."""
    log_dir = output_base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"calculate_wti_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
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
# LOAD CURVES
# =============================================================================

def load_backfill_curve(
    entity_code: str,
    park_date: date,
    output_base: Path,
) -> Optional[pd.DataFrame]:
    """Load backfill curve for an entity and date."""
    curves_dir = output_base / "curves" / "backfill"
    filename = f"{entity_code}_{park_date.strftime('%Y-%m-%d')}.csv"
    filepath = curves_dir / filename
    
    if not filepath.exists():
        return None
    
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception:
        return None


def load_forecast_curve(
    entity_code: str,
    park_date: date,
    output_base: Path,
) -> Optional[pd.DataFrame]:
    """Load forecast curve for an entity and date."""
    curves_dir = output_base / "curves" / "forecast"
    filename = f"{entity_code}_{park_date.strftime('%Y-%m-%d')}.csv"
    filepath = curves_dir / filename
    
    if not filepath.exists():
        return None
    
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception:
        return None


def get_actual_for_entity_slot(
    entity_code: str,
    park_date: date,
    time_slot: str,
    output_base: Path,
) -> Optional[float]:
    """
    Get ACTUAL wait time for an entity, date, and time slot.
    
    Priority:
    1. Backfill curve (observed or imputed)
    2. Forecast curve (predicted)
    3. None (no data available)
    
    Returns:
        ACTUAL wait time (minutes), or None if not available or closed
    """
    # Try backfill first (historical)
    df_backfill = load_backfill_curve(entity_code, park_date, output_base)
    if df_backfill is not None:
        slot_data = df_backfill[df_backfill["time_slot"] == time_slot]
        if not slot_data.empty:
            actual = slot_data.iloc[0]["actual"]
            if pd.notna(actual):
                return float(actual)
    
    # Try forecast (future)
    df_forecast = load_forecast_curve(entity_code, park_date, output_base)
    if df_forecast is not None:
        slot_data = df_forecast[df_forecast["time_slot"] == time_slot]
        if not slot_data.empty:
            actual = slot_data.iloc[0]["actual_predicted"]
            if pd.notna(actual):
                return float(actual)
    
    return None


# =============================================================================
# TIME SLOT GENERATION
# =============================================================================

def generate_time_slots(
    park_open_time: str,
    park_close_time: str,
) -> list[str]:
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
            slots.append(time_slot)
            
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
            slots.append(time_slot)
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    else:
        # Normal day: open to close
        while current_hour < close_hour or (current_hour == close_hour and current_min <= close_min):
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            slots.append(time_slot)
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    
    return slots


# =============================================================================
# WTI CALCULATION
# =============================================================================

def calculate_wti_for_park_date(
    park_code: str,
    park_date: date,
    entities: list[str],
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """
    Calculate WTI for a specific park and date.
    
    Returns:
        DataFrame with columns: park_code, park_date, time_slot, wti, n_entities, min_actual, max_actual
        Returns None if park hours not available
    """
    # Get park hours
    versioned_df = load_versioned_table(output_base)
    if versioned_df is None:
        if logger:
            logger.warning(f"Versioned park hours table not found for {park_code} {park_date}")
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
            logger.warning(f"No time slots generated for {park_code} {park_date}")
        return None
    
    # Filter entities to this park
    park_entities = [e for e in entities if e.startswith(park_code)]
    
    if not park_entities:
        if logger:
            logger.debug(f"No entities found for park {park_code}")
        return None
    
    # Calculate WTI for each time slot
    results = []
    for time_slot in time_slots:
        actuals = []
        
        # Collect ACTUAL values for all entities at this time slot
        for entity_code in park_entities:
            actual = get_actual_for_entity_slot(
                entity_code,
                park_date,
                time_slot,
                output_base,
            )
            if actual is not None:
                actuals.append(actual)
        
        # Calculate WTI (mean of non-null ACTUAL values)
        if actuals:
            wti = sum(actuals) / len(actuals)
            min_actual = min(actuals)
            max_actual = max(actuals)
            n_entities = len(actuals)
        else:
            # No data available for this time slot
            wti = None
            min_actual = None
            max_actual = None
            n_entities = 0
        
        results.append({
            "park_code": park_code,
            "park_date": park_date,
            "time_slot": time_slot,
            "wti": wti,
            "n_entities": n_entities,
            "min_actual": min_actual,
            "max_actual": max_actual,
        })
    
    return pd.DataFrame(results)


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Calculate Wait Time Index (WTI) for parks and dates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--output-base",
        type=str,
        help="Pipeline output base directory (default: from config/config.json)",
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD, default: earliest available curve date)",
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD, default: latest available curve date)",
    )
    
    parser.add_argument(
        "--park",
        type=str,
        help="Calculate WTI for specific park only (default: all parks)",
    )
    
    parser.add_argument(
        "--max-dates",
        type=int,
        help="Limit number of dates to process (for testing)",
    )
    
    args = parser.parse_args()
    
    # Get output base
    if args.output_base:
        base = Path(args.output_base)
    else:
        base = get_output_base()
    
    # Set up logging
    logger = setup_logging(base)
    logger.info("WTI Calculation")
    logger.info(f"Output base: {base}")
    
    # Load entities
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
    logger.info(f"Found {len(all_entities)} entities")
    
    # Get unique parks
    parks = set()
    for entity_code in all_entities:
        if len(entity_code) >= 2:
            parks.add(entity_code[:2])
    
    if args.park:
        parks = {args.park.upper()}
        logger.info(f"Processing single park: {args.park}")
    else:
        logger.info(f"Processing {len(parks)} parks: {sorted(parks)}")
    
    # Determine date range
    if args.start_date and args.end_date:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    else:
        # Scan curves to find date range
        logger.info("Scanning curves to determine date range...")
        backfill_dir = base / "curves" / "backfill"
        forecast_dir = base / "curves" / "forecast"
        
        dates = set()
        
        # Scan backfill
        if backfill_dir.exists():
            for csv_file in backfill_dir.glob("*.csv"):
                # Parse date from filename: {entity}_{YYYY-MM-DD}.csv
                parts = csv_file.stem.split("_")
                if len(parts) >= 4:  # entity_code may have underscores
                    date_str = "_".join(parts[-3:])  # YYYY-MM-DD
                    try:
                        dates.add(date.fromisoformat(date_str))
                    except ValueError:
                        pass
        
        # Scan forecast
        if forecast_dir.exists():
            for csv_file in forecast_dir.glob("*.csv"):
                parts = csv_file.stem.split("_")
                if len(parts) >= 4:
                    date_str = "_".join(parts[-3:])
                    try:
                        dates.add(date.fromisoformat(date_str))
                    except ValueError:
                        pass
        
        if dates:
            start_date = min(dates)
            end_date = max(dates)
            logger.info(f"Found date range: {start_date} to {end_date}")
        else:
            logger.error("No curves found. Run generate_backfill.py and/or generate_forecast.py first.")
            sys.exit(1)
    
    # Generate date range
    dates_list = []
    current = start_date
    while current <= end_date:
        dates_list.append(current)
        current += timedelta(days=1)
    
    if args.max_dates:
        dates_list = dates_list[:args.max_dates]
        logger.info(f"Limited to {len(dates_list)} dates")
    
    logger.info(f"Calculating WTI for {len(parks)} parks × {len(dates_list)} dates = {len(parks) * len(dates_list)} park-dates")
    
    # Process each park-date
    all_wti = []
    total_processed = 0
    total_saved = 0
    
    for park_code in sorted(parks):
        logger.info(f"Processing park: {park_code}")
        
        for park_date in dates_list:
            try:
                df_wti = calculate_wti_for_park_date(
                    park_code,
                    park_date,
                    all_entities,
                    base,
                    logger,
                )
                
                if df_wti is not None and len(df_wti) > 0:
                    all_wti.append(df_wti)
                    total_saved += 1
                
                total_processed += 1
                
            except Exception as e:
                logger.error(f"Error processing {park_code} {park_date}: {e}", exc_info=True)
                total_processed += 1
    
    # Combine and save
    if all_wti:
        logger.info("Combining WTI results...")
        df_combined = pd.concat(all_wti, ignore_index=True)
        
        # Save to Parquet
        wti_dir = base / "wti"
        wti_dir.mkdir(parents=True, exist_ok=True)
        wti_path = wti_dir / "wti.parquet"
        
        df_combined.to_parquet(wti_path, index=False)
        logger.info(f"Saved WTI to: {wti_path}")
        logger.info(f"  Total rows: {len(df_combined):,}")
        logger.info(f"  Date range: {df_combined['park_date'].min()} to {df_combined['park_date'].max()}")
        logger.info(f"  Parks: {sorted(df_combined['park_code'].unique())}")
        
        # Also save as CSV for easy inspection
        csv_path = wti_dir / "wti.csv"
        df_combined.to_csv(csv_path, index=False)
        logger.info(f"Also saved as CSV: {csv_path}")
    else:
        logger.warning("No WTI data generated")
    
    logger.info("")
    logger.info("WTI calculation complete")
    logger.info(f"  Total processed: {total_processed}")
    logger.info(f"  Total saved: {total_saved}")


if __name__ == "__main__":
    main()
