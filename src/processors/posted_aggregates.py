"""
Posted Aggregates Module

================================================================================
PURPOSE
================================================================================
Builds historical aggregates of POSTED wait times to enable predicted POSTED
for future dates. Predicted POSTED is used for:
  1. Live comparison: "We predicted POSTED = X, we observe POSTED = Y"
  2. Building trust: Show accuracy of predictions in real-time
  3. Live streaming content: Watch predictions perform in real-time

Aggregation strategy: (entity_code, dategroupid, hour) → weighted median/mean POSTED
Recency weighting: More recent dates weighted higher (1.0 / (1.0 + days_ago / 365.0))

================================================================================
USAGE
================================================================================
  from processors.posted_aggregates import build_posted_aggregates, get_predicted_posted
  
  # Build aggregates from historical fact data
  aggregates = build_posted_aggregates(output_base, logger)
  
  # Get predicted POSTED for a future date/time
  predicted_posted = get_predicted_posted(
      entity_code="MK101",
      park_date=date(2026, 6, 15),
      hour=14,  # 2 PM
      aggregates=aggregates,
      output_base=output_base,
  )

================================================================================
AGGREGATION STRATEGY
================================================================================
- **Grouping**: (entity_code, dategroupid, hour)
- **Aggregation**: Median POSTED (robust to outliers)
- **Fallback**: If no data for (entity, dategroupid, hour), try:
  1. (entity, dategroupid) → median across all hours
  2. (entity, hour) → median across all dategroupids
  3. (entity) → median across all dategroupids and hours
  4. (park_code, hour) → park-level median
  5. Return None if no data available

================================================================================
STORAGE
================================================================================
Aggregates are saved to `aggregates/posted_aggregates.parquet` for fast lookup.
Format: entity_code, dategroupid, hour, posted_median (weighted), posted_mean (weighted), 
        posted_median_unweighted, posted_mean_unweighted, posted_count, avg_recency_weight,
        min_park_date, max_park_date
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from zoneinfo import ZoneInfo

from processors.entity_index import load_entity_data
from processors.features import add_dategroupid, add_park_code, add_park_date
from utils import get_output_base


# =============================================================================
# BUILD AGGREGATES
# =============================================================================

def build_posted_aggregates(
    output_base: Path,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Build POSTED aggregates from historical fact data.
    
    Aggregates POSTED wait times by (entity_code, dategroupid, hour) → median.
    
    Args:
        output_base: Pipeline output base directory
        min_date: Minimum park_date to include (default: all available)
        max_date: Maximum park_date to include (default: today)
        logger: Optional logger
    
    Returns:
        DataFrame with columns: entity_code, dategroupid, hour, posted_median (weighted), 
        posted_mean (weighted), posted_median_unweighted, posted_mean_unweighted, 
        posted_count, avg_recency_weight, min_park_date, max_park_date
    """
    from processors.features import load_dims
    
    if logger:
        logger.info("Building POSTED aggregates from historical data...")
    
    # Load dimensions
    dims = load_dims(output_base, logger)
    dimdategroupid = dims.get("dimdategroupid")
    
    if dimdategroupid is None or dimdategroupid.empty:
        raise ValueError("dimdategroupid not available")
    
    # Load all fact data (or sample if too large)
    clean_dir = output_base / "fact_tables" / "clean"
    if not clean_dir.exists():
        if logger:
            logger.warning(f"Fact tables directory not found: {clean_dir}")
        return pd.DataFrame(columns=["entity_code", "dategroupid", "hour", "posted_median", "posted_mean", "posted_count"])
    
    # Collect POSTED data
    all_posted: list[pd.DataFrame] = []
    
    # Find all CSVs
    csvs = list(clean_dir.rglob("*.csv"))
    if logger:
        logger.info(f"Scanning {len(csvs)} fact table CSVs...")
    
    posted_count = 0
    processed_count = 0
    _logged_first_error = False

    for csv_path in csvs:
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            
            # Filter to POSTED only
            df_posted = df[df["wait_time_type"] == "POSTED"].copy()
            if df_posted.empty:
                continue
            
            posted_count += len(df_posted)
            
            # Add park_date and park_code
            df_posted = add_park_date(df_posted)
            df_posted = add_park_code(df_posted)
            
            # Filter by date range if specified
            if min_date or max_date:
                df_posted["park_date_obj"] = pd.to_datetime(df_posted["park_date"], errors="coerce").dt.date
                if min_date:
                    df_posted = df_posted[df_posted["park_date_obj"] >= min_date]
                if max_date:
                    df_posted = df_posted[df_posted["park_date_obj"] <= max_date]
                df_posted = df_posted.drop(columns=["park_date_obj"], errors="ignore")
            
            if df_posted.empty:
                continue
            
            # Add dategroupid
            df_posted = add_dategroupid(df_posted, dimdategroupid, logger)
            # Rename pred_dategroupid to dategroupid for consistency
            if "pred_dategroupid" in df_posted.columns:
                df_posted = df_posted.rename(columns={"pred_dategroupid": "dategroupid"})
            
            # Extract hour from observed_at
            observed_dt = pd.to_datetime(df_posted["observed_at"], errors="coerce", utc=True)
            df_posted["hour"] = observed_dt.dt.hour
            
            # Calculate recency weight (same formula as park hours donor)
            # Weight = 1.0 / (1.0 + days_ago / 365.0)
            # More recent dates get higher weight
            park_date_dt = pd.to_datetime(df_posted["park_date"], errors="coerce")
            today_dt = pd.Timestamp(date.today())
            days_ago = (today_dt - park_date_dt).dt.days
            df_posted["recency_weight"] = 1.0 / (1.0 + days_ago / 365.0)
            
            # Select columns
            keep_cols = ["entity_code", "dategroupid", "hour", "wait_time_minutes", "park_date", "recency_weight"]
            available_cols = [c for c in keep_cols if c in df_posted.columns]
            df_posted = df_posted[available_cols].copy()
            
            # Rename wait_time_minutes to posted
            df_posted = df_posted.rename(columns={"wait_time_minutes": "posted"})
            
            # Filter out nulls
            before_null_filter = len(df_posted)
            df_posted = df_posted[
                df_posted["posted"].notna() &
                df_posted["dategroupid"].notna() &
                df_posted["hour"].notna() &
                df_posted["recency_weight"].notna()
            ]
            
            if not df_posted.empty:
                all_posted.append(df_posted)
                processed_count += len(df_posted)
        
        except Exception as e:
            if logger:
                if not _logged_first_error:
                    _logged_first_error = True
                    logger.warning(
                        "First exception during posted aggregates (file=%s): %s",
                        csv_path,
                        e,
                        exc_info=True,
                    )
                else:
                    logger.debug(f"Error reading {csv_path}: {e}")
            continue
    
    if logger:
        logger.info(f"Found {posted_count:,} POSTED rows across all files")
        logger.info(f"After processing: {processed_count:,} rows")
    
    if not all_posted:
        if logger:
            logger.warning("No POSTED data found")
        return pd.DataFrame(columns=["entity_code", "dategroupid", "hour", "posted_median", "posted_mean", "posted_count"])
    
    # Combine all POSTED data
    combined = pd.concat(all_posted, ignore_index=True)
    
    if logger:
        logger.info(f"Combined {len(combined):,} POSTED observations")
    
    # Aggregate by (entity_code, dategroupid, hour) with recency weighting
    # Use weighted median and weighted mean
    def weighted_median(values: pd.Series, weights: pd.Series) -> float:
        """Calculate weighted median."""
        if len(values) == 0:
            return None
        # Sort by values
        sorted_df = pd.DataFrame({"value": values, "weight": weights}).sort_values("value")
        cumsum_weights = sorted_df["weight"].cumsum()
        total_weight = cumsum_weights.iloc[-1]
        median_idx = (cumsum_weights >= total_weight / 2).idxmax()
        return float(sorted_df.loc[median_idx, "value"])
    
    def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
        """Calculate weighted mean."""
        if len(values) == 0 or weights.sum() == 0:
            return None
        return float((values * weights).sum() / weights.sum())
    
    aggregates_list = []
    for (entity, dgid, hour), group in combined.groupby(["entity_code", "dategroupid", "hour"]):
        posted_values = group["posted"]
        weights = group["recency_weight"]
        
        # Calculate weighted statistics
        wgt_median = weighted_median(posted_values, weights)
        wgt_mean = weighted_mean(posted_values, weights)
        count = len(group)
        
        # Also calculate unweighted for comparison
        unweighted_median = float(posted_values.median()) if not posted_values.empty else None
        unweighted_mean = float(posted_values.mean()) if not posted_values.empty else None
        
        aggregates_list.append({
            "entity_code": entity,
            "dategroupid": dgid,
            "hour": hour,
            "posted_median": wgt_median,
            "posted_mean": wgt_mean,
            "posted_median_unweighted": unweighted_median,
            "posted_mean_unweighted": unweighted_mean,
            "posted_count": count,
            "avg_recency_weight": float(weights.mean()),
            "min_park_date": group["park_date"].min(),
            "max_park_date": group["park_date"].max(),
        })
    
    aggregates = pd.DataFrame(aggregates_list)
    
    # Sort
    aggregates = aggregates.sort_values(["entity_code", "dategroupid", "hour"]).reset_index(drop=True)
    
    if logger:
        logger.info(f"Built aggregates: {len(aggregates):,} (entity, dategroupid, hour) combinations")
        logger.info(f"  Entities: {aggregates['entity_code'].nunique()}")
        logger.info(f"  Dategroupids: {aggregates['dategroupid'].nunique()}")
        logger.info(f"  Hours: {aggregates['hour'].nunique()}")
        logger.info(f"  Using recency weighting: more recent dates weighted higher")
    
    return aggregates


def save_posted_aggregates(
    aggregates: pd.DataFrame,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Path:
    """
    Save POSTED aggregates to Parquet file.
    
    Args:
        aggregates: Aggregates DataFrame
        output_base: Pipeline output base directory
        logger: Optional logger
    
    Returns:
        Path to saved file
    """
    aggregates_dir = output_base / "aggregates"
    aggregates_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = aggregates_dir / "posted_aggregates.parquet"
    
    try:
        aggregates.to_parquet(output_path, index=False, engine="pyarrow")
        
        if logger:
            logger.info(f"Saved aggregates to {output_path}")
        
        return output_path
    except Exception as e:
        if logger:
            logger.error(f"Failed to save aggregates: {e}")
        raise


def load_posted_aggregates(
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """
    Load POSTED aggregates from Parquet file.
    
    Args:
        output_base: Pipeline output base directory
        logger: Optional logger
    
    Returns:
        Aggregates DataFrame or None if not found
    """
    aggregates_path = output_base / "aggregates" / "posted_aggregates.parquet"
    
    if not aggregates_path.exists():
        if logger:
            logger.debug("Posted aggregates file not found")
        return None
    
    try:
        aggregates = pd.read_parquet(aggregates_path, engine="pyarrow")
        
        if logger:
            logger.debug(f"Loaded aggregates: {len(aggregates):,} rows")
        
        return aggregates
    except Exception as e:
        if logger:
            logger.warning(f"Failed to load aggregates: {e}")
        return None


# =============================================================================
# GET PREDICTED POSTED
# =============================================================================

def get_predicted_posted(
    entity_code: str,
    park_date: date,
    hour: int,
    aggregates: Optional[pd.DataFrame] = None,
    output_base: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[float]:
    """
    Get predicted POSTED for a future date/time using historical aggregates.
    
    Uses fallback strategy if exact match not found:
    1. (entity, dategroupid, hour)
    2. (entity, dategroupid) → median across hours
    3. (entity, hour) → median across dategroupids
    4. (entity) → median across all
    5. (park_code, hour) → park-level
    6. None
    
    Args:
        entity_code: Entity code (e.g., "MK101")
        park_date: Park date
        hour: Hour of day (0-23)
        aggregates: Aggregates DataFrame (if None, loads from file)
        output_base: Pipeline output base directory (if aggregates is None)
        logger: Optional logger
    
    Returns:
        Predicted POSTED value or None if no data available
    """
    # Load aggregates if not provided
    if aggregates is None:
        if output_base is None:
            output_base = get_output_base()
        aggregates = load_posted_aggregates(output_base, logger)
        
        if aggregates is None or aggregates.empty:
            if logger:
                logger.warning("No aggregates available")
            return None
    
    # Get dategroupid for this date
    from processors.features import load_dims
    
    dims = load_dims(output_base, logger) if output_base else {}
    dimdategroupid = dims.get("dimdategroupid")
    
    dategroupid = None
    if dimdategroupid is not None and not dimdategroupid.empty:
        park_date_str = park_date.strftime("%Y-%m-%d")
        date_col = None
        for col in ["park_date", "date"]:
            if col in dimdategroupid.columns:
                date_col = col
                break
        
        if date_col:
            match = dimdategroupid[dimdategroupid[date_col] == park_date_str]
            if not match.empty:
                dgid_col = None
                for col in ["dategroupid", "date_group_id"]:
                    if col in dimdategroupid.columns:
                        dgid_col = col
                        break
                if dgid_col:
                    dategroupid = match.iloc[0][dgid_col]
    
    # Try exact match: (entity, dategroupid, hour)
    if dategroupid:
        exact_match = aggregates[
            (aggregates["entity_code"] == entity_code) &
            (aggregates["dategroupid"] == dategroupid) &
            (aggregates["hour"] == hour)
        ]
        if not exact_match.empty:
            return float(exact_match.iloc[0]["posted_median"])
    
    # Fallback 1: (entity, dategroupid) → median across hours
    if dategroupid:
        entity_dgid = aggregates[
            (aggregates["entity_code"] == entity_code) &
            (aggregates["dategroupid"] == dategroupid)
        ]
        if not entity_dgid.empty:
            return float(entity_dgid["posted_median"].median())
    
    # Fallback 2: (entity, hour) → median across dategroupids
    entity_hour = aggregates[
        (aggregates["entity_code"] == entity_code) &
        (aggregates["hour"] == hour)
    ]
    if not entity_hour.empty:
        return float(entity_hour["posted_median"].median())
    
    # Fallback 3: (entity) → median across all
    entity_all = aggregates[aggregates["entity_code"] == entity_code]
    if not entity_all.empty:
        return float(entity_all["posted_median"].median())
    
    # Fallback 4: (park_code, hour) → park-level
    park_code = entity_code[:2] if len(entity_code) >= 2 else None
    if park_code:
        # Get park_code from entity_code prefix
        park_hour = aggregates[
            (aggregates["entity_code"].str.startswith(park_code)) &
            (aggregates["hour"] == hour)
        ]
        if not park_hour.empty:
            return float(park_hour["posted_median"].median())
    
    # No data available
    if logger:
        logger.debug(f"No predicted POSTED available for {entity_code}, {park_date}, hour {hour}")
    
    return None


def get_predicted_posted_batch(
    entity_code: str,
    park_date: date,
    aggregates: Optional[pd.DataFrame] = None,
    output_base: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Get predicted POSTED for all hours of a day.
    
    Args:
        entity_code: Entity code
        park_date: Park date
        aggregates: Aggregates DataFrame (if None, loads from file)
        output_base: Pipeline output base directory (if aggregates is None)
        logger: Optional logger
    
    Returns:
        DataFrame with columns: hour, posted_predicted
    """
    results = []
    
    for hour in range(24):
        predicted = get_predicted_posted(
            entity_code,
            park_date,
            hour,
            aggregates=aggregates,
            output_base=output_base,
            logger=logger,
        )
        results.append({
            "hour": hour,
            "posted_predicted": predicted,
        })
    
    return pd.DataFrame(results)


def get_predicted_posted_5min_slots(
    entity_code: str,
    park_date: date,
    park_open_time: Optional[str] = None,
    park_close_time: Optional[str] = None,
    aggregates: Optional[pd.DataFrame] = None,
    output_base: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Get predicted POSTED for all 5-minute time slots in a day.
    
    Generates 5-minute intervals from park open to close, using hourly aggregates
    to fill in predicted POSTED for each slot.
    
    Args:
        entity_code: Entity code
        park_date: Park date
        park_open_time: Park opening time (HH:MM format, e.g., "09:00"). If None, uses park hours.
        park_close_time: Park closing time (HH:MM format, e.g., "22:00"). If None, uses park hours.
        aggregates: Aggregates DataFrame (if None, loads from file)
        output_base: Pipeline output base directory (if aggregates is None)
        logger: Optional logger
    
    Returns:
        DataFrame with columns: time_slot (HH:MM), hour, posted_predicted
    """
    # Get park hours if not provided
    if park_open_time is None or park_close_time is None:
        from processors.park_hours_versioning import get_park_hours_for_date, load_versioned_table
        
        if output_base is None:
            output_base = get_output_base()
        
        versioned_df = load_versioned_table(output_base)
        if versioned_df is not None:
            from datetime import datetime
            from zoneinfo import ZoneInfo
            hours = get_park_hours_for_date(
                park_date,
                entity_code[:2] if len(entity_code) >= 2 else "MK",
                versioned_df,
                as_of=datetime.now(ZoneInfo("UTC")),
                logger=logger,
            )
            if hours:
                if park_open_time is None:
                    park_open_time = hours.get("opening_time", "09:00")
                if park_close_time is None:
                    park_close_time = hours.get("closing_time", "22:00")
    
    # Default if still None
    if park_open_time is None:
        park_open_time = "09:00"
    if park_close_time is None:
        park_close_time = "22:00"
    
    # Parse times
    open_hour, open_min = map(int, park_open_time.split(":"))
    close_hour, close_min = map(int, park_close_time.split(":"))
    
    # Generate 5-minute slots
    results = []
    current_hour = open_hour
    current_min = open_min
    
    # Handle overnight (close < open)
    if close_hour < open_hour or (close_hour == open_hour and close_min < open_min):
        # Overnight: from open to midnight, then midnight to close
        # First part: open to 23:55
        while current_hour < 24:
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            predicted = get_predicted_posted(
                entity_code,
                park_date,
                current_hour,
                aggregates=aggregates,
                output_base=output_base,
                logger=logger,
            )
            results.append({
                "time_slot": time_slot,
                "hour": current_hour,
                "posted_predicted": predicted,
            })
            
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
            predicted = get_predicted_posted(
                entity_code,
                park_date,
                current_hour,
                aggregates=aggregates,
                output_base=output_base,
                logger=logger,
            )
            results.append({
                "time_slot": time_slot,
                "hour": current_hour,
                "posted_predicted": predicted,
            })
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    else:
        # Normal day: open to close
        while current_hour < close_hour or (current_hour == close_hour and current_min <= close_min):
            time_slot = f"{current_hour:02d}:{current_min:02d}"
            predicted = get_predicted_posted(
                entity_code,
                park_date,
                current_hour,
                aggregates=aggregates,
                output_base=output_base,
                logger=logger,
            )
            results.append({
                "time_slot": time_slot,
                "hour": current_hour,
                "posted_predicted": predicted,
            })
            
            current_min += 5
            if current_min >= 60:
                current_min = 0
                current_hour += 1
    
    return pd.DataFrame(results)
