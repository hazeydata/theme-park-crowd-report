"""
Feature Engineering Module

================================================================================
PURPOSE
================================================================================
Adds modeling features to fact table rows. Transforms raw fact data into
feature-rich DataFrames ready for model training and inference.

Features added:
  - pred_mins_since_6am: Minutes since 6am from observed_at
  - pred_dategroupid: Date group ID (join to dimdategroupid)
  - pred_season, pred_season_year: Season and season year (join to dimseason)
  - wgt_geo_decay: Geometric decay weight for training (0.5^(days_since_observed/730))
  - observed_wait_time: Target variable (from wait_time_minutes)
  - park_date: Operational date (6am rule)
  - park_code: Derived from entity_code prefix
  - pred_mins_since_park_open: Minutes since park opening time
  - pred_park_open_hour: Opening hour (0-23)
  - pred_park_close_hour: Closing hour (0-23)
  - pred_park_hours_open: Hours the park is open
  - pred_emh_morning: True if morning Extra Magic Hours
  - pred_emh_evening: True if evening Extra Magic Hours

================================================================================
USAGE
================================================================================
  from processors.features import add_features
  
  # Load entity data
  df = load_entity_data(entity_code, output_base, index_db)
  
  # Add features
  df_features = add_features(df, output_base)
  
  # Now ready for encoding and modeling

================================================================================
ENCODING (separate step - not yet implemented)
================================================================================
Encoding (categorical → numeric for ML) is a separate step after features.
This module outputs clean features with categorical columns (pred_dategroupid,
pred_season, pred_season_year, park_code, entity_code). Encoding will happen
in a separate module (e.g., `processors/encoding.py`) before training.

This separation:
  - Keeps features pure (transformations only)
  - Allows flexible encoding strategies (one-hot, label, target, etc.)
  - Makes testing easier (test features independently)
  - Follows attraction-io pattern: features → premodelling → encode → train
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo

# Import shared utilities
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

from get_tp_wait_time_data_from_s3 import PARK_CODE_MAP, derive_park_date, get_park_code

# Park code to timezone mapping
PARK_TIMEZONE_MAP = {
    # WDW parks (Eastern)
    "mk": "America/New_York",
    "ep": "America/New_York",
    "hs": "America/New_York",
    "ak": "America/New_York",
    # DLR parks (Pacific)
    "dl": "America/Los_Angeles",
    "ca": "America/Los_Angeles",
    # UOR parks (Eastern)
    "ia": "America/New_York",
    "uf": "America/New_York",
    "eu": "America/New_York",
    # USH (Pacific)
    "uh": "America/Los_Angeles",
    # TDR parks (Tokyo)
    "tdl": "Asia/Tokyo",
    "tds": "Asia/Tokyo",
}


# =============================================================================
# LOAD DIMENSIONS
# =============================================================================

def load_dims(output_base: Path, logger: Optional[logging.Logger] = None) -> dict:
    """
    Load dimension tables needed for feature engineering.
    
    Returns:
        dict with keys: dimdategroupid, dimseason
    """
    dim_dir = output_base / "dimension_tables"
    dims = {}
    
    # Load dimdategroupid
    dg_path = dim_dir / "dimdategroupid.csv"
    if dg_path.exists():
        try:
            dims["dimdategroupid"] = pd.read_csv(dg_path, low_memory=False)
            if logger:
                logger.debug(f"Loaded dimdategroupid: {len(dims['dimdategroupid'])} rows")
        except Exception as e:
            if logger:
                logger.warning(f"Could not load dimdategroupid: {e}")
            dims["dimdategroupid"] = None
    else:
        if logger:
            logger.warning(f"dimdategroupid not found: {dg_path}")
        dims["dimdategroupid"] = None
    
    # Load dimseason
    season_path = dim_dir / "dimseason.csv"
    if season_path.exists():
        try:
            dims["dimseason"] = pd.read_csv(season_path, low_memory=False)
            if logger:
                logger.debug(f"Loaded dimseason: {len(dims['dimseason'])} rows")
        except Exception as e:
            if logger:
                logger.warning(f"Could not load dimseason: {e}")
            dims["dimseason"] = None
    else:
        if logger:
            logger.warning(f"dimseason not found: {season_path}")
        dims["dimseason"] = None
    
    return dims


# =============================================================================
# FEATURE FUNCTIONS
# =============================================================================

def add_mins_since_6am(df: pd.DataFrame, observed_at_col: str = "observed_at") -> pd.DataFrame:
    """
    Add pred_mins_since_6am: minutes since 6am from observed_at.
    
    Formula: (hour - 6) * 60 + minute
    If hour < 6, add 1440 (previous day's minutes)
    
    Uses the timezone from observed_at (ISO 8601 with offset). Each entity belongs
    to one park, so all rows should have the same timezone.
    
    Args:
        df: DataFrame with observed_at column (ISO 8601 with timezone offset)
        observed_at_col: Name of observed_at column (default: "observed_at")
    
    Returns:
        DataFrame with pred_mins_since_6am column added
    """
    df = df.copy()
    
    # Parse observed_at - convert to UTC to handle mixed timezones
    # This avoids the pandas warning and ensures consistent datetime dtype
    dt_utc = pd.to_datetime(df[observed_at_col], errors="coerce", utc=True)
    
    # Check if parsing succeeded
    if dt_utc.isna().all():
        raise ValueError(f"Failed to parse {observed_at_col} as datetime. Sample values: {df[observed_at_col].head(3).tolist()}")
    
    # Get park code from entity_code (first 2 characters) to determine timezone
    # All rows for an entity should be from the same park
    if "entity_code" in df.columns and len(df) > 0:
        park_code = df["entity_code"].iloc[0][:2].lower()
        park_tz = PARK_TIMEZONE_MAP.get(park_code, "America/New_York")
    else:
        # Fallback to Eastern if we can't determine park
        park_tz = "America/New_York"
    
    # Convert UTC to park's local timezone to get local time components
    dt_local = dt_utc.dt.tz_convert(park_tz)
    
    # Extract local time components
    hours = dt_local.dt.hour
    minutes = dt_local.dt.minute
    
    # Calculate minutes since 6am
    mins_since_6am = (hours - 6) * 60 + minutes
    
    # If hour < 6, add 1440 (previous day)
    mask = hours < 6
    if mask.any():
        mins_since_6am.loc[mask] = mins_since_6am[mask] + 1440
    
    df["pred_mins_since_6am"] = mins_since_6am.astype("Int64")  # Nullable integer
    return df


def add_park_date(df: pd.DataFrame, observed_at_col: str = "observed_at") -> pd.DataFrame:
    """
    Add park_date: operational date using 6am rule in park timezone.
    
    Uses timezone from observed_at (ISO 8601 with offset). Each entity belongs
    to one park, so all rows should have the same timezone.
    
    Args:
        df: DataFrame with observed_at column (ISO 8601 with timezone offset)
        observed_at_col: Name of observed_at column
    
    Returns:
        DataFrame with park_date column added (YYYY-MM-DD string)
    """
    df = df.copy()
    
    # Derive park from entity_code if not present
    if "park_code" not in df.columns:
        df["park_code"] = get_park_code(df["entity_code"])
    
    # Parse observed_at to extract timezone
    # observed_at is ISO 8601 with offset (e.g., "2024-01-15T10:30:00-05:00")
    # Parse as UTC first, then we'll extract the timezone
    dt_parsed = pd.to_datetime(df[observed_at_col], errors="coerce", utc=True)
    
    # Get timezone from first row (all rows for same entity should have same TZ)
    # The timezone is embedded in the ISO string, so we need to parse it properly
    if len(df) > 0:
        # Parse first row to get timezone
        first_str = str(df[observed_at_col].iloc[0])
        # Try to extract timezone from string or use parsed datetime's timezone
        if dt_parsed.iloc[0].tz is not None:
            # Convert to that timezone to get the local timezone info
            tz = dt_parsed.iloc[0].tz
            # derive_park_date expects a ZoneInfo, so convert
            if hasattr(tz, 'key'):
                tz_info = ZoneInfo(tz.key)
            else:
                # Fallback: use Eastern
                tz_info = ZoneInfo("America/New_York")
        else:
            # Fallback: use Eastern (most parks)
            tz_info = ZoneInfo("America/New_York")
    else:
        tz_info = ZoneInfo("America/New_York")
    
    df["park_date"] = derive_park_date(df[observed_at_col], tz_info)
    return df


def add_dategroupid(
    df: pd.DataFrame,
    dimdategroupid: Optional[pd.DataFrame],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add pred_dategroupid: join to dimdategroupid on park_date.
    
    Args:
        df: DataFrame with park_date column
        dimdategroupid: dimdategroupid DataFrame (park_date, date_group_id, ...)
        logger: Optional logger
    
    Returns:
        DataFrame with pred_dategroupid column added
    """
    df = df.copy()
    
    if dimdategroupid is None or dimdategroupid.empty:
        if logger:
            logger.warning("dimdategroupid not available; pred_dategroupid will be null")
        df["pred_dategroupid"] = None
        return df
    
    # Find date column in dimdategroupid
    date_col = None
    for col in ["park_date", "date", "park_day_id"]:
        if col in dimdategroupid.columns:
            date_col = col
            break
    
    if date_col is None:
        if logger:
            logger.warning("dimdategroupid missing date column; pred_dategroupid will be null")
        df["pred_dategroupid"] = None
        return df
    
    # Find date_group_id column
    dgid_col = None
    for col in ["date_group_id", "dategroupid", "date_group"]:
        if col in dimdategroupid.columns:
            dgid_col = col
            break
    
    if dgid_col is None:
        if logger:
            logger.warning("dimdategroupid missing date_group_id column; pred_dategroupid will be null")
        df["pred_dategroupid"] = None
        return df
    
    # Normalize dates for join
    dim = dimdategroupid.copy()
    dim["_park_date_norm"] = pd.to_datetime(dim[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_park_date_norm"] = pd.to_datetime(df["park_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Left join
    merged = df.merge(
        dim[[dgid_col, "_park_date_norm"]],
        on="_park_date_norm",
        how="left",
    )
    df["pred_dategroupid"] = merged[dgid_col]
    
    # Cleanup temp column
    df = df.drop(columns=["_park_date_norm"], errors="ignore")
    
    return df


def add_season(
    df: pd.DataFrame,
    dimseason: Optional[pd.DataFrame],
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add pred_season and pred_season_year: join to dimseason on park_date.
    
    Args:
        df: DataFrame with park_date column
        dimseason: dimseason DataFrame (park_date, season, season_year)
        logger: Optional logger
    
    Returns:
        DataFrame with pred_season and pred_season_year columns added
    """
    df = df.copy()
    
    if dimseason is None or dimseason.empty:
        if logger:
            logger.warning("dimseason not available; pred_season and pred_season_year will be null")
        df["pred_season"] = None
        df["pred_season_year"] = None
        return df
    
    # Find date column
    date_col = None
    for col in ["park_date", "date", "park_day_id"]:
        if col in dimseason.columns:
            date_col = col
            break
    
    if date_col is None:
        if logger:
            logger.warning("dimseason missing date column; pred_season will be null")
        df["pred_season"] = None
        df["pred_season_year"] = None
        return df
    
    # Normalize dates for join
    dim = dimseason.copy()
    dim["_park_date_norm"] = pd.to_datetime(dim[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_park_date_norm"] = pd.to_datetime(df["park_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Left join
    merged = df.merge(
        dim[["season", "season_year", "_park_date_norm"]],
        on="_park_date_norm",
        how="left",
    )
    df["pred_season"] = merged["season"]
    df["pred_season_year"] = merged["season_year"]
    
    # Cleanup temp column
    df = df.drop(columns=["_park_date_norm"], errors="ignore")
    
    return df


def add_geometric_decay(
    df: pd.DataFrame,
    observed_at_col: str = "observed_at",
    reference_date: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add wgt_geo_decay: geometric decay weight for training.
    
    Formula: 0.5^(days_since_observed / 730)
    - More recent observations get higher weight
    - 730 days = half-life (weight = 0.5 after 2 years)
    
    Args:
        df: DataFrame with observed_at column
        observed_at_col: Name of observed_at column
        reference_date: Reference date for decay calculation (default: now in UTC)
        logger: Optional logger
    
    Returns:
        DataFrame with wgt_geo_decay column added
    """
    df = df.copy()
    
    if reference_date is None:
        reference_date = datetime.now(ZoneInfo("UTC"))
    
    # Parse observed_at (preserve timezone from string)
    observed_dt = pd.to_datetime(df[observed_at_col], errors="coerce", utc=True)
    
    # Calculate days since observed
    # Convert reference_date to UTC timestamp
    if isinstance(reference_date, datetime) and reference_date.tzinfo is not None:
        reference_ts = pd.Timestamp(reference_date).tz_convert("UTC")
    else:
        reference_ts = pd.Timestamp(reference_date, tz="UTC")
    
    days_since = (reference_ts - observed_dt).dt.total_seconds() / (24 * 3600)
    
    # Geometric decay: 0.5^(days / 730)
    df["wgt_geo_decay"] = 0.5 ** (days_since / 730.0)
    
    # Handle nulls
    df["wgt_geo_decay"] = df["wgt_geo_decay"].fillna(0.0)
    
    return df


def add_park_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add park_code: derived from entity_code prefix.
    
    Args:
        df: DataFrame with entity_code column
    
    Returns:
        DataFrame with park_code column added
    """
    df = df.copy()
    if "park_code" not in df.columns:
        df["park_code"] = get_park_code(df["entity_code"])
    return df


def _parse_park_time(time_str, park_date_str, park_tz_str):
    """
    Parse time string (ISO8601 or HH:MM) to datetime in park timezone.
    
    Helper function for add_park_hours.
    """
    if pd.isna(time_str) or time_str is None:
        return None
    
    time_str = str(time_str).strip()
    if not time_str:
        return None
    
    # Try parsing as ISO8601 datetime first
    try:
        dt = pd.to_datetime(time_str, errors="raise")
        if dt.tz is not None:
            # Already timezone-aware, convert to park timezone
            return dt
        # Not timezone-aware, assume it's in park timezone
        tz = ZoneInfo(park_tz_str)
        return dt.tz_localize(tz)
    except (ValueError, TypeError):
        pass
    
    # Try parsing as HH:MM or HH:MM:SS
    try:
        parts = time_str.split(":")
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                # Combine with park_date
                park_date = pd.to_datetime(park_date_str)
                tz = ZoneInfo(park_tz_str)
                return pd.Timestamp(
                    year=park_date.year,
                    month=park_date.month,
                    day=park_date.day,
                    hour=hour,
                    minute=minute,
                    tz=tz,
                )
    except (ValueError, TypeError, IndexError):
        pass
    
    return None


def add_observed_wait_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add observed_wait_time: target variable from wait_time_minutes.
    
    This is a simple rename/alias for modeling clarity.
    
    Args:
        df: DataFrame with wait_time_minutes column
    
    Returns:
        DataFrame with observed_wait_time column added
    """
    df = df.copy()
    df["observed_wait_time"] = df["wait_time_minutes"]
    return df


# =============================================================================
# PARK HOURS FEATURES
# =============================================================================

def _parse_park_time(time_str, park_date_str, park_tz_str):
    """
    Parse time string (ISO8601 or HH:MM) to datetime in park timezone.
    
    Helper function for add_park_hours.
    """
    if pd.isna(time_str) or time_str is None:
        return None
    
    time_str = str(time_str).strip()
    if not time_str:
        return None
    
    # Try parsing as ISO8601 datetime first
    try:
        dt = pd.to_datetime(time_str, errors="raise")
        if dt.tz is not None:
            # Already timezone-aware, convert to park timezone
            return dt
        # Not timezone-aware, assume it's in park timezone
        tz = ZoneInfo(park_tz_str)
        return dt.tz_localize(tz)
    except (ValueError, TypeError):
        pass
    
    # Try parsing as HH:MM or HH:MM:SS
    try:
        parts = time_str.split(":")
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                # Combine with park_date
                park_date = pd.to_datetime(park_date_str)
                tz = ZoneInfo(park_tz_str)
                return pd.Timestamp(
                    year=park_date.year,
                    month=park_date.month,
                    day=park_date.day,
                    hour=hour,
                    minute=minute,
                    tz=tz,
                )
    except (ValueError, TypeError, IndexError):
        pass
    
    return None


def add_park_hours(
    df: pd.DataFrame,
    dimparkhours: Optional[pd.DataFrame],
    output_base: Optional[Path] = None,
    as_of: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Add park hours features: mins_since_park_open, park open/close hours, EMH flags.
    
    Uses versioned park hours table if available (dimparkhours_with_donor.csv), otherwise
    falls back to flat dimparkhours.csv. The versioned table provides:
      - Official hours (from S3 sync)
      - Predicted hours (from donor day imputation)
      - Change tracking and confidence scores
    
    Joins to park hours on (park_date, park_code) and calculates:
      - pred_mins_since_park_open: Minutes since park opening time
      - pred_park_open_hour: Opening hour (0-23)
      - pred_park_close_hour: Closing hour (0-23)
      - pred_park_hours_open: Hours the park is open
      - pred_emh_morning: True if morning EMH
      - pred_emh_evening: True if evening EMH
    
    Args:
        df: DataFrame with park_date, park_code, observed_at columns
        dimparkhours: Flat dimparkhours DataFrame (fallback if versioned table not available)
        output_base: Pipeline output base directory (for loading versioned table)
        as_of: Timestamp for version selection (default: now)
        logger: Optional logger
    
    Returns:
        DataFrame with park hours features added
    """
    df = df.copy()
    
    # Try to use versioned table first
    versioned_df = None
    use_versioned = False
    if output_base is not None:
        try:
            from processors.park_hours_versioning import (
                get_park_hours_for_date,
                load_versioned_table,
            )
            versioned_df = load_versioned_table(output_base)
            if versioned_df is not None and not versioned_df.empty:
                use_versioned = True
                if logger:
                    logger.debug("Using versioned park hours table")
        except ImportError:
            pass
        except Exception as e:
            if logger:
                logger.debug(f"Could not load versioned table: {e}, using flat table")
    
    # Use versioned table if available
    if use_versioned and versioned_df is not None:
        if as_of is None:
            as_of = datetime.now(ZoneInfo("UTC"))
        
        # Get park timezone from park_code
        if len(df) > 0 and "park_code" in df.columns:
            first_park_code = str(df["park_code"].iloc[0]).lower().strip()
            park_tz_str = PARK_TIMEZONE_MAP.get(first_park_code, "America/New_York")
        else:
            park_tz_str = "America/New_York"
        
        # Get park hours for each unique (park_date, park_code) combination
        unique_dates = df[["park_date", "park_code"]].drop_duplicates()
        hours_dict = {}
        
        for _, row in unique_dates.iterrows():
            park_date = pd.to_datetime(row["park_date"], errors="coerce").date()
            if pd.isna(park_date):
                continue
            park_code = str(row["park_code"]).upper().strip()
            
            hours = get_park_hours_for_date(
                park_date,
                park_code,
                versioned_df,
                as_of=as_of,
                logger=logger,
            )
            if hours:
                hours_dict[(park_date, park_code)] = hours
        
        # Merge hours into df
        def get_hours(row):
            park_date = pd.to_datetime(row["park_date"], errors="coerce").date()
            if pd.isna(park_date):
                return None
            park_code = str(row["park_code"]).upper().strip()
            return hours_dict.get((park_date, park_code))
        
        df["_park_hours"] = df.apply(get_hours, axis=1)
        
        # Extract and parse times
        df["_opening_time_str"] = df["_park_hours"].apply(
            lambda h: h.get("opening_time") if h else None
        )
        df["_closing_time_str"] = df["_park_hours"].apply(
            lambda h: h.get("closing_time") if h else None
        )
        df["pred_emh_morning"] = df["_park_hours"].apply(
            lambda h: h.get("emh_morning", False) if h else False
        )
        df["pred_emh_evening"] = df["_park_hours"].apply(
            lambda h: h.get("emh_evening", False) if h else False
        )
        
        # Parse times
        opening_dt = df.apply(
            lambda row: _parse_park_time(row["_opening_time_str"], row["park_date"], park_tz_str),
            axis=1,
        )
        closing_dt = df.apply(
            lambda row: _parse_park_time(row["_closing_time_str"], row["park_date"], park_tz_str),
            axis=1,
        )
        
        # Calculate features
        observed_dt = pd.to_datetime(df["observed_at"], errors="coerce", utc=True)
        if observed_dt.dt.tz is None:
            observed_dt = observed_dt.dt.tz_localize("UTC")
        observed_dt = observed_dt.dt.tz_convert(park_tz_str)
        
        mins_since_open = (observed_dt - opening_dt).dt.total_seconds() / 60.0
        df["pred_mins_since_park_open"] = mins_since_open.astype("Float64")
        df["pred_park_open_hour"] = opening_dt.dt.hour.astype("Int64")
        df["pred_park_close_hour"] = closing_dt.dt.hour.astype("Int64")
        
        # Calculate hours_open (handle overnight)
        hours_open = (closing_dt - opening_dt).dt.total_seconds() / 3600.0
        mask_overnight = closing_dt < opening_dt
        if mask_overnight.any():
            hours_open.loc[mask_overnight] = hours_open.loc[mask_overnight] + 24.0
        df["pred_park_hours_open"] = hours_open.astype("Float64")
        
        # Cleanup temp columns
        df = df.drop(columns=["_park_hours", "_opening_time_str", "_closing_time_str"], errors="ignore")
        
        return df
    
    # Fallback to flat dimparkhours if versioned table not available
    if dimparkhours is None or dimparkhours.empty:
        if logger:
            logger.warning("dimparkhours not available; park hours features will be null")
        df["pred_mins_since_park_open"] = None
        df["pred_park_open_hour"] = None
        df["pred_park_close_hour"] = None
        df["pred_park_hours_open"] = None
        df["pred_emh_morning"] = False
        df["pred_emh_evening"] = False
        return df
    
    # Find columns in dimparkhours
    date_col = None
    for col in ["park_date", "date", "park_day_id"]:
        if col in dimparkhours.columns:
            date_col = col
            break
    
    park_col = None
    for col in ["park_code", "park", "code"]:
        if col in dimparkhours.columns:
            park_col = col
            break
    
    open_col = None
    for col in ["opening_time", "open", "open_time", "open_time_1"]:
        if col in dimparkhours.columns:
            open_col = col
            break
    
    close_col = None
    for col in ["closing_time", "close", "close_time", "close_time_1"]:
        if col in dimparkhours.columns:
            close_col = col
            break
    
    if not date_col or not park_col or not open_col or not close_col:
        if logger:
            logger.warning("dimparkhours missing required columns; park hours features will be null")
        df["pred_mins_since_park_open"] = None
        df["pred_park_open_hour"] = None
        df["pred_park_close_hour"] = None
        df["pred_park_hours_open"] = None
        df["pred_emh_morning"] = False
        df["pred_emh_evening"] = False
        return df
    
    # Normalize dates and park codes for join
    dim = dimparkhours.copy()
    dim["_park_date_norm"] = pd.to_datetime(dim[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    dim["_park_code_norm"] = dim[park_col].astype(str).str.strip().str.upper()
    df["_park_date_norm"] = pd.to_datetime(df["park_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_park_code_norm"] = df["park_code"].astype(str).str.strip().str.upper()
    
    # Join to dimparkhours
    merge_cols = ["_park_date_norm", "_park_code_norm"]
    keep_cols = [open_col, close_col]
    
    # Add EMH columns if present
    emh_morning_col = None
    emh_evening_col = None
    for col in ["emh_morning", "emh_am", "early_magic_hours_morning"]:
        if col in dim.columns:
            emh_morning_col = col
            keep_cols.append(col)
            break
    
    for col in ["emh_evening", "emh_pm", "early_magic_hours_evening"]:
        if col in dim.columns:
            emh_evening_col = col
            keep_cols.append(col)
            break
    
    merged = df.merge(
        dim[merge_cols + keep_cols],
        on=merge_cols,
        how="left",
    )
    
    # Get park timezone from park_code
    # Each entity belongs to one park, so all rows should have same park_code
    if len(df) > 0 and "park_code" in df.columns:
        first_park_code = str(df["park_code"].iloc[0]).lower().strip()
        park_tz_str = PARK_TIMEZONE_MAP.get(first_park_code, "America/New_York")
    else:
        park_tz_str = "America/New_York"  # Default
    
    # Parse opening and closing times
    opening_dt = merged.apply(
        lambda row: _parse_park_time(row[open_col], row["_park_date_norm"], park_tz_str),
        axis=1,
    )
    closing_dt = merged.apply(
        lambda row: _parse_park_time(row[close_col], row["_park_date_norm"], park_tz_str),
        axis=1,
    )
    
    # Calculate pred_mins_since_park_open
    observed_dt = pd.to_datetime(df["observed_at"], errors="coerce", utc=True)
    if observed_dt.dt.tz is None:
        observed_dt = observed_dt.dt.tz_localize("UTC")
    observed_dt = observed_dt.dt.tz_convert(park_tz_str)
    
    mins_since_open = (observed_dt - opening_dt).dt.total_seconds() / 60.0
    df["pred_mins_since_park_open"] = mins_since_open.astype("Float64")  # Nullable float
    
    # Extract hours
    df["pred_park_open_hour"] = opening_dt.dt.hour.astype("Int64")
    df["pred_park_close_hour"] = closing_dt.dt.hour.astype("Int64")
    
    # Calculate hours_open (handle overnight)
    hours_open = (closing_dt - opening_dt).dt.total_seconds() / 3600.0
    # If closing is before opening (overnight), add 24 hours
    mask_overnight = closing_dt < opening_dt
    if mask_overnight.any():
        hours_open.loc[mask_overnight] = hours_open.loc[mask_overnight] + 24.0
    df["pred_park_hours_open"] = hours_open.astype("Float64")
    
    # EMH flags
    if emh_morning_col:
        df["pred_emh_morning"] = merged[emh_morning_col].fillna(False).astype(bool)
    else:
        df["pred_emh_morning"] = False
    
    if emh_evening_col:
        df["pred_emh_evening"] = merged[emh_evening_col].fillna(False).astype(bool)
    else:
        df["pred_emh_evening"] = False
    
    # Cleanup temp columns
    df = df.drop(columns=["_park_date_norm", "_park_code_norm"], errors="ignore")
    
    return df


def add_features(
    df: pd.DataFrame,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
    *,
    include_park_hours: bool = True,
) -> pd.DataFrame:
    """
    Add all features to a fact DataFrame.
    
    This is the main entry point for feature engineering. It:
    1. Adds park_date (6am rule)
    2. Adds park_code (from entity_code)
    3. Adds pred_mins_since_6am
    4. Joins to dimdategroupid → pred_dategroupid
    5. Joins to dimseason → pred_season, pred_season_year
    6. Adds wgt_geo_decay
    7. Adds observed_wait_time (target)
    8. Adds park hours features (if include_park_hours=True)
    
    Args:
        df: DataFrame with columns: entity_code, observed_at, wait_time_type, wait_time_minutes
        output_base: Pipeline output base directory (for loading dimensions)
        logger: Optional logger
        include_park_hours: If True, add park hours features (default: True)
    
    Returns:
        DataFrame with all features added (pred_*, wgt_geo_decay, observed_wait_time, park_date, park_code)
    """
    if df.empty:
        return df
    
    # Validate required columns
    required = ["entity_code", "observed_at", "wait_time_minutes"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    # Load dimensions
    dims = load_dims(output_base, logger)
    
    # Load dimparkhours if needed
    dimparkhours = None
    if include_park_hours:
        dim_dir = output_base / "dimension_tables"
        park_hours_path = dim_dir / "dimparkhours.csv"
        if park_hours_path.exists():
            try:
                dimparkhours = pd.read_csv(park_hours_path, low_memory=False)
                if logger:
                    logger.debug(f"Loaded dimparkhours: {len(dimparkhours)} rows")
            except Exception as e:
                if logger:
                    logger.warning(f"Could not load dimparkhours: {e}")
        else:
            if logger:
                logger.warning(f"dimparkhours not found: {park_hours_path}")
    
    # Add features in order
    df = add_park_code(df)
    df = add_park_date(df)
    df = add_mins_since_6am(df)
    df = add_dategroupid(df, dims["dimdategroupid"], logger)
    df = add_season(df, dims["dimseason"], logger)
    df = add_geometric_decay(df)
    df = add_observed_wait_time(df)
    
    # Add park hours features
    if include_park_hours:
        df = add_park_hours(df, dimparkhours, output_base=output_base, logger=logger)
    
    return df
