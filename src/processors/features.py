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

Future:
  - add_park_hours: Park hours features (needs dimparkhours → donor bridge)

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
    
    # Parse observed_at - pandas will preserve timezone from ISO 8601 string
    # If it has offset (e.g., -05:00), it becomes timezone-aware
    # If it has Z, we parse as UTC
    dt = pd.to_datetime(df[observed_at_col], errors="coerce")
    
    # If not timezone-aware, try to infer (shouldn't happen with our data)
    if dt.dt.tz is None:
        # Try parsing as UTC first
        dt = pd.to_datetime(df[observed_at_col], errors="coerce", utc=True)
    
    # Convert to timezone-aware if still not (fallback - shouldn't happen)
    if dt.dt.tz is None:
        # Default to Eastern (most parks)
        dt = dt.dt.tz_localize("America/New_York", ambiguous="infer", nonexistent="shift_forward")
    
    # Extract local time components (timezone is already in the datetime)
    hours = dt.dt.hour
    minutes = dt.dt.minute
    
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
# MAIN FEATURE ADDITION FUNCTION
# =============================================================================

def add_features(
    df: pd.DataFrame,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
    *,
    include_park_hours: bool = False,
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
    
    Future: add_park_hours (when dimparkhours bridge is ready)
    
    Args:
        df: DataFrame with columns: entity_code, observed_at, wait_time_type, wait_time_minutes
        output_base: Pipeline output base directory (for loading dimensions)
        logger: Optional logger
        include_park_hours: If True, add park hours features (not yet implemented)
    
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
    
    # Add features in order
    df = add_park_code(df)
    df = add_park_date(df)
    df = add_mins_since_6am(df)
    df = add_dategroupid(df, dims["dimdategroupid"], logger)
    df = add_season(df, dims["dimseason"], logger)
    df = add_geometric_decay(df)
    df = add_observed_wait_time(df)
    
    # Future: add_park_hours when ready
    if include_park_hours:
        if logger:
            logger.warning("add_park_hours not yet implemented; skipping")
    
    return df
