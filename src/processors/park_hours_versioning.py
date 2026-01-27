"""
Park Hours Versioning Module

================================================================================
PURPOSE
================================================================================
Manages versioned park hours with support for:
  - Official hours (from S3 sync)
  - Predicted hours (from donor day imputation)
  - Change tracking (detect when official hours change)
  - Temporal queries (get hours valid at a specific time)

This module handles the versioned park hours table and provides functions
for querying, updating, and creating predicted versions.

================================================================================
USAGE
================================================================================
  from processors.park_hours_versioning import (
      get_park_hours_for_date,
      build_park_hours_lookup_table,
      create_official_version,
      create_predicted_version_from_donor,
  )
  
  # Get hours for a date (uses best available version)
  hours = get_park_hours_for_date(park_date, park_code, dimparkhours_versioned, as_of=now)
  
  # Batch: build (park_date, park_code) -> hours lookup for many keys at once
  # Park hours are per park, not per entity; use this for feature engineering.
  lookup = build_park_hours_lookup_table(versioned_df, keys_df, as_of=now)
  
  # Create official version when syncing from S3
  create_official_version(park_date, park_code, opening_time, closing_time, ...)
  
  # Create predicted version from donor day
  create_predicted_version_from_donor(target_date, target_park, donor_date, ...)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo

# Import shared utilities
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))


# =============================================================================
# CONSTANTS
# =============================================================================

VERSIONED_TABLE_NAME = "dimparkhours_with_donor.csv"

VERSION_TYPES = {
    "official": 1,  # Priority 1: highest
    "final": 2,     # Priority 2: finalized official
    "predicted": 3, # Priority 3: predicted/imputed
    "historical": 4, # Priority 4: past actual
}

# Days before date when official hours are considered "final" (unlikely to change)
FINAL_DAYS_THRESHOLD = 7

# Default for blank datetime columns (Pacific UTC-8). Ensures opening/closing_time are never null.
DEFAULT_DATETIME_BLANK = "1999-01-01T00:00:00-08:00"


# =============================================================================
# LOAD VERSIONED TABLE
# =============================================================================

def load_versioned_table(output_base: Path) -> Optional[pd.DataFrame]:
    """
    Load the versioned park hours table.
    
    Args:
        output_base: Pipeline output base directory
    
    Returns:
        DataFrame with versioned park hours, or None if not found
    """
    dim_dir = output_base / "dimension_tables"
    path = dim_dir / VERSIONED_TABLE_NAME
    
    if not path.exists():
        return None
    
    try:
        df = pd.read_csv(path, low_memory=False)
        # Parse timestamps
        for col in ["created_at", "valid_from", "valid_until"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    except Exception as e:
        logging.warning(f"Could not load versioned park hours: {e}")
        return None


# =============================================================================
# QUERY FUNCTIONS
# =============================================================================

def get_park_hours_for_date(
    park_date: date,
    park_code: str,
    versioned_df: pd.DataFrame,
    as_of: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[dict]:
    """
    Get park hours for a given date, using the best available version as of 'as_of'.
    
    Priority:
    1. Official (if valid and not expired)
    2. Final (if date is within FINAL_DAYS_THRESHOLD or past)
    3. Predicted (if no official and date is future)
    4. Historical (if date is past)
    
    Args:
        park_date: Park operational date
        park_code: Park code (MK, EP, etc.)
        versioned_df: Versioned park hours DataFrame
        as_of: Timestamp for version selection (default: now in UTC)
        logger: Optional logger
    
    Returns:
        dict with park hours fields, or None if not found
    """
    if versioned_df is None or versioned_df.empty:
        return None
    
    if as_of is None:
        as_of = datetime.now(ZoneInfo("UTC"))
    
    park_date_str = park_date.strftime("%Y-%m-%d")
    park_code_upper = str(park_code).upper().strip()
    
    # Filter to matching park_date and park_code
    mask = (
        (versioned_df["park_date"] == park_date_str) &
        (versioned_df["park_code"].astype(str).str.upper().str.strip() == park_code_upper)
    )
    candidates = versioned_df[mask].copy()
    
    if candidates.empty:
        return None
    
    # Filter by temporal validity
    valid_mask = (
        (candidates["valid_from"] <= as_of) &
        (
            candidates["valid_until"].isna() |
            (candidates["valid_until"] > as_of)
        )
    )
    candidates = candidates[valid_mask]
    
    if candidates.empty:
        return None
    
    # Sort by priority (version_type) and recency (created_at DESC)
    candidates["_priority"] = candidates["version_type"].map(VERSION_TYPES).fillna(99)
    candidates = candidates.sort_values(
        by=["_priority", "created_at"],
        ascending=[True, False]
    )
    
    # Return best match; ensure opening/closing_time are never blank (data quality)
    best = candidates.iloc[0]
    _ot = best.get("opening_time")
    _ct = best.get("closing_time")
    opening = _ot if (_ot is not None and str(_ot).strip() and str(_ot).strip().lower() != "nan") else DEFAULT_DATETIME_BLANK
    closing = _ct if (_ct is not None and str(_ct).strip() and str(_ct).strip().lower() != "nan") else DEFAULT_DATETIME_BLANK
    result = {
        "opening_time": opening,
        "closing_time": closing,
        "emh_morning": bool(best.get("emh_morning", False)),
        "emh_evening": bool(best.get("emh_evening", False)),
        "version_type": best.get("version_type"),
        "version_id": best.get("version_id"),
        "confidence": best.get("confidence"),
        "change_probability": best.get("change_probability"),
        "source": best.get("source"),
    }
    
    return result


def build_park_hours_lookup_table(
    versioned_df: pd.DataFrame,
    keys_df: pd.DataFrame,
    as_of: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Build a (park_date, park_code) -> park hours lookup table in one vectorized pass.
    
    Park hours are per (park_date, park_code), not per entity. This function
    takes the set of (park_date, park_code) keys needed and returns a lookup
    DataFrame with one row per key and columns: park_date, park_code,
    opening_time, closing_time, emh_morning, emh_evening.
    
    Uses the same priority and temporal logic as get_park_hours_for_date, but
    operates in bulk via merges and groupby, so it is O(unique keys + versioned rows)
    instead of O(unique keys * cost_per_lookup).
    
    Args:
        versioned_df: Versioned park hours DataFrame (from load_versioned_table)
        keys_df: DataFrame with columns park_date, park_code (e.g. from
                 df[["park_date", "park_code"]].drop_duplicates())
        as_of: Timestamp for version selection (default: now UTC)
        logger: Optional logger
    
    Returns:
        DataFrame with columns park_date, park_code, opening_time, closing_time,
        emh_morning, emh_evening. Index reset. Missing hours get DEFAULT_DATETIME_BLANK.
    """
    if versioned_df is None or versioned_df.empty or keys_df is None or keys_df.empty:
        return pd.DataFrame(columns=[
            "park_date", "park_code", "opening_time", "closing_time",
            "emh_morning", "emh_evening",
        ])
    
    if as_of is None:
        as_of = datetime.now(ZoneInfo("UTC"))
    
    # Normalize keys to (park_date_str, park_code_upper) for merge
    keys = keys_df[["park_date", "park_code"]].drop_duplicates()
    keys["_park_date"] = pd.to_datetime(keys["park_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    keys["_park_code"] = keys["park_code"].astype(str).str.upper().str.strip()
    keys_norm = keys[["_park_date", "_park_code"]].drop_duplicates()
    
    # Normalize versioned_df for merge
    v = versioned_df.copy()
    v["_park_date"] = v["park_date"].astype(str).str.strip()
    v["_park_code"] = v["park_code"].astype(str).str.upper().str.strip()
    
    # Merge: each key gets all matching versioned rows
    merged = keys_norm.merge(
        v,
        on=["_park_date", "_park_code"],
        how="left",
    )
    
    if merged.empty:
        out = keys_norm.rename(columns={"_park_date": "park_date", "_park_code": "park_code"})
        out["opening_time"] = DEFAULT_DATETIME_BLANK
        out["closing_time"] = DEFAULT_DATETIME_BLANK
        out["emh_morning"] = False
        out["emh_evening"] = False
        return out[["park_date", "park_code", "opening_time", "closing_time", "emh_morning", "emh_evening"]]
    
    # Temporal validity
    valid = (
        (merged["valid_from"] <= as_of) &
        (merged["valid_until"].isna() | (merged["valid_until"] > as_of))
    )
    merged = merged.loc[valid]
    
    if merged.empty:
        out = keys_norm.rename(columns={"_park_date": "park_date", "_park_code": "park_code"})
        out["opening_time"] = DEFAULT_DATETIME_BLANK
        out["closing_time"] = DEFAULT_DATETIME_BLANK
        out["emh_morning"] = False
        out["emh_evening"] = False
        return out[["park_date", "park_code", "opening_time", "closing_time", "emh_morning", "emh_evening"]]
    
    # Priority and recency: same as get_park_hours_for_date
    merged["_priority"] = merged["version_type"].map(VERSION_TYPES).fillna(99)
    merged = merged.sort_values(
        by=["_park_date", "_park_code", "_priority", "created_at"],
        ascending=[True, True, True, False],
    )
    
    # Keep first (best) per (park_date, park_code)
    best = merged.drop_duplicates(subset=["_park_date", "_park_code"], keep="first")
    
    # Output columns; fill blank opening/closing with default
    ot = best["opening_time"].astype(str)
    ct = best["closing_time"].astype(str)
    out = best[["_park_date", "_park_code"]].rename(columns={"_park_date": "park_date", "_park_code": "park_code"})
    out["opening_time"] = ot.where(
        ot.notna() & (ot.str.strip() != "") & (ot.str.strip().str.lower() != "nan"),
        DEFAULT_DATETIME_BLANK,
    )
    out["closing_time"] = ct.where(
        ct.notna() & (ct.str.strip() != "") & (ct.str.strip().str.lower() != "nan"),
        DEFAULT_DATETIME_BLANK,
    )
    out["emh_morning"] = best["emh_morning"].fillna(False).astype(bool)
    out["emh_evening"] = best["emh_evening"].fillna(False).astype(bool)
    
    return out.reset_index(drop=True)


# =============================================================================
# VERSION CREATION
# =============================================================================

def create_official_version(
    park_date: date,
    park_code: str,
    opening_time: str,
    closing_time: str,
    emh_morning: bool = False,
    emh_evening: bool = False,
    versioned_df: Optional[pd.DataFrame] = None,
    created_at: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> tuple[pd.DataFrame, bool]:
    """
    Create or update an official version of park hours.
    
    If an official version already exists for this (park_date, park_code), marks
    the old one as expired (valid_until=created_at) and creates a new one.
    
    Args:
        park_date: Park operational date
        park_code: Park code
        opening_time: Opening time (HH:MM:SS or HH:MM)
        closing_time: Closing time (HH:MM:SS or HH:MM)
        emh_morning: Morning EMH flag
        emh_evening: Evening EMH flag
        versioned_df: Existing versioned DataFrame (None to create new)
        created_at: Timestamp for version creation (default: now)
        logger: Optional logger
    
    Returns:
        (updated_df, changed) tuple where changed=True if hours actually changed
    """
    if created_at is None:
        created_at = datetime.now(ZoneInfo("UTC"))
    
    park_date_str = park_date.strftime("%Y-%m-%d")
    park_code_upper = str(park_code).upper().strip()
    version_id = f"official_{created_at.strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize DataFrame if needed
    if versioned_df is None:
        versioned_df = pd.DataFrame(columns=[
            "park_date", "park_code", "version_type", "version_id", "source",
            "created_at", "valid_from", "valid_until",
            "opening_time", "closing_time", "emh_morning", "emh_evening",
            "confidence", "change_probability", "notes"
        ])
    
    # Check if official version exists
    existing_mask = (
        (versioned_df["park_date"] == park_date_str) &
        (versioned_df["park_code"].astype(str).str.upper().str.strip() == park_code_upper) &
        (versioned_df["version_type"] == "official") &
        (versioned_df["valid_until"].isna())
    )
    existing = versioned_df[existing_mask]
    
    # Detect if hours changed
    changed = False
    if not existing.empty:
        old = existing.iloc[0]
        old_opening = str(old.get("opening_time", "")).strip()
        old_closing = str(old.get("closing_time", "")).strip()
        old_emh_m = bool(old.get("emh_morning", False))
        old_emh_e = bool(old.get("emh_evening", False))
        
        if (
            old_opening != str(opening_time).strip() or
            old_closing != str(closing_time).strip() or
            old_emh_m != emh_morning or
            old_emh_e != emh_evening
        ):
            changed = True
            # Mark old version as expired
            versioned_df.loc[existing_mask, "valid_until"] = created_at
            if logger:
                logger.info(
                    f"Park hours changed for {park_code} {park_date_str}: "
                    f"opening {old_opening}→{opening_time}, "
                    f"closing {old_closing}→{closing_time}, "
                    f"emh_morning {old_emh_m}→{emh_morning}, "
                    f"emh_evening {old_emh_e}→{emh_evening}"
                )
    
    # Create new official version
    new_row = {
        "park_date": park_date_str,
        "park_code": park_code_upper,
        "version_type": "official",
        "version_id": version_id,
        "source": "s3_sync",
        "created_at": created_at,
        "valid_from": created_at,
        "valid_until": None,
        "opening_time": opening_time,
        "closing_time": closing_time,
        "emh_morning": emh_morning,
        "emh_evening": emh_evening,
        "confidence": 1.0,  # Official hours are 100% confident
        "change_probability": None,  # Will be calculated separately
        "notes": None,
    }
    
    versioned_df = pd.concat([versioned_df, pd.DataFrame([new_row])], ignore_index=True)
    
    return versioned_df, changed


def calculate_change_probability(
    park_date: date,
    park_code: str,
    dategroupid: Optional[str] = None,
    days_until_date: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> float:
    """
    Calculate probability that official hours will change.
    
    This is a simple rule-based model. Can be enhanced with ML later.
    
    Args:
        park_date: Park operational date
        park_code: Park code
        dategroupid: Date group ID (if available)
        days_until_date: Days until park_date (if None, calculated from today)
        logger: Optional logger
    
    Returns:
        Probability (0.0-1.0) that hours will change
    """
    if days_until_date is None:
        today = date.today()
        days_until_date = (park_date - today).days
    
    # Base probability decreases as date approaches
    if days_until_date <= FINAL_DAYS_THRESHOLD:
        return 0.05  # Very low - hours are likely final
    elif days_until_date <= 30:
        return 0.20  # Low - within a month
    elif days_until_date <= 90:
        return 0.40  # Medium - 1-3 months out
    else:
        return 0.60  # Higher - more than 3 months out
    
    # TODO: Enhance with dategroupid (holidays more likely to change)
    # TODO: Enhance with historical change patterns per park
    # TODO: Enhance with season (peak season more stable)


def create_predicted_version_from_donor(
    target_date: date,
    target_park_code: str,
    donor_date: date,
    donor_park_code: str,
    dimparkhours_flat: pd.DataFrame,
    dimdategroupid: Optional[pd.DataFrame],
    versioned_df: Optional[pd.DataFrame] = None,
    created_at: Optional[datetime] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[pd.DataFrame]:
    """
    Create a predicted version from a donor day.
    
    Uses donor day's hours and calculates confidence based on similarity.
    
    Args:
        target_date: Date to predict hours for
        target_park_code: Park code to predict for
        donor_date: Donor day date
        donor_park_code: Donor day park code (should match target_park_code)
        dimparkhours_flat: Flat dimparkhours table (for getting donor hours)
        dimdategroupid: dimdategroupid table (for calculating similarity)
        versioned_df: Existing versioned DataFrame
        created_at: Timestamp for version creation
        logger: Optional logger
    
    Returns:
        Updated versioned_df with new predicted version, or None on error
    """
    if created_at is None:
        created_at = datetime.now(ZoneInfo("UTC"))
    
    # Get donor hours from flat table
    donor_date_str = donor_date.strftime("%Y-%m-%d")
    donor_mask = (
        (dimparkhours_flat["park_date"] == donor_date_str) &
        (dimparkhours_flat["park_code"].astype(str).str.upper().str.strip() == str(donor_park_code).upper().strip())
    )
    donor_row = dimparkhours_flat[donor_mask]
    
    if donor_row.empty:
        if logger:
            logger.warning(f"Donor day not found: {donor_park_code} {donor_date_str}")
        return None
    
    donor = donor_row.iloc[0]
    
    # Calculate confidence based on similarity
    confidence = 1.0  # Start with full confidence if dategroupid matches
    
    # Check dategroupid match if available
    if dimdategroupid is not None:
        target_date_str = target_date.strftime("%Y-%m-%d")
        # Find date column in dimdategroupid
        date_col = None
        for col in ["park_date", "date", "park_day_id"]:
            if col in dimdategroupid.columns:
                date_col = col
                break
        if date_col:
            target_dg = dimdategroupid[dimdategroupid[date_col] == target_date_str]
            donor_dg = dimdategroupid[dimdategroupid[date_col] == donor_date_str]
            
            if not target_dg.empty and not donor_dg.empty:
                # Find date_group_id column
                dgid_col = None
                for col in ["date_group_id", "dategroupid", "date_group"]:
                    if col in dimdategroupid.columns:
                        dgid_col = col
                        break
                if dgid_col:
                    target_dgid = target_dg.iloc[0].get(dgid_col)
                    donor_dgid = donor_dg.iloc[0].get(dgid_col)
                    
                    if target_dgid != donor_dgid:
                        confidence = 0.7  # Lower confidence if dategroupid doesn't match
    
    # Apply recency weighting
    days_ago = (date.today() - donor_date).days
    recency_weight = 1.0 / (1.0 + days_ago / 365.0)
    confidence *= recency_weight
    
    # Create predicted version
    target_date_str = target_date.strftime("%Y-%m-%d")
    target_park_upper = str(target_park_code).upper().strip()
    version_id = f"predicted_donor_{donor_date_str}_{created_at.strftime('%Y%m%d_%H%M%S')}"
    
    if versioned_df is None:
        versioned_df = pd.DataFrame(columns=[
            "park_date", "park_code", "version_type", "version_id", "source",
            "created_at", "valid_from", "valid_until",
            "opening_time", "closing_time", "emh_morning", "emh_evening",
            "confidence", "change_probability", "notes"
        ])
    
    # Get hours from donor; use default if blank (data quality)
    _ot = donor.get("opening_time")
    _ct = donor.get("closing_time")
    opening_time = _ot if (_ot is not None and str(_ot).strip() and str(_ot).strip().lower() != "nan") else DEFAULT_DATETIME_BLANK
    closing_time = _ct if (_ct is not None and str(_ct).strip() and str(_ct).strip().lower() != "nan") else DEFAULT_DATETIME_BLANK
    emh_morning = bool(donor.get("emh_morning", False))
    emh_evening = bool(donor.get("emh_evening", False))
    
    new_row = {
        "park_date": target_date_str,
        "park_code": target_park_upper,
        "version_type": "predicted",
        "version_id": version_id,
        "source": "donor_imputation",
        "created_at": created_at,
        "valid_from": created_at,
        "valid_until": None,  # Will be set when official hours arrive
        "opening_time": opening_time,
        "closing_time": closing_time,
        "emh_morning": emh_morning,
        "emh_evening": emh_evening,
        "confidence": confidence,
        "change_probability": None,
        "notes": f"Donor: {donor_park_code} {donor_date_str}",
    }
    
    versioned_df = pd.concat([versioned_df, pd.DataFrame([new_row])], ignore_index=True)
    
    if logger:
        logger.debug(
            f"Created predicted version for {target_park_code} {target_date_str} "
            f"from donor {donor_park_code} {donor_date_str} (confidence={confidence:.2f})"
        )
    
    return versioned_df


def find_best_donor_day(
    target_date: date,
    target_park_code: str,
    dimparkhours_flat: pd.DataFrame,
    dimdategroupid: Optional[pd.DataFrame],
    logger: Optional[logging.Logger] = None,
) -> Optional[tuple[date, float]]:
    """
    Find the best donor day for a target date using dategroupid matching and recency.
    
    Args:
        target_date: Date to find donor for
        target_park_code: Park code
        dimparkhours_flat: Flat dimparkhours table
        dimdategroupid: dimdategroupid table
        logger: Optional logger
    
    Returns:
        (donor_date, score) tuple, or None if no donor found
    """
    target_park_upper = str(target_park_code).upper().strip()
    
    # Filter to same park, past dates only
    park_mask = (
        (dimparkhours_flat["park_code"].astype(str).str.upper().str.strip() == target_park_upper) &
        (pd.to_datetime(dimparkhours_flat["park_date"], errors="coerce") < pd.Timestamp(target_date))
    )
    candidates = dimparkhours_flat[park_mask].copy()
    
    if candidates.empty:
        return None
    
    # Get target dategroupid
    target_dgid = None
    if dimdategroupid is not None:
        target_date_str = target_date.strftime("%Y-%m-%d")
        # Find date column in dimdategroupid
        date_col = None
        for col in ["park_date", "date", "park_day_id"]:
            if col in dimdategroupid.columns:
                date_col = col
                break
        if date_col:
            target_dg = dimdategroupid[dimdategroupid[date_col] == target_date_str]
            if not target_dg.empty:
                # Find date_group_id column
                dgid_col = None
                for col in ["date_group_id", "dategroupid", "date_group"]:
                    if col in dimdategroupid.columns:
                        dgid_col = col
                        break
                if dgid_col:
                    target_dgid = target_dg.iloc[0].get(dgid_col)
    
    # Score each candidate
    best_score = -1.0
    best_donor = None
    
    for _, row in candidates.iterrows():
        donor_date = pd.to_datetime(row["park_date"], errors="coerce").date()
        if donor_date is None:
            continue
        
        # Check dategroupid match
        dgid_match = False
        if target_dgid is not None and dimdategroupid is not None:
            donor_date_str = row["park_date"]
            # Find date column in dimdategroupid
            date_col = None
            for col in ["park_date", "date", "park_day_id"]:
                if col in dimdategroupid.columns:
                    date_col = col
                    break
            if date_col:
                donor_dg = dimdategroupid[dimdategroupid[date_col] == donor_date_str]
                if not donor_dg.empty:
                    # Find date_group_id column
                    dgid_col = None
                    for col in ["date_group_id", "dategroupid", "date_group"]:
                        if col in dimdategroupid.columns:
                            dgid_col = col
                            break
                    if dgid_col:
                        donor_dgid = donor_dg.iloc[0].get(dgid_col)
                        dgid_match = (donor_dgid == target_dgid)
        
        # Recency weight
        days_ago = (date.today() - donor_date).days
        recency_weight = 1.0 / (1.0 + days_ago / 365.0)
        
        # Score: dategroupid match is required, recency is multiplier
        if dgid_match:
            score = 1.0 * recency_weight
        else:
            score = 0.7 * recency_weight  # Lower score if no dategroupid match
        
        if score > best_score:
            best_score = score
            best_donor = donor_date
    
    if best_donor is None:
        return None
    
    return (best_donor, best_score)


# =============================================================================
# SAVE VERSIONED TABLE
# =============================================================================

def save_versioned_table(
    versioned_df: pd.DataFrame,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Save the versioned park hours table to CSV.
    
    Args:
        versioned_df: Versioned DataFrame to save
        output_base: Pipeline output base directory
        logger: Optional logger
    """
    dim_dir = output_base / "dimension_tables"
    dim_dir.mkdir(parents=True, exist_ok=True)
    
    path = dim_dir / VERSIONED_TABLE_NAME
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    
    try:
        versioned_df.to_csv(tmp_path, index=False)
        import os
        os.replace(tmp_path, path)
        if logger:
            logger.info(f"Saved versioned park hours: {len(versioned_df):,} rows to {path}")
    except Exception as e:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        if logger:
            logger.error(f"Failed to save versioned park hours: {e}")
        raise
