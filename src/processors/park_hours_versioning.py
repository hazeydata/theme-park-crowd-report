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
      create_official_version,
      create_predicted_version_from_donor,
  )
  
  # Get hours for a date (uses best available version)
  hours = get_park_hours_for_date(park_date, park_code, dimparkhours_versioned, as_of=now)
  
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
    
    # Return best match
    best = candidates.iloc[0]
    result = {
        "opening_time": best.get("opening_time"),
        "closing_time": best.get("closing_time"),
        "emh_morning": bool(best.get("emh_morning", False)),
        "emh_evening": bool(best.get("emh_evening", False)),
        "version_type": best.get("version_type"),
        "version_id": best.get("version_id"),
        "confidence": best.get("confidence"),
        "change_probability": best.get("change_probability"),
        "source": best.get("source"),
    }
    
    return result


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
    
    # Get hours from donor
    opening_time = donor.get("opening_time")
    closing_time = donor.get("closing_time")
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
