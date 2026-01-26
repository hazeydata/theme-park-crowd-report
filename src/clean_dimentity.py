#!/usr/bin/env python3
"""
Clean dimentity.csv

Applies cleaning rules to dimension_tables/dimentity.csv:
- Uppercase entity_code, park_code
- Lowercase property_code
- Trim all string columns
- Convert empty strings to NULL
- Apply defaults: opened_on = park opening date if blank, extinct_on = 2099-01-01 if blank
- Convert boolean columns (fastpass_available, priority_available)
- Validate data types and constraints

Usage:
    python src/clean_dimentity.py
    python src/clean_dimentity.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from utils import get_output_base

# Park opening dates (for opened_on default)
PARK_OPENING_DATES = {
    "MK": date(1971, 10, 1),   # Magic Kingdom
    "EP": date(1982, 10, 1),   # EPCOT
    "HS": date(1989, 5, 1),    # Hollywood Studios
    "AK": date(1998, 4, 22),   # Animal Kingdom
    "DL": date(1955, 7, 17),   # Disneyland
    "CA": date(2001, 2, 8),    # California Adventure
    "TDL": date(1983, 4, 15),  # Tokyo Disneyland
    "TDS": date(2001, 9, 4),   # Tokyo DisneySea
    "UF": date(1990, 6, 7),    # Universal Studios Florida
    "IA": date(1999, 5, 28),   # Islands of Adventure
    "EU": date(2025, 6, 1),    # Epic Universe (future)
    "USH": date(1964, 7, 15),  # Universal Studios Hollywood
}

DEFAULT_EXTINCT_DATE = date(2099, 1, 1)  # Far future = still open
DIMENTITY_NAME = "dimentity.csv"


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"clean_dimentity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


def clean_string_column(series: pd.Series, uppercase: bool = False, lowercase: bool = False) -> pd.Series:
    """Trim string column and optionally convert case. Empty strings -> NULL."""
    result = series.astype(str).str.strip()
    if uppercase:
        result = result.str.upper()
    elif lowercase:
        result = result.str.lower()
    # Convert empty strings to NULL
    result = result.replace("", None)
    result = result.where(result != "nan", None)  # "nan" string -> NULL
    return result


def parse_date_column(series: pd.Series) -> pd.Series:
    """Parse date column, handling various formats. Returns date series."""
    # Try parsing as date
    try:
        return pd.to_datetime(series, errors="coerce").dt.date
    except Exception:
        # If that fails, try common formats
        for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d/%m/%Y"]:
            try:
                parsed = pd.to_datetime(series, format=fmt, errors="coerce")
                if parsed.notna().any():
                    return parsed.dt.date
            except Exception:
                continue
        # If all fail, return as-is (will be NULL)
        return pd.to_datetime(series, errors="coerce").dt.date


def convert_bool_column(series: pd.Series) -> pd.Series:
    """Convert column to boolean, handling various formats."""
    if series.dtype == "bool":
        return series
    
    # Convert to string, lowercase, trim
    s = series.astype(str).str.lower().str.strip()
    
    # Map common boolean representations
    true_values = {"true", "1", "yes", "y", "t"}
    false_values = {"false", "0", "no", "n", "f", "nan", ""}
    
    result = pd.Series([None] * len(series), dtype="object")
    result[s.isin(true_values)] = True
    result[s.isin(false_values)] = False
    
    # Default to False if still None
    result = result.fillna(False)
    return result.astype("bool")


def clean_dimentity(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply cleaning rules to dimentity DataFrame."""
    df = df.copy()
    original_rows = len(df)
    logger.info(f"Starting with {original_rows:,} rows, {len(df.columns)} columns")

    # ----- Rename columns to standard names -----
    rename_map = {}
    # Handle entity_code: prefer "code", then "attraction_code"
    if "entity_code" not in df.columns:
        if "code" in df.columns:
            rename_map["code"] = "entity_code"
        elif "attraction_code" in df.columns:
            rename_map["attraction_code"] = "entity_code"
    # If both exist, drop attraction_code after renaming code
    if "code" in df.columns and "attraction_code" in df.columns and "entity_code" not in df.columns:
        # Rename code first, then drop attraction_code
        df = df.rename(columns={"code": "entity_code"})
        df = df.drop(columns=["attraction_code"])
        logger.info("Renamed 'code' to 'entity_code' and dropped duplicate 'attraction_code'")
    elif rename_map:
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns: {rename_map}")
    
    if "name" in df.columns and "entity_name" not in df.columns:
        df = df.rename(columns={"name": "entity_name"})
        logger.info("Renamed 'name' to 'entity_name'")

    # ----- String columns: trim and case conversion -----
    if "entity_code" in df.columns:
        df["entity_code"] = clean_string_column(df["entity_code"], uppercase=True)
        logger.info(f"Cleaned entity_code: uppercase, trimmed")
    
    # Extract park_code from entity_code prefix if needed (e.g., "MK101" -> "MK")
    if "entity_code" in df.columns and "park_code" not in df.columns:
        df["park_code"] = df["entity_code"].str[:2].str.upper()
        logger.info(f"Extracted park_code from entity_code prefix")
    
    # Trim all other string columns
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        if col not in ["entity_code", "park_code"]:
            df[col] = clean_string_column(df[col])
            logger.info(f"Trimmed {col}")

    # ----- Date columns: opened_on and extinct_on -----
    if "opened_on" in df.columns:
        # Parse date column (currently string)
        df["opened_on"] = parse_date_column(df["opened_on"])
        null_count = df["opened_on"].isna().sum()
        logger.info(f"Parsed opened_on: {null_count} nulls before defaults")
        
        # Apply default: park opening date if blank
        # Use park_code extracted from code prefix
        if "park_code" in df.columns:
            for park_code, default_date in PARK_OPENING_DATES.items():
                mask = df["opened_on"].isna() & (df["park_code"] == park_code)
                if mask.any():
                    df.loc[mask, "opened_on"] = default_date
                    logger.info(f"Applied default opened_on = {default_date} for {mask.sum()} rows with park_code = {park_code}")
        
        # Final fallback: use earliest date
        if df["opened_on"].isna().any():
            fallback = date(1955, 7, 17)  # Disneyland opening
            df["opened_on"] = df["opened_on"].fillna(fallback)
            logger.info(f"Applied fallback opened_on = {fallback} for {df['opened_on'].isna().sum()} remaining nulls")
        
        # Convert to string format YYYY-MM-DD for CSV
        df["opened_on"] = df["opened_on"].astype(str)
    
    if "extinct_on" in df.columns:
        # Parse date column (currently string)
        df["extinct_on"] = parse_date_column(df["extinct_on"])
        null_count = df["extinct_on"].isna().sum()
        logger.info(f"Parsed extinct_on: {null_count} nulls before defaults")
        
        # Apply default: 2099-01-01 if blank (far future = still open)
        mask = df["extinct_on"].isna()
        if mask.any():
            df.loc[mask, "extinct_on"] = DEFAULT_EXTINCT_DATE
            logger.info(f"Applied default extinct_on = {DEFAULT_EXTINCT_DATE} for {mask.sum()} rows")
        
        # Convert to string format YYYY-MM-DD for CSV
        df["extinct_on"] = df["extinct_on"].astype(str)
    
    # ----- Boolean columns -----
    # Note: Most boolean columns are already bool type, but check for any that need conversion
    # Common boolean columns in dimentity: fastpass_booth, single_rider, open_emh_morning, etc.
    bool_columns = [
        "fastpass_booth", "single_rider", "open_emh_morning", "open_emh_evening",
        "open_very_merry", "open_not_so_scary", "seasonal", "has_posted", "intense"
    ]
    for col in bool_columns:
        if col in df.columns:
            if df[col].dtype != "bool":
                df[col] = convert_bool_column(df[col])
                logger.info(f"Converted {col} to boolean")
            else:
                logger.info(f"{col} already boolean type")

    # ----- Numeric columns: validate ranges -----
    # Note: Actual column is "height_restriction" not "height_requirement_inches"
    if "height_restriction" in df.columns:
        df["height_restriction"] = pd.to_numeric(df["height_restriction"], errors="coerce")
        # Set negative values to NULL
        mask = df["height_restriction"] < 0
        if mask.any():
            df.loc[mask, "height_restriction"] = None
            logger.info(f"Set {mask.sum()} negative height_restriction to NULL")
    
    # Note: Actual column is "duration" not "duration_minutes"
    if "duration" in df.columns:
        df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
        # Set non-positive values to NULL
        mask = df["duration"] <= 0
        if mask.any():
            df.loc[mask, "duration"] = None
            logger.info(f"Set {mask.sum()} non-positive duration to NULL")

    logger.info(f"Cleaning complete: {len(df):,} rows")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean dimension_tables/dimentity.csv"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Clean dimentity.csv")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMENTITY_NAME
    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        logger.error("Run get_entity_table_from_s3.py first")
        sys.exit(1)

    # Read
    try:
        df = pd.read_csv(in_path, low_memory=False)
        logger.info(f"Read {in_path}: {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Failed to read {in_path}: {e}")
        sys.exit(1)

    # Clean
    df_cleaned = clean_dimentity(df, logger)

    # Write (atomic)
    out_path = dim_dir / DIMENTITY_NAME
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        df_cleaned.to_csv(tmp_path, index=False)
        os.replace(tmp_path, out_path)
        logger.info(f"Wrote cleaned {out_path} ({len(df_cleaned):,} rows)")
    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        logger.error(f"Failed to write {out_path}: {e}")
        sys.exit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
