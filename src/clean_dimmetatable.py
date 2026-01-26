#!/usr/bin/env python3
"""
Clean dimmetatable.csv

Applies cleaning rules to dimension_tables/dimmetatable.csv:
- Rename: date -> park_date (if needed)
- Clean park_code: uppercase
- Clean park_date: ensure YYYY-MM-DD format
- Convert boolean columns to boolean
- Trim all string columns
- Convert empty strings to NULL

Usage:
    python src/clean_dimmetatable.py
    python src/clean_dimmetatable.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from utils import get_output_base

DIMMETATABLE_NAME = "dimmetatable.csv"


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"clean_dimmetatable_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def clean_dimmetatable(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply cleaning rules to dimmetatable DataFrame."""
    df = df.copy()
    original_rows = len(df)
    logger.info(f"Starting with {original_rows:,} rows, {len(df.columns)} columns")

    # ----- Rename columns to standard names -----
    rename_map = {}
    # Handle DATE (uppercase) or date (lowercase)
    if "DATE" in df.columns and "park_date" not in df.columns:
        rename_map["DATE"] = "park_date"
    elif "date" in df.columns and "park_date" not in df.columns:
        rename_map["date"] = "park_date"
    if "park" in df.columns and "park_code" not in df.columns:
        rename_map["park"] = "park_code"
    if "park_abbreviation" in df.columns and "park_code" not in df.columns:
        rename_map["park_abbreviation"] = "park_code"
    if "park_abbrev" in df.columns and "park_code" not in df.columns:
        rename_map["park_abbrev"] = "park_code"
    if "property_abbrev" in df.columns and "property_code" not in df.columns:
        rename_map["property_abbrev"] = "property_code"
    
    if rename_map:
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns to standard names: {rename_map}")

    # ----- Clean park_code: uppercase -----
    if "park_code" in df.columns:
        df["park_code"] = clean_string_column(df["park_code"], uppercase=True)
        logger.info(f"Cleaned park_code: uppercase, trimmed")

    # ----- Clean property_code: lowercase -----
    if "property_code" in df.columns:
        df["property_code"] = clean_string_column(df["property_code"], lowercase=True)
        logger.info(f"Cleaned property_code: lowercase, trimmed")

    # ----- Clean park_date: ensure YYYY-MM-DD format -----
    if "park_date" in df.columns:
        df["park_date"] = pd.to_datetime(df["park_date"], errors="coerce")
        df["park_date"] = df["park_date"].dt.strftime("%Y-%m-%d")
        null_count = df["park_date"].isna().sum()
        if null_count > 0:
            logger.warning(f"park_date has {null_count} nulls after parsing")
        logger.info(f"Cleaned park_date: YYYY-MM-DD format")

    # ----- Convert boolean columns -----
    # Common boolean column patterns in metatable (be more specific to avoid converting time columns)
    # Look for columns that are explicitly flags (MORN, EVE, YEST, TOM, DAY, WK, etc.) not times (OPEN, CLOSE, T1, T2)
    bool_patterns = [
        "MORN", "EVE", "YEST", "TOM",  # EMH flags
        "PRDDAY", "PRDNGT", "FIREWK", "SHWNGT", "FIREWKS",  # Event flags
        "OPTIMIZERS",  # Count flags
    ]
    # Exclude time columns (OPEN, CLOSE, T1, T2, HOURS, HOURSEMH)
    exclude_patterns = ["OPEN", "CLOSE", "T1", "T2", "HOURS", "HOURSEMH", "DT1", "DT2", "NT1", "NT2"]
    
    bool_columns = []
    for col in df.columns:
        col_lower = col.upper()  # Check uppercase version
        if any(pattern in col_lower for pattern in bool_patterns):
            if not any(exclude in col_lower for exclude in exclude_patterns):
                bool_columns.append(col)
    
    for col in bool_columns:
        if df[col].dtype != "bool":
            df[col] = convert_bool_column(df[col])
            logger.info(f"Converted {col} to boolean")
        else:
            logger.info(f"{col} already boolean type")

    # ----- Trim all other string columns -----
    string_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in string_cols:
        if col not in ["park_code", "property_code", "park_date"]:
            df[col] = clean_string_column(df[col])
            logger.info(f"Trimmed {col}")

    logger.info(f"Cleaning complete: {len(df):,} rows")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean dimension_tables/dimmetatable.csv"
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
    logger.info("Clean dimmetatable.csv")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMMETATABLE_NAME
    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        logger.error("Run get_metatable_from_s3.py first")
        sys.exit(1)

    # Read
    try:
        df = pd.read_csv(in_path, low_memory=False)
        logger.info(f"Read {in_path}: {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Failed to read {in_path}: {e}")
        sys.exit(1)

    # Clean
    df_cleaned = clean_dimmetatable(df, logger)

    # Write (atomic)
    out_path = dim_dir / DIMMETATABLE_NAME
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
