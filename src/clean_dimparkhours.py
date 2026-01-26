#!/usr/bin/env python3
"""
Clean dimparkhours.csv

Applies cleaning rules to dimension_tables/dimparkhours.csv:
- Rename columns: park -> park_code, date -> park_date
- Ensure dates are YYYY-MM-DD format
- Times are already ISO8601 with timezone (keep as-is)
- Convert emh_morning, emh_evening to boolean
- Drop or keep predicted_* columns (all null currently)

Usage:
    python src/clean_dimparkhours.py
    python src/clean_dimparkhours.py --output-base "D:\\Path"
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

DIMPARKHOURS_NAME = "dimparkhours.csv"


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"clean_dimparkhours_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def clean_dimparkhours(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply cleaning rules to dimparkhours DataFrame."""
    df = df.copy()
    original_rows = len(df)
    logger.info(f"Starting with {original_rows:,} rows, {len(df.columns)} columns")

    # ----- Rename columns to standard names -----
    rename_map = {}
    if "park" in df.columns and "park_code" not in df.columns:
        rename_map["park"] = "park_code"
    if "date" in df.columns and "park_date" not in df.columns:
        rename_map["date"] = "park_date"
    if "park_day_id" in df.columns:
        # Keep park_day_id if it exists (may be useful), but ensure park_date is primary
        pass
    if "park_day" in df.columns and "park_date" not in df.columns:
        rename_map["park_day"] = "park_date"
    
    if rename_map:
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns to standard names: {rename_map}")

    # ----- Clean park_code: uppercase -----
    if "park_code" in df.columns:
        df["park_code"] = df["park_code"].astype(str).str.strip().str.upper()
        logger.info(f"Cleaned park_code: uppercase, trimmed")

    # ----- Clean park_date: ensure YYYY-MM-DD format -----
    if "park_date" in df.columns:
        # Parse date (may be string or datetime)
        df["park_date"] = pd.to_datetime(df["park_date"], errors="coerce")
        # Convert to YYYY-MM-DD string format
        df["park_date"] = df["park_date"].dt.strftime("%Y-%m-%d")
        null_count = df["park_date"].isna().sum()
        if null_count > 0:
            logger.warning(f"park_date has {null_count} nulls after parsing")
        logger.info(f"Cleaned park_date: YYYY-MM-DD format")

    # ----- Times: already ISO8601 with timezone, keep as-is -----
    time_cols = ["opening_time", "opening_time_with_emh", "closing_time", "closing_time_with_emh_or_party"]
    for col in time_cols:
        if col in df.columns:
            # Validate ISO8601 format
            try:
                pd.to_datetime(df[col], errors="raise")
                logger.info(f"{col}: ISO8601 format validated")
            except Exception as e:
                logger.warning(f"{col}: Some values may not be valid ISO8601: {e}")

    # ----- Convert EMH columns to boolean -----
    if "emh_morning" in df.columns:
        if df["emh_morning"].dtype != "bool":
            df["emh_morning"] = df["emh_morning"].astype(float).fillna(0.0).astype(bool)
            logger.info(f"Converted emh_morning to boolean")
        else:
            logger.info(f"emh_morning already boolean")
    
    if "emh_evening" in df.columns:
        if df["emh_evening"].dtype != "bool":
            df["emh_evening"] = df["emh_evening"].astype(float).fillna(0.0).astype(bool)
            logger.info(f"Converted emh_evening to boolean")
        else:
            logger.info(f"emh_evening already boolean")

    # ----- Optional: Drop predicted_* columns if all null -----
    predicted_cols = [col for col in df.columns if col.startswith("predicted_")]
    if predicted_cols:
        all_null_cols = []
        for col in predicted_cols:
            if df[col].isna().all():
                all_null_cols.append(col)
        
        if all_null_cols:
            logger.info(f"Found {len(all_null_cols)} predicted_* columns that are 100% null")
            # Keep them for now (may be used in future), but log
            # Uncomment to drop:
            # df = df.drop(columns=all_null_cols)
            # logger.info(f"Dropped columns: {all_null_cols}")

    logger.info(f"Cleaning complete: {len(df):,} rows")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean dimension_tables/dimparkhours.csv"
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
    logger.info("Clean dimparkhours.csv")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMPARKHOURS_NAME
    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        logger.error("Run get_park_hours_from_s3.py first")
        sys.exit(1)

    # Read
    try:
        df = pd.read_csv(in_path, low_memory=False)
        logger.info(f"Read {in_path}: {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Failed to read {in_path}: {e}")
        sys.exit(1)

    # Clean
    df_cleaned = clean_dimparkhours(df, logger)

    # Write (atomic)
    out_path = dim_dir / DIMPARKHOURS_NAME
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
