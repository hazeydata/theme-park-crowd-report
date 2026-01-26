#!/usr/bin/env python3
"""
Clean dimeventdays.csv

Applies cleaning rules to dimension_tables/dimeventdays.csv:
- Rename: date -> park_date
- Clean park_abbreviation (currently 100% null - may need to derive or drop)
- Uppercase event_abbreviation
- Times are already ISO8601 with timezone (keep as-is)

Usage:
    python src/clean_dimeventdays.py
    python src/clean_dimeventdays.py --output-base "D:\\Path"
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

DIMEVENTDAYS_NAME = "dimeventdays.csv"


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"clean_dimeventdays_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def clean_dimeventdays(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply cleaning rules to dimeventdays DataFrame."""
    df = df.copy()
    original_rows = len(df)
    logger.info(f"Starting with {original_rows:,} rows, {len(df.columns)} columns")

    # ----- Rename columns to standard names -----
    rename_map = {}
    if "date" in df.columns and "park_date" not in df.columns:
        rename_map["date"] = "park_date"
    if "park_abbreviation" in df.columns and "park_code" not in df.columns:
        rename_map["park_abbreviation"] = "park_code"
    if "park_abbrev" in df.columns and "park_code" not in df.columns:
        rename_map["park_abbrev"] = "park_code"
    
    if rename_map:
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns to standard names: {rename_map}")

    # ----- Clean park_date: ensure YYYY-MM-DD format -----
    if "park_date" in df.columns:
        df["park_date"] = pd.to_datetime(df["park_date"], errors="coerce")
        df["park_date"] = df["park_date"].dt.strftime("%Y-%m-%d")
        null_count = df["park_date"].isna().sum()
        if null_count > 0:
            logger.warning(f"park_date has {null_count} nulls after parsing")
        logger.info(f"Cleaned park_date: YYYY-MM-DD format")

    # ----- Clean park_code (formerly park_abbreviation) -----
    if "park_code" in df.columns:
        null_count = df["park_code"].isna().sum()
        if null_count == len(df):
            logger.warning(f"park_code is 100% null - keeping column but noting issue")
            # Could derive from event_abbreviation or other sources, but for now keep as-is
        else:
            # Convert to string, uppercase, trim
            df["park_code"] = df["park_code"].astype(str).str.strip().str.upper()
            df["park_code"] = df["park_code"].replace("NAN", None)
            logger.info(f"Cleaned park_code: uppercase, trimmed ({null_count} nulls)")

    # ----- Clean event_abbreviation: uppercase -----
    if "event_abbreviation" in df.columns:
        df["event_abbreviation"] = df["event_abbreviation"].astype(str).str.strip().str.upper()
        logger.info(f"Cleaned event_abbreviation: uppercase, trimmed")

    # ----- Times: already ISO8601 with timezone, keep as-is -----
    time_cols = ["event_opening_time", "event_closing_time"]
    for col in time_cols:
        if col in df.columns:
            # Validate ISO8601 format
            try:
                pd.to_datetime(df[col], errors="raise")
                logger.info(f"{col}: ISO8601 format validated")
            except Exception as e:
                logger.warning(f"{col}: Some values may not be valid ISO8601: {e}")

    logger.info(f"Cleaning complete: {len(df):,} rows")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean dimension_tables/dimeventdays.csv"
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
    logger.info("Clean dimeventdays.csv")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMEVENTDAYS_NAME
    if not in_path.exists():
        logger.error(f"Input file not found: {in_path}")
        logger.error("Run get_events_from_s3.py first")
        sys.exit(1)

    # Read
    try:
        df = pd.read_csv(in_path, low_memory=False)
        logger.info(f"Read {in_path}: {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Failed to read {in_path}: {e}")
        sys.exit(1)

    # Clean
    df_cleaned = clean_dimeventdays(df, logger)

    # Write (atomic)
    out_path = dim_dir / DIMEVENTDAYS_NAME
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
