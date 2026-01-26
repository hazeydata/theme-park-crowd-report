#!/usr/bin/env python3
"""
Clean dimevents.csv

Applies cleaning rules to dimension_tables/dimevents.csv:
- Lowercase property_abbrev
- Uppercase event_abbreviation, event_code
- Trim all string columns
- Convert event_hard_ticket to boolean (currently int64 0/1)

Usage:
    python src/clean_dimevents.py
    python src/clean_dimevents.py --output-base "D:\\Path"
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

DIMEVENTS_NAME = "dimevents.csv"


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"clean_dimevents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def clean_dimevents(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Apply cleaning rules to dimevents DataFrame."""
    df = df.copy()
    original_rows = len(df)
    logger.info(f"Starting with {original_rows:,} rows, {len(df.columns)} columns")

    # ----- Rename columns to standard names -----
    rename_map = {}
    if "property_abbrev" in df.columns and "property_code" not in df.columns:
        rename_map["property_abbrev"] = "property_code"
    
    if rename_map:
        df = df.rename(columns=rename_map)
        logger.info(f"Renamed columns to standard names: {rename_map}")

    # ----- Clean property_code: lowercase -----
    if "property_code" in df.columns:
        df["property_code"] = df["property_code"].astype(str).str.strip().str.lower()
        logger.info(f"Cleaned property_code: lowercase, trimmed")

    # ----- Clean event_abbreviation: uppercase -----
    if "event_abbreviation" in df.columns:
        df["event_abbreviation"] = df["event_abbreviation"].astype(str).str.strip().str.upper()
        logger.info(f"Cleaned event_abbreviation: uppercase, trimmed")

    # ----- Clean event_code: uppercase -----
    if "event_code" in df.columns:
        df["event_code"] = df["event_code"].astype(str).str.strip().str.upper()
        # Convert empty strings to NULL
        df["event_code"] = df["event_code"].replace("", None)
        logger.info(f"Cleaned event_code: uppercase, trimmed, empty -> NULL")

    # ----- Clean event_name: trim -----
    if "event_name" in df.columns:
        df["event_name"] = df["event_name"].astype(str).str.strip()
        # Convert empty strings to NULL
        df["event_name"] = df["event_name"].replace("", None)
        logger.info(f"Cleaned event_name: trimmed, empty -> NULL")

    # ----- Convert event_hard_ticket to boolean -----
    if "event_hard_ticket" in df.columns:
        if df["event_hard_ticket"].dtype != "bool":
            # Convert int64 0/1 to boolean
            df["event_hard_ticket"] = df["event_hard_ticket"].astype(bool)
            logger.info(f"Converted event_hard_ticket to boolean")
        else:
            logger.info(f"event_hard_ticket already boolean")

    logger.info(f"Cleaning complete: {len(df):,} rows")
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean dimension_tables/dimevents.csv"
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
    logger.info("Clean dimevents.csv")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMEVENTS_NAME
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
    df_cleaned = clean_dimevents(df, logger)

    # Write (atomic)
    out_path = dim_dir / DIMEVENTS_NAME
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
