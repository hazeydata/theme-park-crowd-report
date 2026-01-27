#!/usr/bin/env python3
"""
Migrate dimparkhours to versioned format

Converts existing dimparkhours.csv to dimparkhours_with_donor.csv with
all existing hours marked as 'official' versions.

Usage:
    python src/migrate_park_hours_to_versioned.py
    python src/migrate_park_hours_to_versioned.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

from processors.park_hours_versioning import create_official_version, save_versioned_table
from utils import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"migrate_park_hours_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Migrate dimparkhours.csv to versioned format"
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
    logger.info("Migrate dimparkhours to versioned format")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    # Load existing dimparkhours
    dimparkhours_path = dim_dir / "dimparkhours.csv"
    if not dimparkhours_path.exists():
        logger.error(f"dimparkhours.csv not found: {dimparkhours_path}")
        logger.error("Run get_park_hours_from_s3.py and clean_dimparkhours.py first")
        sys.exit(1)

    try:
        df = pd.read_csv(dimparkhours_path, low_memory=False)
        logger.info(f"Loaded dimparkhours: {len(df):,} rows")
    except Exception as e:
        logger.error(f"Failed to read dimparkhours: {e}")
        sys.exit(1)

    # Find date and park columns
    date_col = None
    for col in ["park_date", "date", "park_day_id"]:
        if col in df.columns:
            date_col = col
            break

    park_col = None
    for col in ["park_code", "park", "code"]:
        if col in df.columns:
            park_col = col
            break

    open_col = None
    for col in ["opening_time", "open", "open_time"]:
        if col in df.columns:
            open_col = col
            break

    close_col = None
    for col in ["closing_time", "close", "close_time"]:
        if col in df.columns:
            close_col = col
            break

    if not date_col or not park_col or not open_col or not close_col:
        logger.error("dimparkhours missing required columns")
        sys.exit(1)

    # Initialize versioned DataFrame
    versioned_df = None
    created_at = datetime.now(ZoneInfo("UTC"))

    # Convert each row to official version
    logger.info("Converting rows to official versions...")
    for idx, row in df.iterrows():
        try:
            park_date = pd.to_datetime(row[date_col], errors="coerce").date()
            if pd.isna(park_date):
                logger.warning(f"Row {idx}: invalid date, skipping")
                continue

            park_code = str(row[park_col]).upper().strip()
            opening_time = str(row[open_col]).strip()
            closing_time = str(row[close_col]).strip()
            emh_morning = bool(row.get("emh_morning", False))
            emh_evening = bool(row.get("emh_evening", False))

            versioned_df, changed = create_official_version(
                park_date=park_date,
                park_code=park_code,
                opening_time=opening_time,
                closing_time=closing_time,
                emh_morning=emh_morning,
                emh_evening=emh_evening,
                versioned_df=versioned_df,
                created_at=created_at,
                logger=logger,
            )

            if (idx + 1) % 1000 == 0:
                logger.info(f"Processed {idx + 1:,} rows...")

        except Exception as e:
            logger.warning(f"Row {idx}: error converting: {e}")
            continue

    if versioned_df is None or versioned_df.empty:
        logger.error("No rows converted")
        sys.exit(1)

    logger.info(f"Converted {len(versioned_df):,} official versions")

    # Save versioned table
    try:
        save_versioned_table(versioned_df, base, logger)
        logger.info("Migration complete!")
    except Exception as e:
        logger.error(f"Failed to save versioned table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
