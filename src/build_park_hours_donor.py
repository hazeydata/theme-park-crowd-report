#!/usr/bin/env python3
"""
Build Predicted Park Hours from Donor Days

Fills gaps in future park hours by finding best donor days (same dategroupid,
weighted by recency) and creating predicted versions.

Usage:
    python src/build_park_hours_donor.py
    python src/build_park_hours_donor.py --output-base "D:\\Path" --max-days-ahead 730
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

from processors.park_hours_versioning import (
    find_best_donor_day,
    create_predicted_version_from_donor,
    load_versioned_table,
    save_versioned_table,
)
from utils import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"build_park_hours_donor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
        description="Build predicted park hours from donor days"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--max-days-ahead",
        type=int,
        default=730,
        help="Maximum days ahead to fill (default: 730 = 2 years)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Build Predicted Park Hours from Donor Days")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Max days ahead: {args.max_days_ahead}")

    # Load required tables
    dimparkhours_path = dim_dir / "dimparkhours.csv"
    if not dimparkhours_path.exists():
        logger.error(f"dimparkhours.csv not found: {dimparkhours_path}")
        sys.exit(1)

    try:
        dimparkhours_flat = pd.read_csv(dimparkhours_path, low_memory=False)
        logger.info(f"Loaded dimparkhours: {len(dimparkhours_flat):,} rows")
    except Exception as e:
        logger.error(f"Failed to load dimparkhours: {e}")
        sys.exit(1)

    dimdategroupid_path = dim_dir / "dimdategroupid.csv"
    dimdategroupid = None
    if dimdategroupid_path.exists():
        try:
            dimdategroupid = pd.read_csv(dimdategroupid_path, low_memory=False)
            logger.info(f"Loaded dimdategroupid: {len(dimdategroupid):,} rows")
        except Exception as e:
            logger.warning(f"Could not load dimdategroupid: {e}")

    # Load or create versioned table
    versioned_df = load_versioned_table(base)
    if versioned_df is None:
        logger.warning("Versioned table not found. Run migrate_park_hours_to_versioned.py first")
        logger.info("Creating new versioned table...")
        versioned_df = pd.DataFrame(columns=[
            "park_date", "park_code", "version_type", "version_id", "source",
            "created_at", "valid_from", "valid_until",
            "opening_time", "closing_time", "emh_morning", "emh_evening",
            "confidence", "change_probability", "notes"
        ])

    # Get list of parks
    park_col = None
    for col in ["park_code", "park", "code"]:
        if col in dimparkhours_flat.columns:
            park_col = col
            break

    if not park_col:
        logger.error("Could not find park column in dimparkhours")
        sys.exit(1)

    parks = dimparkhours_flat[park_col].astype(str).str.upper().str.strip().unique()
    logger.info(f"Found {len(parks)} parks: {', '.join(parks)}")

    # Generate date range: tomorrow to max_days_ahead
    today = date.today()
    start_date = today + timedelta(days=1)
    end_date = today + timedelta(days=args.max_days_ahead)

    logger.info(f"Filling gaps from {start_date} to {end_date}")

    # For each park and date, check if we need predicted version
    created_count = 0
    skipped_count = 0

    for park_code in parks:
        logger.info(f"Processing {park_code}...")
        
        for target_date in pd.date_range(start_date, end_date, freq="D"):
            target_date = target_date.date()
            
            # Check if official version exists
            from processors.park_hours_versioning import get_park_hours_for_date
            existing = get_park_hours_for_date(
                target_date,
                park_code,
                versioned_df,
                logger=logger,
            )
            
            # Skip if official version exists
            if existing and existing.get("version_type") == "official":
                skipped_count += 1
                continue
            
            # Skip if predicted version already exists (don't overwrite)
            if existing and existing.get("version_type") == "predicted":
                skipped_count += 1
                continue
            
            # Find best donor day
            donor_result = find_best_donor_day(
                target_date,
                park_code,
                dimparkhours_flat,
                dimdategroupid,
                logger=logger,
            )
            
            if donor_result is None:
                logger.debug(f"No donor found for {park_code} {target_date}")
                skipped_count += 1
                continue
            
            donor_date, score = donor_result
            
            # Create predicted version
            try:
                versioned_df = create_predicted_version_from_donor(
                    target_date=target_date,
                    target_park_code=park_code,
                    donor_date=donor_date,
                    donor_park_code=park_code,
                    dimparkhours_flat=dimparkhours_flat,
                    dimdategroupid=dimdategroupid,
                    versioned_df=versioned_df,
                    logger=logger,
                )
                created_count += 1
                
                if created_count % 100 == 0:
                    logger.info(f"Created {created_count:,} predicted versions...")
            
            except Exception as e:
                logger.warning(f"Failed to create predicted version for {park_code} {target_date}: {e}")
                continue

    logger.info(f"Created {created_count:,} predicted versions")
    logger.info(f"Skipped {skipped_count:,} dates (already have official or predicted)")

    # Save versioned table
    try:
        save_versioned_table(versioned_df, base, logger)
        logger.info("Done!")
    except Exception as e:
        logger.error(f"Failed to save versioned table: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
