#!/usr/bin/env python3
"""
Build Posted Aggregates

Builds historical aggregates of POSTED wait times for predicted POSTED generation.

Usage:
    python scripts/build_posted_aggregates.py
    python scripts/build_posted_aggregates.py --output-base "D:\\Path" --min-date 2024-01-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from processors.posted_aggregates import (
    build_posted_aggregates,
    save_posted_aggregates,
)
from utils import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"build_posted_aggregates_{datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d_%H%M%S')}.log"

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
        description="Build POSTED aggregates from historical fact data"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--min-date",
        type=str,
        help="Minimum park_date to include (YYYY-MM-DD)",
    )
    ap.add_argument(
        "--max-date",
        type=str,
        help="Maximum park_date to include (YYYY-MM-DD, default: today)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Build POSTED Aggregates")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    # Parse dates
    min_date = None
    if args.min_date:
        try:
            min_date = datetime.strptime(args.min_date, "%Y-%m-%d").date()
            logger.info(f"Min date: {min_date}")
        except ValueError:
            logger.error(f"Invalid min-date format: {args.min_date}. Use YYYY-MM-DD")
            sys.exit(1)

    max_date = None
    if args.max_date:
        try:
            max_date = datetime.strptime(args.max_date, "%Y-%m-%d").date()
            logger.info(f"Max date: {max_date}")
        except ValueError:
            logger.error(f"Invalid max-date format: {args.max_date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        max_date = date.today()
        logger.info(f"Max date: {max_date} (today)")

    # Build aggregates
    try:
        aggregates = build_posted_aggregates(
            base,
            min_date=min_date,
            max_date=max_date,
            logger=logger,
        )

        if aggregates.empty:
            logger.warning("No aggregates generated")
            sys.exit(1)

        # Save aggregates
        output_path = save_posted_aggregates(aggregates, base, logger)
        logger.info(f"Successfully built and saved aggregates: {len(aggregates):,} rows")
        logger.info(f"Output: {output_path}")

    except Exception as e:
        logger.error(f"Failed to build aggregates: {e}", exc_info=True)
        sys.exit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
