#!/usr/bin/env python3
"""
Events Dimension Tables Builder

================================================================================
PURPOSE
================================================================================
Fetches events dimension data from S3 and writes two dimension tables:

  1. DOWNLOADS current_event_days.csv and current_events.csv from
     s3://touringplans_stats/export/events/
  2. WRITES dimension_tables/dimeventdays.csv (events by day, using event codes)
  3. WRITES dimension_tables/dimevents.csv (lookup table for event codes)

Used as auxiliary data for modeling, WTI, and joining with wait-time fact tables.
Same S3 bucket and AWS credentials as wait-time ETL, entity table, park hours.

================================================================================
S3 SOURCE
================================================================================
  - Bucket: touringplans_stats
  - Prefix: export/events/
  - Files (only these two; others in export/events/ are ignored):
      - current_event_days.csv — list of events by day using event codes
        (date, park_abbreviation, event_abbreviation, event_opening_time, event_closing_time)
      - current_events.csv — lookup table for event codes
        (property_abbrev, event_abbreviation, event_code, event_name, event_hard_ticket)

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimeventdays.csv under --output-base
  - dimension_tables/dimevents.csv under --output-base
  - Logs: logs/get_events_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/get_events_from_s3.py
  python src/get_events_from_s3.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError, ResponseStreamingError


# =============================================================================
# CONFIGURATION
# =============================================================================
# S3 bucket and prefix must match the export layout. Only these two files
# are used; other export/events/ files (e.g. event_days_*.csv, events_*.csv) ignored.

S3_BUCKET = "touringplans_stats"
S3_EVENTS_PREFIX = "export/events/"

EVENT_FILES = [
    ("current_event_days.csv", "dimeventdays.csv"),
    ("current_events.csv", "dimevents.csv"),
]

DEFAULT_OUTPUT_BASE = Path(
    r"D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report"
)
MAX_RETRIES = 3
RETRY_WAIT = [1, 2, 4]


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """
    Set up file and console logging for the events run.
    Log file: get_events_YYYYMMDD_HHMMSS.log under log_dir.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"get_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


# =============================================================================
# S3 HELPERS
# =============================================================================

def _download_csv(s3, bucket: str, key: str, logger: logging.Logger) -> bytes | None:
    """
    Download an S3 object as bytes. Retries on ClientError, ResponseStreamingError, OSError.
    Returns None on failure after MAX_RETRIES attempts.
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            return resp["Body"].read()
        except (ClientError, ResponseStreamingError, OSError) as e:
            wait = RETRY_WAIT[attempt] if attempt < len(RETRY_WAIT) else RETRY_WAIT[-1]
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Error reading {key} (attempt {attempt + 1}/{MAX_RETRIES}): {e}. Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                logger.error(f"Failed to read {key} after {MAX_RETRIES} attempts: {e}")
                return None
    return None


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch events from S3 and build dimension_tables/dimeventdays.csv and dimevents.csv"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=DEFAULT_OUTPUT_BASE,
        help="Output base directory (dimension_tables and logs under it)",
    )
    args = ap.parse_args()

    # ----- STEP 1: Resolve paths and set up logging -----
    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Events dimension tables build")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"S3 bucket: {S3_BUCKET}  prefix: {S3_EVENTS_PREFIX}")

    # ----- STEP 2: Initialize S3 client (retries, timeouts) -----
    try:
        config = Config(
            retries={"max_attempts": 5, "mode": "adaptive"},
            read_timeout=120,
            connect_timeout=60,
        )
        s3 = boto3.client("s3", config=config)
        logger.info("S3 client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        sys.exit(1)

    # ----- STEP 3: Fetch each events CSV and write to dimension_tables -----
    dim_dir.mkdir(parents=True, exist_ok=True)
    failed = False

    for s3_name, out_name in EVENT_FILES:
        key = S3_EVENTS_PREFIX + s3_name
        raw = _download_csv(s3, S3_BUCKET, key, logger)
        if raw is None:
            failed = True
            continue
        try:
            df = pd.read_csv(io.BytesIO(raw), low_memory=False)
        except Exception as e:
            logger.error(f"Failed to parse {key}: {e}")
            failed = True
            continue
        out_path = dim_dir / out_name
        try:
            df.to_csv(out_path, index=False)
            logger.info(f"Wrote {out_path} ({len(df):,} rows, {len(df.columns)} columns)")
        except Exception as e:
            logger.error(f"Failed to write {out_path}: {e}")
            failed = True

    if failed:
        sys.exit(1)
    logger.info("Done.")


if __name__ == "__main__":
    main()
