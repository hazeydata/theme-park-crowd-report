#!/usr/bin/env python3
"""
Park Hours Dimension Table Builder

================================================================================
PURPOSE
================================================================================
Fetches park hours dimension data from S3 and builds a single master table.
Used as auxiliary data for modeling, WTI, and joining with wait-time fact tables.

  1. DOWNLOADS {prop}_park_hours.csv from s3://touringplans_stats/export/park_hours/
  2. COMBINES them (union of columns; missing cols filled with NaN)
  3. WRITES dimension_tables/dimparkhours.csv

Same pattern as entity table: same bucket, same properties (dlr, tdr, uor, ush, wdw).

================================================================================
S3 SOURCE
================================================================================
  - Bucket: touringplans_stats
  - Prefix: export/park_hours/
  - Files: dlr_park_hours.csv, tdr_park_hours.csv, uor_park_hours.csv,
           ush_park_hours.csv, wdw_park_hours.csv
  - Properties: dlr, tdr, uor, ush, wdw. Other files in that location are ignored.

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimparkhours.csv under --output-base (same base as wait-time ETL).
  - Logs: logs/get_park_hours_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/get_park_hours_from_s3.py
  python src/get_park_hours_from_s3.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError, ResponseStreamingError

from utils import get_output_base

# Import versioning module (optional - only if versioned table exists)
try:
    from processors.park_hours_versioning import (
        create_official_version,
        load_versioned_table,
        save_versioned_table,
    )
    VERSIONING_AVAILABLE = True
except ImportError:
    VERSIONING_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================
# S3 bucket and prefix must match the export layout. PARK_HOURS_FILES are the
# only files we use; others under export/park_hours/ are ignored.

S3_BUCKET = "touringplans_stats"
S3_PARK_HOURS_PREFIX = "export/park_hours/"

# Only these park-hours files are used. Others in export/park_hours/ are ignored.
PARK_HOURS_FILES = [
    "dlr_park_hours.csv",
    "tdr_park_hours.csv",
    "uor_park_hours.csv",
    "ush_park_hours.csv",
    "wdw_park_hours.csv",
]

DIMPARKHOURS_NAME = "dimparkhours.csv"
# Default for blank datetime columns when updating versioned table (Pacific UTC-8)
DEFAULT_DATETIME_BLANK = "1999-01-01T00:00:00-08:00"
MAX_RETRIES = 3
RETRY_WAIT = [1, 2, 4]


# =============================================================================
# LOGGING
# =============================================================================
# File + console, same pattern as entity table and wait-time ETL.

def setup_logging(log_dir: Path) -> logging.Logger:
    """
    Set up file and console logging for the park-hours run.
    Log file: get_park_hours_YYYYMMDD_HHMMSS.log under log_dir.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"get_park_hours_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# Download object as bytes with retries. Same retry pattern as entity table.

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
# PARK HOURS BUILD
# =============================================================================
# Fetch each CSV, concatenate with outer join (union of columns).

def _fetch_and_combine(s3, bucket: str, keys: list[str], logger: logging.Logger) -> pd.DataFrame | None:
    """
    Download each CSV from S3, concatenate with outer join (union of columns).
    Skips files that fail to download or parse; continues with the rest.
    Returns combined DataFrame, or None if all downloads failed.
    """
    frames: list[pd.DataFrame] = []

    for key in keys:
        raw = _download_csv(s3, bucket, key, logger)
        if raw is None:
            continue
        try:
            df = pd.read_csv(io.BytesIO(raw), low_memory=False)
        except Exception as e:
            logger.error(f"Failed to parse {key}: {e}")
            continue
        frames.append(df)
        logger.info(f"Loaded {key}: {len(df):,} rows, {len(df.columns)} columns")

    if not frames:
        logger.error("No park-hours files could be loaded.")
        return None

    combined = pd.concat(frames, ignore_index=True, join="outer")
    logger.info(f"Combined: {len(combined):,} rows, {len(combined.columns)} columns")
    return combined


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch park-hours files from S3 and build dimension_tables/dimparkhours.csv"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    args = ap.parse_args()

    # ----- STEP 1: Resolve paths and set up logging -----
    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Park hours dimension table build")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"S3 bucket: {S3_BUCKET}  prefix: {S3_PARK_HOURS_PREFIX}")

    # ----- STEP 2: Initialize S3 client (retries, timeouts) -----
    try:
        config = Config(
            retries={"max_attempts": 5, "mode": "adaptive"},
            read_timeout=120,
            connect_timeout=60,
            proxies={},  # Disable proxies
        )
        s3 = boto3.client("s3", config=config)
        logger.info("S3 client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        sys.exit(1)

    # ----- STEP 3: Fetch park-hours CSVs and combine -----
    keys = [S3_PARK_HOURS_PREFIX + f for f in PARK_HOURS_FILES]
    combined = _fetch_and_combine(s3, S3_BUCKET, keys, logger)
    if combined is None:
        sys.exit(1)

    # ----- STEP 4: Write dimension_tables/dimparkhours.csv (atomic) -----
    dim_dir.mkdir(parents=True, exist_ok=True)
    out_path = dim_dir / DIMPARKHOURS_NAME
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        combined.to_csv(tmp_path, index=False)
        os.replace(tmp_path, out_path)
        logger.info(f"Wrote {out_path}")
    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        logger.error(f"Failed to write {out_path}: {e}")
        sys.exit(1)

    # ----- STEP 5: Update versioned table (if it exists) -----
    if VERSIONING_AVAILABLE:
        try:
            versioned_df = load_versioned_table(base)
            if versioned_df is None:
                logger.info("Versioned table not found; skipping version creation (run migrate_park_hours_to_versioned.py first)")
            else:
                logger.info("Updating versioned table with new official hours...")
                
                # Find date and park columns
                date_col = None
                for col in ["park_date", "date", "park_day_id"]:
                    if col in combined.columns:
                        date_col = col
                        break
                
                park_col = None
                for col in ["park_code", "park", "code"]:
                    if col in combined.columns:
                        park_col = col
                        break
                
                open_col = None
                for col in ["opening_time", "open", "open_time"]:
                    if col in combined.columns:
                        open_col = col
                        break
                
                close_col = None
                for col in ["closing_time", "close", "close_time"]:
                    if col in combined.columns:
                        close_col = col
                        break
                
                if date_col and park_col and open_col and close_col:
                    changes_count = 0
                    for idx, row in combined.iterrows():
                        try:
                            park_date = pd.to_datetime(row[date_col], errors="coerce").date()
                            if pd.isna(park_date):
                                continue
                            
                            park_code = str(row[park_col]).upper().strip()
                            _ot = row[open_col]
                            _ct = row[close_col]
                            opening_time = (str(_ot).strip() if pd.notna(_ot) and str(_ot).strip() and str(_ot).strip().lower() != "nan" else DEFAULT_DATETIME_BLANK)
                            closing_time = (str(_ct).strip() if pd.notna(_ct) and str(_ct).strip() and str(_ct).strip().lower() != "nan" else DEFAULT_DATETIME_BLANK)
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
                                logger=logger,
                            )
                            
                            if changed:
                                changes_count += 1
                        except Exception as e:
                            logger.warning(f"Row {idx}: error creating version: {e}")
                            continue
                    
                    if changes_count > 0:
                        logger.info(f"Detected {changes_count} changes in park hours")
                    
                    # Save versioned table
                    save_versioned_table(versioned_df, base, logger)
                    logger.info("Versioned table updated")
                else:
                    logger.warning("Could not find required columns for versioning")
        except Exception as e:
            logger.warning(f"Failed to update versioned table: {e} (continuing...)")

    logger.info("Done.")


if __name__ == "__main__":
    main()
