#!/usr/bin/env python3
"""
Entity Table (dimEntity) Builder

================================================================================
PURPOSE
================================================================================
Fetches entity dimension data from S3 and builds a single master table (dimEntity).
Used as auxiliary data for modeling, WTI, and joining with wait-time fact tables.

  1. DOWNLOADS current_*_entities.csv from s3://touringplans_stats/export/entities/
  2. COMBINES them (union of columns; missing cols filled with NaN)
  3. NORMALIZES the "land" column (ensure consistent type; add if missing)
  4. WRITES dimension_tables/dimentity.csv

Adapted from legacy Julia (run_dimEntity.jl): same S3 source, same property
files, same land-column handling. We only fetch and combine; no S3 upload.

================================================================================
S3 SOURCE
================================================================================
  - Bucket: touringplans_stats
  - Prefix: export/entities/
  - Files: current_dlr_entities.csv, current_tdr_entities.csv, current_uor_entities.csv,
           current_ush_entities.csv, current_wdw_entities.csv
  - Properties: dlr (Disneyland Resort), tdr (Tokyo Disney), uor (Universal Orlando
    incl. Epic Universe / EU), ush (Universal Studios Hollywood), wdw (Walt Disney World)
  - Other files in that location are ignored.

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimentity.csv under --output-base (same base as wait-time ETL).
  - Logs: logs/get_entity_table_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/get_entity_table_from_s3.py
  python src/get_entity_table_from_s3.py --output-base "D:\\Path"
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

# =============================================================================
# CONFIGURATION
# =============================================================================
# S3 bucket and prefix must match the export layout. ENTITY_FILES are the only
# files we use; others under export/entities/ are ignored.

S3_BUCKET = "touringplans_stats"
S3_ENTITIES_PREFIX = "export/entities/"

# Only these entity files are used. Others in export/entities/ are ignored.
ENTITY_FILES = [
    "current_dlr_entities.csv",
    "current_tdr_entities.csv",
    "current_uor_entities.csv",
    "current_ush_entities.csv",
    "current_wdw_entities.csv",
]

DIMENTITY_NAME = "dimentity.csv"
MAX_RETRIES = 3
RETRY_WAIT = [1, 2, 4]


# =============================================================================
# LOGGING
# =============================================================================
# File + console, same pattern as wait-time ETL. Log dir under output base.

def setup_logging(log_dir: Path) -> logging.Logger:
    """
    Set up file and console logging for the entity table run.
    Log file: get_entity_table_YYYYMMDD_HHMMSS.log under log_dir.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"get_entity_table_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# Download object as bytes with retries. Same retry pattern as wait-time ETL
# (connection/stream errors, exponential backoff).

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
# ENTITY TABLE BUILD
# =============================================================================
# Normalize land column (Julia: add if missing, else convert to Union{Missing,String}).
# Fetch each CSV, normalize, concatenate with outer join (union of columns).

def _normalize_land(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure "land" column exists and has consistent type (object, None/NaN for missing).
    Matches legacy Julia: add column of missing if absent; otherwise convert to string-like.
    """
    if "land" in df.columns:
        df = df.copy()
        df["land"] = df["land"].astype(object).where(df["land"].notna(), None)
        return df
    df = df.copy()
    df["land"] = None
    return df


def _fetch_and_combine(s3, bucket: str, keys: list[str], logger: logging.Logger) -> pd.DataFrame | None:
    """
    Download each CSV from S3, normalize land, concatenate with outer join (union of columns).
    Skips files that fail to download or parse; continues with the rest.
    Returns combined DataFrame, or None if all downloads failed.
    """
    frames: list[pd.DataFrame] = []

    for key in keys:
        raw = _download_csv(s3, bucket, key, logger)
        if raw is None:
            continue
        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception as e:
            logger.error(f"Failed to parse {key}: {e}")
            continue
        df = _normalize_land(df)
        frames.append(df)
        logger.info(f"Loaded {key}: {len(df):,} rows, {len(df.columns)} columns")

    if not frames:
        logger.error("No entity files could be loaded.")
        return None

    combined = pd.concat(frames, ignore_index=True, join="outer")
    logger.info(f"Combined: {len(combined):,} rows, {len(combined.columns)} columns")
    return combined


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch entity files from S3 and build dimension_tables/dimentity.csv"
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
    logger.info("Entity table (dimEntity) build")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"S3 bucket: {S3_BUCKET}  prefix: {S3_ENTITIES_PREFIX}")

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

    # ----- STEP 3: Fetch entity CSVs and combine -----
    keys = [S3_ENTITIES_PREFIX + f for f in ENTITY_FILES]
    combined = _fetch_and_combine(s3, S3_BUCKET, keys, logger)
    if combined is None:
        sys.exit(1)

    # ----- STEP 4: Write dimension_tables/dimentity.csv (atomic) -----
    dim_dir.mkdir(parents=True, exist_ok=True)
    out_path = dim_dir / DIMENTITY_NAME
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

    logger.info("Done.")


if __name__ == "__main__":
    main()
