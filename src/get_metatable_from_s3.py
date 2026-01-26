#!/usr/bin/env python3
"""
Metatable (dimMetatable) Fetcher

================================================================================
PURPOSE
================================================================================
Fetches the metatable dimension from S3 and writes dimension_tables/dimmetatable.csv.
The metatable provides metadata about park days (extra magic hours, parades,
closures, etc.). No transformation—download and save.

  - DOWNLOADS current_metatable.csv from s3://touringplans_stats/export/metatable/
  - WRITES dimension_tables/dimmetatable.csv under --output-base

Adapted from legacy Julia run_dimMetatable.jl. Same S3 bucket; we write only
to dimension_tables (no S3 upload).

================================================================================
S3 SOURCE
================================================================================
  - Bucket: touringplans_stats
  - Key: export/metatable/current_metatable.csv

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimmetatable.csv under --output-base
  - Logs: logs/get_metatable_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/get_metatable_from_s3.py
  python src/get_metatable_from_s3.py --output-base "D:\\Path"
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

S3_BUCKET = "touringplans_stats"
S3_METATABLE_KEY = "export/metatable/current_metatable.csv"
DIMMETATABLE_NAME = "dimmetatable.csv"
MAX_RETRIES = 3
RETRY_WAIT = [1, 2, 4]


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging. Log file: get_metatable_YYYYMMDD_HHMMSS.log."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"get_metatable_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
    """Download S3 object as bytes. Retries on ClientError, ResponseStreamingError, OSError."""
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
# MAIN
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Fetch metatable from S3 and write dimension_tables/dimmetatable.csv"
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
    logger.info("Metatable (dimMetatable) fetch")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"S3 bucket: {S3_BUCKET}  key: {S3_METATABLE_KEY}")

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

    raw = _download_csv(s3, S3_BUCKET, S3_METATABLE_KEY, logger)
    if raw is None:
        sys.exit(1)

    try:
        df = pd.read_csv(io.BytesIO(raw), low_memory=False)
    except Exception as e:
        logger.error(f"Failed to parse {S3_METATABLE_KEY}: {e}")
        sys.exit(1)

    dim_dir.mkdir(parents=True, exist_ok=True)
    out_path = dim_dir / DIMMETATABLE_NAME
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, out_path)
        logger.info(f"Wrote {out_path} ({len(df):,} rows, {len(df.columns)} columns)")
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
