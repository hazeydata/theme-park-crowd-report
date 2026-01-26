#!/usr/bin/env python3
"""
Build or Rebuild Entity Metadata Index

================================================================================
PURPOSE
================================================================================
Scans all fact table CSVs in fact_tables/clean/ and builds/rebuilds the entity
metadata index (state/entity_index.sqlite). Useful for:
  - Initial index creation
  - Rebuilding after index corruption
  - Updating index if it got out of sync

The index tracks per-entity:
  - latest_park_date (max date with observations)
  - latest_observed_at (max timestamp)
  - row_count (total rows)
  - first_seen_at, updated_at (timestamps)

================================================================================
USAGE
================================================================================
  # Build index from all CSVs
  python src/build_entity_index.py

  # Rebuild (delete existing and start fresh)
  python src/build_entity_index.py --rebuild

  # Custom output base
  python src/build_entity_index.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

# Import shared utilities
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from get_tp_wait_time_data_from_s3 import derive_park_date, get_park_code
from processors.entity_index import ensure_index_db, update_index_from_dataframe
from utils import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Setup logging to file and console."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "build_entity_index.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger(__name__)


def scan_and_build_index(
    clean_dir: Path,
    index_db: Path,
    logger: logging.Logger,
    rebuild: bool = False,
) -> int:
    """
    Scan all CSVs in clean_dir and build/update entity index.
    
    Returns:
        Number of entities indexed
    """
    if not clean_dir.exists():
        logger.error(f"Fact tables directory not found: {clean_dir}")
        return 0
    
    # Delete existing index if rebuilding
    if rebuild and index_db.exists():
        logger.info(f"Rebuilding: deleting existing index {index_db}")
        index_db.unlink()
    
    ensure_index_db(index_db)
    logger.info(f"Building entity index: {index_db}")
    logger.info(f"Scanning CSVs in: {clean_dir}")
    
    # Find all CSVs
    csvs = list(clean_dir.rglob("*.csv"))
    if not csvs:
        logger.warning(f"No CSVs found in {clean_dir}")
        return 0
    
    logger.info(f"Found {len(csvs)} CSV files")
    
    # Process in batches to avoid memory issues
    batch_size = 100
    total_entities = 0
    entities_seen = set()
    
    # Use Eastern timezone as default for park_date derivation
    # (In practice, each park has its own TZ, but for index building this is fine)
    default_tz = ZoneInfo("America/New_York")
    
    for i in range(0, len(csvs), batch_size):
        batch = csvs[i : i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(csvs) + batch_size - 1)//batch_size} ({len(batch)} files)")
        
        batch_dfs: list[pd.DataFrame] = []
        for csv_path in batch:
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                if df.empty:
                    continue
                
                # Ensure required columns
                required = ["entity_code", "observed_at"]
                if not all(c in df.columns for c in required):
                    logger.warning(f"Skipping {csv_path.name}: missing required columns")
                    continue
                
                # Derive park_date if not present
                if "park_date" not in df.columns:
                    # Try to parse from filename: {park}_{YYYY-MM-DD}.csv
                    stem = csv_path.stem
                    if "_" in stem:
                        parts = stem.split("_", 1)
                        if len(parts) == 2:
                            date_str = parts[1]
                            try:
                                # Validate it's a date
                                pd.to_datetime(date_str)
                                df["park_date"] = date_str
                            except:
                                # Fall back to deriving from observed_at
                                df["park_date"] = derive_park_date(df["observed_at"], default_tz)
                        else:
                            df["park_date"] = derive_park_date(df["observed_at"], default_tz)
                    else:
                        df["park_date"] = derive_park_date(df["observed_at"], default_tz)
                
                batch_dfs.append(df[["entity_code", "observed_at", "park_date"]])
                entities_seen.update(df["entity_code"].unique())
                
            except Exception as e:
                logger.warning(f"Error reading {csv_path}: {e}")
                continue
        
        if batch_dfs:
            # Combine batch and update index
            batch_df = pd.concat(batch_dfs, ignore_index=True)
            updated = update_index_from_dataframe(batch_df, index_db, logger)
            total_entities = len(entities_seen)
            logger.info(f"Batch complete: {updated} entities updated, {total_entities} total unique entities")
    
    logger.info(f"Index build complete: {total_entities} unique entities")
    return total_entities


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build or rebuild entity metadata index from fact table CSVs"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing index and rebuild from scratch",
    )
    args = ap.parse_args()
    
    output_base = args.output_base.resolve()
    clean_dir = output_base / "fact_tables" / "clean"
    index_db = output_base / "state" / "entity_index.sqlite"
    log_dir = output_base / "logs"
    
    logger = setup_logging(log_dir)
    logger.info("=" * 70)
    logger.info("Entity Metadata Index Builder")
    logger.info("=" * 70)
    logger.info(f"Output base: {output_base}")
    logger.info(f"Fact tables: {clean_dir}")
    logger.info(f"Index DB: {index_db}")
    logger.info(f"Rebuild: {args.rebuild}")
    logger.info("=" * 70)
    
    entities = scan_and_build_index(clean_dir, index_db, logger, rebuild=args.rebuild)
    
    if entities > 0:
        logger.info(f"Successfully indexed {entities} entities")
    else:
        logger.warning("No entities indexed. Check that fact_tables/clean/ contains CSVs.")
    
    logger.info("Done.")


if __name__ == "__main__":
    main()
