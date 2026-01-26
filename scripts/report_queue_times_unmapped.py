#!/usr/bin/env python3
"""
Queue-Times Unmapped Attractions Report

================================================================================
PURPOSE
================================================================================
Identifies queue-times.com attractions that have no row in
config/queue_times_entity_mapping.csv. Outputs a reviewable CSV to add new
mappings.

  1. Fetches parks and queue_times from queue-times.com API
  2. Collects (park_code, queue_times_id, queue_times_name) for all rides in
     mapped parks (QUEUE_TIMES_PARK_MAP)
  3. Left-joins to queue_times_entity_mapping on (park_code, queue_times_id)
  4. Writes unattributed rows to reports/queue_times_unmapped.csv

================================================================================
OUTPUT
================================================================================
  - CSV: reports/queue_times_unmapped.csv
    Columns: park_code, queue_times_id, queue_times_name, last_seen (report run date, YYYY-MM-DD)
  - Override with --report. Use --output-base to match pipeline output.

Usage:
  python scripts/report_queue_times_unmapped.py
  python scripts/report_queue_times_unmapped.py --output-base "D:\\Path" --report reports/unmapped.csv
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Allow importing from src
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
from get_wait_times_from_queue_times import (
    QUEUE_TIMES_PARK_MAP,
    fetch_park_wait_times,
    fetch_parks,
)
from utils import get_output_base


def _collect_feed(parks: list, logger: logging.Logger) -> pd.DataFrame:
    """From queue-times API, collect (park_code, queue_times_id, queue_times_name) for all rides in mapped parks."""
    rows = []
    for park in parks:
        park_id = park.get("id")
        if park_id not in QUEUE_TIMES_PARK_MAP:
            continue
        park_code = QUEUE_TIMES_PARK_MAP[park_id]
        data = fetch_park_wait_times(park_id, logger)
        if data is None:
            continue
        # Same shape as get_wait_times: lands[].rides and top-level rides
        rides = []
        if "lands" in data:
            for land in data["lands"]:
                if "rides" in land:
                    rides.extend(land["rides"])
        if "rides" in data:
            rides.extend(data["rides"])
        for r in rides:
            rid = r.get("id")
            name = r.get("name", "")
            if rid is None:
                continue
            rows.append({"park_code": park_code, "queue_times_id": float(rid), "queue_times_name": str(name or "").strip()})
    if not rows:
        return pd.DataFrame(columns=["park_code", "queue_times_id", "queue_times_name"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["park_code", "queue_times_id"])
    return df


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Report queue-times.com attractions not in queue_times_entity_mapping.csv"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base (from config or default)",
    )
    ap.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Output path for CSV (default: output_base/reports/queue_times_unmapped.csv)",
    )
    args = ap.parse_args()
    base = args.output_base.resolve()
    out = args.report.resolve() if args.report else (base / "reports" / "queue_times_unmapped.csv")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logger = logging.getLogger(__name__)

    # Load mapping
    config_dir = Path(__file__).resolve().parent.parent / "config"
    mapping_path = config_dir / "queue_times_entity_mapping.csv"
    if not mapping_path.exists():
        logger.error(f"Mapping not found: {mapping_path}")
        sys.exit(1)
    mapping = pd.read_csv(mapping_path)
    mapping["queue_times_id"] = pd.to_numeric(mapping["queue_times_id"], errors="coerce")
    mapping = mapping[["park_code", "queue_times_id", "entity_code"]].dropna(subset=["queue_times_id"])

    # Fetch feed
    parks = fetch_parks(logger)
    if not parks:
        logger.error("Failed to fetch parks")
        sys.exit(1)
    feed = _collect_feed(parks, logger)
    if feed.empty:
        logger.warning("No rides collected from queue-times API")
        unmapped = pd.DataFrame(columns=["park_code", "queue_times_id", "queue_times_name", "last_seen"])
    else:
        # Left join: unmapped = feed rows with no matching entity_code
        merged = feed.merge(
            mapping,
            on=["park_code", "queue_times_id"],
            how="left",
        )
        unmapped = merged[merged["entity_code"].isna()][["park_code", "queue_times_id", "queue_times_name"]].copy()
        unmapped["last_seen"] = date.today().isoformat()

    # Write
    out.parent.mkdir(parents=True, exist_ok=True)
    unmapped.to_csv(out, index=False)
    logger.info(
        f"Feed: {len(feed)} attractions, mapped: {len(feed) - len(unmapped)}, unmapped: {len(unmapped)}. "
        f"Report: {out}"
    )


if __name__ == "__main__":
    main()
