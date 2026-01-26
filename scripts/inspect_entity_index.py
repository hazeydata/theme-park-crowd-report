#!/usr/bin/env python3
"""
Inspect Entity Metadata Index

Quick script to query and display entity index contents.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# Import shared utilities
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.entity_index import get_all_entities, get_entities_needing_modeling
from utils import get_output_base


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect entity metadata index")
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--needing-modeling",
        action="store_true",
        help="Show only entities needing modeling",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of results (default: 20)",
    )
    args = ap.parse_args()
    
    output_base = args.output_base.resolve()
    index_db = output_base / "state" / "entity_index.sqlite"
    
    if not index_db.exists():
        print(f"ERROR: Entity index not found: {index_db}")
        print("Run: python src/build_entity_index.py")
        sys.exit(1)
    
    if args.needing_modeling:
        print("Entities needing modeling:")
        print("=" * 80)
        entities = get_entities_needing_modeling(index_db, min_age_hours=0)
        if not entities:
            print("No entities need modeling.")
        else:
            for i, (entity_code, latest_observed_at, last_modeled_at) in enumerate(entities[:args.limit], 1):
                print(f"{i}. {entity_code}")
                print(f"   Latest observed: {latest_observed_at}")
                print(f"   Last modeled: {last_modeled_at or 'Never'}")
                print()
            if len(entities) > args.limit:
                print(f"... and {len(entities) - args.limit} more")
    else:
        print("All entities in index:")
        print("=" * 80)
        df = get_all_entities(index_db)
        if df.empty:
            print("Index is empty.")
        else:
            print(f"Total entities: {len(df)}")
            print()
            print(df.head(args.limit).to_string(index=False))
            if len(df) > args.limit:
                print(f"\n... and {len(df) - args.limit} more")
            
            # Summary stats
            print()
            print("Summary:")
            print(f"  Total entities: {len(df)}")
            print(f"  Entities with last_modeled_at: {df['last_modeled_at'].notna().sum()}")
            print(f"  Entities never modeled: {df['last_modeled_at'].isna().sum()}")
            if 'row_count' in df.columns:
                print(f"  Total rows indexed: {df['row_count'].sum():,}")
                print(f"  Avg rows per entity: {df['row_count'].mean():.1f}")


if __name__ == "__main__":
    main()
