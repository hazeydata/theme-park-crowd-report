#!/usr/bin/env python3
"""
Find entities with ACTUAL data across different parks.

Quick script to identify entities that have ACTUAL wait time observations
for training purposes.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import sqlite3
from processors.entity_index import load_entity_data
from utils.paths import get_output_base


def main() -> None:
    base = Path(get_output_base())
    index_db = base / "state" / "entity_index.sqlite"
    
    # Get all entities from index
    conn = sqlite3.connect(str(index_db))
    df_index = pd.read_sql_query(
        "SELECT entity_code, latest_park_date FROM entity_index ORDER BY latest_park_date DESC",
        conn
    )
    conn.close()
    
    print(f"Total entities in index: {len(df_index)}")
    print("\nFinding entities with ACTUAL data...")
    
    entities_with_actual = []
    park_counts = {}
    
    # Check CSVs directly for faster lookup
    clean_dir = base / "fact_tables" / "clean"
    if not clean_dir.exists():
        print(f"Fact tables directory not found: {clean_dir}")
        return
    
    # Sample a few CSVs to find entities with ACTUAL data
    csv_files = list(clean_dir.rglob("*.csv"))[:100]  # Sample first 100 CSVs
    
    print(f"Sampling {len(csv_files)} CSV files...")
    
    entities_found = set()
    
    for csv_path in csv_files:
        try:
            # Read just a sample to check for ACTUAL data
            df = pd.read_csv(csv_path, nrows=1000, low_memory=False)
            if "wait_time_type" in df.columns and "ACTUAL" in df["wait_time_type"].values:
                # Get unique entities from this file
                if "entity_code" in df.columns:
                    for entity in df[df["wait_time_type"] == "ACTUAL"]["entity_code"].unique():
                        entities_found.add(entity)
        except Exception as e:
            continue
    
    print(f"Found {len(entities_found)} entities with ACTUAL data in sample")
    
    # Now check full counts for these entities
    for entity in list(entities_found)[:50]:  # Limit to 50 for speed
        try:
            df = load_entity_data(entity, base, db_path=index_db, logger=None)
            if not df.empty and "ACTUAL" in df["wait_time_type"].values:
                actual_count = len(df[df["wait_time_type"] == "ACTUAL"])
                # Derive park code from entity_code prefix
                park_code = entity[:2].lower() if len(entity) >= 2 else "unknown"
                entities_with_actual.append((entity, park_code, actual_count))
                
                if park_code not in park_counts:
                    park_counts[park_code] = []
                park_counts[park_code].append(entity)
        except Exception as e:
            continue
    
    print(f"\nFound {len(entities_with_actual)} entities with ACTUAL data:")
    print("\nBy park:")
    for park_code, entities in sorted(park_counts.items()):
        print(f"  {park_code.upper()}: {len(entities)} entities")
        print(f"    Sample: {', '.join(entities[:5])}")
    
    print(f"\nTop 30 entities with most ACTUAL data:")
    entities_with_actual.sort(key=lambda x: x[2], reverse=True)
    for entity, park_code, count in entities_with_actual[:30]:
        print(f"  {entity} ({park_code}): {count:,} ACTUAL rows")
    
    # Select diverse entities for training
    print("\n\nSuggested entities for training (diverse parks):")
    selected = []
    parks_seen = set()
    
    for entity, park_code, count in entities_with_actual:
        if park_code not in parks_seen or len(selected) < 20:
            selected.append(entity)
            parks_seen.add(park_code)
            if len(selected) >= 20:
                break
    
    print(f"Selected {len(selected)} entities:")
    for entity in selected:
        print(f"  {entity}")


if __name__ == "__main__":
    main()
