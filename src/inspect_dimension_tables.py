#!/usr/bin/env python3
"""
Inspect Dimension Tables

Inspects all dimension tables to show actual columns, data types, null counts,
and sample values. Use this to understand the actual schema before creating
cleaning scripts.

Usage:
    python src/inspect_dimension_tables.py
    python src/inspect_dimension_tables.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from utils import get_output_base

DIMENSION_TABLES = [
    "dimentity.csv",
    "dimparkhours.csv",
    "dimeventdays.csv",
    "dimevents.csv",
    "dimmetatable.csv",
    "dimdategroupid.csv",
    "dimseason.csv",
]


def inspect_table(path: Path, table_name: str) -> None:
    """Inspect a single dimension table and print its structure."""
    if not path.exists():
        print(f"\n{'='*80}")
        print(f"{table_name}: FILE NOT FOUND")
        print(f"Path: {path}")
        return

    try:
        df = pd.read_csv(path, low_memory=False, nrows=1000)  # Sample first 1000 rows
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"{table_name}: ERROR READING")
        print(f"Path: {path}")
        print(f"Error: {e}")
        return

    print(f"\n{'='*80}")
    print(f"{table_name}")
    print(f"{'='*80}")
    print(f"Path: {path}")
    print(f"Rows (sample): {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nColumn Details:")
    print(f"{'Column':<30} {'Type':<15} {'Nulls':<10} {'Sample Values'}")
    print("-" * 80)

    for col in df.columns:
        dtype = str(df[col].dtype)
        null_count = df[col].isna().sum()
        null_pct = (null_count / len(df) * 100) if len(df) > 0 else 0
        
        # Get sample non-null values
        non_null = df[col].dropna()
        if len(non_null) > 0:
            samples = non_null.head(3).tolist()
            sample_str = ", ".join([str(s)[:30] for s in samples])
            if len(non_null) > 3:
                sample_str += "..."
        else:
            sample_str = "(all null)"

        print(f"{col:<30} {dtype:<15} {null_count:>5} ({null_pct:>5.1f}%)  {sample_str}")

    # Check for empty strings
    empty_strings = {}
    for col in df.columns:
        if df[col].dtype == "object":
            empty = (df[col] == "").sum()
            if empty > 0:
                empty_strings[col] = empty
    
    if empty_strings:
        print(f"\nEmpty Strings (should be NULL):")
        for col, count in empty_strings.items():
            print(f"  {col}: {count} empty strings")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Inspect dimension tables to see actual columns and data"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    dim_dir = base / "dimension_tables"

    print("=" * 80)
    print("Dimension Tables Inspection")
    print("=" * 80)
    print(f"Output base: {base}")
    print(f"Dimension tables directory: {dim_dir}")

    if not dim_dir.exists():
        print(f"\nERROR: Dimension tables directory does not exist: {dim_dir}")
        print("Run dimension fetch scripts first:")
        print("  python src/get_entity_table_from_s3.py")
        print("  python src/get_park_hours_from_s3.py")
        print("  python src/get_events_from_s3.py")
        print("  python src/get_metatable_from_s3.py")
        print("  python src/build_dimdategroupid.py")
        print("  python src/build_dimseason.py")
        return

    for table_name in DIMENSION_TABLES:
        path = dim_dir / table_name
        inspect_table(path, table_name)

    print(f"\n{'='*80}")
    print("Inspection complete.")
    print("=" * 80)


if __name__ == "__main__":
    main()
