#!/usr/bin/env python3
"""
Test Entity Metadata Index

================================================================================
PURPOSE
================================================================================
Comprehensive tests for the entity metadata index:
  - Index creation and schema
  - Updating from DataFrames
  - Querying entities needing modeling
  - Loading entity data (selective CSV reading)
  - Marking entities as modeled
  - Incremental updates

================================================================================
USAGE
================================================================================
  # Run all tests
  python tests/test_entity_index.py

  # Run with verbose output
  python tests/test_entity_index.py --verbose
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

# Import module under test
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.entity_index import (
    ensure_index_db,
    get_all_entities,
    get_entities_needing_modeling,
    load_entity_data,
    mark_entity_modeled,
    update_index_from_dataframe,
)


# =============================================================================
# TEST HELPERS
# =============================================================================

def create_test_csv(
    csv_path: Path,
    entity_code: str,
    observed_at: str,
    wait_time_type: str = "POSTED",
    wait_time_minutes: int = 30,
) -> None:
    """Create a test fact CSV with one row."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({
        "entity_code": [entity_code],
        "observed_at": [observed_at],
        "wait_time_type": [wait_time_type],
        "wait_time_minutes": [wait_time_minutes],
    })
    df.to_csv(csv_path, index=False)


def assert_equal(actual, expected, msg: str = ""):
    """Assert two values are equal."""
    if actual != expected:
        raise AssertionError(f"{msg}\n  Expected: {expected}\n  Actual: {actual}")


def assert_true(condition, msg: str = ""):
    """Assert condition is True."""
    if not condition:
        raise AssertionError(f"{msg}\n  Condition was False")


# =============================================================================
# TESTS
# =============================================================================

def test_index_creation(tmp_dir: Path, verbose: bool) -> bool:
    """Test that index database is created with correct schema."""
    if verbose:
        print("Test 1: Index creation and schema")
    
    index_db = tmp_dir / "entity_index.sqlite"
    
    # Ensure parent directory exists and is writable
    index_db.parent.mkdir(parents=True, exist_ok=True)
    if verbose:
        print(f"  Creating index at: {index_db}")
        print(f"  Parent exists: {index_db.parent.exists()}")
        print(f"  Parent writable: {index_db.parent.is_dir()}")
    
    ensure_index_db(index_db)
    
    assert_true(index_db.exists(), "Index database should exist")
    
    # Verify schema by querying
    import sqlite3
    with sqlite3.connect(str(index_db)) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='entity_index'")
        assert_true(cursor.fetchone() is not None, "entity_index table should exist")
        
        cursor = conn.execute("PRAGMA table_info(entity_index)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        assert_true("entity_code" in columns, "Should have entity_code column")
        assert_true("latest_park_date" in columns, "Should have latest_park_date column")
        assert_true("latest_observed_at" in columns, "Should have latest_observed_at column")
        assert_true("last_modeled_at" in columns, "Should have last_modeled_at column")
    
    if verbose:
        print("  ✓ Index created with correct schema")
    return True


def test_index_update(tmp_dir: Path, verbose: bool) -> bool:
    """Test updating index from DataFrame."""
    if verbose:
        print("Test 2: Index update from DataFrame")
    
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    # Create test DataFrame
    now = datetime.now(ZoneInfo("UTC"))
    df = pd.DataFrame({
        "entity_code": ["MK101", "MK101", "EP09"],
        "observed_at": [
            (now - timedelta(hours=2)).isoformat(),
            (now - timedelta(hours=1)).isoformat(),  # Newer
            (now - timedelta(hours=3)).isoformat(),
        ],
        "park_date": ["2026-01-25", "2026-01-25", "2026-01-24"],
    })
    
    # Update index
    updated = update_index_from_dataframe(df, index_db, None)
    assert_equal(updated, 2, "Should update 2 entities (MK101, EP09)")
    
    # Verify MK101 has latest timestamp
    all_entities = get_all_entities(index_db)
    assert_true(len(all_entities) == 2, "Should have 2 entities")
    
    mk101 = all_entities[all_entities["entity_code"] == "MK101"].iloc[0]
    assert_equal(mk101["latest_park_date"], "2026-01-25", "MK101 latest_park_date")
    assert_true(
        mk101["latest_observed_at"] == (now - timedelta(hours=1)).isoformat(),
        "MK101 should have latest observed_at"
    )
    assert_equal(mk101["row_count"], 2, "MK101 should have row_count=2")
    
    if verbose:
        print(f"  ✓ Updated {updated} entities correctly")
    return True


def test_incremental_update(tmp_dir: Path, verbose: bool) -> bool:
    """Test that incremental updates work (updating existing entities)."""
    if verbose:
        print("Test 3: Incremental updates")
    
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    now = datetime.now(ZoneInfo("UTC"))
    
    # First update
    df1 = pd.DataFrame({
        "entity_code": ["MK101"],
        "observed_at": [(now - timedelta(days=2)).isoformat()],
        "park_date": ["2026-01-23"],
    })
    update_index_from_dataframe(df1, index_db, None)
    
    # Second update with newer data
    df2 = pd.DataFrame({
        "entity_code": ["MK101"],
        "observed_at": [(now - timedelta(hours=1)).isoformat()],
        "park_date": ["2026-01-25"],
    })
    update_index_from_dataframe(df2, index_db, None)
    
    # Verify latest values
    all_entities = get_all_entities(index_db)
    mk101 = all_entities[all_entities["entity_code"] == "MK101"].iloc[0]
    assert_equal(mk101["latest_park_date"], "2026-01-25", "Should update to latest date")
    assert_true(
        mk101["latest_observed_at"] == (now - timedelta(hours=1)).isoformat(),
        "Should update to latest timestamp"
    )
    assert_equal(mk101["row_count"], 2, "Should accumulate row_count")
    
    if verbose:
        print("  ✓ Incremental updates work correctly")
    return True


def test_query_entities_needing_modeling(tmp_dir: Path, verbose: bool) -> bool:
    """Test querying entities that need re-modeling."""
    if verbose:
        print("Test 4: Query entities needing modeling")
    
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    now = datetime.now(ZoneInfo("UTC"))
    
    # Add entities with different states
    df = pd.DataFrame({
        "entity_code": ["MK101", "EP09", "HS05"],
        "observed_at": [
            (now - timedelta(hours=1)).isoformat(),  # New
            (now - timedelta(days=2)).isoformat(),   # Old
            (now - timedelta(hours=1)).isoformat(),  # New
        ],
        "park_date": ["2026-01-25", "2026-01-23", "2026-01-25"],
    })
    update_index_from_dataframe(df, index_db, None)
    
    # Mark EP09 as modeled (old data, already modeled)
    mark_entity_modeled("EP09", index_db)
    
    # Mark HS05 as modeled (new data, but already modeled)
    mark_entity_modeled("HS05", index_db)
    
    # Query entities needing modeling
    entities = get_entities_needing_modeling(index_db, min_age_hours=0)
    entity_codes = {e[0] for e in entities}
    
    assert_true("MK101" in entity_codes, "MK101 should need modeling (new data, not modeled)")
    assert_true("EP09" not in entity_codes, "EP09 should not need modeling (old data, already modeled)")
    assert_true("HS05" not in entity_codes, "HS05 should not need modeling (already modeled)")
    
    if verbose:
        print(f"  ✓ Found {len(entities)} entities needing modeling: {entity_codes}")
    return True


def test_load_entity_data(tmp_dir: Path, verbose: bool) -> bool:
    """Test loading entity data from CSVs (selective reading)."""
    if verbose:
        print("Test 5: Load entity data (selective CSV reading)")
    
    output_base = tmp_dir / "output"
    clean_dir = output_base / "fact_tables" / "clean"
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    # Create test CSVs for different parks
    # MK101 in mk park
    create_test_csv(
        clean_dir / "2026-01" / "mk_2026-01-25.csv",
        "MK101",
        (datetime.now(ZoneInfo("UTC")) - timedelta(hours=2)).isoformat(),
        wait_time_minutes=30,
    )
    create_test_csv(
        clean_dir / "2026-01" / "mk_2026-01-24.csv",
        "MK101",
        (datetime.now(ZoneInfo("UTC")) - timedelta(days=1, hours=2)).isoformat(),
        wait_time_minutes=45,
    )
    
    # EP09 in ep park (should not be loaded for MK101)
    create_test_csv(
        clean_dir / "2026-01" / "ep_2026-01-25.csv",
        "EP09",
        (datetime.now(ZoneInfo("UTC")) - timedelta(hours=1)).isoformat(),
        wait_time_minutes=20,
    )
    
    # Load MK101 data
    df = load_entity_data("MK101", output_base, index_db, None)
    
    assert_equal(len(df), 2, "Should load 2 rows for MK101")
    assert_true(all(df["entity_code"] == "MK101"), "All rows should be MK101")
    assert_equal(len(df[df["wait_time_minutes"] == 30]), 1, "Should have one row with 30 min")
    assert_equal(len(df[df["wait_time_minutes"] == 45]), 1, "Should have one row with 45 min")
    
    # Verify EP09 data is NOT loaded
    assert_true("EP09" not in df["entity_code"].values, "Should not load EP09 data")
    
    if verbose:
        print(f"  ✓ Loaded {len(df)} rows for MK101 (only mk park CSVs)")
    return True


def test_mark_entity_modeled(tmp_dir: Path, verbose: bool) -> bool:
    """Test marking entity as modeled."""
    if verbose:
        print("Test 6: Mark entity as modeled")
    
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    now = datetime.now(ZoneInfo("UTC"))
    
    # Add entity
    df = pd.DataFrame({
        "entity_code": ["MK101"],
        "observed_at": [(now - timedelta(hours=1)).isoformat()],
        "park_date": ["2026-01-25"],
    })
    update_index_from_dataframe(df, index_db, None)
    
    # Verify not modeled yet
    entities = get_entities_needing_modeling(index_db, min_age_hours=0)
    assert_true("MK101" in {e[0] for e in entities}, "MK101 should need modeling")
    
    # Mark as modeled
    mark_entity_modeled("MK101", index_db)
    
    # Verify modeled
    entities = get_entities_needing_modeling(index_db, min_age_hours=0)
    assert_true("MK101" not in {e[0] for e in entities}, "MK101 should not need modeling after marking")
    
    # Verify last_modeled_at is set
    all_entities = get_all_entities(index_db)
    mk101 = all_entities[all_entities["entity_code"] == "MK101"].iloc[0]
    assert_true(mk101["last_modeled_at"] is not None, "last_modeled_at should be set")
    
    if verbose:
        print("  ✓ Entity marked as modeled correctly")
    return True


def test_min_age_hours_filter(tmp_dir: Path, verbose: bool) -> bool:
    """Test min_age_hours filter in query."""
    if verbose:
        print("Test 7: min_age_hours filter")
    
    index_db = tmp_dir / "entity_index.sqlite"
    ensure_index_db(index_db)
    
    now = datetime.now(ZoneInfo("UTC"))
    
    # Add entities with different ages
    df = pd.DataFrame({
        "entity_code": ["MK101", "EP09"],
        "observed_at": [
            (now - timedelta(minutes=30)).isoformat(),  # Very fresh (30 min old)
            (now - timedelta(hours=25)).isoformat(),    # Older (25 hours old)
        ],
        "park_date": ["2026-01-25", "2026-01-24"],
    })
    update_index_from_dataframe(df, index_db, None)
    
    # Query with min_age_hours=24 (only entities with data at least 24h old)
    entities = get_entities_needing_modeling(index_db, min_age_hours=24)
    entity_codes = {e[0] for e in entities}
    
    assert_true("EP09" in entity_codes, "EP09 should be included (25h old)")
    assert_true("MK101" not in entity_codes, "MK101 should be excluded (30min old, < 24h)")
    
    if verbose:
        print(f"  ✓ min_age_hours filter works: {entity_codes}")
    return True


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Test Entity Metadata Index")
    ap.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = ap.parse_args()
    
    # Create temporary directory for tests (in workspace to avoid Windows permission issues)
    workspace_tmp = Path(__file__).parent.parent / "temp" / "test_entity_index"
    if workspace_tmp.exists():
        shutil.rmtree(workspace_tmp, ignore_errors=True)
    workspace_tmp.mkdir(parents=True, exist_ok=True)
    tmp_dir = workspace_tmp
    
    try:
        
        print("=" * 70)
        print("Entity Metadata Index Tests")
        print("=" * 70)
        print(f"Temp directory: {tmp_dir}")
        print()
        
        tests = [
            ("Index Creation", test_index_creation),
            ("Index Update", test_index_update),
            ("Incremental Update", test_incremental_update),
            ("Query Entities Needing Modeling", test_query_entities_needing_modeling),
            ("Load Entity Data", test_load_entity_data),
            ("Mark Entity Modeled", test_mark_entity_modeled),
            ("Min Age Hours Filter", test_min_age_hours_filter),
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            try:
                if args.verbose:
                    print()
                test_func(tmp_dir, args.verbose)
                passed += 1
                if not args.verbose:
                    print(f"PASS: {test_name}")
            except Exception as e:
                failed += 1
                print(f"FAIL: {test_name}: {e}")
                if args.verbose:
                    import traceback
                    traceback.print_exc()
        
        print()
        print("=" * 70)
        print(f"Results: {passed} passed, {failed} failed")
        print("=" * 70)
        
        # Cleanup
        if workspace_tmp.exists():
            try:
                shutil.rmtree(workspace_tmp, ignore_errors=True)
            except:
                pass
        
        if failed > 0:
            sys.exit(1)
    finally:
        # Final cleanup attempt
        if workspace_tmp.exists():
            try:
                shutil.rmtree(workspace_tmp, ignore_errors=True)
            except:
                pass


if __name__ == "__main__":
    main()
