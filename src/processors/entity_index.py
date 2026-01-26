"""
Entity Metadata Index

================================================================================
PURPOSE
================================================================================
Tracks per-entity metadata (latest observation date, latest timestamp, etc.)
to enable efficient modeling workflows:
  - Find entities with new observations (need re-modeling)
  - Load entity data selectively (only relevant park-date CSVs)
  - Avoid full scans of fact tables

The index is a SQLite database that's updated incrementally when new fact
CSVs are written. Each entity belongs to exactly one park (derived from
entity_code prefix).

================================================================================
SCHEMA
================================================================================
CREATE TABLE entity_index (
    entity_code TEXT PRIMARY KEY,
    latest_park_date TEXT NOT NULL,      -- YYYY-MM-DD, max date with observations
    latest_observed_at TEXT NOT NULL,   -- ISO 8601 timestamp, max observed_at
    row_count INTEGER DEFAULT 0,         -- Total rows for this entity (optional, for stats)
    last_modeled_at TEXT,                -- ISO 8601 timestamp, when we last ran modeling
    first_seen_at TEXT NOT NULL,        -- ISO 8601 timestamp, when first added to index
    updated_at TEXT NOT NULL             -- ISO 8601 timestamp, last update
);

================================================================================
USAGE
================================================================================
  # Update index from a DataFrame (called during ETL write)
  update_index_from_dataframe(df, index_db_path)

  # Find entities needing re-modeling
  entities = get_entities_needing_modeling(index_db_path, min_age_hours=0)

  # Load entity data (selective CSV reading)
  df = load_entity_data(entity_code, output_base, index_db_path)

  # Mark entity as modeled
  mark_entity_modeled(entity_code, index_db_path)
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from zoneinfo import ZoneInfo

# Import park code derivation (entity_code prefix -> park)
# Avoid circular import by importing only what we need
import sys
if str(Path(__file__).parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent))

# PARK_CODE_MAP and get_park_code logic (inline to avoid circular import)
_PARK_CODE_MAP = {
    "MK": "mk", "EP": "ep", "HS": "hs", "AK": "ak", "BB": "bb", "TL": "tl",
    "DL": "dl", "CA": "ca",
    "TDL": "tdl", "TDS": "tds",
    "IA": "ia", "UF": "uf", "EU": "eu", "USH": "uh",
}

def _get_park_code_from_entity(entity_code: str) -> str:
    """Derive park code from entity_code prefix (e.g. MK101 -> mk)."""
    if not entity_code:
        return ""
    import re
    s = str(entity_code).upper().strip()
    m = re.search(r"\d", s)
    prefix = s[: m.start()] if m else s
    return _PARK_CODE_MAP.get(prefix, prefix.lower())


# =============================================================================
# DATABASE SETUP
# =============================================================================

def ensure_index_db(db_path: Path) -> None:
    """Create entity_index table if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_index (
                entity_code TEXT PRIMARY KEY,
                latest_park_date TEXT NOT NULL,
                latest_observed_at TEXT NOT NULL,
                row_count INTEGER DEFAULT 0,
                last_modeled_at TEXT,
                first_seen_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_latest_observed_at 
            ON entity_index(latest_observed_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_last_modeled_at 
            ON entity_index(last_modeled_at)
        """)
        conn.commit()


# =============================================================================
# INDEX UPDATES (from DataFrame)
# =============================================================================

def update_index_from_dataframe(
    df: pd.DataFrame,
    db_path: Path,
    logger: Optional[logging.Logger] = None,
) -> int:
    """
    Update entity index from a DataFrame of fact rows.
    
    For each unique entity_code in df:
      - Update latest_park_date if this date is newer
      - Update latest_observed_at if any timestamp is newer
      - Increment row_count
      - Set updated_at = now()
    
    Args:
        df: DataFrame with columns: entity_code, observed_at, and optionally park_date
        db_path: Path to SQLite index database
        logger: Optional logger
    
    Returns:
        Number of entities updated
    """
    if df.empty or "entity_code" not in df.columns or "observed_at" not in df.columns:
        return 0
    
    ensure_index_db(db_path)
    
    # Derive park_date if not present (from observed_at using 6am rule)
    if "park_date" not in df.columns:
        # Need timezone - use Eastern as default (most parks)
        # In practice, this should be passed in or derived per entity
        # For now, we'll parse observed_at and derive date from it
        df = df.copy()
        df["park_date"] = pd.to_datetime(df["observed_at"], errors="coerce").dt.date.astype(str)
    
    # Aggregate per entity: max park_date, max observed_at, count
    agg = df.groupby("entity_code").agg({
        "park_date": "max",
        "observed_at": "max",
        "entity_code": "count",  # row count
    }).rename(columns={"entity_code": "row_count_new"})
    
    now = datetime.now(ZoneInfo("UTC")).isoformat()
    
    updated = 0
    with sqlite3.connect(str(db_path)) as conn:
        for entity_code, row in agg.iterrows():
            latest_park_date = str(row["park_date"])
            latest_observed_at = str(row["observed_at"])
            row_count_new = int(row["row_count_new"])
            
            # Check if entity exists
            cursor = conn.execute(
                "SELECT latest_park_date, latest_observed_at, row_count, first_seen_at FROM entity_index WHERE entity_code = ?",
                (entity_code,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing entity
                existing_park_date = existing[0]
                existing_observed_at = existing[1]
                existing_row_count = existing[2] or 0
                first_seen_at = existing[3]
                
                # Only update if we have newer data
                if latest_park_date > existing_park_date or latest_observed_at > existing_observed_at:
                    conn.execute("""
                        UPDATE entity_index
                        SET latest_park_date = ?,
                            latest_observed_at = ?,
                            row_count = row_count + ?,
                            updated_at = ?
                        WHERE entity_code = ?
                    """, (
                        max(latest_park_date, existing_park_date),
                        max(latest_observed_at, existing_observed_at),
                        row_count_new,
                        now,
                        entity_code,
                    ))
                    updated += 1
            else:
                # Insert new entity
                conn.execute("""
                    INSERT INTO entity_index 
                    (entity_code, latest_park_date, latest_observed_at, row_count, first_seen_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (entity_code, latest_park_date, latest_observed_at, row_count_new, now, now))
                updated += 1
        
        conn.commit()
    
    if logger:
        logger.debug(f"Entity index: updated {updated} entities from {len(agg)} unique entities")
    
    return updated


# =============================================================================
# QUERY ENTITIES
# =============================================================================

def get_entities_needing_modeling(
    db_path: Path,
    min_age_hours: float = 0.0,
    logger: Optional[logging.Logger] = None,
) -> List[tuple[str, str, Optional[str]]]:
    """
    Find entities that need re-modeling (have new observations since last_modeled_at).
    
    Args:
        db_path: Path to SQLite index database
        min_age_hours: Only return entities where latest_observed_at is at least
                       this many hours old (to avoid modeling on very fresh data)
        logger: Optional logger
    
    Returns:
        List of (entity_code, latest_observed_at, last_modeled_at) tuples
    """
    if not db_path.exists():
        if logger:
            logger.warning(f"Entity index not found: {db_path}")
        return []
    
    ensure_index_db(db_path)
    
    cutoff = None
    if min_age_hours > 0:
        cutoff_dt = datetime.now(ZoneInfo("UTC")) - pd.Timedelta(hours=min_age_hours)
        cutoff = cutoff_dt.isoformat()
    
    with sqlite3.connect(str(db_path)) as conn:
        if cutoff:
            cursor = conn.execute("""
                SELECT entity_code, latest_observed_at, last_modeled_at
                FROM entity_index
                WHERE (last_modeled_at IS NULL OR latest_observed_at > last_modeled_at)
                  AND latest_observed_at <= ?
                ORDER BY latest_observed_at DESC
            """, (cutoff,))
        else:
            cursor = conn.execute("""
                SELECT entity_code, latest_observed_at, last_modeled_at
                FROM entity_index
                WHERE last_modeled_at IS NULL OR latest_observed_at > last_modeled_at
                ORDER BY latest_observed_at DESC
            """)
        
        results = cursor.fetchall()
    
    if logger:
        logger.info(f"Found {len(results)} entities needing modeling")
    
    return results


def get_all_entities(db_path: Path) -> pd.DataFrame:
    """Get all entities from index as DataFrame."""
    if not db_path.exists():
        return pd.DataFrame()
    
    ensure_index_db(db_path)
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query("SELECT * FROM entity_index ORDER BY entity_code", conn)


# =============================================================================
# MARK ENTITY AS MODELED
# =============================================================================

def mark_entity_modeled(
    entity_code: str,
    db_path: Path,
    modeled_at: Optional[str] = None,
) -> None:
    """
    Mark an entity as modeled (set last_modeled_at = now()).
    
    Args:
        entity_code: Entity to mark
        db_path: Path to SQLite index database
        modeled_at: Optional ISO timestamp (defaults to now)
    """
    ensure_index_db(db_path)
    if modeled_at is None:
        modeled_at = datetime.now(ZoneInfo("UTC")).isoformat()
    
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE entity_index SET last_modeled_at = ? WHERE entity_code = ?",
            (modeled_at, entity_code)
        )
        conn.commit()


# =============================================================================
# LOAD ENTITY DATA (selective CSV reading)
# =============================================================================

def load_entity_data(
    entity_code: str,
    output_base: Path,
    db_path: Optional[Path] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Load all fact rows for a specific entity by reading only relevant park-date CSVs.
    
    Since each entity belongs to exactly one park (derived from entity_code prefix),
    we only need to scan CSVs for that park.
    
    Args:
        entity_code: Entity to load (e.g., "MK101")
        output_base: Pipeline output base directory
        db_path: Optional path to index DB (for date range hints - future optimization)
        logger: Optional logger
    
    Returns:
        DataFrame with columns: entity_code, observed_at, wait_time_type, wait_time_minutes
    """
    # Derive park from entity_code prefix
    park_code = _get_park_code_from_entity(entity_code)
    if not park_code:
        if logger:
            logger.warning(f"Could not derive park from entity_code: {entity_code}")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
    
    clean_dir = output_base / "fact_tables" / "clean"
    if not clean_dir.exists():
        if logger:
            logger.warning(f"Fact tables directory not found: {clean_dir}")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
    
    # Find all CSVs for this park
    csvs: List[Path] = []
    for csv_path in clean_dir.rglob("*.csv"):
        # Parse park from filename: {park}_{YYYY-MM-DD}.csv
        stem = csv_path.stem
        if "_" in stem:
            park_from_file = stem.split("_")[0]
            if park_from_file == park_code:
                csvs.append(csv_path)
    
    if not csvs:
        if logger:
            logger.debug(f"No CSVs found for park {park_code} (entity {entity_code})")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
    
    # Load and filter each CSV
    dfs: List[pd.DataFrame] = []
    for csv_path in sorted(csvs):
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            if "entity_code" not in df.columns:
                continue
            # Filter to this entity
            entity_df = df[df["entity_code"] == entity_code].copy()
            if not entity_df.empty:
                dfs.append(entity_df[["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]])
        except Exception as e:
            if logger:
                logger.warning(f"Error reading {csv_path}: {e}")
            continue
    
    if not dfs:
        if logger:
            logger.debug(f"No data found for entity {entity_code}")
        return pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
    
    result = pd.concat(dfs, ignore_index=True)
    result = result.sort_values("observed_at").reset_index(drop=True)
    
    if logger:
        logger.debug(f"Loaded {len(result)} rows for entity {entity_code} from {len(dfs)} CSVs")
    
    return result
