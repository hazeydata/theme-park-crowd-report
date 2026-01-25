#!/usr/bin/env python3
"""
Theme Park Wait Time Fact Table Builder

================================================================================
PURPOSE
================================================================================
This script is the main ETL (Extract, Transform, Load) pipeline for theme park
wait time data. It:

  1. MERGES yesterday's queue-times from staging/queue_times into fact_tables/clean (then deletes staged files)
  2. READS raw wait time data from AWS S3 (standby wait times + fastpass/priority)
  3. CLASSIFIES each file by type (Standby, New Fastpass, Old Fastpass)
  4. PARSES the data using modular parsers (ported from proven Julia logic)
  5. DEDUPLICATES rows using a persistent SQLite database
  6. DERIVES park codes and operational dates from the data
  7. WRITES clean CSV files organized by park and date

================================================================================
OUTPUT
================================================================================
  - One CSV per (park, date): fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv
  - Each CSV has 4 columns:
      entity_code       (str, uppercase)  e.g. "MK101", "EP09"
      observed_at       (str, ISO with timezone)  e.g. "2024-01-15T10:30:00-05:00"
      wait_time_type    "POSTED" | "ACTUAL" | "PRIORITY"
      wait_time_minutes (int)  30, 45, or 8888 for PRIORITY sellout

================================================================================
KEY FEATURES
================================================================================
  - INCREMENTAL: Only processes new or modified S3 files (tracked in state/)
  - DEDUPLICATION: SQLite DB ensures no duplicate rows across runs
  - FAILED-FILE SKIP: Old files that fail 3+ times are skipped (see OLD_FILE_DAYS)
  - CONNECTION RETRIES: Handles transient S3/network errors automatically
  - PROCESS LOCK: Prevents multiple instances from running at once

================================================================================
MODULES USED
================================================================================
  - src/parsers/wait_time_parsers.py   Standby and fastpass parsers
  - src/utils/file_identification.py  File type classifier (Standby vs Fastpass)
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Dict, Set

import boto3
import pandas as pd
from botocore.exceptions import ClientError, ResponseStreamingError
from botocore.config import Config
from zoneinfo import ZoneInfo

# ----- Ensure we can import from src/ when run from project root -----
if str(Path(__file__).parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent))

from parsers import parse_standby_chunk, parse_fastpass_stream
from utils import get_wait_time_filetype


# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# S3 bucket and path patterns for listing files
DEFAULT_PROPS = ["wdw", "dlr", "uor", "ush", "tdr"]
S3_BUCKET = "touringplans_stats"
S3_STANDBY_PREFIX_FMT = "export/wait_times/{prop}/"
S3_PRIORITY_PREFIX_FMT = "export/fastpass_times/{prop}/"

# Default output base (Dropbox). Override with --output-base.
DEFAULT_OUTPUT_BASE = Path(r"D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report")

# State and output file names (under output_base/state/ and output_base/...)
LOCAL_SAMPLE_NAME = "wait_time_fact_table_sample.csv"
PROCESSED_FILES_JSON = "processed_files.json"
FAILED_FILES_JSON = "failed_files.json"
DEDUPE_DB_NAME = "dedupe.sqlite"
LOCK_FILE_NAME = "processing.lock"

# ----- Skip repeatedly-failed files (e.g. 2014 fastpass that cannot be parsed) -----
# If a file has failed >= FAILED_SKIP_THRESHOLD times AND its S3 LastModified
# is older than OLD_FILE_DAYS, we skip it on future runs instead of retrying.
FAILED_SKIP_THRESHOLD = 3
OLD_FILE_DAYS = 600

# Chunked processing and reservoir sampling
SAMPLE_K = 1000
CHUNKSIZE = 250_000


# =============================================================================
# PARK CODE MAPPING
# =============================================================================
# Maps entity_code prefix (e.g. "MK", "EP") to short lowercase park code.
# Used when deriving park from entity_code for output path and grouping.

PARK_CODE_MAP = {
    "MK": "mk", "EP": "ep", "HS": "hs", "AK": "ak", "BB": "bb", "TL": "tl",
    "DL": "dl", "CA": "ca",
    "TDL": "tdl", "TDS": "tds",
    "IA": "ia", "UF": "uf", "EU": "eu", "USH": "uh",
}


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """
    Set up file-based and console logging for the pipeline run.
    Each run creates a new log file: get_tp_wait_time_data_YYYYMMDD_HHMMSS.log
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"get_tp_wait_time_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# PROCESS LOCK (Prevent Multiple Simultaneous Runs)
# =============================================================================
# Uses a lock file in state/. If another instance is running, we exit.
# Stale locks (older than 24 hours) are removed automatically.

def acquire_lock(lock_file: Path, logger: logging.Logger) -> bool:
    """Acquire process lock. Returns True if lock acquired, False otherwise."""
    try:
        if lock_file.exists():
            lock_age = time.time() - lock_file.stat().st_mtime
            if lock_age > 86400:
                logger.warning(f"Stale lock file (age: {lock_age/3600:.1f}h). Removing.")
                lock_file.unlink()
            else:
                logger.error(f"Lock file exists: {lock_file}")
                logger.error("Another instance may be running. Delete lock file if sure.")
                return False

        lock_file.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_file, "w") as f:
            f.write(f"PID: {os.getpid()}\n")
            f.write(f"Start: {datetime.now().isoformat()}\n")
            f.write(f"Script: {sys.argv[0]}\n")
        return True
    except Exception as e:
        logger.error(f"Error acquiring lock: {e}")
        return False


def release_lock(lock_file: Path, logger: logging.Logger) -> None:
    """Release the process lock (remove lock file)."""
    try:
        if lock_file.exists():
            lock_file.unlink()
    except Exception as e:
        logger.warning(f"Error releasing lock: {e}")


# =============================================================================
# PROCESSED FILES STATE
# =============================================================================
# Tracks which S3 files have been successfully processed. Format: key -> ISO timestamp.
# Used for incremental runs: we only process new files or files modified since last run.

def load_processed_files(state_file: Path) -> Dict[str, str]:
    """Load processed-files state. Returns dict: S3 key -> last_modified ISO string."""
    if not state_file.exists():
        return {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            processed = data.get("processed_files", {})
            if isinstance(processed, list):
                return {k: "" for k in processed}
            return processed
    except Exception as e:
        logging.warning(f"Error loading processed files: {e}. Starting fresh.")
        return {}


def save_processed_files(state_file: Path, processed_files: Dict[str, str]) -> None:
    """Save processed-files state to JSON."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "processed_files": processed_files,
        "last_updated": datetime.now().isoformat(),
        "total_files": len(processed_files),
    }
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving processed files: {e}")


# =============================================================================
# FAILED FILES STATE
# =============================================================================
# Tracks files that have failed (parse errors, connection errors, etc.).
# Used to skip *old* repeatedly-failed files: if failures >= FAILED_SKIP_THRESHOLD
# and file's S3 LastModified is older than OLD_FILE_DAYS, we skip them.

def load_failed_files(state_dir: Path) -> Dict[str, dict]:
    """Load failed-files state. Returns dict: key -> {failures, last_attempt, last_modified}."""
    path = state_dir / FAILED_FILES_JSON
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("failed_files", {})
    except Exception as e:
        logging.warning(f"Error loading failed files: {e}. Starting fresh.")
        return {}


def save_failed_files(state_dir: Path, failed_files: Dict[str, dict]) -> None:
    """Save failed-files state to JSON."""
    path = state_dir / FAILED_FILES_JSON
    state_dir.mkdir(parents=True, exist_ok=True)
    payload = {"failed_files": failed_files, "last_updated": datetime.now().isoformat()}
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        logging.error(f"Error saving failed files: {e}")


def _is_file_old(last_modified: datetime) -> bool:
    """True if the file's last-modified date is older than OLD_FILE_DAYS."""
    now = datetime.now(ZoneInfo("UTC"))
    lm = last_modified
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=ZoneInfo("UTC"))
    else:
        lm = lm.astimezone(ZoneInfo("UTC"))
    return (now - lm).days >= OLD_FILE_DAYS


def _record_failure(failed_files: Dict[str, dict], key: str, last_modified: datetime) -> None:
    """Increment failure count for a file. Called when processing fails."""
    finfo = failed_files.get(key, {})
    n = finfo.get("failures", 0) + 1
    lm_str = last_modified.isoformat() if hasattr(last_modified, "isoformat") else str(last_modified)
    failed_files[key] = {
        "failures": n,
        "last_attempt": datetime.now(ZoneInfo("UTC")).isoformat(),
        "last_modified": lm_str,
    }


# =============================================================================
# S3 HELPERS (Read-Only)
# =============================================================================

def list_s3_csvs(s3, bucket: str, prefix: str) -> List[Tuple[str, datetime]]:
    """List all CSV keys under prefix with their LastModified. Returns (key, last_modified)."""
    keys: List[Tuple[str, datetime]] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".csv"):
                lm = obj.get("LastModified", datetime.now())
                keys.append((k, lm))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    keys.sort(key=lambda x: x[0])
    return keys


def s3_text_stream(s3, bucket: str, key: str):
    """Open S3 object as text stream (UTF-8, errors=replace)."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    return io.TextIOWrapper(obj["Body"], encoding="utf-8", errors="replace", newline="")


# =============================================================================
# DEDUPLICATION (SQLite)
# =============================================================================
# We use a table with PK (entity_code, observed_at, wait_time_type, wait_time_minutes).
# INSERT OR IGNORE; rowcount indicates new vs duplicate. Only new rows are written to CSV.

def ensure_sqlite(conn: sqlite3.Connection) -> None:
    """Create dedupe_keys table if it does not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dedupe_keys (
            entity_code TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            wait_time_type TEXT NOT NULL,
            wait_time_minutes INTEGER NOT NULL,
            PRIMARY KEY (entity_code, observed_at, wait_time_type, wait_time_minutes)
        )
    """)
    conn.commit()


def insert_new_mask(conn: sqlite3.Connection, df: pd.DataFrame) -> pd.Series:
    """Insert rows into dedupe_keys. Returns boolean Series: True = new, False = duplicate."""
    cur = conn.cursor()
    cur.execute("BEGIN")
    mask = []
    for t in zip(
        df["entity_code"],
        df["observed_at"],
        df["wait_time_type"],
        df["wait_time_minutes"].astype(int),
    ):
        cur.execute(
            "INSERT OR IGNORE INTO dedupe_keys(entity_code,observed_at,wait_time_type,wait_time_minutes) VALUES (?,?,?,?)",
            t,
        )
        mask.append(cur.rowcount == 1)
    conn.commit()
    return pd.Series(mask, index=df.index)


# =============================================================================
# TIMEZONE AND PARK HELPERS
# =============================================================================

_TZ_REGEX_HAS_OFFSET = re.compile(r"(?:Z|[+\-]\d{2}:?\d{2})\Z", re.IGNORECASE)


def _prop_from_key(key: str) -> str:
    """Extract property (wdw, dlr, etc.) from S3 key path."""
    parts = key.split("/")
    for marker in ("wait_times", "fastpass_times"):
        if marker in parts:
            i = parts.index(marker)
            if i + 1 < len(parts):
                return parts[i + 1].lower().strip()
    return ""


def _zone_from_key(key: str) -> ZoneInfo:
    """Return timezone for the property implied by the S3 key."""
    prop = _prop_from_key(key)
    if prop == "dlr":
        return ZoneInfo("America/Los_Angeles")
    if prop == "tdr":
        return ZoneInfo("Asia/Tokyo")
    return ZoneInfo("America/New_York")


def ensure_observed_at_has_offset(df: pd.DataFrame, tz: ZoneInfo) -> pd.DataFrame:
    """Ensure observed_at strings have timezone offset; localize if missing."""
    if df.empty or "observed_at" not in df.columns:
        return df
    s = df["observed_at"].astype("string").str.strip()
    missing = ~s.fillna("").str.contains(_TZ_REGEX_HAS_OFFSET, regex=True, na=False)
    if not missing.any():
        return df
    to_parse = s[missing]
    parsed = pd.to_datetime(to_parse, errors="coerce")
    ok = parsed.notna()
    if ok.any():
        localized = parsed[ok].dt.tz_localize(tz, nonexistent="shift_forward", ambiguous="infer")
        formatted = localized.dt.strftime("%Y-%m-%dT%H:%M:%S%z").str.replace(
            r"([+\-]\d{2})(\d{2})$", r"\1:\2", regex=True
        )
        for idx, val in zip(formatted.index, formatted.values):
            df.at[idx, "observed_at"] = val
    return df


def collapse_priority_dupes_keep_last(df: pd.DataFrame) -> pd.DataFrame:
    """For PRIORITY rows, keep last per (entity_code, observed_at)."""
    if df.empty:
        return df
    df = df.sort_values(["entity_code", "observed_at"])
    return df.drop_duplicates(subset=["entity_code", "observed_at"], keep="last")


# =============================================================================
# RESERVOIR SAMPLE (for wait_time_fact_table_sample.csv)
# =============================================================================

def reservoir_update(reservoir: List[pd.Series], row: pd.Series, k: int, seen_n: int) -> None:
    """Update reservoir sample with one row."""
    if len(reservoir) < k:
        reservoir.append(row)
    else:
        j = random.randint(0, seen_n)
        if j < k:
            reservoir[j] = row


# =============================================================================
# PARK CODE AND PARK DATE DERIVATION
# =============================================================================
# Park code: from entity_code prefix (e.g. MK101 -> mk).
# Park date: 6 AM rule — if local hour < 6, use previous calendar date.

def get_park_code(entity_code: pd.Series) -> pd.Series:
    """Derive park code from entity_code (e.g. MK101 -> mk)."""
    def one(ec: str) -> str:
        if pd.isna(ec) or not ec:
            return ""
        s = str(ec).upper().strip()
        m = re.search(r"\d", s)
        prefix = s[: m.start()] if m else s
        return PARK_CODE_MAP.get(prefix, prefix.lower())

    return entity_code.apply(one)


def derive_park_date(observed_at: pd.Series, tz: ZoneInfo) -> pd.Series:
    """Derive park operational date from observed_at using 6 AM rule."""
    dt = pd.to_datetime(observed_at, errors="coerce", utc=True)
    dt_local = dt.dt.tz_convert(tz)
    mask = dt_local.dt.hour < 6
    park_dt = dt_local.copy()
    if mask.any():
        park_dt.loc[mask] = dt_local[mask] - timedelta(days=1)
    return park_dt.dt.date.astype(str)


# =============================================================================
# CSV WRITING (Grouped by Park and Date)
# =============================================================================
# Writes one CSV per (park, park_date). Appends if file exists.

def write_grouped_csvs(df: pd.DataFrame, clean_dir: Path, tz: ZoneInfo, logger: logging.Logger) -> int:
    """Write DataFrame to CSVs grouped by (park, park_date). Returns total rows written."""
    if df.empty:
        return 0
    df = df.copy()
    df["park"] = get_park_code(df["entity_code"])
    df["park_date"] = derive_park_date(df["observed_at"], tz)
    before = len(df)
    df = df.dropna(subset=["park_date"])
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df)} rows with invalid park_date")
    if df.empty:
        return 0

    out_cols = ["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]
    total = 0
    for (park, park_date), grp in df[out_cols + ["park", "park_date"]].groupby(["park", "park_date"]):
        if grp.empty:
            continue
        grp = grp.sort_values("observed_at")
        out = grp[out_cols].copy()
        ym = park_date[:7]
        target_dir = clean_dir / ym
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"{park}_{park_date}.csv"
        exists = path.exists()
        mode, header = ("a", False) if exists else ("w", True)
        out.to_csv(path, mode=mode, header=header, index=False)
        total += len(out)
        action = "Appended" if exists else "Wrote"
        logger.info(f"{action} {len(out)} rows to {path} (park={park}, date={park_date})")
    return total


# =============================================================================
# COMMAND-LINE ARGUMENTS AND OUTPUT DIRECTORIES
# =============================================================================

def parse_args():
    ap = argparse.ArgumentParser(description="Wait Time FACT TABLE builder")
    ap.add_argument("--props", default=",".join(DEFAULT_PROPS), help="Comma-separated properties (wdw,dlr,...)")
    ap.add_argument("--output-base", type=str, default=str(DEFAULT_OUTPUT_BASE), help="Output base directory")
    ap.add_argument("--chunksize", type=int, default=CHUNKSIZE)
    ap.add_argument("--sample-k", type=int, default=SAMPLE_K)
    ap.add_argument("--full-rebuild", action="store_true", help="Process all files, ignore processed state")
    ap.add_argument("--no-incremental", action="store_true", help="Deprecated: use --full-rebuild")
    return ap.parse_args()


def get_output_directories(output_base: Path) -> Dict[str, Path]:
    """Build directory structure under output_base."""
    ym = datetime.now().strftime("%Y-%m")
    dirs = {
        "base": output_base,
        "fact_tables": output_base / "fact_tables",
        "fact_tables_clean": output_base / "fact_tables" / "clean",
        "staging_queue_times": output_base / "staging" / "queue_times",
        "samples": output_base / "samples" / ym,
        "state": output_base / "state",
        "logs": output_base / "logs",
        "work": output_base / "work",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def merge_yesterday_queue_times(dirs: Dict[str, Path], logger: logging.Logger) -> int:
    """
    Append yesterday's queue-times from staging/queue_times into fact_tables/clean,
    then delete the staged files. Uses Eastern date for 'yesterday'.
    Called at the start of the morning ETL so fact_tables stay static until the
    daily load; the queue-times scraper writes to staging only.
    """
    et = ZoneInfo("America/New_York")
    yesterday = (datetime.now(et).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    ym = yesterday[:7]
    staging_dir = dirs["staging_queue_times"] / ym
    if not staging_dir.exists():
        logger.debug(f"Merge queue-times: no staging dir {staging_dir} for yesterday={yesterday}")
        return 0
    total = 0
    pattern = f"*_{yesterday}.csv"
    for path in sorted(staging_dir.glob(pattern)):
        try:
            df = pd.read_csv(path)
            for c in ["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]:
                if c not in df.columns:
                    logger.warning(f"Merge queue-times: skipping {path.name}, missing column {c}")
                    break
            else:
                park = path.stem.replace(f"_{yesterday}", "")
                fact_dir = dirs["fact_tables_clean"] / ym
                fact_dir.mkdir(parents=True, exist_ok=True)
                fact_path = fact_dir / path.name
                exists = fact_path.exists()
                mode, header = ("a", False) if exists else ("w", True)
                df[["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]].to_csv(
                    fact_path, mode=mode, header=header, index=False
                )
                total += len(df)
                logger.info(f"Merged {len(df)} queue-times rows into {fact_path.relative_to(dirs['base'])} (park={park})")
                path.unlink()
        except Exception as e:
            logger.warning(f"Merge queue-times: failed {path.name}: {e}")
    if total:
        logger.info(f"Merge queue-times: appended {total} rows for yesterday={yesterday}")
    return total


# =============================================================================
# SINGLE-FILE PROCESSING
# =============================================================================
# Routes to Standby or Fastpass parser, dedupes, writes CSV. Returns (rows_written, rows_seen).

def process_file(
    s3,
    key: str,
    file_type: str,
    tz: ZoneInfo,
    conn: sqlite3.Connection,
    clean_dir: Path,
    logger: logging.Logger,
    chunksize: int,
    reservoir: List[pd.Series],
    seen: int,
    sample_k: int,
) -> Tuple[int, int]:
    """Process one S3 file. Returns (rows_written, rows_seen). Raises on failure."""
    rows_written = 0
    rows_seen = 0

    try:
        if file_type == "Standby":
            logger.info(f"Processing Standby file: {key}")
            max_retries = 3
            reader = None
            for attempt in range(max_retries):
                try:
                    stream = s3_text_stream(s3, S3_BUCKET, key)
                    reader = pd.read_csv(stream, chunksize=chunksize, low_memory=False)
                    break
                except (ResponseStreamingError, ConnectionError, IOError) as e:
                    if attempt < max_retries - 1:
                        wait = 2 ** attempt
                        logger.warning(f"Connection error reading {key} (attempt {attempt+1}/{max_retries}): {e}. Retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        logger.error(f"Failed to read {key} after {max_retries} attempts: {e}")
                        raise
            if reader is None:
                raise Exception(f"Failed to create reader for {key}")

            for chunk_idx, chunk in enumerate(reader):
                df = parse_standby_chunk(chunk)
                if df.empty:
                    continue
                df = ensure_observed_at_has_offset(df, tz)
                df = df.dropna(subset=["entity_code", "observed_at", "wait_time_minutes"])
                df["wait_time_minutes"] = pd.to_numeric(df["wait_time_minutes"], errors="coerce").astype("Int64")
                df = df.dropna(subset=["wait_time_minutes"])
                if df.empty:
                    continue
                new_mask = insert_new_mask(conn, df)
                new_df = df.loc[new_mask]
                if new_df.empty:
                    continue
                rows_seen += len(df)
                chunk_written = write_grouped_csvs(new_df, clean_dir, tz, logger)
                rows_written += chunk_written
                for _, r in new_df.iterrows():
                    reservoir_update(reservoir, r, sample_k, seen)
                    seen += 1

        elif file_type in ("New Fastpass", "Old Fastpass"):
            logger.info(f"Processing {file_type} file: {key}")
            for chunk_idx, out in enumerate(parse_fastpass_stream(s3, S3_BUCKET, key, chunksize, file_type=file_type)):
                if out.empty:
                    continue
                before = len(out)
                out = collapse_priority_dupes_keep_last(out)
                collapsed = before - len(out)
                if collapsed:
                    logger.info(f"Collapsed {collapsed} duplicate PRIORITY timestamps in {key}")
                out = ensure_observed_at_has_offset(out, tz)
                out = out.dropna(subset=["entity_code", "observed_at", "wait_time_minutes"])
                out["wait_time_minutes"] = pd.to_numeric(out["wait_time_minutes"], errors="coerce").astype("Int64")
                out = out.dropna(subset=["wait_time_minutes"])
                if out.empty:
                    continue
                new_mask = insert_new_mask(conn, out)
                new_df = out.loc[new_mask]
                if new_df.empty:
                    continue
                rows_seen += len(out)
                chunk_written = write_grouped_csvs(new_df, clean_dir, tz, logger)
                rows_written += chunk_written
                for _, r in new_df.iterrows():
                    reservoir_update(reservoir, r, sample_k, seen)
                    seen += 1

        else:
            logger.warning(f"Skipping unknown file type: {key}")
            return 0, 0

        logger.info(f"Completed {key}: wrote {rows_written} new rows (seen {rows_seen} total)")
        return rows_written, rows_seen

    except Exception as e:
        logger.error(f"Error processing {key}: {e}", exc_info=True)
        raise


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    # ----- Parse CLI and set up paths -----
    args = parse_args()
    props = [p.strip() for p in args.props.split(",") if p.strip()]
    if args.no_incremental:
        args.full_rebuild = True
        logging.warning("--no-incremental is deprecated. Use --full-rebuild.")

    output_base = Path(args.output_base).resolve()
    dirs = get_output_directories(output_base)

    # ----- STEP 1: Logging -----
    logger = setup_logging(dirs["logs"])
    logger.info("=" * 70)
    logger.info("Starting Wait Time Fact Table Build")
    logger.info(f"Output base: {output_base}")
    logger.info(f"Properties: {', '.join(props)}")
    logger.info(f"Full rebuild: {args.full_rebuild}")
    logger.info("=" * 70)

    # ----- STEP 1b: Merge yesterday's queue-times from staging into fact_tables -----
    merge_yesterday_queue_times(dirs, logger)

    # ----- STEP 2: Load state (processed + failed files) -----
    state_file = dirs["state"] / PROCESSED_FILES_JSON
    if args.full_rebuild:
        processed_files: Dict[str, str] = {}
        logger.info("Full rebuild: ignoring processed-files tracking")
    else:
        processed_files = load_processed_files(state_file)
        logger.info(f"Loaded {len(processed_files)} previously processed files")

    failed_files: Dict[str, dict] = load_failed_files(dirs["state"])
    if failed_files:
        logger.info(f"Loaded {len(failed_files)} failed-files entries (skip if old + >={FAILED_SKIP_THRESHOLD} failures)")

    # ----- STEP 3: S3 client with retries -----
    try:
        config = Config(
            retries={"max_attempts": 5, "mode": "adaptive"},
            read_timeout=300,
            connect_timeout=60,
        )
        s3 = boto3.client("s3", config=config)
        logger.info("S3 client initialized with retry configuration")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        sys.exit(1)

    local_sample = dirs["samples"] / LOCAL_SAMPLE_NAME
    dedupe_db = dirs["state"] / DEDUPE_DB_NAME
    lock_file = dirs["state"] / LOCK_FILE_NAME

    # ----- STEP 4: Acquire process lock -----
    if not acquire_lock(lock_file, logger):
        logger.error("=" * 70)
        logger.error("ANOTHER INSTANCE IS ALREADY RUNNING")
        logger.error("=" * 70)
        logger.error(f"Lock file: {lock_file}")
        sys.exit(1)
    logger.info("Process lock acquired")

    # ----- STEP 5: Dedupe DB and counters -----
    conn = sqlite3.connect(str(dedupe_db))
    ensure_sqlite(conn)
    logger.info(f"Using dedupe database: {dedupe_db}")

    reservoir: List[pd.Series] = []
    seen: int = 0
    kept_rows: int = 0
    new_files_processed = 0
    skipped_files = 0
    skipped_old_failed = 0
    file_type_stats = {"Standby": 0, "New Fastpass": 0, "Old Fastpass": 0, "Unknown": 0}

    try:
        # ----- STEP 6: List all S3 CSVs (standby + fastpass) -----
        all_keys: List[Tuple[str, datetime]] = []
        for prop in props:
            standby_prefix = S3_STANDBY_PREFIX_FMT.format(prop=prop)
            priority_prefix = S3_PRIORITY_PREFIX_FMT.format(prop=prop)
            try:
                skeys = list_s3_csvs(s3, S3_BUCKET, standby_prefix)
                pkeys = list_s3_csvs(s3, S3_BUCKET, priority_prefix)
                logger.info(f"{prop}: standby={len(skeys)} priority={len(pkeys)}")
                all_keys.extend(skeys)
                all_keys.extend(pkeys)
            except Exception as e:
                logger.error(f"Error listing files for {prop}: {e}")
                continue

        total = len(all_keys)
        logger.info(f"Total files found: {total}")

        # ----- STEP 7: Filter to new/changed only (incremental) -----
        if not args.full_rebuild:
            new_keys: List[Tuple[str, datetime]] = []
            for key, last_modified in all_keys:
                if key not in processed_files:
                    new_keys.append((key, last_modified))
                else:
                    stored = processed_files.get(key, "")
                    if stored:
                        try:
                            ts = datetime.fromisoformat(stored.replace("Z", "+00:00"))
                            lm = last_modified
                            if lm.tzinfo is None:
                                lm = lm.replace(tzinfo=ts.tzinfo)
                            if lm > ts:
                                new_keys.append((key, last_modified))
                        except (ValueError, AttributeError):
                            new_keys.append((key, last_modified))
                    else:
                        new_keys.append((key, last_modified))
            skipped_files = total - len(new_keys)
            all_keys = new_keys
            logger.info(f"New/changed files to process: {len(all_keys)} (skipped {skipped_files} already processed)")

        # ----- STEP 8: Filter out old repeatedly-failed files -----
        filtered: List[Tuple[str, datetime]] = []
        for key, last_modified in all_keys:
            finfo = failed_files.get(key)
            if finfo and finfo.get("failures", 0) >= FAILED_SKIP_THRESHOLD and _is_file_old(last_modified):
                skipped_old_failed += 1
                logger.info(f"Skipping {key}: old file (>{OLD_FILE_DAYS}d), {finfo.get('failures')} prior failures")
                continue
            filtered.append((key, last_modified))
        all_keys = filtered
        if skipped_old_failed:
            logger.info(f"Skipped {skipped_old_failed} old repeatedly-failed files")

        # ----- STEP 9: Process each file -----
        for idx, (key, last_modified) in enumerate(all_keys, 1):
            logger.info(f"({idx}/{len(all_keys)}) {key} (modified: {last_modified})")

            try:
                file_type = get_wait_time_filetype(key)
                file_type_stats[file_type] = file_type_stats.get(file_type, 0) + 1
                logger.info(f"Classified {key} as {file_type}")

                if file_type == "Unknown":
                    logger.warning(f"Skipping unknown file type: {key}")
                    continue

                tz = _zone_from_key(key)
                rows_written, rows_seen = process_file(
                    s3=s3,
                    key=key,
                    file_type=file_type,
                    tz=tz,
                    conn=conn,
                    clean_dir=dirs["fact_tables_clean"],
                    logger=logger,
                    chunksize=args.chunksize,
                    reservoir=reservoir,
                    seen=seen,
                    sample_k=args.sample_k,
                )
                kept_rows += rows_written
                seen += rows_seen
                processed_files[key] = last_modified.isoformat()
                new_files_processed += 1
                failed_files.pop(key, None)

            except ClientError as e:
                logger.warning(f"Skipping {key}: {e}")
                _record_failure(failed_files, key, last_modified)
                continue
            except ResponseStreamingError as e:
                logger.error(f"Connection error processing {key}: {e}")
                _record_failure(failed_files, key, last_modified)
                continue
            except Exception as e:
                logger.error(f"Error processing {key}: {e}", exc_info=True)
                _record_failure(failed_files, key, last_modified)
                continue

        logger.info(f"Deduped rows written: {kept_rows:,}")
        logger.info(f"New files processed: {new_files_processed}")
        logger.info(f"File type breakdown: {file_type_stats}")

        # ----- STEP 10: Save sample CSV -----
        if reservoir:
            sample_df = pd.DataFrame(reservoir).sample(frac=1.0, random_state=42).reset_index(drop=True)
        else:
            sample_df = pd.DataFrame(columns=["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"])
        sample_df.to_csv(local_sample, index=False)
        logger.info(f"Sample CSV saved: {local_sample} ({len(sample_df)} rows)")

        # ----- STEP 11: Persist state -----
        save_processed_files(state_file, processed_files)
        logger.info(f"Processed files state saved: {len(processed_files)} total files")
        save_failed_files(dirs["state"], failed_files)
        if failed_files:
            logger.info(f"Failed files state saved: {len(failed_files)} entries")

        # ----- STEP 12: Build complete summary -----
        logger.info("")
        logger.info("=" * 70)
        logger.info("          BUILD COMPLETE")
        logger.info("=" * 70)
        logger.info(f"CSV files .....: {dirs['fact_tables_clean']}")
        logger.info(f"Sample CSV ....: {local_sample}")
        logger.info(f"Dedupe SQLite .: {dedupe_db}")
        skip_msg = f", {skipped_old_failed} old repeatedly-failed skipped" if skipped_old_failed else ""
        logger.info(f"Files .........: {new_files_processed} new, {skipped_files} skipped{skip_msg}")
        logger.info(f"Rows written ..: {kept_rows:,}")
        logger.info(f"File types ....: {file_type_stats}")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()
        release_lock(lock_file, logger)


if __name__ == "__main__":
    random.seed(1234)
    main()
