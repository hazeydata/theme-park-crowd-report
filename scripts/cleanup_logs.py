#!/usr/bin/env python3
"""
Clean up old log files from the pipeline.

Removes log files older than a specified number of days, optionally keeping
the N most recent logs per log type (e.g., keep 10 most recent train_batch_entities logs).

Usage:
    # Dry run (show what would be deleted)
    python scripts/cleanup_logs.py --dry-run
    
    # Delete logs older than 30 days, keep 10 most recent per type
    python scripts/cleanup_logs.py --days 30 --keep-recent 10
    
    # Delete all logs older than 7 days
    python scripts/cleanup_logs.py --days 7
    
    # Delete specific log pattern (e.g., all train_entity_model logs older than 14 days)
    python scripts/cleanup_logs.py --days 14 --pattern "train_entity_model_*.log"
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.paths import get_output_base


def setup_logging(log_dir: Path, dry_run: bool = False) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    suffix = "_dryrun" if dry_run else ""
    log_file = log_dir / f"cleanup_logs_{datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d_%H%M%S')}{suffix}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Log cleanup {'(DRY RUN)' if dry_run else ''} started. Log file: {log_file}")
    return logger


def group_logs_by_type(log_files: list[Path]) -> dict[str, list[Path]]:
    """
    Group log files by their base type (e.g., train_batch_entities, get_park_hours).
    
    Examples:
        train_batch_entities_20260127_190228.log -> train_batch_entities
        get_park_hours_20260127_060006.log -> get_park_hours
        train_entity_model_20260127_105001.log -> train_entity_model
    """
    grouped = defaultdict(list)
    
    for log_file in log_files:
        # Extract base name (everything before the timestamp)
        # Pattern: {base}_{YYYYMMDD}_{HHMMSS}.log
        match = re.match(r"^(.+?)_\d{8}_\d{6}\.log$", log_file.name)
        if match:
            log_type = match.group(1)
            grouped[log_type].append(log_file)
        else:
            # Fallback: use filename without extension as type
            log_type = log_file.stem
            grouped[log_type].append(log_file)
    
    return dict(grouped)


def cleanup_logs(
    log_dir: Path,
    days: int,
    keep_recent: int | None = None,
    pattern: str | None = None,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> dict[str, int]:
    """
    Clean up old log files.
    
    Args:
        log_dir: Directory containing log files
        days: Delete logs older than this many days
        keep_recent: Keep this many most recent logs per log type (None = no limit)
        pattern: Optional glob pattern to filter logs (e.g., "train_entity_model_*.log")
        dry_run: If True, don't actually delete files, just report what would be deleted
        logger: Optional logger for output
    
    Returns:
        Dictionary with counts: {"deleted": N, "kept": M, "errors": K}
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    cutoff_date = datetime.now() - timedelta(days=days)
    cutoff_timestamp = cutoff_date.timestamp()
    
    logger.info(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Cutoff age: {days} days")
    if keep_recent:
        logger.info(f"Keeping {keep_recent} most recent logs per type")
    if pattern:
        logger.info(f"Pattern filter: {pattern}")
    logger.info("")
    
    # Find all log files
    if pattern:
        log_files = list(log_dir.glob(pattern))
    else:
        log_files = list(log_dir.glob("*.log"))
    
    if not log_files:
        logger.info("No log files found")
        return {"deleted": 0, "kept": 0, "errors": 0}
    
    logger.info(f"Found {len(log_files)} log file(s)")
    
    # Group by log type
    grouped_logs = group_logs_by_type(log_files)
    logger.info(f"Grouped into {len(grouped_logs)} log type(s)")
    logger.info("")
    
    stats = {"deleted": 0, "kept": 0, "errors": 0}
    
    for log_type, type_logs in sorted(grouped_logs.items()):
        logger.info(f"Processing {log_type}: {len(type_logs)} file(s)")
        
        # Sort by modification time (newest first)
        type_logs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        
        # Determine which files to keep
        files_to_keep = []
        files_to_delete = []
        
        for log_file in type_logs:
            file_mtime = log_file.stat().st_mtime
            file_date = datetime.fromtimestamp(file_mtime)
            age_days = (datetime.now() - file_date).days
            
            # Keep if:
            # 1. Newer than cutoff date, OR
            # 2. Within the "keep_recent" count for this type
            is_recent = file_mtime > cutoff_timestamp
            is_in_keep_list = keep_recent and len(files_to_keep) < keep_recent
            
            if is_recent or is_in_keep_list:
                files_to_keep.append(log_file)
            else:
                files_to_delete.append(log_file)
        
        logger.info(f"  Keeping: {len(files_to_keep)} file(s)")
        logger.info(f"  Deleting: {len(files_to_delete)} file(s)")
        
        # Show sample of files being kept/deleted
        if files_to_keep:
            logger.debug(f"  Keeping: {files_to_keep[0].name} (most recent)")
        if files_to_delete:
            logger.debug(f"  Deleting: {files_to_delete[0].name} (oldest to delete)")
        
        # Delete files
        for log_file in files_to_delete:
            try:
                if not dry_run:
                    log_file.unlink()
                    logger.debug(f"  Deleted: {log_file.name}")
                else:
                    logger.debug(f"  Would delete: {log_file.name}")
                stats["deleted"] += 1
            except Exception as e:
                logger.error(f"  Error deleting {log_file.name}: {e}")
                stats["errors"] += 1
        
        stats["kept"] += len(files_to_keep)
        logger.info("")
    
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean up old log files from the pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run: show what would be deleted
  python scripts/cleanup_logs.py --dry-run --days 30
  
  # Delete logs older than 30 days, keep 10 most recent per type
  python scripts/cleanup_logs.py --days 30 --keep-recent 10
  
  # Delete all train_entity_model logs older than 14 days
  python scripts/cleanup_logs.py --days 14 --pattern "train_entity_model_*.log"
  
  # Delete all logs older than 7 days (no keep-recent limit)
  python scripts/cleanup_logs.py --days 7
        """,
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--days",
        type=int,
        default=30,
        help="Delete logs older than this many days (default: 30)",
    )
    ap.add_argument(
        "--keep-recent",
        type=int,
        help="Keep this many most recent logs per log type (default: no limit)",
    )
    ap.add_argument(
        "--pattern",
        type=str,
        help="Optional glob pattern to filter logs (e.g., 'train_entity_model_*.log')",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    ap.add_argument(
        "--log-dir",
        type=Path,
        help="Override log directory (default: output_base/logs)",
    )
    
    args = ap.parse_args()
    
    base = args.output_base.resolve()
    log_dir = args.log_dir.resolve() if args.log_dir else base / "logs"
    
    if not log_dir.exists():
        print(f"Error: Log directory does not exist: {log_dir}")
        sys.exit(1)
    
    logger = setup_logging(log_dir, dry_run=args.dry_run)
    
    logger.info("=" * 60)
    logger.info("Log Cleanup")
    logger.info("=" * 60)
    logger.info(f"Log directory: {log_dir}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("")
    
    stats = cleanup_logs(
        log_dir=log_dir,
        days=args.days,
        keep_recent=args.keep_recent,
        pattern=args.pattern,
        dry_run=args.dry_run,
        logger=logger,
    )
    
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info(f"Deleted: {stats['deleted']} file(s)")
    logger.info(f"Kept: {stats['kept']} file(s)")
    if stats["errors"] > 0:
        logger.warning(f"Errors: {stats['errors']} file(s)")
    logger.info("")
    
    if args.dry_run:
        logger.info("DRY RUN - No files were actually deleted")
    else:
        logger.info("Cleanup complete!")
    
    # Exit with error code if there were errors
    if stats["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
