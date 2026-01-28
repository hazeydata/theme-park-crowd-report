#!/usr/bin/env python3
"""
Batch Train Models for Multiple Entities

Trains XGBoost models (with-POSTED and without-POSTED) for multiple entities.
Can query entity index for entities needing training, or train a specified list.

Usage:
    # Train all entities that need modeling (from entity index)
    python scripts/train_batch_entities.py
    
    # Train specific entities
    python scripts/train_batch_entities.py --entities MK101 MK102 AK01
    
    # Train entities from a file (one entity code per line)
    python scripts/train_batch_entities.py --entity-list entities.txt
    
    # Train only entities with data at least 24 hours old
    python scripts/train_batch_entities.py --min-age-hours 24
    
    # Limit number of entities to train
    python scripts/train_batch_entities.py --max-entities 10
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.entity_index import get_entities_needing_modeling
from utils.entity_names import format_entity_display
from utils.paths import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"train_batch_entities_{datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d_%H%M%S')}.log"

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


def train_single_entity(
    entity_code: str,
    output_base: Path,
    train_script: Path,
    python_exe: str,
    train_ratio: float,
    val_ratio: float,
    skip_encoding: bool,
    sample: int | None,
    skip_park_hours: bool,
    logger: logging.Logger,
) -> tuple[bool, str]:
    """
    Train a single entity by calling train_entity_model.py as a subprocess.
    
    Returns:
        (success: bool, message: str)
    """
    cmd = [
        python_exe,
        str(train_script),
        "--entity", entity_code,
        "--output-base", str(output_base),
        "--train-ratio", str(train_ratio),
        "--val-ratio", str(val_ratio),
    ]
    
    if skip_encoding:
        cmd.append("--skip-encoding")
    if sample:
        cmd.extend(["--sample", str(sample)])
    if skip_park_hours:
        cmd.append("--skip-park-hours")
    
    try:
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout per entity
        )
        elapsed = time.time() - start_time
        
        # Format elapsed time: show minutes if >= 60 seconds
        if elapsed >= 60:
            minutes = int(elapsed // 60)
            seconds = elapsed % 60
            elapsed_str = f"{minutes}m {seconds:.1f}s"
        else:
            elapsed_str = f"{elapsed:.1f}s"
        
        if result.returncode == 0:
            return True, f"SUCCESS ({elapsed_str})"
        else:
            error_msg = result.stderr[:500] if result.stderr else "Unknown error"
            return False, f"FAILED ({elapsed_str}): {error_msg}"
    
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT (>1 hour)"
    except Exception as e:
        return False, f"ERROR: {str(e)[:200]}"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Batch train XGBoost models for multiple entities"
    )
    ap.add_argument(
        "--entities",
        nargs="+",
        help="Specific entity codes to train (e.g., MK101 MK102 AK01)",
    )
    ap.add_argument(
        "--entity-list",
        type=Path,
        help="File containing entity codes (one per line)",
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--min-age-hours",
        type=float,
        default=0.0,
        help="Only train entities where latest_observed_at is at least this many hours old (default: 0)",
    )
    ap.add_argument(
        "--max-entities",
        type=int,
        help="Maximum number of entities to train (default: no limit)",
    )
    ap.add_argument(
        "--train-ratio",
        type=float,
        default=0.7,
        help="Training set proportion (default: 0.7)",
    )
    ap.add_argument(
        "--val-ratio",
        type=float,
        default=0.15,
        help="Validation set proportion (default: 0.15)",
    )
    ap.add_argument(
        "--skip-encoding",
        action="store_true",
        help="Skip encoding step (assumes data is already encoded)",
    )
    ap.add_argument(
        "--sample",
        type=int,
        help="Use only first N rows per entity for testing (speeds up training significantly)",
    )
    ap.add_argument(
        "--skip-park-hours",
        action="store_true",
        help="Skip park hours features (faster, but less accurate)",
    )
    ap.add_argument(
        "--min-observations",
        type=int,
        default=500,
        help="Minimum ACTUAL observations required for XGBoost training (default: 500). Entities with fewer will get mean-based models.",
    )
    ap.add_argument(
        "--python",
        type=str,
        default="python3",
        help="Python executable to use (default: python3)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)
    index_db = base / "state" / "entity_index.sqlite"
    train_script = Path(__file__).parent / "train_entity_model.py"

    logger.info("=" * 60)
    logger.info("Batch Training Models for Multiple Entities")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Python executable: {args.python}")
    logger.info(f"Train script: {train_script}")

    # Determine which entities to train
    entities_to_train: list[str] = []
    
    if args.entities:
        # Explicit list provided
        entities_to_train = args.entities
        logger.info(f"Training {len(entities_to_train)} specified entities")
    
    elif args.entity_list:
        # Read from file
        if not args.entity_list.exists():
            logger.error(f"Entity list file not found: {args.entity_list}")
            sys.exit(1)
        
        with open(args.entity_list, "r", encoding="utf-8") as f:
            entities_to_train = [
                line.strip() for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
        logger.info(f"Loaded {len(entities_to_train)} entities from {args.entity_list}")
    
    else:
        # Query entity index for entities needing training
        logger.info(f"Querying entity index for entities needing training...")
        logger.info(f"  Min age: {args.min_age_hours} hours")
        logger.info(f"  Min target observations: {args.min_observations} (ACTUAL for STANDBY, PRIORITY for PRIORITY)")
        
        # Filter entities: require at least min_observations of ACTUAL OR PRIORITY
        # This filters out entities like TDS36 that only have POSTED (no ACTUAL or PRIORITY)
        # Note: We use min_target_count because we don't know queue type until we check dimentity,
        # but we can filter out entities that have neither ACTUAL nor PRIORITY observations
        entities_needing = get_entities_needing_modeling(
            index_db,
            min_age_hours=args.min_age_hours,
            min_target_count=args.min_observations,  # Filter entities with insufficient ACTUAL OR PRIORITY
            logger=logger,
        )
        
        entities_to_train = [entity_code for entity_code, _, _ in entities_needing]
        
        if not entities_to_train:
            logger.info("No entities found that need training")
            sys.exit(0)
        
        logger.info(f"Found {len(entities_to_train)} entities needing training")
        
        # Note: Entities with < min_observations ACTUAL observations will automatically
        # get mean-based models (created by train_entity_model.py)
        # We still process all entities - the training script handles the threshold check
        
        # Log sample of entities
        sample_size = min(10, len(entities_to_train))
        logger.info(f"Sample entities: {', '.join(entities_to_train[:sample_size])}")
        if len(entities_to_train) > sample_size:
            logger.info(f"... and {len(entities_to_train) - sample_size} more")
    
    # Apply max limit if specified
    if args.max_entities and len(entities_to_train) > args.max_entities:
        logger.info(f"Limiting to first {args.max_entities} entities (from {len(entities_to_train)})")
        entities_to_train = entities_to_train[:args.max_entities]
    
    logger.info("=" * 60)
    logger.info(f"Training {len(entities_to_train)} entities")
    logger.info("=" * 60)
    logger.info(f"Train ratio: {args.train_ratio}, Val ratio: {args.val_ratio}")
    logger.info(f"Min ACTUAL observations for XGBoost: {args.min_observations} (entities with fewer will get mean models)")
    if args.sample:
        logger.info(f"Sampling: {args.sample} rows per entity")
    if args.skip_park_hours:
        logger.info("Skipping park hours features")
    logger.info("")

    # Train each entity
    start_time = time.time()
    results = {
        "success": [],
        "failed": [],
    }
    
    for i, entity_code in enumerate(entities_to_train, 1):
        entity_display = format_entity_display(entity_code, base)
        logger.info("-" * 60)
        logger.info(f"[{i}/{len(entities_to_train)}] Training {entity_display}...")
        
        success, message = train_single_entity(
            entity_code,
            base,
            train_script,
            args.python,
            args.train_ratio,
            args.val_ratio,
            args.skip_encoding,
            args.sample,
            args.skip_park_hours,
            logger,
        )
        
        if success:
            results["success"].append(entity_code)
            logger.info(f"  {message}")
        else:
            results["failed"].append((entity_code, message))
            logger.warning(f"  {message}")
    
    # Summary
    total_time = time.time() - start_time
    
    # Format total time: show hours if >= 60 minutes
    if total_time >= 3600:
        hours = int(total_time // 3600)
        minutes = (total_time % 3600) / 60
        total_time_str = f"{hours}h {minutes:.1f}m"
    elif total_time >= 60:
        minutes = total_time / 60
        total_time_str = f"{minutes:.1f} minutes"
    else:
        total_time_str = f"{total_time:.1f} seconds"
    
    # Format average time per entity
    avg_time = total_time / len(entities_to_train)
    if avg_time >= 60:
        avg_minutes = int(avg_time // 60)
        avg_seconds = avg_time % 60
        avg_time_str = f"{avg_minutes}m {avg_seconds:.1f}s"
    else:
        avg_time_str = f"{avg_time:.1f}s"
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("Batch Training Summary")
    logger.info("=" * 60)
    logger.info(f"Total entities: {len(entities_to_train)}")
    logger.info(f"  Successful: {len(results['success'])}")
    logger.info(f"  Failed: {len(results['failed'])}")
    logger.info(f"Total time: {total_time_str}")
    logger.info(f"Average time per entity: {avg_time_str}")
    
    if results["success"]:
        logger.info("")
        logger.info("Successfully trained entities:")
        for entity in results["success"]:
            entity_display = format_entity_display(entity, base)
            logger.info(f"  - {entity_display}")
    
    if results["failed"]:
        logger.info("")
        logger.warning("Failed entities:")
        for entity, reason in results["failed"]:
            entity_display = format_entity_display(entity, base)
            logger.warning(f"  - {entity_display}: {reason}")
    
    logger.info("")
    logger.info("Done!")
    
    # Exit with error code if any failed
    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
