#!/usr/bin/env python3
"""
Train Model for Entity

Trains XGBoost models (with-POSTED and without-POSTED) for a specific entity.

Usage:
    python scripts/train_entity_model.py --entity MK101
    python scripts/train_entity_model.py --entity MK101 --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.encoding import encode_features
from processors.entity_index import load_entity_data, mark_entity_modeled
from processors.features import add_features
from processors.training import train_entity_model
from utils.entity_names import format_entity_display, is_priority_queue
from utils.paths import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    from datetime import datetime
    from zoneinfo import ZoneInfo
    log_file = log_dir / f"train_entity_model_{datetime.now(ZoneInfo('UTC')).strftime('%Y%m%d_%H%M%S')}.log"

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


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Train XGBoost models for an entity"
    )
    ap.add_argument(
        "--entity",
        type=str,
        required=True,
        help="Entity code (e.g., MK101)",
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
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
        help="Use only first N rows for testing (speeds up training significantly)",
    )
    ap.add_argument(
        "--skip-park-hours",
        action="store_true",
        help="Skip park hours features (faster, but less accurate)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)
    index_db = base / "state" / "entity_index.sqlite"
    
    entity_display = format_entity_display(args.entity, base)

    logger.info("=" * 60)
    logger.info(f"Training models for entity: {entity_display}")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Train ratio: {args.train_ratio}, Val ratio: {args.val_ratio}")

    # Determine queue type: PRIORITY (fastpass_booth=TRUE) or STANDBY (fastpass_booth=FALSE)
    is_priority = is_priority_queue(args.entity, base)
    queue_type = "PRIORITY" if is_priority else "STANDBY"
    target_wait_type = "PRIORITY" if is_priority else "ACTUAL"
    
    logger.info(f"Queue type: {queue_type} (fastpass_booth={is_priority})")
    
    # Quick pre-check: Load a small sample first to check if entity has the required wait_time_type
    # This avoids loading all data for entities that only have POSTED (no ACTUAL)
    logger.info("Checking if entity has required wait time type...")
    from processors.entity_index import load_entity_data
    df_sample = load_entity_data(args.entity, base, db_path=index_db, logger=logger)
    
    if df_sample.empty:
        logger.error(f"No data found for entity {entity_display}")
        sys.exit(1)
    
    # Check wait_time_type distribution in sample
    wait_type_counts = df_sample["wait_time_type"].value_counts()
    logger.info(f"Wait time type distribution (sample): {dict(wait_type_counts)}")
    
    target_count_sample = len(df_sample[df_sample["wait_time_type"] == target_wait_type])
    
    # If no target wait_time_type found in sample, check full dataset (might be sparse)
    # But if sample is large enough and still 0, skip full load
    if target_count_sample == 0 and len(df_sample) >= 1000:
        logger.warning(f"No {target_wait_type} observations found in sample of {len(df_sample):,} rows")
        logger.info("Entity likely has no {target_wait_type} data - will create mean model with 0")
        # Load full dataset anyway to be sure, but this is a warning case
    elif target_count_sample == 0:
        logger.info(f"No {target_wait_type} in initial sample ({len(df_sample):,} rows) - loading full dataset to verify...")
    
    # Load full entity data
    logger.info("Loading full entity data...")
    df = load_entity_data(args.entity, base, db_path=index_db, logger=logger)
    
    logger.info(f"Loaded {len(df):,} rows")
    
    # Show wait_time_type distribution to help understand why entity is being processed
    wait_type_counts = df["wait_time_type"].value_counts()
    logger.info(f"Wait time type distribution: {dict(wait_type_counts)}")
    
    # Check observation count for the appropriate wait time type
    df_target = df[df["wait_time_type"] == target_wait_type]
    target_count = len(df_target)
    logger.info(f"{target_wait_type} observations: {target_count:,}")
    
    MIN_OBSERVATIONS_FOR_TRAINING = 500
    
    # Early exit with clear message if entity has no target wait_time_type data
    # (e.g., TDS36 has only POSTED, no ACTUAL)
    if target_count == 0:
        logger.warning(f"Entity {entity_display} has no {target_wait_type} observations")
        logger.info(f"Available wait time types: {list(wait_type_counts.keys())}")
        logger.info(f"This entity will be skipped for {target_wait_type} modeling")
        logger.info("Creating mean model with default value of 0...")
    
    if target_count < MIN_OBSERVATIONS_FOR_TRAINING:
        logger.info(f"Entity has {target_count:,} {target_wait_type} observations (< {MIN_OBSERVATIONS_FOR_TRAINING})")
        logger.info("Creating mean-based model instead of XGBoost model...")
        
        # Calculate mean wait time from target observations
        if target_count > 0:
            mean_wait_time = float(df_target["wait_time_minutes"].mean())
            logger.info(f"Mean {target_wait_type} wait time: {mean_wait_time:.2f} minutes")
        else:
            logger.warning(f"No {target_wait_type} observations found - using default mean of 0")
            mean_wait_time = 0.0
        
        # Save mean model
        from processors.training import save_mean_model
        save_mean_model(
            args.entity,
            base,
            mean_wait_time,
            target_count,
            logger=logger,
        )
        
        # Mark entity as modeled
        mark_entity_modeled(args.entity, index_db)
        logger.info(f"\nMarked {entity_display} as modeled (mean-based)")
        logger.info("\nDone!")
        sys.exit(0)
    
    # Sample data if requested (for faster testing)
    if args.sample and args.sample > 0:
        original_len = len(df)
        df = df.head(args.sample).copy()
        logger.info(f"Sampled to {len(df):,} rows (from {original_len:,}) for faster testing")

    # Add features
    logger.info("Adding features...")
    df_features = add_features(df, base, logger=logger, include_park_hours=not args.skip_park_hours)
    
    if df_features.empty:
        logger.error("No data after feature engineering")
        sys.exit(1)
    
    logger.info(f"Features added: {len(df_features.columns)} columns")

    # Encode categorical features
    if not args.skip_encoding:
        logger.info("Encoding categorical features...")
        df_encoded, mappings = encode_features(
            df_features,
            base,
            strategy="label",
            handle_unknown="encode",  # Allow new values to be encoded with new IDs
            logger=logger,
        )
        logger.info(f"Encoded {len(mappings)} categorical columns")
    else:
        logger.info("Skipping encoding (assumes already encoded)")
        df_encoded = df_features

    # Train models
    logger.info("Training models...")
    try:
        models, metrics = train_entity_model(
            df_encoded,
            args.entity,
            base,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
            target_wait_type=target_wait_type,
            logger=logger,
        )
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Training Summary")
        logger.info("=" * 60)
        
        for model_type, model_metrics in metrics.items():
            if model_metrics:
                logger.info(f"\n{model_type.upper()}:")
                logger.info(f"  MAE:  {model_metrics.get('mae', 'N/A'):.2f}")
                logger.info(f"  RMSE: {model_metrics.get('rmse', 'N/A'):.2f}")
                logger.info(f"  R²:   {model_metrics.get('r2', 'N/A'):.3f}")
                if model_metrics.get('mape'):
                    logger.info(f"  MAPE: {model_metrics.get('mape', 'N/A'):.2f}%")
        
        # Mark entity as modeled
        mark_entity_modeled(args.entity, index_db)
        logger.info(f"\nMarked {entity_display} as modeled in entity index")
        
        logger.info("\nDone!")
        
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
