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
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)
    index_db = base / "state" / "entity_index.sqlite"

    logger.info("=" * 60)
    logger.info(f"Training models for entity: {args.entity}")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Train ratio: {args.train_ratio}, Val ratio: {args.val_ratio}")

    # Load entity data
    logger.info("Loading entity data...")
    df = load_entity_data(args.entity, base, db_path=index_db, logger=logger)
    
    if df.empty:
        logger.error(f"No data found for entity {args.entity}")
        sys.exit(1)
    
    logger.info(f"Loaded {len(df):,} rows")
    
    # Sample data if requested (for faster testing)
    if args.sample and args.sample > 0:
        original_len = len(df)
        df = df.head(args.sample).copy()
        logger.info(f"Sampled to {len(df):,} rows (from {original_len:,}) for faster testing")

    # Add features
    logger.info("Adding features...")
    df_features = add_features(df, base, logger=logger)
    
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
        mark_entity_modeled(args.entity, index_db, logger=logger)
        logger.info(f"\nMarked {args.entity} as modeled in entity index")
        
        logger.info("\nDone!")
        
    except Exception as e:
        logger.error(f"Training failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
