"""
Test Modeling Pipeline

================================================================================
PURPOSE
================================================================================
Tests the complete modeling pipeline with a small subset:
1. Forecast generation (future dates)
2. Backfill generation (historical dates)
3. WTI calculation

Uses a small number of entities and dates for quick validation.

================================================================================
USAGE
================================================================================
  python scripts/test_modeling_pipeline.py
  python scripts/test_modeling_pipeline.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.paths import get_output_base


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging() -> logging.Logger:
    """Set up logging to console."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger(__name__)


# =============================================================================
# TEST FUNCTIONS
# =============================================================================

def test_forecast(
    output_base: Path,
    logger: logging.Logger,
    test_entities: list[str],
    test_dates: list[date],
) -> bool:
    """Test forecast generation."""
    logger.info("=" * 80)
    logger.info("TEST 1: Forecast Generation")
    logger.info("=" * 80)
    
    # Build command
    cmd = [
        sys.executable,
        str(Path(__file__).parent / "generate_forecast.py"),
        "--output-base",
        str(output_base),
        "--max-entities",
        str(len(test_entities)),
        "--max-dates",
        str(len(test_dates)),
    ]
    
    # Add entity filter if only one
    if len(test_entities) == 1:
        cmd.extend(["--entity", test_entities[0]])
    
    # Add date range
    cmd.extend([
        "--start-date",
        min(test_dates).strftime("%Y-%m-%d"),
        "--end-date",
        max(test_dates).strftime("%Y-%m-%d"),
    ])
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("Forecast generation completed successfully")
        logger.info(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Forecast generation failed: {e}")
        logger.error(f"Error output:\n{e.stderr}")
        return False


def test_backfill(
    output_base: Path,
    logger: logging.Logger,
    test_entities: list[str],
    test_dates: list[date],
) -> bool:
    """Test backfill generation."""
    logger.info("=" * 80)
    logger.info("TEST 2: Backfill Generation")
    logger.info("=" * 80)
    
    # Use historical dates (before today)
    today = date.today()
    historical_dates = [d for d in test_dates if d < today]
    
    if not historical_dates:
        logger.warning("No historical dates in test range - skipping backfill test")
        return True
    
    # Build command
    cmd = [
        sys.executable,
        str(Path(__file__).parent / "generate_backfill.py"),
        "--output-base",
        str(output_base),
        "--max-entities",
        str(len(test_entities)),
        "--max-dates",
        str(len(historical_dates)),
        "--start-date",
        min(historical_dates).strftime("%Y-%m-%d"),
        "--end-date",
        max(historical_dates).strftime("%Y-%m-%d"),
    ]
    
    # Add entity filter if only one
    if len(test_entities) == 1:
        cmd.extend(["--entity", test_entities[0]])
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("Backfill generation completed successfully")
        logger.info(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Backfill generation failed: {e}")
        logger.error(f"Error output:\n{e.stderr}")
        return False


def test_wti(
    output_base: Path,
    logger: logging.Logger,
    test_dates: list[date],
) -> bool:
    """Test WTI calculation."""
    logger.info("=" * 80)
    logger.info("TEST 3: WTI Calculation")
    logger.info("=" * 80)
    
    # Build command
    cmd = [
        sys.executable,
        str(Path(__file__).parent / "calculate_wti.py"),
        "--output-base",
        str(output_base),
        "--start-date",
        min(test_dates).strftime("%Y-%m-%d"),
        "--end-date",
        max(test_dates).strftime("%Y-%m-%d"),
        "--max-dates",
        str(len(test_dates)),
    ]
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info("WTI calculation completed successfully")
        logger.info(f"Output:\n{result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"WTI calculation failed: {e}")
        logger.error(f"Error output:\n{e.stderr}")
        return False


def validate_outputs(
    output_base: Path,
    logger: logging.Logger,
    test_entities: list[str],
    test_dates: list[date],
) -> bool:
    """Validate generated outputs."""
    logger.info("=" * 80)
    logger.info("TEST 4: Output Validation")
    logger.info("=" * 80)
    
    import pandas as pd
    
    all_valid = True
    
    # Check forecast curves
    forecast_dir = output_base / "curves" / "forecast"
    if forecast_dir.exists():
        forecast_files = list(forecast_dir.glob("*.csv"))
        logger.info(f"Found {len(forecast_files)} forecast curve files")
        
        for entity in test_entities[:3]:  # Check first 3 entities
            for test_date in test_dates[:2]:  # Check first 2 dates
                filename = f"{entity}_{test_date.strftime('%Y-%m-%d')}.csv"
                filepath = forecast_dir / filename
                
                if filepath.exists():
                    try:
                        df = pd.read_csv(filepath)
                        required_cols = ["entity_code", "park_date", "time_slot", "actual_predicted", "posted_predicted"]
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        
                        if missing_cols:
                            logger.error(f"  {filename}: Missing columns: {missing_cols}")
                            all_valid = False
                        else:
                            logger.info(f"  {filename}: ✓ Valid ({len(df)} time slots)")
                    except Exception as e:
                        logger.error(f"  {filename}: Error reading file: {e}")
                        all_valid = False
    else:
        logger.warning("Forecast directory not found")
    
    # Check backfill curves
    backfill_dir = output_base / "curves" / "backfill"
    if backfill_dir.exists():
        backfill_files = list(backfill_dir.glob("*.csv"))
        logger.info(f"Found {len(backfill_files)} backfill curve files")
        
        today = date.today()
        historical_dates = [d for d in test_dates if d < today]
        
        for entity in test_entities[:3]:
            for test_date in historical_dates[:2]:
                filename = f"{entity}_{test_date.strftime('%Y-%m-%d')}.csv"
                filepath = backfill_dir / filename
                
                if filepath.exists():
                    try:
                        df = pd.read_csv(filepath)
                        required_cols = ["entity_code", "park_date", "time_slot", "actual", "source"]
                        missing_cols = [col for col in required_cols if col not in df.columns]
                        
                        if missing_cols:
                            logger.error(f"  {filename}: Missing columns: {missing_cols}")
                            all_valid = False
                        else:
                            logger.info(f"  {filename}: ✓ Valid ({len(df)} time slots)")
                    except Exception as e:
                        logger.error(f"  {filename}: Error reading file: {e}")
                        all_valid = False
    else:
        logger.warning("Backfill directory not found")
    
    # Check WTI
    wti_path = output_base / "wti" / "wti.parquet"
    if wti_path.exists():
        try:
            df = pd.read_parquet(wti_path)
            required_cols = ["park_code", "park_date", "time_slot", "wti", "n_entities"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"WTI file: Missing columns: {missing_cols}")
                all_valid = False
            else:
                logger.info(f"WTI file: ✓ Valid ({len(df)} rows)")
                logger.info(f"  Date range: {df['park_date'].min()} to {df['park_date'].max()}")
                logger.info(f"  Parks: {sorted(df['park_code'].unique())}")
        except Exception as e:
            logger.error(f"WTI file: Error reading: {e}")
            all_valid = False
    else:
        logger.warning("WTI file not found")
    
    return all_valid


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Test modeling pipeline with small subset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--output-base",
        type=str,
        help="Pipeline output base directory (default: from config/config.json)",
    )
    
    parser.add_argument(
        "--entity",
        type=str,
        help="Test with specific entity (default: first entity from index)",
    )
    
    parser.add_argument(
        "--num-entities",
        type=int,
        default=2,
        help="Number of entities to test (default: 2)",
    )
    
    parser.add_argument(
        "--num-dates",
        type=int,
        default=7,
        help="Number of dates to test (default: 7)",
    )
    
    args = parser.parse_args()
    
    # Get output base
    if args.output_base:
        base = Path(args.output_base)
    else:
        base = get_output_base()
    
    # Set up logging
    logger = setup_logging()
    logger.info("Modeling Pipeline Test")
    logger.info(f"Output base: {base}")
    
    # Get test entities
    if args.entity:
        test_entities = [args.entity]
    else:
        # Load from entity index
        index_db = base / "state" / "entity_index.sqlite"
        if not index_db.exists():
            logger.error(f"Entity index not found: {index_db}")
            logger.error("Run build_entity_index.py first")
            sys.exit(1)
        
        from processors.entity_index import get_all_entities
        
        all_entities_df = get_all_entities(index_db)
        if all_entities_df.empty:
            logger.error("No entities found in entity index")
            sys.exit(1)
        
        all_entities = all_entities_df["entity_code"].tolist()
        test_entities = all_entities[:args.num_entities]
        logger.info(f"Selected {len(test_entities)} test entities: {test_entities}")
    
    # Generate test dates (mix of past and future)
    today = date.today()
    test_dates = []
    
    # Future dates (for forecast)
    for i in range(1, args.num_dates // 2 + 1):
        test_dates.append(today + timedelta(days=i))
    
    # Past dates (for backfill)
    for i in range(1, args.num_dates // 2 + 1):
        test_dates.append(today - timedelta(days=i))
    
    test_dates = sorted(test_dates)
    logger.info(f"Test date range: {min(test_dates)} to {max(test_dates)} ({len(test_dates)} dates)")
    
    # Run tests
    results = {}
    
    # Test 1: Forecast
    results["forecast"] = test_forecast(base, logger, test_entities, test_dates)
    
    # Test 2: Backfill
    results["backfill"] = test_backfill(base, logger, test_entities, test_dates)
    
    # Test 3: WTI
    results["wti"] = test_wti(base, logger, test_dates)
    
    # Test 4: Validate outputs
    results["validation"] = validate_outputs(base, logger, test_entities, test_dates)
    
    # Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"  {test_name.upper()}: {status}")
    
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("")
        logger.info("All tests passed! ✓")
        sys.exit(0)
    else:
        logger.info("")
        logger.error("Some tests failed. See errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
