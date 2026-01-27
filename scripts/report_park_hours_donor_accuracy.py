#!/usr/bin/env python3
"""
Report Donor Park Hours Accuracy

Analyzes how well predicted (donor) park hours match official hours when they arrive.
Tracks accuracy metrics over time to evaluate donor selection strategy.

Usage:
    python scripts/report_park_hours_donor_accuracy.py
    python scripts/report_park_hours_donor_accuracy.py --output-base "D:\\Path" --min-confidence 0.7
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

from processors.park_hours_versioning import load_versioned_table
from utils import get_output_base

from typing import Optional


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"report_park_hours_donor_accuracy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def parse_time_to_minutes(time_str: str) -> Optional[int]:
    """Parse time string (HH:MM or HH:MM:SS) to minutes since midnight."""
    if pd.isna(time_str) or time_str is None:
        return None
    
    time_str = str(time_str).strip()
    if not time_str:
        return None
    
    try:
        parts = time_str.split(":")
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return hour * 60 + minute
    except (ValueError, TypeError, IndexError):
        pass
    
    return None


def calculate_accuracy_metrics(
    versioned_df: pd.DataFrame,
    min_confidence: float = 0.0,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Calculate accuracy metrics for predicted park hours.
    
    For each (park_date, park_code) that has both:
      - A predicted version (source='donor_imputation')
      - An official version (version_type='official')
    
    Compare predicted vs official and calculate:
      - opening_time_error_minutes: Absolute difference in opening time
      - closing_time_error_minutes: Absolute difference in closing time
      - emh_morning_match: True if EMH morning matches
      - emh_evening_match: True if EMH evening matches
      - confidence: Confidence score from predicted version
      - days_until_date: Days between predicted creation and park_date
    
    Args:
        versioned_df: Versioned park hours DataFrame
        min_confidence: Minimum confidence to include (default: 0.0 = all)
        logger: Optional logger
    
    Returns:
        DataFrame with accuracy metrics
    """
    # Filter to predicted versions with sufficient confidence
    predicted = versioned_df[
        (versioned_df["version_type"] == "predicted") &
        (versioned_df["source"] == "donor_imputation") &
        (versioned_df["confidence"].fillna(0.0) >= min_confidence)
    ].copy()
    
    if predicted.empty:
        if logger:
            logger.warning("No predicted versions found")
        return pd.DataFrame()
    
    # For each predicted version, find corresponding official version
    results = []
    
    for _, pred_row in predicted.iterrows():
        park_date = pred_row["park_date"]
        park_code = pred_row["park_code"]
        
        # Find official version for same (park_date, park_code)
        official = versioned_df[
            (versioned_df["park_date"] == park_date) &
            (versioned_df["park_code"] == park_code) &
            (versioned_df["version_type"] == "official")
        ]
        
        if official.empty:
            continue  # No official version yet, skip
        
        official_row = official.iloc[0]
        
        # Parse times to minutes
        pred_open_min = parse_time_to_minutes(pred_row["opening_time"])
        pred_close_min = parse_time_to_minutes(pred_row["closing_time"])
        official_open_min = parse_time_to_minutes(official_row["opening_time"])
        official_close_min = parse_time_to_minutes(official_row["closing_time"])
        
        if pred_open_min is None or pred_close_min is None or official_open_min is None or official_close_min is None:
            continue  # Skip if we can't parse times
        
        # Calculate errors
        opening_error = abs(pred_open_min - official_open_min)
        closing_error = abs(pred_close_min - official_close_min)
        
        # EMH matches
        emh_morning_match = bool(pred_row.get("emh_morning", False)) == bool(official_row.get("emh_morning", False))
        emh_evening_match = bool(pred_row.get("emh_evening", False)) == bool(official_row.get("emh_evening", False))
        
        # Days until date
        pred_created = pd.to_datetime(pred_row["created_at"], errors="coerce")
        park_date_obj = pd.to_datetime(park_date, errors="coerce").date()
        if pd.isna(pred_created) or pd.isna(park_date_obj):
            days_until_date = None
        else:
            days_until_date = (park_date_obj - pred_created.date()).days
        
        results.append({
            "park_date": park_date,
            "park_code": park_code,
            "predicted_created_at": pred_row["created_at"],
            "official_created_at": official_row["created_at"],
            "predicted_opening_time": pred_row["opening_time"],
            "official_opening_time": official_row["opening_time"],
            "predicted_closing_time": pred_row["closing_time"],
            "official_closing_time": official_row["closing_time"],
            "opening_time_error_minutes": opening_error,
            "closing_time_error_minutes": closing_error,
            "emh_morning_match": emh_morning_match,
            "emh_evening_match": emh_evening_match,
            "predicted_emh_morning": bool(pred_row.get("emh_morning", False)),
            "official_emh_morning": bool(official_row.get("emh_morning", False)),
            "predicted_emh_evening": bool(pred_row.get("emh_evening", False)),
            "official_emh_evening": bool(official_row.get("emh_evening", False)),
            "confidence": pred_row.get("confidence"),
            "days_until_date": days_until_date,
            "donor_date": pred_row.get("notes", "").replace("Donor: ", "").split()[0] if pred_row.get("notes") else None,
        })
    
    if not results:
        return pd.DataFrame()
    
    return pd.DataFrame(results)


def generate_report(
    accuracy_df: pd.DataFrame,
    output_base: Path,
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Generate markdown report with accuracy metrics.
    
    Args:
        accuracy_df: DataFrame with accuracy metrics
        output_base: Pipeline output base directory
        logger: Optional logger
    """
    if accuracy_df.empty:
        report = "# Donor Park Hours Accuracy Report\n\nNo data available yet.\n"
    else:
        report = "# Donor Park Hours Accuracy Report\n\n"
        report += f"**Generated**: {datetime.now(ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
        report += f"**Total Predictions Evaluated**: {len(accuracy_df):,}\n\n"
        
        # Overall metrics
        report += "## Overall Accuracy\n\n"
        report += f"- **Mean Opening Time Error**: {accuracy_df['opening_time_error_minutes'].mean():.1f} minutes\n"
        report += f"- **Median Opening Time Error**: {accuracy_df['opening_time_error_minutes'].median():.1f} minutes\n"
        report += f"- **Mean Closing Time Error**: {accuracy_df['closing_time_error_minutes'].mean():.1f} minutes\n"
        report += f"- **Median Closing Time Error**: {accuracy_df['closing_time_error_minutes'].median():.1f} minutes\n"
        report += f"- **EMH Morning Match Rate**: {accuracy_df['emh_morning_match'].mean() * 100:.1f}%\n"
        report += f"- **EMH Evening Match Rate**: {accuracy_df['emh_evening_match'].mean() * 100:.1f}%\n\n"
        
        # Accuracy by confidence
        report += "## Accuracy by Confidence Level\n\n"
        confidence_bins = [0.0, 0.5, 0.7, 0.9, 1.0]
        for i in range(len(confidence_bins) - 1):
            mask = (accuracy_df["confidence"] >= confidence_bins[i]) & (accuracy_df["confidence"] < confidence_bins[i + 1])
            subset = accuracy_df[mask]
            if not subset.empty:
                report += f"### Confidence {confidence_bins[i]:.1f} - {confidence_bins[i + 1]:.1f}\n\n"
                report += f"- Count: {len(subset):,}\n"
                report += f"- Mean Opening Error: {subset['opening_time_error_minutes'].mean():.1f} min\n"
                report += f"- Mean Closing Error: {subset['closing_time_error_minutes'].mean():.1f} min\n"
                report += f"- EMH Morning Match: {subset['emh_morning_match'].mean() * 100:.1f}%\n"
                report += f"- EMH Evening Match: {subset['emh_evening_match'].mean() * 100:.1f}%\n\n"
        
        # Accuracy by park
        report += "## Accuracy by Park\n\n"
        for park_code in sorted(accuracy_df["park_code"].unique()):
            park_data = accuracy_df[accuracy_df["park_code"] == park_code]
            report += f"### {park_code}\n\n"
            report += f"- Count: {len(park_data):,}\n"
            report += f"- Mean Opening Error: {park_data['opening_time_error_minutes'].mean():.1f} min\n"
            report += f"- Mean Closing Error: {park_data['closing_time_error_minutes'].mean():.1f} min\n"
            report += f"- EMH Morning Match: {park_data['emh_morning_match'].mean() * 100:.1f}%\n"
            report += f"- EMH Evening Match: {park_data['emh_evening_match'].mean() * 100:.1f}%\n\n"
        
        # Accuracy by days until date
        report += "## Accuracy by Days Until Date\n\n"
        days_bins = [0, 30, 60, 90, 180, 365, float('inf')]
        for i in range(len(days_bins) - 1):
            if days_bins[i + 1] == float('inf'):
                mask = accuracy_df["days_until_date"] >= days_bins[i]
                label = f"{days_bins[i]}+ days"
            else:
                mask = (accuracy_df["days_until_date"] >= days_bins[i]) & (accuracy_df["days_until_date"] < days_bins[i + 1])
                label = f"{days_bins[i]}-{days_bins[i + 1]} days"
            
            subset = accuracy_df[mask]
            if not subset.empty:
                report += f"### {label}\n\n"
                report += f"- Count: {len(subset):,}\n"
                report += f"- Mean Opening Error: {subset['opening_time_error_minutes'].mean():.1f} min\n"
                report += f"- Mean Closing Error: {subset['closing_time_error_minutes'].mean():.1f} min\n\n"
        
        # Worst predictions
        report += "## Worst Predictions (Top 10 by Opening + Closing Error)\n\n"
        accuracy_df["total_error"] = accuracy_df["opening_time_error_minutes"] + accuracy_df["closing_time_error_minutes"]
        worst = accuracy_df.nlargest(10, "total_error")[
            ["park_date", "park_code", "opening_time_error_minutes", "closing_time_error_minutes",
             "confidence", "donor_date", "predicted_opening_time", "official_opening_time",
             "predicted_closing_time", "official_closing_time"]
        ]
        report += worst.to_markdown(index=False)
        report += "\n\n"
    
    # Write report
    reports_dir = output_base / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "park_hours_donor_accuracy.md"
    
    try:
        report_path.write_text(report, encoding="utf-8")
        if logger:
            logger.info(f"Wrote report: {report_path}")
    except Exception as e:
        if logger:
            logger.error(f"Failed to write report: {e}")
        raise


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Report donor park hours accuracy"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence to include (default: 0.0 = all)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Donor Park Hours Accuracy Report")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Min confidence: {args.min_confidence}")

    # Load versioned table
    versioned_df = load_versioned_table(base)
    if versioned_df is None or versioned_df.empty:
        logger.error("Versioned park hours table not found. Run migrate_park_hours_to_versioned.py first.")
        sys.exit(1)

    logger.info(f"Loaded versioned table: {len(versioned_df):,} rows")

    # Calculate accuracy metrics
    accuracy_df = calculate_accuracy_metrics(
        versioned_df,
        min_confidence=args.min_confidence,
        logger=logger,
    )

    if accuracy_df.empty:
        logger.warning("No accuracy data available (no predicted versions with matching official versions)")
    else:
        logger.info(f"Calculated accuracy for {len(accuracy_df):,} predictions")

    # Generate report
    generate_report(accuracy_df, base, logger)

    # Also save CSV for analysis
    if not accuracy_df.empty:
        reports_dir = base / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        csv_path = reports_dir / "park_hours_donor_accuracy.csv"
        try:
            accuracy_df.to_csv(csv_path, index=False)
            logger.info(f"Saved accuracy data CSV: {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to save CSV: {e}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
