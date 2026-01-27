#!/usr/bin/env python3
"""
Report Posted Prediction Accuracy

Analyzes how well predicted POSTED (from aggregates) matches observed POSTED when it arrives.
Tracks accuracy metrics over time to evaluate the aggregation approach.

Usage:
    python scripts/report_posted_accuracy.py
    python scripts/report_posted_accuracy.py --output-base "D:\\Path" --min-days-ago 30
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from zoneinfo import ZoneInfo

from processors.posted_aggregates import get_predicted_posted, load_posted_aggregates
from processors.features import add_dategroupid, add_park_code, add_park_date, load_dims
from utils import get_output_base


def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"report_posted_accuracy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def load_historical_posted(
    output_base: Path,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Load historical POSTED data from fact tables.
    
    Args:
        output_base: Pipeline output base directory
        min_date: Minimum park_date to include
        max_date: Maximum park_date to include
        logger: Optional logger
    
    Returns:
        DataFrame with POSTED observations
    """
    clean_dir = output_base / "fact_tables" / "clean"
    if not clean_dir.exists():
        if logger:
            logger.warning(f"Fact tables directory not found: {clean_dir}")
        return pd.DataFrame()
    
    # Load dimensions for dategroupid
    dims = load_dims(output_base, logger)
    dimdategroupid = dims.get("dimdategroupid")
    
    all_posted = []
    csvs = list(clean_dir.rglob("*.csv"))
    
    if logger:
        logger.info(f"Scanning {len(csvs)} fact table CSVs for POSTED data...")
    
    for csv_path in csvs:
        try:
            df = pd.read_csv(csv_path, low_memory=False)
            df_posted = df[df["wait_time_type"] == "POSTED"].copy()
            if df_posted.empty:
                continue
            
            # Add park_date and park_code
            df_posted = add_park_date(df_posted)
            df_posted = add_park_code(df_posted)
            
            # Filter by date range
            if min_date or max_date:
                df_posted["park_date_obj"] = pd.to_datetime(df_posted["park_date"], errors="coerce").dt.date
                if min_date:
                    df_posted = df_posted[df_posted["park_date_obj"] >= min_date]
                if max_date:
                    df_posted = df_posted[df_posted["park_date_obj"] <= max_date]
                df_posted = df_posted.drop(columns=["park_date_obj"], errors="ignore")
            
            if df_posted.empty:
                continue
            
            # Add dategroupid
            if dimdategroupid is not None:
                df_posted = add_dategroupid(df_posted, dimdategroupid, logger)
            
            # Extract hour from observed_at
            observed_dt = pd.to_datetime(df_posted["observed_at"], errors="coerce")
            df_posted["hour"] = observed_dt.dt.hour
            df_posted["minute"] = observed_dt.dt.minute
            
            # Round to nearest 5-minute slot
            df_posted["time_slot_minutes"] = (df_posted["hour"] * 60 + (df_posted["minute"] // 5) * 5)
            df_posted["time_slot"] = (
                (df_posted["time_slot_minutes"] // 60).astype(int).astype(str).str.zfill(2) + ":" +
                (df_posted["time_slot_minutes"] % 60).astype(int).astype(str).str.zfill(2)
            )
            
            all_posted.append(df_posted)
        
        except Exception as e:
            if logger:
                logger.debug(f"Error reading {csv_path}: {e}")
            continue
    
    if not all_posted:
        return pd.DataFrame()
    
    combined = pd.concat(all_posted, ignore_index=True)
    
    if logger:
        logger.info(f"Loaded {len(combined):,} POSTED observations")
    
    return combined


def calculate_accuracy_metrics(
    output_base: Path,
    min_days_ago: int = 0,
    max_days_ago: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Calculate accuracy metrics for predicted POSTED.
    
    For each observed POSTED, get the predicted POSTED (as if we were forecasting)
    and compare them.
    
    Args:
        output_base: Pipeline output base directory
        min_days_ago: Minimum days ago to include (default: 0 = all)
        max_days_ago: Maximum days ago to include (default: None = all)
        logger: Optional logger
    
    Returns:
        DataFrame with accuracy metrics
    """
    # Load aggregates
    aggregates = load_posted_aggregates(output_base, logger)
    if aggregates is None or aggregates.empty:
        if logger:
            logger.error("Posted aggregates not found. Run build_posted_aggregates.py first.")
        return pd.DataFrame()
    
    # Load historical POSTED data
    today = date.today()
    min_date = (today - timedelta(days=max_days_ago)) if max_days_ago else None
    max_date = (today - timedelta(days=min_days_ago)) if min_days_ago > 0 else None
    
    observed = load_historical_posted(output_base, min_date=min_date, max_date=max_date, logger=logger)
    
    if observed.empty:
        if logger:
            logger.warning("No observed POSTED data found")
        return pd.DataFrame()
    
    # For each observed POSTED, get predicted POSTED
    results = []
    
    if logger:
        logger.info("Comparing predicted vs observed POSTED...")
    
    for idx, row in observed.iterrows():
        entity_code = row["entity_code"]
        park_date = pd.to_datetime(row["park_date"], errors="coerce").date()
        hour = int(row["hour"])
        observed_posted = row["wait_time_minutes"]
        
        if pd.isna(park_date) or pd.isna(observed_posted) or pd.isna(hour):
            continue
        
        # Get predicted POSTED (as if we were forecasting this date)
        # Use a date before the observed date to simulate forecasting
        # For accuracy tracking, we'll use the aggregates as-is
        predicted_posted = get_predicted_posted(
            entity_code,
            park_date,
            hour,
            aggregates=aggregates,
            output_base=output_base,
            logger=None,  # Don't log every lookup
        )
        
        if predicted_posted is None:
            continue
        
        # Calculate error
        error = abs(predicted_posted - observed_posted)
        pct_error = (error / observed_posted * 100) if observed_posted > 0 else None
        
        # Get metadata from aggregates
        agg_row = aggregates[
            (aggregates["entity_code"] == entity_code) &
            (aggregates["dategroupid"] == row.get("dategroupid")) &
            (aggregates["hour"] == hour)
        ]
        
        avg_recency_weight = None
        posted_count = None
        if not agg_row.empty:
            avg_recency_weight = agg_row.iloc[0].get("avg_recency_weight")
            posted_count = agg_row.iloc[0].get("posted_count")
        
        # Days ago
        days_ago = (date.today() - park_date).days
        
        results.append({
            "park_date": park_date.strftime("%Y-%m-%d"),
            "entity_code": entity_code,
            "park_code": row.get("park_code"),
            "dategroupid": row.get("dategroupid"),
            "hour": hour,
            "time_slot": row.get("time_slot"),
            "observed_posted": observed_posted,
            "predicted_posted": predicted_posted,
            "error_minutes": error,
            "pct_error": pct_error,
            "avg_recency_weight": avg_recency_weight,
            "posted_count": posted_count,
            "days_ago": days_ago,
        })
    
    if not results:
        return pd.DataFrame()
    
    df = pd.DataFrame(results)
    
    if logger:
        logger.info(f"Calculated accuracy for {len(df):,} predictions")
    
    return df


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
        report = "# Posted Prediction Accuracy Report\n\nNo data available yet.\n"
    else:
        report = "# Posted Prediction Accuracy Report\n\n"
        report += f"**Generated**: {datetime.now(ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
        report += f"**Total Predictions Evaluated**: {len(accuracy_df):,}\n\n"
        
        # Overall metrics
        report += "## Overall Accuracy\n\n"
        report += f"- **Mean Absolute Error**: {accuracy_df['error_minutes'].mean():.1f} minutes\n"
        report += f"- **Median Absolute Error**: {accuracy_df['error_minutes'].median():.1f} minutes\n"
        report += f"- **Root Mean Squared Error**: {(accuracy_df['error_minutes']**2).mean()**0.5:.1f} minutes\n"
        
        pct_errors = accuracy_df['pct_error'].dropna()
        if not pct_errors.empty:
            report += f"- **Mean Absolute Percentage Error**: {pct_errors.mean():.1f}%\n"
            report += f"- **Median Absolute Percentage Error**: {pct_errors.median():.1f}%\n"
        
        report += "\n"
        
        # Accuracy by recency weight
        report += "## Accuracy by Recency Weight\n\n"
        weight_bins = [0.0, 0.3, 0.5, 0.7, 0.9, 1.0]
        for i in range(len(weight_bins) - 1):
            mask = (accuracy_df["avg_recency_weight"] >= weight_bins[i]) & (accuracy_df["avg_recency_weight"] < weight_bins[i + 1])
            subset = accuracy_df[mask]
            if not subset.empty:
                report += f"### Weight {weight_bins[i]:.1f} - {weight_bins[i + 1]:.1f}\n\n"
                report += f"- Count: {len(subset):,}\n"
                report += f"- Mean Error: {subset['error_minutes'].mean():.1f} min\n"
                report += f"- Median Error: {subset['error_minutes'].median():.1f} min\n"
                if not subset['pct_error'].dropna().empty:
                    report += f"- Mean % Error: {subset['pct_error'].mean():.1f}%\n"
                report += "\n"
        
        # Accuracy by posted_count (sample size)
        report += "## Accuracy by Sample Size\n\n"
        count_bins = [0, 10, 50, 100, 500, float('inf')]
        for i in range(len(count_bins) - 1):
            if count_bins[i + 1] == float('inf'):
                mask = accuracy_df["posted_count"] >= count_bins[i]
                label = f"{count_bins[i]}+ observations"
            else:
                mask = (accuracy_df["posted_count"] >= count_bins[i]) & (accuracy_df["posted_count"] < count_bins[i + 1])
                label = f"{count_bins[i]}-{count_bins[i + 1]} observations"
            
            subset = accuracy_df[mask]
            if not subset.empty:
                report += f"### {label}\n\n"
                report += f"- Count: {len(subset):,}\n"
                report += f"- Mean Error: {subset['error_minutes'].mean():.1f} min\n"
                report += f"- Median Error: {subset['error_minutes'].median():.1f} min\n"
                report += "\n"
        
        # Accuracy by park
        report += "## Accuracy by Park\n\n"
        for park_code in sorted(accuracy_df["park_code"].dropna().unique()):
            park_data = accuracy_df[accuracy_df["park_code"] == park_code]
            report += f"### {park_code}\n\n"
            report += f"- Count: {len(park_data):,}\n"
            report += f"- Mean Error: {park_data['error_minutes'].mean():.1f} min\n"
            report += f"- Median Error: {park_data['error_minutes'].median():.1f} min\n"
            if not park_data['pct_error'].dropna().empty:
                report += f"- Mean % Error: {park_data['pct_error'].mean():.1f}%\n"
            report += "\n"
        
        # Accuracy by hour
        report += "## Accuracy by Hour of Day\n\n"
        for hour in sorted(accuracy_df["hour"].unique()):
            hour_data = accuracy_df[accuracy_df["hour"] == hour]
            report += f"### {hour:02d}:00\n\n"
            report += f"- Count: {len(hour_data):,}\n"
            report += f"- Mean Error: {hour_data['error_minutes'].mean():.1f} min\n"
            report += f"- Median Error: {hour_data['error_minutes'].median():.1f} min\n"
            report += "\n"
        
        # Accuracy by days ago
        report += "## Accuracy by Days Ago\n\n"
        days_bins = [0, 7, 30, 90, 180, 365, float('inf')]
        for i in range(len(days_bins) - 1):
            if days_bins[i + 1] == float('inf'):
                mask = accuracy_df["days_ago"] >= days_bins[i]
                label = f"{days_bins[i]}+ days ago"
            else:
                mask = (accuracy_df["days_ago"] >= days_bins[i]) & (accuracy_df["days_ago"] < days_bins[i + 1])
                label = f"{days_bins[i]}-{days_bins[i + 1]} days ago"
            
            subset = accuracy_df[mask]
            if not subset.empty:
                report += f"### {label}\n\n"
                report += f"- Count: {len(subset):,}\n"
                report += f"- Mean Error: {subset['error_minutes'].mean():.1f} min\n"
                report += f"- Median Error: {subset['error_minutes'].median():.1f} min\n"
                report += "\n"
        
        # Worst predictions
        report += "## Worst Predictions (Top 20 by Error)\n\n"
        worst = accuracy_df.nlargest(20, "error_minutes")[
            ["park_date", "entity_code", "park_code", "hour", "time_slot",
             "observed_posted", "predicted_posted", "error_minutes", "pct_error",
             "posted_count", "avg_recency_weight"]
        ]
        report += worst.to_markdown(index=False)
        report += "\n\n"
        
        # Best predictions (smallest errors)
        report += "## Best Predictions (Top 20 - Smallest Errors)\n\n"
        best = accuracy_df.nsmallest(20, "error_minutes")[
            ["park_date", "entity_code", "park_code", "hour", "time_slot",
             "observed_posted", "predicted_posted", "error_minutes", "pct_error",
             "posted_count", "avg_recency_weight"]
        ]
        report += best.to_markdown(index=False)
        report += "\n\n"
    
    # Write report
    reports_dir = output_base / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = reports_dir / "posted_accuracy.md"
    
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
        description="Report posted prediction accuracy"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    ap.add_argument(
        "--min-days-ago",
        type=int,
        default=0,
        help="Minimum days ago to include (default: 0 = all)",
    )
    ap.add_argument(
        "--max-days-ago",
        type=int,
        help="Maximum days ago to include (default: None = all)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Posted Prediction Accuracy Report")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")
    logger.info(f"Min days ago: {args.min_days_ago}")
    if args.max_days_ago:
        logger.info(f"Max days ago: {args.max_days_ago}")

    # Calculate accuracy metrics
    accuracy_df = calculate_accuracy_metrics(
        base,
        min_days_ago=args.min_days_ago,
        max_days_ago=args.max_days_ago,
        logger=logger,
    )

    if accuracy_df.empty:
        logger.warning("No accuracy data available")
    else:
        logger.info(f"Calculated accuracy for {len(accuracy_df):,} predictions")

    # Generate report
    generate_report(accuracy_df, base, logger)

    # Also save CSV for analysis
    if not accuracy_df.empty:
        reports_dir = base / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        csv_path = reports_dir / "posted_accuracy.csv"
        try:
            accuracy_df.to_csv(csv_path, index=False)
            logger.info(f"Saved accuracy data CSV: {csv_path}")
        except Exception as e:
            logger.warning(f"Failed to save CSV: {e}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
