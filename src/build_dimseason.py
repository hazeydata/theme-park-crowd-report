#!/usr/bin/env python3
"""
Season (dimSeason) Builder

================================================================================
PURPOSE
================================================================================
Builds dimension_tables/dimseason.csv from dimdategroupid. Assigns season and
season_year based on date_group_id patterns (holidays, carry days, Presidents +
Mardi Gras overlap, general seasonal buckets). Used for modeling and cohort
analysis.

  - READS dimension_tables/dimdategroupid.csv (park_date, date_group_id)
  - ASSIGNS season via CHRISTMAS_PEAK override, holiday patterns (with carry),
    Presidents Day + Mardi Gras combined window, then seasonal patterns
  - ADDS season_year (season_YYYY; Jan CHRISTMAS/CHRISTMAS_PEAK use prior year)
  - WRITES dimension_tables/dimseason.csv (park_date, season, season_year)

Adapted from legacy Julia run_dimSeason.jl. Always overwrites. Depends on
dimdategroupid; run build_dimdategroupid first.

================================================================================
INPUT
================================================================================
  - dimension_tables/dimdategroupid.csv under --output-base
  - Requires columns: park_date, date_group_id (and year, month for logic)

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimseason.csv under --output-base
  - Logs: logs/build_dimseason_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/build_dimseason.py
  python src/build_dimseason.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from utils import get_output_base

# =============================================================================
# CONFIGURATION
# =============================================================================

DIMDATEGROUPID_NAME = "dimdategroupid.csv"
DIMSEASON_NAME = "dimseason.csv"

# (regex_pattern, season_label, carry_before, carry_after). Match date_group_id.
HOLIDAY_PATTERNS = [
    (r"MARTIN_LUTHER|MLK", "MLK_JR_DAY", 3, 2),
    (r"PRESIDENTS", "PRESIDENTS_DAY", 3, 2),
    (r"MARDI_GRAS", "MARDI_GRAS", 3, 2),
    (r"MEMORIAL", "MEMORIAL_DAY", 3, 2),
    (r"LABOR", "LABOR_DAY", 3, 2),
    (r"THANKSGIVING", "THANKSGIVING", 1, 1),
    (r"CHRISTMAS|NEW_YEAR|BOXING", "CHRISTMAS", 1, 1),
    (r"EASTER_MONDAY|EASTER_SATURDAY|EASTER_SUNDAY|GOOD_FRIDAY", "EASTER", 1, 1),
    (r"JERSEY", "JERSEY_WEEK", 1, 1),
    (r"HALLOWEEN", "HALLOWEEN", 1, 1),
    (r"VETERANS", "VETERANS_DAY", 1, 1),
    (r"COLUMBUS", "COLUMBUS_DAY", 1, 1),
    (r"MARATHON", "MARATHON", 1, 1),
]

# (regex_pattern, season_label). Only assign if season still blank.
SEASONAL_PATTERNS = [
    (r"AFTER_EASTER", "AFTER_EASTER"),
    (r"BEFORE_EASTER", "BEFORE_EASTER"),
    (r"MAY_WEEK", "SPRING"),
    (r"JUN_WEEK|JUL_WEEK|INDEPENDENCE|AUG_WEEK", "SUMMER"),
    (r"SEP_WEEK|OCT_WEEK|NOV_WEEK", "AUTUMN"),
    (r"DEC_WEEK|JAN_WEEK|FEB_WEEK", "WINTER"),
]


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging. Log file: build_dimseason_YYYYMMDD_HHMMSS.log."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"build_dimseason_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# SEASON ASSIGNMENT
# =============================================================================

def assign_seasons(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Assign season from date_group_id. CHRISTMAS_PEAK override, holiday patterns
    with carry, Presidents+Mardi Gras combined window, then seasonal patterns.
    """
    df = df.copy()
    df["date_group_id"] = df["date_group_id"].astype(str).str.upper()
    df["season"] = ""

    n = len(df)
    park_date = pd.to_datetime(df["park_date"])
    month = df["month"]
    day = df["day"]

    # ----- CHRISTMAS_PEAK: Dec 27 – Jan 1 inclusive -----
    mask_cp = ((month == 12) & (day >= 27)) | ((month == 1) & (day <= 1))
    df.loc[mask_cp, "season"] = "CHRISTMAS_PEAK"
    logger.info("CHRISTMAS_PEAK override (Dec 27-Jan 1) applied")

    # ----- Holiday patterns with carry -----
    for pattern, label, carry_before, carry_after in HOLIDAY_PATTERNS:
        rx = re.compile(pattern)
        match_mask = df["date_group_id"].str.contains(rx, na=False)
        match_idx = df.index[match_mask].tolist()
        for idx in match_idx:
            pos = df.index.get_loc(idx)
            if df.at[idx, "season"] == "":
                df.at[idx, "season"] = label
            for o in range(1, carry_before + 1):
                i = pos - o
                if i >= 0 and df.iloc[i]["season"] == "":
                    df.iloc[i, df.columns.get_loc("season")] = label
            for o in range(1, carry_after + 1):
                i = pos + o
                if i < n and df.iloc[i]["season"] == "":
                    df.iloc[i, df.columns.get_loc("season")] = label
    logger.info("Holiday patterns (with carry) applied")

    # ----- Presidents Day + Mardi Gras combined window -----
    combined = "PRESIDENTS_DAY_MARDI_GRAS"
    pres_dates = set(df.loc[df["season"] == "PRESIDENTS_DAY", "park_date"].astype(str))
    mardi_dates = set(df.loc[df["season"] == "MARDI_GRAS", "park_date"].astype(str))
    date_to_pos = {str(d): i for i, d in enumerate(df["park_date"])}

    for pd_str in pres_dates:
        d = pd.to_datetime(pd_str)
        window = [d + timedelta(days=k) for k in range(-3, 4)]
        window_str = {x.strftime("%Y-%m-%d") for x in window}
        if window_str & mardi_dates:
            for w in window:
                ws = w.strftime("%Y-%m-%d")
                if ws in date_to_pos:
                    df.iloc[date_to_pos[ws], df.columns.get_loc("season")] = combined
    logger.info("Presidents Day + Mardi Gras combined window applied")

    # ----- Seasonal patterns (only if still blank) -----
    for pattern, label in SEASONAL_PATTERNS:
        rx = re.compile(pattern)
        blank = df["season"] == ""
        matches = df["date_group_id"].str.contains(rx, na=False)
        df.loc[blank & matches, "season"] = label
    logger.info("Seasonal patterns applied")

    # ----- season_year -----
    year = df["year"]
    season = df["season"]
    jan_christmas = (month == 1) & (season.isin(["CHRISTMAS", "CHRISTMAS_PEAK"]))
    y = year.where(~jan_christmas, year - 1)
    df["season_year"] = season + "_" + y.astype(int).astype(str)
    logger.info("season_year added")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build dimension_tables/dimseason.csv from dimdategroupid"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Season (dimSeason) build")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    in_path = dim_dir / DIMDATEGROUPID_NAME
    if not in_path.exists():
        logger.error(f"Missing input: {in_path}. Run build_dimdategroupid first.")
        sys.exit(1)

    try:
        df = pd.read_csv(in_path, low_memory=False)
    except Exception as e:
        logger.error(f"Failed to read {in_path}: {e}")
        sys.exit(1)

    required = {"park_date", "date_group_id", "year", "month", "day"}
    missing = required - set(df.columns)
    if missing:
        logger.error(f"dimdategroupid missing columns: {missing}")
        sys.exit(1)

    df = assign_seasons(df, logger)
    out_df = df[["park_date", "season", "season_year"]].copy()
    out_df = out_df.sort_values("park_date").reset_index(drop=True)

    dim_dir.mkdir(parents=True, exist_ok=True)
    out_path = dim_dir / DIMSEASON_NAME
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        out_df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, out_path)
        logger.info(f"Wrote {out_path} ({len(out_df):,} rows)")
    except Exception as e:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        logger.error(f"Failed to write {out_path}: {e}")
        sys.exit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
