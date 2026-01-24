#!/usr/bin/env python3
"""
Date Group ID (dimDateGroupID) Builder

================================================================================
PURPOSE
================================================================================
Builds a single dimension table that combines:
  - Date spine (dimDate-like): park_date and calendar attributes
  - Holiday codes and names (dimHolidays-like)
  - date_group_id: categorical label for modeling and cohort analysis

Output: dimension_tables/dimdategroupid.csv with all date fields, holiday fields,
and date_group_id. Replaces the legacy separate dimdate, dimholidays, and
dimdategroupid tables with one combined table.

Adapted from legacy Julia: run_dimDate.jl, run_dimHolidays.jl, run_dimDateGroupID.jl.
Same logic (Easter, holidays, GFR is_easter_over, week_of_month, direct_map, NJC,
Dec 27–30); we always overwrite and write only to dimension_tables/ (no S3).

================================================================================
DATE RANGE
================================================================================
  - Start: 2005-01-01 (legacy default)
  - End: today + 2 years (for prediction). "Today" = park_day in Eastern:
    same 6 AM rule as wait-time ETL: if Eastern hour < 6, use previous
    calendar date.

================================================================================
OUTPUT
================================================================================
  - dimension_tables/dimdategroupid.csv under --output-base
  - Logs: output/logs/build_dimdategroupid_YYYYMMDD_HHMMSS.log

================================================================================
USAGE
================================================================================
  python src/build_dimdategroupid.py
  python src/build_dimdategroupid.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo


# =============================================================================
# CONFIGURATION
# =============================================================================

START_DATE = date(2005, 1, 1)
YEARS_AHEAD = 2
EASTERN = ZoneInfo("America/New_York")
# Default to project's output/ directory (relative to script location)
DEFAULT_OUTPUT_BASE = Path(__file__).parent.parent / "output"
DIMDATEGROUPID_NAME = "dimdategroupid.csv"

# Holiday code -> date_group_id label (Julia direct_map). Uppercased on output.
DIRECT_MAP = {
    "ASH": "Ash_Wednesday",
    "EST": "Easter_Saturday",
    "ESS": "Easter_Sunday",
    "ESM": "Easter_Monday",
    "GFR": "Good_Friday",
    "HAL": "Halloween",
    "IND": "Independence_Day",
    "LAB": "Labor_Day",
    "MEM": "Memorial_Day",
    "MLK": "Martin_Luther_King_Day",
    "NYD": "New_Years_Day",
    "NYE": "New_Years_Eve",
    "PRS": "Presidents_Day",
    "THK": "Thanksgiving",
    "VET": "Veterans_Day",
    "BOX": "Boxing_Day",
    "COL": "Columbus_Day",
    "CMD": "Christmas_Day",
    "CME": "Christmas_Eve",
    "MGR": "Mardi_Gras",
    "PMP": "Presidents_Day_With_Mardi_Gras",
    "PMM": "Mardi_Gras_With_Presidents_Day",
}

# holidaycode -> holidayname (Julia name_map). PMP/PMM overwritten after PRS/MGR.
NAME_MAP = {
    "NYD": "New Year's Day",
    "MLK": "Martin Luther King Jr. Day",
    "PRS": "Presidents' Day",
    "MGR": "Mardi Gras",
    "PMP": "Presidents' Day / Mardi Gras (Presidents' Day)",
    "PMM": "Presidents' Day / Mardi Gras (Mardi Gras)",
    "ASH": "Ash Wednesday",
    "GFR": "Good Friday",
    "EST": "Easter Saturday",
    "ESS": "Easter Sunday",
    "ESM": "Easter Monday",
    "MEM": "Memorial Day",
    "IND": "Independence Day",
    "LAB": "Labor Day",
    "COL": "Columbus Day",
    "HAL": "Halloween",
    "NJC": "Jersey Week",
    "VET": "Veterans Day",
    "THK": "Thanksgiving",
    "CME": "Christmas Eve",
    "CMD": "Christmas Day",
    "BOX": "Boxing Day",
    "NYE": "New Year's Eve",
}


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(log_dir: Path) -> logging.Logger:
    """Set up file and console logging. Log file: build_dimdategroupid_YYYYMMDD_HHMMSS.log."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"build_dimdategroupid_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
# TODAY (PARK_DAY IN EASTERN)
# =============================================================================
# 6 AM rule: if Eastern hour < 6, use previous calendar date. Matches wait-time
# derive_park_date logic; Eastern is the reference for "today" in the dimension.

def today_park_day_eastern() -> date:
    """Return today's park_day in Eastern (6 AM rollover)."""
    now = datetime.now(EASTERN)
    if now.hour < 6:
        return (now.date() - timedelta(days=1))
    return now.date()


# =============================================================================
# EASTER (ANONYMOUS GREGORIAN)
# =============================================================================

def easter_date(year: int) -> date:
    """Compute Easter Sunday for the given year (Anonymous Gregorian algorithm)."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l_val = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_val) // 451
    month = (h + l_val - 7 * m + 114) // 31
    day = (h + l_val - 7 * m + 114) % 31 + 1
    return date(year, month, day)


# =============================================================================
# DIMDATE (DATE SPINE + ATTRIBUTES)
# =============================================================================

def build_dimdate(today: date, logger: logging.Logger) -> pd.DataFrame:
    """
    Build date spine from START_DATE to today + YEARS_AHEAD.
    Add year, month, day, day_of_week, quarter, week_of_year, day_of_year,
    month_name, month_mmm, day_of_week_name, day_of_week_ddd, offsets, flags, etc.
    """
    end = today + timedelta(days=365 * YEARS_AHEAD)
    dr = pd.date_range(START_DATE, end, freq="D")
    df = pd.DataFrame({"park_date": pd.to_datetime(dr)})
    df["year"] = df["park_date"].dt.year
    df["month"] = df["park_date"].dt.month
    df["day"] = df["park_date"].dt.day
    iso = df["park_date"].dt.isocalendar()
    df["day_of_week"] = iso["day"]  # Mon=1 .. Sun=7
    df["quarter"] = df["park_date"].dt.quarter
    df["week_of_year"] = iso["week"].astype("int64")
    df["day_of_year"] = df["park_date"].dt.dayofyear
    df["month_name"] = df["park_date"].dt.strftime("%B")
    df["month_mmm"] = df["park_date"].dt.strftime("%b")
    df["month_m"] = df["month_mmm"].str[0]
    df["day_of_week_name"] = df["park_date"].dt.strftime("%A")
    df["day_of_week_ddd"] = df["park_date"].dt.strftime("%a")
    df["day_of_week_d"] = df["day_of_week_ddd"].str[0]
    df["month_year_mmm_yyyy"] = df["month_mmm"] + "-" + df["year"].astype(str)
    df["quarter_year_q_yyyy"] = "Q" + df["quarter"].astype(str) + "-" + df["year"].astype(str)
    df["year_yy"] = "'" + (df["year"] % 100).astype(str).str.zfill(2)

    today_dt = pd.Timestamp(today)
    df["cur_day_offset"] = (df["park_date"] - today_dt).dt.days
    df["cur_month_offset"] = (
        (df["year"] - today_dt.year) * 12 + df["month"] - today_dt.month
    )
    df["cur_quarter_offset"] = (
        (df["year"] - today_dt.year) * 4
        + (df["month"] - 1) // 3
        - (today_dt.month - 1) // 3
    )
    df["cur_year_offset"] = df["year"] - today_dt.year
    df["future_date"] = (df["park_date"] > today_dt).map({True: "Future", False: "Past"})

    df["ytd_flag"] = df["day_of_year"] <= today_dt.dayofyear
    df["mtd_flag"] = df["day"] <= today_dt.day

    ym = df["year"].astype(str) + "_" + df["month"].astype(str).str.zfill(2) + df["month_mmm"]
    df["output_file_label"] = ym.str.upper()

    logger.info(f"DimDate spine: {len(df):,} rows ({df['park_date'].min()} to {df['park_date'].max()})")
    return df


# =============================================================================
# DIMHOLIDAYS (HOLIDAY CODES + NAMES)
# =============================================================================

def add_holidays(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Add holidaycode and holidayname. Default NONE / None.
    Easter-derived (MGR, ASH, GFR, EST, ESS, ESM), fixed dates, nth-weekday rules.
    PMP/PMM overwrite when Presidents' Day and Mardi Gras are adjacent.
    """
    df = df.copy()
    df["holidaycode"] = "NONE"
    df["holidayname"] = "None"

    park_date = df["park_date"].dt.date
    year = df["year"]
    month = df["month"]
    day = df["day"]
    dow = df["day_of_week_name"]

    easter_dates = df["year"].map(easter_date)  # Series of date

    df.loc[(month == 1) & (day == 1), "holidaycode"] = "NYD"
    df.loc[(month == 1) & (dow == "Monday") & (day >= 15) & (day <= 21), "holidaycode"] = "MLK"
    df.loc[(month == 2) & (dow == "Monday") & (day >= 15) & (day <= 21), "holidaycode"] = "PRS"
    df.loc[park_date == (easter_dates - timedelta(days=47)), "holidaycode"] = "MGR"
    df.loc[park_date == (easter_dates - timedelta(days=46)), "holidaycode"] = "ASH"
    df.loc[park_date == (easter_dates - timedelta(days=2)), "holidaycode"] = "GFR"
    df.loc[park_date == (easter_dates - timedelta(days=1)), "holidaycode"] = "EST"
    df.loc[park_date == easter_dates, "holidaycode"] = "ESS"
    df.loc[park_date == (easter_dates + timedelta(days=1)), "holidaycode"] = "ESM"
    df.loc[(month == 5) & (dow == "Monday") & (day >= 25) & (day <= 31), "holidaycode"] = "MEM"
    df.loc[(month == 7) & (day == 4), "holidaycode"] = "IND"
    df.loc[(month == 9) & (dow == "Monday") & (day >= 1) & (day <= 7), "holidaycode"] = "LAB"
    df.loc[(month == 10) & (dow == "Monday") & (day >= 8) & (day <= 14), "holidaycode"] = "COL"
    df.loc[(month == 10) & (day == 31), "holidaycode"] = "HAL"
    # Jersey Week: Tue–Sat in Nov 2–8
    jersey = (month == 11) & (dow.isin(["Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"])) & (day >= 2) & (day <= 8)
    df.loc[jersey, "holidaycode"] = "NJC"
    df.loc[(month == 11) & (day == 11), "holidaycode"] = "VET"
    df.loc[(month == 11) & (dow == "Thursday") & (day >= 22) & (day <= 28), "holidaycode"] = "THK"
    df.loc[(month == 12) & (day == 24), "holidaycode"] = "CME"
    df.loc[(month == 12) & (day == 25), "holidaycode"] = "CMD"
    df.loc[(month == 12) & (day == 26), "holidaycode"] = "BOX"
    df.loc[(month == 12) & (day == 31), "holidaycode"] = "NYE"

    # PMP / PMM: Presidents' Day and Mardi Gras adjacent (overwrite PRS/MGR)
    prs_idx = df.index[df["holidaycode"] == "PRS"].tolist()
    mgr_set = set(df.index[df["holidaycode"] == "MGR"])
    for i in prs_idx:
        if (i + 1) in mgr_set:
            df.at[i, "holidaycode"] = "PMP"
            df.at[i + 1, "holidaycode"] = "PMM"

    for code, name in NAME_MAP.items():
        df.loc[df["holidaycode"] == code, "holidayname"] = name

    logger.info("Holidays applied (PMP/PMM overwrite when adjacent)")
    return df


# =============================================================================
# DATE_GROUP_ID
# =============================================================================

def add_date_group_id(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Compute date_group_id. Default: easter_prefix + month_mmm + _ + week + day_of_week_ddd.
    Overrides: direct_map, NJC -> Jersey_Week_ + ddd, Dec 27–30 -> Dec27..Dec30.
    GFR sets is_easter_over for rest of year; Mar/Apr get Before/After_Easter_.
    """
    df = df.copy()
    df["date_group_id"] = "...needs assigning..."
    df["is_easter_over"] = 0

    # GFR: from each Good Friday onward in that year, is_easter_over = 1
    gfr = df["holidaycode"] == "GFR"
    for idx in df.index[gfr]:
        y = df.at[idx, "year"]
        mask = (df["year"] == y) & (df["park_date"] >= df.at[idx, "park_date"])
        df.loc[mask, "is_easter_over"] = 1

    df["easter_prefix"] = ""
    mar_apr = (df["month"] >= 3) & (df["month"] <= 4)
    df.loc[mar_apr & (df["is_easter_over"] == 0), "easter_prefix"] = "Before_Easter_"
    df.loc[mar_apr & (df["is_easter_over"] == 1), "easter_prefix"] = "After_Easter_"

    df["week_of_month"] = ((df["day"] - 1) // 7 + 1).astype(int)
    df["week"] = "week" + df["week_of_month"].astype(str) + "_"
    df.loc[df["week_of_month"].isin([4, 5]), "week"] = "week4or5_"

    default = df["easter_prefix"] + df["month_mmm"] + "_" + df["week"] + df["day_of_week_ddd"]
    df["date_group_id"] = default

    for code, label in DIRECT_MAP.items():
        df.loc[df["holidaycode"] == code, "date_group_id"] = label

    njc = df["holidaycode"] == "NJC"
    df.loc[njc, "date_group_id"] = "Jersey_Week_" + df.loc[njc, "day_of_week_ddd"]

    df.loc[(df["month"] == 12) & (df["day"] == 27), "date_group_id"] = "Dec27"
    df.loc[(df["month"] == 12) & (df["day"] == 28), "date_group_id"] = "Dec28"
    df.loc[(df["month"] == 12) & (df["day"] == 29), "date_group_id"] = "Dec29"
    df.loc[(df["month"] == 12) & (df["day"] == 30), "date_group_id"] = "Dec30"

    df["date_group_id"] = df["date_group_id"].str.upper()
    logger.info("date_group_id assigned (direct_map, NJC, Dec27-Dec30)")
    return df


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build dimension_tables/dimdategroupid.csv (date spine + holidays + date_group_id)"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=DEFAULT_OUTPUT_BASE,
        help="Output base directory (dimension_tables and logs under it)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    log_dir = base / "logs"
    dim_dir = base / "dimension_tables"
    logger = setup_logging(log_dir)

    logger.info("=" * 60)
    logger.info("Date Group ID (dimDateGroupID) build")
    logger.info("=" * 60)
    logger.info(f"Output base: {base}")

    today = today_park_day_eastern()
    logger.info(f"Today (park_day Eastern): {today}")

    # ----- STEP 1: DimDate spine -----
    df = build_dimdate(today, logger)

    # ----- STEP 2: Holidays -----
    df = add_holidays(df, logger)

    # ----- STEP 3: date_group_id -----
    df = add_date_group_id(df, logger)

    # Drop intermediates used only for computation
    drop = ["is_easter_over", "easter_prefix", "week_of_month", "week"]
    df = df.drop(columns=[c for c in drop if c in df.columns])

    # Ensure park_date is date-only string for CSV (YYYY-MM-DD)
    df["park_date"] = pd.to_datetime(df["park_date"]).dt.strftime("%Y-%m-%d")
    df = df.sort_values("park_date").reset_index(drop=True)

    dim_dir.mkdir(parents=True, exist_ok=True)
    out_path = dim_dir / DIMDATEGROUPID_NAME
    try:
        df.to_csv(out_path, index=False)
        logger.info(f"Wrote {out_path} ({len(df):,} rows, {len(df.columns)} columns)")
    except Exception as e:
        logger.error(f"Failed to write {out_path}: {e}")
        sys.exit(1)

    logger.info("Done.")


if __name__ == "__main__":
    main()
