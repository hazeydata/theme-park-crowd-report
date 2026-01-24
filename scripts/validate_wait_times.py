#!/usr/bin/env python3
"""
Validate Wait Time Fact Table CSVs

Reads fact_tables/clean/YYYY-MM/{park}_{date}.csv and checks:
  - Schema: required columns, types
  - wait_time_minutes by wait_time_type:
      POSTED/ACTUAL: valid 0–1000 (int); outlier if >= 300
      PRIORITY:      valid -100..2000 or 8888; outlier if < -100 or (> 2000 and != 8888)

Invalid = outside valid range (fails validation).
Outlier = invalid, or valid-but-flagged (POSTED/ACTUAL >= 300).

Writes a JSON report and exits 1 if any invalid rows; else 0.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# Default output base (must match ETL). Override with --output-base.
DEFAULT_OUTPUT_BASE = Path(
    r"D:\Dropbox (TouringPlans.com)\stats team\pipeline\hazeydata\theme-park-crowd-report"
)
REQUIRED_COLUMNS = ["entity_code", "observed_at", "wait_time_type", "wait_time_minutes"]
VALID_WAIT_TYPES = {"POSTED", "ACTUAL", "PRIORITY"}

# Ranges
POSTED_ACTUAL_MIN, POSTED_ACTUAL_MAX = 0, 1000
POSTED_ACTUAL_OUTLIER_THRESHOLD = 300
PRIORITY_MIN, PRIORITY_MAX = -100, 2000
PRIORITY_SOLDOUT = 8888


def _parse_date_from_path(path: Path) -> str | None:
    """Extract YYYY-MM-DD from fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv."""
    name = path.stem  # e.g. mk_2026-01-24
    m = re.match(r"[a-z0-9]+_(\d{4}-\d{2}-\d{2})$", name, re.I)
    return m.group(1) if m else None


def _is_in_lookback(date_str: str, lookback_days: int) -> bool:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        delta = datetime.now().date() - d
        return 0 <= delta.days <= lookback_days
    except Exception:
        return False


def _check_wait_minutes(
    wt: str,
    val,
) -> tuple[bool, bool]:
    """
    Returns (is_valid, is_outlier).
    Invalid => outlier. Valid but suspicious (POSTED/ACTUAL >= 300) => outlier.
    """
    try:
        v = int(val)
    except (TypeError, ValueError):
        return False, True

    if wt in ("POSTED", "ACTUAL"):
        valid = POSTED_ACTUAL_MIN <= v <= POSTED_ACTUAL_MAX
        outlier = not valid or v >= POSTED_ACTUAL_OUTLIER_THRESHOLD
        return valid, outlier
    if wt == "PRIORITY":
        valid = (PRIORITY_MIN <= v <= PRIORITY_MAX) or v == PRIORITY_SOLDOUT
        outlier = not valid
        return valid, outlier
    return False, True


def validate_file(path: Path) -> dict:
    """
    Validate a single park-day CSV. Returns dict with keys:
      ok, schema_ok, total_rows, invalid_rows, outlier_rows, errors[], sample_invalid[], sample_outlier[]
    """
    out = {
        "ok": True,
        "schema_ok": True,
        "total_rows": 0,
        "invalid_rows": 0,
        "outlier_rows": 0,
        "errors": [],
        "sample_invalid": [],
        "sample_outlier": [],
    }
    try:
        df = pd.read_csv(path, nrows=0)
    except Exception as e:
        out["ok"] = False
        out["schema_ok"] = False
        out["errors"].append(f"read_csv: {e}")
        return out

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        out["ok"] = False
        out["schema_ok"] = False
        out["errors"].append(f"missing columns: {missing}")
        return out

    try:
        df = pd.read_csv(path)
    except Exception as e:
        out["ok"] = False
        out["errors"].append(f"read_csv full: {e}")
        return out

    out["total_rows"] = len(df)
    if df.empty:
        return out

    invalid_count = 0
    outlier_count = 0
    sample_invalid: list[dict] = []
    sample_outlier: list[dict] = []

    for idx, row in df.iterrows():
        wt = str(row.get("wait_time_type", "")).strip().upper()
        if wt not in VALID_WAIT_TYPES:
            invalid_count += 1
            outlier_count += 1
            if len(sample_invalid) < 5:
                sample_invalid.append(
                    {"row": int(idx), "wait_time_type": str(row.get("wait_time_type")), "wait_time_minutes": row.get("wait_time_minutes")}
                )
            continue

        valid, outlier = _check_wait_minutes(wt, row.get("wait_time_minutes"))
        if not valid:
            invalid_count += 1
        if outlier:
            outlier_count += 1
        if not valid and len(sample_invalid) < 5:
            sample_invalid.append(
                {
                    "row": int(idx),
                    "wait_time_type": wt,
                    "wait_time_minutes": row.get("wait_time_minutes"),
                    "entity_code": str(row.get("entity_code", ""))[:20],
                }
            )
        if outlier and len(sample_outlier) < 5:
            sample_outlier.append(
                {
                    "row": int(idx),
                    "wait_time_type": wt,
                    "wait_time_minutes": row.get("wait_time_minutes"),
                    "entity_code": str(row.get("entity_code", ""))[:20],
                }
            )

    out["invalid_rows"] = invalid_count
    out["outlier_rows"] = outlier_count
    out["sample_invalid"] = sample_invalid
    out["sample_outlier"] = sample_outlier
    if invalid_count > 0:
        out["ok"] = False
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Validate wait time fact table CSVs (schema, ranges, outliers)"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=DEFAULT_OUTPUT_BASE,
        help="Output base directory (fact_tables/clean under it)",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Only validate park-day files from the last N days (default: 7)",
    )
    ap.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write JSON report to this path (default: validation/validate_wait_times_YYYYMMDD_HHMMSS.json)",
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Validate all park-day files; ignore --lookback-days",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    clean_dir = base / "fact_tables" / "clean"
    if not clean_dir.is_dir():
        print(f"ERROR: {clean_dir} not found", file=sys.stderr)
        sys.exit(2)

    # Discover CSVs
    csvs: list[Path] = []
    for p in sorted(clean_dir.rglob("*.csv")):
        if not p.is_file():
            continue
        if args.all:
            csvs.append(p)
        else:
            d = _parse_date_from_path(p)
            if d and _is_in_lookback(d, args.lookback_days):
                csvs.append(p)

    # Validate
    results: list[dict] = []
    total_rows = 0
    total_invalid = 0
    total_outlier = 0
    files_with_invalid: list[str] = []
    files_with_outliers: list[str] = []

    for path in csvs:
        rel = path.relative_to(base) if base in path.parents else path
        r = validate_file(path)
        r["file"] = str(rel)
        results.append(r)
        total_rows += r["total_rows"]
        total_invalid += r["invalid_rows"]
        total_outlier += r["outlier_rows"]
        if r["invalid_rows"] > 0:
            files_with_invalid.append(r["file"])
        if r["outlier_rows"] > 0:
            files_with_outliers.append(r["file"])

    report_dir = base / "validation"
    report_dir.mkdir(parents=True, exist_ok=True)
    if args.report is not None:
        report_path = Path(args.report)
    else:
        report_path = report_dir / f"validate_wait_times_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    report = {
        "timestamp": datetime.now().isoformat(),
        "output_base": str(base),
        "lookback_days": None if args.all else args.lookback_days,
        "files_checked": len(csvs),
        "total_rows": total_rows,
        "total_invalid": total_invalid,
        "total_outlier": total_outlier,
        "files_with_invalid": files_with_invalid,
        "files_with_outliers": files_with_outliers,
        "passed": total_invalid == 0,
        "results": results,
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    # Summary
    print(f"Validated {len(csvs)} files, {total_rows:,} rows")
    print(f"Invalid: {total_invalid:,}  |  Outliers: {total_outlier:,}")
    print(f"Report: {report_path}")
    if files_with_invalid:
        print(f"Files with invalid rows: {len(files_with_invalid)}")
        for f in files_with_invalid[:10]:
            print(f"  - {f}")
        if len(files_with_invalid) > 10:
            print(f"  ... and {len(files_with_invalid) - 10} more")
    if files_with_outliers and total_invalid == 0:
        print(f"Files with outliers (>=300 POSTED/ACTUAL): {len(files_with_outliers)}")
        for f in files_with_outliers[:5]:
            print(f"  - {f}")
        if len(files_with_outliers) > 5:
            print(f"  ... and {len(files_with_outliers) - 5} more")

    sys.exit(0 if total_invalid == 0 else 1)


if __name__ == "__main__":
    main()
