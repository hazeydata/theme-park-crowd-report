#!/usr/bin/env python3
"""
Wait Time Database Report

================================================================================
PURPOSE
================================================================================
Produces an easily consumable Markdown report of what's in the wait time fact
table. Scans fact_tables/clean/YYYY-MM/{park}_{date}.csv (same layout as ETL
output) and summarizes:

  1. SUMMARY    — Date range, parks, park-day (file) count, total rows
  2. BY PARK    — Per-park: file count, row count, date range
  3. RECENT     — Grid: last N days × parks (✓/— or row counts)

Use for daily or ad-hoc checks. Report is overwritten each run so you can
always open the same file (e.g. reports/wait_time_db_report.md).

================================================================================
OUTPUT
================================================================================
  - Markdown report: reports/wait_time_db_report.md under output base
  - Override with --report. Use --output-base to match ETL output location.

================================================================================
MODES
================================================================================
  - Default: Count rows per file (slower on network/slow paths like Dropbox).
  - --quick: Skip row counts; recent grid shows ✓ (file exists) / — (no file)
    only. Faster for daily checks on large or remote output bases.

Usage:
  python scripts/report_wait_time_db.py
  python scripts/report_wait_time_db.py --quick --lookback-days 7
  python scripts/report_wait_time_db.py --output-base "D:\\Path" --report reports/db.md
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Allow importing from src (for get_output_base)
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))
from utils import get_output_base


# =============================================================================
# HELPERS: Parse park and date from filenames
# =============================================================================
# Fact table CSVs follow fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv.
# We extract (park, date) from the stem (e.g. mk_2026-01-24 -> ("mk", "2026-01-24")).

def _parse_park_date(path: Path) -> tuple[str | None, str | None]:
    """
    Extract (park, YYYY-MM-DD) from a park-day CSV path.
    Stem format: {park}_{YYYY-MM-DD}. Returns (None, None) if no match.
    """
    name = path.stem
    m = re.match(r"([a-z0-9]+)_(\d{4}-\d{2}-\d{2})$", name, re.I)
    if not m:
        return None, None
    return m.group(1).lower(), m.group(2)


# =============================================================================
# HELPERS: Row count (optional; skipped in --quick)
# =============================================================================
# Count data rows (excluding header) via buffered chunk read. Used for
# per-park and per-park-day stats and for the recent grid when not --quick.

def _row_count(path: Path, *, quick: bool = False) -> int:
    """
    Count data rows in a CSV (lines minus 1 for header).
    If quick=True, skip counting and return -1 (caller treats as "presence only").
    """
    if quick:
        return -1
    try:
        with open(path, "rb") as f:
            n = 0
            buf = bytearray(1 << 20)
            while True:
                got = f.readinto(buf)
                if not got:
                    break
                n += buf[:got].count(b"\n")
            return max(0, n - 1)
    except Exception:
        return -1


# =============================================================================
# BUILD SUMMARY: Scan all park-day CSVs
# =============================================================================
# Walks fact_tables/clean/, parses each CSV path, optionally counts rows.
# Returns aggregates (parks, dates, by_park, park_date_rows) for the report.

def _build_summary(clean_dir: Path, *, quick: bool = False) -> dict:
    """
    Scan clean_dir for park-day CSVs and build a summary dict.

    Returns:
      - files: list of (park, date_str, path)
      - parks: sorted list of park codes
      - dates: sorted list of YYYY-MM-DD strings
      - by_park: {park: {files, rows, dates}}
      - total_rows: sum of rows across all files
      - park_date_rows: {(park, date) -> row_count} for recent grid
      - quick: whether row counts were skipped
    """
    files: list[tuple[str, str, Path]] = []

    for p in sorted(clean_dir.rglob("*.csv")):
        if not p.is_file():
            continue
        park, date_str = _parse_park_date(p)
        if park and date_str:
            files.append((park, date_str, p))

    if not files:
        return {
            "files": [],
            "parks": set(),
            "dates": set(),
            "by_park": defaultdict(lambda: {"files": 0, "rows": 0, "dates": set()}),
            "total_rows": 0,
            "park_date_rows": {},
            "quick": quick,
        }

    parks = {f[0] for f in files}
    dates = {f[1] for f in files}
    by_park: dict[str, dict] = defaultdict(lambda: {"files": 0, "rows": 0, "dates": set()})
    park_date_rows: dict[tuple[str, str], int] = {}
    total_rows = 0

    for park, date_str, p in files:
        n = _row_count(p, quick=quick)
        r = max(0, n)
        by_park[park]["files"] += 1
        by_park[park]["rows"] += r
        by_park[park]["dates"].add(date_str)
        if r >= 0:
            park_date_rows[(park, date_str)] = r
        else:
            park_date_rows[(park, date_str)] = 0  # quick: presence only
        total_rows += r

    return {
        "files": files,
        "parks": sorted(parks),
        "dates": sorted(dates),
        "by_park": dict(by_park),
        "total_rows": total_rows,
        "park_date_rows": park_date_rows,
        "quick": quick,
    }


# =============================================================================
# RECENT GRID: Last N days × parks
# =============================================================================
# Builds a grid of (date, [(park, rows_or_None), ...]) for the last lookback_days
# (including today). Used for the "Recent coverage" section of the report.

def _recent_grid(
    summary: dict,
    lookback_days: int,
) -> list[tuple[str, list[tuple[str, int | None]]]]:
    """
    Build grid for last lookback_days (0 = today, 1 = yesterday, ...).

    Each row is (date_str, [(park, count_or_None), ...]) in summary["parks"] order.
    None means no file for that (park, date).
    """
    today = datetime.now().date()
    grid: list[tuple[str, list[tuple[str, int | None]]]] = []
    parks = summary["parks"]
    cache = summary.get("park_date_rows") or {}

    for d in range(lookback_days, -1, -1):
        dt = today - timedelta(days=d)
        date_str = dt.strftime("%Y-%m-%d")
        row: list[tuple[str, int | None]] = []
        for park in parks:
            r = cache.get((park, date_str))
            row.append((park, r if r is not None else None))
        grid.append((date_str, row))

    return grid


# =============================================================================
# WRITE MARKDOWN REPORT
# =============================================================================
# Emits the report sections: Summary, By park, Recent coverage. Uses ✓/— in
# quick mode for the grid; otherwise row counts.

def _write_md(
    out_path: Path,
    summary: dict,
    lookback_days: int,
    output_base: Path,
    generated_at: str,
    *,
    quick: bool = False,
) -> None:
    """Write the Markdown report to out_path."""
    lines: list[str] = []

    # ----- Header -----
    lines.append("# Wait Time Database Report")
    lines.append("")
    lines.append(f"**Generated:** {generated_at}  ")
    lines.append(f"**Output base:** `{output_base}`  ")
    lines.append("")

    if not summary["files"]:
        lines.append("No park-day CSVs found under `fact_tables/clean/`.")
        lines.append("")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    total_files = len(summary["files"])
    total_rows = summary["total_rows"]
    parks = summary["parks"]
    dates = summary["dates"]
    date_min, date_max = min(dates), max(dates)

    # ----- Section 1: Summary -----
    lines.append("## Summary")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| **Date range** | {date_min} → {date_max} |")
    lines.append(f"| **Parks** | {', '.join(parks)} |")
    lines.append(f"| **Park-days (files)** | {total_files:,} |")
    if quick:
        lines.append("| **Total rows** | — *(use without --quick for counts)* |")
    else:
        lines.append(f"| **Total rows** | {total_rows:,} |")
    lines.append("")

    # ----- Section 2: By park -----
    lines.append("## By park")
    lines.append("")
    if quick:
        lines.append("| Park | Files | Date range |")
        lines.append("|------|-------|------------|")
        for park in parks:
            info = summary["by_park"][park]
            dr = sorted(info["dates"])
            rng = f"{min(dr)} → {max(dr)}" if dr else "—"
            lines.append(f"| {park} | {info['files']:,} | {rng} |")
    else:
        lines.append("| Park | Files | Rows | Date range |")
        lines.append("|------|-------|------|------------|")
        for park in parks:
            info = summary["by_park"][park]
            dr = sorted(info["dates"])
            rng = f"{min(dr)} → {max(dr)}" if dr else "—"
            lines.append(f"| {park} | {info['files']:,} | {info['rows']:,} | {rng} |")
    lines.append("")

    # ----- Section 3: Recent coverage grid -----
    lines.append(f"## Recent coverage (last {lookback_days} days)")
    lines.append("")
    if quick:
        lines.append("✓ = file exists. — = no file.")
        lines.append("")
        grid = _recent_grid(summary, lookback_days)
        header = ["Date"] + parks
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for date_str, row in grid:
            cells = [date_str]
            for _, cnt in row:
                cells.append("✓" if cnt is not None else "—")
            lines.append("| " + " | ".join(cells) + " |")
    else:
        lines.append("Cell = row count for that park-day. — = no file.")
        lines.append("")
        grid = _recent_grid(summary, lookback_days)
        header = ["Date"] + parks
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(["---"] * len(header)) + "|")
        for date_str, row in grid:
            cells = [date_str]
            for _, cnt in row:
                cells.append(f"{cnt:,}" if cnt is not None and cnt >= 0 else "—")
            lines.append("| " + " | ".join(cells) + " |")
    lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("*Report from `scripts/report_wait_time_db.py`*")
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Produce a human-readable report of what's in the wait time fact table."
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base (from config/config.json or default)",
    )
    ap.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Write Markdown to this path (default: reports/wait_time_db_report.md)",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=14,
        help="Recent-coverage grid: last N days (default: 14)",
    )
    ap.add_argument(
        "--quick",
        action="store_true",
        help="Skip row counts; grid shows ✓/— only (faster on slow paths)",
    )
    args = ap.parse_args()

    # ----- Resolve paths -----
    base = args.output_base.resolve()
    clean_dir = base / "fact_tables" / "clean"
    if not clean_dir.is_dir():
        print(f"ERROR: {clean_dir} not found", file=sys.stderr)
        sys.exit(2)

    # ----- Build summary and write report -----
    summary = _build_summary(clean_dir, quick=args.quick)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report_dir = base / "reports"
    report_path = args.report.resolve() if args.report else report_dir / "wait_time_db_report.md"
    _write_md(report_path, summary, args.lookback_days, base, generated_at, quick=args.quick)

    # ----- Print brief summary to stdout -----
    print(f"Report: {report_path}")
    if summary["files"]:
        rows_msg = f"  |  Rows: {summary['total_rows']:,}" if not args.quick else ""
        print(f"Park-days: {len(summary['files']):,}{rows_msg}  |  Parks: {', '.join(summary['parks'])}")
        print(f"Date range: {min(summary['dates'])} -> {max(summary['dates'])}")
    else:
        print("No park-day CSVs found.")


if __name__ == "__main__":
    main()
