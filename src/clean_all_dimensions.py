#!/usr/bin/env python3
"""
Clean All Dimension Tables

Runs all dimension table cleaning scripts in sequence:
1. clean_dimentity.py
2. clean_dimparkhours.py
3. clean_dimeventdays.py
4. clean_dimevents.py

Usage:
    python src/clean_all_dimensions.py
    python src/clean_all_dimensions.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from utils import get_output_base

CLEANING_SCRIPTS = [
    "clean_dimentity.py",
    "clean_dimparkhours.py",
    "clean_dimeventdays.py",
    "clean_dimevents.py",
    "clean_dimmetatable.py",
]


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Clean all dimension tables"
    )
    ap.add_argument(
        "--output-base",
        type=Path,
        default=get_output_base(),
        help="Output base directory (from config/config.json or default)",
    )
    args = ap.parse_args()

    base = args.output_base.resolve()
    script_dir = Path(__file__).parent

    print("=" * 60)
    print("Clean All Dimension Tables")
    print("=" * 60)
    print(f"Output base: {base}")
    print()

    failed = []
    for script in CLEANING_SCRIPTS:
        script_path = script_dir / script
        if not script_path.exists():
            print(f"ERROR: Script not found: {script_path}")
            failed.append(script)
            continue

        print(f"Running {script}...")
        print("-" * 60)
        
        cmd = [
            sys.executable,
            str(script_path),
            "--output-base",
            str(base),
        ]
        
        result = subprocess.run(cmd, capture_output=False)
        
        if result.returncode != 0:
            print(f"ERROR: {script} failed with exit code {result.returncode}")
            failed.append(script)
        else:
            print(f"SUCCESS: {script} completed")
        print()

    if failed:
        print("=" * 60)
        print(f"FAILED: {len(failed)} script(s) failed")
        for script in failed:
            print(f"  - {script}")
        print("=" * 60)
        sys.exit(1)
    else:
        print("=" * 60)
        print("SUCCESS: All dimension tables cleaned")
        print("=" * 60)


if __name__ == "__main__":
    main()
