#!/usr/bin/env python3
"""Quick script to check batch training status."""

import sys
from pathlib import Path

if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.paths import get_output_base

base = Path(get_output_base())
logs_dir = base / "logs"

# Find latest batch training log
log_files = sorted(logs_dir.glob("train_batch_entities_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)

if not log_files:
    print("No batch training logs found")
    sys.exit(0)

latest_log = log_files[0]
print(f"Latest log: {latest_log.name}")
print(f"Last updated: {Path(latest_log).stat().st_mtime}")
print()

# Read log content
with open(latest_log, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Count successes and failures
successes = [l for l in lines if "SUCCESS" in l]
failures = [l for l in lines if "FAILED" in l]

# Find current entity being trained
current_entity = None
for line in reversed(lines[-20:]):  # Check last 20 lines
    if "Training" in line and "[" in line:
        parts = line.split()
        for i, part in enumerate(parts):
            if "Training" in part and i + 1 < len(parts):
                current_entity = parts[i + 1].rstrip("...")
                break
        if current_entity:
            break

print(f"Completed: {len(successes)}")
print(f"Failed: {len(failures)}")
if current_entity:
    print(f"Currently training: {current_entity}")
else:
    # Check if batch is complete
    if "Batch Training Summary" in "".join(lines[-50:]):
        print("Status: COMPLETE")
        # Show summary
        summary_start = None
        for i, line in enumerate(lines):
            if "Batch Training Summary" in line:
                summary_start = i
                break
        if summary_start:
            print("\nSummary:")
            for line in lines[summary_start:summary_start+10]:
                print(line.rstrip())
    else:
        print("Status: Running or unknown")

# Show recent activity
print("\nRecent activity (last 5 lines):")
for line in lines[-5:]:
    print(line.rstrip())
