#!/usr/bin/env python3
"""
Test Feature Engineering

Quick test script to verify features are added correctly.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo

# Import feature module
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.features import add_features
from utils import get_output_base


def main() -> None:
    # Create sample fact data
    now = datetime.now(ZoneInfo("UTC"))
    
    # Create sample data with timezone offsets (like real data)
    et = ZoneInfo("America/New_York")
    now_et = now.astimezone(et)
    
    df = pd.DataFrame({
        "entity_code": ["MK101", "MK101", "EP09"],
        "observed_at": [
            (now_et - timedelta(hours=2)).isoformat(),
            (now_et - timedelta(hours=1)).isoformat(),
            (now_et - timedelta(hours=3)).isoformat(),
        ],
        "wait_time_type": ["POSTED", "ACTUAL", "POSTED"],
        "wait_time_minutes": [30, 45, 20],
    })
    
    print("Original data:")
    print(df)
    print()
    
    # Add features
    output_base = get_output_base()
    df_features = add_features(df, output_base)
    
    print("With features:")
    print(df_features[["entity_code", "observed_at", "park_date", "park_code", 
                       "pred_mins_since_6am", "pred_dategroupid", "pred_season", 
                       "pred_season_year", "wgt_geo_decay", "observed_wait_time"]])
    print()
    
    print("Feature columns added:")
    feature_cols = [c for c in df_features.columns if c.startswith("pred_") or c in ["wgt_geo_decay", "observed_wait_time", "park_date", "park_code"]]
    for col in feature_cols:
        print(f"  {col}: {df_features[col].dtype}")


if __name__ == "__main__":
    main()
