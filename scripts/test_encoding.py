#!/usr/bin/env python3
"""
Test Encoding Module

Quick test script to verify encoding functionality.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.encoding import encode_features, load_encoding_mappings
from processors.features import add_features
from utils import get_output_base


def main() -> None:
    """Test encoding with sample data."""
    output_base = get_output_base()
    
    # Create sample fact data
    df = pd.DataFrame({
        "entity_code": ["MK101", "MK101", "EP09", "HS01"],
        "observed_at": [
            "2026-01-25T10:00:00-05:00",
            "2026-01-25T11:00:00-05:00",
            "2026-01-25T12:00:00-05:00",
            "2026-01-25T13:00:00-05:00",
        ],
        "wait_time_type": ["POSTED", "ACTUAL", "POSTED", "ACTUAL"],
        "wait_time_minutes": [30, 45, 20, 15],
    })
    
    print("=" * 60)
    print("Original Data")
    print("=" * 60)
    print(df)
    print()
    
    # Add features
    print("=" * 60)
    print("Adding Features...")
    print("=" * 60)
    df_features = add_features(df, output_base, include_park_hours=False)
    
    print("\nFeatures DataFrame:")
    print(df_features[["entity_code", "park_code", "pred_dategroupid", "pred_season", "pred_season_year"]].head())
    print()
    
    # Test label encoding
    print("=" * 60)
    print("Label Encoding")
    print("=" * 60)
    df_label, mappings_label = encode_features(
        df_features,
        output_base,
        strategy="label",
        save_mappings=True,
    )
    
    print("\nEncoded DataFrame (label):")
    categorical_cols = ["pred_dategroupid", "pred_season", "pred_season_year", "park_code", "entity_code"]
    available_cols = [c for c in categorical_cols if c in df_label.columns]
    print(df_label[available_cols].head())
    print()
    
    print("Mappings:")
    for col, mapping in mappings_label.items():
        print(f"  {col}: {len(mapping)} categories")
        if len(mapping) <= 10:
            for key, val in list(mapping.items())[:5]:
                print(f"    {key} → {val}")
        else:
            print(f"    (showing first 5 of {len(mapping)})")
            for key, val in list(mapping.items())[:5]:
                print(f"    {key} → {val}")
    print()
    
    # Test loading mappings
    print("=" * 60)
    print("Loading Saved Mappings")
    print("=" * 60)
    loaded_mappings = load_encoding_mappings(output_base)
    print(f"Loaded {len(loaded_mappings)} column mappings")
    print()
    
    # Test one-hot encoding
    print("=" * 60)
    print("One-Hot Encoding")
    print("=" * 60)
    df_onehot, mappings_onehot = encode_features(
        df_features,
        output_base,
        strategy="one_hot",
        save_mappings=False,  # Don't overwrite label mappings
    )
    
    print("\nEncoded DataFrame (one-hot):")
    onehot_cols = [c for c in df_onehot.columns if any(c.startswith(col + "_") for col in categorical_cols)]
    if onehot_cols:
        print(df_onehot[onehot_cols].head())
    print()
    
    print(f"Original columns: {len(df_features.columns)}")
    print(f"One-hot columns: {len(df_onehot.columns)}")
    print(f"New columns created: {len(onehot_cols)}")
    print()
    
    print("=" * 60)
    print("Test Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
