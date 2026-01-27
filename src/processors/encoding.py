"""
Encoding Module

================================================================================
PURPOSE
================================================================================
Converts categorical features to numeric format for machine learning models.
Supports multiple encoding strategies: label encoding, one-hot encoding, and
target encoding (future).

Categorical features to encode:
  - pred_dategroupid: Date group ID (integer-like but categorical)
  - pred_season: Season name (e.g., "Spring", "Summer")
  - pred_season_year: Season year (integer-like but categorical)
  - park_code: Park code (e.g., "MK", "EP")
  - entity_code: Entity/attraction code (e.g., "MK101")

================================================================================
USAGE
================================================================================
  from processors.encoding import encode_features
  
  # Load features (from add_features)
  df_features = add_features(df, output_base)
  
  # Encode categorical features
  df_encoded, encoding_mappings = encode_features(
      df_features,
      output_base,
      strategy="label",  # or "one_hot"
  )
  
  # For inference (with saved mappings):
  df_encoded = encode_features(
      df_features,
      output_base,
      strategy="label",
      mappings=encoding_mappings,  # Use saved mappings
  )

================================================================================
ENCODING STRATEGIES
================================================================================
1. **Label Encoding** (default, recommended for tree-based models):
   - Maps each unique category to an integer (0, 1, 2, ...)
   - Preserves ordinal relationships if they exist
   - Efficient for XGBoost, LightGBM, etc.
   - Example: ["MK", "EP", "HS"] → [0, 1, 2]

2. **One-Hot Encoding**:
   - Creates binary columns for each category
   - Example: park_code "MK" → park_code_MK=1, park_code_EP=0, park_code_HS=0
   - Useful for linear models or when categories have no ordinal meaning
   - Can create many columns for high-cardinality features

3. **Target Encoding** (future):
   - Encodes categories based on target variable statistics
   - Example: Mean observed_wait_time per park_code
   - Requires target variable and can overfit if not regularized

================================================================================
MAPPINGS STORAGE
================================================================================
Encoding mappings are saved to `state/encoding_mappings.json` for reuse during
inference. This ensures consistent encoding between training and prediction.

Mappings structure:
  {
    "strategy": "label",
    "columns": {
      "park_code": {
        "MK": 0,
        "EP": 1,
        "HS": 2,
        ...
      },
      "entity_code": {
        "MK101": 0,
        "MK102": 1,
        ...
      },
      ...
    },
    "created_at": "2026-01-25T12:00:00Z"
  }
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd
from zoneinfo import ZoneInfo

from utils import get_output_base


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default categorical columns to encode
DEFAULT_CATEGORICAL_COLUMNS = [
    "pred_dategroupid",
    "pred_season",
    "pred_season_year",
    "park_code",
    "entity_code",
]

# Columns that should be treated as integers after encoding (for label encoding)
INTEGER_ENCODED_COLUMNS = [
    "pred_dategroupid",
    "pred_season_year",
]


# =============================================================================
# LABEL ENCODING
# =============================================================================

def _label_encode_column(
    series: pd.Series,
    mapping: Optional[Dict[str, int]] = None,
    handle_unknown: str = "error",
) -> tuple[pd.Series, Dict[str, int]]:
    """
    Label encode a categorical series.
    
    Args:
        series: Categorical series to encode
        mapping: Optional pre-existing mapping (for inference)
        handle_unknown: How to handle unknown values ("error", "ignore", or "encode")
    
    Returns:
        Tuple of (encoded_series, mapping_dict)
    """
    # Convert to string and handle nulls
    series_str = series.astype(str)
    series_str = series_str.replace("nan", None)
    series_str = series_str.replace("None", None)
    
    if mapping is None:
        # Training: create mapping from unique values
        unique_vals = series_str.dropna().unique()
        unique_vals = sorted([v for v in unique_vals if v is not None])
        mapping = {val: idx for idx, val in enumerate(unique_vals)}
    
    # Apply mapping
    def map_value(val):
        if pd.isna(val) or val is None:
            return None
        val_str = str(val)
        if val_str in mapping:
            return mapping[val_str]
        elif handle_unknown == "error":
            raise ValueError(f"Unknown value '{val_str}' not in mapping")
        elif handle_unknown == "ignore":
            return None
        else:  # encode (assign new ID)
            max_id = max(mapping.values()) if mapping else -1
            mapping[val_str] = max_id + 1
            return mapping[val_str]
    
    encoded = series_str.apply(map_value)
    return encoded, mapping


def _label_encode(
    df: pd.DataFrame,
    columns: list[str],
    mappings: Optional[Dict[str, Dict[str, int]]] = None,
    handle_unknown: str = "error",
) -> tuple[pd.DataFrame, Dict[str, Dict[str, int]]]:
    """
    Label encode multiple categorical columns.
    
    Args:
        df: DataFrame with categorical columns
        columns: List of column names to encode
        mappings: Optional pre-existing mappings (for inference)
        handle_unknown: How to handle unknown values
    
    Returns:
        Tuple of (encoded_df, mappings_dict)
    """
    df = df.copy()
    if mappings is None:
        mappings = {}
    
    new_mappings = {}
    
    for col in columns:
        if col not in df.columns:
            continue
        
        col_mapping = mappings.get(col)
        encoded, col_mapping = _label_encode_column(
            df[col],
            mapping=col_mapping,
            handle_unknown=handle_unknown,
        )
        
        # Replace column
        df[col] = encoded
        
        # Convert to integer if appropriate
        if col in INTEGER_ENCODED_COLUMNS:
            df[col] = df[col].astype("Int64")  # Nullable integer
        else:
            df[col] = df[col].astype("Int64")  # Use nullable integer for all
        
        new_mappings[col] = col_mapping
    
    return df, new_mappings


# =============================================================================
# ONE-HOT ENCODING
# =============================================================================

def _one_hot_encode(
    df: pd.DataFrame,
    columns: list[str],
    mappings: Optional[Dict[str, list[str]]] = None,
    handle_unknown: str = "ignore",
) -> tuple[pd.DataFrame, Dict[str, list[str]]]:
    """
    One-hot encode multiple categorical columns.
    
    Args:
        df: DataFrame with categorical columns
        columns: List of column names to encode
        mappings: Optional pre-existing category lists (for inference)
        handle_unknown: How to handle unknown values ("ignore" or "error")
    
    Returns:
        Tuple of (encoded_df, mappings_dict)
    """
    df = df.copy()
    if mappings is None:
        mappings = {}
    
    new_mappings = {}
    
    for col in columns:
        if col not in df.columns:
            continue
        
        # Get unique categories
        if col in mappings:
            categories = mappings[col]
        else:
            # Training: get unique categories
            unique_vals = df[col].dropna().unique()
            categories = sorted([str(v) for v in unique_vals if pd.notna(v)])
            new_mappings[col] = categories
        
        # Create one-hot columns
        for cat in categories:
            new_col = f"{col}_{cat}"
            df[new_col] = (df[col] == cat).astype(int)
        
        # Drop original column
        df = df.drop(columns=[col])
    
    return df, new_mappings


# =============================================================================
# MAIN ENCODING FUNCTION
# =============================================================================

def encode_features(
    df: pd.DataFrame,
    output_base: Path,
    strategy: str = "label",
    columns: Optional[list[str]] = None,
    mappings: Optional[Dict] = None,
    save_mappings: bool = True,
    handle_unknown: str = "error",
    logger: Optional[logging.Logger] = None,
) -> tuple[pd.DataFrame, Dict]:
    """
    Encode categorical features to numeric format.
    
    Args:
        df: DataFrame with features (from add_features)
        output_base: Pipeline output base directory
        strategy: Encoding strategy ("label" or "one_hot")
        columns: List of columns to encode (default: DEFAULT_CATEGORICAL_COLUMNS)
        mappings: Pre-existing mappings (for inference)
        save_mappings: If True, save mappings to state/encoding_mappings.json
        handle_unknown: How to handle unknown values ("error", "ignore", or "encode")
        logger: Optional logger
    
    Returns:
        Tuple of (encoded_df, mappings_dict)
    """
    if df.empty:
        return df, {}
    
    if columns is None:
        columns = DEFAULT_CATEGORICAL_COLUMNS.copy()
    
    # Filter to columns that exist in DataFrame
    available_columns = [col for col in columns if col in df.columns]
    missing_columns = [col for col in columns if col not in df.columns]
    
    if missing_columns and logger:
        logger.warning(f"Columns not found in DataFrame: {missing_columns}")
    
    if not available_columns:
        if logger:
            logger.warning("No categorical columns to encode")
        return df, {}
    
    # Load mappings if not provided
    if mappings is None:
        mappings = load_encoding_mappings(output_base, strategy=strategy, logger=logger)
    
    # Apply encoding strategy
    if strategy == "label":
        df_encoded, new_mappings = _label_encode(
            df,
            available_columns,
            mappings=mappings,
            handle_unknown=handle_unknown,
        )
    elif strategy == "one_hot":
        df_encoded, new_mappings = _one_hot_encode(
            df,
            available_columns,
            mappings=mappings,
            handle_unknown=handle_unknown,
        )
    else:
        raise ValueError(f"Unknown encoding strategy: {strategy}. Use 'label' or 'one_hot'")
    
    # Save mappings if requested
    if save_mappings and new_mappings:
        save_encoding_mappings(
            new_mappings,
            output_base,
            strategy=strategy,
            logger=logger,
        )
    
    if logger:
        logger.info(f"Encoded {len(available_columns)} columns using {strategy} encoding")
        if strategy == "one_hot":
            new_cols = [c for c in df_encoded.columns if c not in df.columns]
            logger.info(f"Created {len(new_cols)} one-hot columns")
    
    return df_encoded, new_mappings


# =============================================================================
# MAPPINGS STORAGE
# =============================================================================

def save_encoding_mappings(
    mappings: Dict,
    output_base: Path,
    strategy: str = "label",
    logger: Optional[logging.Logger] = None,
) -> None:
    """
    Save encoding mappings to JSON file.
    
    Args:
        mappings: Mappings dictionary
        output_base: Pipeline output base directory
        strategy: Encoding strategy used
        logger: Optional logger
    """
    state_dir = output_base / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    
    mappings_file = state_dir / "encoding_mappings.json"
    
    # Prepare data for JSON serialization
    data = {
        "strategy": strategy,
        "columns": mappings,
        "created_at": datetime.now(ZoneInfo("UTC")).isoformat(),
    }
    
    try:
        with open(mappings_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        if logger:
            logger.info(f"Saved encoding mappings to {mappings_file}")
    except Exception as e:
        if logger:
            logger.error(f"Failed to save encoding mappings: {e}")
        raise


def load_encoding_mappings(
    output_base: Path,
    strategy: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Dict:
    """
    Load encoding mappings from JSON file.
    
    Args:
        output_base: Pipeline output base directory
        strategy: Expected strategy (if None, uses whatever is in file)
        logger: Optional logger
    
    Returns:
        Mappings dictionary (empty if file not found)
    """
    state_dir = output_base / "state"
    mappings_file = state_dir / "encoding_mappings.json"
    
    if not mappings_file.exists():
        if logger:
            logger.debug("Encoding mappings file not found, starting fresh")
        return {}
    
    try:
        with open(mappings_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Check strategy if specified
        if strategy is not None and data.get("strategy") != strategy:
            if logger:
                logger.warning(
                    f"Strategy mismatch: file has '{data.get('strategy')}', "
                    f"expected '{strategy}'. Loading anyway."
                )
        
        mappings = data.get("columns", {})
        
        if logger:
            logger.info(f"Loaded encoding mappings from {mappings_file}")
            logger.debug(f"Strategy: {data.get('strategy')}, Columns: {list(mappings.keys())}")
        
        return mappings
    except Exception as e:
        if logger:
            logger.warning(f"Failed to load encoding mappings: {e}")
        return {}
