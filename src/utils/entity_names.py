"""
Utility functions for entity name lookups.

Provides helper functions to get entity short names from dimentity.csv
for improved logging readability.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from utils.paths import get_output_base

# Cache for entity names to avoid repeated file reads
_entity_names_cache: Optional[dict[str, str]] = None


def get_entity_short_name(
    entity_code: str,
    output_base: Optional[Path] = None,
    use_cache: bool = True,
) -> Optional[str]:
    """
    Get the short name for an entity code from dimentity.csv.
    
    Args:
        entity_code: Entity code (e.g., "AK03", "MK101")
        output_base: Optional output base directory (defaults to get_output_base())
        use_cache: Whether to use cached entity names (default: True)
    
    Returns:
        Short name if found, None otherwise
    """
    global _entity_names_cache
    
    if output_base is None:
        output_base = get_output_base()
    
    # Load cache if not already loaded
    if not use_cache or _entity_names_cache is None:
        dimentity_path = output_base / "dimension_tables" / "dimentity.csv"
        
        if not dimentity_path.exists():
            return None
        
        try:
            df = pd.read_csv(dimentity_path, low_memory=False)
            
            # Handle different column name variations
            code_col = None
            for col in ["entity_code", "code", "attraction_code"]:
                if col in df.columns:
                    code_col = col
                    break
            
            short_name_col = None
            for col in ["short_name", "name", "entity_name"]:
                if col in df.columns:
                    short_name_col = col
                    break
            
            if code_col and short_name_col:
                # Create lookup dict
                _entity_names_cache = dict(
                    zip(df[code_col].astype(str).str.upper(), df[short_name_col].astype(str))
                )
            else:
                _entity_names_cache = {}
        except Exception:
            _entity_names_cache = {}
    
    if _entity_names_cache is None:
        return None
    
    # Look up entity code (case-insensitive)
    return _entity_names_cache.get(entity_code.upper())


def format_entity_display(
    entity_code: str,
    output_base: Optional[Path] = None,
    use_cache: bool = True,
) -> str:
    """
    Format entity code with short name for display.
    
    Returns: "ENTITY_CODE - Short Name" if short name found, else just "ENTITY_CODE"
    
    Args:
        entity_code: Entity code
        output_base: Optional output base directory
        use_cache: Whether to use cached entity names
    
    Returns:
        Formatted string like "AK03 - Greeting Trails" or "AK03"
    """
    short_name = get_entity_short_name(entity_code, output_base, use_cache)
    
    if short_name:
        return f"{entity_code} - {short_name}"
    else:
        return entity_code


def get_entity_property(
    entity_code: str,
    property_name: str,
    output_base: Optional[Path] = None,
) -> Optional[any]:
    """
    Get a property value for an entity from dimentity.csv.
    
    Args:
        entity_code: Entity code (e.g., "AK03", "MK101")
        property_name: Property name (e.g., "fastpass_booth", "short_name")
        output_base: Optional output base directory (defaults to get_output_base())
    
    Returns:
        Property value if found, None otherwise
    """
    if output_base is None:
        output_base = get_output_base()
    
    dimentity_path = output_base / "dimension_tables" / "dimentity.csv"
    
    if not dimentity_path.exists():
        return None
    
    try:
        df = pd.read_csv(dimentity_path, low_memory=False)
        
        # Handle different column name variations for entity_code
        code_col = None
        for col in ["entity_code", "code", "attraction_code"]:
            if col in df.columns:
                code_col = col
                break
        
        if not code_col:
            return None
        
        if property_name not in df.columns:
            return None
        
        # Find entity row (case-insensitive match)
        entity_upper = entity_code.upper()
        entity_row = df[df[code_col].astype(str).str.upper().str.strip() == entity_upper]
        
        if entity_row.empty:
            return None
        
        value = entity_row.iloc[0][property_name]
        
        # Handle NaN/None
        if pd.isna(value):
            return None
        
        return value
        
    except Exception:
        return None


def is_priority_queue(
    entity_code: str,
    output_base: Optional[Path] = None,
) -> bool:
    """
    Check if an entity is a PRIORITY queue (fastpass_booth = TRUE).
    
    Args:
        entity_code: Entity code
        output_base: Optional output base directory
    
    Returns:
        True if priority queue, False if standby queue (or if cannot determine)
    """
    fastpass_booth = get_entity_property(entity_code, "fastpass_booth", output_base)
    
    # Handle various boolean representations
    if fastpass_booth is None:
        return False  # Default to standby if unknown
    
    # Convert to bool if needed
    # pandas may return numpy.bool_ or Python bool
    import numpy as np
    if isinstance(fastpass_booth, (bool, np.bool_)):
        return bool(fastpass_booth)
    if isinstance(fastpass_booth, str):
        return fastpass_booth.lower() in ["true", "1", "yes", "t"]
    if isinstance(fastpass_booth, (int, float)):
        return bool(fastpass_booth)
    
    # Try direct bool conversion as fallback
    try:
        return bool(fastpass_booth)
    except (ValueError, TypeError):
        return False


def clear_entity_names_cache() -> None:
    """Clear the entity names cache (useful for testing or after dimentity updates)."""
    global _entity_names_cache
    _entity_names_cache = None
