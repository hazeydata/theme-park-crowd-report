"""
Output base path: single source for pipeline output (Dropbox or config).

Reads config/config.json (output_base) when present; otherwise uses the
shared default. Used by ETL, dimension scripts, queue-times, and PS1
wrappers so all write to the same output_base (and thus one logs/).
"""

from __future__ import annotations

import json
from pathlib import Path

# Fallback when config/config.json is missing or has no output_base
_DEFAULT_OUTPUT_BASE = Path(
    "/home/fred/TouringPlans.com Dropbox/fred hazelton/stats team/pipeline/hazeydata/theme-park-crowd-report"
)


def _project_root() -> Path:
    """Project root (theme-park-crowd-report/)."""
    return Path(__file__).resolve().parent.parent.parent


def get_output_base() -> Path:
    """
    Return the pipeline output base directory.

    Uses config/config.json "output_base" when present and non-empty;
    otherwise returns the default Dropbox path. Enables one output_base
    for ETL, dimension fetch, queue-times, and reports.
    """
    root = _project_root()
    cfg_path = root / "config" / "config.json"
    if not cfg_path.exists():
        return _DEFAULT_OUTPUT_BASE
    try:
        with open(cfg_path, encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("output_base") if isinstance(data, dict) else None
        if raw and isinstance(raw, str) and raw.strip():
            return Path(raw.strip()).resolve()
    except (OSError, json.JSONDecodeError, TypeError):
        pass
    return _DEFAULT_OUTPUT_BASE
