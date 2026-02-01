"""
Pipeline status file for dashboard and monitoring.

Writes/reads output_base/state/pipeline_status.json with:
  - pipeline: started_at, current_step, steps (etl, dimensions, aggregates, report, training, forecast, wti)
  - training: entities [{ code, name, status: pending|running|done|failed }], current_index, total
  - queue_times: (dashboard fills via pgrep; optional last_updated if we add heartbeat)
  - last_updated: ISO8601
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

try:
    import fcntl
except ImportError:
    fcntl = None  # Windows

STEP_ORDER = (
    "etl",
    "dimensions",
    "aggregates",
    "report",
    "training",
    "forecast",
    "wti",
)


def _status_path(output_base: Path) -> Path:
    return output_base / "state" / "pipeline_status.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load(output_base: Path) -> dict[str, Any]:
    """Load pipeline status from state/pipeline_status.json. Returns empty dict if missing."""
    path = _status_path(output_base)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save(output_base: Path, data: dict[str, Any]) -> None:
    """Write pipeline status to state/pipeline_status.json."""
    path = _status_path(output_base)
    path.parent.mkdir(parents=True, exist_ok=True)
    data["last_updated"] = _now()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _load_and_save(output_base: Path, update_fn: Callable[[dict], None]) -> None:
    """Load status, call update_fn(data), save. Uses file lock for safe concurrent updates."""
    path = _status_path(output_base)
    lock_path = path.parent / ".pipeline_status.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = None
    try:
        lock_file = open(lock_path, "w")
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
    except (OSError, AttributeError):
        pass
    try:
        data = load(output_base)
        update_fn(data)
        data["last_updated"] = _now()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    finally:
        if lock_file is not None and fcntl is not None:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            lock_file.close()


def _merge(data: dict, updates: dict) -> None:
    """Recursively merge updates into data (in place)."""
    for k, v in updates.items():
        if k in data and isinstance(data[k], dict) and isinstance(v, dict):
            _merge(data[k], v)
        else:
            data[k] = v


def pipeline_start(output_base: Path) -> None:
    """Mark pipeline run started; current_step = etl, all steps pending."""
    steps = {s: {"status": "pending"} for s in STEP_ORDER}
    save(output_base, {
        "pipeline": {
            "started_at": _now(),
            "current_step": "etl",
            "steps": steps,
        },
        "training": {"entities": [], "current_index": 0, "total": 0},
    })


def step_done(output_base: Path, step_name: str) -> None:
    """Mark step as done and advance current_step to next."""
    step_name = step_name.lower().replace(" ", "_").replace("(incremental)", "").strip("_")
    data = load(output_base)
    data.setdefault("pipeline", {})
    data["pipeline"].setdefault("steps", {})
    data["pipeline"]["steps"][step_name] = {"status": "done", "done_at": _now()}
    idx = STEP_ORDER.index(step_name) if step_name in STEP_ORDER else -1
    if idx >= 0 and idx + 1 < len(STEP_ORDER):
        data["pipeline"]["current_step"] = STEP_ORDER[idx + 1]
    save(output_base, data)


def step_failed(output_base: Path, step_name: str) -> None:
    """Mark step as failed."""
    step_name = step_name.lower().replace(" ", "_").replace("(incremental)", "").strip("_")
    data = load(output_base)
    data.setdefault("pipeline", {})
    data["pipeline"].setdefault("steps", {})
    data["pipeline"]["steps"][step_name] = {"status": "failed", "failed_at": _now()}
    data["pipeline"]["current_step"] = step_name
    save(output_base, data)


def training_set_entities(output_base: Path, entities: list[dict]) -> None:
    """Set training entity list. Each item: { code, name, status: "pending" }."""
    data = load(output_base)
    data.setdefault("training", {})
    data["training"]["entities"] = [
        {"code": e["code"], "name": e.get("name", e["code"]), "status": "pending"}
        for e in entities
    ]
    data["training"]["total"] = len(entities)
    data["training"]["current_index"] = 0
    data["training"]["current_entity"] = None
    data["training"].pop("workers", None)  # clear when starting new entity list
    save(output_base, data)


def training_set_workers(output_base: Path, workers: int) -> None:
    """Set number of parallel training workers (for dashboard)."""
    _load_and_save(output_base, lambda data: data.setdefault("training", {}).__setitem__("workers", workers))


def training_set_entity_status(output_base: Path, entity_code: str, status: str) -> None:
    """Set a single entity's status (running|done|failed). Safe for concurrent calls from parallel workers."""
    def update(data: dict) -> None:
        data.setdefault("training", {})
        for e in data["training"].get("entities", []):
            if e.get("code") == entity_code:
                e["status"] = status
                break
    _load_and_save(output_base, update)


def training_set_current(
    output_base: Path,
    index: int,
    entity_code: str,
    status: str,
) -> None:
    """Set current training entity and that entity's status (running|done|failed)."""
    data = load(output_base)
    data.setdefault("training", {})
    data["training"]["current_index"] = index
    data["training"]["current_entity"] = entity_code
    entities = data["training"].get("entities", [])
    for e in entities:
        if e.get("code") == entity_code:
            e["status"] = status
            break
    save(output_base, data)
