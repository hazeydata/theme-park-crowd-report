#!/usr/bin/env python3
"""Update pipeline_status.json from shell or train_batch_entities. Used by run_daily_pipeline.sh and train_batch_entities.py."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.paths import get_output_base
from utils.pipeline_status import (
    load,
    pipeline_start,
    save,
    step_done,
    step_failed,
    training_set_current,
    training_set_entities,
)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-base", type=Path, default=None, help="Output base (default: config)")
    sub = ap.add_subparsers(dest="action", required=True)

    # Pipeline start
    sub.add_parser("pipeline-start")

    # Step done/failed
    p_step = sub.add_parser("step")
    p_step.add_argument("step", choices=["etl", "dimensions", "aggregates", "report", "training", "forecast", "wti"])
    p_step.add_argument("status", choices=["done", "failed"])

    # Training: set entity list (JSON array of {code, name})
    p_entities = sub.add_parser("training-entities")
    p_entities.add_argument("entities_json", help='JSON array e.g. [{"code":"EP09","name":"Soarin\'"}]')

    # Training: set current entity
    p_cur = sub.add_parser("training-current")
    p_cur.add_argument("index", type=int)
    p_cur.add_argument("entity_code", type=str)
    p_cur.add_argument("status", choices=["running", "done", "failed"])

    args = ap.parse_args()
    base = args.output_base or get_output_base()
    base = base.resolve() if hasattr(base, "resolve") else Path(base).resolve()

    if args.action == "pipeline-start":
        pipeline_start(base)
    elif args.action == "step":
        if args.status == "failed":
            step_failed(base, args.step)
        else:
            step_done(base, args.step)
    elif args.action == "training-entities":
        training_set_entities(base, json.loads(args.entities_json))
    elif args.action == "training-current":
        training_set_current(base, args.index, args.entity_code, args.status)
    else:
        ap.error("missing action")


if __name__ == "__main__":
    main()
