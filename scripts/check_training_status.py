#!/usr/bin/env python3
"""Quick script to check training status."""

import sys
from pathlib import Path

if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from processors.entity_index import get_all_entities, get_entities_needing_modeling
from utils.paths import get_output_base

base = Path(get_output_base())
index_db = base / "state" / "entity_index.sqlite"

# Get all entities
all_entities = get_all_entities(index_db)
print(f"Total entities in index: {len(all_entities)}")

# Get entities needing training
entities_needing = get_entities_needing_modeling(index_db, min_age_hours=0.0)
print(f"\nEntities needing training: {len(entities_needing)}")

# Count already trained
trained_count = len(all_entities) - len(entities_needing)
print(f"Already trained: {trained_count}")

# Check models directory
models_dir = base / "models"
if models_dir.exists():
    model_dirs = [d for d in models_dir.iterdir() if d.is_dir()]
    print(f"\nModels directory contains: {len(model_dirs)} entity model folders")

print("\nSample entities needing training:")
for i, (entity, latest_obs, last_modeled) in enumerate(entities_needing[:10]):
    print(f"  {entity}: latest={latest_obs}, last_modeled={last_modeled or 'Never'}")
