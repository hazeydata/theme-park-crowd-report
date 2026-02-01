# Pipeline Review & Recommendations for Bam-Bam

**Author:** Wilma (Pipeline Ops)  
**Date:** January 31, 2026  
**Branch:** `wilma/pipeline-ops` (proposed)

---

## Executive Summary

The pipeline's main bottleneck is **training time**. Each entity trains 2000 XGBoost trees on potentially hundreds of thousands of observations. With ~1700 entities and no parallelization, this can take 24+ hours.

**Key Findings:**
1. Training is sequential (one entity at a time)
2. 2000 trees per model with no early stopping
3. No parallelization infrastructure
4. Entities are re-trained whenever new data arrives (daily)

**Top Recommendations:**
1. Add early stopping (could cut tree count 50-80%)
2. Implement parallel training (multiprocessing)
3. Tiered training strategy (important entities daily, others weekly)
4. Incremental model updates instead of full retraining

---

## Current Architecture

### Pipeline Flow
```
ETL (S3 → fact_tables)
    ↓
Dimensions (entity, park_hours, events, etc.)
    ↓
Posted Aggregates
    ↓
Training (THE BOTTLENECK)
    ↓
Forecast
    ↓
WTI Calculation
```

### Training Flow (per entity)
```
train_batch_entities.py
    ↓ (for each entity, sequentially)
    subprocess → train_entity_model.py
        ↓
        load_entity_data()     # Load from CSVs
        ↓
        add_features()         # Feature engineering
        ↓
        encode_features()      # Label encoding
        ↓
        train_xgb_model()      # 2000 trees, no early stopping
        ↓
        save_model()           # JSON + metadata
```

### XGBoost Configuration
```python
DEFAULT_XGB_PARAMS = {
    "objective": "reg:absoluteerror",  # MAE loss
    "tree_method": "hist",              # Fast histogram method ✓
    "max_depth": 6,                     # Reasonable
    "learning_rate": 0.1,               # Standard
    "n_estimators": 2000,               # ← ISSUE: Always trains full 2000
    "subsample": 0.5,                   # Reasonable
    "min_child_weight": 10,             # Reasonable
}
EARLY_STOPPING_ROUNDS = None           # ← ISSUE: No early stopping!
```

---

## Identified Issues

### 🔴 Issue 1: No Early Stopping (CRITICAL)
**Impact:** Training 2x-5x longer than necessary

The pipeline always trains 2000 trees even if the model converged at tree 400. XGBoost's early stopping can detect when validation loss stops improving.

**Current Code (training.py:246):**
```python
EARLY_STOPPING_ROUNDS = None  # Julia uses watchlist=() so no early stop
```

**Recommendation:**
```python
EARLY_STOPPING_ROUNDS = 50  # Stop if val loss doesn't improve for 50 rounds
```

**Expected Impact:** 50-80% reduction in training time for most entities.

**Note:** The comment says "Julia legacy" — but if we're building a new Python pipeline, we can improve on legacy. The question for Fred: Is exact Julia parity required, or can we improve?

---

### 🔴 Issue 2: Sequential Training (CRITICAL)
**Impact:** Can't utilize multiple CPU cores

`train_batch_entities.py` trains entities one at a time via subprocess. A 32-core machine trains at 3% CPU utilization.

**Current Code (train_batch_entities.py:165):**
```python
for i, entity_code in enumerate(entities_to_train, 1):
    # ... train one entity ...
    success, message = train_single_entity(...)
```

**Recommendation:** Use `multiprocessing.Pool` or `concurrent.futures.ProcessPoolExecutor`:
```python
from concurrent.futures import ProcessPoolExecutor, as_completed

MAX_WORKERS = min(8, os.cpu_count())  # Don't overwhelm the system

with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {
        executor.submit(train_single_entity, entity, ...): entity
        for entity in entities_to_train
    }
    for future in as_completed(futures):
        entity = futures[future]
        success, message = future.result()
        # ... log result ...
```

**Expected Impact:** 
- 4-core machine: ~4x speedup
- 8-core machine: ~6-8x speedup (diminishing returns due to I/O)

**Considerations:**
- Memory: Each XGBoost process uses ~1-4GB RAM. Need `MAX_WORKERS` × 4GB available.
- Logging: Need thread-safe logging or separate log files per entity.
- Error handling: One failure shouldn't kill the pool.

---

### 🟡 Issue 3: Full Retrain on Any New Data (MEDIUM)
**Impact:** Entities with 1 new observation trigger full retrain

Any entity with `latest_observed_at > last_modeled_at` gets re-trained. This means:
- Entity with 100,000 observations gets 1 new → full 2000-tree retrain
- Daily: Most popular rides retrain daily

**Current Code (entity_index.py, get_entities_needing_modeling):**
```python
WHERE last_modeled_at IS NULL 
   OR latest_observed_at > last_modeled_at
```

**Recommendations:**

**Option A: Minimum New Data Threshold**
```python
WHERE last_modeled_at IS NULL 
   OR (latest_observed_at > last_modeled_at 
       AND new_observation_count >= 100)  # Only if 100+ new rows
```
Requires tracking `observation_count_at_last_model` in entity_index.

**Option B: Age-Based Retraining**
```python
# High-traffic entities: retrain if >24h old
# Low-traffic entities: retrain if >7 days old
```

**Option C: Incremental Learning**
XGBoost supports continuing training from a checkpoint:
```python
model = xgb.train(params, dtrain, 
                  xgb_model=existing_model,  # Continue from here
                  num_boost_round=200)       # Add 200 more trees
```
This could add new trees based on new data without retraining from scratch.

---

### 🟡 Issue 4: No Training Prioritization (MEDIUM)
**Impact:** Low-value entities trained before high-value ones

All entities are treated equally. If pipeline times out, important entities might not be trained.

**Recommendation:** Priority tiers:
```python
PRIORITY_TIERS = {
    "critical": ["MK", "EP", "HS", "AK"],  # WDW big 4
    "high": ["DL", "CA", "USH"],            # Other major parks
    "medium": ["UF", "IA", "EU"],           # Universal
    "low": ["TDL", "TDS", "BB", "TL"],      # Tokyo, water parks
}

# Sort entities by tier, then by actual_count (descending)
entities_to_train = sorted(
    entities_to_train,
    key=lambda e: (tier_order(e), -get_actual_count(e))
)
```

---

### 🟢 Issue 5: Subprocess Overhead (MINOR)
**Impact:** ~1-2 seconds per entity startup

Each entity spawns a new Python process, re-imports modules, reconnects to SQLite.

**Current:** `subprocess.run([python, train_entity_model.py, ...])`

**Recommendation:** For parallel implementation, use in-process training:
```python
# Instead of subprocess, call directly:
from scripts.train_entity_model import train_entity_main
train_entity_main(entity_code, output_base, ...)
```

This requires refactoring `train_entity_model.py` to be importable (it currently uses `main()` with `argparse`). Low priority if we're doing parallel anyway.

---

### 🟢 Issue 6: No Model Versioning (MINOR)
**Impact:** Can't roll back to previous model

Models are overwritten in place: `models/{entity}/model_with_posted.json`

**Recommendation:** Add versioning:
```
models/{entity}/
    model_with_posted_20260131_1530.json
    model_with_posted_20260130_0800.json  (previous)
    current_with_posted.json → symlink to latest
```

Or simpler: keep last N versions.

---

## Implementation Plan (APPROVED)

> **Fred's direction:** Go aggressive. Speed over legacy parity. Use RAM liberally.

### Phase 1: Quick Wins — DO NOW ⚡
**Goal:** 70%+ training time reduction

1. **Enable Early Stopping**
   - `EARLY_STOPPING_ROUNDS = 50` in training.py
   - Trees will converge at ~400 instead of 2000
   - **1 line change** — just do it
   
2. **Add Training Priority**
   - WDW parks (MK, EP, HS, AK) train first
   - Sort by importance + observation count
   - ~20 lines in train_batch_entities.py

### Phase 2: Aggressive Parallelization — NEXT 🚀
**Goal:** Max out available cores + RAM

1. **ProcessPoolExecutor with High Worker Count**
   ```python
   import psutil
   
   available_ram_gb = psutil.virtual_memory().available / (1024**3)
   ram_per_worker = 4  # GB estimate per XGBoost process
   max_by_ram = int(available_ram_gb * 0.8 / ram_per_worker)  # Use 80% of available
   max_by_cpu = os.cpu_count()
   
   MAX_WORKERS = min(max_by_ram, max_by_cpu, 16)  # Cap at 16
   ```
   
2. **Don't hold back on RAM** — Fred approved aggressive usage
   - Monitor with psutil, but start high
   - Better to use 80% RAM than 20% CPU

### Phase 3: Smart Retraining — LATER
- Minimum new data threshold (100+ new rows to trigger retrain)
- Tiered schedules (critical = daily, others = weekly)

---

## Dashboard Requirements (Separate Workstream)

Fred needs visibility into:

1. **Pipeline Status**
   - Current step (ETL/Training/Forecast/etc.)
   - Progress (entity 45/200)
   - Estimated completion time

2. **Model Quality**
   - MAE/RMSE per entity
   - Accuracy trends over time
   - Entities with degrading performance

3. **Wait Time Curves**
   - Actual vs Predicted for any entity
   - By time of day, day of week
   - Interactive date selection

4. **Data Freshness**
   - Latest observation per entity
   - Entities with stale data
   - Queue-times fetch status

**Location:** hazeydata.ai (private dashboard page)
**Tech:** Could extend existing `/dashboard` (Dash) or build new

---

## Fred's Answers ✅

| Question | Answer |
|----------|--------|
| **Julia Parity** | ❌ Not required — **improve on legacy** |
| **Accuracy vs Speed** | ✅ Accept small accuracy loss for big speed gain |
| **RAM** | ✅ Use as much as possible without affecting other processes |
| **Dashboard Priority** | Speed first, dashboard later |

**Bottom line:** Go aggressive on parallelization and early stopping. Don't hold back.

---

## Files to Modify

| File | Changes | Risk |
|------|---------|------|
| `src/processors/training.py` | Enable early stopping | Low |
| `scripts/train_batch_entities.py` | Priority sorting, parallelization | Medium |
| `src/processors/entity_index.py` | Track obs count at training | Low |
| `scripts/run_daily_pipeline.sh` | Add timing, better logging | Low |

---

## Next Steps for Bam-Bam

**Fred has approved everything. Go.**

### Immediate (Phase 1)
1. Open `src/processors/training.py`
2. Find `EARLY_STOPPING_ROUNDS = None`
3. Change to `EARLY_STOPPING_ROUNDS = 50`
4. Add priority sorting in `scripts/train_batch_entities.py`

### Then (Phase 2)  
5. Add `ProcessPoolExecutor` to `train_batch_entities.py`
6. Use `psutil` to detect available RAM
7. Set aggressive `MAX_WORKERS` (use 80% available RAM)

### Files to Touch
| File | Change |
|------|--------|
| `src/processors/training.py:246` | Early stopping = 50 |
| `scripts/train_batch_entities.py` | Priority sort + parallelization |

---

**Fred's exact words:** "Accept small accuracy sacrifice for big speed gain. Use as much RAM as you can. Focus on speed first."

*Let's make this pipeline sing.* 🦴
