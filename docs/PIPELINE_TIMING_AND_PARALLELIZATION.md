# Pipeline timing and parallelization

Why the daily pipeline can exceed 24 hours and how to speed it up.

---

## 1. Where time is spent

| Step | Typical duration | Bottleneck |
|------|------------------|------------|
| **1. ETL** (incremental) | Minutes to tens of minutes | S3 reads, parsing, dedupe |
| **2. Dimension fetches** | Minutes | S3 + local builds |
| **3. Posted aggregates** | Tens of minutes | Scanning fact tables, grouping |
| **4. Wait time DB report** | Minutes | Scanning fact tables |
| **5. Batch training** | **~30+ hours** (sequential) | **One entity at a time** |
| **6. Forecast** | Tens of minutes to hours | One entity-date at a time |
| **7. WTI** | Minutes | Aggregation over curves |

**Training dominates.** With ~148 entities and ~12–13 minutes per entity (XGBoost + feature prep), sequential training alone is **148 × 13 min ≈ 32 hours**. That alone exceeds 24 hours.

---

## 2. Why training is slow

- **train_batch_entities.py** runs **one entity at a time**: for each entity it calls `train_entity_model.py` as a subprocess and waits for it to finish.
- **train_entity_model.py** per entity:
  - Loads entity data from fact tables
  - Builds features (park hours, encoding, etc.)
  - Trains up to two XGBoost models (with-POSTED, without-POSTED)
  - Saves models and metadata
- Each entity writes to its **own** directory (`models/{entity_code}/`), so there is **no file conflict** between entities. Training is **embarrassingly parallel** over entities.

---

## 3. Parallelization strategy

### 3.1 Training: `--workers N`

**train_batch_entities.py** supports **`--workers N`** (default 1). With N > 1, N entities are trained **in parallel** in separate processes (each running `train_entity_model.py`).

- **Recommendation:** Set `--workers` to the number of CPU cores you can dedicate (e.g. 4–8). More workers = more memory (each worker loads its own data and trains XGBoost).
- **Rough speedup:** With 4 workers, training time drops from ~32 h to ~8 h. With 8 workers, ~4 h.
- **Daily pipeline:** Use the same value in cron, e.g.  
  `train_batch_entities.py --min-age-hours 24 --workers 4`

### 3.2 Forecast: `--workers N` (optional)

**generate_forecast.py** can support **`--workers N`** to process entity-date pairs in parallel. Forecast is usually much faster than training (seconds per entity-date), but with many entities × many dates it can add up. Parallel forecast is optional and can be added if needed.

### 3.3 Other steps

- **ETL:** Already I/O bound; parallelizing within the script would require careful locking (dedupe DB, CSV appends). Not changed for now.
- **Posted aggregates:** Single pass over fact tables; could be parallelized by partition if needed.
- **WTI:** Aggregation over precomputed curves; typically fast.

---

## 4. Using parallel training

**Manual run:**
```bash
python scripts/train_batch_entities.py --min-age-hours 24 --workers 4
```

**Daily pipeline (run_daily_pipeline.sh):**  
The master script invokes `train_batch_entities.py --min-age-hours 24`. To use 4 workers, either:

- Add a `--workers` option to **run_daily_pipeline.sh** and pass it through to `train_batch_entities.py`, or  
- Edit the line in **run_daily_pipeline.sh** to:
  `train_batch_entities.py --min-age-hours 24 --workers 4`

**Dashboard:** With parallel training, "current entity" may show the last completed or a representative in-progress entity; multiple entities run at once.

---

## 5. Memory and CPU

- Each training worker loads one entity’s data and trains 1–2 XGBoost models. Plan for **~1–2 GB RAM per worker** (depends on entity data size).
- XGBoost uses multiple threads per process by default. If you run N workers, you may want to limit XGBoost threads (e.g. `nthread=2`) so total CPU usage stays reasonable. See **docs/XGBOOST_PARAMS.md** if you tune this.

---

## 6. Summary

| Change | Effect |
|--------|--------|
| **train_batch_entities.py --workers 4** | Training ~32 h → ~8 h (rough) |
| **train_batch_entities.py --workers 8** | Training ~32 h → ~4 h (rough) |
| **generate_forecast.py --workers N** (optional) | Shorten forecast step if it’s slow |

With **--workers 4** or **--workers 8**, the full daily pipeline (ETL → dimensions → aggregates → report → training → forecast → WTI) can complete **within 24 hours** so the next 6 AM run is not blocked by the previous day’s run still training.
