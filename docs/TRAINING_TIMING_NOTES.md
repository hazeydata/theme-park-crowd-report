# Training Timing Notes

## Normal per-entity timing

From batch runs, **typical** per-entity training times:

- **~7–10 minutes** for most entities (e.g. EP10 ~7 min, AK02 ~5 min)
- **Up to ~19 minutes** for some heavy entities (e.g. EP09 ~19 min, AK01 ~13 min)
- **Average** ~5–6 minutes across a mixed batch

If a single entity runs **longer than ~25–30 minutes**, something is likely wrong (bottleneck or hang).

## AK86 “over an hour” case (2026-01-27)

**Symptom:** AK86 (Flight of Passage) was still “processing” after more than an hour.

**Cause:** AK86 had **450,942 rows** (14,682 ACTUAL). The run got stuck in **“Adding features…”**, inside `add_park_hours()` in `src/processors/features.py`.

**Why it was slow (fixed):**

1. **Many unique (park_date, park_code):** With ~450k rows over many years, there can be **thousands** of distinct dates.
2. **Park-hours loop (fixed):** The code used to call `get_park_hours_for_date(...)` once per unique `(park_date, park_code)` in a Python loop. It now uses **`build_park_hours_lookup_table()`**: one vectorized pass over the versioned table and one merge. Park hours are per (park_date, park_code), not per entity, so a single lookup table covers all rows.
3. **No cap on rows:** There is still no automatic limit on rows when building features; very large entities still do full feature engineering, but add_park_hours is no longer the bottleneck.

**Recommendation for very large entities:**

- Use **`--sample N`** (e.g. `--sample 100000`) to cap rows before features and training, or  
- Use **`--skip-park-hours`** to avoid the park-hours loop and keep runtime manageable.

For **batch** runs, consider:

- A **`--max-rows`** (or similar) option that applies sampling when `len(df) > N`, or  
- Skipping park-hours (or using a flat park-hours table) for entities above a row threshold.

## What to do if an entity runs >30 minutes

1. **Check the entity’s own log**  
   e.g. `logs/train_entity_model_YYYYMMDD_HHMMSS.log`  
   Find the **last logged step** (e.g. “Adding features…”, “Encoding…”, “Training…”).

2. **If it’s “Adding features…”**  
   Likely stuck in `add_park_hours` (or similar) on a **very large** entity.  
   - Stop the run.  
   - Retry with `--sample 100000` or `--skip-park-hours` for that entity (or both for a quick test).

3. **If the batch script hits the 1-hour timeout**  
   The batch job will log `TIMEOUT (>1 hour)` for that entity and move on. You can then rerun that entity with sampling or `--skip-park-hours` as above.
