# Documentation Review Summary

Complete review of documentation to identify key decisions and requirements before proceeding with implementation.

---

## 1. Time Resolution: 5-Minute Intervals

### Decision
**All predictions, curves, and WTI use 5-minute time slots.**

### Documentation References
- **MODELING_AND_WTI_METHODOLOGY.md** (line 86):
  > "Time resolution: Use **5‑minute** slots for daily curves and WTI aggregation. We collect every 5 min from queue-times, so 5 min is tenable and matches collection; we can always downsample to 10 or 15 for specific consumers. Legacy used 15 min; 5 min gives finer resolution."

- **MODELING_AND_WTI_METHODOLOGY.md** (line 162):
  > "For each (entity, park_date) in the past, **5‑min time_slots**"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 174):
  > "For each (entity, park_date) from tomorrow to +2 years, **5‑min time_slots**"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 184):
  > "For each (park, park_date), **5‑min slots** in operating window"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 238):
  > "Backfill job: Loop (entity, park_date) in historical range, **5‑min time_slots**"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 239):
  > "Forecast job: Loop (entity, park_date) from tomorrow to +2 years, **5‑min time_slots**"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 240):
  > "WTI job: For each (park, park_date), **5‑min slots** in operating window"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 321):
  > "Time slot for curves and WTI: **5 minutes.** Matches queue-times collection; finer than legacy's 15 min."

### Implementation Impact
- **Posted Aggregates**: Currently aggregates by (entity, dategroupid, hour) → median POSTED. This is correct for building aggregates, but when generating predictions, we need to:
  1. Generate 5-minute time slots for each hour the park is open
  2. Use the hourly aggregate for all 5-minute slots within that hour
  3. Store predictions at 5-minute resolution

- **Forecast Generation**: Must create predictions for every 5-minute interval the park is open, not just hourly.

---

## 2. Julia Usage

### Documentation References
- **ATTRACTION_IO_ALIGNMENT.md**: References Julia in the context of the legacy attraction-io pipeline, which uses Julia for modeling. Mentions compatibility/alignment but does not explicitly state "use Julia when advantageous."

- **LEGACY_PIPELINE_CRITICAL_REVIEW.md**: Reviews the Julia-based legacy pipeline but focuses on improvements in Python.

### Finding
**No explicit guidance found** about "use Julia when advantageous." The documentation mentions Julia primarily in the context of:
- Understanding the legacy system (attraction-io)
- Compatibility/alignment considerations
- Option to feed data to Julia pipeline if desired

### Recommendation
If Julia usage is desired for performance-critical operations, this should be explicitly documented. Current implementation is Python-based.

---

## 3. Predicted POSTED Requirements

### Purpose
1. **Live comparison**: "We predicted POSTED = X, we observe POSTED = Y"
2. **Building trust**: Show accuracy of predictions in real-time
3. **Live streaming content**: Watch predictions perform in real-time

### Documentation References
- **MODELING_AND_WTI_METHODOLOGY.md** (line 70):
  > "Predicted POSTED: Build from historical aggregates: (entity, dategroupid, hour) → median or mean POSTED. Publish **predicted POSTED** for each (entity, park_date, time_slot) in the forecast."

- **MODELING_AND_WTI_METHODOLOGY.md** (line 175):
  > "Predicted POSTED: from aggregates (entity, dategroupid, hour) → median POSTED"

- **MODELING_AND_WTI_METHODOLOGY.md** (line 235):
  > "POSTED aggregate job (required): From fact: (entity_code, dategroupid, hour) → median POSTED. Produces predicted POSTED for forecast (live comparison and trust)."

### Implementation Status
✅ **Posted aggregates module created** - aggregates by (entity, dategroupid, hour)
❌ **Missing**: 5-minute interval generation for forecast predictions

---

## 4. Forecast Requirements

### What Forecast Must Generate
For each (entity, park_date) from tomorrow to +2 years, **5-minute time slots**:
1. **Predicted POSTED**: From aggregates (entity, dategroupid, hour) → median POSTED
2. **Predicted ACTUAL**: Features from dims only; **without-POSTED** model

### Output Format
- `curves/forecast/` with columns: (entity, park_date, time_slot, actual_predicted, posted_predicted)
- `time_slot` should be 5-minute intervals (e.g., "10:00", "10:05", "10:10", ...)

### Documentation References
- **MODELING_AND_WTI_METHODOLOGY.md** (line 174-178):
  > "STAGE E: Forecast (future ACTUAL and POSTED curves)
  > - For each (entity, park_date) from tomorrow to +2 years, **5‑min time_slots**:
  >   - **Predicted POSTED:** from aggregates (entity, dategroupid, hour) → median POSTED
  >   - **Predicted ACTUAL:** features from dims only; **without-POSTED** model
  > - Output: curves/forecast/ (entity, park_date, time_slot, actual_predicted, posted_predicted)"

---

## 5. Key Implementation Gaps

### Current Issues
1. ❌ **Posted aggregates only by hour**: Need to generate 5-minute predictions from hourly aggregates
2. ❌ **No forecast script**: Need to create forecast generation that:
   - Generates 5-minute time slots for park operating hours
   - Gets predicted POSTED for each slot (from hourly aggregates)
   - Gets predicted ACTUAL for each slot (from without-POSTED model)
   - Writes to `curves/forecast/`

### Required Fixes
1. **Update posted_aggregates.py**: Add function to generate 5-minute predictions from hourly aggregates
2. **Create forecast script**: Generate both predicted ACTUAL and predicted POSTED at 5-minute resolution
3. **Time slot generation**: Helper to create 5-minute intervals for park operating hours

---

## 6. Other Key Decisions from Documentation

### WTI (Wait Time Index)
- **Time resolution**: 5 minutes
- **Entity set**: All attractions (no core list)
- **Null handling**: Exclude (entity, time_slot) where ACTUAL is null (closed)
- **Calculation**: Mean ACTUAL over (entity, time_slot) where actual is not null

### Models
- **Two models**: With-POSTED (backfill, live) and without-POSTED (forecast)
- **Algorithm**: XGBoost (gradient boosted trees)
- **Objective**: reg:squarederror (mean squared error)

### Data Architecture
- **Fact tables**: `fact_tables/clean/YYYY-MM/{park}_{YYYY-MM-DD}.csv`
- **Dimensions**: `dimension_tables/*.csv`
- **Models**: `models/{entity_code}/model_with_posted.json`, `model_without_posted.json`
- **Forecast curves**: `curves/forecast/`
- **WTI**: `wti/wti.parquet`

---

## 7. Next Steps

1. **Fix posted aggregates**: Add 5-minute interval generation
2. **Create forecast script**: Generate predictions at 5-minute resolution
3. **Document Julia usage** (if applicable): Add explicit guidance on when to use Julia vs Python
4. **Test forecast generation**: Verify 5-minute intervals are generated correctly
