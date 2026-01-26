# Modeling and Wait Time Index (WTI): Methodology and Roadmap

Critical review of the proposed ACTUAL-curve and WTI methodology, plus a detailed map of data architecture, engineering, and modeling to achieve it. References: [SCHEMA.md](SCHEMA.md), [ATTRACTION_IO_ALIGNMENT.md](ATTRACTION_IO_ALIGNMENT.md), [LEGACY_PIPELINE_CRITICAL_REVIEW.md](LEGACY_PIPELINE_CRITICAL_REVIEW.md).

---

## 1. Stated Purposes

1. **Historical ACTUAL curves** — For every attraction, park, and day in the past: compute the full ACTUAL wait-time curve by modeling the relationship between POSTED (and possibly PRIORITY) and ACTUAL, using features such as minutes since 6am, minutes since park open, date_group_id, season, season_year, and others.

2. **Future ACTUAL curves** — Use the same (or sibling) modeling to predict ACTUAL for future park-days from tomorrow through tomorrow + 2 years. We also produce **predicted POSTED** for those same (entity, park_date, time_slot) so that in live settings we can **compare predicted POSTED to observed POSTED** — building trust and supporting live content (“we predicted X; we’re seeing Y”).

3. **Live ACTUAL prediction** — When we receive a POSTED value from any source, predict ACTUAL with very low latency. Improve over time using TouringPlans’ observed ACTUALS.

4. **Wait Time Index (WTI)** — From stable observed and predicted **ACTUAL** curves only: WTI = average ACTUAL across **all attractions** in a park for a park-day, **excluding only (entity, time_slot) where ACTUAL is null** (e.g. closed). Predicted POSTED is **not** used in WTI. Replaces 1–10 crowd calendar; comparable over time.

**Latency goal:** Apps, dashboards, and APIs should react quickly to an observed wait time: either retrieve a precomputed ACTUAL (observed or predicted) or compute it from POSTED with minimal delay.

---

## 2. Critical Review

### 2.1 Strengths

| Aspect | Why it helps |
|--------|--------------|
| **Unified target** | One target (ACTUAL) across backfill, forecast, and live keeps definitions consistent and simplifies WTI. |
| **POSTED as main covariate** | POSTED is the most direct operational signal; modeling POSTED→ACTUAL is well-motivated. |
| **Rich feature set** | mins_since_6am, mins_since_park_open, dategroupid, season, season_year capture time-of-day, calendar, and demand. Park hours and 6am rule are already in our pipeline. |
| **WTI as a single metric** | Average ACTUAL is intuitive and comparable across days and years without 1–10 thresholds or moving scales. |
| **XGBoost** | Handles mixed types, non-linearity, and missing values; fast to train and to run at inference; widely used and interpretable via SHAP. |
| **Leverage TouringPlans ACTUALS** | TP’s observed ACTUALS are the main source of ground truth; continuous use for training and evaluation is essential. |

### 2.2 Challenges and Proposed Improvements

#### A. POSTED → ACTUAL is stochastic and context-dependent

**Issue:** The same POSTED can correspond to different ACTUALS depending on throughput, merge points, breakdowns, and operations. A single deterministic mapping is too rigid.

**Improvements:**
- Treat **POSTED as a primary covariate** plus **interactions**: e.g. `POSTED × mins_since_park_open`, `POSTED × dategroupid`, `POSTED × is_peak_hour`. Let the model learn context-dependent elasticity.
- Add **rolling POSTED summaries** (e.g. mean POSTED in last 30 or 60 minutes for that entity) to capture ramping or cooling.
- Consider **quantile regression** (e.g. 10th, 50th, 90th percentiles of ACTUAL) for “best/typical/worst” and for WTI uncertainty bands.
- **Residual monitoring**: Flag (entity, time) with large residuals for ops review (possible breakdown, bad POSTED, or special event).

#### B. PRIORITY in the model

**Issue:** PRIORITY is a different outcome (availability, 8888, return-time minutes). For ACTUAL prediction it could inform, not be the target.

**For v1:** We **ignore PRIORITY** as a feature in the ACTUAL model. Keeps the model simpler; we can add **PRIORITY-derived features** (e.g. `priority_sold_out_today`, `priority_return_minutes_recent`, `has_priority`) in **v2** if we find they help. A separate PRIORITY model for “will I get a return time?” / 8888 is also a v2 option.

#### C. Three regimes: backfill, forecast, live

**Issue:** The three use cases need different inputs and pipelines.

| Regime | POSTED available? | ACTUAL available? | Main difficulty |
|--------|-------------------|-------------------|-----------------|
| **Backfill** | Often yes (sparse or dense) | Sparse; many (entity, park_date) have none | Imputing ACTUAL when POSTED is missing or when we have no ACTUAL to train on for that slot |
| **Forecast** | No (future) | No | Must predict ACTUAL without same-day POSTED |
| **Live** | Yes (just observed) | Sometimes (we can later compare to TP) | Low-latency inference |

**Improvements:**

- **Backfill**
  - **Where we have POSTED in a window:** Use the model: features + POSTED → ACTUAL. For slots with no POSTED in that window, options: (1) **Interpolate** from neighboring POSTED (e.g. 10‑min buckets: use last/next POSTED or a smoothed series), (2) **Prior only**: use a conservative prior (e.g. park-hour-entity average from days where we do have ACTUAL), (3) **Only impute where we have at least one POSTED** in a surrounding window and mark the rest “no estimate.” Start with (3) and (1) for smoother curves.
  - **Where we have no ACTUAL for training:** Use a **chronological train/val/test** split. Train on (entity, park_date) with ACTUAL; validate imputation on held-out park-days that have ACTUAL. For entities/dates with zero ACTUAL ever, we must rely on a **global or park-level prior** or on transfer from similar entities.

- **Forecast**
  - For future days we have **no same-day POSTED**. We still need **predicted ACTUAL** (for WTI and apps) and, for **live comparison and trust**, **predicted POSTED**.
  - **Predicted POSTED:** Build from historical aggregates: (entity, dategroupid, hour) → median or mean POSTED. Publish **predicted POSTED** for each (entity, park_date, time_slot) in the forecast. **Use:** In live settings, compare “we predicted POSTED = X” to “we observe POSTED = Y” — directly interpretable and builds trust in the modelling. **WTI is unchanged:** WTI uses only observed and predicted **ACTUAL**; predicted POSTED is not an input to WTI.
  - **Predicted ACTUAL (forecast):** Use a **features-only** model (no POSTED): mins_since_6am, mins_since_park_open, dategroupid, season, season_year, entity, park, etc. **Optional v2:** Use predicted POSTED as an extra feature in the ACTUAL forecast model; we can test if it helps. For v1, ACTUAL forecast stays features-only.
  - **Summary:** We **do** produce **predicted POSTED** (from aggregates) and **predicted ACTUAL** (from the features-only model). Both are forecast outputs; predicted POSTED is for comparison and live content, not for WTI.

- **Live**
  - We have **POSTED now**. Features (mins_since_6am, mins_since_park_open, dategroupid, season, etc.) are cheap to compute; the bottleneck is model load and forward pass. **Improvements:** (a) keep a **warm model** in memory or in a small inference service, (b) **ONNX** or **treelite** for fast, portable inference, (c) **caching**: for repeated (entity, time_bucket, posted_bucket) in a short window, return cached ACTUAL, (d) **precomputed lookup** for a discretized grid (entity × dategroupid × hour × posted_bucket) as a fast path when we can afford coarser resolution.

#### D. WTI: definition and robustness

**Issue:** “Average ACTUAL over all attractions” requires: which entities, which time resolution, how to treat closed and missing.

**Improvements:**
- **Entity set:** Use **all attractions** (all entities in dimentity or all with at least one non-null ACTUAL or predicted ACTUAL that day). We **do not** maintain a “core” list: that becomes a pain (ride closures, new openings). The set naturally adapts as we add/remove entities in dimentity.
- **Closed and null — no explicit “open” list:** We **do not** need to define “which rides are open.” Instead, we use a **strict null rule**: when a ride is **closed**, ACTUAL is **null** (undefined). Those (entity, time_slot) are **excluded** from the WTI mean. So: **WTI = mean(actual) over (entity, time_slot) where actual is not null.** We need signals to set null:
  - **“0 mins” when closed is a false signal:** Queue-times (and possibly others) may report **0** for a closed ride. That is not a real 0‑minute wait; it must be treated as **null** so it does not count in WTI. We must detect “closed” and set to null. (Queue-times: check API for `is_open` / `operating`; if we infer from context, document the rule.)
  - **TouringPlans “closed” signal:** TP has a good closed/operating signal. We **do not currently capture it** in this pipeline. **Enhancement:** Ingest TP’s closed/operating_status when we have access; use it to set ACTUAL (and POSTED) to null when closed.
- **Time resolution:** Use **5‑minute** slots for daily curves and WTI aggregation. We collect every 5 min from queue-times, so 5 min is tenable and matches collection; we can always downsample to 10 or 15 for specific consumers. Legacy used 15 min; 5 min gives finer resolution.
- **Missing ACTUAL (not closed):** For (entity, time_slot) where we have no “closed” signal: **observed** ACTUAL (from TP or POSTED→ACTUAL imputation) or **predicted** ACTUAL (from the forecast model). **Rule:** Prefer observed when available; otherwise use predicted. If we have neither, **exclude** that (entity, time_slot) from the mean. Do not “double count” when we have both.
- **Comparability over time:** Using all attractions keeps the definition simple; the only exclusion is null (closed). Document any change in how we set null (e.g. when we add TP closed).

#### E. Sparse ACTUAL and cold start

**Issue:** Many (entity, park_date) have no observed ACTUAL. Model quality depends on POSTED coverage and on how well we transfer across entities/dates.

**Improvements:**
- **Stratified evaluation:** Report metrics by “has ACTUAL” vs “ACTUAL-only imputed” and by entity/park. Validate backfill on park-days where we *do* have ACTUAL.
- **Entity representation:** Use **entity_id** (or park_id) as a categorical feature so the model can learn entity-specific POSTED→ACTUAL slopes. If an entity has very few ACTUAL, the model falls back to park or global. Consider **entity embedding** or **hierarchical** ideas later; start with a categorical.
- **Cold start:** New entity or new dategroupid: use **park-level** or **global** means/medians as prior until we have enough data.

#### F. Distribution shift and retraining

**Issue:** POSTED/ACTUAL relationship can change (ops, LL changes, new attractions). A static model will drift.

**Improvements:**
- **Scheduled retraining:** e.g. weekly full retrain on last N days (or all history with appropriate sampling). Version models (e.g. `model_v2_20250126`) and keep the previous for rollback.
- **Drift detection:** Monitor **residuals** (and, if available, feature distributions) over time. Trigger a retrain or alert when error or distribution shifts beyond a threshold.
- **Incremental data:** Ensure ETL and feature pipelines can append new fact and dimension data so that “last N days” is always available.

#### G. Quantile regression (optional) — what it is

Our primary model is **mean regression**: XGBoost with `reg:squarederror` (or `reg:absoluteerror`) predicts the **conditional mean** E[ACTUAL | X] — one number per row. Same idea as TREENET (Minitab) or any gradient-boosted trees for regression.

**Quantile regression** predicts **conditional quantiles** instead of the mean. For each input we get, for example:
- 10th percentile: “in at least 10% of cases, ACTUAL ≤ this value” (optimistic),
- 50th (median): typical,
- 90th: “in at least 90% of cases, ACTUAL ≤ this value” (pessimistic).

XGBoost supports this with `objective='reg:quantileerror'` and `quantile_alpha` (e.g. 0.1, 0.5, 0.9). It’s the **same tree-based boosting**; only the **loss** changes from squared error to quantile loss. We’d train one model per quantile (or a multi-output setup if available).

**Uses:** (a) “Best / typical / worst” wait for an attraction or WTI; (b) **WTI uncertainty bands** (e.g. WTI as median plus 10th–90th range); (c) asymmetric loss if we care more about over- or under-prediction.

**Recommendation:** Treat as **optional for v1**. The mean model is the base; add quantile models later if we want intervals and bands.

---

## 3. Detailed Map: How to Achieve It

### 3.1 Stage overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  EXISTING: fact_tables/clean, dimension_tables, staging/queue_times, validate_wait_times │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  STAGE A: Feature layer & model-ready dataset                                           │
│  - Entity-grouped (or park-date) fact + dimensions → feature script                     │
│  - Output: model_ready/(train|val|test)/ or model_ready/features.parquet                │
│  - Rows: (entity, observed_at, park_date, posted, actual, mins_since_6am,               │
│          mins_since_park_open, dategroupid, season, season_year, (v2: priority_*), …)    │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
┌──────────────────────────────────────┐    ┌──────────────────────────────────────────────┐
│  STAGE B: Train & validate           │    │  STAGE C: POSTED aggregates (required)       │
│  - **Two models:**                   │    │  - By (entity, dategroupid, hour): median    │
│    (1) With-POSTED: ACTUAL ~ POSTED  │    │    POSTED. Produces predicted POSTED for   │
│        + features. Backfill + live.  │    │    forecast — for live comparison and          │
│    (2) Without-POSTED: ACTUAL ~      │    │    trust. Not used in WTI.                  │
│        features only. Forecast.      │    └──────────────────────────────────────────────┘
│  - Chronological split; val on       │                         │
│    recent park-days                  │                         │
│  - Export: joblib + ONNX (optional)  │                         │
│  - Versioning: models/model_*_*.     │                         │
└──────────────────────────────────────┘                         │
                    │                                             │
                    └───────────────────┬─────────────────────────┘
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  STAGE D: Backfill (historical ACTUAL curves)                                           │
│  - For each (entity, park_date) in the past, **5‑min time_slots**:                      │
│    - Build features; get POSTED from fact (or interpolate); if missing, prior or skip   │
│    - Run **with-POSTED** model → imputed ACTUAL                                         │
│  - Where we have observed ACTUAL: keep it; else use imputed                             │
│  - **Closed → null:** set actual=null when we have a closed signal; 0 when closed → null│
│  - Output: curves/backfill/ (entity, park_date, time_slot, actual, source=observed|     │
│            imputed); null where closed                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  STAGE E: Forecast (future ACTUAL and POSTED curves)                                    │
│  - For each (entity, park_date) from tomorrow to +2 years, **5‑min time_slots**:        │
│    - **Predicted POSTED:** from aggregates (entity, dategroupid, hour) → median POSTED  │
│    - **Predicted ACTUAL:** features from dims only; **without-POSTED** model            │
│  - Output: curves/forecast/ (entity, park_date, time_slot, actual_predicted,            │
│            posted_predicted). posted_predicted for live comparison and trust.           │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│  STAGE F: WTI computation                                                               │
│  - For each (park, park_date), **5‑min slots** in operating window:                     │
│    - **All entities** (no core list); for each (entity, time_slot):                     │
│      actual = observed else imputed else predicted; **if closed → null**                │
│    - **WTI = mean(actual) over (entity, time_slot) where actual is not null**           │
│  - Output: wti/wti.parquet (park, park_date, wti, n_entities, …)                        │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
┌──────────────────────────────────────┐    ┌──────────────────────────────────────────────┐
│  STAGE G: Live inference service     │    │  STAGE H: Lookup / precomputed cache         │
│  - Input: (entity, observed_at,      │    │  - Forecast and WTI are precomputed           │
│    posted)                           │    │  - For “today”: optional incremental WTI     │
│  - Features on-the-fly; model        │    │    update as new POSTED→ACTUAL arrives       │
│    forward pass                      │    │  - App: (entity, park_date, time_slot) →     │
│  - Output: predicted ACTUAL          │    │    predicted_actual or WTI                   │
│  - Deploy: FastAPI + joblib/ONNX;    │    │  - Store: Parquet, Redis, or embedded DB     │
│    or in-process in app              │    │    keyed by (park, park_date) or (entity,    │
│  - Optional: cache for               │    │    park_date, time_slot)                     │
│    (entity, time_bucket, posted_     │    └──────────────────────────────────────────────┘
│    bucket)                           │
└──────────────────────────────────────┘
```

### 3.2 Data architecture

| Layer | Purpose | Format / location | Update frequency |
|-------|---------|-------------------|------------------|
| **Fact (existing)** | Raw wait times: entity, observed_at, wait_type, minutes | `fact_tables/clean/YYYY-MM/{park}_{date}.csv` | 5am/7am ETL; queue-times merge |
| **Staging (existing)** | Live queue-times before merge | `staging/queue_times/` | Every 5 min when in-window |
| **Dimensions (existing)** | dimentity, dimparkhours, dimdategroupid, dimseason, etc. | `dimension_tables/*.csv` | 6am dimension fetch |
| **Model-ready** | Fact + features, join dims; one row per (entity, observed_at, wait_type) | `model_ready/features.parquet` or split train/val/test | On-demand or nightly when building datasets |
| **POSTED aggregates** | (entity, dategroupid, hour) → median POSTED. **Required** for predicted POSTED in forecast (live comparison and trust). | `aggregates/posted_entity_dgid_hr.parquet` | When building forecast |
| **Models** | **Two** XGBoost: with-POSTED (backfill, live), without-POSTED (forecast); + ONNX optional | `models/model_with_posted_*.joblib`, `model_without_posted_*.joblib` | After each retrain |
| **Backfill curves** | (entity, park_date, time_slot, actual, source) | `curves/backfill/` or `fact_tables/actual_curves/` | After backfill run (e.g. weekly or one-off) |
| **Forecast curves** | (entity, park_date, time_slot, actual_predicted, posted_predicted) | `curves/forecast/` | After 6am dims; e.g. daily for tomorrow→+2yr |
| **WTI** | (park, park_date, wti, n_entities, …) | `wti/wti.parquet` or `wti/park_date.csv` | With forecast; “today” can be incremental |
| **Live inference** | In-memory model + optional Redis (or similar) for hot (entity, time, posted) | Process / Redis | Model: on deploy; cache: continuous |

**Principles:**
- **Single output_base** for all of the above (except live service, which may run elsewhere).
- **Parquet** for model-ready, aggregates, curves, and WTI when we need columnar and partitions (e.g. by park_date).
- **Idempotent jobs:** Backfill, forecast, and WTI can re-run and overwrite; we avoid appends that duplicate.

### 3.3 Data engineering

| Task | Description |
|------|-------------|
| **Entity-grouped fact** | Build `fact_tables/by_entity/{entity}.csv` (or Parquet) from park-date CSVs. Enables entity-level feature builds and training loops. |
| **Feature module** | `add_mins_since_6am`, `add_park_hours` (mins_since_park_open, open/close, emh), `add_dategroupid`, `add_season`, `add_geometric_decay` (for training weight). (v2: PRIORITY-derived flags.) Input: fact rows + dims; output: rows with `pred_*` and `observed_wait_time` (from `wait_time_minutes` where type=ACTUAL). |
| **Train/val/test split** | Chronological. For example: train on park_dates before date D1, val on [D1, D2), test on [D2, D3). Or by entity: ensure we have ACTUAL in each split for entities we care about. |
| **POSTED aggregate job (required)** | From fact: (entity_code, dategroupid, hour) → median POSTED. Produces predicted POSTED for forecast (live comparison and trust). ACTUAL forecast stays features-only. |
| **Validation before model** | Reuse and extend `validate_wait_times`: ensure fact rows used for training have valid POSTED and ACTUAL in range; exclude 8888 from ACTUAL target; log exclusions. |
| **Closed and null handling** | **Rule:** When a ride is closed, ACTUAL (and POSTED for that slot) = **null**. **“0 when closed”** from queue-times (or similar) is a false signal → set to null. Use: (a) TouringPlans “closed” signal when we ingest it, (b) queue-times API `is_open`/`operating` if available, (c) documented inference rules until we have (a)/(b). Exclude null from WTI mean. |
| **Backfill job** | Loop (entity, park_date) in historical range, **5‑min time_slots**. (1) Features from dims, (2) POSTED from fact — aggregate to 5‑min or interpolate, (3) where POSTED missing: interpolate, prior, or skip, (4) run **with-POSTED** model → imputed ACTUAL, (5) where observed ACTUAL: keep and `source=observed`; else `source=imputed`, (6) **closed → null**. Parallelize by park_date or entity. |
| **Forecast job** | Loop (entity, park_date) from tomorrow to +2 years, **5‑min time_slots**. (1) **Predicted POSTED** from aggregates (entity, dategroupid, hour). (2) **Features from dims only**; run **without-POSTED** model → predicted ACTUAL. Output: `actual_predicted`, `posted_predicted`. Run after 6am dims. |
| **WTI job** | For each (park, park_date), **5‑min slots** in operating window: (1) **all entities** (no core list), (2) for each (entity, time_slot): actual = observed else imputed else predicted; **exclude if actual is null (closed)**, (3) WTI = mean(actual) over included (entity, time_slot), (4) n_entities, min, max. Write to `wti/`. |
| **Incremental “today” WTI (optional)** | When new POSTED→ACTUAL (live or from latest staging) arrives for today: update only the affected (entity, time_slot) and recompute mean for that park_date. Store in same WTI table with a “as_of” or “updated_at” if we need history. |

### 3.4 Data science and statistical methods

| Method | Use |
|--------|-----|
| **Regression (XGBoost)** | **Two models.** (1) **With-POSTED:** ACTUAL ~ POSTED + mins_since_6am + mins_since_park_open + dategroupid + season + season_year + entity + park + POSTED×hour + POSTED×dategroupid + rolling_posted_*. (v2: priority_*.) For backfill and live. (2) **Without-POSTED:** ACTUAL ~ same features **except POSTED**. For forecast. Use `reg:squarederror` or `reg:absoluteerror`. Same tree-based boosting as TREENET-style models. |
| **Quantile regression** | **Optional (v2):** XGBoost `reg:quantileerror` with `quantile_alpha` 0.1, 0.5, 0.9. Predicts conditional quantiles (not mean) for “best/typical/worst” and WTI uncertainty bands. Same boosting; different loss. See §2.2 G. |
| **Chronological splitting** | Train/val/test by park_date to avoid leakage and to simulate production. |
| **Stratified evaluation** | Metrics by: has ACTUAL vs imputed-only; by park; by dategroupid (e.g. holiday vs not). MAE, RMSE, MAPE; also correlation and residual plots. |
| **Imputation** | For backfill: (1) last-observation carry-forward or linear interpolation of POSTED within a window, (2) prior = park-entity-hour mean ACTUAL from days with ACTUAL, (3) mark `source=imputed` and track coverage. |
| **Features-only forecast** | **ACTUAL** forecast uses **no POSTED**: mins_since_6am, mins_since_park_open, dategroupid, season, season_year, entity, park, etc. (v2: priority_*.) **Predicted POSTED** from aggregates is a **separate output** for live comparison and trust; optional v2: use as feature in ACTUAL model. |
| **Predicted POSTED** | From (entity, dategroupid, hour) aggregates: median POSTED. Published per (entity, park_date, time_slot) in forecast. For live comparison and trust; not used in WTI or in ACTUAL model. |
| **PRIORITY as features** | **v2:** Binary and numeric summaries (sold out, recent return minutes) as covariates for ACTUAL. v1: ignore PRIORITY. |
| **Residual analysis** | Monitor residuals by entity, park_date, hour. Flag anomalies for ops; feed into drift detection. |

### 3.5 Computational methods

| Method | Use |
|--------|-----|
| **Vectorized feature build** | Pandas (or Polars) for mins_since_6am, park_date, joins to dimdategroupid, dimseason, dimparkhours. Avoid row-by-row Python where possible. |
| **Partitioned reads** | For backfill and forecast: read fact and curves by `park_date` or `entity` to limit memory. Parquet partitioning by `park_date` or `entity`. |
| **Parallel over entities or park_dates** | Backfill and forecast: joblib, multiprocessing, or Dask over (entity, park_date). Each worker loads model once and runs a batch. |
| **Batch inference** | In backfill/forecast, run `model.predict(X_batch)` instead of per-row. |
| **Sparse storage** | If we have many (entity, park_date, time_slot) with missing ACTUAL, store only non-missing or only slots we impute/predict. |

### 3.6 Optimization methods

| Method | Use |
|--------|-----|
| **Hyperparameter tuning** | Optuna, GridSearch, or Bayesian optimization over max_depth, learning_rate, n_estimators, min_child_weight, subsample, colsample_bytree. Metric: val MAE or RMSE. Run when we add features or on a schedule (e.g. quarterly). |
| **Early stopping** | XGBoost `early_stopping_rounds` on a validation set to avoid overfitting. |
| **Feature selection** | From SHAP or built-in importance: drop low-importance features to speed inference and reduce overfitting. Re-evaluate after adding new covariates. |
| **Model size vs accuracy** | Limit `max_depth` and `n_estimators` to keep models small for fast load and inference in live service. |

### 3.7 AI-assisted and decision-support techniques

| Technique | Use |
|-----------|-----|
| **SHAP / feature importance** | Interpret which features (POSTED, hour, dategroupid, etc.) drive ACTUAL. Suggest new interactions or features. |
| **Automated HPO** | Optuna (or similar) to search over model and, if needed, over feature sets or lags. |
| **Anomaly detection** | On residuals: flag (entity, park_date, time_slot) where |actual - predicted| > k×MAD or similar for ops review. |
| **Drift detection** | Monitor val error or residual distribution over calendar time; trigger retrain or alert when degradation. |
| **Natural language (optional)** | For ops or product: “Why is WTI high today at MK?” — use a summary of dategroupid, season, entities down, or recent POSTED vs usual. Can be template-based first; LLM later if needed. |

---

## 4. Latency and “lightning fast” design

| Path | Latency target | Approach |
|------|----------------|----------|
| **Predicted ACTUAL (forecast)** | &lt; 10–50 ms | Precomputed. Lookup (entity, park_date, time_slot) in Parquet, Arrow, or Redis. App gets park_date and time_slot from current time and park TZ. |
| **Observed ACTUAL (from POSTED now)** | &lt; 50–200 ms | Warm model in memory; feature build is a few scalars and a dim lookup (dategroupid, season from park_date — can be cached per park_date); single `model.predict(X)` for one row. Option: ONNX runtime; optional cache for (entity, time_bucket, posted_bucket). |
| **WTI for a park-date** | &lt; 10 ms | Precomputed. Lookup (park, park_date) in `wti/wti.parquet` or similar. For “today” with incremental updates: one read and possibly one lightweight recompute of the mean when new data arrives. |

**Precomputation vs on-demand:**
- **Forecast and WTI:** Always precomputed in batch. No on-demand model runs for “what will MK be tomorrow?”
- **Live POSTED→ACTUAL:** On-demand model call. Optional: precomputed lookup table for a discretized (entity, dategroupid, hour, posted_bucket) for a “fast path” when we can accept coarser resolution.

---

## 5. Suggested implementation order

1. **Feature layer** — `add_mins_since_6am`, `add_dategroupid`, `add_season`, `add_park_hours` (and `add_geometric_decay` for training). Input: fact + dims. Output: feature-rich rows with `observed_wait_time` for ACTUAL rows.
2. **Entity-grouped fact and model-ready dataset** — Build entity-grouped fact; join features; define train/val split. Validate with `validate_wait_times` and basic coverage reports.
3. **Train two XGBoost models** — (1) **With-POSTED:** ACTUAL ~ POSTED + features. (2) **Without-POSTED:** ACTUAL ~ features only. Chronological split; export joblib; evaluate on val. SHAP and residual checks.
4. **Closed / null convention** — Define and implement: when to set ACTUAL/POSTED to null (TP closed when we have it; queue-times “0 when closed” once we can detect it). Document in schema and validation.
5. **Backfill** — Historical (entity, park_date), **5‑min slots**, with-POSTED model; merge observed ACTUAL where available; closed → null. Write curves.
6. **Forecast** — **POSTED aggregates** (entity, dategroupid, hour)→median from fact. Tomorrow → +2 years, **5‑min slots**: **posted_predicted** from aggregates, **actual_predicted** from **without-POSTED** model. Write forecast curves (actual_predicted, posted_predicted).
7. **WTI** — **All entities**; **5‑min** operating window; mean ACTUAL over (entity, time_slot) **where actual is not null**. Write WTI table.
8. **Live inference** — (entity, observed_at, posted) → features → **with-POSTED** model.predict. ONNX and caching as needed.
9. **Incremental “today” WTI and retraining** — Optional: update “today” as new data arrives; scheduled retrain and drift monitoring.

---

## 6. Decided and open design choices

**Decided:**
- **WTI entity set:** **All attractions** (no maintained core list). Exclude only (entity, time_slot) where ACTUAL is **null** (closed).
- **Closed and null:** Strict **null** when closed. “0 mins” from queue-times when the ride is closed = **false signal** → null. We need: TP “closed” (when we ingest it), queue-times `is_open`/operating (if available), or documented inference rules.
- **Time slot for curves and WTI:** **5 minutes.** Matches queue-times collection; finer than legacy’s 15 min. We can downsample to 10 or 15 for specific consumers.
- **Forecast:** **ACTUAL** from features-only (without-POSTED) model. **Predicted POSTED** from (entity, dategroupid, hour) aggregates — for live comparison and trust; **not** used in WTI.
- **Two ACTUAL models:** With-POSTED (backfill, live) and without-POSTED (forecast).
- **Predicted POSTED:** Produced for forecast (live comparison and trust); not an input to WTI.
- **PRIORITY:** v1 ignore in ACTUAL model; v2 for PRIORITY-derived features and separate PRIORITY model.
- **Quantile regression:** Optional for v2; mean regression is the base.

**Open / to decide when we implement:**
- **Ingesting TouringPlans “closed” / operating_status:** We do not capture it yet. Design fact or a companion feed to support null when closed.
- **Detecting “0 when closed” from queue-times:** Check API for `is_open` or similar; if absent, define an inference rule (e.g. 0 during known closed hours) and document.
- **One global vs per-entity:** Start with one global model + entity (and park) as categorical; compare to per-entity if we have enough ACTUAL.

---

## 7. References

- [SCHEMA.md](SCHEMA.md) — Fact table and 6am rule
- [ATTRACTION_IO_ALIGNMENT.md](ATTRACTION_IO_ALIGNMENT.md) — Legacy feature list and goals
- [LEGACY_PIPELINE_CRITICAL_REVIEW.md](LEGACY_PIPELINE_CRITICAL_REVIEW.md) — What we improve relative to attraction-io
