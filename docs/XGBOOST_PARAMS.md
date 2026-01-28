# XGBoost Training Parameters

We train per-entity XGBoost models in [src/processors/training.py](../src/processors/training.py) to predict ACTUAL (or PRIORITY) wait times. **Parameters match the Julia legacy** (attraction-io, XGBoost.jl in `run_trainer.jl`) so behavior and predictions align with the proven pipeline.

---

## Current Python Parameters (aligned with Julia)

Defined in `src/processors/training.py` as `DEFAULT_XGB_PARAMS` and `EARLY_STOPPING_ROUNDS`:

| Parameter            | Value | Julia (run_trainer.jl) |
|----------------------|-------|------------------------|
| `objective`          | `"reg:absoluteerror"` | `objective = "reg:absoluteerror"` (MAE) |
| `tree_method`        | `"hist"` | `tree_method = use_gpu ? "gpu_hist" : "hist"`; we use CPU `"hist"` |
| `max_depth`          | `6` | `max_depth = 6` |
| `learning_rate`      | `0.1` | `eta = 0.1` |
| `n_estimators`      | `2000` | `num_round = 2000` — need 2000 trees for accurate prediction |
| `subsample`          | `0.5` | `subsample = 0.5` |
| `colsample_bytree`   | `1.0` | not set in Julia → XGBoost default 1.0 |
| `min_child_weight`   | `10` | `min_child_weight = 10` |
| `random_state`       | `42` | (Python-only for reproducibility) |
| `verbosity`          | `0` | `verbosity = 0` |
| **Early stopping**  | **None** | Julia uses `watchlist = ()` → no early stop; we run all 2000 rounds |

Julia snippet from `run_trainer.jl`:

```julia
booster = xgboost(
    dtrain;
    num_round = 2000,
    eta = 0.1,
    max_depth = 6,
    subsample = 0.5,
    min_child_weight = 10,
    objective = "reg:absoluteerror",
    tree_method = use_gpu ? "gpu_hist" : "hist",
    nthread = XGB_THREADS,
    verbosity = 0,
    watchlist = ()
)
```

---

## Parameter name mapping (Python ↔ Julia / XGBoost native)

| Python (training.py) | XGBoost native / Julia |
|----------------------|-------------------------|
| `learning_rate`      | `eta`                   |
| `n_estimators`       | `num_round` (Julia) / `num_boost_round` (Python train call) |
| `max_depth`          | `max_depth`             |
| `subsample`          | `subsample`             |
| `colsample_bytree`   | `colsample_bytree`     |
| `min_child_weight`   | `min_child_weight`      |
| `objective`          | `objective`             |
| early stopping       | Julia: `watchlist = ()` = no early stop; Python: `early_stopping_rounds=None` |

---

## Notes

- **2000 rounds**: Julia runs 2000 trees by default; we need that many for accurate enough predictions. Training time per entity will be longer than with 100 rounds + early stop.
- **reg:absoluteerror**: Legacy uses MAE, not MSE; we use `reg:absoluteerror` to match.
- **GPU**: Julia can use `gpu_hist` when `use_gpu` is true; our Python code uses `"hist"` (CPU). To use GPU in Python you would set `tree_method="gpu_hist"` and ensure an NVIDIA GPU + CUDA-enabled XGBoost build.
