"""
Training Module

================================================================================
PURPOSE
================================================================================
Trains XGBoost models to predict ACTUAL wait times from features and POSTED.

Two models are trained:
  1. **With-POSTED**: ACTUAL ~ POSTED + features (for backfill and live inference)
  2. **Without-POSTED**: ACTUAL ~ features only (for forecast)

Uses chronological train/val/test split by park_date to avoid temporal leakage.

================================================================================
USAGE
================================================================================
  from processors.training import train_entity_model
  
  # Load entity data, add features, encode
  df = load_entity_data(entity_code, output_base, index_db)
  df_features = add_features(df, output_base)
  df_encoded, _ = encode_features(df_features, output_base)
  
  # Train models
  models, metrics = train_entity_model(
      df_encoded,
      entity_code,
      output_base,
      train_ratio=0.7,
      val_ratio=0.15,
  )
  
  # Models saved to models/{entity_code}/model_with_posted.json
  #                  models/{entity_code}/model_without_posted.json

================================================================================
MODEL ARCHITECTURE
================================================================================
- **Algorithm**: XGBoost (gradient boosted trees)
- **Objective**: reg:squarederror (mean squared error)
- **Features**: 
  - With-POSTED: wait_time_minutes (POSTED), pred_*, park_code, entity_code (encoded)
  - Without-POSTED: pred_*, park_code, entity_code (encoded), no POSTED
- **Target**: observed_wait_time (ACTUAL wait times only)
- **Split**: Chronological by park_date (train < val < test)

================================================================================
EVALUATION METRICS
================================================================================
- MAE (Mean Absolute Error)
- RMSE (Root Mean Squared Error)
- MAPE (Mean Absolute Percentage Error)
- R² (Coefficient of Determination)
- Correlation (Pearson)
"""

from __future__ import annotations

import json
import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from zoneinfo import ZoneInfo

try:
    import xgboost as xgb
except ImportError:
    xgb = None

from utils import get_output_base


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default XGBoost hyperparameters
DEFAULT_XGB_PARAMS = {
    "objective": "reg:squarederror",
    "tree_method": "hist",
    "max_depth": 6,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 3,
    "random_state": 42,
    "verbosity": 0,
}

# Early stopping rounds
EARLY_STOPPING_ROUNDS = 10


# =============================================================================
# DATA PREPARATION
# =============================================================================

def prepare_training_data(
    df: pd.DataFrame,
    include_posted: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, pd.Series, list[str]]:
    """
    Prepare training data: select features and target.
    
    Args:
        df: DataFrame with features and encoded categoricals
        include_posted: If True, include POSTED wait_time_minutes as feature
        logger: Optional logger
    
    Returns:
        Tuple of (X, y, feature_names)
    """
    # Filter to ACTUAL wait times only (our target)
    df_actual = df[df["wait_time_type"] == "ACTUAL"].copy()
    
    if df_actual.empty:
        raise ValueError("No ACTUAL wait times found in data")
    
    # Select features
    feature_cols = [
        "pred_mins_since_6am",
        "pred_dategroupid",
        "pred_season",
        "pred_season_year",
        "park_code",
        "entity_code",
    ]
    
    # Add park hours features if available
    park_hours_cols = [
        "pred_mins_since_park_open",
        "pred_park_open_hour",
        "pred_park_close_hour",
        "pred_park_hours_open",
        "pred_emh_morning",
        "pred_emh_evening",
    ]
    for col in park_hours_cols:
        if col in df_actual.columns:
            feature_cols.append(col)
    
    # Add POSTED if requested
    if include_posted:
        # For with-POSTED model, we need POSTED values
        # Strategy: Join POSTED to ACTUAL rows by matching entity_code and park_date
        # Use the closest POSTED time to each ACTUAL time (within same park_date)
        df_posted = df[df["wait_time_type"] == "POSTED"].copy()
        
        if not df_posted.empty:
            # Ensure park_date exists in both
            if "park_date" not in df_posted.columns:
                from processors.features import add_park_date
                df_posted = add_park_date(df_posted)
            
            # Merge POSTED to ACTUAL: for each ACTUAL row, find closest POSTED
            df_actual_with_posted = df_actual.copy()
            df_actual_with_posted["posted_wait_time"] = None
            
            # Group by entity and park_date for efficiency
            for (entity, park_date), group in df_actual_with_posted.groupby(["entity_code", "park_date"]):
                # Get POSTED for this entity and park_date
                posted_subset = df_posted[
                    (df_posted["entity_code"] == entity) &
                    (df_posted["park_date"] == park_date)
                ]
                
                if posted_subset.empty:
                    continue
                
                # Convert times
                group_times = pd.to_datetime(group["observed_at"], errors="coerce")
                posted_times = pd.to_datetime(posted_subset["observed_at"], errors="coerce")
                
                # For each ACTUAL time, find closest POSTED
                for idx in group.index:
                    if pd.isna(group_times.loc[idx]):
                        continue
                    
                    time_diffs = (posted_times - group_times.loc[idx]).abs()
                    closest_idx = time_diffs.idxmin()
                    df_actual_with_posted.loc[idx, "posted_wait_time"] = posted_subset.loc[closest_idx, "wait_time_minutes"]
            
            df_actual = df_actual_with_posted
            feature_cols.append("posted_wait_time")
        else:
            if logger:
                logger.warning("No POSTED data found; training without-POSTED model only")
            include_posted = False
    
    # Select only available feature columns
    available_features = [col for col in feature_cols if col in df_actual.columns]
    missing_features = [col for col in feature_cols if col not in df_actual.columns]
    
    if missing_features and logger:
        logger.warning(f"Missing features: {missing_features}")
    
    if not available_features:
        raise ValueError("No features available for training")
    
    # Extract X and y
    X = df_actual[available_features].copy()
    y = df_actual["observed_wait_time"].copy()
    
    # Drop rows with null target
    mask = y.notna()
    X = X[mask].copy()
    y = y[mask].copy()
    
    if len(X) == 0:
        raise ValueError("No valid training examples after filtering nulls")
    
    # Convert boolean columns to int
    for col in X.columns:
        if X[col].dtype == bool:
            X[col] = X[col].astype(int)
    
    # Fill remaining nulls with median (for numeric) or mode (for categorical)
    for col in X.columns:
        if X[col].isna().any():
            if X[col].dtype in [np.int64, np.float64, "Int64", "Float64"]:
                X[col] = X[col].fillna(X[col].median())
            else:
                X[col] = X[col].fillna(X[col].mode()[0] if not X[col].mode().empty else 0)
    
    if logger:
        logger.info(f"Prepared {len(X)} training examples with {len(available_features)} features")
        if include_posted:
            logger.info(f"  - POSTED coverage: {(X['posted_wait_time'].notna().sum() / len(X) * 100):.1f}%")
    
    return X, y, available_features


def split_by_date(
    df: pd.DataFrame,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame chronologically by park_date.
    
    Args:
        df: DataFrame with park_date column
        train_ratio: Proportion for training (default: 0.7)
        val_ratio: Proportion for validation (default: 0.15)
        # test_ratio = 1 - train_ratio - val_ratio
    
    Returns:
        Tuple of (train_df, val_df, test_df)
    """
    if "park_date" not in df.columns:
        raise ValueError("DataFrame must have 'park_date' column")
    
    # Get unique dates and sort
    unique_dates = sorted(df["park_date"].unique())
    n_dates = len(unique_dates)
    
    # Calculate split indices
    train_end = int(n_dates * train_ratio)
    val_end = int(n_dates * (train_ratio + val_ratio))
    
    train_dates = set(unique_dates[:train_end])
    val_dates = set(unique_dates[train_end:val_end])
    test_dates = set(unique_dates[val_end:])
    
    # Split DataFrame
    train_df = df[df["park_date"].isin(train_dates)].copy()
    val_df = df[df["park_date"].isin(val_dates)].copy()
    test_df = df[df["park_date"].isin(test_dates)].copy()
    
    return train_df, val_df, test_df


# =============================================================================
# MODEL TRAINING
# =============================================================================

def train_xgb_model(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    params: Optional[Dict] = None,
    early_stopping_rounds: int = EARLY_STOPPING_ROUNDS,
    logger: Optional[logging.Logger] = None,
) -> xgb.XGBRegressor:
    """
    Train XGBoost regression model.
    
    Args:
        X_train: Training features
        y_train: Training target
        X_val: Validation features
        y_val: Validation target
        params: XGBoost parameters (default: DEFAULT_XGB_PARAMS)
        early_stopping_rounds: Early stopping rounds
        logger: Optional logger
    
    Returns:
        Trained XGBoost model
    """
    if xgb is None:
        raise ImportError("XGBoost not installed. Install with: pip install xgboost")
    
    if params is None:
        params = DEFAULT_XGB_PARAMS.copy()
    
    # Create DMatrix for XGBoost
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dval = xgb.DMatrix(X_val, label=y_val)
    
    # Train model
    model = xgb.train(
        params,
        dtrain,
        num_boost_round=params.get("n_estimators", 100),
        evals=[(dtrain, "train"), (dval, "val")],
        early_stopping_rounds=early_stopping_rounds,
        verbose_eval=False,
    )
    
    if logger:
        logger.info(f"Trained XGBoost model: {model.best_iteration} rounds (best iteration)")
    
    # Convert to sklearn API for easier use
    sklearn_model = xgb.XGBRegressor(**params)
    sklearn_model._Booster = model
    sklearn_model._le = None  # No label encoding needed for regression
    
    return sklearn_model


# =============================================================================
# EVALUATION
# =============================================================================

def evaluate_model(
    model: xgb.XGBRegressor,
    X: pd.DataFrame,
    y: pd.Series,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, float]:
    """
    Evaluate model and return metrics.
    
    Args:
        model: Trained XGBoost model
        X: Features
        y: True target values
        logger: Optional logger
    
    Returns:
        Dictionary of metrics (MAE, RMSE, MAPE, R², correlation)
    """
    y_pred = model.predict(X)
    
    # Calculate metrics
    mae = mean_absolute_error(y, y_pred)
    rmse = np.sqrt(mean_squared_error(y, y_pred))
    r2 = r2_score(y, y_pred)
    
    # MAPE (handle division by zero)
    mask = y != 0
    if mask.sum() > 0:
        mape = np.mean(np.abs((y[mask] - y_pred[mask]) / y[mask])) * 100
    else:
        mape = np.nan
    
    # Correlation
    correlation = np.corrcoef(y, y_pred)[0, 1]
    
    metrics = {
        "mae": float(mae),
        "rmse": float(rmse),
        "mape": float(mape) if not np.isnan(mape) else None,
        "r2": float(r2),
        "correlation": float(correlation) if not np.isnan(correlation) else None,
    }
    
    if logger:
        logger.info(f"Metrics: MAE={mae:.2f}, RMSE={rmse:.2f}, R²={r2:.3f}, Corr={correlation:.3f}")
    
    return metrics


# =============================================================================
# MODEL PERSISTENCE
# =============================================================================

def save_model(
    model: xgb.XGBRegressor,
    entity_code: str,
    output_base: Path,
    model_type: str,
    feature_names: list[str],
    metrics: Dict[str, float],
    logger: Optional[logging.Logger] = None,
) -> Path:
    """
    Save trained model and metadata.
    
    Args:
        model: Trained XGBoost model
        entity_code: Entity code
        output_base: Pipeline output base directory
        model_type: "with_posted" or "without_posted"
        feature_names: List of feature names
        metrics: Evaluation metrics
        logger: Optional logger
    
    Returns:
        Path to saved model file
    """
    model_dir = output_base / "models" / entity_code
    model_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model (XGBoost native format)
    model_path = model_dir / f"model_{model_type}.json"
    model.get_booster().save_model(str(model_path))
    
    # Save metadata
    metadata = {
        "entity_code": entity_code,
        "model_type": model_type,
        "feature_names": feature_names,
        "metrics": metrics,
        "created_at": datetime.now(ZoneInfo("UTC")).isoformat(),
        "xgb_params": model.get_params(),
    }
    
    metadata_path = model_dir / f"metadata_{model_type}.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    if logger:
        logger.info(f"Saved model: {model_path}")
        logger.info(f"Saved metadata: {metadata_path}")
    
    return model_path


def load_model(
    entity_code: str,
    output_base: Path,
    model_type: str,
) -> Tuple[xgb.XGBRegressor, Dict]:
    """
    Load trained model and metadata.
    
    Args:
        entity_code: Entity code
        output_base: Pipeline output base directory
        model_type: "with_posted" or "without_posted"
    
    Returns:
        Tuple of (model, metadata)
    """
    if xgb is None:
        raise ImportError("XGBoost not installed. Install with: pip install xgboost")
    
    model_dir = output_base / "models" / entity_code
    model_path = model_dir / f"model_{model_type}.json"
    metadata_path = model_dir / f"metadata_{model_type}.json"
    
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    
    # Load model
    model = xgb.XGBRegressor()
    model.load_model(str(model_path))
    
    # Load metadata
    if metadata_path.exists():
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    else:
        metadata = {}
    
    return model, metadata


# =============================================================================
# MAIN TRAINING FUNCTION
# =============================================================================

def train_entity_model(
    df: pd.DataFrame,
    entity_code: str,
    output_base: Path,
    train_ratio: float = 0.7,
    val_ratio: float = 0.15,
    xgb_params: Optional[Dict] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[Dict[str, xgb.XGBRegressor], Dict[str, Dict[str, float]]]:
    """
    Train both with-POSTED and without-POSTED models for an entity.
    
    Args:
        df: DataFrame with features and encoded categoricals
        entity_code: Entity code
        output_base: Pipeline output base directory
        train_ratio: Training set proportion (default: 0.7)
        val_ratio: Validation set proportion (default: 0.15)
        xgb_params: Optional XGBoost parameters
        logger: Optional logger
    
    Returns:
        Tuple of (models_dict, metrics_dict)
        models_dict: {"with_posted": model, "without_posted": model}
        metrics_dict: {"with_posted": metrics, "without_posted": metrics}
    """
    if xgb is None:
        raise ImportError("XGBoost not installed. Install with: pip install xgboost")
    
    if logger:
        logger.info(f"Training models for entity: {entity_code}")
    
    # Ensure park_date exists
    if "park_date" not in df.columns:
        from processors.features import add_park_date
        df = add_park_date(df)
    
    # Split by date
    train_df, val_df, test_df = split_by_date(df, train_ratio, val_ratio)
    
    if logger:
        logger.info(f"Split: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")
    
    models = {}
    all_metrics = {}
    
    # Train with-POSTED model
    try:
        if logger:
            logger.info("Training with-POSTED model...")
        
        X_train, y_train, feature_names = prepare_training_data(
            train_df,
            include_posted=True,
            logger=logger,
        )
        X_val, y_val, _ = prepare_training_data(
            val_df,
            include_posted=True,
            logger=logger,
        )
        X_test, y_test, _ = prepare_training_data(
            test_df,
            include_posted=True,
            logger=logger,
        )
        
        model_with = train_xgb_model(
            X_train, y_train, X_val, y_val,
            params=xgb_params,
            logger=logger,
        )
        
        # Evaluate on test set
        test_metrics = evaluate_model(model_with, X_test, y_test, logger)
        
        # Save model
        save_model(
            model_with,
            entity_code,
            output_base,
            "with_posted",
            feature_names,
            test_metrics,
            logger,
        )
        
        models["with_posted"] = model_with
        all_metrics["with_posted"] = test_metrics
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to train with-POSTED model: {e}")
        models["with_posted"] = None
        all_metrics["with_posted"] = {}
    
    # Train without-POSTED model
    try:
        if logger:
            logger.info("Training without-POSTED model...")
        
        X_train, y_train, feature_names = prepare_training_data(
            train_df,
            include_posted=False,
            logger=logger,
        )
        X_val, y_val, _ = prepare_training_data(
            val_df,
            include_posted=False,
            logger=logger,
        )
        X_test, y_test, _ = prepare_training_data(
            test_df,
            include_posted=False,
            logger=logger,
        )
        
        model_without = train_xgb_model(
            X_train, y_train, X_val, y_val,
            params=xgb_params,
            logger=logger,
        )
        
        # Evaluate on test set
        test_metrics = evaluate_model(model_without, X_test, y_test, logger)
        
        # Save model
        save_model(
            model_without,
            entity_code,
            output_base,
            "without_posted",
            feature_names,
            test_metrics,
            logger,
        )
        
        models["without_posted"] = model_without
        all_metrics["without_posted"] = test_metrics
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to train without-POSTED model: {e}")
        models["without_posted"] = None
        all_metrics["without_posted"] = {}
    
    return models, all_metrics
