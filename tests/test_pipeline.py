"""Pipeline tests: predictor loading, feature extraction, label generation, walk-forward CV."""

import numpy as np
import pandas as pd
import pytest


# -----------------------------------------------------------------------
# Test 1: ArbitragePredictor loads without error (empty artifacts → 503 path)
# -----------------------------------------------------------------------

def test_predictor_loads_with_empty_artifacts():
    """ArbitragePredictor should initialize even when no model files exist."""
    from ml.serving.predictor import ArbitragePredictor

    predictor = ArbitragePredictor()
    # With empty/missing artifacts, is_loaded() should be False
    # but construction itself must not raise
    assert predictor is not None
    # xgboost not trained → not loaded
    if predictor.xgboost_model is None:
        assert not predictor.is_loaded()


# -----------------------------------------------------------------------
# Test 2: Feature extractor output columns
# -----------------------------------------------------------------------

def test_feature_extractor_output_columns(tmp_path):
    """Feature extractor should produce expected columns from sample data."""
    from ml.features.feature_extractor import (
        compute_rolling_volatility,
        add_time_features,
    )

    # Build a tiny sample DataFrame mimicking silver prices
    n = 100
    prices = pd.DataFrame({
        "price": np.random.uniform(30000, 31000, n),
        "volume": np.random.uniform(0.01, 1.0, n),
        "event_time": pd.date_range("2026-01-01", periods=n, freq="s"),
        "exchange": "binance",
        "symbol": "BTC/USD",
    })

    result = compute_rolling_volatility(prices)
    assert "rolling_vol_15s" in result.columns
    assert "rolling_vol_60s" in result.columns
    assert "log_return" in result.columns

    result = add_time_features(result, "event_time")
    assert "time_sin" in result.columns
    assert "time_cos" in result.columns

    # time_sin and time_cos should be in [-1, 1]
    assert result["time_sin"].between(-1, 1).all()
    assert result["time_cos"].between(-1, 1).all()


# -----------------------------------------------------------------------
# Test 3: Label generator — no future leakage
# -----------------------------------------------------------------------

def test_label_generator_no_future_leakage():
    """Labels at time T should only use data from time <= T (spread_pct shift)."""
    from ml.training.label_generator import generate_labels

    n = 200
    df = pd.DataFrame({
        "spread_pct": np.linspace(0.001, 0.003, n),
        "spread_abs": np.linspace(10, 30, n),
        "event_time": pd.date_range("2026-01-01", periods=n, freq="100ms"),
    })

    labelled = generate_labels(df, execution_latency_ms=200, spread_threshold=0.0015)

    # labelled should have fewer rows than input (tail rows dropped)
    assert len(labelled) < n

    # The label column should exist and only be 0 or 1
    assert "label" in labelled.columns
    assert set(labelled["label"].unique()).issubset({0, 1})

    # Key check: the last rows of the original df should be missing
    # (because we can't look ahead from them)
    original_last_time = df["event_time"].iloc[-1]
    labelled_last_time = labelled["event_time"].iloc[-1]
    assert labelled_last_time < original_last_time


# -----------------------------------------------------------------------
# Test 4: Walk-forward splits are chronological and non-overlapping
# -----------------------------------------------------------------------

def test_walk_forward_splits_chronological():
    """Walk-forward splits must be chronological and non-overlapping."""
    from ml.evaluation.walk_forward_cv import walk_forward_split

    n = 500
    df = pd.DataFrame({
        "value": np.arange(n),
        "time": pd.date_range("2026-01-01", periods=n, freq="s"),
    })

    splits = walk_forward_split(df, n_splits=5)
    assert len(splits) > 0

    for train_idx, val_idx, test_idx in splits:
        # Train comes before val
        assert train_idx[-1] < val_idx[0], "Train must end before val starts"

        # Val comes before test
        assert val_idx[-1] < test_idx[0], "Val must end before test starts"

        # No overlap
        train_set = set(train_idx)
        val_set = set(val_idx)
        test_set = set(test_idx)
        assert train_set.isdisjoint(val_set), "Train and val must not overlap"
        assert val_set.isdisjoint(test_set), "Val and test must not overlap"
        assert train_set.isdisjoint(test_set), "Train and test must not overlap"

    # Expanding window: each subsequent fold should have more training data
    if len(splits) >= 2:
        assert len(splits[1][0]) > len(splits[0][0]), \
            "Walk-forward should use expanding train window"
