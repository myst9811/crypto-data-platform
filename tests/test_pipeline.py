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

def test_generate_labels_uses_future_net_profit():
    """Label is 1 only when the future price pair yields a positive profit
    after fees. Uses a tiny hand-constructed frame so the expected labels
    can be computed manually.
    """
    from ml.training.label_generator import generate_labels

    # Five rows, 100ms apart, same symbol + exchange pair.
    # shift_rows will be 2 given execution_latency_ms=200 and median=100ms.
    df = pd.DataFrame({
        "event_time": pd.to_datetime([
            "2026-04-20T00:00:00.000Z",
            "2026-04-20T00:00:00.100Z",
            "2026-04-20T00:00:00.200Z",
            "2026-04-20T00:00:00.300Z",
            "2026-04-20T00:00:00.400Z",
        ]),
        "symbol":     ["BTC/USD"] * 5,
        "exchange_a": ["binance"] * 5,
        "exchange_b": ["coinbase"] * 5,
        # Future profit at row 0 uses prices from row 2 (shift=2):
        #   pa=100, pb=101 -> gross 1% -> net 1% - 0.1505% > 0 -> label 1
        # Future profit at row 1 uses prices from row 3:
        #   pa=100, pb=100.05 -> gross 0.05% -> net < 0 -> label 0
        # Future profit at row 2 uses prices from row 4:
        #   pa=100, pb=100 -> gross 0 -> net < 0 -> label 0
        "price_a":   [100.0, 100.0, 100.0, 100.0,  100.0],
        "price_b":   [100.0, 100.0, 101.0, 100.05, 100.0],
        "spread_pct":[0.0,   0.0,   0.01,  0.0005, 0.0],
        "spread_abs":[0.0,   0.0,   1.0,   0.05,   0.0],
    })

    labelled = generate_labels(df, execution_latency_ms=200)

    assert len(labelled) == 3
    assert "label" in labelled.columns
    assert set(labelled["label"].unique()).issubset({0, 1})
    assert list(labelled["label"].astype(int)) == [1, 0, 0]


def test_generate_labels_fees_can_flip_label_to_zero():
    """High-fee exchange pair: a future gross spread above the OLD spread
    threshold must still produce label=0 when fees exceed the gross gain.
    This catches a regression to the old spread-only labelling.
    """
    from ml.training.label_generator import generate_labels

    # Kraken taker fee = 0.26% per leg; round-trip taker = 0.52%;
    # withdrawals sum to ~0.0003%. Round-trip ratio ~0.005203.
    df = pd.DataFrame({
        "event_time": pd.to_datetime([
            "2026-04-20T00:00:00.000Z",
            "2026-04-20T00:00:00.100Z",
            "2026-04-20T00:00:00.200Z",
        ]),
        "symbol":     ["BTC/USD"] * 3,
        "exchange_a": ["kraken"] * 3,
        "exchange_b": ["kraken"] * 3,
        "price_a":    [100.0, 100.0, 100.0],
        # Future gross = (100.3 - 100) / 100 = 0.3%  (above old 0.0015 threshold)
        # Future net   = 0.003 - 0.005203 < 0  -> label 0 under new logic
        "price_b":    [100.0, 100.0, 100.3],
        "spread_pct": [0.0,   0.0,   0.003],
        "spread_abs": [0.0,   0.0,   0.3],
    })

    labelled = generate_labels(df, execution_latency_ms=200)

    assert int(labelled.iloc[0]["label"]) == 0


def test_generate_labels_no_direct_spread_leakage():
    """Flat current spread with a profitable FUTURE spread must still
    produce label=1 - proves the label is not a function of the
    CURRENT spread_pct feature value.
    """
    from ml.training.label_generator import generate_labels

    df = pd.DataFrame({
        "event_time": pd.to_datetime([
            "2026-04-20T00:00:00.000Z",
            "2026-04-20T00:00:00.100Z",
            "2026-04-20T00:00:00.200Z",
        ]),
        "symbol":     ["BTC/USD"] * 3,
        "exchange_a": ["binance"] * 3,
        "exchange_b": ["coinbase"] * 3,
        "price_a":    [100.0, 100.0, 100.0],
        "price_b":    [100.0, 100.0, 101.0],   # flat now, spike later
        "spread_pct": [0.0,   0.0,   0.01],    # current feature is 0 for row 0
        "spread_abs": [0.0,   0.0,   1.0],
    })

    labelled = generate_labels(df, execution_latency_ms=200)

    assert int(labelled.iloc[0]["label"]) == 1
    assert float(labelled.iloc[0]["spread_pct"]) == 0.0


# -----------------------------------------------------------------------
# Test 3b: compute_trade_profit_pct — fee-aware round-trip profit
# -----------------------------------------------------------------------

def test_compute_trade_profit_pct_direction_and_fees():
    """Profit calc must pick the correct buy/sell direction per row and
    subtract round-trip taker + withdrawal fees (percent units).
    """
    from ml.training.label_generator import compute_trade_profit_pct

    # price_a < price_b -> buy on exchange_a, sell on exchange_b.
    # gross = (101 - 100) / 100 = 0.01 (1%)
    # fees  = binance_taker(0.1) + coinbase_taker(0.05)
    #       + binance_withdrawal(0.0005) + coinbase_withdrawal(0.0)
    #       = 0.1505 % -> 0.001505 as ratio
    # net   = 0.01 - 0.001505 = 0.008495
    row = pd.Series({
        "price_a": 100.0, "price_b": 101.0,
        "exchange_a": "binance", "exchange_b": "coinbase",
    })
    assert abs(compute_trade_profit_pct(row) - 0.008495) < 1e-9

    # Reversed direction - price_a > price_b -> buy on exchange_b, sell on exchange_a.
    row_rev = pd.Series({
        "price_a": 101.0, "price_b": 100.0,
        "exchange_a": "binance", "exchange_b": "coinbase",
    })
    assert abs(compute_trade_profit_pct(row_rev) - 0.008495) < 1e-9

    # Unknown exchange -> raises (fail loud rather than silently pricing 0 fees)
    with pytest.raises(KeyError):
        compute_trade_profit_pct(pd.Series({
            "price_a": 1.0, "price_b": 2.0,
            "exchange_a": "binance", "exchange_b": "ftx",
        }))


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
