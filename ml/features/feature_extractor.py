"""Extract features from Delta Lake tables for ML training."""

import numpy as np
import pandas as pd
from pathlib import Path


DATA_ROOT = Path(__file__).parent.parent.parent / "data"
FEATURE_STORE_PATH = DATA_ROOT / "features" / "feature_store.parquet"


def load_spreads() -> pd.DataFrame:
    """Load spreads from Gold Delta table."""
    spreads_path = DATA_ROOT / "gold" / "spreads"
    if not spreads_path.exists():
        raise FileNotFoundError(f"Spreads table not found at {spreads_path}")
    from deltalake import DeltaTable
    dt = DeltaTable(str(spreads_path))
    return dt.to_pandas()


def load_silver_prices() -> pd.DataFrame:
    """Load silver prices from Delta table."""
    prices_path = DATA_ROOT / "silver" / "prices"
    if not prices_path.exists():
        raise FileNotFoundError(f"Silver prices not found at {prices_path}")
    from deltalake import DeltaTable
    dt = DeltaTable(str(prices_path))
    return dt.to_pandas()


def compute_rolling_volatility(prices: pd.DataFrame) -> pd.DataFrame:
    """Compute rolling volatility at 15s and 60s windows."""
    prices = prices.sort_values("event_time")
    prices["log_return"] = np.log(prices["price"] / prices["price"].shift(1))

    # Approximate windows by count (assuming ~1 tick per second)
    prices["rolling_vol_15s"] = (
        prices["log_return"].rolling(window=15, min_periods=2).std()
    )
    prices["rolling_vol_60s"] = (
        prices["log_return"].rolling(window=60, min_periods=2).std()
    )
    return prices


def add_time_features(df: pd.DataFrame, time_col: str = "event_time") -> pd.DataFrame:
    """Add cyclical time features (sin/cos of hour)."""
    ts = pd.to_datetime(df[time_col])
    hour_frac = ts.dt.hour + ts.dt.minute / 60.0 + ts.dt.second / 3600.0
    df["time_sin"] = np.sin(hour_frac / 24.0 * 2 * np.pi)
    df["time_cos"] = np.cos(hour_frac / 24.0 * 2 * np.pi)
    return df


def extract_features() -> pd.DataFrame:
    """Build the full feature DataFrame and save to feature store.

    Columns: spread_abs, spread_pct, price_a, price_b,
             rolling_vol_15s, rolling_vol_60s,
             time_sin, time_cos, garch_forecast, latency_ms
    """
    spreads = load_spreads()
    prices = load_silver_prices()

    # Add rolling volatility from prices (aggregate across exchanges)
    vol_df = compute_rolling_volatility(prices)
    vol_agg = (
        vol_df.groupby("event_time")
        .agg(
            rolling_vol_15s=("rolling_vol_15s", "mean"),
            rolling_vol_60s=("rolling_vol_60s", "mean"),
        )
        .reset_index()
    )

    # Merge volatility onto spreads
    spreads["event_time"] = pd.to_datetime(spreads["event_time"])
    vol_agg["event_time"] = pd.to_datetime(vol_agg["event_time"])

    features = pd.merge_asof(
        spreads.sort_values("event_time"),
        vol_agg.sort_values("event_time"),
        on="event_time",
        direction="backward",
    )

    features = add_time_features(features, "event_time")

    # Placeholder columns filled later by GARCH and latency measurement
    features["garch_forecast"] = 0.0
    features["latency_ms"] = 50.0

    feature_cols = [
        "spread_abs", "spread_pct", "price_a", "price_b",
        "rolling_vol_15s", "rolling_vol_60s",
        "time_sin", "time_cos", "garch_forecast", "latency_ms",
        "event_time", "symbol", "exchange_a", "exchange_b",
    ]

    result = features[[c for c in feature_cols if c in features.columns]].copy()
    result = result.dropna(subset=["spread_abs", "spread_pct"])
    result = result.fillna(0.0)

    FEATURE_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(FEATURE_STORE_PATH, index=False)
    print(f"Feature store saved: {FEATURE_STORE_PATH}  ({len(result)} rows)")
    return result


if __name__ == "__main__":
    extract_features()
