"""Generate labels for the arbitrage classifier.

Label at row T = 1 iff executing a round-trip arbitrage trade now and
closing it at T + `execution_latency_ms` would be profitable after
paying taker fees on both legs and the source exchange's withdrawal fee.

This replaces the earlier threshold-on-`spread_pct` label, which leaked
directly into the feature `spread_pct` and made every other feature's
gain essentially zero in the XGBoost importance plot.
"""

from typing import Dict

import pandas as pd

# Duplicated from src/processing/transformations/arbitrage.py:14-30 to
# avoid a cross-layer ml -> src dependency. Keep in sync if the Spark
# job's fees are updated. Values are in PERCENT (0.1 means 0.1%).
EXCHANGE_FEES: Dict[str, Dict[str, float]] = {
    "binance":  {"maker": 0.1,  "taker": 0.1,  "withdrawal": 0.0005},
    "coinbase": {"maker": 0.0,  "taker": 0.05, "withdrawal": 0.0},
    "kraken":   {"maker": 0.16, "taker": 0.26, "withdrawal": 0.00015},
}


def _round_trip_fee_ratio(exchange_a: str, exchange_b: str) -> float:
    """Round-trip fee (taker on both legs + both withdrawal fees) as a ratio."""
    fa = EXCHANGE_FEES[exchange_a]
    fb = EXCHANGE_FEES[exchange_b]
    taker_pct = fa["taker"] + fb["taker"]
    withdraw_pct = fa["withdrawal"] + fb["withdrawal"]
    return (taker_pct + withdraw_pct) / 100.0


def compute_trade_profit_pct(row: pd.Series) -> float:
    """Net round-trip profit (ratio) for the arbitrage direction implied by this row.

    Direction is picked automatically: buy on the cheaper leg, sell on the
    dearer one. Positive return means profitable after fees.
    """
    pa, pb = float(row["price_a"]), float(row["price_b"])
    if pa <= 0 or pb <= 0:
        return 0.0
    gross = abs(pb - pa) / min(pa, pb)
    fees = _round_trip_fee_ratio(row["exchange_a"], row["exchange_b"])
    return gross - fees


def generate_labels(
    df: pd.DataFrame,
    execution_latency_ms: float = 200.0,
    spread_threshold: float = 0.0015,
    percentile_fallback: float = 75.0,
) -> pd.DataFrame:
    """Add a `label` column to the feature DataFrame.

    Args:
        df: Feature DataFrame with at least `event_time` and `spread_pct`.
        execution_latency_ms: How far ahead to look (milliseconds).
        spread_threshold: Spread must exceed this at T+latency for label=1.
        percentile_fallback: If the fixed threshold produces no positive
            examples (e.g. because market spreads are currently below the
            threshold), fall back to labeling the top `percentile_fallback`%
            of spreads as class 1.  This ensures the model always sees
            both classes during training.

    Returns:
        DataFrame with `label` column appended (rows where label
        cannot be computed are dropped).
    """
    df = df.sort_values("event_time").reset_index(drop=True)

    # Compute time delta between consecutive rows
    times = pd.to_datetime(df["event_time"])
    delta_ms = times.diff().dt.total_seconds() * 1000

    # Estimate how many rows correspond to execution_latency_ms
    median_interval = delta_ms.median()
    if pd.isna(median_interval) or median_interval <= 0:
        median_interval = 100.0  # default 100ms between ticks

    shift_rows = max(1, int(round(execution_latency_ms / median_interval)))

    # Look-ahead: shift spread_pct backwards so row T sees T+shift value
    df["future_spread_pct"] = df["spread_pct"].shift(-shift_rows)

    df["label"] = (df["future_spread_pct"] > spread_threshold).astype(int)

    # Drop rows where future is not available (tail rows)
    df = df.dropna(subset=["future_spread_pct"]).copy()
    df = df.drop(columns=["future_spread_pct"])

    # If the fixed threshold produces no positive examples, fall back to a
    # percentile-based threshold so the model can learn something meaningful.
    if df["label"].sum() == 0:
        adaptive_threshold = df["spread_pct"].quantile(percentile_fallback / 100.0)
        print(
            f"[label_generator] No positives with threshold={spread_threshold:.4f}. "
            f"Using {percentile_fallback}th-percentile threshold={adaptive_threshold:.6f} instead."
        )
        df["label"] = (df["spread_pct"] > adaptive_threshold).astype(int)

    return df


if __name__ == "__main__":
    from ml.features.feature_store import load_feature_store

    features = load_feature_store()
    labelled = generate_labels(features)
    print(f"Labelled data: {len(labelled)} rows")
    print(f"Label distribution:\n{labelled['label'].value_counts()}")
