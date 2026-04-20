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
    percentile_fallback: float = 75.0,
) -> pd.DataFrame:
    """Add a `label` column = 1 iff a round-trip arbitrage opened now would
    be profitable after fees when closed at T + latency.

    Args:
        df: Feature DataFrame with event_time, symbol, exchange_a,
            exchange_b, price_a, price_b (all required).
        execution_latency_ms: How far ahead the position is closed.
        percentile_fallback: If no row clears zero net profit (thin market
            / small sample), label the top `percentile_fallback`% of
            FUTURE net profits as 1 so training still has both classes.
            The fallback thresholds the FUTURE quantity, not any current
            feature, so it does not leak.

    Returns:
        DataFrame with `label` appended; rows whose future lookup is
        unavailable are dropped.
    """
    required = {"event_time", "symbol", "exchange_a", "exchange_b",
                "price_a", "price_b"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(
            f"generate_labels missing required columns: {sorted(missing)}"
        )

    df = df.sort_values(["symbol", "exchange_a", "exchange_b", "event_time"])
    df = df.reset_index(drop=True)

    times = pd.to_datetime(df["event_time"])
    delta_ms = times.diff().dt.total_seconds() * 1000
    median_interval = delta_ms.median()
    if pd.isna(median_interval) or median_interval <= 0:
        median_interval = 100.0
    shift_rows = max(1, int(round(execution_latency_ms / median_interval)))

    group = ["symbol", "exchange_a", "exchange_b"]
    df["future_price_a"] = df.groupby(group)["price_a"].shift(-shift_rows)
    df["future_price_b"] = df.groupby(group)["price_b"].shift(-shift_rows)

    df = df.dropna(subset=["future_price_a", "future_price_b"]).copy()

    df["future_net_profit"] = df.apply(
        lambda r: compute_trade_profit_pct(pd.Series({
            "price_a": r["future_price_a"],
            "price_b": r["future_price_b"],
            "exchange_a": r["exchange_a"],
            "exchange_b": r["exchange_b"],
        })),
        axis=1,
    )

    df["label"] = (df["future_net_profit"] > 0).astype(int)

    if df["label"].sum() == 0:
        adaptive = df["future_net_profit"].quantile(percentile_fallback / 100.0)
        print(
            f"[label_generator] No profitable rows at fee-net threshold. "
            f"Falling back to {percentile_fallback}th-percentile of FUTURE "
            f"net profit (= {adaptive:.6f})."
        )
        df["label"] = (df["future_net_profit"] > adaptive).astype(int)

    return df.drop(
        columns=["future_price_a", "future_price_b", "future_net_profit"]
    ).reset_index(drop=True)


if __name__ == "__main__":
    from ml.features.feature_store import load_feature_store

    features = load_feature_store()
    labelled = generate_labels(features)
    print(f"Labelled data: {len(labelled)} rows")
    print(f"Label distribution:\n{labelled['label'].value_counts()}")
