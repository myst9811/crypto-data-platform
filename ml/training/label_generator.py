"""Generate labels for arbitrage classifier.

For each row at time T, look ahead by execution_latency_ms.
Label = 1 if spread_pct at T+latency is still > 0.0015, else 0.
Uses pandas shift on time-sorted data — no lookahead leakage.
"""

import pandas as pd


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
