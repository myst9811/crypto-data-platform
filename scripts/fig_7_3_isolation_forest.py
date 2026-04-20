"""Fig 7.3 — Isolation Forest anomaly-score distribution + feature scatter."""

import pickle
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ml.features.feature_store import load_feature_store  # noqa: E402

MODEL_PATH = REPO_ROOT / "ml" / "artifacts" / "isolation_forest.pkl"
OUT_PATH = REPO_ROOT / "docs" / "figures" / "fig_7_3_isolation_forest.png"


def build_features(df):
    """Reproduce feature pipeline from ml/training/train_isolation_forest.py:22-41."""
    df = df.sort_values("event_time").reset_index(drop=True)
    df["spread_rolling_mean"] = df["spread_abs"].rolling(30, min_periods=1).mean()
    df["spread_deviation"] = df["spread_abs"] - df["spread_rolling_mean"]
    df["vol_rolling_mean"] = df["rolling_vol_15s"].rolling(30, min_periods=1).mean()
    df["volume_spike_ratio"] = (
        df["rolling_vol_15s"] / df["vol_rolling_mean"].replace(0, np.nan)
    ).fillna(1.0)
    df["orderbook_imbalance"] = (
        (df["price_a"] - df["price_b"]) / (df["price_a"] + df["price_b"]).replace(0, np.nan)
    ).fillna(0.0)
    return df[["spread_deviation", "volume_spike_ratio", "orderbook_imbalance"]].fillna(0.0).values


def main() -> None:
    with MODEL_PATH.open("rb") as f:
        model = pickle.load(f)

    df = load_feature_store()
    X = build_features(df)
    scores = model.decision_function(X)
    preds = model.predict(X)
    anomaly_rate = float((preds == -1).mean())

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.hist(scores[preds == 1], bins=50, alpha=0.7, label="Normal", color="#2E86AB")
    ax1.hist(scores[preds == -1], bins=50, alpha=0.7, label="Anomaly", color="#E63946")
    ax1.axvline(0, color="black", linestyle="--", linewidth=1, label="Decision boundary")
    ax1.set_xlabel("Anomaly Score (decision_function)")
    ax1.set_ylabel("Count")
    ax1.set_title(f"(a) Score distribution — anomaly rate = {anomaly_rate:.2%}")
    ax1.legend()

    ax2.scatter(
        X[preds == 1, 0], X[preds == 1, 2],
        s=8, alpha=0.5, c="#2E86AB", label="Normal",
    )
    ax2.scatter(
        X[preds == -1, 0], X[preds == -1, 2],
        s=20, alpha=0.9, c="#E63946", label="Anomaly",
    )
    ax2.set_xlabel("Spread deviation (absolute)")
    ax2.set_ylabel("Orderbook imbalance proxy")
    ax2.set_title("(b) Feature space — normal vs. anomalous points")
    ax2.legend()
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PATH, dpi=300, bbox_inches="tight")
    print(f"Saved: {OUT_PATH.relative_to(REPO_ROOT)}  (anomaly rate = {anomaly_rate:.4f})")


if __name__ == "__main__":
    main()
