"""Fig 7.1 — XGBoost arbitrage classifier feature importance (horizontal bar)."""

import pickle
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = REPO_ROOT / "ml" / "artifacts" / "xgboost_arbitrage.pkl"
OUT_PATH = REPO_ROOT / "docs" / "figures" / "fig_7_1_xgboost_importance.png"

# Feature order must match ml/training/train_xgboost.py:20-24
FEATURES = [
    "spread_abs", "spread_pct", "price_a", "price_b",
    "rolling_vol_15s", "rolling_vol_60s",
    "time_sin", "time_cos", "garch_forecast", "latency_ms",
]


def main() -> None:
    with MODEL_PATH.open("rb") as f:
        model = pickle.load(f)

    importance = np.asarray(model.feature_importances_)
    order = np.argsort(importance)  # ascending → largest at top of barh
    sorted_features = [FEATURES[i] for i in order]
    sorted_importance = importance[order]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(sorted_features, sorted_importance, color="#2E86AB")
    ax.set_xlabel("Feature Importance (gain)")
    ax.set_title("XGBoost Arbitrage Classifier — Feature Importance")
    for bar, val in zip(bars, sorted_importance):
        ax.text(
            val + max(sorted_importance) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}",
            va="center",
            fontsize=9,
        )
    plt.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PATH, dpi=300, bbox_inches="tight")
    print(f"Saved: {OUT_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
