"""Fig 7.4 — GARCH(1,1) volatility (ETH/USD): realised vs. fitted + 30-step forecast."""

import pickle
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from ml.features.feature_extractor import load_silver_prices  # noqa: E402

MODEL_PATH = REPO_ROOT / "ml" / "artifacts" / "garch_ETH_USD.pkl"
OUT_PATH = REPO_ROOT / "docs" / "figures" / "fig_7_4_garch_eth.png"
SYMBOL = "ETH/USD"
FORECAST_HORIZON = 30


def main() -> None:
    with MODEL_PATH.open("rb") as f:
        result = pickle.load(f)

    prices = load_silver_prices()
    eth = (
        prices[prices["symbol"] == SYMBOL]
        .sort_values("event_time")
        .reset_index(drop=True)
    )
    eth["log_return"] = np.log(eth["price"] / eth["price"].shift(1))
    returns = (eth["log_return"].dropna() * 100).reset_index(drop=True)
    timestamps = pd.to_datetime(
        eth["event_time"].iloc[1 : len(returns) + 1]
    ).reset_index(drop=True)

    cond_vol = np.asarray(result.conditional_volatility)
    # Fitted series may be shorter than returns if training trimmed leading NaNs.
    n = min(len(cond_vol), len(timestamps), len(returns))
    cond_vol = cond_vol[-n:]
    ts = timestamps.iloc[-n:].reset_index(drop=True)
    r = returns.iloc[-n:].reset_index(drop=True)
    realised = r.rolling(window=60, min_periods=10).std()

    fc = result.forecast(horizon=FORECAST_HORIZON, reindex=False)
    fc_vol = np.sqrt(fc.variance.values[-1])
    fc_times = pd.date_range(ts.iloc[-1], periods=FORECAST_HORIZON + 1, freq="1s")[1:]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(ts, realised, label="Realised volatility (rolling 60s σ)",
            color="#6C757D", alpha=0.7, linewidth=1)
    ax.plot(ts, cond_vol, label="GARCH(1,1) fitted volatility",
            color="#2E86AB", linewidth=1.2)
    ax.plot(fc_times, fc_vol, label=f"{FORECAST_HORIZON}-step forecast",
            color="#E63946", linestyle="--", linewidth=1.5)
    ax.set_xlabel("Time")
    ax.set_ylabel("Volatility (% log-return σ)")
    ax.set_title(f"GARCH(1,1) Volatility — {SYMBOL}  (AIC = {result.aic:.0f})")
    ax.legend(loc="upper right")
    ax.grid(alpha=0.3)
    plt.xticks(rotation=30)
    plt.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PATH, dpi=300, bbox_inches="tight")
    print(f"Saved: {OUT_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
