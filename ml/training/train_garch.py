"""Train GARCH(1,1) volatility models per symbol."""

import pickle
import warnings
import numpy as np
import pandas as pd
import mlflow
from pathlib import Path
from arch import arch_model

from ml.mlflow_setup import get_or_create_experiment
from ml.features.feature_extractor import load_silver_prices

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"


def train():
    prices = load_silver_prices()
    prices = prices.sort_values("event_time")

    experiment_id = get_or_create_experiment("anomaly_detection")

    symbols = prices["symbol"].unique()
    print(f"Training GARCH models for {len(symbols)} symbols")

    for symbol in symbols:
        sym_prices = prices[prices["symbol"] == symbol].copy()
        sym_prices = sym_prices.sort_values("event_time").reset_index(drop=True)

        if len(sym_prices) < 50:
            print(f"Skipping {symbol}: only {len(sym_prices)} rows")
            continue

        # Compute log returns
        sym_prices["log_return"] = np.log(
            sym_prices["price"] / sym_prices["price"].shift(1)
        )
        returns = sym_prices["log_return"].dropna().values

        # Scale returns to avoid convergence issues
        returns = returns * 100

        if len(returns) < 30:
            continue

        with mlflow.start_run(experiment_id=experiment_id,
                              run_name=f"garch_{symbol}"):
            mlflow.log_param("symbol", symbol)
            mlflow.log_param("n_observations", len(returns))

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    model = arch_model(returns, vol="GARCH", p=1, q=1)
                    result = model.fit(disp="off")

                aic = float(result.aic)
                bic = float(result.bic)
                mlflow.log_metric("aic", aic)
                mlflow.log_metric("bic", bic)

                model_path = ARTIFACTS_DIR / f"garch_{symbol.replace('/', '_')}.pkl"
                ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
                with open(model_path, "wb") as f:
                    pickle.dump(result, f)
                mlflow.log_artifact(str(model_path))

                print(f"{symbol}: AIC={aic:.2f}, BIC={bic:.2f}")

            except Exception as e:
                print(f"{symbol}: GARCH fit failed — {e}")
                mlflow.log_param("error", str(e))


if __name__ == "__main__":
    train()
