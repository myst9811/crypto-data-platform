"""Train Isolation Forest for anomaly detection on price data."""

import pickle
import numpy as np
import pandas as pd
import mlflow
from pathlib import Path
from sklearn.ensemble import IsolationForest

from ml.mlflow_setup import get_or_create_experiment
from ml.features.feature_store import load_feature_store
from ml.utils.safe_artifact import sign_artifact, ArtifactIntegrityError

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "isolation_forest.pkl"


def train():
    df = load_feature_store()

    # Features: spread_abs deviation from rolling mean, volume spike ratio, orderbook imbalance proxy
    df = df.sort_values("event_time").reset_index(drop=True)

    df["spread_rolling_mean"] = df["spread_abs"].rolling(window=30, min_periods=1).mean()
    df["spread_deviation"] = df["spread_abs"] - df["spread_rolling_mean"]

    # Volume spike ratio: current vol proxy / rolling mean vol
    if "rolling_vol_15s" in df.columns:
        df["vol_rolling_mean"] = df["rolling_vol_15s"].rolling(window=30, min_periods=1).mean()
        df["volume_spike_ratio"] = df["rolling_vol_15s"] / df["vol_rolling_mean"].replace(0, np.nan)
        df["volume_spike_ratio"] = df["volume_spike_ratio"].fillna(1.0)
    else:
        df["volume_spike_ratio"] = 1.0

    # Orderbook imbalance proxy (use price difference as proxy)
    df["orderbook_imbalance"] = (
        (df["price_a"] - df["price_b"]) / (df["price_a"] + df["price_b"]).replace(0, np.nan)
    ).fillna(0.0)

    feature_cols = ["spread_deviation", "volume_spike_ratio", "orderbook_imbalance"]
    X = df[feature_cols].fillna(0.0).values

    if len(X) < 10:
        print("Not enough data to train. Need at least 10 rows.")
        return

    # Chronological: fit on first 70%
    train_end = int(len(X) * 0.70)
    X_train = X[:train_end]
    X_full = X

    experiment_id = get_or_create_experiment("anomaly_detection")

    with mlflow.start_run(experiment_id=experiment_id, run_name="isolation_forest"):
        params = {
            "contamination": 0.05,
            "n_estimators": 100,
            "random_state": 42,
        }
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(X_train))

        model = IsolationForest(**params)
        model.fit(X_train)

        # Predict on full dataset
        predictions = model.predict(X_full)
        anomaly_rate = float((predictions == -1).mean())
        mlflow.log_metric("anomaly_rate", anomaly_rate)
        mlflow.log_metric("anomaly_count", int((predictions == -1).sum()))

        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)
        try:
            sign_artifact(MODEL_PATH)
        except ArtifactIntegrityError:
            print("WARN: CRYPTO_MODEL_HMAC_KEY not set; artifact left unsigned.")
        mlflow.log_artifact(str(MODEL_PATH))

        print(f"Anomaly rate: {anomaly_rate:.4f}")
        print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    train()
