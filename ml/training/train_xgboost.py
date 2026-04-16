"""Train XGBoost arbitrage classifier."""

import pickle
import numpy as np
import pandas as pd
import mlflow
from pathlib import Path
from sklearn.metrics import classification_report
from xgboost import XGBClassifier

from ml.mlflow_setup import get_or_create_experiment
from ml.features.feature_store import load_feature_store
from ml.training.label_generator import generate_labels
from ml.evaluation.metrics import compute_classifier_metrics
from ml.utils.safe_artifact import sign_artifact, ArtifactIntegrityError

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "xgboost_arbitrage.pkl"

FEATURE_COLS = [
    "spread_abs", "spread_pct", "price_a", "price_b",
    "rolling_vol_15s", "rolling_vol_60s",
    "time_sin", "time_cos", "garch_forecast", "latency_ms",
]


def train():
    features = load_feature_store()
    df = generate_labels(features)
    df = df.dropna(subset=FEATURE_COLS + ["label"]).reset_index(drop=True)

    if len(df) < 20:
        print("Not enough data to train. Need at least 20 rows.")
        return

    # Chronological split: 70% train, 15% val, 15% test — NO shuffling
    n = len(df)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)

    train_df = df.iloc[:train_end]
    val_df = df.iloc[train_end:val_end]
    test_df = df.iloc[val_end:]

    X_train, y_train = train_df[FEATURE_COLS].values, train_df["label"].values
    X_val, y_val = val_df[FEATURE_COLS].values, val_df["label"].values
    X_test, y_test = test_df[FEATURE_COLS].values, test_df["label"].values

    # Auto scale_pos_weight
    n_neg = (y_train == 0).sum()
    n_pos = (y_train == 1).sum()
    spw = n_neg / max(n_pos, 1)

    params = {
        "n_estimators": 200,
        "max_depth": 5,
        "learning_rate": 0.05,
        "scale_pos_weight": spw,
        "use_label_encoder": False,
        "eval_metric": "logloss",
        "random_state": 42,
    }

    experiment_id = get_or_create_experiment("arbitrage_classifier")

    with mlflow.start_run(experiment_id=experiment_id, run_name="xgboost_arb"):
        mlflow.log_params(params)
        mlflow.log_param("train_size", len(train_df))
        mlflow.log_param("val_size", len(val_df))
        mlflow.log_param("test_size", len(test_df))

        model = XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )

        # Evaluate on test set
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = compute_classifier_metrics(y_test, y_pred, y_prob)
        mlflow.log_metrics(metrics)

        # Feature importance
        importance = dict(zip(FEATURE_COLS, model.feature_importances_.tolist()))
        mlflow.log_dict(importance, "feature_importance.json")

        # Save model
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)
        try:
            sign_artifact(MODEL_PATH)
        except ArtifactIntegrityError:
            print("WARN: CRYPTO_MODEL_HMAC_KEY not set; artifact left unsigned.")
        mlflow.log_artifact(str(MODEL_PATH))

        print("Classification Report (test set):")
        print(classification_report(y_test, y_pred))
        print(f"Metrics: {metrics}")
        print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    train()
