"""Evaluation metrics for classifiers and regressors."""

import numpy as np
from typing import Dict
from sklearn.metrics import (
    precision_score, recall_score, f1_score, roc_auc_score,
    mean_squared_error, mean_absolute_error,
)


def compute_classifier_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: np.ndarray | None = None,
) -> Dict[str, float]:
    """Compute precision, recall, F1, and AUC-ROC."""
    metrics = {
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
    }
    if y_prob is not None and len(np.unique(y_true)) > 1:
        metrics["auc_roc"] = float(roc_auc_score(y_true, y_prob))
    else:
        metrics["auc_roc"] = 0.0

    # Log to MLflow if there is an active run
    try:
        import mlflow
        if mlflow.active_run():
            mlflow.log_metrics(metrics)
    except Exception:
        pass

    return metrics


def compute_regression_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
) -> Dict[str, float]:
    """Compute RMSE, MAE, and directional accuracy."""
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))

    # Directional accuracy: did we predict the correct sign of change?
    direction_true = np.sign(np.diff(np.concatenate([[0], y_true])))
    direction_pred = np.sign(np.diff(np.concatenate([[0], y_pred])))
    dir_acc = float(np.mean(direction_true == direction_pred))

    metrics = {"rmse": rmse, "mae": mae, "directional_accuracy": dir_acc}

    try:
        import mlflow
        if mlflow.active_run():
            mlflow.log_metrics(metrics)
    except Exception:
        pass

    return metrics
