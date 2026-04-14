"""Model registry helpers backed by MLflow."""

import mlflow
from typing import Dict, Any, Optional
from ml.mlflow_setup import get_or_create_experiment, TRACKING_URI

mlflow.set_tracking_uri(TRACKING_URI)


def get_latest_run(experiment_name: str) -> Optional[Dict[str, Any]]:
    """Get metrics from the latest run of an experiment."""
    exp = mlflow.get_experiment_by_name(experiment_name)
    if exp is None:
        return None

    runs = mlflow.search_runs(
        experiment_ids=[exp.experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )

    if runs.empty:
        return None

    row = runs.iloc[0]
    metrics = {k.replace("metrics.", ""): v for k, v in row.items() if k.startswith("metrics.")}
    params = {k.replace("params.", ""): v for k, v in row.items() if k.startswith("params.")}

    return {
        "run_id": row["run_id"],
        "status": row["status"],
        "metrics": metrics,
        "params": params,
    }


def get_all_latest_metrics() -> Dict[str, Dict[str, float]]:
    """Get latest metrics for all experiments."""
    results = {}
    for name in ["arbitrage_classifier", "price_direction_lstm", "anomaly_detection"]:
        run = get_latest_run(name)
        if run:
            results[name] = run["metrics"]
    return results
