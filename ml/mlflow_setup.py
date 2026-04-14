"""MLflow experiment tracking setup."""

import mlflow
from pathlib import Path


TRACKING_URI = str(Path(__file__).parent.parent / "mlruns")
mlflow.set_tracking_uri(TRACKING_URI)

EXPERIMENTS = {
    "arbitrage_classifier": "arbitrage_classifier",
    "price_direction_lstm": "price_direction_lstm",
    "anomaly_detection": "anomaly_detection",
}


def get_or_create_experiment(name: str) -> str:
    """Get or create an MLflow experiment by name and return its ID."""
    experiment = mlflow.get_experiment_by_name(name)
    if experiment is None:
        experiment_id = mlflow.create_experiment(name)
    else:
        experiment_id = experiment.experiment_id
    return experiment_id
