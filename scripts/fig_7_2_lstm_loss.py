"""Fig 7.2 — LSTM train/validation loss curve from MLflow."""

from pathlib import Path

import matplotlib.pyplot as plt
import mlflow
from mlflow.tracking import MlflowClient

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "docs" / "figures" / "fig_7_2_lstm_loss.png"
EXPERIMENT_NAME = "price_direction_lstm"


def main() -> None:
    # Point MLflow at the repo-local tracking store
    mlflow.set_tracking_uri(f"file://{REPO_ROOT / 'mlruns'}")
    client = MlflowClient()

    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        raise SystemExit(
            f"No MLflow experiment '{EXPERIMENT_NAME}'. "
            "Run `python -m ml.training.train_lstm` first."
        )

    runs = client.search_runs(
        [exp.experiment_id], order_by=["start_time DESC"], max_results=1
    )
    if not runs:
        raise SystemExit(
            f"Experiment '{EXPERIMENT_NAME}' has no runs. "
            "Run `python -m ml.training.train_lstm` first."
        )

    run_id = runs[0].info.run_id
    train_hist = client.get_metric_history(run_id, "train_loss")
    val_hist = client.get_metric_history(run_id, "val_loss")

    if not train_hist or not val_hist:
        raise SystemExit(
            f"Run {run_id} missing train_loss/val_loss metrics. "
            "Retrain the LSTM with the latest train_lstm.py."
        )

    epochs = [m.step + 1 for m in train_hist]
    train_loss = [m.value for m in train_hist]
    val_loss = [m.value for m in val_hist]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(epochs, train_loss, marker="o", label="Train Loss", color="#2E86AB")
    ax.plot(epochs, val_loss, marker="s", label="Validation Loss", color="#E63946")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Binary Cross-Entropy Loss")
    ax.set_title("Bidirectional LSTM — Training vs. Validation Loss")
    ax.legend()
    ax.grid(alpha=0.3)
    plt.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PATH, dpi=300, bbox_inches="tight")
    print(
        f"Saved: {OUT_PATH.relative_to(REPO_ROOT)}  "
        f"(final train={train_loss[-1]:.4f}, val={val_loss[-1]:.4f})"
    )


if __name__ == "__main__":
    main()
