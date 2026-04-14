"""Train bidirectional LSTM for price direction prediction."""

import numpy as np
import pandas as pd
import mlflow
import torch
import torch.nn as nn
from pathlib import Path
from torch.utils.data import Dataset, DataLoader

from ml.mlflow_setup import get_or_create_experiment
from ml.features.feature_extractor import load_silver_prices
from ml.evaluation.metrics import compute_regression_metrics

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "lstm_price_direction.pt"

SEQUENCE_LEN = 60
FEATURES = ["price", "volume", "spread", "rolling_vol", "time_sin", "time_cos"]
HORIZON_SECONDS = 30


class PriceDirectionDataset(Dataset):
    def __init__(self, sequences: np.ndarray, labels: np.ndarray):
        self.X = torch.tensor(sequences, dtype=torch.float32)
        self.y = torch.tensor(labels, dtype=torch.float32)

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


class PriceDirectionLSTM(nn.Module):
    def __init__(self, input_size=6, hidden_size=64, num_layers=2, dropout=0.2):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            bidirectional=True,
            dropout=dropout,
            batch_first=True,
        )
        self.fc = nn.Linear(hidden_size * 2, 1)
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        out, _ = self.lstm(x)
        out = out[:, -1, :]  # last timestep
        return self.sigmoid(self.fc(out)).squeeze(-1)


def prepare_data():
    """Prepare sequences and labels from silver prices."""
    prices = load_silver_prices()
    prices = prices.sort_values("event_time").reset_index(drop=True)

    # Compute needed features
    prices["log_return"] = np.log(prices["price"] / prices["price"].shift(1))
    prices["rolling_vol"] = prices["log_return"].rolling(15, min_periods=2).std()

    # Spread proxy: difference from rolling mean
    prices["spread"] = prices["price"] - prices["price"].rolling(30, min_periods=1).mean()

    # Time features
    ts = pd.to_datetime(prices["event_time"])
    hour_frac = ts.dt.hour + ts.dt.minute / 60.0
    prices["time_sin"] = np.sin(hour_frac / 24.0 * 2 * np.pi)
    prices["time_cos"] = np.cos(hour_frac / 24.0 * 2 * np.pi)

    prices = prices.dropna().reset_index(drop=True)

    feature_cols = ["price", "volume", "spread", "rolling_vol", "time_sin", "time_cos"]
    data = prices[feature_cols].values

    # Normalise features
    means = data.mean(axis=0)
    stds = data.std(axis=0)
    stds[stds == 0] = 1.0
    data = (data - means) / stds

    # Label: price direction at T+30s, discard small moves (<0.05%)
    # Approximate shift by count (assuming ~1 tick/sec)
    shift = HORIZON_SECONDS
    future_price = prices["price"].shift(-shift)
    pct_change = (future_price - prices["price"]) / prices["price"]

    # Build sequences
    sequences, labels = [], []
    for i in range(SEQUENCE_LEN, len(data) - shift):
        change = pct_change.iloc[i]
        if pd.isna(change) or abs(change) < 0.0005:
            continue
        seq = data[i - SEQUENCE_LEN: i]
        label = 1.0 if change > 0 else 0.0
        sequences.append(seq)
        labels.append(label)

    return np.array(sequences), np.array(labels)


def train():
    sequences, labels = prepare_data()

    if len(sequences) < 50:
        print(f"Not enough data ({len(sequences)} sequences). Need at least 50.")
        return

    # Walk-forward: 70/15/15
    n = len(sequences)
    train_end = int(n * 0.70)
    val_end = int(n * 0.85)

    X_train, y_train = sequences[:train_end], labels[:train_end]
    X_val, y_val = sequences[train_end:val_end], labels[train_end:val_end]
    X_test, y_test = sequences[val_end:], labels[val_end:]

    train_ds = PriceDirectionDataset(X_train, y_train)
    val_ds = PriceDirectionDataset(X_val, y_val)
    test_ds = PriceDirectionDataset(X_test, y_test)

    train_dl = DataLoader(train_ds, batch_size=64, shuffle=False)
    val_dl = DataLoader(val_ds, batch_size=64)
    test_dl = DataLoader(test_ds, batch_size=64)

    device = torch.device("cpu")
    model = PriceDirectionLSTM(input_size=6, hidden_size=64, num_layers=2, dropout=0.2)
    model = model.to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    criterion = nn.BCELoss()

    experiment_id = get_or_create_experiment("price_direction_lstm")

    with mlflow.start_run(experiment_id=experiment_id, run_name="lstm_direction"):
        mlflow.log_params({
            "sequence_len": SEQUENCE_LEN,
            "hidden_size": 64,
            "num_layers": 2,
            "bidirectional": True,
            "dropout": 0.2,
            "horizon_seconds": HORIZON_SECONDS,
            "train_size": len(train_ds),
            "val_size": len(val_ds),
            "test_size": len(test_ds),
        })

        best_val_loss = float("inf")
        epochs = 30

        for epoch in range(epochs):
            # Train
            model.train()
            train_loss = 0.0
            for X_batch, y_batch in train_dl:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                optimizer.zero_grad()
                pred = model(X_batch)
                loss = criterion(pred, y_batch)
                loss.backward()
                optimizer.step()
                train_loss += loss.item() * len(y_batch)
            train_loss /= len(train_ds)

            # Validate
            model.eval()
            val_loss = 0.0
            with torch.no_grad():
                for X_batch, y_batch in val_dl:
                    X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                    pred = model(X_batch)
                    val_loss += criterion(pred, y_batch).item() * len(y_batch)
            val_loss /= len(val_ds)

            mlflow.log_metrics(
                {"train_loss": train_loss, "val_loss": val_loss},
                step=epoch,
            )

            if val_loss < best_val_loss:
                best_val_loss = val_loss

            if (epoch + 1) % 10 == 0:
                print(f"Epoch {epoch+1}/{epochs}  train_loss={train_loss:.4f}  val_loss={val_loss:.4f}")

        # Evaluate on test
        model.eval()
        all_preds, all_labels = [], []
        with torch.no_grad():
            for X_batch, y_batch in test_dl:
                preds = model(X_batch.to(device)).cpu().numpy()
                all_preds.extend(preds)
                all_labels.extend(y_batch.numpy())

        all_preds = np.array(all_preds)
        all_labels = np.array(all_labels)

        dir_pred = (all_preds > 0.5).astype(float)
        dir_acc = float(np.mean(dir_pred == all_labels))
        rmse = float(np.sqrt(np.mean((all_preds - all_labels) ** 2)))

        mlflow.log_metric("directional_accuracy", dir_acc)
        mlflow.log_metric("rmse", rmse)

        # Save model
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        torch.save(model.state_dict(), MODEL_PATH)
        mlflow.log_artifact(str(MODEL_PATH))

        print(f"Test directional accuracy: {dir_acc:.4f}")
        print(f"Test RMSE: {rmse:.4f}")
        print(f"Model saved to {MODEL_PATH}")


if __name__ == "__main__":
    train()
