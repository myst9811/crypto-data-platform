"""ArbitragePredictor — loads all models and runs inference pipeline."""

import pickle
import numpy as np
import torch
from pathlib import Path
from typing import Dict, Any, Optional

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"


class ArbitragePredictor:
    """Loads trained models and produces arbitrage predictions.

    Pipeline order:
      1. IsolationForest anomaly check
      2. GARCH volatility forecast
      3. LSTM direction prediction
      4. XGBoost arbitrage probability
    """

    def __init__(self):
        self.isolation_forest = self._load_pickle("isolation_forest.pkl")
        self.xgboost_model = self._load_pickle("xgboost_arbitrage.pkl")
        self.garch_models: Dict[str, Any] = {}
        self.lstm_model = None
        self._load_garch_models()
        self._load_lstm()

    @staticmethod
    def _load_pickle(filename: str) -> Optional[Any]:
        path = ARTIFACTS_DIR / filename
        if path.exists():
            with open(path, "rb") as f:
                return pickle.load(f)
        return None

    def _load_garch_models(self):
        for p in ARTIFACTS_DIR.glob("garch_*.pkl"):
            symbol = p.stem.replace("garch_", "").replace("_", "/")
            with open(p, "rb") as f:
                self.garch_models[symbol] = pickle.load(f)

    def _load_lstm(self):
        from ml.training.train_lstm import PriceDirectionLSTM
        path = ARTIFACTS_DIR / "lstm_price_direction.pt"
        if path.exists():
            model = PriceDirectionLSTM(input_size=6, hidden_size=64, num_layers=2)
            model.load_state_dict(torch.load(path, map_location="cpu"))
            model.eval()
            self.lstm_model = model

    def is_loaded(self) -> bool:
        """Check if at least the core models are loaded."""
        return self.xgboost_model is not None

    def predict(self, features_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Run the full prediction pipeline.

        Args:
            features_dict: Dict with keys matching FEATURE_COLS
                           plus 'symbol' for GARCH lookup.

        Returns:
            {anomaly: bool, garch_vol: float, lstm_direction: int, arb_probability: float}
        """
        result: Dict[str, Any] = {
            "anomaly": False,
            "garch_vol": 0.0,
            "lstm_direction": 0,
            "arb_probability": 0.0,
        }

        # Step 1: Isolation Forest anomaly check
        if self.isolation_forest is not None:
            iso_features = np.array([[
                features_dict.get("spread_deviation", 0.0),
                features_dict.get("volume_spike_ratio", 1.0),
                features_dict.get("orderbook_imbalance", 0.0),
            ]])
            pred = self.isolation_forest.predict(iso_features)
            if pred[0] == -1:
                result["anomaly"] = True
                result["arb_probability"] = 0.0
                return result

        # Step 2: GARCH forecast
        symbol = features_dict.get("symbol", "")
        if symbol in self.garch_models:
            try:
                garch_result = self.garch_models[symbol]
                forecast = garch_result.forecast(horizon=1)
                result["garch_vol"] = float(forecast.variance.values[-1, 0])
            except Exception:
                result["garch_vol"] = 0.0

        # Step 3: LSTM direction
        if self.lstm_model is not None:
            seq = features_dict.get("lstm_sequence")  # shape (60, 6)
            if seq is not None:
                tensor = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)
                with torch.no_grad():
                    prob = self.lstm_model(tensor).item()
                result["lstm_direction"] = 1 if prob > 0.5 else 0

        # Step 4: XGBoost arbitrage probability
        if self.xgboost_model is not None:
            xgb_features = np.array([[
                features_dict.get("spread_abs", 0.0),
                features_dict.get("spread_pct", 0.0),
                features_dict.get("price_a", 0.0),
                features_dict.get("price_b", 0.0),
                features_dict.get("rolling_vol_15s", 0.0),
                features_dict.get("rolling_vol_60s", 0.0),
                features_dict.get("time_sin", 0.0),
                features_dict.get("time_cos", 0.0),
                result["garch_vol"],
                features_dict.get("latency_ms", 50.0),
            ]])
            prob = self.xgboost_model.predict_proba(xgb_features)[:, 1]
            result["arb_probability"] = float(prob[0])

        return result
