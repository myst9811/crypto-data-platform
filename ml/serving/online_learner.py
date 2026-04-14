"""Online learning with River's AdaptiveRandomForestClassifier."""

import pickle
from pathlib import Path
from collections import deque
from typing import Dict, Any

from river.forest import ARFClassifier

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "online_learner.pkl"


class OnlineLearner:
    """Adaptive online learner for streaming arbitrage prediction."""

    def __init__(self):
        self.model = ARFClassifier(n_models=10, seed=42)
        self._update_count = 0
        self._recent_correct = deque(maxlen=500)
        self._load_state()

    def _load_state(self):
        if MODEL_PATH.exists():
            try:
                with open(MODEL_PATH, "rb") as f:
                    state = pickle.load(f)
                self.model = state["model"]
                self._update_count = state["update_count"]
                self._recent_correct = state["recent_correct"]
            except Exception:
                pass  # start fresh

    def _save_state(self):
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        with open(MODEL_PATH, "wb") as f:
            pickle.dump({
                "model": self.model,
                "update_count": self._update_count,
                "recent_correct": self._recent_correct,
            }, f)

    def learn_one(self, features: Dict[str, Any], label: int):
        """Update the model with a single observation."""
        pred = self.model.predict_one(features)
        self._recent_correct.append(int(pred == label))

        self.model.learn_one(features, label)
        self._update_count += 1

        if self._update_count % 1000 == 0:
            self._save_state()

    def predict_one(self, features: Dict[str, Any]) -> int:
        """Predict class for a single observation."""
        return self.model.predict_one(features)

    @property
    def accuracy(self) -> float:
        """Rolling accuracy over last 500 samples."""
        if not self._recent_correct:
            return 0.0
        return sum(self._recent_correct) / len(self._recent_correct)
