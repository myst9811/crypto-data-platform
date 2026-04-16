"""Online learning with River's AdaptiveRandomForestClassifier."""

import logging
import os
import pickle
import tempfile
from pathlib import Path
from collections import deque
from typing import Dict, Any

from river.forest import ARFClassifier

from ml.utils.safe_artifact import safe_load_pickle, sign_artifact, ArtifactIntegrityError

ARTIFACTS_DIR = Path(__file__).parent.parent / "artifacts"
MODEL_PATH = ARTIFACTS_DIR / "online_learner.pkl"
logger = logging.getLogger(__name__)


def _validate_features(features):
    if not isinstance(features, dict):
        raise TypeError(f"features must be dict, got {type(features).__name__}")
    import math
    for k, v in features.items():
        if isinstance(v, bool):
            raise TypeError(f"feature {k!r} is bool; expected numeric")
        if not isinstance(v, (int, float)):
            raise TypeError(f"feature {k!r} must be numeric, got {type(v).__name__}")
        if isinstance(v, float) and not math.isfinite(v):
            if math.isnan(v):
                raise ValueError(f"feature {k!r} is NaN; expected finite")
            raise ValueError(f"feature {k!r} is Inf; expected finite")


def _validate_label(label):
    if isinstance(label, bool) or not isinstance(label, int):
        raise TypeError(f"label must be int, got {type(label).__name__}")
    if label not in (0, 1):
        raise ValueError(f"label must be 0 or 1, got {label}")


class OnlineLearner:
    """Adaptive online learner for streaming arbitrage prediction."""

    def __init__(self):
        self.model = ARFClassifier(n_models=10, seed=42)
        self._update_count = 0
        self._recent_correct = deque(maxlen=500)
        self._load_state()

    def _load_state(self):
        try:
            state = safe_load_pickle(MODEL_PATH)
        except ArtifactIntegrityError as e:
            logger.error("Refusing to load tampered online learner: %s", e)
            return
        if state is None:
            return
        self.model = state["model"]
        self._update_count = state["update_count"]
        self._recent_correct = state["recent_correct"]

    def _save_state(self):
        ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
        # Atomic write: pickle to temp file in same dir, then os.replace
        fd, tmp_name = tempfile.mkstemp(
            prefix=".online_learner.", suffix=".pkl.tmp", dir=str(ARTIFACTS_DIR)
        )
        try:
            with os.fdopen(fd, "wb") as f:
                pickle.dump({
                    "model": self.model,
                    "update_count": self._update_count,
                    "recent_correct": self._recent_correct,
                }, f)
            os.replace(tmp_name, MODEL_PATH)
        except Exception:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise
        try:
            sign_artifact(MODEL_PATH)
        except ArtifactIntegrityError:
            # Dev mode (no HMAC key): skip signing, loader will warn on next load
            pass

    def learn_one(self, features: Dict[str, Any], label: int):
        """Update the model with a single observation.

        Validates input to prevent adaptive-model poisoning via NaN/Inf or
        non-numeric features, and rejects non-binary labels.
        """
        _validate_features(features)
        _validate_label(label)

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
