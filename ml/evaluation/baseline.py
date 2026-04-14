"""Rule-based baseline for arbitrage classification."""

import numpy as np
import pandas as pd
from typing import Dict
from ml.evaluation.metrics import compute_classifier_metrics


class RuleBasedBaseline:
    """Predict 1 if spread_pct > threshold, else 0."""

    def __init__(self, threshold: float = 0.0015):
        self.threshold = threshold

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        return (df["spread_pct"] > self.threshold).astype(int).values


def evaluate_baseline(
    df: pd.DataFrame,
    threshold: float = 0.0015,
) -> Dict[str, float]:
    """Evaluate the rule-based baseline on labelled data."""
    baseline = RuleBasedBaseline(threshold=threshold)
    y_pred = baseline.predict(df)
    y_true = df["label"].values
    y_prob = df["spread_pct"].values  # use raw spread as "probability"

    return compute_classifier_metrics(y_true, y_pred, y_prob)
