"""Walk-forward cross-validation for time-series data."""

import numpy as np
import pandas as pd
from typing import List, Tuple


def walk_forward_split(
    df: pd.DataFrame,
    n_splits: int = 5,
) -> List[Tuple[np.ndarray, np.ndarray, np.ndarray]]:
    """Split data chronologically into expanding train/val/test windows.

    Args:
        df: DataFrame (must already be sorted by time).
        n_splits: Number of walk-forward folds.

    Returns:
        List of (train_idx, val_idx, test_idx) index arrays.
    """
    n = len(df)
    splits = []

    # Each fold: expanding train, fixed-size val and test
    fold_size = n // (n_splits + 2)  # reserve space for val+test

    for i in range(n_splits):
        train_end = fold_size * (i + 2)
        val_end = train_end + fold_size
        test_end = min(val_end + fold_size, n)

        if val_end >= n or test_end <= val_end:
            break

        train_idx = np.arange(0, train_end)
        val_idx = np.arange(train_end, val_end)
        test_idx = np.arange(val_end, test_end)

        splits.append((train_idx, val_idx, test_idx))

    return splits
