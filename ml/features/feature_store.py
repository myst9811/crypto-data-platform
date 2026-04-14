"""Feature store utilities — load cached features."""

import pandas as pd
from pathlib import Path

FEATURE_STORE_PATH = (
    Path(__file__).parent.parent.parent / "data" / "features" / "feature_store.parquet"
)


def load_feature_store() -> pd.DataFrame:
    """Load the feature store parquet file."""
    if not FEATURE_STORE_PATH.exists():
        raise FileNotFoundError(
            f"Feature store not found at {FEATURE_STORE_PATH}. "
            "Run ml/features/feature_extractor.py first."
        )
    return pd.read_parquet(FEATURE_STORE_PATH)


def get_features_for_symbol(symbol: str) -> pd.DataFrame:
    """Load features filtered by symbol."""
    df = load_feature_store()
    if "symbol" in df.columns:
        return df[df["symbol"] == symbol].copy()
    return df
