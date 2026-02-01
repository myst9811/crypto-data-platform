"""Data access layer for the serving module."""

from .cache import DataCache
from .models import (
    PriceData,
    VWAPData,
    VolumeData,
    LiquidityData,
    ArbitrageData,
)

# Conditional imports based on available backends
try:
    from .delta_reader import DeltaReader
    SPARK_READER_AVAILABLE = True
except ImportError:
    DeltaReader = None
    SPARK_READER_AVAILABLE = False

try:
    from .pandas_delta_reader import PandasDeltaReader
    PANDAS_READER_AVAILABLE = True
except ImportError:
    PandasDeltaReader = None
    PANDAS_READER_AVAILABLE = False

__all__ = [
    "DataCache",
    "PriceData",
    "VWAPData",
    "VolumeData",
    "LiquidityData",
    "ArbitrageData",
    "DeltaReader",
    "PandasDeltaReader",
    "SPARK_READER_AVAILABLE",
    "PANDAS_READER_AVAILABLE",
]
