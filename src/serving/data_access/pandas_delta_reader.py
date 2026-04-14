"""Delta Lake reader using deltalake package (no Spark required)."""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List
import logging
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from deltalake import DeltaTable
    DELTALAKE_AVAILABLE = True
except ImportError:
    DELTALAKE_AVAILABLE = False

from src.serving.config import ServingConfig
from src.serving.data_access.cache import DataCache, get_cache
from src.serving.data_access.models import (
    PriceData,
    VWAPData,
    VolumeData,
    LiquidityData,
    ArbitrageData,
)

logger = logging.getLogger(__name__)


class PandasDeltaReader:
    """Read-only Delta Lake reader using pandas (no Spark required)."""

    def __init__(self, cache: Optional[DataCache] = None):
        """
        Initialize Delta reader.

        Args:
            cache: Optional cache instance (uses global cache if not provided)
        """
        if not DELTALAKE_AVAILABLE:
            raise ImportError(
                "deltalake package not installed. "
                "Install with: pip install deltalake"
            )

        self.cache = cache or get_cache(
            ttl=ServingConfig.CACHE_TTL_SECONDS,
            max_size=ServingConfig.CACHE_MAX_SIZE,
        )
        self.config = ServingConfig
        logger.info("PandasDeltaReader initialized (Spark-free mode)")

    def _read_delta(self, path: str) -> pd.DataFrame:
        """Read Delta table to pandas DataFrame with column harmonization."""
        try:
            dt = DeltaTable(path)
            df = dt.to_pandas()

            # Harmonize column names: new pipeline uses 'symbol'/'event_time',
            # but the API layer expects 'standard_symbol'/'timestamp'.
            if "symbol" in df.columns and "standard_symbol" not in df.columns:
                df["standard_symbol"] = df["symbol"]
            if "event_time" in df.columns and "timestamp" not in df.columns:
                df["timestamp"] = df["event_time"]

            return df
        except Exception as e:
            logger.error(f"Failed to read Delta table at {path}: {e}")
            return pd.DataFrame()

    def _table_exists(self, path: str) -> bool:
        """Check if Delta table exists."""
        try:
            DeltaTable(path)
            return True
        except Exception:
            return False

    # =====================
    # Price Data (Silver)
    # =====================

    def get_latest_prices(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 100,
    ) -> List[PriceData]:
        """Get latest prices from normalized_prices table."""
        cache_key = f"prices:latest:{symbol}:{exchange}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            logger.warning("Normalized prices table does not exist")
            return []

        df = self._read_delta(self.config.SILVER_PRICES_PATH)

        if df.empty:
            return []

        if symbol:
            df = df[df["standard_symbol"] == symbol]
        if exchange:
            df = df[df["exchange"] == exchange]

        df = df.sort_values("timestamp", ascending=False).head(limit)

        result = [PriceData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_price_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        exchange: Optional[str] = None,
        limit: int = 1000,
    ) -> List[PriceData]:
        """Get historical prices for a symbol."""
        cache_key = f"prices:history:{symbol}:{exchange}:{start}:{end}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            return []

        df = self._read_delta(self.config.SILVER_PRICES_PATH)

        if df.empty:
            return []

        df = df[
            (df["standard_symbol"] == symbol)
            & (df["timestamp"] >= start)
            & (df["timestamp"] <= end)
        ]

        if exchange:
            df = df[df["exchange"] == exchange]

        df = df.sort_values("timestamp", ascending=False).head(limit)

        result = [PriceData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_price_comparison(self, symbol: str) -> List[PriceData]:
        """Get latest prices across all exchanges for comparison."""
        cache_key = f"prices:compare:{symbol}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            return []

        df = self._read_delta(self.config.SILVER_PRICES_PATH)

        if df.empty:
            return []

        df = df[df["standard_symbol"] == symbol]

        # Get latest price per exchange
        df = df.sort_values("timestamp", ascending=False)
        df = df.groupby("exchange").first().reset_index()

        result = [PriceData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    # =====================
    # VWAP Data (Gold)
    # =====================

    def get_vwap(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        window_duration: Optional[str] = None,
        limit: int = 100,
    ) -> List[VWAPData]:
        """Get VWAP metrics."""
        cache_key = f"vwap:{symbol}:{exchange}:{window_duration}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VWAP_PATH):
            logger.warning("VWAP table does not exist")
            return []

        df = self._read_delta(self.config.GOLD_VWAP_PATH)

        if df.empty:
            return []

        if symbol:
            df = df[df["standard_symbol"] == symbol]
        if exchange:
            df = df[df["exchange"] == exchange]
        if window_duration:
            df = df[df["window_duration"] == window_duration]

        df = df.sort_values("window_end", ascending=False).head(limit)

        result = [VWAPData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_vwap_history(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        window_duration: str = "1min",
        exchange: Optional[str] = None,
    ) -> List[VWAPData]:
        """Get historical VWAP data."""
        cache_key = f"vwap:history:{symbol}:{exchange}:{window_duration}:{start}:{end}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VWAP_PATH):
            return []

        df = self._read_delta(self.config.GOLD_VWAP_PATH)

        if df.empty:
            return []

        df = df[
            (df["standard_symbol"] == symbol)
            & (df["window_duration"] == window_duration)
            & (df["window_start"] >= start)
            & (df["window_end"] <= end)
        ]

        if exchange:
            df = df[df["exchange"] == exchange]

        df = df.sort_values("window_start")

        result = [VWAPData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    # =====================
    # Volume Data (Gold)
    # =====================

    def get_volume_aggregates(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        window_duration: Optional[str] = None,
        limit: int = 100,
    ) -> List[VolumeData]:
        """Get volume aggregates."""
        cache_key = f"volume:{symbol}:{exchange}:{window_duration}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VOLUME_PATH):
            logger.warning("Volume aggregates table does not exist")
            return []

        df = self._read_delta(self.config.GOLD_VOLUME_PATH)

        if df.empty:
            return []

        if symbol:
            df = df[df["standard_symbol"] == symbol]
        if exchange:
            df = df[df["exchange"] == exchange]
        if window_duration:
            df = df[df["window_duration"] == window_duration]

        df = df.sort_values("window_end", ascending=False).head(limit)

        result = [VolumeData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_volume_rankings(
        self, symbol: str, window_duration: str = "1min"
    ) -> List[VolumeData]:
        """Get exchange rankings by volume for a symbol."""
        cache_key = f"volume:rankings:{symbol}:{window_duration}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VOLUME_PATH):
            return []

        df = self._read_delta(self.config.GOLD_VOLUME_PATH)

        if df.empty:
            return []

        df = df[
            (df["standard_symbol"] == symbol)
            & (df["window_duration"] == window_duration)
        ]

        # Get latest window for each exchange
        df = df.sort_values("window_end", ascending=False)
        df = df.groupby("exchange").first().reset_index()
        df = df.sort_values("volume_rank")

        result = [VolumeData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    # =====================
    # Liquidity Data (Gold)
    # =====================

    def get_liquidity_metrics(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 100,
    ) -> List[LiquidityData]:
        """Get liquidity metrics."""
        cache_key = f"liquidity:{symbol}:{exchange}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_LIQUIDITY_PATH):
            logger.warning("Liquidity metrics table does not exist")
            return []

        df = self._read_delta(self.config.GOLD_LIQUIDITY_PATH)

        if df.empty:
            return []

        if symbol:
            df = df[df["standard_symbol"] == symbol]
        if exchange:
            df = df[df["exchange"] == exchange]

        df = df.sort_values("timestamp", ascending=False).head(limit)

        result = [LiquidityData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_liquidity_rankings(self, symbol: str) -> List[LiquidityData]:
        """Get exchange rankings by liquidity score."""
        cache_key = f"liquidity:rankings:{symbol}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_LIQUIDITY_PATH):
            return []

        df = self._read_delta(self.config.GOLD_LIQUIDITY_PATH)

        if df.empty:
            return []

        df = df[df["standard_symbol"] == symbol]

        # Get latest metrics per exchange
        df = df.sort_values("timestamp", ascending=False)
        df = df.groupby("exchange").first().reset_index()
        df = df.sort_values("liquidity_score", ascending=False)

        result = [LiquidityData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    # =====================
    # Arbitrage Data (Gold)
    # =====================

    def get_arbitrage_opportunities(
        self,
        symbol: Optional[str] = None,
        min_profit: Optional[float] = None,
        limit: int = 100,
    ) -> List[ArbitrageData]:
        """Get arbitrage opportunities."""
        cache_key = f"arbitrage:{symbol}:{min_profit}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            logger.warning("Arbitrage opportunities table does not exist")
            return []

        df = self._read_delta(self.config.GOLD_ARBITRAGE_PATH)

        if df.empty:
            return []

        if symbol:
            df = df[df["trading_pair"] == symbol]
        if min_profit is not None:
            df = df[df["net_profit_percent"] >= min_profit]

        df = df.sort_values("detection_timestamp", ascending=False).head(limit)

        result = [ArbitrageData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_active_arbitrage(
        self, min_profit: float = 0.5, max_age_seconds: int = 60
    ) -> List[ArbitrageData]:
        """Get currently viable arbitrage opportunities."""
        cache_key = f"arbitrage:active:{min_profit}:{max_age_seconds}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            return []

        cutoff_time = datetime.now() - timedelta(seconds=max_age_seconds)

        df = self._read_delta(self.config.GOLD_ARBITRAGE_PATH)

        if df.empty:
            return []

        df = df[
            (df["net_profit_percent"] >= min_profit)
            & (df["detection_timestamp"] >= cutoff_time)
            & (df["recommended_action"] != "ignore")
        ]
        df = df.sort_values("net_profit_percent", ascending=False)

        result = [ArbitrageData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_arbitrage_history(
        self,
        start: datetime,
        end: datetime,
        symbol: Optional[str] = None,
        limit: int = 1000,
    ) -> List[ArbitrageData]:
        """Get historical arbitrage opportunities."""
        cache_key = f"arbitrage:history:{symbol}:{start}:{end}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            return []

        df = self._read_delta(self.config.GOLD_ARBITRAGE_PATH)

        if df.empty:
            return []

        df = df[
            (df["detection_timestamp"] >= start)
            & (df["detection_timestamp"] <= end)
        ]

        if symbol:
            df = df[df["trading_pair"] == symbol]

        df = df.sort_values("detection_timestamp", ascending=False).head(limit)

        result = [ArbitrageData(**row) for row in df.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    # =====================
    # Utility Methods
    # =====================

    def get_available_symbols(self) -> List[str]:
        """Get list of available trading symbols."""
        return self.config.TRADING_PAIRS

    def get_available_exchanges(self) -> List[str]:
        """Get list of available exchanges."""
        return self.config.EXCHANGES

    def get_available_windows(self) -> List[str]:
        """Get list of available window durations."""
        return self.config.WINDOW_DURATIONS

    def health_check(self) -> dict:
        """Check connectivity to Delta Lake tables."""
        return {
            "silver_prices": self._table_exists(self.config.SILVER_PRICES_PATH),
            "gold_vwap": self._table_exists(self.config.GOLD_VWAP_PATH),
            "gold_volume": self._table_exists(self.config.GOLD_VOLUME_PATH),
            "gold_liquidity": self._table_exists(self.config.GOLD_LIQUIDITY_PATH),
            "gold_arbitrage": self._table_exists(self.config.GOLD_ARBITRAGE_PATH),
        }

    def close(self) -> None:
        """Cleanup (no-op for pandas reader)."""
        logger.info("PandasDeltaReader closed")
