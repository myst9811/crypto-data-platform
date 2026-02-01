"""Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations."""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List
import logging
import pandas as pd

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from src.utils.delta_utils import DeltaLakeManager
from src.serving.config import ServingConfig
from src.serving.data_access.cache import DataCache, cached, get_cache
from src.serving.data_access.models import (
    PriceData,
    VWAPData,
    VolumeData,
    LiquidityData,
    ArbitrageData,
)

logger = logging.getLogger(__name__)


class DeltaReader:
    """Read-only Delta Lake reader for API and Dashboard."""

    def __init__(self, spark: Optional[SparkSession] = None, cache: Optional[DataCache] = None):
        """
        Initialize Delta reader.

        Args:
            spark: Optional Spark session (creates one if not provided)
            cache: Optional cache instance (uses global cache if not provided)
        """
        self.spark = spark or self._create_spark_session()
        self.manager = DeltaLakeManager(self.spark)
        self.cache = cache or get_cache(
            ttl=ServingConfig.CACHE_TTL_SECONDS,
            max_size=ServingConfig.CACHE_MAX_SIZE,
        )
        self.config = ServingConfig
        logger.info("DeltaReader initialized")

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for reading."""
        return (
            SparkSession.builder.appName(ServingConfig.SPARK_APP_NAME)
            .master(ServingConfig.SPARK_MASTER)
            .config(
                "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .getOrCreate()
        )

    def _to_pandas(self, df: DataFrame) -> pd.DataFrame:
        """Convert Spark DataFrame to Pandas."""
        return df.toPandas()

    def _table_exists(self, path: str) -> bool:
        """Check if Delta table exists."""
        return self.manager.table_exists(path)

    # =====================
    # Price Data (Silver)
    # =====================

    def get_latest_prices(
        self,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 100,
    ) -> List[PriceData]:
        """
        Get latest prices from normalized_prices table.

        Args:
            symbol: Filter by symbol (e.g., "BTC/USD")
            exchange: Filter by exchange
            limit: Maximum records to return

        Returns:
            List of PriceData objects
        """
        cache_key = f"prices:latest:{symbol}:{exchange}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            logger.warning("Normalized prices table does not exist")
            return []

        df = self.manager.read_from_delta(self.config.SILVER_PRICES_PATH)

        if symbol:
            df = df.filter(F.col("standard_symbol") == symbol)
        if exchange:
            df = df.filter(F.col("exchange") == exchange)

        df = df.orderBy(F.col("timestamp").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [PriceData(**row) for row in pdf.to_dict("records")]
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
        """
        Get historical prices for a symbol.

        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            exchange: Optional exchange filter
            limit: Maximum records

        Returns:
            List of PriceData objects
        """
        cache_key = f"prices:history:{symbol}:{exchange}:{start}:{end}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            return []

        df = self.manager.read_from_delta(self.config.SILVER_PRICES_PATH)
        df = df.filter(
            (F.col("standard_symbol") == symbol)
            & (F.col("timestamp") >= start)
            & (F.col("timestamp") <= end)
        )

        if exchange:
            df = df.filter(F.col("exchange") == exchange)

        df = df.orderBy(F.col("timestamp").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [PriceData(**row) for row in pdf.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_price_comparison(self, symbol: str) -> List[PriceData]:
        """
        Get latest prices across all exchanges for comparison.

        Args:
            symbol: Trading symbol

        Returns:
            List of latest PriceData per exchange
        """
        cache_key = f"prices:compare:{symbol}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.SILVER_PRICES_PATH):
            return []

        df = self.manager.read_from_delta(self.config.SILVER_PRICES_PATH)
        df = df.filter(F.col("standard_symbol") == symbol)

        # Get latest price per exchange
        from pyspark.sql.window import Window

        window = Window.partitionBy("exchange").orderBy(F.col("timestamp").desc())
        df = df.withColumn("row_num", F.row_number().over(window))
        df = df.filter(F.col("row_num") == 1).drop("row_num")

        pdf = self._to_pandas(df)
        result = [PriceData(**row) for row in pdf.to_dict("records")]
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
        """
        Get VWAP metrics.

        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            window_duration: Filter by window (1min, 5min, 15min, 1h)
            limit: Maximum records

        Returns:
            List of VWAPData objects
        """
        cache_key = f"vwap:{symbol}:{exchange}:{window_duration}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VWAP_PATH):
            logger.warning("VWAP table does not exist")
            return []

        df = self.manager.read_from_delta(self.config.GOLD_VWAP_PATH)

        if symbol:
            df = df.filter(F.col("standard_symbol") == symbol)
        if exchange:
            df = df.filter(F.col("exchange") == exchange)
        if window_duration:
            df = df.filter(F.col("window_duration") == window_duration)

        df = df.orderBy(F.col("window_end").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [VWAPData(**row) for row in pdf.to_dict("records")]
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
        """
        Get historical VWAP data.

        Args:
            symbol: Trading symbol
            start: Start datetime
            end: End datetime
            window_duration: Window duration
            exchange: Optional exchange filter

        Returns:
            List of VWAPData objects
        """
        cache_key = f"vwap:history:{symbol}:{exchange}:{window_duration}:{start}:{end}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VWAP_PATH):
            return []

        df = self.manager.read_from_delta(self.config.GOLD_VWAP_PATH)
        df = df.filter(
            (F.col("standard_symbol") == symbol)
            & (F.col("window_duration") == window_duration)
            & (F.col("window_start") >= start)
            & (F.col("window_end") <= end)
        )

        if exchange:
            df = df.filter(F.col("exchange") == exchange)

        df = df.orderBy(F.col("window_start"))
        pdf = self._to_pandas(df)

        result = [VWAPData(**row) for row in pdf.to_dict("records")]
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
        """
        Get volume aggregates.

        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            window_duration: Filter by window duration
            limit: Maximum records

        Returns:
            List of VolumeData objects
        """
        cache_key = f"volume:{symbol}:{exchange}:{window_duration}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VOLUME_PATH):
            logger.warning("Volume aggregates table does not exist")
            return []

        df = self.manager.read_from_delta(self.config.GOLD_VOLUME_PATH)

        if symbol:
            df = df.filter(F.col("standard_symbol") == symbol)
        if exchange:
            df = df.filter(F.col("exchange") == exchange)
        if window_duration:
            df = df.filter(F.col("window_duration") == window_duration)

        df = df.orderBy(F.col("window_end").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [VolumeData(**row) for row in pdf.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_volume_rankings(
        self, symbol: str, window_duration: str = "1min"
    ) -> List[VolumeData]:
        """
        Get exchange rankings by volume for a symbol.

        Args:
            symbol: Trading symbol
            window_duration: Window duration

        Returns:
            List of VolumeData sorted by rank
        """
        cache_key = f"volume:rankings:{symbol}:{window_duration}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_VOLUME_PATH):
            return []

        df = self.manager.read_from_delta(self.config.GOLD_VOLUME_PATH)

        # Get latest window for each exchange
        from pyspark.sql.window import Window

        window = Window.partitionBy("exchange").orderBy(F.col("window_end").desc())

        df = df.filter(
            (F.col("standard_symbol") == symbol)
            & (F.col("window_duration") == window_duration)
        )
        df = df.withColumn("row_num", F.row_number().over(window))
        df = df.filter(F.col("row_num") == 1).drop("row_num")
        df = df.orderBy(F.col("volume_rank"))

        pdf = self._to_pandas(df)
        result = [VolumeData(**row) for row in pdf.to_dict("records")]
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
        """
        Get liquidity metrics.

        Args:
            symbol: Filter by symbol
            exchange: Filter by exchange
            limit: Maximum records

        Returns:
            List of LiquidityData objects
        """
        cache_key = f"liquidity:{symbol}:{exchange}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_LIQUIDITY_PATH):
            logger.warning("Liquidity metrics table does not exist")
            return []

        df = self.manager.read_from_delta(self.config.GOLD_LIQUIDITY_PATH)

        if symbol:
            df = df.filter(F.col("standard_symbol") == symbol)
        if exchange:
            df = df.filter(F.col("exchange") == exchange)

        df = df.orderBy(F.col("timestamp").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [LiquidityData(**row) for row in pdf.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_liquidity_rankings(self, symbol: str) -> List[LiquidityData]:
        """
        Get exchange rankings by liquidity score.

        Args:
            symbol: Trading symbol

        Returns:
            List of LiquidityData sorted by liquidity_score desc
        """
        cache_key = f"liquidity:rankings:{symbol}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_LIQUIDITY_PATH):
            return []

        df = self.manager.read_from_delta(self.config.GOLD_LIQUIDITY_PATH)

        # Get latest metrics per exchange
        from pyspark.sql.window import Window

        window = Window.partitionBy("exchange").orderBy(F.col("timestamp").desc())

        df = df.filter(F.col("standard_symbol") == symbol)
        df = df.withColumn("row_num", F.row_number().over(window))
        df = df.filter(F.col("row_num") == 1).drop("row_num")
        df = df.orderBy(F.col("liquidity_score").desc())

        pdf = self._to_pandas(df)
        result = [LiquidityData(**row) for row in pdf.to_dict("records")]
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
        """
        Get arbitrage opportunities.

        Args:
            symbol: Filter by trading pair
            min_profit: Minimum net profit percentage
            limit: Maximum records

        Returns:
            List of ArbitrageData objects
        """
        cache_key = f"arbitrage:{symbol}:{min_profit}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            logger.warning("Arbitrage opportunities table does not exist")
            return []

        df = self.manager.read_from_delta(self.config.GOLD_ARBITRAGE_PATH)

        if symbol:
            df = df.filter(F.col("trading_pair") == symbol)
        if min_profit is not None:
            df = df.filter(F.col("net_profit_percent") >= min_profit)

        df = df.orderBy(F.col("detection_timestamp").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [ArbitrageData(**row) for row in pdf.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_active_arbitrage(
        self, min_profit: float = 0.5, max_age_seconds: int = 60
    ) -> List[ArbitrageData]:
        """
        Get currently viable arbitrage opportunities.

        Args:
            min_profit: Minimum net profit percentage
            max_age_seconds: Maximum age of opportunity in seconds

        Returns:
            List of ArbitrageData objects
        """
        cache_key = f"arbitrage:active:{min_profit}:{max_age_seconds}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            return []

        cutoff_time = datetime.now() - timedelta(seconds=max_age_seconds)

        df = self.manager.read_from_delta(self.config.GOLD_ARBITRAGE_PATH)
        df = df.filter(
            (F.col("net_profit_percent") >= min_profit)
            & (F.col("detection_timestamp") >= cutoff_time)
            & (F.col("recommended_action") != "ignore")
        )
        df = df.orderBy(F.col("net_profit_percent").desc())

        pdf = self._to_pandas(df)
        result = [ArbitrageData(**row) for row in pdf.to_dict("records")]
        self.cache.set(cache_key, result)
        return result

    def get_arbitrage_history(
        self,
        start: datetime,
        end: datetime,
        symbol: Optional[str] = None,
        limit: int = 1000,
    ) -> List[ArbitrageData]:
        """
        Get historical arbitrage opportunities.

        Args:
            start: Start datetime
            end: End datetime
            symbol: Optional trading pair filter
            limit: Maximum records

        Returns:
            List of ArbitrageData objects
        """
        cache_key = f"arbitrage:history:{symbol}:{start}:{end}:{limit}"
        cached_result = self.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

        if not self._table_exists(self.config.GOLD_ARBITRAGE_PATH):
            return []

        df = self.manager.read_from_delta(self.config.GOLD_ARBITRAGE_PATH)
        df = df.filter(
            (F.col("detection_timestamp") >= start)
            & (F.col("detection_timestamp") <= end)
        )

        if symbol:
            df = df.filter(F.col("trading_pair") == symbol)

        df = df.orderBy(F.col("detection_timestamp").desc()).limit(limit)
        pdf = self._to_pandas(df)

        result = [ArbitrageData(**row) for row in pdf.to_dict("records")]
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
        """
        Check connectivity to Delta Lake tables.

        Returns:
            Dict with table availability status
        """
        return {
            "silver_prices": self._table_exists(self.config.SILVER_PRICES_PATH),
            "gold_vwap": self._table_exists(self.config.GOLD_VWAP_PATH),
            "gold_volume": self._table_exists(self.config.GOLD_VOLUME_PATH),
            "gold_liquidity": self._table_exists(self.config.GOLD_LIQUIDITY_PATH),
            "gold_arbitrage": self._table_exists(self.config.GOLD_ARBITRAGE_PATH),
        }

    def close(self) -> None:
        """Close Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("DeltaReader Spark session closed")
