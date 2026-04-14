"""FastAPI dependency injection for API routes."""

from functools import lru_cache
from typing import Generator, Union
import logging
import os

from src.serving.config import ServingConfig
from src.serving.data_access.cache import DataCache, get_cache

logger = logging.getLogger(__name__)

# Check for available backends
# Always prefer the lightweight deltalake (pandas) reader for the API.
# PySpark is reserved for the Spark Streaming job in a separate process.
PYSPARK_AVAILABLE = False  # disabled for API — avoids JVM conflicts
DELTALAKE_AVAILABLE = False

try:
    from deltalake import DeltaTable
    DELTALAKE_AVAILABLE = True
except ImportError:
    logger.info("deltalake package not available")

if not DELTALAKE_AVAILABLE:
    try:
        from pyspark.sql import SparkSession
        PYSPARK_AVAILABLE = True
    except ImportError:
        logger.info("PySpark not available either")

# Import appropriate reader
if DELTALAKE_AVAILABLE:
    from src.serving.data_access.pandas_delta_reader import PandasDeltaReader as DeltaReader
    logger.info("Using pandas-based PandasDeltaReader (lightweight, no JVM)")
elif PYSPARK_AVAILABLE:
    from src.serving.data_access.delta_reader import DeltaReader
    logger.info("Using PySpark-based DeltaReader")
else:
    DeltaReader = None
    logger.warning(
        "No Delta Lake reader available. "
        "Install either pyspark or deltalake package."
    )

# Type alias for reader
ReaderType = Union["DeltaReader", None]

# Global instances (singletons)
_spark_session = None
_delta_reader: ReaderType = None
_cache: DataCache = None


def get_spark_session():
    """
    Get or create singleton Spark session.
    Returns None if PySpark is not available.
    """
    global _spark_session

    if not PYSPARK_AVAILABLE:
        return None

    if _spark_session is None:
        from pyspark.sql import SparkSession

        logger.info("Creating new Spark session for API")
        _spark_session = (
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
            .getOrCreate()
        )
    return _spark_session


def get_data_cache() -> DataCache:
    """
    Get or create singleton cache instance.

    Returns:
        DataCache instance
    """
    global _cache
    if _cache is None:
        _cache = get_cache(
            ttl=ServingConfig.CACHE_TTL_SECONDS,
            max_size=ServingConfig.CACHE_MAX_SIZE,
        )
    return _cache


def get_delta_reader() -> ReaderType:
    """
    Get or create singleton DeltaReader instance.
    Automatically selects PySpark or pandas-based reader.

    Returns:
        DeltaReader or PandasDeltaReader instance
    """
    global _delta_reader

    if DeltaReader is None:
        raise RuntimeError(
            "No Delta Lake reader available. "
            "Install pyspark (pip install -r requirements/spark.txt) "
            "or deltalake (pip install deltalake)"
        )

    if _delta_reader is None:
        logger.info("Creating new DeltaReader for API")
        cache = get_data_cache()

        if PYSPARK_AVAILABLE:
            # Use Spark-based reader
            _delta_reader = DeltaReader(
                spark=get_spark_session(),
                cache=cache,
            )
        else:
            # Use pandas-based reader
            _delta_reader = DeltaReader(cache=cache)

    return _delta_reader


def reader_dependency() -> Generator[ReaderType, None, None]:
    """
    FastAPI dependency for DeltaReader.

    Yields:
        DeltaReader instance
    """
    yield get_delta_reader()


def cache_dependency() -> Generator[DataCache, None, None]:
    """
    FastAPI dependency for DataCache.

    Yields:
        DataCache instance
    """
    yield get_data_cache()


def shutdown():
    """Cleanup resources on shutdown."""
    global _spark_session, _delta_reader, _cache

    if _delta_reader:
        _delta_reader.close()
        _delta_reader = None

    if _spark_session:
        _spark_session.stop()
        _spark_session = None

    if _cache:
        _cache.clear()
        _cache = None

    logger.info("API dependencies cleaned up")


def get_backend_info() -> dict:
    """Get information about the current backend."""
    return {
        "pyspark_available": PYSPARK_AVAILABLE,
        "deltalake_available": DELTALAKE_AVAILABLE,
        "active_backend": "pyspark" if PYSPARK_AVAILABLE else (
            "deltalake" if DELTALAKE_AVAILABLE else "none"
        ),
    }
