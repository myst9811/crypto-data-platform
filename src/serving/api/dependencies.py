"""FastAPI dependency injection for API routes."""

from functools import lru_cache
from typing import Generator
import logging

from pyspark.sql import SparkSession

from src.serving.config import ServingConfig
from src.serving.data_access.delta_reader import DeltaReader
from src.serving.data_access.cache import DataCache, get_cache

logger = logging.getLogger(__name__)

# Global instances (singletons)
_spark_session: SparkSession = None
_delta_reader: DeltaReader = None
_cache: DataCache = None


def get_spark_session() -> SparkSession:
    """
    Get or create singleton Spark session.

    Returns:
        SparkSession instance
    """
    global _spark_session
    if _spark_session is None:
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


def get_delta_reader() -> DeltaReader:
    """
    Get or create singleton DeltaReader instance.

    Returns:
        DeltaReader instance
    """
    global _delta_reader
    if _delta_reader is None:
        logger.info("Creating new DeltaReader for API")
        _delta_reader = DeltaReader(
            spark=get_spark_session(),
            cache=get_data_cache(),
        )
    return _delta_reader


def reader_dependency() -> Generator[DeltaReader, None, None]:
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
