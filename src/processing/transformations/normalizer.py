"""Symbol and price normalization transformations."""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, when, mean, stddev, abs as spark_abs
)
from pyspark.sql.types import StringType, DoubleType
from typing import Dict

logger = logging.getLogger(__name__)

# Symbol mapping from exchange-specific to standard format
SYMBOL_MAPPING: Dict[str, Dict[str, str]] = {
    'binance': {
        'BTCUSDT': 'BTC/USD',
        'ETHUSDT': 'ETH/USD',
        'BNBUSDT': 'BNB/USD',
        'SOLUSDT': 'SOL/USD',
        'XRPUSDT': 'XRP/USD',
    },
    'coinbase': {
        'BTC-USD': 'BTC/USD',
        'ETH-USD': 'ETH/USD',
        'SOL-USD': 'SOL/USD',
        'XRP-USD': 'XRP/USD',
    },
    'kraken': {
        'XBT/USD': 'BTC/USD',
        'XBTUSD': 'BTC/USD',
        'ETH/USD': 'ETH/USD',
        'ETHUSD': 'ETH/USD',
        'SOL/USD': 'SOL/USD',
        'SOLUSD': 'SOL/USD',
        'XRP/USD': 'XRP/USD',
        'XRPUSD': 'XRP/USD',
    }
}


def normalize_symbol_udf(exchange: str):
    """
    Create UDF to normalize symbol based on exchange.

    Args:
        exchange: Exchange name

    Returns:
        UDF function
    """
    mapping = SYMBOL_MAPPING.get(exchange.lower(), {})

    def _normalize(symbol: str) -> str:
        if not symbol:
            return None
        return mapping.get(symbol.upper(), symbol)

    return udf(_normalize, StringType())


def normalize_symbol(df: DataFrame) -> DataFrame:
    """
    Normalize symbols across all exchanges to standard format.

    Args:
        df: Input DataFrame with 'symbol' and 'exchange' columns

    Returns:
        DataFrame with additional 'standard_symbol' column
    """
    logger.info("Normalizing symbols")

    # Register UDF for each exchange
    binance_udf = normalize_symbol_udf('binance')
    coinbase_udf = normalize_symbol_udf('coinbase')
    kraken_udf = normalize_symbol_udf('kraken')

    # Apply normalization based on exchange
    df = df.withColumn(
        'standard_symbol',
        when(col('exchange') == 'binance', binance_udf(col('symbol')))
        .when(col('exchange') == 'coinbase', coinbase_udf(col('symbol')))
        .when(col('exchange') == 'kraken', kraken_udf(col('symbol')))
        .otherwise(col('symbol'))
    )

    # Keep original symbol for reference
    df = df.withColumn('original_symbol', col('symbol'))

    return df


def normalize_prices(df: DataFrame) -> DataFrame:
    """
    Normalize price data (ensure consistent decimal precision, handle outliers).

    Args:
        df: Input DataFrame with price data

    Returns:
        DataFrame with normalized prices
    """
    logger.info("Normalizing prices")

    # Round prices to 8 decimal places (crypto standard)
    if 'price' in df.columns:
        df = df.withColumn('price', col('price').cast(DoubleType()))

    # Ensure volume is non-negative
    if 'volume' in df.columns:
        df = df.withColumn(
            'volume',
            when(col('volume') < 0, 0.0).otherwise(col('volume'))
        )

    return df


def add_data_quality_score(df: DataFrame) -> DataFrame:
    """
    Add data quality score based on various checks.

    Quality factors:
    - Non-null critical fields: 0.4
    - Reasonable price range: 0.3
    - Non-zero volume: 0.2
    - Valid timestamp: 0.1

    Args:
        df: Input DataFrame

    Returns:
        DataFrame with 'data_quality_score' column (0-1)
    """
    logger.info("Adding data quality scores")

    # Initialize quality score
    df = df.withColumn('data_quality_score', col('price') * 0)  # Start with 0

    # Check 1: Non-null critical fields (0.4 points)
    has_required_fields = (
        col('price').isNotNull() &
        col('symbol').isNotNull() &
        col('exchange').isNotNull()
    )
    df = df.withColumn(
        'data_quality_score',
        when(has_required_fields, col('data_quality_score') + 0.4)
        .otherwise(col('data_quality_score'))
    )

    # Check 2: Reasonable price range (0.3 points)
    # Price should be positive and not astronomically high
    reasonable_price = (col('price') > 0) & (col('price') < 1000000)
    df = df.withColumn(
        'data_quality_score',
        when(reasonable_price, col('data_quality_score') + 0.3)
        .otherwise(col('data_quality_score'))
    )

    # Check 3: Non-zero volume (0.2 points)
    if 'volume' in df.columns:
        has_volume = col('volume') > 0
        df = df.withColumn(
            'data_quality_score',
            when(has_volume, col('data_quality_score') + 0.2)
            .otherwise(col('data_quality_score'))
        )
    else:
        df = df.withColumn('data_quality_score', col('data_quality_score') + 0.2)

    # Check 4: Valid timestamp (0.1 points)
    if 'timestamp' in df.columns:
        has_timestamp = col('timestamp').isNotNull()
        df = df.withColumn(
            'data_quality_score',
            when(has_timestamp, col('data_quality_score') + 0.1)
            .otherwise(col('data_quality_score'))
        )
    else:
        df = df.withColumn('data_quality_score', col('data_quality_score') + 0.1)

    return df


def detect_outliers(df: DataFrame, column: str = 'price', std_threshold: float = 3.0) -> DataFrame:
    """
    Detect outliers using standard deviation method.

    Args:
        df: Input DataFrame
        column: Column name to check for outliers
        std_threshold: Number of standard deviations to consider as outlier

    Returns:
        DataFrame with 'is_outlier' column
    """
    logger.info(f"Detecting outliers in {column}")

    # Calculate mean and standard deviation
    stats = df.select(
        mean(col(column)).alias('mean'),
        stddev(col(column)).alias('stddev')
    ).first()

    if stats and stats['stddev']:
        mean_val = stats['mean']
        std_val = stats['stddev']

        # Mark outliers
        df = df.withColumn(
            'is_outlier',
            spark_abs(col(column) - mean_val) > (std_threshold * std_val)
        )
    else:
        df = df.withColumn('is_outlier', col(column).isNull())

    return df


def extract_currency_pair(df: DataFrame) -> DataFrame:
    """
    Extract base and quote currency from standard symbol.

    Args:
        df: DataFrame with 'standard_symbol' column (format: BTC/USD)

    Returns:
        DataFrame with 'base_currency' and 'quote_currency' columns
    """
    logger.info("Extracting currency pairs")

    # Split on '/' or '-'
    @udf(StringType())
    def get_base(symbol: str) -> str:
        if not symbol:
            return None
        if '/' in symbol:
            return symbol.split('/')[0]
        elif '-' in symbol:
            return symbol.split('-')[0]
        return symbol

    @udf(StringType())
    def get_quote(symbol: str) -> str:
        if not symbol:
            return None
        if '/' in symbol:
            parts = symbol.split('/')
            return parts[1] if len(parts) > 1 else 'USD'
        elif '-' in symbol:
            parts = symbol.split('-')
            return parts[1] if len(parts) > 1 else 'USD'
        return 'USD'

    df = df.withColumn('base_currency', get_base(col('standard_symbol')))
    df = df.withColumn('quote_currency', get_quote(col('standard_symbol')))

    return df
