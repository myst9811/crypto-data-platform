"""Arbitrage detection transformations."""

import logging
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, window, min as spark_min, max as spark_max,
    first, last, count, avg, current_timestamp, lit
)
from typing import Dict

logger = logging.getLogger(__name__)

# Exchange fee structure (maker/taker fees + withdrawal fees)
EXCHANGE_FEES: Dict[str, Dict[str, float]] = {
    'binance': {
        'maker': 0.1,  # 0.1%
        'taker': 0.1,
        'withdrawal': 0.0005  # BTC withdrawal fee
    },
    'coinbase': {
        'maker': 0.0,  # 0%
        'taker': 0.05,
        'withdrawal': 0.0
    },
    'kraken': {
        'maker': 0.16,  # 0.16%
        'taker': 0.26,
        'withdrawal': 0.00015
    }
}


def calculate_spread(df: DataFrame, window_duration: str = '10 seconds') -> DataFrame:
    """
    Calculate price spread across exchanges for each trading pair.

    Args:
        df: Input DataFrame with normalized prices
        window_duration: Time window for grouping prices

    Returns:
        DataFrame with spread calculations
    """
    logger.info(f"Calculating price spreads with window: {window_duration}")

    # Group by time window and trading pair
    windowed_df = df.groupBy(
        window(col('timestamp'), window_duration),
        col('standard_symbol')
    ).agg(
        spark_min('price').alias('min_price'),
        spark_max('price').alias('max_price'),
        avg('price').alias('avg_price'),
        count('*').alias('num_quotes')
    )

    # Calculate spread
    windowed_df = windowed_df.withColumn(
        'spread_absolute',
        col('max_price') - col('min_price')
    )

    windowed_df = windowed_df.withColumn(
        'spread_percent',
        ((col('max_price') - col('min_price')) / col('min_price')) * 100
    )

    return windowed_df


def detect_arbitrage_opportunities(
    df: DataFrame,
    threshold_percent: float = 0.5,
    min_volume: float = 1.0,
    window_duration: str = '10 seconds'
) -> DataFrame:
    """
    Detect arbitrage opportunities across exchanges.

    Algorithm:
    1. Window the stream by time and trading pair
    2. Find min price (buy exchange) and max price (sell exchange)
    3. Calculate spread percentage
    4. Calculate net profit after fees
    5. Filter opportunities above threshold

    Args:
        df: Input DataFrame with normalized prices
        threshold_percent: Minimum profit percentage to flag
        min_volume: Minimum volume required
        window_duration: Time window for grouping

    Returns:
        DataFrame with arbitrage opportunities
    """
    logger.info(f"Detecting arbitrage opportunities (threshold: {threshold_percent}%)")

    # Create time windows
    windowed = df.groupBy(
        window(col('timestamp'), window_duration),
        col('standard_symbol')
    )

    # For each window, get the min and max prices with their exchanges
    # We need to use window functions to get the exchange info
    w = Window.partitionBy(
        window(col('timestamp'), window_duration),
        col('standard_symbol')
    ).orderBy(col('price'))

    # Add row number to identify min/max
    from pyspark.sql.functions import row_number, desc

    df_with_rank = df.withColumn('rank_asc', row_number().over(w))

    w_desc = Window.partitionBy(
        window(col('timestamp'), window_duration),
        col('standard_symbol')
    ).orderBy(desc('price'))

    df_with_rank = df_with_rank.withColumn('rank_desc', row_number().over(w_desc))

    # Get buy opportunity (lowest price)
    buy_df = df_with_rank.filter(col('rank_asc') == 1).select(
        window(col('timestamp'), window_duration).alias('window'),
        col('standard_symbol').alias('trading_pair'),
        col('exchange').alias('buy_exchange'),
        col('price').alias('buy_price'),
        col('volume').alias('buy_volume')
    )

    # Get sell opportunity (highest price)
    sell_df = df_with_rank.filter(col('rank_desc') == 1).select(
        window(col('timestamp'), window_duration).alias('window'),
        col('standard_symbol').alias('trading_pair'),
        col('exchange').alias('sell_exchange'),
        col('price').alias('sell_price'),
        col('volume').alias('sell_volume')
    )

    # Join buy and sell opportunities
    arb_df = buy_df.join(
        sell_df,
        on=['window', 'trading_pair'],
        how='inner'
    )

    # Filter where buy and sell are different exchanges
    arb_df = arb_df.filter(col('buy_exchange') != col('sell_exchange'))

    # Calculate spread
    arb_df = arb_df.withColumn(
        'spread_absolute',
        col('sell_price') - col('buy_price')
    )

    arb_df = arb_df.withColumn(
        'spread_percent',
        ((col('sell_price') - col('buy_price')) / col('buy_price')) * 100
    )

    # Calculate fees and net profit
    arb_df = calculate_fees(arb_df)

    # Calculate net profit after fees
    arb_df = arb_df.withColumn(
        'net_profit_percent',
        col('spread_percent') - col('total_fees_percent')
    )

    # Filter profitable opportunities
    arb_df = arb_df.filter(
        (col('net_profit_percent') > threshold_percent) &
        (col('buy_volume') >= min_volume) &
        (col('sell_volume') >= min_volume)
    )

    # Add metadata
    arb_df = arb_df.withColumn('detection_timestamp', current_timestamp())
    arb_df = arb_df.withColumn(
        'min_volume',
        spark_min(col('buy_volume'), col('sell_volume'))
    )

    # Add recommendation
    arb_df = arb_df.withColumn(
        'recommended_action',
        col('net_profit_percent').cast('string').substr(1, 1).cast('string')
    )

    arb_df = arb_df.withColumn(
        'recommended_action',
        col('net_profit_percent').cast('string')  # Placeholder logic
    )

    arb_df = arb_df.withColumn(
        'recommended_action',
        (col('net_profit_percent') >= 1.0).cast('string')
    )

    arb_df = arb_df.withColumn(
        'recommended_action',
        col('net_profit_percent').cast('string')
    )

    # Simplified recommendation logic
    arb_df = arb_df.withColumn(
        'recommended_action',
        lit('monitor')  # Default to monitor for now
    )

    # Extract window timestamps
    arb_df = arb_df.withColumn('window_start', col('window.start'))
    arb_df = arb_df.withColumn('window_end', col('window.end'))

    # Select final columns
    arb_df = arb_df.select(
        'trading_pair',
        'buy_exchange',
        'buy_price',
        'sell_exchange',
        'sell_price',
        'spread_percent',
        'spread_absolute',
        'buy_fee_percent',
        'sell_fee_percent',
        'withdrawal_fee',
        'total_fees_percent',
        'net_profit_percent',
        'min_volume',
        'window_start',
        'window_end',
        'detection_timestamp',
        'recommended_action'
    )

    logger.info(f"Arbitrage opportunities detected")
    return arb_df


def calculate_fees(df: DataFrame) -> DataFrame:
    """
    Calculate trading and withdrawal fees for arbitrage.

    Args:
        df: DataFrame with buy_exchange and sell_exchange columns

    Returns:
        DataFrame with fee columns added
    """
    logger.info("Calculating fees")

    # Add buy exchange fees
    df = df.withColumn(
        'buy_fee_percent',
        col('buy_exchange').cast('string')
    )

    # Map exchange fees (simplified - in production use UDF with EXCHANGE_FEES dict)
    df = df.withColumn(
        'buy_fee_percent',
        col('buy_exchange')
        .substr(1, 1)  # Placeholder
    )

    # Simplified fee calculation (use actual EXCHANGE_FEES in production)
    from pyspark.sql.functions import when

    df = df.withColumn(
        'buy_fee_percent',
        when(col('buy_exchange') == 'binance', lit(0.1))
        .when(col('buy_exchange') == 'coinbase', lit(0.05))
        .when(col('buy_exchange') == 'kraken', lit(0.26))
        .otherwise(lit(0.1))
    )

    df = df.withColumn(
        'sell_fee_percent',
        when(col('sell_exchange') == 'binance', lit(0.1))
        .when(col('sell_exchange') == 'coinbase', lit(0.05))
        .when(col('sell_exchange') == 'kraken', lit(0.26))
        .otherwise(lit(0.1))
    )

    df = df.withColumn(
        'withdrawal_fee',
        when(col('sell_exchange') == 'binance', lit(0.0005))
        .when(col('sell_exchange') == 'coinbase', lit(0.0))
        .when(col('sell_exchange') == 'kraken', lit(0.00015))
        .otherwise(lit(0.0))
    )

    # Calculate total fees as percentage
    df = df.withColumn(
        'total_fees_percent',
        col('buy_fee_percent') + col('sell_fee_percent') + (col('withdrawal_fee') * 100)
    )

    return df


def filter_liquidity(df: DataFrame, min_liquidity_score: float = 0.5) -> DataFrame:
    """
    Filter arbitrage opportunities by liquidity score.

    Args:
        df: DataFrame with arbitrage opportunities
        min_liquidity_score: Minimum liquidity score required

    Returns:
        Filtered DataFrame
    """
    logger.info(f"Filtering by liquidity (min score: {min_liquidity_score})")

    # Placeholder liquidity check
    df = df.withColumn('liquidity_check_passed', lit(True))

    return df.filter(col('liquidity_check_passed') == True)
