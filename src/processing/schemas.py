"""Spark schema definitions for crypto market data."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    ArrayType,
    IntegerType,
    BooleanType
)

# Raw trade schema from Kafka
TRADE_SCHEMA = StructType([
    StructField("type", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("timestamp", LongType(), True),  # Unix timestamp
    StructField("side", StringType(), True),  # buy/sell
    StructField("trade_id", LongType(), True),
    StructField("exchange", StringType(), False),
    StructField("ingestion_timestamp", StringType(), True),  # ISO format
    StructField("raw_message", StringType(), True)  # JSON string of original message
])

# Order book level schema (for nested structure)
ORDER_LEVEL_SCHEMA = ArrayType(
    StructType([
        StructField("price", DoubleType(), False),
        StructField("volume", DoubleType(), False)
    ])
)

# Order book schema from Kafka
ORDERBOOK_SCHEMA = StructType([
    StructField("type", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("bids", ORDER_LEVEL_SCHEMA, True),
    StructField("asks", ORDER_LEVEL_SCHEMA, True),
    StructField("timestamp", LongType(), True),  # Unix timestamp
    StructField("exchange", StringType(), False),
    StructField("snapshot", BooleanType(), True),  # Full snapshot vs incremental update
    StructField("last_update_id", LongType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("raw_message", StringType(), True)
])

# Ticker schema from Kafka
TICKER_SCHEMA = StructType([
    StructField("type", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("last_price", DoubleType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("quote_volume", DoubleType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("price_change_percent", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("exchange", StringType(), False),
    StructField("best_bid", DoubleType(), True),
    StructField("best_ask", DoubleType(), True),
    StructField("vwap", DoubleType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("raw_message", StringType(), True)
])

# Normalized price schema (Silver layer)
NORMALIZED_PRICE_SCHEMA = StructType([
    StructField("standard_symbol", StringType(), False),  # BTC/USD format
    StructField("exchange", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", DoubleType(), False),
    StructField("side", StringType(), True),
    StructField("timestamp", TimestampType(), False),  # Converted to timestamp
    StructField("processing_timestamp", TimestampType(), False),
    StructField("data_quality_score", DoubleType(), True),  # 0-1 score
    StructField("is_outlier", BooleanType(), True),
    StructField("original_symbol", StringType(), True),  # Original exchange symbol
    StructField("price_usd", DoubleType(), True),  # Normalized to USD
    StructField("base_currency", StringType(), True),
    StructField("quote_currency", StringType(), True)
])

# Arbitrage opportunity schema (Gold layer)
ARBITRAGE_SCHEMA = StructType([
    StructField("trading_pair", StringType(), False),
    StructField("buy_exchange", StringType(), False),
    StructField("buy_price", DoubleType(), False),
    StructField("sell_exchange", StringType(), False),
    StructField("sell_price", DoubleType(), False),
    StructField("spread_percent", DoubleType(), False),
    StructField("spread_absolute", DoubleType(), False),
    StructField("estimated_profit_percent", DoubleType(), False),
    StructField("buy_fee_percent", DoubleType(), True),
    StructField("sell_fee_percent", DoubleType(), True),
    StructField("withdrawal_fee", DoubleType(), True),
    StructField("net_profit_percent", DoubleType(), True),
    StructField("min_volume", DoubleType(), True),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("detection_timestamp", TimestampType(), False),
    StructField("liquidity_check_passed", BooleanType(), True),
    StructField("recommended_action", StringType(), True)  # "execute", "monitor", "ignore"
])

# VWAP schema (Gold layer)
VWAP_SCHEMA = StructType([
    StructField("standard_symbol", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("vwap", DoubleType(), False),  # Volume Weighted Average Price
    StructField("total_volume", DoubleType(), False),
    StructField("total_value", DoubleType(), False),  # price * volume
    StructField("num_trades", LongType(), False),
    StructField("window_duration", StringType(), False),  # "1min", "5min", "15min", "1h"
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("min_price", DoubleType(), True),
    StructField("max_price", DoubleType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("std_dev_price", DoubleType(), True)
])

# Volume aggregate schema (Gold layer)
VOLUME_AGGREGATE_SCHEMA = StructType([
    StructField("standard_symbol", StringType(), False),
    StructField("exchange", StringType(), True),  # Null for cross-exchange aggregates
    StructField("total_volume", DoubleType(), False),
    StructField("buy_volume", DoubleType(), True),
    StructField("sell_volume", DoubleType(), True),
    StructField("num_trades", LongType(), False),
    StructField("window_duration", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), False),
    StructField("volume_rank", IntegerType(), True),  # Rank by volume
    StructField("exchange_market_share", DoubleType(), True),  # % of total volume
])

# Liquidity metrics schema (Gold layer)
LIQUIDITY_METRIC_SCHEMA = StructType([
    StructField("standard_symbol", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("bid_price", DoubleType(), False),
    StructField("ask_price", DoubleType(), False),
    StructField("spread_absolute", DoubleType(), False),
    StructField("spread_percent", DoubleType(), False),
    StructField("bid_depth", DoubleType(), True),  # Total volume in top N bids
    StructField("ask_depth", DoubleType(), True),  # Total volume in top N asks
    StructField("total_depth", DoubleType(), True),
    StructField("depth_imbalance", DoubleType(), True),  # (bid_depth - ask_depth) / total_depth
    StructField("liquidity_score", DoubleType(), True),  # Custom score: volume / spread
    StructField("orderbook_levels", IntegerType(), True),  # Number of price levels
    StructField("timestamp", TimestampType(), False),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True)
])

# Kafka raw message schema (for reading from Kafka)
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), False),  # JSON string
    StructField("topic", StringType(), False),
    StructField("partition", IntegerType(), False),
    StructField("offset", LongType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("timestampType", IntegerType(), False)
])


def get_schema_by_type(data_type: str) -> StructType:
    """
    Get Spark schema by data type name.

    Args:
        data_type: Type of data (trade, orderbook, ticker, etc.)

    Returns:
        Corresponding StructType schema

    Raises:
        ValueError: If data_type is not recognized
    """
    schemas = {
        'trade': TRADE_SCHEMA,
        'orderbook': ORDERBOOK_SCHEMA,
        'ticker': TICKER_SCHEMA,
        'normalized_price': NORMALIZED_PRICE_SCHEMA,
        'arbitrage': ARBITRAGE_SCHEMA,
        'vwap': VWAP_SCHEMA,
        'volume_aggregate': VOLUME_AGGREGATE_SCHEMA,
        'liquidity_metric': LIQUIDITY_METRIC_SCHEMA,
        'kafka_message': KAFKA_MESSAGE_SCHEMA
    }

    if data_type not in schemas:
        raise ValueError(
            f"Unknown data type: {data_type}. "
            f"Available types: {', '.join(schemas.keys())}"
        )

    return schemas[data_type]
