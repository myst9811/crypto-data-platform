"""Stream processing module for crypto market data."""

from .schemas import (
    TRADE_SCHEMA,
    ORDERBOOK_SCHEMA,
    TICKER_SCHEMA,
    NORMALIZED_PRICE_SCHEMA,
    ARBITRAGE_SCHEMA,
    VWAP_SCHEMA,
    VOLUME_AGGREGATE_SCHEMA,
    LIQUIDITY_METRIC_SCHEMA
)

from .spark_streaming import CryptoStreamingApp

__all__ = [
    'TRADE_SCHEMA',
    'ORDERBOOK_SCHEMA',
    'TICKER_SCHEMA',
    'NORMALIZED_PRICE_SCHEMA',
    'ARBITRAGE_SCHEMA',
    'VWAP_SCHEMA',
    'VOLUME_AGGREGATE_SCHEMA',
    'LIQUIDITY_METRIC_SCHEMA',
    'CryptoStreamingApp',
]
