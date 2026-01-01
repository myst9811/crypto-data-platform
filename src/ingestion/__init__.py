"""Data ingestion layer for crypto market data."""

from .base_producer import BaseProducer
from .binance_producer import BinanceProducer
from .coinbase_producer import CoinbaseProducer
from .kraken_producer import KrakenProducer

__all__ = [
    'BaseProducer',
    'BinanceProducer',
    'CoinbaseProducer',
    'KrakenProducer',
]
