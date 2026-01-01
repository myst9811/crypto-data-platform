"""Configuration for exchange ingestion."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load exchange configuration
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "exchanges.yaml"

with open(CONFIG_PATH, 'r') as f:
    EXCHANGE_CONFIG = yaml.safe_load(f)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_TRADES = os.getenv("KAFKA_TOPIC_RAW_TRADES", "raw-trades")
KAFKA_TOPIC_ORDERBOOK = os.getenv("KAFKA_TOPIC_RAW_ORDERBOOK", "raw-orderbook")
KAFKA_TOPIC_TICKER = os.getenv("KAFKA_TOPIC_RAW_TICKER", "raw-ticker")

# Trading pairs
TRADING_PAIRS = os.getenv("TRADING_PAIRS", "BTC/USD,ETH/USD,BNB/USD,SOL/USD,XRP/USD").split(",")

# Exchange credentials (optional)
EXCHANGE_CREDENTIALS = {
    'binance': {
        'api_key': os.getenv("BINANCE_API_KEY", ""),
        'api_secret': os.getenv("BINANCE_API_SECRET", "")
    },
    'coinbase': {
        'api_key': os.getenv("COINBASE_API_KEY", ""),
        'api_secret': os.getenv("COINBASE_API_SECRET", "")
    },
    'kraken': {
        'api_key': os.getenv("KRAKEN_API_KEY", ""),
        'api_secret': os.getenv("KRAKEN_API_SECRET", "")
    }
}


def get_exchange_config(exchange_name: str) -> Dict[str, Any]:
    """
    Get configuration for a specific exchange.

    Args:
        exchange_name: Name of the exchange (binance, coinbase, kraken)

    Returns:
        Exchange configuration dictionary
    """
    return EXCHANGE_CONFIG['exchanges'].get(exchange_name, {})


def get_symbol_mapping() -> Dict[str, Dict[str, str]]:
    """
    Get symbol mapping for all exchanges.

    Returns:
        Symbol mapping dictionary
    """
    return EXCHANGE_CONFIG.get('symbol_mapping', {})


def get_standard_pairs() -> list:
    """
    Get list of standard trading pairs.

    Returns:
        List of standard trading pairs
    """
    return EXCHANGE_CONFIG.get('standard_pairs', [])
