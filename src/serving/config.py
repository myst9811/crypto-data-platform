"""Configuration for the serving layer."""

import os
from pathlib import Path
from typing import List
import yaml
from dotenv import load_dotenv

load_dotenv()

# Load spark config for Delta Lake paths
CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "spark_config.yaml"

with open(CONFIG_PATH, "r") as f:
    _spark_config = yaml.safe_load(f)


class ServingConfig:
    """Configuration for the serving layer."""

    # Delta Lake paths from spark_config.yaml
    DELTA_LAKE_BASE = _spark_config["delta_lake"]["base_path"]

    # Gold layer paths (analytics data)
    GOLD_VWAP_PATH: str = _spark_config["delta_lake"]["gold"]["vwap"]
    GOLD_VOLUME_PATH: str = _spark_config["delta_lake"]["gold"]["volume_aggregates"]
    GOLD_LIQUIDITY_PATH: str = _spark_config["delta_lake"]["gold"]["liquidity_metrics"]
    GOLD_ARBITRAGE_PATH: str = _spark_config["delta_lake"]["gold"]["arbitrage_opportunities"]

    # Silver layer paths (normalized data)
    SILVER_PRICES_PATH: str = _spark_config["delta_lake"]["silver"]["normalized_prices"]
    SILVER_ORDERBOOK_PATH: str = _spark_config["delta_lake"]["silver"]["orderbook"]
    SILVER_TICKER_PATH: str = _spark_config["delta_lake"]["silver"]["ticker"]

    # Supported trading pairs and exchanges
    TRADING_PAIRS: List[str] = ["BTC/USD", "ETH/USD", "BNB/USD", "SOL/USD", "XRP/USD"]
    EXCHANGES: List[str] = ["binance", "coinbase", "kraken"]

    # Window durations for aggregations
    WINDOW_DURATIONS: List[str] = ["1min", "5min", "15min", "1h"]

    # Cache configuration
    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "10"))
    CACHE_MAX_SIZE: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))

    # API configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_PREFIX: str = "/api/v1"

    # Dashboard configuration
    DASHBOARD_HOST: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    DASHBOARD_PORT: int = int(os.getenv("DASHBOARD_PORT", "8501"))

    # Spark configuration for reader
    SPARK_APP_NAME: str = "crypto-api-reader"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    # Arbitrage thresholds
    ARBITRAGE_MIN_PROFIT: float = float(
        _spark_config.get("arbitrage", {}).get("threshold_percent", 0.5)
    )

    @classmethod
    def get_delta_paths(cls) -> dict:
        """Get all Delta Lake paths as a dictionary."""
        return {
            "gold": {
                "vwap": cls.GOLD_VWAP_PATH,
                "volume": cls.GOLD_VOLUME_PATH,
                "liquidity": cls.GOLD_LIQUIDITY_PATH,
                "arbitrage": cls.GOLD_ARBITRAGE_PATH,
            },
            "silver": {
                "prices": cls.SILVER_PRICES_PATH,
                "orderbook": cls.SILVER_ORDERBOOK_PATH,
                "ticker": cls.SILVER_TICKER_PATH,
            },
        }
