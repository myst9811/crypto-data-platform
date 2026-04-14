"""Configuration for the serving layer."""

import os
from pathlib import Path
from typing import List
import yaml
from dotenv import load_dotenv

load_dotenv()

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "spark_config.yaml"

with open(CONFIG_PATH, "r") as f:
    _spark_config = yaml.safe_load(f)

_dl = _spark_config.get("delta_lake", {})


class ServingConfig:
    """Configuration for the serving layer."""

    DELTA_LAKE_BASE = _dl.get("base_path", "./data")

    # Gold layer paths
    GOLD_VWAP_PATH: str = _dl.get("gold", {}).get("vwap", "./data/gold/vwap")
    GOLD_SPREADS_PATH: str = _dl.get("gold", {}).get("spreads", "./data/gold/spreads")
    GOLD_ARBITRAGE_SIGNALS_PATH: str = _dl.get("gold", {}).get(
        "arbitrage_signals", "./data/gold/arbitrage_signals"
    )
    # Legacy paths (kept for existing route compat)
    GOLD_VOLUME_PATH: str = _dl.get("gold", {}).get(
        "volume_aggregates", "./data/gold/volume_aggregates"
    )
    GOLD_LIQUIDITY_PATH: str = _dl.get("gold", {}).get(
        "liquidity_metrics", "./data/gold/liquidity_metrics"
    )
    GOLD_ARBITRAGE_PATH: str = _dl.get("gold", {}).get(
        "arbitrage_opportunities", "./data/gold/arbitrage_opportunities"
    )

    # Silver layer paths
    SILVER_PRICES_PATH: str = _dl.get("silver", {}).get("prices", "./data/silver/prices")
    SILVER_ORDERBOOK_PATH: str = _dl.get("silver", {}).get(
        "orderbook", "./data/silver/orderbook"
    )
    SILVER_TICKER_PATH: str = _dl.get("silver", {}).get(
        "ticker", "./data/silver/ticker"
    )

    TRADING_PAIRS: List[str] = ["BTC/USD", "ETH/USD", "BNB/USD", "SOL/USD", "XRP/USD"]
    EXCHANGES: List[str] = ["binance", "coinbase", "kraken"]
    WINDOW_DURATIONS: List[str] = ["1min", "5min", "15min", "1h"]

    CACHE_TTL_SECONDS: int = int(os.getenv("CACHE_TTL_SECONDS", "10"))
    CACHE_MAX_SIZE: int = int(os.getenv("CACHE_MAX_SIZE", "1000"))

    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_PREFIX: str = "/api/v1"

    DASHBOARD_HOST: str = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    DASHBOARD_PORT: int = int(os.getenv("DASHBOARD_PORT", "8501"))

    SPARK_APP_NAME: str = "crypto-api-reader"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    ARBITRAGE_MIN_PROFIT: float = float(
        _spark_config.get("arbitrage", {}).get("threshold_percent", 0.15)
    )

    @classmethod
    def get_delta_paths(cls) -> dict:
        return {
            "gold": {
                "vwap": cls.GOLD_VWAP_PATH,
                "spreads": cls.GOLD_SPREADS_PATH,
                "arbitrage_signals": cls.GOLD_ARBITRAGE_SIGNALS_PATH,
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
