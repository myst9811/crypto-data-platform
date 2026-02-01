"""Dashboard-specific configuration."""

import os
from typing import List
from src.serving.config import ServingConfig


class DashboardConfig:
    """Configuration for Streamlit dashboard."""

    # Page configuration
    PAGE_TITLE: str = "Crypto Data Platform"
    PAGE_ICON: str = "📊"
    LAYOUT: str = "wide"

    # Data settings (inherit from ServingConfig)
    TRADING_PAIRS: List[str] = ServingConfig.TRADING_PAIRS
    EXCHANGES: List[str] = ServingConfig.EXCHANGES
    WINDOW_DURATIONS: List[str] = ServingConfig.WINDOW_DURATIONS

    # Refresh settings
    DEFAULT_REFRESH_INTERVAL: int = 10  # seconds
    REFRESH_OPTIONS: List[int] = [5, 10, 30, 60]  # seconds

    # Chart settings
    DEFAULT_CHART_HEIGHT: int = 400
    DEFAULT_CHART_WIDTH: int = None  # Use container width

    # Color scheme
    COLORS = {
        "primary": "#1f77b4",
        "secondary": "#ff7f0e",
        "success": "#2ca02c",
        "danger": "#d62728",
        "warning": "#ffbb00",
        "info": "#17a2b8",
        "buy": "#2ca02c",  # Green for buy
        "sell": "#d62728",  # Red for sell
        "binance": "#f3ba2f",
        "coinbase": "#0052ff",
        "kraken": "#5741d9",
    }

    # Exchange display names
    EXCHANGE_NAMES = {
        "binance": "Binance",
        "coinbase": "Coinbase",
        "kraken": "Kraken",
    }

    # Table settings
    DEFAULT_TABLE_HEIGHT: int = 400
    MAX_TABLE_ROWS: int = 100

    # API settings (for fetching data)
    API_BASE_URL: str = os.getenv(
        "API_BASE_URL",
        f"http://localhost:{ServingConfig.API_PORT}{ServingConfig.API_PREFIX}",
    )

    # Dashboard host/port
    HOST: str = ServingConfig.DASHBOARD_HOST
    PORT: int = ServingConfig.DASHBOARD_PORT

    @classmethod
    def get_exchange_color(cls, exchange: str) -> str:
        """Get color for exchange."""
        return cls.COLORS.get(exchange.lower(), cls.COLORS["primary"])

    @classmethod
    def get_exchange_name(cls, exchange: str) -> str:
        """Get display name for exchange."""
        return cls.EXCHANGE_NAMES.get(exchange.lower(), exchange.title())
