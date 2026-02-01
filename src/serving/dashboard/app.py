"""Streamlit dashboard main application."""

import streamlit as st
import requests
from datetime import datetime

# Page configuration must be first Streamlit command
st.set_page_config(
    page_title="Crypto Data Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.serving.dashboard.config import DashboardConfig
from src.serving.dashboard.components.metrics import kpi_card, status_indicator


def check_api_health() -> dict:
    """Check API health status."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/health/ready",
            timeout=5,
        )
        if response.status_code == 200:
            return response.json()
        return {"status": "unhealthy", "components": {}}
    except Exception as e:
        return {"status": "error", "error": str(e), "components": {}}


def main():
    """Main dashboard application."""
    # Title and description
    st.title("📊 Crypto Data Platform Dashboard")
    st.markdown(
        """
        Real-time cryptocurrency market analytics powered by Apache Spark and Delta Lake.

        **Data Sources:** Binance | Coinbase | Kraken
        """
    )

    # Divider
    st.divider()

    # System Status Section
    st.subheader("System Status")

    with st.spinner("Checking system health..."):
        health = check_api_health()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        api_status = health.get("status", "unknown")
        status_indicator(
            "API Status",
            api_status in ["ready", "healthy", "alive"],
            "Online",
            api_status.title(),
        )

    with col2:
        components = health.get("components", {})
        silver_ok = components.get("silver_prices", False)
        status_indicator("Silver Layer", silver_ok)

    with col3:
        gold_ok = any([
            components.get("gold_vwap", False),
            components.get("gold_volume", False),
            components.get("gold_liquidity", False),
            components.get("gold_arbitrage", False),
        ])
        status_indicator("Gold Layer", gold_ok)

    with col4:
        st.markdown(f"**Last Updated:** {datetime.now().strftime('%H:%M:%S')}")

    st.divider()

    # Quick Stats Section
    st.subheader("Quick Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        kpi_card(
            title="Trading Pairs",
            value=len(DashboardConfig.TRADING_PAIRS),
            subtitle="Monitored symbols",
            icon="💱",
        )

    with col2:
        kpi_card(
            title="Exchanges",
            value=len(DashboardConfig.EXCHANGES),
            subtitle="Data sources",
            icon="🏦",
        )

    with col3:
        kpi_card(
            title="Window Sizes",
            value=len(DashboardConfig.WINDOW_DURATIONS),
            subtitle="Aggregation windows",
            icon="⏱️",
        )

    with col4:
        kpi_card(
            title="Data Freshness",
            value="10s",
            subtitle="Update interval",
            icon="🔄",
        )

    st.divider()

    # Navigation Info
    st.subheader("📂 Dashboard Pages")

    st.markdown(
        """
        Navigate using the sidebar to explore detailed analytics:

        | Page | Description |
        |------|-------------|
        | **1. Price Monitor** | Real-time prices across exchanges |
        | **2. VWAP Analysis** | Volume Weighted Average Price charts |
        | **3. Arbitrage Alerts** | Cross-exchange profit opportunities |
        | **4. Volume Analysis** | Trading volume and market share |
        | **5. Liquidity Depth** | Order book depth and spreads |
        | **6. Exchange Comparison** | Side-by-side exchange metrics |
        """
    )

    st.divider()

    # Available Trading Pairs
    st.subheader("📈 Monitored Trading Pairs")

    pairs_col1, pairs_col2, pairs_col3, pairs_col4, pairs_col5 = st.columns(5)
    for i, pair in enumerate(DashboardConfig.TRADING_PAIRS):
        with [pairs_col1, pairs_col2, pairs_col3, pairs_col4, pairs_col5][i % 5]:
            st.info(pair)

    # Available Exchanges
    st.subheader("🏦 Connected Exchanges")

    for exchange in DashboardConfig.EXCHANGES:
        color = DashboardConfig.get_exchange_color(exchange)
        name = DashboardConfig.get_exchange_name(exchange)
        st.markdown(
            f"<span style='color:{color}; font-weight:bold;'>● {name}</span>",
            unsafe_allow_html=True,
        )

    st.divider()

    # Footer
    st.caption(
        f"Crypto Data Platform v1.0.0 | "
        f"API: {DashboardConfig.API_BASE_URL} | "
        f"Dashboard refreshes every {DashboardConfig.DEFAULT_REFRESH_INTERVAL}s"
    )


if __name__ == "__main__":
    main()
