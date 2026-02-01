"""Price Monitor Page - Real-time prices across exchanges."""

import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.serving.dashboard.config import DashboardConfig
from src.serving.dashboard.components import (
    symbol_filter,
    exchange_filter,
    refresh_rate_filter,
    price_metric,
    multi_metric_row,
    create_price_chart,
    price_table,
)

st.set_page_config(
    page_title="Price Monitor - Crypto Platform",
    page_icon="💰",
    layout="wide",
)

st.title("💰 Price Monitor")
st.markdown("Real-time cryptocurrency prices across exchanges")


def fetch_prices(symbol: str = None, exchange: str = None, limit: int = 100):
    """Fetch prices from API."""
    try:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if exchange:
            params["exchange"] = exchange

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/prices",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch prices: {e}")
        return {"data": [], "count": 0}


def fetch_price_comparison(symbol: str):
    """Fetch price comparison across exchanges."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/prices/compare",
            params={"symbol": symbol},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        st.error(f"Failed to fetch comparison: {e}")
        return None


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_symbol = symbol_filter(key="price_symbol")
    selected_exchange = exchange_filter(include_all=True, key="price_exchange")
    refresh_interval = refresh_rate_filter(key="price_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=True, key="price_auto")

st.divider()

# Price comparison section
st.subheader(f"📊 {selected_symbol} Price Comparison")

comparison = fetch_price_comparison(selected_symbol)
if comparison and comparison.get("prices"):
    prices = comparison["prices"]

    # Display metrics for each exchange
    metrics = []
    for p in prices:
        metrics.append({
            "type": "price",
            "symbol": p.get("symbol", selected_symbol),
            "price": p.get("price", 0),
            "exchange": p.get("exchange"),
        })

    if metrics:
        multi_metric_row(metrics, columns=len(metrics))

    # Spread info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Min Price", f"${comparison.get('min_price', 0):,.2f}")
    with col2:
        st.metric("Max Price", f"${comparison.get('max_price', 0):,.2f}")
    with col3:
        spread = comparison.get("spread_percent")
        st.metric("Spread", f"{spread:.3f}%" if spread else "N/A")
else:
    st.info("No price comparison data available. Make sure the data pipeline is running.")

st.divider()

# Price history chart
st.subheader("📈 Price History")

prices_data = fetch_prices(
    symbol=selected_symbol,
    exchange=selected_exchange,
    limit=500,
)

if prices_data.get("data"):
    df = pd.DataFrame(prices_data["data"])

    # Convert timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Create chart
    fig = create_price_chart(
        df,
        symbol=selected_symbol,
        x_col="timestamp",
        y_col="price",
        color_col="exchange",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No price history data available")

st.divider()

# Price table
st.subheader("📋 Recent Prices")

if prices_data.get("data"):
    df = pd.DataFrame(prices_data["data"])
    price_table(df, height=400)

    # Show stats
    st.caption(f"Showing {len(df)} records | Last updated: {datetime.now().strftime('%H:%M:%S')}")
else:
    st.info("No price data available")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
