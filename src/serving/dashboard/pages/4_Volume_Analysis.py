"""Volume Analysis Page - Trading volume and market share."""

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
    window_duration_filter,
    refresh_rate_filter,
    create_volume_chart,
    create_market_share_pie,
    volume_rankings_table,
)

st.set_page_config(
    page_title="Volume Analysis - Crypto Platform",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Volume Analysis")
st.markdown("Trading volume metrics and market share analysis")


def fetch_volume(symbol: str = None, exchange: str = None, window: str = None, limit: int = 100):
    """Fetch volume data from API."""
    try:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if exchange:
            params["exchange"] = exchange
        if window:
            params["window"] = window

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/volume",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch volume data: {e}")
        return {"data": [], "count": 0}


def fetch_volume_rankings(symbol: str, window: str = "1min"):
    """Fetch volume rankings from API."""
    try:
        params = {"symbol": symbol, "window": window}

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/volume/rankings",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"rankings": [], "total_volume": 0}
    except Exception as e:
        st.error(f"Failed to fetch rankings: {e}")
        return {"rankings": [], "total_volume": 0}


def fetch_market_share(symbol: str, window: str = "1min"):
    """Fetch market share data from API."""
    try:
        params = {"symbol": symbol, "window": window}

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/volume/market-share",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": []}
    except Exception as e:
        st.error(f"Failed to fetch market share: {e}")
        return {"data": []}


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_symbol = symbol_filter(key="vol_symbol")
    selected_exchange = exchange_filter(include_all=True, key="vol_exchange")
    selected_window = window_duration_filter(key="vol_window")
    st.divider()
    refresh_interval = refresh_rate_filter(key="vol_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=False, key="vol_auto")

st.divider()

# Volume overview
st.subheader(f"📊 {selected_symbol} Volume Overview")

rankings_data = fetch_volume_rankings(selected_symbol, selected_window)

if rankings_data.get("rankings"):
    col1, col2 = st.columns([1, 1])

    with col1:
        # Summary metrics
        total_volume = rankings_data.get("total_volume", 0)
        st.metric("Total Volume", f"{total_volume:,.4f}")

        # Rankings table
        st.markdown("**Exchange Rankings:**")
        df = pd.DataFrame(rankings_data["rankings"])
        volume_rankings_table(df, height=300)

    with col2:
        # Market share pie chart
        market_share = fetch_market_share(selected_symbol, selected_window)
        if market_share.get("data"):
            ms_df = pd.DataFrame(market_share["data"])
            fig = create_market_share_pie(ms_df, height=350)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No market share data available")
else:
    st.info("No volume rankings available. Make sure the data pipeline is running.")

st.divider()

# Volume over time chart
st.subheader("📈 Volume Over Time")

volume_data = fetch_volume(
    symbol=selected_symbol,
    exchange=selected_exchange,
    window=selected_window,
    limit=500,
)

if volume_data.get("data"):
    df = pd.DataFrame(volume_data["data"])

    # Convert timestamps
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"])

    # Chart options
    col1, col2 = st.columns([3, 1])
    with col2:
        stacked = st.checkbox("Stacked bars", value=True, key="vol_stacked")

    fig = create_volume_chart(
        df,
        symbol=selected_symbol,
        stacked=stacked,
        height=400,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No volume chart data available")

st.divider()

# Buy/Sell breakdown
st.subheader("🔄 Buy vs Sell Volume")

if volume_data.get("data"):
    df = pd.DataFrame(volume_data["data"])

    # Aggregate buy/sell
    if "buy_volume" in df.columns and "sell_volume" in df.columns:
        total_buy = df["buy_volume"].sum()
        total_sell = df["sell_volume"].sum()
        total = total_buy + total_sell

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Buy Volume", f"{total_buy:,.4f}")
            if total > 0:
                st.progress(total_buy / total)
        with col2:
            st.metric("Sell Volume", f"{total_sell:,.4f}")
            if total > 0:
                st.progress(total_sell / total)
        with col3:
            ratio = (total_buy / total_sell) if total_sell > 0 else 0
            st.metric("Buy/Sell Ratio", f"{ratio:.2f}")
            if ratio > 1:
                st.success("↑ Buy pressure")
            elif ratio < 1:
                st.error("↓ Sell pressure")
            else:
                st.info("Balanced")
    else:
        st.info("Buy/sell breakdown not available")
else:
    st.info("No data for buy/sell analysis")

st.divider()

# Volume comparison across symbols
st.subheader("📊 Symbol Comparison")

comparison_cols = st.columns(len(DashboardConfig.TRADING_PAIRS))

for i, symbol in enumerate(DashboardConfig.TRADING_PAIRS):
    vol_data = fetch_volume(symbol=symbol, window=selected_window, limit=1)
    with comparison_cols[i]:
        if vol_data.get("data"):
            latest = vol_data["data"][0]
            st.metric(
                label=symbol,
                value=f"{latest.get('total_volume', 0):,.2f}",
                delta=f"{latest.get('num_trades', 0)} trades",
            )
        else:
            st.metric(label=symbol, value="N/A")

st.caption(f"Window: {selected_window} | Last updated: {datetime.now().strftime('%H:%M:%S')}")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
