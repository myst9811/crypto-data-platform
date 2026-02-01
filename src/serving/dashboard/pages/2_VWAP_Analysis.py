"""VWAP Analysis Page - Volume Weighted Average Price charts."""

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
    quick_time_range_filter,
    create_vwap_chart,
    vwap_table,
)

st.set_page_config(
    page_title="VWAP Analysis - Crypto Platform",
    page_icon="📈",
    layout="wide",
)

st.title("📈 VWAP Analysis")
st.markdown("Volume Weighted Average Price analytics with multiple time windows")


def fetch_vwap(symbol: str = None, exchange: str = None, window: str = None, limit: int = 100):
    """Fetch VWAP data from API."""
    try:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if exchange:
            params["exchange"] = exchange
        if window:
            params["window"] = window

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/vwap",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch VWAP: {e}")
        return {"data": [], "count": 0}


def fetch_vwap_history(symbol: str, start: datetime, end: datetime, window: str, exchange: str = None):
    """Fetch historical VWAP data."""
    try:
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "window": window,
        }
        if exchange:
            params["exchange"] = exchange

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/vwap/{symbol}/history",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch VWAP history: {e}")
        return {"data": [], "count": 0}


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_symbol = symbol_filter(key="vwap_symbol")
    selected_exchange = exchange_filter(include_all=True, key="vwap_exchange")
    selected_window = window_duration_filter(key="vwap_window")
    st.divider()
    start_time, end_time = quick_time_range_filter(key="vwap_time")
    st.divider()
    refresh_interval = refresh_rate_filter(key="vwap_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=False, key="vwap_auto")

st.divider()

# Latest VWAP metrics
st.subheader(f"📊 Latest {selected_symbol} VWAP")

vwap_data = fetch_vwap(
    symbol=selected_symbol,
    exchange=selected_exchange,
    window=selected_window,
    limit=10,
)

if vwap_data.get("data"):
    latest = vwap_data["data"][0] if vwap_data["data"] else None

    if latest:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("VWAP", f"${latest.get('vwap', 0):,.2f}")
        with col2:
            st.metric("Volume", f"{latest.get('total_volume', 0):,.4f}")
        with col3:
            st.metric("Trades", f"{latest.get('num_trades', 0):,}")
        with col4:
            std_dev = latest.get("std_dev_price")
            st.metric("Std Dev", f"${std_dev:.4f}" if std_dev else "N/A")

        # Price range
        min_p = latest.get("min_price")
        max_p = latest.get("max_price")
        if min_p and max_p:
            st.info(f"Price Range: ${min_p:,.2f} - ${max_p:,.2f} (Window: {latest.get('window_duration', 'N/A')})")
else:
    st.info("No VWAP data available. Make sure the data pipeline is running.")

st.divider()

# VWAP Chart
st.subheader("📈 VWAP Chart")

# Fetch more data for chart
chart_data = fetch_vwap(
    symbol=selected_symbol,
    exchange=selected_exchange,
    window=selected_window,
    limit=500,
)

if chart_data.get("data"):
    df = pd.DataFrame(chart_data["data"])

    # Convert timestamps
    for col in ["window_start", "window_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    fig = create_vwap_chart(
        df,
        symbol=selected_symbol,
        with_bands=True,
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No VWAP chart data available")

st.divider()

# VWAP Table
st.subheader("📋 VWAP Details")

if chart_data.get("data"):
    df = pd.DataFrame(chart_data["data"])
    vwap_table(df, height=400)

    st.caption(f"Showing {len(df)} records | Window: {selected_window} | Last updated: {datetime.now().strftime('%H:%M:%S')}")
else:
    st.info("No VWAP data available")

# Comparison across windows
st.divider()
st.subheader("🔄 Window Comparison")

windows = DashboardConfig.WINDOW_DURATIONS
cols = st.columns(len(windows))

for i, window in enumerate(windows):
    window_data = fetch_vwap(
        symbol=selected_symbol,
        window=window,
        limit=1,
    )
    with cols[i]:
        if window_data.get("data"):
            latest = window_data["data"][0]
            st.metric(
                label=window.upper(),
                value=f"${latest.get('vwap', 0):,.2f}",
                delta=f"{latest.get('total_volume', 0):,.2f} vol",
            )
        else:
            st.metric(label=window.upper(), value="N/A")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
