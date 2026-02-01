"""Liquidity Depth Page - Order book depth and spreads."""

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
    spread_metric,
    liquidity_table,
)

st.set_page_config(
    page_title="Liquidity Depth - Crypto Platform",
    page_icon="📚",
    layout="wide",
)

st.title("📚 Liquidity Depth")
st.markdown("Order book depth analysis and spread metrics")


def fetch_liquidity(symbol: str = None, exchange: str = None, limit: int = 100):
    """Fetch liquidity data from API."""
    try:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if exchange:
            params["exchange"] = exchange

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/liquidity",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch liquidity data: {e}")
        return {"data": [], "count": 0}


def fetch_liquidity_rankings(symbol: str):
    """Fetch liquidity rankings from API."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/liquidity/rankings",
            params={"symbol": symbol},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"rankings": [], "best_exchange": None}
    except Exception as e:
        st.error(f"Failed to fetch rankings: {e}")
        return {"rankings": [], "best_exchange": None}


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_symbol = symbol_filter(key="liq_symbol")
    selected_exchange = exchange_filter(include_all=True, key="liq_exchange")
    st.divider()
    refresh_interval = refresh_rate_filter(key="liq_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=False, key="liq_auto")

st.divider()

# Liquidity overview
st.subheader(f"📊 {selected_symbol} Liquidity Overview")

rankings_data = fetch_liquidity_rankings(selected_symbol)

if rankings_data.get("rankings"):
    rankings = rankings_data["rankings"]
    best_exchange = rankings_data.get("best_exchange")

    if best_exchange:
        st.success(f"🏆 Best Liquidity: **{DashboardConfig.get_exchange_name(best_exchange)}**")

    # Display spread metrics for each exchange
    cols = st.columns(len(rankings))
    for i, liq in enumerate(rankings):
        with cols[i]:
            spread_metric(
                exchange=liq.get("exchange", ""),
                spread_percent=liq.get("spread_percent", 0),
                spread_absolute=liq.get("spread_absolute"),
            )
            st.caption(f"Score: {liq.get('liquidity_score', 0):,.0f}")
else:
    st.info("No liquidity rankings available. Make sure the data pipeline is running.")

st.divider()

# Depth comparison
st.subheader("📈 Depth Comparison")

liquidity_data = fetch_liquidity(
    symbol=selected_symbol,
    exchange=selected_exchange,
    limit=100,
)

if liquidity_data.get("data"):
    df = pd.DataFrame(liquidity_data["data"])

    # Group by exchange for latest data
    if "exchange" in df.columns:
        latest_by_exchange = []
        for exchange in df["exchange"].unique():
            ex_data = df[df["exchange"] == exchange].iloc[0] if len(df[df["exchange"] == exchange]) > 0 else None
            if ex_data is not None:
                latest_by_exchange.append(ex_data.to_dict())

        if latest_by_exchange:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Bid Depth by Exchange:**")
                for data in latest_by_exchange:
                    exchange = data.get("exchange", "")
                    bid_depth = data.get("bid_depth", 0) or 0
                    name = DashboardConfig.get_exchange_name(exchange)
                    st.markdown(f"- **{name}**: {bid_depth:,.2f}")

            with col2:
                st.markdown("**Ask Depth by Exchange:**")
                for data in latest_by_exchange:
                    exchange = data.get("exchange", "")
                    ask_depth = data.get("ask_depth", 0) or 0
                    name = DashboardConfig.get_exchange_name(exchange)
                    st.markdown(f"- **{name}**: {ask_depth:,.2f}")

            st.divider()

            # Depth imbalance
            st.subheader("⚖️ Depth Imbalance")

            imbalance_cols = st.columns(len(latest_by_exchange))
            for i, data in enumerate(latest_by_exchange):
                with imbalance_cols[i]:
                    exchange = data.get("exchange", "")
                    imbalance = data.get("depth_imbalance", 0) or 0
                    name = DashboardConfig.get_exchange_name(exchange)

                    st.metric(name, f"{imbalance:.3f}")

                    if imbalance > 0.1:
                        st.success("↑ Buy side heavy")
                    elif imbalance < -0.1:
                        st.error("↓ Sell side heavy")
                    else:
                        st.info("Balanced")
else:
    st.info("No depth data available")

st.divider()

# Spread analysis
st.subheader("📉 Spread Analysis")

if liquidity_data.get("data"):
    df = pd.DataFrame(liquidity_data["data"])

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Current Spreads:**")
        if "exchange" in df.columns and "spread_percent" in df.columns:
            # Get latest per exchange
            for exchange in df["exchange"].unique():
                ex_data = df[df["exchange"] == exchange].iloc[0]
                spread = ex_data.get("spread_percent", 0)
                name = DashboardConfig.get_exchange_name(exchange)

                color = "green" if spread < 0.1 else "orange" if spread < 0.5 else "red"
                st.markdown(f"- **{name}**: :{color}[{spread:.4f}%]")

    with col2:
        st.markdown("**Best Bid/Ask:**")
        if "exchange" in df.columns:
            for exchange in df["exchange"].unique():
                ex_data = df[df["exchange"] == exchange].iloc[0]
                bid = ex_data.get("bid_price", 0)
                ask = ex_data.get("ask_price", 0)
                name = DashboardConfig.get_exchange_name(exchange)
                st.markdown(f"- **{name}**: ${bid:,.2f} / ${ask:,.2f}")
else:
    st.info("No spread data available")

st.divider()

# Detailed liquidity table
st.subheader("📋 Liquidity Details")

if liquidity_data.get("data"):
    df = pd.DataFrame(liquidity_data["data"])
    liquidity_table(df, height=400)

    st.caption(f"Showing {len(df)} records | Last updated: {datetime.now().strftime('%H:%M:%S')}")
else:
    st.info("No liquidity data available")

st.divider()

# Best execution recommendation
st.subheader("🎯 Best Execution")

if rankings_data.get("rankings"):
    rankings = rankings_data["rankings"]

    st.markdown(
        """
        Based on current liquidity metrics, here's the recommended execution order:

        | Rank | Exchange | Spread | Liquidity Score | Recommendation |
        |------|----------|--------|-----------------|----------------|
        """
    )

    for i, liq in enumerate(rankings[:3], 1):
        exchange = DashboardConfig.get_exchange_name(liq.get("exchange", ""))
        spread = liq.get("spread_percent", 0)
        score = liq.get("liquidity_score", 0) or 0

        if i == 1:
            rec = "✅ Best for large orders"
        elif i == 2:
            rec = "🔄 Good alternative"
        else:
            rec = "⚠️ Use for small orders only"

        st.markdown(f"| {i} | {exchange} | {spread:.4f}% | {score:,.0f} | {rec} |")
else:
    st.info("No execution recommendations available")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
