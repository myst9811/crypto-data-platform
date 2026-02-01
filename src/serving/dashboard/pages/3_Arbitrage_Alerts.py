"""Arbitrage Alerts Page - Cross-exchange profit opportunities."""

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
    profit_threshold_filter,
    refresh_rate_filter,
    arbitrage_metric,
    create_arbitrage_chart,
    arbitrage_table,
)

st.set_page_config(
    page_title="Arbitrage Alerts - Crypto Platform",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Arbitrage Alerts")
st.markdown("Cross-exchange arbitrage opportunity detection")


def fetch_arbitrage(symbol: str = None, min_profit: float = None, limit: int = 100):
    """Fetch arbitrage opportunities from API."""
    try:
        params = {"limit": limit}
        if symbol:
            params["symbol"] = symbol
        if min_profit is not None:
            params["min_profit"] = min_profit

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/arbitrage",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"data": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch arbitrage data: {e}")
        return {"data": [], "count": 0}


def fetch_active_arbitrage(min_profit: float = 0.5, max_age: int = 60):
    """Fetch currently active arbitrage opportunities."""
    try:
        params = {
            "min_profit": min_profit,
            "max_age_seconds": max_age,
        }

        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/arbitrage/active",
            params=params,
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return {"opportunities": [], "count": 0}
    except Exception as e:
        st.error(f"Failed to fetch active arbitrage: {e}")
        return {"opportunities": [], "count": 0}


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    filter_symbol = st.checkbox("Filter by symbol", value=False, key="arb_filter_symbol")
    selected_symbol = None
    if filter_symbol:
        selected_symbol = symbol_filter(key="arb_symbol")
    min_profit = profit_threshold_filter(default=0.5, key="arb_profit")
    max_age = st.slider("Max Age (seconds)", 10, 300, 60, key="arb_age")
    st.divider()
    refresh_interval = refresh_rate_filter(default=5, key="arb_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=True, key="arb_auto")

st.divider()

# Active opportunities alert
st.subheader("🔔 Active Opportunities")

active_data = fetch_active_arbitrage(min_profit=min_profit, max_age=max_age)

if active_data.get("opportunities"):
    opportunities = active_data["opportunities"]

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Active Opportunities", len(opportunities))
    with col2:
        if opportunities:
            avg_profit = sum(o.get("net_profit_percent", 0) for o in opportunities) / len(opportunities)
            st.metric("Avg Profit", f"{avg_profit:.2f}%")
        else:
            st.metric("Avg Profit", "N/A")
    with col3:
        if opportunities:
            max_profit = max(o.get("net_profit_percent", 0) for o in opportunities)
            st.metric("Max Profit", f"{max_profit:.2f}%")
        else:
            st.metric("Max Profit", "N/A")
    with col4:
        st.metric("Threshold", f">{min_profit}%")

    st.divider()

    # Display top opportunities
    st.subheader("🏆 Top Opportunities")

    # Sort by profit
    sorted_opps = sorted(opportunities, key=lambda x: x.get("net_profit_percent", 0), reverse=True)[:5]

    for opp in sorted_opps:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            arbitrage_metric(
                buy_exchange=opp.get("buy_exchange", ""),
                sell_exchange=opp.get("sell_exchange", ""),
                profit_percent=opp.get("net_profit_percent", 0),
            )
        with col2:
            st.markdown(
                f"**{opp.get('trading_pair', 'N/A')}** | "
                f"Buy @ ${opp.get('buy_price', 0):,.2f} | "
                f"Sell @ ${opp.get('sell_price', 0):,.2f} | "
                f"Spread: {opp.get('spread_percent', 0):.2f}%"
            )
        with col3:
            action = opp.get("recommended_action", "monitor")
            if action == "execute":
                st.success(f"✅ {action.upper()}")
            elif action == "monitor":
                st.warning(f"👀 {action.upper()}")
            else:
                st.error(f"⚠️ {action.upper()}")

else:
    st.info(f"No active opportunities with >{min_profit}% profit found in the last {max_age}s")

st.divider()

# Historical opportunities chart
st.subheader("📈 Opportunity History")

arb_data = fetch_arbitrage(
    symbol=selected_symbol,
    min_profit=min_profit,
    limit=500,
)

if arb_data.get("data"):
    df = pd.DataFrame(arb_data["data"])

    # Convert timestamps
    if "detection_timestamp" in df.columns:
        df["detection_timestamp"] = pd.to_datetime(df["detection_timestamp"])

    fig = create_arbitrage_chart(df, height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No historical arbitrage data available")

st.divider()

# Detailed table
st.subheader("📋 All Opportunities")

if arb_data.get("data"):
    df = pd.DataFrame(arb_data["data"])
    arbitrage_table(df, height=400)

    st.caption(f"Showing {len(df)} opportunities | Min profit: {min_profit}% | Last updated: {datetime.now().strftime('%H:%M:%S')}")
else:
    st.info("No arbitrage data available")

# Exchange pair analysis
st.divider()
st.subheader("🔄 Exchange Pair Analysis")

if arb_data.get("data"):
    df = pd.DataFrame(arb_data["data"])

    # Count opportunities by exchange pair
    if "buy_exchange" in df.columns and "sell_exchange" in df.columns:
        df["pair"] = df["buy_exchange"] + " → " + df["sell_exchange"]
        pair_counts = df["pair"].value_counts().head(5)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Most Common Pairs:**")
            for pair, count in pair_counts.items():
                st.markdown(f"- {pair}: **{count}** opportunities")

        with col2:
            st.markdown("**Avg Profit by Pair:**")
            avg_by_pair = df.groupby("pair")["net_profit_percent"].mean().sort_values(ascending=False).head(5)
            for pair, avg in avg_by_pair.items():
                st.markdown(f"- {pair}: **{avg:.2f}%** avg profit")
else:
    st.info("No data for exchange pair analysis")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
