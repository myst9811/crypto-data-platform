"""Exchange Comparison Page - Side-by-side exchange metrics."""

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
    window_duration_filter,
    refresh_rate_filter,
    create_exchange_radar,
)

st.set_page_config(
    page_title="Exchange Comparison - Crypto Platform",
    page_icon="🔄",
    layout="wide",
)

st.title("🔄 Exchange Comparison")
st.markdown("Compare performance metrics across exchanges")


def fetch_prices(symbol: str):
    """Fetch price comparison."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/prices/compare",
            params={"symbol": symbol},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def fetch_volume_rankings(symbol: str, window: str):
    """Fetch volume rankings."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/volume/rankings",
            params={"symbol": symbol, "window": window},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def fetch_liquidity_rankings(symbol: str):
    """Fetch liquidity rankings."""
    try:
        response = requests.get(
            f"{DashboardConfig.API_BASE_URL}/liquidity/rankings",
            params={"symbol": symbol},
            timeout=10,
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


# Sidebar filters
with st.sidebar:
    st.header("Filters")
    selected_symbol = symbol_filter(key="cmp_symbol")
    selected_window = window_duration_filter(key="cmp_window")
    st.divider()
    refresh_interval = refresh_rate_filter(key="cmp_refresh")
    auto_refresh = st.checkbox("Auto-refresh", value=False, key="cmp_auto")

st.divider()

# Fetch all data
prices_data = fetch_prices(selected_symbol)
volume_data = fetch_volume_rankings(selected_symbol, selected_window)
liquidity_data = fetch_liquidity_rankings(selected_symbol)

# Price comparison
st.subheader(f"💰 {selected_symbol} Price Comparison")

if prices_data and prices_data.get("prices"):
    cols = st.columns(len(prices_data["prices"]))
    for i, price in enumerate(prices_data["prices"]):
        with cols[i]:
            exchange = price.get("exchange", "")
            name = DashboardConfig.get_exchange_name(exchange)
            color = DashboardConfig.get_exchange_color(exchange)

            st.markdown(
                f"<h3 style='color:{color}'>{name}</h3>",
                unsafe_allow_html=True,
            )
            st.metric("Price", f"${price.get('price', 0):,.2f}")
else:
    st.info("No price data available")

st.divider()

# Side-by-side metrics
st.subheader("📊 Detailed Comparison")

exchanges = DashboardConfig.EXCHANGES
cols = st.columns(len(exchanges))

# Build metrics for each exchange
exchange_metrics = {}

for exchange in exchanges:
    metrics = {
        "price": 0,
        "volume": 0,
        "spread": 0,
        "liquidity_score": 0,
        "trades": 0,
    }

    # Get price
    if prices_data and prices_data.get("prices"):
        for p in prices_data["prices"]:
            if p.get("exchange") == exchange:
                metrics["price"] = p.get("price", 0)
                break

    # Get volume
    if volume_data and volume_data.get("rankings"):
        for v in volume_data["rankings"]:
            if v.get("exchange") == exchange:
                metrics["volume"] = v.get("total_volume", 0)
                metrics["trades"] = v.get("num_trades", 0)
                break

    # Get liquidity
    if liquidity_data and liquidity_data.get("rankings"):
        for liq in liquidity_data["rankings"]:
            if liq.get("exchange") == exchange:
                metrics["spread"] = liq.get("spread_percent", 0)
                metrics["liquidity_score"] = liq.get("liquidity_score", 0) or 0
                break

    exchange_metrics[exchange] = metrics

# Display metrics
for i, exchange in enumerate(exchanges):
    with cols[i]:
        name = DashboardConfig.get_exchange_name(exchange)
        color = DashboardConfig.get_exchange_color(exchange)
        metrics = exchange_metrics[exchange]

        st.markdown(
            f"<h4 style='color:{color}; border-bottom: 3px solid {color}; padding-bottom: 10px;'>{name}</h4>",
            unsafe_allow_html=True,
        )

        st.metric("Price", f"${metrics['price']:,.2f}")
        st.metric("Volume", f"{metrics['volume']:,.4f}")
        st.metric("Spread", f"{metrics['spread']:.4f}%")
        st.metric("Liquidity Score", f"{metrics['liquidity_score']:,.0f}")
        st.metric("Trades", f"{metrics['trades']:,}")

st.divider()

# Radar chart comparison
st.subheader("🎯 Normalized Comparison")

if exchange_metrics:
    # Normalize metrics for radar chart (0-100 scale)
    radar_metrics = {}

    # Find max values for normalization
    max_price = max(m["price"] for m in exchange_metrics.values()) or 1
    max_volume = max(m["volume"] for m in exchange_metrics.values()) or 1
    max_liquidity = max(m["liquidity_score"] for m in exchange_metrics.values()) or 1
    min_spread = min(m["spread"] for m in exchange_metrics.values() if m["spread"] > 0) or 0.001

    for exchange, metrics in exchange_metrics.items():
        radar_metrics[exchange] = {
            "Price Competitiveness": (1 - (metrics["price"] / max_price)) * 100 if max_price else 50,
            "Volume": (metrics["volume"] / max_volume) * 100 if max_volume else 0,
            "Liquidity": (metrics["liquidity_score"] / max_liquidity) * 100 if max_liquidity else 0,
            "Tight Spread": (min_spread / metrics["spread"]) * 100 if metrics["spread"] > 0 else 100,
        }

    fig = create_exchange_radar(radar_metrics, height=450)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# Fee comparison
st.subheader("💳 Fee Comparison")

fee_data = {
    "binance": {"maker": 0.10, "taker": 0.10, "withdrawal": 0.05},
    "coinbase": {"maker": 0.00, "taker": 0.05, "withdrawal": 0.00},
    "kraken": {"maker": 0.16, "taker": 0.26, "withdrawal": 0.015},
}

fee_cols = st.columns(len(exchanges))

for i, exchange in enumerate(exchanges):
    with fee_cols[i]:
        name = DashboardConfig.get_exchange_name(exchange)
        fees = fee_data.get(exchange, {})

        st.markdown(f"**{name}**")
        st.markdown(f"- Maker: {fees.get('maker', 0):.2f}%")
        st.markdown(f"- Taker: {fees.get('taker', 0):.2f}%")
        st.markdown(f"- Withdrawal: {fees.get('withdrawal', 0):.3f}%")

st.divider()

# Summary recommendation
st.subheader("🏆 Summary")

if exchange_metrics:
    # Calculate scores
    scores = {}
    for exchange, metrics in exchange_metrics.items():
        # Lower spread is better, higher volume and liquidity are better
        spread_score = (1 / metrics["spread"]) if metrics["spread"] > 0 else 0
        volume_score = metrics["volume"]
        liquidity_score = metrics["liquidity_score"]

        # Combined score (weighted)
        scores[exchange] = (
            spread_score * 0.4 +
            (volume_score / max_volume * 100 if max_volume else 0) * 0.3 +
            (liquidity_score / max_liquidity * 100 if max_liquidity else 0) * 0.3
        )

    # Sort by score
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Overall Ranking:**")
        for i, (exchange, score) in enumerate(ranked, 1):
            name = DashboardConfig.get_exchange_name(exchange)
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
            st.markdown(f"{medal} **{i}. {name}** (Score: {score:.1f})")

    with col2:
        best = ranked[0][0] if ranked else None
        if best:
            best_name = DashboardConfig.get_exchange_name(best)
            st.success(f"**Recommended Exchange: {best_name}**")
            st.markdown(
                f"""
                - Best for: Lowest spreads, high liquidity
                - Consider: Fee structure for your trading volume
                - Note: Rankings based on current market conditions
                """
            )

st.caption(f"Symbol: {selected_symbol} | Window: {selected_window} | Last updated: {datetime.now().strftime('%H:%M:%S')}")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
