"""Live Prices Page - Poll API for latest prices."""

import streamlit as st
import pandas as pd
import requests
import time

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.serving.dashboard.config import DashboardConfig

st.set_page_config(page_title="Live Prices", page_icon="💰", layout="wide")
st.title("💰 Live Prices")

API = DashboardConfig.API_BASE_URL

placeholder = st.empty()


def fetch_prices():
    try:
        r = requests.get(f"{API}/prices", params={"limit": 200}, timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


for _ in range(300):
    data = fetch_prices()
    with placeholder.container():
        if data is None:
            st.warning("API not reachable or no price data yet.")
        else:
            prices = data.get("data", []) if isinstance(data, dict) else data
            if prices:
                df = pd.DataFrame(prices)

                # Summary metrics
                if "symbol" in df.columns:
                    symbols = df["symbol"].unique()
                    cols = st.columns(min(len(symbols), 5))
                    for i, sym in enumerate(symbols[:5]):
                        sym_df = df[df["symbol"] == sym]
                        latest_price = sym_df["price"].iloc[0] if len(sym_df) > 0 else 0
                        with cols[i]:
                            st.metric(sym, f"${latest_price:,.2f}")

                st.divider()

                # Price table per exchange
                exchanges = df["exchange"].unique() if "exchange" in df.columns else []
                for exch in exchanges:
                    exch_df = df[df["exchange"] == exch].copy()
                    st.subheader(f"{exch.title()}")

                    if "symbol" in exch_df.columns and "price" in exch_df.columns:
                        # Pivot: one column per symbol
                        pivot = exch_df.groupby("symbol")["price"].last()
                        st.dataframe(
                            pivot.to_frame("Latest Price").style.format("${:,.2f}"),
                            use_container_width=True,
                        )
            else:
                st.info("Waiting for price data...")

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")
    time.sleep(2)
