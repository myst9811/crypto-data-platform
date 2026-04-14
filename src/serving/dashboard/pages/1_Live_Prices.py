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
        r = requests.get(f"{API}/prices/latest", timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


# Auto-refresh loop
for _ in range(300):  # 10 min max
    data = fetch_prices()
    with placeholder.container():
        if data is None:
            st.warning("API not reachable or no price data yet.")
        else:
            prices = data if isinstance(data, list) else data.get("data", [])
            if prices:
                df = pd.DataFrame(prices)
                if "exchange" in df.columns and "price" in df.columns:
                    for exch in df["exchange"].unique():
                        st.subheader(f"{exch.title()}")
                        exch_df = df[df["exchange"] == exch]
                        st.line_chart(
                            exch_df.set_index(
                                exch_df.columns[
                                    exch_df.columns.str.contains("symbol|standard")
                                ][0]
                                if any(exch_df.columns.str.contains("symbol|standard"))
                                else exch_df.index
                            )[["price"]],
                        )
                else:
                    st.dataframe(df)
            else:
                st.info("Waiting for price data...")

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")
    time.sleep(2)
