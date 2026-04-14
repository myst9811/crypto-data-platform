"""Volume Analysis Page - Read Silver prices, compute rolling volume."""

import streamlit as st
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

st.set_page_config(page_title="Volume Analysis", page_icon="📊", layout="wide")
st.title("📊 Volume Analysis")

PRICES_PATH = "data/silver/prices"


@st.cache_data(ttl=10)
def load_prices():
    p = Path(PRICES_PATH)
    if not p.exists():
        return None
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(str(p))
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Error loading prices: {e}")
        return None


df = load_prices()

if df is None or df.empty:
    st.warning("No silver price data available yet. Start the Spark pipeline first.")
else:
    df["event_time"] = pd.to_datetime(df["event_time"])
    df = df.sort_values("event_time")

    # Rolling 5-min volume per exchange
    exchanges = df["exchange"].unique().tolist() if "exchange" in df.columns else []

    st.subheader("Rolling 5-Minute Volume by Exchange")

    for exch in exchanges:
        exch_df = df[df["exchange"] == exch].copy()
        exch_df = exch_df.set_index("event_time")
        if "volume" in exch_df.columns:
            rolling_vol = exch_df["volume"].rolling("5min").sum()
            st.subheader(f"{exch.title()}")
            st.bar_chart(rolling_vol)

    st.subheader("Raw Volume Data (last 50)")
    st.dataframe(df[["event_time", "exchange", "symbol", "volume"]].tail(50))
