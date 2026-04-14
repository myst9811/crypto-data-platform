"""VWAP Analysis Page - Read from Gold Delta table."""

import streamlit as st
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

st.set_page_config(page_title="VWAP Analysis", page_icon="📈", layout="wide")
st.title("📈 VWAP Analysis")

VWAP_PATH = "data/gold/vwap"


@st.cache_data(ttl=10)
def load_vwap():
    p = Path(VWAP_PATH)
    if not p.exists():
        return None
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(str(p))
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Error loading VWAP: {e}")
        return None


df = load_vwap()

if df is None or df.empty:
    st.warning("No VWAP data available yet. Start the Spark streaming pipeline first.")
else:
    # Filters
    symbols = df["symbol"].unique().tolist() if "symbol" in df.columns else []
    exchanges = df["exchange"].unique().tolist() if "exchange" in df.columns else []

    col1, col2 = st.columns(2)
    with col1:
        sel_symbol = st.selectbox("Symbol", ["All"] + symbols)
    with col2:
        sel_exchange = st.selectbox("Exchange", ["All"] + exchanges)

    filtered = df.copy()
    if sel_symbol != "All":
        filtered = filtered[filtered["symbol"] == sel_symbol]
    if sel_exchange != "All":
        filtered = filtered[filtered["exchange"] == sel_exchange]

    # Chart
    time_col = "window_start" if "window_start" in filtered.columns else filtered.columns[0]
    if "vwap" in filtered.columns:
        filtered = filtered.sort_values(time_col)
        chart_data = filtered.set_index(time_col)[["vwap"]]
        st.line_chart(chart_data)
    else:
        st.dataframe(filtered)

    st.subheader("Raw Data")
    st.dataframe(filtered.tail(50))
