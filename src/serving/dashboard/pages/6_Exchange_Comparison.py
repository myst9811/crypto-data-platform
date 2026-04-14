"""Exchange Comparison Page - Cross-exchange spread analysis."""

import streamlit as st
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

st.set_page_config(page_title="Exchange Comparison", page_icon="🔄", layout="wide")
st.title("🔄 Exchange Comparison")

SPREADS_PATH = "data/gold/spreads"


@st.cache_data(ttl=10)
def load_spreads():
    p = Path(SPREADS_PATH)
    if not p.exists():
        return None
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(str(p))
        return dt.to_pandas()
    except Exception as e:
        st.error(f"Error loading spreads: {e}")
        return None


df = load_spreads()

if df is None or df.empty:
    st.warning("No spread data available yet. Start the Spark pipeline first.")
else:
    df["event_time"] = pd.to_datetime(df["event_time"])
    df = df.sort_values("event_time")

    symbols = df["symbol"].unique().tolist() if "symbol" in df.columns else []
    default_symbols = [s for s in ["BTC/USD", "ETH/USD"] if s in symbols]

    sel_symbols = st.multiselect(
        "Select Symbols",
        symbols,
        default=default_symbols or symbols[:2],
    )

    for sym in sel_symbols:
        st.subheader(f"{sym} — Spread % Over Time")
        sym_df = df[df["symbol"] == sym].copy()

        if "spread_pct" in sym_df.columns and len(sym_df) > 0:
            # Pivot by exchange pair
            sym_df["pair"] = sym_df["exchange_a"] + " vs " + sym_df["exchange_b"]
            pivot = sym_df.pivot_table(
                index="event_time",
                columns="pair",
                values="spread_pct",
                aggfunc="mean",
            )
            st.line_chart(pivot)

    st.subheader("Raw Spread Data (last 50)")
    st.dataframe(df.tail(50))
