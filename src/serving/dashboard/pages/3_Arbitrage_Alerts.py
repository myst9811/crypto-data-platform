"""Arbitrage Alerts Page - Poll ML-enriched signals."""

import streamlit as st
import pandas as pd
import requests
import time

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.serving.dashboard.config import DashboardConfig

st.set_page_config(page_title="Arbitrage Alerts", page_icon="🚨", layout="wide")
st.title("🚨 Arbitrage Alerts")

API = DashboardConfig.API_BASE_URL
placeholder = st.empty()


def fetch_arbitrage():
    try:
        r = requests.get(f"{API}/arbitrage/live", timeout=5)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def color_probability(val):
    if val is None:
        return ""
    try:
        v = float(val)
        if v > 0.7:
            return "background-color: #2ca02c; color: white"
        elif v > 0.4:
            return "background-color: #ffbb00; color: black"
        else:
            return "background-color: #d62728; color: white"
    except (ValueError, TypeError):
        return ""


for _ in range(200):
    data = fetch_arbitrage()
    with placeholder.container():
        if data is None:
            st.warning("No arbitrage signals available. Run the pipeline and ML training first.")
        else:
            signals = data if isinstance(data, list) else []
            if signals:
                df = pd.DataFrame(signals)

                # Add anomaly icon
                if "anomaly_flag" in df.columns:
                    df["status"] = df["anomaly_flag"].apply(
                        lambda x: "⚠️ Anomaly" if x else "✅ Normal"
                    )

                st.dataframe(
                    df.style.applymap(
                        color_probability,
                        subset=["arb_probability"] if "arb_probability" in df.columns else [],
                    ),
                    use_container_width=True,
                )
            else:
                st.info("No active arbitrage signals.")

        st.caption(f"Last refresh: {time.strftime('%H:%M:%S')}")
    time.sleep(3)
