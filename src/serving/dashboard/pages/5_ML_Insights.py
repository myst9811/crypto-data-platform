"""ML Insights Page - Model performance metrics and feature importance."""

import streamlit as st
import pandas as pd
import requests
import pickle
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))

from src.serving.dashboard.config import DashboardConfig

st.set_page_config(page_title="ML Insights", page_icon="🧠", layout="wide")
st.title("🧠 ML Insights")

API = DashboardConfig.API_BASE_URL
ARTIFACTS_DIR = Path(__file__).parent.parent.parent.parent.parent / "ml" / "artifacts"


# --- Model Performance Metrics ---
st.subheader("Model Performance")

try:
    r = requests.get(f"{API}/model/performance", timeout=5)
    if r.status_code == 200:
        perf = r.json()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("**XGBoost (Arbitrage)**")
            xgb = perf.get("xgboost", {})
            st.metric("F1 Score", f"{xgb.get('f1', 0):.3f}")
            st.metric("AUC-ROC", f"{xgb.get('auc_roc', 0):.3f}")
            st.metric("Precision", f"{xgb.get('precision', 0):.3f}")
            st.metric("Recall", f"{xgb.get('recall', 0):.3f}")

        with col2:
            st.markdown("**LSTM (Price Direction)**")
            lstm = perf.get("lstm", {})
            st.metric("Directional Accuracy", f"{lstm.get('directional_accuracy', 0):.3f}")
            st.metric("RMSE", f"{lstm.get('rmse', 0):.4f}")

        with col3:
            st.markdown("**Baseline (Rule-Based)**")
            base = perf.get("baseline", {})
            st.metric("F1 Score", f"{base.get('f1', 0):.3f}")
            st.metric("AUC-ROC", f"{base.get('auc_roc', 0):.3f}")
    else:
        st.warning("Model performance API unavailable.")
except Exception as e:
    st.warning(f"Could not fetch model performance: {e}")


# --- Feature Importance ---
st.subheader("XGBoost Feature Importance")

xgb_path = ARTIFACTS_DIR / "xgboost_arbitrage.pkl"
if xgb_path.exists():
    try:
        with open(xgb_path, "rb") as f:
            model = pickle.load(f)
        feature_names = [
            "spread_abs", "spread_pct", "price_a", "price_b",
            "rolling_vol_15s", "rolling_vol_60s",
            "time_sin", "time_cos", "garch_forecast", "latency_ms",
        ]
        importance = dict(zip(feature_names, model.feature_importances_))
        imp_df = pd.DataFrame(
            list(importance.items()), columns=["Feature", "Importance"]
        ).sort_values("Importance", ascending=True)
        st.bar_chart(imp_df.set_index("Feature"))
    except Exception as e:
        st.warning(f"Could not load XGBoost model: {e}")
else:
    st.info("XGBoost model not trained yet. Run `python -m ml.training.train_xgboost`.")


# --- Online Learner ---
st.subheader("Online Learner (Adaptive Random Forest)")

online_path = ARTIFACTS_DIR / "online_learner.pkl"
if online_path.exists():
    try:
        with open(online_path, "rb") as f:
            state = pickle.load(f)
        recent = state.get("recent_correct", [])
        acc = sum(recent) / len(recent) if recent else 0.0
        st.metric("Rolling Accuracy (500 samples)", f"{acc:.3f}")
        st.metric("Total Updates", state.get("update_count", 0))
    except Exception as e:
        st.warning(f"Could not load online learner: {e}")
else:
    st.info("Online learner not initialised yet.")
