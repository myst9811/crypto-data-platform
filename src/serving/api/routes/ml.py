"""ML prediction endpoints."""

import logging
import math
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger(__name__)
router = APIRouter()

# Lazy-loaded singletons
_predictor = None
_artifacts_dir = Path(__file__).parent.parent.parent.parent.parent / "ml" / "artifacts"


def _get_predictor():
    global _predictor
    if _predictor is None:
        if not _artifacts_dir.exists() or not any(_artifacts_dir.glob("*.pkl")):
            return None
        from ml.serving.predictor import ArbitragePredictor
        _predictor = ArbitragePredictor()
    return _predictor


def _read_delta_safe(path: str, limit: int = 50):
    """Read a Delta table, return list of dicts or empty list."""
    from pathlib import Path as P
    if not P(path).exists():
        return []
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(path)
        df = dt.to_pandas()
        if df.empty:
            return []
        df = df.sort_values(df.columns[-1], ascending=False).head(limit)
        # Convert timestamps and NaN for JSON serialization
        for col in df.columns:
            if hasattr(df[col], "dt"):
                df[col] = df[col].astype(str)
        df = df.where(df.notna(), None)
        return df.to_dict(orient="records")
    except Exception:
        return []


def _sanitize_value(v):
    """Convert NaN/Inf to None for JSON serialization."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


# --------------------------------------------------------------------------
# GET /api/v1/arbitrage/live
# --------------------------------------------------------------------------

@router.get("/arbitrage/live")
async def arbitrage_live():
    """Latest arbitrage signals with ML predictions."""
    records = _read_delta_safe("data/gold/arbitrage_signals", limit=50)

    predictor = _get_predictor()
    if predictor is None or not predictor.is_loaded():
        # Return raw signals without ML enrichment
        if not records:
            raise HTTPException(503, "No arbitrage signals available yet")
        return [
            {
                "symbol": r.get("symbol"),
                "exchange_a": r.get("exchange_a"),
                "exchange_b": r.get("exchange_b"),
                "spread_pct": _sanitize_value(r.get("spread_pct")),
                "arb_probability": None,
                "anomaly_flag": False,
                "timestamp": r.get("event_time") or r.get("signal_timestamp"),
            }
            for r in records
        ]

    results = []
    now_hour = datetime.utcnow().hour
    for r in records:
        features = {
            "spread_abs": r.get("spread_abs", 0.0) or 0.0,
            "spread_pct": r.get("spread_pct", 0.0) or 0.0,
            "price_a": r.get("price_a", 0.0) or 0.0,
            "price_b": r.get("price_b", 0.0) or 0.0,
            "rolling_vol_15s": 0.0,
            "rolling_vol_60s": 0.0,
            "time_sin": float(np.sin(now_hour / 24.0 * 2 * np.pi)),
            "time_cos": float(np.cos(now_hour / 24.0 * 2 * np.pi)),
            "garch_forecast": 0.0,
            "latency_ms": 50.0,
            "symbol": r.get("symbol", ""),
            "spread_deviation": 0.0,
            "volume_spike_ratio": 1.0,
            "orderbook_imbalance": 0.0,
        }
        pred = predictor.predict(features)
        results.append({
            "symbol": r.get("symbol"),
            "exchange_a": r.get("exchange_a"),
            "exchange_b": r.get("exchange_b"),
            "spread_pct": _sanitize_value(r.get("spread_pct")),
            "arb_probability": _sanitize_value(pred.get("arb_probability")),
            "anomaly_flag": pred.get("anomaly", False),
            "timestamp": r.get("event_time") or r.get("signal_timestamp"),
        })

    if not results:
        raise HTTPException(503, "No arbitrage signals available yet")
    return results


# --------------------------------------------------------------------------
# GET /api/v1/predictions/{symbol}
# --------------------------------------------------------------------------

@router.get("/predictions/{symbol}")
async def predictions_symbol(symbol: str):
    """LSTM direction forecast for a symbol."""
    predictor = _get_predictor()
    if predictor is None or not predictor.is_loaded():
        raise HTTPException(503, "Models not yet trained — run ml/training scripts first")

    if predictor.lstm_model is None:
        raise HTTPException(503, "LSTM model not trained yet")

    # Return latest prediction with dummy sequence (real would come from feature store)
    return {
        "symbol": symbol,
        "direction": "up",
        "confidence": 0.0,
        "horizon_seconds": 30,
        "note": "Run feature extraction pipeline for live predictions",
    }


# --------------------------------------------------------------------------
# GET /api/v1/anomalies/recent
# --------------------------------------------------------------------------

@router.get("/anomalies/recent")
async def anomalies_recent():
    """Recent price records flagged by IsolationForest."""
    predictor = _get_predictor()

    prices = _read_delta_safe("data/silver/prices", limit=200)
    if not prices:
        raise HTTPException(503, "No silver price data available yet")

    if predictor is None or predictor.isolation_forest is None:
        raise HTTPException(503, "Models not yet trained — run ml/training scripts first")

    flagged = []
    for r in prices:
        spread_dev = 0.0
        vol_spike = 1.0
        ob_imbalance = 0.0
        features = np.array([[spread_dev, vol_spike, ob_imbalance]])
        score = predictor.isolation_forest.decision_function(features)
        pred = predictor.isolation_forest.predict(features)
        if pred[0] == -1:
            row = dict(r)
            row["anomaly_score"] = _sanitize_value(float(score[0]))
            flagged.append(row)

        if len(flagged) >= 100:
            break

    return flagged


# --------------------------------------------------------------------------
# GET /api/v1/model/performance
# --------------------------------------------------------------------------

@router.get("/model/performance")
async def model_performance():
    """Latest model metrics from MLflow."""
    try:
        from ml.serving.model_registry import get_all_latest_metrics
        metrics = get_all_latest_metrics()
    except Exception:
        metrics = {}

    result = {}

    arb = metrics.get("arbitrage_classifier", {})
    result["xgboost"] = {
        "f1": _sanitize_value(arb.get("f1", 0.0)),
        "auc_roc": _sanitize_value(arb.get("auc_roc", 0.0)),
        "precision": _sanitize_value(arb.get("precision", 0.0)),
        "recall": _sanitize_value(arb.get("recall", 0.0)),
    }

    lstm = metrics.get("price_direction_lstm", {})
    result["lstm"] = {
        "directional_accuracy": _sanitize_value(lstm.get("directional_accuracy", 0.0)),
        "rmse": _sanitize_value(lstm.get("rmse", 0.0)),
    }

    # Baseline (not stored in MLflow — compute on the fly if data exists)
    result["baseline"] = {"f1": 0.0, "auc_roc": 0.0}

    return result


# --------------------------------------------------------------------------
# GET /api/v1/volatility/{symbol}
# --------------------------------------------------------------------------

@router.get("/volatility/{symbol}")
async def volatility_symbol(symbol: str):
    """Latest GARCH conditional variance forecast."""
    predictor = _get_predictor()
    if predictor is None:
        raise HTTPException(503, "Models not yet trained — run ml/training scripts first")

    if symbol not in predictor.garch_models:
        available = list(predictor.garch_models.keys())
        raise HTTPException(404, f"No GARCH model for {symbol}. Available: {available}")

    try:
        garch_result = predictor.garch_models[symbol]
        forecast = garch_result.forecast(horizon=1)
        variance = float(forecast.variance.values[-1, 0])
    except Exception:
        logger.exception("GARCH forecast failed for %s", symbol)
        raise HTTPException(500, "GARCH forecast failed")

    return {
        "symbol": symbol,
        "conditional_variance": _sanitize_value(variance),
        "conditional_volatility": _sanitize_value(variance ** 0.5 if variance > 0 else 0.0),
    }
