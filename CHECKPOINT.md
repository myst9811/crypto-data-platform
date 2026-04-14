# Crypto Data Platform - Development Checkpoint

**Date**: 2026-04-14
**Status**: Full Pipeline + ML Layer Operational

---

## Current State Summary

### Component Status

| Component | Status | Access |
|-----------|--------|--------|
| Zookeeper | Running | localhost:2181 |
| Kafka | Running | localhost:9092 |
| Kafka UI | Running | http://localhost:8080 |
| Binance Producer | Ready (Docker) | WebSocket |
| Coinbase Producer | Ready (Docker) | WebSocket |
| Kraken Producer | Ready (Docker) | WebSocket |
| Spark Streaming | Ready (local mode) | `scripts/start_spark.sh` |
| Delta Lake (Bronze) | Ready | data/bronze/{trades,ticker} |
| Delta Lake (Silver) | Ready | data/silver/prices |
| Delta Lake (Gold) | Ready | data/gold/{vwap,spreads,arbitrage_signals} |
| ML Layer | Ready | ml/ directory |
| FastAPI Server | Ready | http://localhost:8000 |
| Streamlit Dashboard | Ready | http://localhost:8501 |

### ML Models

| Model | File | Purpose |
|-------|------|---------|
| XGBoost | ml/artifacts/xgboost_arbitrage.pkl | Arbitrage probability classifier |
| Isolation Forest | ml/artifacts/isolation_forest.pkl | Anomaly detection on spreads |
| GARCH(1,1) | ml/artifacts/garch_{symbol}.pkl | Per-symbol volatility forecast |
| LSTM | ml/artifacts/lstm_price_direction.pt | Price direction prediction (30s) |
| Online Learner | ml/artifacts/online_learner.pkl | Adaptive streaming classifier |

---

## Quick Start Commands (Mac)

### 1. Start Infrastructure

```bash
# Start Kafka
cd docker && docker compose up -d zookeeper kafka kafka-ui
docker compose run --rm kafka-init
docker compose up -d binance-producer coinbase-producer kraken-producer
cd ..
```

### 2. Start Spark Streaming Pipeline

```bash
# Activate venv and run local Spark
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
./scripts/start_spark.sh
```

### 3. Train ML Models (after data accumulates)

```bash
source .venv/bin/activate

# Extract features from Delta Lake
python -m ml.features.feature_extractor

# Train models
python -m ml.training.train_xgboost
python -m ml.training.train_isolation_forest
python -m ml.training.train_garch
python -m ml.training.train_lstm
```

### 4. Start Serving Layer

```bash
# Terminal 1: API
source .venv/bin/activate
uvicorn src.serving.api.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2: Dashboard
source .venv/bin/activate
streamlit run src/serving/dashboard/app.py
```

### 5. Run Tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

---

## Architecture

```
Exchanges          Kafka              Spark (local)         Delta Lake
─────────          ─────              ─────────────         ──────────
Binance  ──ws──►  raw-trades    ──►  Bronze trades    ──►  data/bronze/trades
Coinbase ──ws──►  raw-ticker    ──►  Bronze ticker    ──►  data/bronze/ticker
Kraken   ──ws──►                ──►  Silver prices    ──►  data/silver/prices
                                 ──►  Gold VWAP        ──►  data/gold/vwap
                                 ──►  Gold spreads     ──►  data/gold/spreads
                                 ──►  Gold arb signals ──►  data/gold/arbitrage_signals

Delta Lake          ML Layer              FastAPI            Streamlit
──────────          ────────              ───────            ─────────
silver/prices  ──►  Feature extraction    /api/v1/prices     Live Prices
gold/spreads   ──►  XGBoost classifier    /api/v1/arb/live   Arbitrage Alerts
gold/arb       ──►  Isolation Forest      /api/v1/anomalies  ML Insights
silver/prices  ──►  GARCH volatility      /api/v1/volatility VWAP Analysis
silver/prices  ──►  LSTM direction        /api/v1/predictions Volume Analysis
                    Online learner        /api/v1/model/perf Exchange Comparison
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/health` | GET | Health check |
| `/api/v1/prices/latest` | GET | Latest prices |
| `/api/v1/vwap` | GET | VWAP metrics |
| `/api/v1/volume` | GET | Volume aggregates |
| `/api/v1/arbitrage/live` | GET | ML-enriched arbitrage signals |
| `/api/v1/predictions/{symbol}` | GET | LSTM direction forecast |
| `/api/v1/anomalies/recent` | GET | IsolationForest flagged prices |
| `/api/v1/model/performance` | GET | MLflow metrics for all models |
| `/api/v1/volatility/{symbol}` | GET | GARCH variance forecast |

---

## Project Structure

```
crypto-data-platform/
├── config/
│   └── spark_config.yaml        # Spark local mode + Delta paths
├── docker/
│   ├── docker-compose.yml       # Kafka + producers (platform: linux/amd64)
│   ├── kafka/init-topics.sh     # Topics + dead-letter
│   └── producer/Dockerfile
├── ml/
│   ├── features/
│   │   ├── feature_extractor.py # Delta → features → parquet
│   │   └── feature_store.py     # Load cached features
│   ├── training/
│   │   ├── label_generator.py   # Lookahead labels (no leakage)
│   │   ├── train_xgboost.py     # Arbitrage classifier
│   │   ├── train_isolation_forest.py
│   │   ├── train_garch.py       # Per-symbol GARCH(1,1)
│   │   └── train_lstm.py        # Bidirectional LSTM
│   ├── evaluation/
│   │   ├── walk_forward_cv.py   # Chronological splits
│   │   ├── metrics.py           # Classifier + regression metrics
│   │   └── baseline.py          # Rule-based baseline
│   ├── serving/
│   │   ├── predictor.py         # ArbitragePredictor pipeline
│   │   ├── online_learner.py    # River adaptive forest
│   │   └── model_registry.py    # MLflow query helpers
│   ├── mlflow_setup.py
│   └── artifacts/               # Trained model files (gitignored)
├── src/
│   ├── ingestion/               # Exchange WebSocket producers
│   │   └── base_producer.py     # Schema validation + dead-letter
│   ├── processing/
│   │   └── spark_streaming.py   # Local mode Spark pipeline
│   ├── serving/
│   │   ├── api/
│   │   │   ├── main.py          # FastAPI app
│   │   │   └── routes/ml.py     # ML prediction endpoints
│   │   └── dashboard/
│   │       └── pages/           # 6 Streamlit pages
│   ├── storage/
│   └── utils/
├── tests/
│   └── test_pipeline.py         # 4 pytest tests
├── scripts/
│   └── start_spark.sh           # Mac start script
├── data/                        # Delta Lake tables (gitignored)
└── CHECKPOINT.md                # This file
```

---

## Environment Requirements

- macOS (Apple Silicon supported)
- Python 3.12 (via Homebrew)
- Java 17 (OpenJDK via Homebrew)
- Docker Desktop for Mac
- ~500MB disk for pip packages + Spark jars

---

*Last updated: 2026-04-14*
