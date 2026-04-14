# Crypto Data Platform

A real-time cryptocurrency analytics platform that ingests live market data from three major exchanges, processes it through a medallion data lakehouse, trains ML models for arbitrage detection and price forecasting, and serves insights through a REST API and interactive dashboard.

---

## What It Does

- Streams live trade and ticker data from **Binance, Coinbase, and Kraken** via WebSocket
- Processes 1M+ price events per session through a **Bronze → Silver → Gold** Delta Lake pipeline powered by Apache Spark Structured Streaming
- Computes **cross-exchange spreads, VWAP, and volume aggregates** in 1-minute rolling windows
- Detects **arbitrage opportunities** (spread > 0.15%) and flags them in real time
- Trains four ML models: **XGBoost arbitrage classifier, Bidirectional LSTM price direction forecast, Isolation Forest anomaly detector, and GARCH(1,1) volatility model**
- Serves all data and predictions through a **FastAPI REST API** with 15 endpoints
- Visualises everything in a **6-page Streamlit dashboard**

---

## Architecture

```
Exchange WebSockets (Binance / Coinbase / Kraken)
        │
        ▼
  Apache Kafka  (raw-trades · raw-ticker · raw-orderbook)
        │
        ▼
  Spark Structured Streaming  —  10-second micro-batches
  ├── Bronze  →  data/bronze/trades, data/bronze/ticker
  ├── Silver  →  data/silver/prices          (normalised, deduplicated)
  └── Gold    →  data/gold/vwap
                 data/gold/spreads
                 data/gold/arbitrage_signals
        │
        ├──► ML Pipeline  →  feature store  →  4 trained models
        │
        └──► FastAPI (:8000)  →  Streamlit Dashboard (:8501)
```

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full diagram (eraser.io compatible), endpoint reference, and service breakdown.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python WebSocket clients (websockets), Apache Kafka |
| Stream Processing | Apache Spark 3.5 Structured Streaming, Delta Lake 3.1 |
| Storage | Delta Lake (medallion: Bronze / Silver / Gold) |
| ML | XGBoost, PyTorch (LSTM), scikit-learn (Isolation Forest), arch (GARCH) |
| Experiment Tracking | MLflow |
| API | FastAPI + Uvicorn |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker (Kafka stack), native Python processes (Spark, API, Dashboard) |
| Language | Python 3.12 |

---

## Data Pipeline

### Medallion Layers

**Bronze** — Raw Kafka messages written as-is to Delta Lake. No parsing, preserves original payloads for replay.

**Silver** — JSON parsed against typed schemas, symbols normalised to a unified format (`BTCUSDT` → `BTC/USD`, `XBT/USD` → `BTC/USD`), null prices filtered. Single unified price stream across all three exchanges.

**Gold** — 1-minute tumbling window aggregations:
- `vwap` — Volume-Weighted Average Price, trade count, price std dev per symbol per exchange
- `spreads` — Cross-exchange price spread (absolute and %) for every exchange pair
- `arbitrage_signals` — Spread rows that exceed the 0.15% profit threshold

### Supported Pairs

`BTC/USD · ETH/USD · BNB/USD · SOL/USD · XRP/USD` across Binance, Coinbase, and Kraken.

---

## ML Models

| Model | Task | Accuracy |
|---|---|---|
| **XGBoost** | Binary: is this spread profitable? | 98.4% F1 |
| **Bidirectional LSTM** | Price direction at T+30s | 69.5% directional accuracy |
| **Isolation Forest** | Anomaly detection on spread/volume features | 4.77% anomaly rate |
| **GARCH(1,1)** | Volatility forecast per symbol | AIC fit on 1M+ returns |

All models trained with chronological 70/15/15 splits (no data leakage), tracked in MLflow.

---

## Getting Started

### Prerequisites

- macOS or Linux
- Python 3.12
- Java 17 (`brew install openjdk@17` on Mac)
- Docker Desktop

### Setup

```bash
git clone https://github.com/myst9811/crypto-data-platform.git
cd crypto-data-platform

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the Full Stack

```bash
./start.sh
```

This starts Kafka (Docker), the three exchange producers (Docker), Spark Streaming (native), FastAPI (native), and the Streamlit dashboard (native).

| URL | Service |
|---|---|
| http://localhost:8501 | Streamlit dashboard |
| http://localhost:8000/api/v1/docs | FastAPI interactive docs |
| http://localhost:8080 | Kafka UI |

### Run in Serving Mode (no Spark/Kafka needed)

If you already have data on disk and don't need live ingestion:

```bash
.venv/bin/python -m src.serving.api.main > logs/api.log 2>&1 &
.venv/bin/python -m streamlit run src/serving/dashboard/Home.py > logs/dashboard.log 2>&1 &
```

The API reads directly from Delta files — no JVM or Docker required.

### Stop Everything

```bash
./stop.sh
docker compose -f docker/docker-compose.yml stop
```

---

## Train ML Models

After at least 2-3 minutes of data collection:

```bash
# 1. Build feature store from Delta tables
.venv/bin/python -m ml.features.feature_extractor

# 2. Train models (after step 1 completes)
.venv/bin/python -m ml.training.train_xgboost
.venv/bin/python -m ml.training.train_isolation_forest
.venv/bin/python -m ml.training.train_garch
.venv/bin/python -m ml.training.train_lstm
```

Models are saved to `ml/artifacts/` and all runs are tracked in `mlruns/`.

---

## API Reference

| Endpoint | Description |
|---|---|
| `GET /api/v1/health` | Liveness check |
| `GET /api/v1/health/ready` | Readiness + Delta table status |
| `GET /api/v1/prices` | Latest prices across all exchanges |
| `GET /api/v1/prices/{symbol}` | Latest price for a symbol |
| `GET /api/v1/prices/{symbol}/history` | Historical price series |
| `GET /api/v1/vwap` | VWAP metrics (1-min windows) |
| `GET /api/v1/vwap/{symbol}/history` | Historical VWAP |
| `GET /api/v1/volume` | Volume aggregates |
| `GET /api/v1/volume/rankings` | Exchange volume rankings |
| `GET /api/v1/arbitrage` | All arbitrage signals |
| `GET /api/v1/arbitrage/active` | Currently viable opportunities |
| `GET /api/v1/ml/predictions/{symbol}` | LSTM price direction forecast |
| `GET /api/v1/ml/arbitrage/live` | XGBoost arbitrage probability |
| `GET /api/v1/ml/anomalies/recent` | Isolation Forest anomaly flags |
| `GET /api/v1/ml/volatility/{symbol}` | GARCH volatility forecast |

Full interactive docs at `http://localhost:8000/api/v1/docs` when the API is running.

---

## Project Structure

```
crypto-data-platform/
├── src/
│   ├── ingestion/          # Exchange WebSocket producers
│   ├── processing/         # Spark Structured Streaming pipeline
│   └── serving/
│       ├── api/            # FastAPI application and routes
│       ├── dashboard/      # Streamlit multi-page dashboard
│       └── data_access/    # Delta Lake reader + Pydantic models
├── ml/
│   ├── features/           # Feature extraction and store
│   ├── training/           # Model training scripts
│   ├── serving/            # Predictor and model registry
│   ├── evaluation/         # Metrics, walk-forward CV
│   └── artifacts/          # Trained model files
├── data/
│   ├── bronze/             # Raw Kafka data (Delta)
│   ├── silver/             # Normalised prices (Delta)
│   ├── gold/               # Aggregated analytics (Delta)
│   ├── checkpoints/        # Spark streaming checkpoints
│   └── features/           # Feature store (Parquet)
├── config/                 # Spark, Kafka, exchange config
├── docker/                 # Docker Compose and Dockerfiles
├── logs/                   # Runtime logs and PIDs
├── tests/                  # Unit tests
├── start.sh                # One-command startup
├── stop.sh                 # Graceful shutdown
├── ARCHITECTURE.md         # Full architecture diagram and reference
└── requirements.txt
```

---

## Memory Requirements

The full stack uses ~5-6 GB RAM. On a 16 GB machine, avoid running other heavy applications simultaneously. The biggest consumers are the Docker VM (Kafka) at ~2 GB and Spark (JVM) at ~2-3 GB.

If memory is constrained, stop Spark and Docker after data collection — the API and dashboard continue working from the Delta files on disk.

---

## License

MIT
