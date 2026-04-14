# Architecture

## Eraser.io Diagram

Paste the block below at [eraser.io](https://eraser.io) to generate the architecture diagram.

```
// Crypto Data Platform — Architecture Diagram
// Paste into eraser.io > New Diagram > Cloud Architecture

direction right

// ── INGESTION ──────────────────────────────────────────────
Binance WebSocket [icon: binance, color: yellow]
Coinbase WebSocket [icon: coinbase, color: blue]
Kraken WebSocket [icon: kraken, color: purple]

Kafka Broker [icon: kafka, color: red] {
  raw-trades
  raw-ticker
  raw-orderbook
}

Binance WebSocket > Kafka Broker: WebSocket stream
Coinbase WebSocket > Kafka Broker: WebSocket stream
Kraken WebSocket > Kafka Broker: WebSocket stream

// ── PROCESSING ─────────────────────────────────────────────
Spark Streaming [icon: apache-spark, color: orange] {
  Bronze Layer [color: orange] {
    bronze/trades
    bronze/ticker
  }
  Silver Layer [color: grey] {
    silver/prices
  }
  Gold Layer [color: yellow] {
    gold/vwap
    gold/spreads
    gold/arbitrage_signals
  }
}

Kafka Broker > Spark Streaming: Kafka source (10s trigger)
Bronze Layer > Silver Layer: parse + normalize symbols
Silver Layer > Gold Layer: 1-min window aggregations

// ── STORAGE ────────────────────────────────────────────────
Delta Lake [icon: databricks, color: blue] {
  Checkpoints
  Transaction Logs
}

Spark Streaming > Delta Lake: append (mergeSchema)

// ── ML LAYER ───────────────────────────────────────────────
ML Pipeline [icon: machine-learning, color: green] {
  Feature Store [color: green] {
    feature_store.parquet
  }
  Models [color: green] {
    XGBoost Arbitrage Classifier
    Bidirectional LSTM
    Isolation Forest
    GARCH x5 Symbols
  }
  MLflow Tracking
}

Delta Lake > Feature Store: spreads + prices
Feature Store > Models: 1237 rows / 10 features
Models > MLflow Tracking: metrics + artifacts

// ── SERVING ────────────────────────────────────────────────
FastAPI [icon: fastapi, color: green] {
  /api/v1/prices
  /api/v1/vwap
  /api/v1/volume
  /api/v1/arbitrage
  /api/v1/ml/predictions
  /api/v1/health
}

Streamlit Dashboard [icon: streamlit, color: red] {
  Live Prices
  VWAP Analysis
  Arbitrage Alerts
  Volume Analysis
  ML Insights
  Exchange Comparison
}

Delta Lake > FastAPI: pandas delta reader (no Spark)
Models > FastAPI: ArbitragePredictor (lazy load)
FastAPI > Streamlit Dashboard: HTTP / REST
```

---

## Data Flow

```
Exchange WebSockets
        │
        ▼
  Kafka Broker (localhost:9092)
  ├── raw-trades
  ├── raw-ticker
  └── raw-orderbook
        │
        ▼
  Spark Structured Streaming (local[2], 10s micro-batches)
        │
        ├── BRONZE  — raw JSON, minimal transform
        │   ├── data/bronze/trades
        │   └── data/bronze/ticker
        │
        ├── SILVER  — parsed, symbol-normalized, null-filtered
        │   └── data/silver/prices       (1M+ rows)
        │
        └── GOLD    — 1-minute window aggregations
            ├── data/gold/vwap           (VWAP per symbol/exchange)
            ├── data/gold/spreads        (cross-exchange spreads)
            └── data/gold/arbitrage_signals  (spread > 0.15%)
                    │
                    ▼
         FastAPI  (:8000/api/v1)   +   ML Predictions
                    │
                    ▼
         Streamlit Dashboard (:8501)
```

---

## Medallion Layers

| Layer | Tables | Key Columns | Trigger |
|---|---|---|---|
| **Bronze** | `trades`, `ticker` | `raw_message`, `ingestion_timestamp` | 10s |
| **Silver** | `prices` | `symbol`, `exchange`, `price`, `volume`, `event_time` | 10s |
| **Gold** | `vwap` | `symbol`, `exchange`, `vwap`, `window_start`, `window_end` | 10s |
| **Gold** | `spreads` | `exchange_a`, `exchange_b`, `spread_abs`, `spread_pct` | 10s |
| **Gold** | `arbitrage_signals` | same as spreads + `signal_timestamp` | 10s |

---

## ML Models

| Model | Task | Input | Output | Artifact |
|---|---|---|---|---|
| **XGBoost** | Arbitrage classification | 10 spread/vol features | P(arbitrage) | `xgboost_arbitrage.pkl` |
| **BiLSTM** | Price direction | 60-step sequences, 6 features | Up/Down (69.5% acc) | `lstm_price_direction.pt` |
| **Isolation Forest** | Anomaly detection | 3 features (spread dev, vol spike, imbalance) | Anomaly score | `isolation_forest.pkl` |
| **GARCH(1,1)** | Volatility forecast | Log returns per symbol | σ² forecast | `garch_{SYMBOL}.pkl` |

Training: chronological 70/15/15 split, no shuffling. Tracked via MLflow.

---

## Services

| Service | Runtime | Port | Role |
|---|---|---|---|
| Zookeeper | Docker | 2181 | Kafka coordination |
| Kafka | Docker | 9092 | Message broker |
| Kafka UI | Docker | 8080 | Topic monitoring |
| Binance Producer | Docker | — | WebSocket → Kafka |
| Coinbase Producer | Docker | — | WebSocket → Kafka |
| Kraken Producer | Docker | — | WebSocket → Kafka |
| Spark Streaming | Native (JVM) | — | Bronze/Silver/Gold ETL |
| FastAPI | Native | 8000 | REST API + ML serving |
| Streamlit | Native | 8501 | Analytics dashboard |

---

## API Endpoints

| Method | Path | Source | Description |
|---|---|---|---|
| GET | `/api/v1/health` | — | Liveness check |
| GET | `/api/v1/health/ready` | Delta tables | Readiness + table status |
| GET | `/api/v1/prices` | Silver | Latest prices, all exchanges |
| GET | `/api/v1/prices/{symbol}` | Silver | Latest price for symbol |
| GET | `/api/v1/prices/{symbol}/history` | Silver | Historical prices |
| GET | `/api/v1/vwap` | Gold/vwap | VWAP metrics |
| GET | `/api/v1/vwap/{symbol}/history` | Gold/vwap | Historical VWAP |
| GET | `/api/v1/volume` | Gold/volume | Volume aggregates |
| GET | `/api/v1/volume/rankings` | Gold/volume | Exchange volume rankings |
| GET | `/api/v1/arbitrage` | Gold/arbitrage | All signals |
| GET | `/api/v1/arbitrage/active` | Gold/arbitrage | Live opportunities |
| GET | `/api/v1/ml/predictions/{symbol}` | ML models | LSTM direction forecast |
| GET | `/api/v1/ml/arbitrage/live` | ML models | XGBoost arb probability |
| GET | `/api/v1/ml/anomalies/recent` | ML models | Isolation forest anomalies |
| GET | `/api/v1/ml/volatility/{symbol}` | ML models | GARCH volatility forecast |

---

## Startup

```bash
./start.sh                    # Full stack (Kafka + Spark + API + Dashboard)
./start.sh --no-spark         # Serving only (reads existing Delta files)
./stop.sh                     # Graceful shutdown of all services
```

**On 16 GB RAM:** Run Docker + Spark together only when collecting data. For everyday use, stop Docker and Spark — the API and dashboard read directly from Delta files on disk without needing either.
