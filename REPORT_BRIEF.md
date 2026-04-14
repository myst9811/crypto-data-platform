# Final Year Project — Report Generation Brief

> Feed this document into your Claude.ai project alongside the sample report and poster.
> Instruct Claude.ai: "Using the sample report as a template for structure, formatting, and academic tone, write my complete Final Year Engineering Report using all the technical details in this brief."

---

## Project Title

**Real-Time Cryptocurrency Analytics Platform with Machine Learning-Driven Arbitrage Detection**

## Student

Shannen Saikia

---

## Project Summary

This project delivers a production-grade, end-to-end data engineering and machine learning platform for real-time cryptocurrency market analytics. The system ingests live trade and ticker data from three major exchanges (Binance, Coinbase, Kraken) via WebSocket connections, processes it through a medallion data lakehouse architecture using Apache Spark Structured Streaming and Delta Lake, and exposes insights through a REST API and an interactive multi-page dashboard. Four machine learning models are trained on the processed data to detect arbitrage opportunities, forecast price direction, identify market anomalies, and model volatility.

---

## Motivation and Problem Statement

Cryptocurrency markets operate 24/7 across dozens of exchanges simultaneously. Price discrepancies between exchanges — known as arbitrage opportunities — are short-lived (often seconds) and require infrastructure capable of ingesting, processing, and analysing data at sub-minute latency to be actionable. Existing retail tools either show stale aggregated data or require expensive proprietary subscriptions.

The core problem this project addresses: **how can a single developer build a low-latency, scalable, ML-augmented crypto analytics platform on commodity hardware using open-source tools?**

Key challenges:
- Multi-exchange WebSocket management with reconnection logic
- Real-time stream processing without a cloud cluster
- Cross-exchange spread computation without Spark self-join limitations
- ML model training on continuously arriving streaming data
- Serving predictions at API latency without a second JVM process

---

## Objectives

1. Ingest live market data from Binance, Coinbase, and Kraken simultaneously via WebSocket
2. Design and implement a Bronze → Silver → Gold medallion data lakehouse using Delta Lake
3. Compute cross-exchange spreads and VWAP aggregates in real time using Spark Structured Streaming
4. Train four ML models (XGBoost, LSTM, Isolation Forest, GARCH) on processed data
5. Expose all data and predictions through a documented REST API (FastAPI)
6. Build an interactive analytics dashboard (Streamlit) consuming the API
7. Validate end-to-end latency from exchange tick to dashboard display

---

## Literature Review Topics to Cover

- **Medallion Architecture**: Databricks' lakehouse pattern (Bronze/Silver/Gold) — cite Armbrust et al. (Delta Lake paper, VLDB 2020)
- **Spark Structured Streaming**: micro-batch processing model, watermarking, exactly-once semantics
- **Cryptocurrency arbitrage**: triangular vs. cross-exchange arbitrage, market efficiency hypothesis, latency constraints
- **VWAP**: Volume-Weighted Average Price as an institutional benchmark (Harris 2003)
- **XGBoost**: Chen & Guestrin (2016), use in financial classification
- **LSTM for time series**: Hochreiter & Schmidhuber (1997), use in price direction prediction
- **Isolation Forest**: Liu et al. (2008), anomaly detection in financial data
- **GARCH**: Bollerslev (1986), volatility clustering in crypto markets
- **Apache Kafka**: log-structured messaging, at-least-once delivery guarantees
- **Delta Lake vs. traditional data warehouses**: ACID transactions on object storage

---

## System Architecture

### Overview

```
Exchange WebSockets → Kafka → Spark Streaming → Delta Lake → FastAPI → Streamlit
                                                         ↓
                                               ML Pipeline (offline training)
                                                         ↓
                                               FastAPI ML endpoints (online serving)
```

### Component Breakdown

**Ingestion Layer (Docker)**
- Three Python WebSocket producers run in Docker containers
- Binance: subscribes to `@trade`, `@depth20@100ms`, `@ticker` streams for 5 pairs
- Coinbase: subscribes to `matches`, `ticker` channels
- Kraken: subscribes to `trade`, `ticker` channels
- All producers publish to Kafka topics: `raw-trades`, `raw-ticker`, `raw-orderbook`
- Kafka configured with 3 partitions per topic, 7-day retention, Snappy compression
- Reconnection logic with exponential backoff in base producer class

**Processing Layer (Apache Spark 3.5, local[2] mode)**
- Single SparkSession with Delta Lake 3.1 extensions
- `PYSPARK_PYTHON` pinned to venv interpreter to prevent version mismatch
- 5 concurrent streaming queries (2 Bronze, 1 Silver, 2 Gold + foreachBatch)

  *Bronze*: Reads raw Kafka bytes, minimal transformation, appends to Delta
  - `data/bronze/trades` — trade messages
  - `data/bronze/ticker` — ticker messages

  *Silver*: Parses JSON against typed PySpark schemas, normalises symbols, filters nulls
  - Symbol normalisation map: `BTCUSDT→BTC/USD`, `XBT/USD→BTC/USD`, `ETH-USD→ETH/USD` etc.
  - `data/silver/prices` — unified price stream, ~1M rows per session

  *Gold*: 1-minute tumbling windows, 10-second watermark, 10-second trigger
  - `data/gold/vwap` — VWAP, total_volume, num_trades, min/max/avg/std price per symbol per exchange
  - `data/gold/spreads` — cross-exchange spread (absolute + %) computed via foreachBatch + pandas
  - `data/gold/arbitrage_signals` — spread rows exceeding 0.15% threshold

**Key Engineering Decision — Pandas in foreachBatch:**
PySpark self-joins on streaming DataFrames produce AMBIGUOUS_COLUMN_REFERENCE errors because both sides of the join share identical internal column IDs when derived from the same base DataFrame. The solution was to convert each micro-batch to a pandas DataFrame inside `foreachBatch`, compute the cross-exchange cartesian product in Python, then write the result back as a Spark DataFrame. This is valid because Gold micro-batches are small (tens of rows per 10-second trigger).

**Storage Layer (Delta Lake)**
- All tables stored as Parquet + Delta transaction log on local filesystem
- `mergeSchema=true` on writes to handle schema evolution
- Checkpoints per stream for fault tolerance and exactly-once semantics
- API reads via `deltalake` Python package (no Spark session — avoids JVM conflict)

**ML Layer**

*Feature Engineering* (`ml/features/feature_extractor.py`):
- Loads `data/gold/spreads` and `data/silver/prices`
- Computes rolling volatility (15s and 60s windows) from log returns
- Adds cyclical time features: `sin(hour/24 × 2π)`, `cos(hour/24 × 2π)`
- Merges using `pandas.merge_asof` (backward-fill) on `event_time`
- Saves to `data/features/feature_store.parquet` (1,237 rows from one session)

*Label Generation* (`ml/training/label_generator.py`):
- Binary label: does the spread at T + execution_latency (200ms default) exceed threshold?
- Adaptive fallback: if fixed threshold (0.15%) produces zero positives (market too efficient), falls back to 75th-percentile threshold — ensures the model always sees both classes
- Shift computed from median row interval to avoid assuming fixed tick frequency

*XGBoost Arbitrage Classifier*:
- 10 input features: spread_abs, spread_pct, price_a, price_b, rolling_vol_15s, rolling_vol_60s, time_sin, time_cos, garch_forecast, latency_ms
- Hyperparameters: n_estimators=200, max_depth=5, learning_rate=0.05
- Auto-computed scale_pos_weight for class imbalance: n_neg / n_pos
- Chronological 70/15/15 split (no shuffling — prevents leakage)
- Result: **98.4% F1, 100% recall on test set (186 samples, 31 positive)**
- Saved to: `ml/artifacts/xgboost_arbitrage.pkl`
- MLflow experiment: `arbitrage_classifier`

*Bidirectional LSTM Price Direction*:
- Architecture: 2-layer BiLSTM, hidden_size=64, dropout=0.2, input_size=6
- Sequence length: 60 timesteps; features: price, volume, spread, rolling_vol, time_sin, time_cos
- Label: binary direction (up/down) at T+30 seconds, discards moves < 0.05%
- Training capped at most recent 50,000 silver rows to prevent memory exhaustion (~1.4 GB otherwise)
- 10 epochs, Adam optimiser (lr=1e-3), BCELoss
- Result: **69.5% directional accuracy on test set** (random baseline = 50%)
- Saved to: `ml/artifacts/lstm_price_direction.pt`
- MLflow experiment: `price_direction_lstm`

*Isolation Forest Anomaly Detector*:
- 3 features: spread deviation from rolling mean, volume spike ratio, orderbook imbalance proxy
- contamination=0.05 (5% expected anomaly rate)
- Result: **4.77% anomaly rate** on live data (consistent with contamination prior)
- Saved to: `ml/artifacts/isolation_forest.pkl`
- MLflow experiment: `anomaly_detection`

*GARCH(1,1) Volatility Models*:
- One model per symbol: BTC/USD, ETH/USD, SOL/USD, XRP/USD, BNB/USD
- Input: log returns computed from silver prices
- Convergence warnings on 3/5 symbols due to inequality constraints in SLSQP optimiser — acknowledged limitation of GARCH on high-frequency crypto data with extreme kurtosis
- AIC values: ETH/USD −3,236,945 | BTC/USD −2,221,745 | BNB/USD −739,778 | SOL/USD −455,371
- Saved to: `ml/artifacts/garch_{SYMBOL}.pkl`

**Serving Layer**

*FastAPI* (Uvicorn, port 8000):
- 7 routers, 15 endpoints, base path `/api/v1`
- Dependency injection via `reader_dependency`: singleton `PandasDeltaReader`
- `PandasDeltaReader` uses `deltalake` Python package — no Spark, no JVM, no port conflicts
- Column harmonisation in `_read_delta()`: maps `symbol→standard_symbol`, `event_time→timestamp` to bridge pipeline output schema with API layer expectations
- Response models: Pydantic v2 with `from_attributes=True`
- Cache: TTL-based query cache (10s default) to avoid re-reading Delta on every request
- ML endpoints lazy-load model artifacts on first request, cache `ArbitragePredictor` instance

*Streamlit Dashboard* (port 8501):
- 6 pages: Live Prices, VWAP Analysis, Arbitrage Alerts, Volume Analysis, ML Insights, Exchange Comparison
- All pages poll the FastAPI via `requests` with 2-second refresh
- Live Prices page: symbol metric cards + per-exchange pivot tables
- ML Insights page: displays XGBoost probability, LSTM direction, anomaly flags, GARCH σ²

---

## Implementation Details

### Technologies and Versions

| Component | Technology | Version |
|---|---|---|
| Language | Python | 3.12.13 |
| Stream Processing | Apache Spark | 3.5.1 |
| Lakehouse Storage | Delta Lake | 3.1.0 |
| Message Broker | Apache Kafka | 3.4.1 (via Confluent CP 7.5.0) |
| ML — Gradient Boosting | XGBoost | 2.x |
| ML — Deep Learning | PyTorch | 2.x |
| ML — Anomaly Detection | scikit-learn | 1.x |
| ML — Volatility | arch | 6.x |
| Experiment Tracking | MLflow | 3.x |
| API Framework | FastAPI | 0.110+ |
| Dashboard | Streamlit | 1.x |
| Containerisation | Docker + Docker Compose | — |
| Runtime Environment | macOS (Apple Silicon M-series) | — |

### Kafka Topics

| Topic | Partitions | Retention | Compression |
|---|---|---|---|
| raw-trades | 3 | 7 days | Snappy |
| raw-ticker | 3 | 7 days | Snappy |
| raw-orderbook | 3 | 7 days | Snappy |

### Delta Lake Schema — Silver Prices

| Column | Type | Description |
|---|---|---|
| symbol | String | Normalised pair (e.g. BTC/USD) |
| exchange | String | binance / coinbase / kraken |
| price | Double | Trade price |
| volume | Double | Trade volume |
| event_time | Timestamp | Exchange timestamp |
| kafka_timestamp | Timestamp | Kafka ingestion timestamp |

### Delta Lake Schema — Gold VWAP

| Column | Type | Description |
|---|---|---|
| symbol | String | Trading pair |
| exchange | String | Exchange name |
| vwap | Double | Volume-weighted average price |
| total_volume | Double | Sum of volume in window |
| total_value | Double | Sum of price×volume |
| num_trades | Long | Trade count |
| min_price | Double | Window minimum |
| max_price | Double | Window maximum |
| avg_price | Double | Window arithmetic mean |
| std_dev_price | Double | Price standard deviation |
| window_start | Timestamp | Window open |
| window_end | Timestamp | Window close |

### Delta Lake Schema — Gold Spreads

| Column | Type | Description |
|---|---|---|
| symbol | String | Trading pair |
| exchange_a | String | First exchange (alphabetically) |
| exchange_b | String | Second exchange |
| price_a | Double | Average price on exchange_a |
| price_b | Double | Average price on exchange_b |
| spread_abs | Double | \|price_b − price_a\| |
| spread_pct | Double | spread_abs / min(price_a, price_b) |
| event_time | Timestamp | Window end time |
| window_start | Timestamp | Window open |
| window_end | Timestamp | Window close |

---

## Results and Evaluation

### Data Volume (Single Session)

| Table | Rows |
|---|---|
| silver/prices | 997,474 |
| gold/vwap | 1,428 |
| gold/spreads | 1,189 |
| gold/arbitrage_signals | 0 (spreads below 0.15% threshold — market efficient) |
| feature_store.parquet | 1,237 |

### ML Model Results

| Model | Metric | Value |
|---|---|---|
| XGBoost | F1 Score | 0.984 |
| XGBoost | AUC-ROC | 1.0 |
| XGBoost | Recall (class 1) | 1.0 |
| LSTM | Directional Accuracy | 69.5% |
| LSTM | RMSE | 0.4608 |
| Isolation Forest | Anomaly Rate | 4.77% |
| GARCH (ETH/USD) | AIC | −3,236,945 |
| GARCH (BTC/USD) | AIC | −2,221,745 |

### API Performance
- Health check (`/health`): <5ms response time
- Prices endpoint (`/prices?limit=200`): <100ms (cached delta read)
- ML prediction endpoint: <200ms (model already loaded in memory)

### End-to-End Latency
- Exchange tick → Kafka: ~50ms (WebSocket RTT)
- Kafka → Bronze Delta: ~10s (micro-batch trigger)
- Bronze → Silver → Gold: ~20s (two additional trigger cycles)
- Gold → API → Dashboard: <100ms
- **Total end-to-end: ~30 seconds** (dominated by Spark micro-batch interval)

---

## Challenges and Solutions

| Challenge | Root Cause | Solution |
|---|---|---|
| PySpark PYTHON_VERSION_MISMATCH | macOS system Python 3.14 used for Spark workers instead of venv Python 3.12 | Set `PYSPARK_PYTHON=sys.executable` before SparkSession creation |
| AMBIGUOUS_COLUMN_REFERENCE on Gold spreads | Self-join of streaming DataFrame — both sides share internal column IDs | Replaced PySpark self-join with pandas cross-exchange computation inside `foreachBatch` |
| DELTA_SCHEMA_NOT_SET on Gold startup | Gold streams started readStream from silver/prices before Silver wrote first batch | Added `_wait_for_silver()` polling loop (120s timeout) before starting Gold streams |
| JVM conflict between Spark and API | API's dependency injection tried to start a second SparkSession in same process | Forced API to always use `PandasDeltaReader` (deltalake Python package, no JVM) |
| All-zero ML labels | Live spreads (max 0.143%) below the 0.15% label threshold → no positive class | Added adaptive 75th-percentile fallback in `label_generator.py` |
| LSTM training hung (1M+ sequences) | Building sequences from all silver rows (~1.4 GB numpy array) before training | Capped `prepare_data()` to most recent 50,000 rows; reduced epochs 30→10 |
| GARCH convergence warnings | SLSQP inequality constraints incompatible with high-kurtosis crypto return distribution | Acknowledged limitation; models still produce AIC/BIC; noted in evaluation |
| `/health/ready` Pydantic validation error | `HealthResponse` model expected bool but got string in error branch | Changed endpoint return type to plain `dict` instead of typed Pydantic model |

---

## System Limitations

1. **Arbitrage signals empty in practice**: Real crypto markets are highly efficient. The 0.15% threshold is never crossed in normal conditions due to high-frequency arbitrageurs. The platform correctly detects and would alert on larger dislocations during volatility events.

2. **Single-node Spark**: Running `local[2]` limits throughput. A production deployment would use a Spark cluster or Databricks.

3. **No order book processing**: The `raw-orderbook` topic is ingested to Bronze but not yet processed to Silver/Gold. True arbitrage execution requires order book depth analysis.

4. **LSTM latency**: A 30-second prediction horizon is too slow for HFT but suitable for medium-frequency strategies.

5. **Memory constraints**: Full stack requires ~5-6 GB RAM. On a 16 GB machine, simultaneous Spark + Docker + training is borderline.

6. **GARCH on tick data**: GARCH(1,1) assumes covariance stationarity — crypto tick data exhibits regime changes and fat tails that violate this assumption.

---

## Future Work

- **Order book Silver/Gold processing**: Compute bid-ask spread, depth imbalance, and liquidity scores from the existing `raw-orderbook` topic
- **Cloud deployment**: Migrate Kafka to Confluent Cloud, Spark to Databricks, Delta Lake to S3/GCS — all open-source components support this
- **River online learning**: Integrate `river` Adaptive Random Forest for concept drift detection as market regimes change
- **WebSocket latency measurement**: Instrument per-exchange WebSocket RTT and expose as a feature for the arbitrage classifier
- **Backtesting engine**: Replay historical Delta Lake data through the ML pipeline to simulate strategy P&L
- **Alert system**: Webhook/Slack notification when arbitrage signals exceed configurable thresholds

---

## Project Repository Structure

```
crypto-data-platform/
├── src/
│   ├── ingestion/          # Binance, Coinbase, Kraken WebSocket producers
│   ├── processing/         # Spark Structured Streaming (Bronze/Silver/Gold)
│   └── serving/
│       ├── api/            # FastAPI application, routes, schemas, dependencies
│       ├── dashboard/      # Streamlit 6-page dashboard
│       └── data_access/    # PandasDeltaReader, Pydantic models, cache
├── ml/
│   ├── features/           # Feature extraction and feature store
│   ├── training/           # XGBoost, LSTM, Isolation Forest, GARCH trainers
│   ├── serving/            # ArbitragePredictor, ModelRegistry
│   ├── evaluation/         # Metrics, walk-forward CV, baselines
│   └── artifacts/          # Trained model files (.pkl, .pt)
├── data/
│   ├── bronze/             # Raw Delta tables
│   ├── silver/             # Normalised Delta tables
│   ├── gold/               # Aggregated Delta tables
│   ├── checkpoints/        # Spark streaming fault-tolerance
│   └── features/           # Feature store (Parquet)
├── config/                 # spark_config.yaml, kafka_topics.yaml, exchanges.yaml
├── docker/                 # docker-compose.yml, Dockerfiles, init-topics.sh
├── tests/                  # Unit tests (pipeline, features, labels)
├── ARCHITECTURE.md         # Eraser.io diagram + full system reference
├── start.sh                # One-command startup script
└── stop.sh                 # Graceful shutdown
```

---

## Key Academic Contributions

1. **Medallion streaming architecture on a single node**: Demonstrated that the Bronze→Silver→Gold pattern can run end-to-end on a MacBook (Apple Silicon) using Spark local mode, with Delta Lake providing ACID guarantees without a distributed cluster.

2. **Pandas-in-foreachBatch pattern**: Documented and solved the PySpark self-join ambiguity limitation in streaming contexts — a practical engineering solution not well-covered in existing literature.

3. **Adaptive label generation for imbalanced financial data**: Proposed a percentile-fallback mechanism for binary label generation when the fixed threshold produces no positive examples due to market efficiency — ensures ML training is always meaningful regardless of market conditions.

4. **End-to-end ML pipeline on streaming data**: From raw WebSocket ticks to trained XGBoost/LSTM/Isolation Forest/GARCH models, with MLflow tracking, served via REST API — all within a single open-source Python stack.

---

## Suggested Report Structure

1. Abstract
2. Introduction (motivation, objectives, scope)
3. Background and Literature Review (Kafka, Spark, Delta Lake, medallion architecture, arbitrage, ML models)
4. System Design and Architecture (diagram, component breakdown, design decisions)
5. Implementation (ingestion, processing pipeline, ML pipeline, serving layer)
6. Results and Evaluation (data volumes, ML metrics, latency, API performance)
7. Discussion (challenges, limitations, comparison to objectives)
8. Conclusion and Future Work
9. References
10. Appendices (API endpoint table, full schema definitions, config files)
