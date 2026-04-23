# Crypto Data Platform — Full Project Context

> Feed this document into Claude.ai to give it complete context of the repository.
> It covers architecture, all implementation decisions, every module, data schemas, ML pipeline, API, and known limitations.

---

## 1. Project Summary

**Real-Time Cryptocurrency Analytics Platform with Machine Learning-Driven Arbitrage Detection**

A production-grade, end-to-end data engineering and ML platform built on a single developer machine (macOS, Apple Silicon) using open-source tools. It:

- Streams live trade and ticker data from **Binance, Coinbase, and Kraken** via WebSocket
- Processes 1M+ price events per session through a **Bronze → Silver → Gold** Delta Lake medallion pipeline powered by Apache Spark Structured Streaming
- Computes **cross-exchange spreads, VWAP, and volume aggregates** in 1-minute rolling windows
- Detects **arbitrage opportunities** (spread > 0.15%) in real time
- Trains **four ML models**: XGBoost arbitrage classifier, Bidirectional LSTM price direction forecast, Isolation Forest anomaly detector, GARCH(1,1) volatility model
- Serves all data and predictions through a **FastAPI REST API** (15 endpoints)
- Visualises everything in a **6-page Streamlit dashboard**

**Supported pairs:** `BTC/USD · ETH/USD · BNB/USD · SOL/USD · XRP/USD` across Binance, Coinbase, and Kraken.

---

## 2. Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.12 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.1 |
| Lakehouse Storage | Delta Lake | 3.1.0 |
| Message Broker | Apache Kafka (Confluent CP) | 3.4.1 / 7.5.0 |
| ML — Gradient Boosting | XGBoost | 2.x |
| ML — Deep Learning | PyTorch (BiLSTM) | 2.x |
| ML — Anomaly Detection | scikit-learn Isolation Forest | 1.x |
| ML — Volatility | arch (GARCH) | 6.x |
| Experiment Tracking | MLflow | 3.x |
| API Framework | FastAPI + Uvicorn | 0.110+ |
| Dashboard | Streamlit + Plotly | 1.x |
| Containerisation | Docker + Docker Compose | — |
| Delta read (no Spark) | deltalake Python package | — |
| Runtime | macOS Apple Silicon (M-series) | — |
| Java | OpenJDK (for Spark JVM) | 17 |

---

## 3. Repository Structure

```
crypto-data-platform/
├── src/
│   ├── ingestion/          # Exchange WebSocket producers (Binance, Coinbase, Kraken)
│   │   ├── base_producer.py      # Abstract base: schema validation, dead-letter, backoff
│   │   ├── binance_producer.py
│   │   ├── coinbase_producer.py
│   │   ├── kraken_producer.py
│   │   └── config.py
│   ├── processing/         # Spark Structured Streaming pipeline
│   │   ├── spark_streaming.py    # CryptoStreamingApp: Bronze/Silver/Gold ETL
│   │   ├── schemas.py
│   │   └── transformations/
│   │       ├── aggregations.py
│   │       ├── arbitrage.py      # Fee definitions for transformations
│   │       └── normalizer.py
│   └── serving/
│       ├── api/
│       │   ├── main.py           # FastAPI app, routers, CORS, rate limiting
│       │   ├── auth.py           # Bearer token auth (optional, env-gated)
│       │   ├── ratelimit.py      # slowapi rate limiter
│       │   ├── dependencies.py   # Singleton PandasDeltaReader injection
│       │   ├── validators.py
│       │   └── routes/           # health, prices, vwap, volume, arbitrage, ml, liquidity
│       ├── dashboard/
│       │   ├── app.py            # Streamlit home page
│       │   ├── config.py         # DashboardConfig
│       │   └── pages/            # 6 Streamlit pages (Price Monitor, VWAP, Arbitrage, Volume, Liquidity, Exchange Comparison)
│       ├── data_access/
│       │   ├── pandas_delta_reader.py  # Spark-free Delta reader (deltalake package)
│       │   ├── models.py               # Pydantic v2 response models
│       │   ├── cache.py                # TTL-based query cache
│       │   └── delta_reader.py
│       └── config.py             # ServingConfig (paths, env, CORS, etc.)
├── ml/
│   ├── features/
│   │   ├── feature_extractor.py  # Delta → pandas → feature_store.parquet
│   │   └── feature_store.py      # Load cached features
│   ├── training/
│   │   ├── label_generator.py    # Fee-net future-profit labels (no leakage)
│   │   ├── train_xgboost.py      # Arbitrage binary classifier
│   │   ├── train_isolation_forest.py
│   │   ├── train_garch.py        # Per-symbol GARCH(1,1)
│   │   └── train_lstm.py         # Bidirectional LSTM direction forecast
│   ├── evaluation/
│   │   ├── walk_forward_cv.py    # Chronological expanding-window splits
│   │   ├── metrics.py            # Classifier + regression metrics helpers
│   │   └── baseline.py           # Rule-based baseline comparisons
│   ├── serving/
│   │   ├── predictor.py          # ArbitragePredictor: full 4-model inference pipeline
│   │   ├── online_learner.py     # River adaptive forest (online learning)
│   │   └── model_registry.py     # MLflow query helpers
│   ├── mlflow_setup.py
│   ├── utils/
│   │   └── safe_artifact.py      # HMAC signing/verification of model artifacts
│   └── artifacts/                # Trained model files (.pkl, .pt) — gitignored
├── data/
│   ├── bronze/             # Raw Kafka messages (Delta Lake)
│   ├── silver/             # Normalised prices (Delta Lake)
│   ├── gold/               # Aggregated analytics (Delta Lake)
│   ├── checkpoints/        # Spark streaming fault-tolerance
│   └── features/           # feature_store.parquet
├── config/
│   ├── spark_config.yaml
│   ├── kafka_topics.yaml
│   └── exchanges.yaml
├── docker/
│   ├── docker-compose.yml  # Zookeeper, Kafka, Kafka-UI, 3 exchange producers
│   ├── kafka/init-topics.sh
│   └── producer/Dockerfile
├── tests/                  # pytest tests
├── scripts/                # start_spark.sh, figure generation scripts
├── docs/figures/           # Report figures (PNG)
├── start.sh / stop.sh
└── ARCHITECTURE.md / README.md / CHECKPOINT.md / REPORT_BRIEF.md
```

---

## 4. Data Pipeline

### 4.1 System Data Flow

```
Exchange WebSockets (Binance / Coinbase / Kraken)
        │ WebSocket streams
        ▼
  Apache Kafka  (raw-trades · raw-ticker · raw-orderbook)
  3 partitions, 7-day retention, Snappy compression
        │ Kafka source (10s micro-batches)
        ▼
  Spark Structured Streaming  [local[2], 2GB driver]
  ├── BRONZE  raw-trades  →  data/bronze/trades    (Delta)
  ├── BRONZE  raw-ticker  →  data/bronze/ticker    (Delta)
  ├── SILVER  parse+normalise  →  data/silver/prices  (Delta, ~1M rows/session)
  ├── GOLD    1-min window  →  data/gold/vwap      (Delta)
  ├── GOLD    foreachBatch  →  data/gold/spreads   (Delta)
  └── GOLD    foreachBatch  →  data/gold/arbitrage_signals  (Delta)
        │
        ├──► ML Pipeline  →  feature_store.parquet  →  4 trained models
        │
        └──► FastAPI (:8000)  →  Streamlit Dashboard (:8501)
```

### 4.2 Medallion Layers

**Bronze** — Raw Kafka messages written to Delta with minimal transformation. `raw-trades` and `raw-ticker` topics stored separately. Each row has: `key`, `value` (raw JSON string), `topic`, `kafka_timestamp`, `processing_timestamp`. No schema parsing — preserves original payloads for replay.

**Silver** — JSON parsed against typed PySpark schemas (`TRADE_SCHEMA`, `TICKER_SCHEMA`). Symbol normalisation applied via UDF. Null prices filtered. Union of trade and ticker sources. Single unified price stream.

Schema — `data/silver/prices`:
| Column | Type | Description |
|---|---|---|
| symbol | String | Normalised pair (e.g. BTC/USD) |
| exchange | String | binance / coinbase / kraken |
| price | Double | Trade/last price |
| volume | Double | Trade volume |
| event_time | Timestamp | Exchange timestamp (falls back to kafka_timestamp) |

**Gold — VWAP** — 1-minute tumbling window per symbol per exchange. Schema: `symbol`, `exchange`, `vwap`, `total_volume`, `total_value`, `num_trades`, `min_price`, `max_price`, `avg_price`, `std_dev_price`, `window_start`, `window_end`, `window_duration`.

**Gold — Spreads** — Cross-exchange spread computed via `foreachBatch` + pandas (avoids PySpark self-join ambiguity). Schema: `symbol`, `exchange_a`, `exchange_b`, `price_a`, `price_b`, `spread_abs`, `spread_pct`, `event_time`, `window_start`, `window_end`.

**Gold — Arbitrage Signals** — Subset of spreads where `spread_pct > 0.0015` (0.15%). Same schema as spreads plus `signal_timestamp`.

### 4.3 Symbol Normalisation Map

```python
SYMBOL_MAP = {
    "binance":  {"BTCUSDT": "BTC/USD", "ETHUSDT": "ETH/USD", "BNBUSDT": "BNB/USD",
                 "SOLUSDT": "SOL/USD", "XRPUSDT": "XRP/USD"},
    "coinbase": {"BTC-USD": "BTC/USD", "ETH-USD": "ETH/USD",
                 "SOL-USD": "SOL/USD", "XRP-USD": "XRP/USD"},
    "kraken":   {"XBT/USD": "BTC/USD", "XBTUSD": "BTC/USD",
                 "ETH/USD": "ETH/USD", "ETHUSD": "ETH/USD",
                 "SOL/USD": "SOL/USD", "SOLUSD": "SOL/USD",
                 "XRP/USD": "XRP/USD", "XRPUSD": "XRP/USD"},
}
```

---

## 5. Ingestion Layer

### 5.1 Base Producer (`src/ingestion/base_producer.py`)

Abstract class all exchange producers inherit from. Key responsibilities:

- **Schema validation**: checks required fields per message type (`type`, `symbol`, `price`, `volume` for trades; `type`, `symbol`, `last_price` for ticker). Missing fields → message sent to `raw-dead-letter` Kafka topic.
- **Exponential backoff reconnection**: on WebSocket close, retries with `initial_delay * backoff_multiplier^retry_count` delay, up to `max_retries=5`.
- **Metadata enrichment**: adds `exchange` name and `ingestion_timestamp` to every message.
- **Raw message opt-in**: `INGEST_INCLUDE_RAW_MESSAGE` env var (default false) controls whether the raw exchange payload is stored in Kafka — off by default to reduce lake size.

### 5.2 Exchange-Specific Producers

- **Binance**: subscribes to `@trade`, `@depth20@100ms`, `@ticker` streams for 5 pairs
- **Coinbase**: subscribes to `matches`, `ticker` channels
- **Kraken**: subscribes to `trade`, `ticker` channels

All run in Docker containers (defined in `docker/docker-compose.yml`, `platform: linux/amd64`).

### 5.3 Kafka Topics

| Topic | Partitions | Retention | Compression |
|---|---|---|---|
| raw-trades | 3 | 7 days | Snappy |
| raw-ticker | 3 | 7 days | Snappy |
| raw-orderbook | 3 | 7 days | Snappy |
| raw-dead-letter | 1 | — | — |

---

## 6. Processing Layer — Spark Streaming

### 6.1 SparkSession Configuration

```
master: local[2]
spark.driver.memory: 2g
spark.sql.shuffle.partitions: 4
spark.jars.packages: io.delta:delta-spark_2.12:3.1.0, org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.retentionDurationCheck.enabled: false
```

**Critical fix**: `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` are pinned to `sys.executable` before SparkSession creation. Without this, macOS system Python (3.14+) is used for workers instead of the venv Python 3.12, causing `PYTHON_VERSION_MISMATCH` errors.

### 6.2 Streaming Query Orchestration (`CryptoStreamingApp`)

1. `_start_bronze()` — 2 queries (trades, ticker)
2. `_start_silver()` — 1 query (prices union)
3. `_wait_for_silver(timeout=120s)` — polls for `data/silver/prices/_delta_log` before starting Gold. If timeout, seeds empty Delta schema so Gold can start.
4. `_start_gold()` — 3 queries (VWAP, spreads, arbitrage_signals)

5 total concurrent streaming queries, all with 10-second `processingTime` triggers.

### 6.3 Key Engineering Decision — pandas in foreachBatch

PySpark self-joins on streaming DataFrames produce `AMBIGUOUS_COLUMN_REFERENCE` errors because both sides of a self-join share internal column IDs when derived from the same base DataFrame. The solution: convert each micro-batch to pandas inside `foreachBatch`, compute the cross-exchange cartesian product in Python, then write the result back. Valid because Gold micro-batches are small (tens of rows per 10-second trigger).

### 6.4 Arbitrage Threshold

```python
if spread_pct > 0.0015:  # 0.15%
```

In practice, live crypto markets are highly efficient — the 0.15% threshold is rarely crossed. The Spark layer correctly computes and would emit signals on larger dislocations during volatility events.

---

## 7. ML Layer

### 7.1 Feature Engineering (`ml/features/feature_extractor.py`)

Loads `data/gold/spreads` and `data/silver/prices` via the `deltalake` package (no Spark). Produces `data/features/feature_store.parquet`.

**10 feature columns:**
| Feature | Source |
|---|---|
| `spread_abs` | Gold/spreads |
| `spread_pct` | Gold/spreads |
| `price_a` | Gold/spreads |
| `price_b` | Gold/spreads |
| `rolling_vol_15s` | Silver/prices — log-return rolling std, window=15 ticks |
| `rolling_vol_60s` | Silver/prices — log-return rolling std, window=60 ticks |
| `time_sin` | `sin(hour_fraction/24 × 2π)` — cyclical time encoding |
| `time_cos` | `cos(hour_fraction/24 × 2π)` — cyclical time encoding |
| `garch_forecast` | Placeholder 0.0 (filled by GARCH model in predictor) |
| `latency_ms` | Placeholder 50.0 (network measurement not yet implemented) |

Merge: `pandas.merge_asof` backward-fill on `event_time` to join volatility onto spreads.

Result: **1,237 rows** from a typical 2–3 hour collection session.

### 7.2 Label Generation (`ml/training/label_generator.py`)

**Important design**: labels are fee-net future profit, not current spread threshold. This avoids feature leakage from `spread_pct` directly into the label.

```
label = 1  iff  future_net_profit > 0

future_net_profit = gross_return(at T + latency) − round_trip_fees
gross_return = |price_b_future − price_a_future| / min(price_a_future, price_b_future)
round_trip_fees = (taker_a + taker_b + withdrawal_a + withdrawal_b) / 100
```

Exchange fees:
```python
EXCHANGE_FEES = {
    "binance":  {"maker": 0.1,  "taker": 0.1,  "withdrawal": 0.0005},
    "coinbase": {"maker": 0.0,  "taker": 0.05, "withdrawal": 0.0},
    "kraken":   {"maker": 0.16, "taker": 0.26, "withdrawal": 0.00015},
}
```

**Adaptive fallback**: if no row produces a positive net profit (thin market / small sample), the labeller falls back to the 75th-percentile of *future* net profit as threshold — ensuring both classes always exist for training. The fallback labels the FUTURE quantity, not any current feature, so no leakage.

**Shift computation**: `shift_rows = max(1, round(execution_latency_ms / median_row_interval))`. Uses median rather than assuming fixed tick frequency.

### 7.3 XGBoost Arbitrage Classifier (`ml/training/train_xgboost.py`)

- **Input**: 10 features above
- **Architecture**: `XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05, scale_pos_weight=n_neg/n_pos, eval_metric="logloss", random_state=42)`
- **Split**: chronological 70/15/15, no shuffling
- **Results**: F1=0.984, AUC-ROC=1.0, Recall(class 1)=1.0 on 186 test samples (31 positive)
- **Artifact**: `ml/artifacts/xgboost_arbitrage.pkl`
- **MLflow experiment**: `arbitrage_classifier`
- HMAC-signed via `safe_artifact.sign_artifact()` if `CRYPTO_MODEL_HMAC_KEY` env is set

### 7.4 Bidirectional LSTM (`ml/training/train_lstm.py`)

- **Task**: binary price direction (up/down) at T+30 seconds
- **Architecture**: 2-layer BiLSTM, `input_size=6`, `hidden_size=64`, `dropout=0.2`, final linear + sigmoid
- **Sequence length**: 60 timesteps
- **Features**: `[price, volume, spread, rolling_vol, time_sin, time_cos]` — z-score normalised
- **Label**: 1 if `pct_change > 0`, else 0. Rows with `|pct_change| < 0.05%` discarded (too small to predict).
- **Memory guard**: capped to most recent 50,000 silver rows — building sequences from 1M+ rows would require ~1.4 GB numpy array
- **Training**: 10 epochs, `Adam(lr=1e-3)`, `BCELoss`, `batch_size=64`, no shuffle
- **Results**: directional accuracy 69.5%, RMSE 0.4608 on test set
- **Artifact**: `ml/artifacts/lstm_price_direction.pt`
- **MLflow experiment**: `price_direction_lstm`

### 7.5 Isolation Forest (`ml/training/train_isolation_forest.py`)

- **Task**: anomaly detection on spread/volume features
- **3 input features**: spread deviation from rolling mean, volume spike ratio, orderbook imbalance proxy
- **`contamination=0.05`** (5% expected anomaly rate)
- **Result**: 4.77% anomaly rate on live data (consistent with prior)
- **Artifact**: `ml/artifacts/isolation_forest.pkl`
- **MLflow experiment**: `anomaly_detection`

### 7.6 GARCH(1,1) Volatility (`ml/training/train_garch.py`)

- One model per symbol: `BTC/USD`, `ETH/USD`, `SOL/USD`, `XRP/USD`, `BNB/USD`
- Input: log returns computed from silver prices
- **Known limitation**: convergence warnings on 3/5 symbols (SLSQP inequality constraints fail with high-kurtosis crypto tick data). Models still produce AIC/BIC values.
- AIC values: ETH/USD −3,236,945 | BTC/USD −2,221,745 | BNB/USD −739,778 | SOL/USD −455,371
- Artifacts: `ml/artifacts/garch_{SYMBOL}.pkl` (e.g. `garch_BTC_USD.pkl`)

### 7.7 ArbitragePredictor (`ml/serving/predictor.py`)

Inference pipeline — all models loaded lazily at first API request, cached in memory:

```
1. IsolationForest anomaly check  →  if anomaly: return early (arb_probability=0)
2. GARCH volatility forecast      →  garch_vol
3. LSTM direction prediction      →  lstm_direction (1=up, 0=down)
4. XGBoost arbitrage probability  →  arb_probability (0.0–1.0)
```

GARCH model loaded by globbing `garch_*.pkl`. Symbol key normalised from filename (`garch_BTC_USD.pkl` → `BTC/USD`). LSTM state dict loaded via `safe_load_torch`.

### 7.8 Artifact Integrity (`ml/utils/safe_artifact.py`)

`sign_artifact(path)` / `safe_load_pickle(path)` — optional HMAC-SHA256 signing of model files. Signed `.sig` sidecar file written alongside artifact. Load fails with `ArtifactIntegrityError` on signature mismatch. Disabled if `CRYPTO_MODEL_HMAC_KEY` env var not set.

### 7.9 Walk-Forward Cross-Validation (`ml/evaluation/walk_forward_cv.py`)

Expanding-window chronological splits. Each fold: train < val < test with no overlap. Subsequent folds have more training data. Used to prevent temporal data leakage in evaluation.

---

## 8. Serving Layer

### 8.1 FastAPI Application (`src/serving/api/main.py`)

- **Base path**: `/api/v1` (from `ServingConfig.API_PREFIX`)
- **Port**: 8000
- **Routers**: health, prices, vwap, volume, liquidity, arbitrage, ml (7 routers, 15 endpoints)
- **Auth**: optional Bearer token via `verify_api_key` dependency. Enabled only if `CRYPTO_API_KEY_HASH` env is set. Health endpoint is always public.
- **Rate limiting**: `slowapi` middleware. Default per-IP per-minute budget from `ServingConfig.RATE_LIMIT_DEFAULT`.
- **CORS**: explicit allowlist from `ServingConfig.CORS_ALLOWED_ORIGINS`
- **Global error handler**: never leaks internal details — always returns `{"detail": "Internal server error"}`
- **Lifecycle**: `lifespan` context manager calls `shutdown()` (releases `PandasDeltaReader`) on teardown

### 8.2 Dependency Injection

`PandasDeltaReader` is a singleton injected via `reader_dependency`. It uses the `deltalake` Python package to read Delta files — **no Spark session, no JVM, no port conflicts with the Spark pipeline**. This was a deliberate design choice after discovering that starting a second SparkSession in the API process caused JVM conflicts.

Column harmonisation in `_read_delta()`: maps `symbol→standard_symbol` and `event_time→timestamp` to bridge Spark pipeline output schema with API layer Pydantic models (which expect `standard_symbol` and `timestamp`).

### 8.3 TTL Cache (`src/serving/data_access/cache.py`)

10-second TTL (default) query cache on `PandasDeltaReader` methods. Cache keys encode query parameters. Avoids re-reading Delta on every HTTP request. Max size configurable.

### 8.4 API Endpoints

| Method | Path | Source | Description |
|---|---|---|---|
| GET | `/api/v1/health` | — | Liveness check |
| GET | `/api/v1/health/ready` | Delta tables | Readiness + table status (returns plain dict, not Pydantic model) |
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
| GET | `/api/v1/ml/arbitrage/live` | ML models | XGBoost arbitrage probability |
| GET | `/api/v1/ml/anomalies/recent` | ML models | Isolation Forest anomaly flags |
| GET | `/api/v1/ml/volatility/{symbol}` | ML models | GARCH volatility forecast |

ML endpoints lazy-load `ArbitragePredictor` on first request and cache the instance.

**Note on `/health/ready`**: This endpoint returns a plain `dict` (not a typed Pydantic model). Using a typed `HealthResponse` model caused validation errors when the error branch returned a string instead of bool. Fixed by returning `dict`.

### 8.5 Streamlit Dashboard (`src/serving/dashboard/`)

- **Port**: 8501
- **6 pages**: Price Monitor, VWAP Analysis, Arbitrage Alerts, Volume Analysis, Liquidity Depth, Exchange Comparison
- All pages poll FastAPI via `requests` with 2-second auto-refresh
- Home page (`app.py`): system status (API health, Silver/Gold layer status), quick KPI cards, page navigation guide
- ML Insights page: displays XGBoost probability, LSTM direction, anomaly flags, GARCH σ²

---

## 9. Infrastructure

### 9.1 Docker Services (`docker/docker-compose.yml`)

| Service | Port | Role |
|---|---|---|
| zookeeper | 2181 | Kafka coordination |
| kafka | 9092 | Message broker |
| kafka-ui | 8080 | Topic monitoring (Provectus UI) |
| binance-producer | — | WebSocket → raw-trades/raw-ticker |
| coinbase-producer | — | WebSocket → raw-trades/raw-ticker |
| kraken-producer | — | WebSocket → raw-trades/raw-ticker |

Exchange producers use `platform: linux/amd64` (needed for Apple Silicon compatibility via Rosetta emulation).

### 9.2 Native Processes (non-Docker)

| Service | Start | Port |
|---|---|---|
| Spark Streaming | `scripts/start_spark.sh` | — |
| FastAPI | `uvicorn src.serving.api.main:app` | 8000 |
| Streamlit | `streamlit run src/serving/dashboard/app.py` | 8501 |

Spark requires Java 17 (`JAVA_HOME` set in start script).

### 9.3 Memory Requirements

Full stack: ~5–6 GB RAM.
- Docker VM (Kafka): ~2 GB
- Spark JVM: ~2–3 GB
- API + Dashboard: ~0.5 GB

On 16 GB machines: avoid running other heavy apps simultaneously. After data collection, stop Spark and Docker — API and dashboard continue working from Delta files on disk (no JVM needed).

---

## 10. Configuration

### 10.1 `config/spark_config.yaml`

```yaml
spark:
  master: "local[2]"
  config:
    spark.driver.memory: "2g"
    spark.sql.shuffle.partitions: 4
    spark.databricks.delta.retentionDurationCheck.enabled: "false"
    spark.streaming.backpressure.enabled: "true"
    spark.serializer: "org.apache.spark.serializer.KryoSerializer"

streaming:
  trigger:
    processing_time: "10 seconds"
  watermark:
    delay: "10 seconds"

kafka:
  bootstrap_servers: "localhost:9092"
  subscribe: "raw-trades,raw-orderbook,raw-ticker"
  starting_offsets: "latest"
  max_offsets_per_trigger: 10000

arbitrage:
  threshold_percent: 0.15
  min_volume: 0.01
  max_spread_age_seconds: 10
```

---

## 11. Tests (`tests/`)

All tests use pytest. Key test files:

**`test_pipeline.py`** (6 tests):
- `test_predictor_loads_with_empty_artifacts` — `ArbitragePredictor` must not raise even when no model files exist
- `test_feature_extractor_output_columns` — validates rolling_vol_15s, rolling_vol_60s, log_return, time_sin, time_cos columns
- `test_generate_labels_uses_future_net_profit` — hand-crafted 5-row frame, verifies labels = [1, 0, 0]
- `test_generate_labels_fees_can_flip_label_to_zero` — Kraken round-trip fees (0.52%) eat into 0.3% gross spread → label=0
- `test_generate_labels_no_direct_spread_leakage` — flat current spread=0 with future spike still produces label=1
- `test_compute_trade_profit_pct_direction_and_fees` — fee calc verified to 9 decimal places
- `test_walk_forward_splits_chronological` — 5 folds, expanding window, no temporal overlap

**`test_api_auth.py`**, **`test_api_ratelimit.py`**, **`test_api_security.py`** — API-layer tests for authentication, rate limiting, and security (no info leakage in 500 responses).

**`test_online_learner_validation.py`**, **`test_safe_artifact.py`**, **`test_symbol_validation.py`** — validation tests for online learning, artifact signing, and symbol normalisation.

Run all: `python -m pytest tests/ -v`

---

## 12. Startup / Shutdown

```bash
# Full stack
./start.sh            # Kafka (Docker) + producers + Spark + FastAPI + Streamlit

# Serving only (existing Delta data, no JVM or Docker needed)
.venv/bin/python -m src.serving.api.main > logs/api.log 2>&1 &
.venv/bin/streamlit run src/serving/dashboard/app.py > logs/dashboard.log 2>&1 &

# Train ML models (after 2-3 min of data)
python -m ml.features.feature_extractor
python -m ml.training.train_xgboost
python -m ml.training.train_isolation_forest
python -m ml.training.train_garch
python -m ml.training.train_lstm

# Stop everything
./stop.sh
docker compose -f docker/docker-compose.yml stop
```

**Interactive docs**: `http://localhost:8000/api/v1/docs`
**Kafka UI**: `http://localhost:8080`
**Dashboard**: `http://localhost:8501`

---

## 13. Data Volumes (Typical Single Session)

| Table | Rows |
|---|---|
| bronze/trades | ~500K–1M |
| bronze/ticker | ~500K |
| silver/prices | ~997,474 |
| gold/vwap | ~1,428 |
| gold/spreads | ~1,189 |
| gold/arbitrage_signals | ~0 (spreads below 0.15% in efficient markets) |
| feature_store.parquet | ~1,237 |

---

## 14. ML Results Summary

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

---

## 15. End-to-End Latency

| Hop | Latency |
|---|---|
| Exchange tick → Kafka | ~50ms (WebSocket RTT) |
| Kafka → Bronze Delta | ~10s (micro-batch trigger) |
| Bronze → Silver → Gold | ~20s (two additional trigger cycles) |
| Gold → API → Dashboard | <100ms |
| **Total end-to-end** | **~30 seconds** (dominated by Spark micro-batch interval) |

API performance:
- `/health`: <5ms
- `/prices?limit=200`: <100ms (cached Delta read)
- `/ml/predictions/{symbol}`: <200ms (model in memory)

---

## 16. Known Challenges and Solutions

| Challenge | Root Cause | Solution |
|---|---|---|
| `PYTHON_VERSION_MISMATCH` | macOS system Python 3.14 used for Spark workers | Set `PYSPARK_PYTHON=sys.executable` before SparkSession creation |
| `AMBIGUOUS_COLUMN_REFERENCE` on Gold spreads | PySpark self-join on streaming DataFrame shares internal column IDs | Replaced with pandas cross-exchange computation inside `foreachBatch` |
| `DELTA_SCHEMA_NOT_SET` on Gold startup | Gold streams started before Silver wrote first batch | Added `_wait_for_silver()` polling loop (120s timeout) |
| JVM conflict between Spark and API | API dependency injection tried to start second SparkSession | Forced API to always use `PandasDeltaReader` (no JVM) |
| All-zero ML labels | Live spreads (max 0.143%) below 0.15% fixed threshold | Added adaptive 75th-percentile fallback in `label_generator.py` |
| LSTM training hung | Building sequences from 1M+ silver rows (~1.4 GB numpy array) | Capped to most recent 50,000 rows; reduced epochs 30→10 |
| GARCH convergence warnings | SLSQP + high-kurtosis crypto returns | Acknowledged limitation; models still produce AIC/BIC |
| `/health/ready` Pydantic validation error | `HealthResponse` model expected bool but got string in error branch | Changed return type to plain `dict` |
| XGBoost feature importance: spread_pct dominated all features | Old label definition directly thresholded `spread_pct` → near-perfect leakage | Redesigned label to fee-net future profit — spread_pct is no longer in the label |

---

## 17. System Limitations

1. **Arbitrage signals empty in practice**: Real markets are highly efficient at 0.15%. The platform correctly detects and would alert on larger dislocations during volatility events (e.g. exchange outages, flash crashes).
2. **Single-node Spark (`local[2]`)**: Limits throughput. Production would use a Spark cluster or Databricks.
3. **Order book not processed to Silver/Gold**: `raw-orderbook` topic is ingested to Bronze but not yet parsed into Silver/Gold tables. True arbitrage execution requires bid-ask depth analysis.
4. **LSTM 30-second horizon**: Too slow for HFT but suitable for medium-frequency strategies.
5. **Memory constraints**: Full stack ~5–6 GB RAM. Simultaneous Spark + Docker + ML training is borderline on 16 GB.
6. **GARCH on tick data**: GARCH(1,1) assumes covariance stationarity — crypto tick data exhibits regime changes and fat tails violating this assumption.
7. **No WebSocket latency measurement**: `latency_ms` feature is currently a static placeholder (50ms). Actual per-exchange RTT is not yet instrumented.

---

## 18. Future Work

- **Order book Silver/Gold processing**: Compute bid-ask spread, depth imbalance, and liquidity scores from `raw-orderbook`
- **Cloud deployment**: Confluent Cloud (Kafka), Databricks (Spark), S3/GCS (Delta Lake)
- **River online learning**: `river` Adaptive Random Forest for concept drift detection
- **Backtesting engine**: Replay historical Delta Lake data through the ML pipeline to simulate strategy P&L
- **Alert system**: Webhook/Slack notification when arbitrage signals exceed configurable thresholds
- **WebSocket latency measurement**: Instrument per-exchange RTT; expose as `latency_ms` ML feature

---

## 19. Key Academic / Engineering Contributions

1. **Medallion streaming architecture on a single node**: Bronze→Silver→Gold on a MacBook using Spark local mode and Delta Lake for ACID guarantees without a distributed cluster.
2. **Pandas-in-foreachBatch pattern**: Documented solution for PySpark self-join ambiguity in streaming contexts.
3. **Adaptive label generation for imbalanced financial data**: Percentile-fallback mechanism ensuring ML training always has both classes regardless of market conditions.
4. **Fee-net future-profit labelling**: Eliminates label leakage from `spread_pct` feature by computing labels from future price pairs minus exchange-specific round-trip fees.
5. **Spark-free API serving**: `PandasDeltaReader` using `deltalake` package enables API to serve data from Delta files without requiring a JVM or Spark session, solving the dual-JVM conflict problem.

---

*Generated: 2026-04-23. Repository: crypto-data-platform (main branch). Last commit: bffecd1.*
