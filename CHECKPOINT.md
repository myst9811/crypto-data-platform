# Crypto Data Platform - Development Checkpoint

**Date**: 2026-02-02
**Status**: Data Pipeline Running (Kafka + Producers)

---

## Current State Summary

### Working Components

| Component | Status | Access |
|-----------|--------|--------|
| Zookeeper | Running | localhost:2181 |
| Kafka | Running | localhost:9092 |
| Kafka UI | Running | http://localhost:8080 |
| Binance Producer | Running | WebSocket connected |
| Coinbase Producer | Running | WebSocket connected |
| Kraken Producer | Running | WebSocket connected |
| FastAPI Server | Ready (manual start) | http://localhost:8000 |
| Streamlit Dashboard | Ready (manual start) | http://localhost:8501 |

### Not Yet Running

| Component | Reason | Next Steps |
|-----------|--------|------------|
| Spark Streaming | Requires Spark cluster setup | See "Spark Setup" section |
| Delta Lake Tables | Depends on Spark | Will be created when Spark runs |
| Airflow | Optional orchestration | Lower priority |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW                                          │
│                                                                             │
│  Exchanges          Producers           Kafka            Spark      Storage │
│  ─────────          ─────────           ─────            ─────      ─────── │
│                                                                             │
│  Binance   ──ws──►  binance-producer  ──►  raw-trades    ──►               │
│  Coinbase  ──ws──►  coinbase-producer ──►  raw-orderbook ──►  Streaming ──► Delta Lake
│  Kraken    ──ws──►  kraken-producer   ──►  raw-ticker    ──►               │
│                                                                             │
│                                                           │                 │
│                                                           ▼                 │
│                                                    Bronze/Silver/Gold       │
│                                                           │                 │
│                                                           ▼                 │
│                                              FastAPI ◄── Delta Tables       │
│                                                  │                          │
│                                                  ▼                          │
│                                              Streamlit Dashboard            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start Commands

### Start Data Pipeline (Docker)

```powershell
# From project root
cd docker
docker-compose up -d zookeeper kafka kafka-ui
# Wait 30 seconds for Kafka to be healthy
docker-compose run --rm kafka-init
docker-compose up -d binance-producer coinbase-producer kraken-producer
```

### Stop Data Pipeline

```powershell
cd docker
docker-compose down
```

### Start Serving Layer (Local Python)

```powershell
# Terminal 1 - Activate venv and start API
.venv\Scripts\Activate
uvicorn src.serving.api.main:app --reload --host 0.0.0.0 --port 8000

# Terminal 2 - Start Dashboard
.venv\Scripts\Activate
streamlit run src/serving/dashboard/app.py
```

### View Logs

```powershell
cd docker
docker-compose logs -f binance-producer   # Binance logs
docker-compose logs -f coinbase-producer  # Coinbase logs
docker-compose logs -f kraken-producer    # Kraken logs
```

---

## Kafka Topics

| Topic | Content | Producers |
|-------|---------|-----------|
| `raw-trades` | Trade executions | Binance, Coinbase, Kraken |
| `raw-orderbook` | Order book snapshots | Binance, Coinbase |
| `raw-ticker` | Price tickers | Binance, Coinbase |

### Verify Data Flowing

1. Open http://localhost:8080 (Kafka UI)
2. Click "Topics" → "raw-trades" → "Messages"
3. Should see live JSON trade data

---

## Project Structure

```
crypto-data-platform/
├── config/                    # Configuration files
│   ├── spark_config.yaml      # Spark streaming config
│   └── spark_config_docker.yaml
├── docker/                    # Docker setup
│   ├── docker-compose.yml     # Main compose file
│   ├── kafka/
│   │   └── init-topics.sh     # Topic initialization
│   ├── producer/
│   │   └── Dockerfile         # Producer container
│   └── spark-app/
│       ├── Dockerfile         # Spark container (not used yet)
│       └── submit-job.sh
├── requirements/              # Modular requirements
│   ├── base.txt               # Core dependencies
│   ├── ingestion.txt          # Kafka + WebSocket
│   ├── processing.txt         # PySpark + Delta
│   ├── serving.txt            # FastAPI + Streamlit
│   └── dev.txt                # Development tools
├── src/
│   ├── ingestion/             # Exchange producers
│   │   ├── base_producer.py   # Base WebSocket class
│   │   ├── binance_producer.py
│   │   ├── coinbase_producer.py
│   │   └── kraken_producer.py
│   ├── processing/            # Spark transformations
│   │   ├── spark_streaming.py # Main streaming app
│   │   └── transformations/   # Bronze/Silver/Gold logic
│   ├── serving/
│   │   ├── api/               # FastAPI endpoints
│   │   └── dashboard/         # Streamlit pages
│   ├── storage/               # Delta Lake writers
│   └── utils/                 # Shared utilities
│       ├── kafka_utils.py     # Kafka wrapper (uses gzip compression)
│       ├── logging_config.py
│       └── delta_utils.py     # Delta Lake helpers
├── scripts/
│   └── start-pipeline.ps1     # Windows startup script
├── data/                      # Delta Lake tables (created by Spark)
└── CHECKPOINT.md              # This file
```

---

## Key Fixes Applied

### 1. PySpark Import Error in Producers
**Problem**: Producers crashed because `utils/__init__.py` imported `delta_utils` which requires PySpark.

**Fix** (`src/utils/__init__.py`):
```python
# Conditional import for Delta Lake (requires PySpark)
try:
    from .delta_utils import DeltaLakeManager
    DELTA_AVAILABLE = True
except ImportError:
    DeltaLakeManager = None
    DELTA_AVAILABLE = False
```

### 2. Snappy Compression Error
**Problem**: Kafka producer failed with "Libraries for snappy compression codec not found".

**Fix** (`src/utils/kafka_utils.py` line 18):
```python
# Changed from snappy to gzip
compression_type: str = "gzip",
```

### 3. API PySpark Dependency
**Problem**: FastAPI server required PySpark for Delta Lake reader.

**Fix**: Created `PandasDeltaReader` (`src/serving/data_access/pandas_delta_reader.py`) that uses `deltalake` Python package instead of PySpark.

---

## Next Steps to Implement

### Priority 1: Spark Streaming (Data Processing)

**Option A: Local Spark (Windows)**
```powershell
# Install PySpark locally
pip install pyspark==3.5.0 delta-spark==3.0.0

# Run streaming job
python -m src.processing.spark_streaming --config config/spark_config.yaml
```

**Option B: Docker Spark Cluster**
- Requires fixing Spark Docker image issues
- Add to docker-compose.yml:
  - spark-master
  - spark-worker
  - spark-streaming (job submitter)

### Priority 2: Complete Dashboard Pages

Dashboard pages exist but need Delta Lake tables:
- `src/serving/dashboard/pages/1_Live_Prices.py`
- `src/serving/dashboard/pages/2_VWAP_Analysis.py`
- `src/serving/dashboard/pages/3_Arbitrage_Alerts.py`
- `src/serving/dashboard/pages/4_Volume_Analysis.py`
- `src/serving/dashboard/pages/5_Liquidity_Depth.py`
- `src/serving/dashboard/pages/6_Exchange_Comparison.py`

### Priority 3: API Endpoints

API endpoints ready but need data:
- `GET /api/v1/prices/latest`
- `GET /api/v1/prices/history`
- `GET /api/v1/vwap`
- `GET /api/v1/arbitrage`
- `GET /api/v1/volume`

### Priority 4: Additional Features

- [ ] Add more trading pairs
- [ ] Historical data backfill
- [ ] Alert notifications (email/Slack)
- [ ] User authentication
- [ ] Rate limiting
- [ ] Monitoring/metrics (Prometheus/Grafana)

---

## Environment Setup (Fresh Install)

```powershell
# 1. Clone and enter project
cd crypto-data-platform

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\Activate

# 3. Install dependencies (choose based on needs)
pip install -r requirements/base.txt        # Core
pip install -r requirements/ingestion.txt   # + Kafka (for producers)
pip install -r requirements/serving.txt     # + FastAPI/Streamlit
pip install -r requirements/processing.txt  # + PySpark (optional, Windows issues)

# 4. Start Docker services
docker-compose -f docker/docker-compose.yml up -d
```

---

## Troubleshooting

### Producers Keep Restarting
```powershell
docker-compose logs binance-producer
```
Common issues:
- Kafka not healthy yet (wait longer)
- Import errors (check for PySpark dependencies)
- Network issues (check exchange WebSocket URLs)

### Kafka UI Shows No Messages
1. Check producers are running: `docker-compose ps`
2. Check producer logs for errors
3. Verify topic exists: http://localhost:8080 → Topics

### API Returns Empty Data
- Delta Lake tables don't exist yet (Spark hasn't run)
- Run Spark streaming to create Bronze/Silver/Gold tables

### PySpark Won't Install on Windows
- Use WSL2 with Ubuntu
- Or use Docker-based Spark
- Or skip Spark and use Kafka data directly

---

## Configuration Files

### Spark Config (`config/spark_config.yaml`)
- Kafka bootstrap servers
- Delta Lake paths
- Window durations (1min, 5min, 1hour)
- Watermark delays
- Arbitrage thresholds

### Docker Config (`docker/docker-compose.yml`)
- Service definitions
- Port mappings
- Environment variables
- Volume mounts

---

## Useful Commands

```powershell
# Docker
docker-compose ps                              # Service status
docker-compose logs -f [service]               # Stream logs
docker-compose down -v                         # Stop and remove volumes
docker-compose build --no-cache [service]      # Rebuild image

# Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
docker exec kafka kafka-console-consumer --topic raw-trades --bootstrap-server localhost:9092 --max-messages 5

# Python
python -m src.ingestion.binance_producer       # Run producer locally
uvicorn src.serving.api.main:app --reload      # Run API
streamlit run src/serving/dashboard/app.py     # Run dashboard
```

---

## Contact / Resources

- Kafka UI: http://localhost:8080
- API Docs: http://localhost:8000/docs (when running)
- Streamlit: http://localhost:8501 (when running)

---

*Last updated: 2026-02-02*
