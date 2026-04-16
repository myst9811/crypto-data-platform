---
type: community
cohesion: 0.06
members: 47
---

# ML Training Pipeline

**Cohesion:** 0.06 - loosely connected
**Members:** 47 nodes

## Members
- [[API Requirements (fastapi, uvicorn, python-socketio)]] - document - requirements/api.txt
- [[Airflow Requirements (apache-airflow, airflow-spark-provider)]] - document - requirements/airflow.txt
- [[All Requirements (full installation aggregator)]] - document - requirements/all.txt
- [[Architecture Diagram (Eraser.io Export)]] - image - diagram-export-14-04-2026-17_59_19.png
- [[Base Requirements (pandas, numpy, pyarrow, pydantic, deltalake)]] - document - requirements/base.txt
- [[Bidirectional LSTM Price Direction Model]] - document - ARCHITECTURE.md
- [[Bronze Layer (raw JSON Delta tables)]] - document - ARCHITECTURE.md
- [[Citation Bollerslev GARCH 1986]] - document - REPORT_BRIEF.md
- [[Citation Chen & Guestrin XGBoost 2016]] - document - REPORT_BRIEF.md
- [[Citation Hochreiter & Schmidhuber LSTM 1997]] - document - REPORT_BRIEF.md
- [[Citation Liu et al. Isolation Forest 2008]] - document - REPORT_BRIEF.md
- [[Crypto Data Platform Architecture]] - document - ARCHITECTURE.md
- [[Crypto Data Platform README]] - document - README.md
- [[Dashboard Requirements (streamlit, plotly, altair)]] - document - requirements/dashboard.txt
- [[Delta Lake Storage (ACID, mergeSchema, checkpoints)]] - document - ARCHITECTURE.md
- [[DeltaTable (deltalake Python package)]] - code - src/serving/dashboard/pages/2_VWAP_Analysis.py
- [[Dev Requirements (pytest, pytest-asyncio, watchdog)]] - document - requirements/dev.txt
- [[Docker Compose (called by start-pipeline.ps1)]] - code - scripts/start-pipeline.ps1
- [[Exchange WebSocket Producers (Binance, Coinbase, Kraken)]] - document - ARCHITECTURE.md
- [[FastAPI REST API (port 8000, 15 endpoints)]] - document - ARCHITECTURE.md
- [[Feature Store (feature_store.parquet)]] - document - ARCHITECTURE.md
- [[GARCH(1,1) Volatility Models (5 symbols)]] - document - ARCHITECTURE.md
- [[Gold Layer (VWAP, spreads, arbitrage signals)]] - document - ARCHITECTURE.md
- [[Ingestion Requirements (kafka-python, confluent-kafka, websockets)]] - document - requirements/ingestion.txt
- [[Isolation Forest Anomaly Detector]] - document - ARCHITECTURE.md
- [[Kafka Broker (raw-trades, raw-ticker, raw-orderbook)]] - document - ARCHITECTURE.md
- [[ML Pipeline (Feature Store + Models + MLflow)]] - document - ARCHITECTURE.md
- [[MLflow Experiment Tracking]] - document - ARCHITECTURE.md
- [[Main requirements.txt (serving + dev + ML)]] - document - requirements.txt
- [[Online Learner (River Adaptive Classifier)]] - document - CHECKPOINT.md
- [[PandasDeltaReader (no-JVM Delta access)]] - document - ARCHITECTURE.md
- [[Platform Development Checkpoint (2026-04-14)]] - document - CHECKPOINT.md
- [[PowerShell Pipeline Startup Script (Windows)]] - code - scripts/start-pipeline.ps1
- [[Problem Cross-Exchange Arbitrage Detection at Sub-Minute Latency]] - document - REPORT_BRIEF.md
- [[Rationale Force API to use PandasDeltaReader (no JVM conflict)]] - document - REPORT_BRIEF.md
- [[Rationale Pandas-in-foreachBatch for PySpark Self-Join Fix]] - document - REPORT_BRIEF.md
- [[Real-Time Cryptocurrency Analytics Platform (FYP Report Brief)]] - document - REPORT_BRIEF.md
- [[Serving Requirements (api + dashboard combined)]] - document - requirements/serving.txt
- [[Silver Layer (parsed, symbol-normalised prices)]] - document - ARCHITECTURE.md
- [[Spark Requirements (pyspark==3.5.0, delta-spark==3.0.0)]] - document - requirements/spark.txt
- [[Spark Structured Streaming (local2, 10s micro-batches)]] - document - ARCHITECTURE.md
- [[Streamlit Dashboard (port 8501, 6 pages)]] - document - ARCHITECTURE.md
- [[Test ArbitragePredictor loads without error]] - code - tests/test_pipeline.py
- [[VWAP Analysis Dashboard Page]] - code - src/serving/dashboard/pages/2_VWAP_Analysis.py
- [[XGBoost Arbitrage Classifier]] - document - ARCHITECTURE.md
- [[ml.serving.predictor.ArbitragePredictor]] - code - tests/test_pipeline.py
- [[st.cache_data(ttl=10) VWAP loader]] - code - src/serving/dashboard/pages/2_VWAP_Analysis.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/ML_Training_Pipeline
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_ML API Routes & Endpoints]]

## Top bridge nodes
- [[Feature Store (feature_store.parquet)]] - degree 6, connects to 1 community
- [[Dev Requirements (pytest, pytest-asyncio, watchdog)]] - degree 3, connects to 1 community
- [[Test ArbitragePredictor loads without error]] - degree 2, connects to 1 community