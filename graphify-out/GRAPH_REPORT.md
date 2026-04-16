# Graph Report - .  (2026-04-16)

## Corpus Check
- Corpus is ~48,250 words - fits in a single context window. You may not need a graph.

## Summary
- 944 nodes · 1804 edges · 42 communities detected
- Extraction: 62% EXTRACTED · 38% INFERRED · 0% AMBIGUOUS · INFERRED: 686 edges (avg confidence: 0.53)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `ServingConfig` - 74 edges
2. `DataCache` - 63 edges
3. `BaseProducer` - 49 edges
4. `PriceData` - 49 edges
5. `VWAPData` - 49 edges
6. `VolumeData` - 49 edges
7. `LiquidityData` - 49 edges
8. `ArbitrageData` - 49 edges
9. `DeltaReader` - 40 edges
10. `DashboardConfig` - 40 edges

## Surprising Connections (you probably didn't know these)
- `DeltaTable (deltalake Python package)` --semantically_similar_to--> `PandasDeltaReader (no-JVM Delta access)`  [INFERRED] [semantically similar]
  src/serving/dashboard/pages/2_VWAP_Analysis.py → ARCHITECTURE.md
- `ml.features.feature_extractor (compute_rolling_volatility, add_time_features)` --implements--> `Feature Store (feature_store.parquet)`  [INFERRED]
  tests/test_pipeline.py → ARCHITECTURE.md
- `Online Learner (River Adaptive Classifier)` --semantically_similar_to--> `XGBoost Arbitrage Classifier`  [INFERRED] [semantically similar]
  CHECKPOINT.md → ARCHITECTURE.md
- `Architecture Diagram (Eraser.io Export)` --references--> `ML Pipeline (Feature Store + Models + MLflow)`  [EXTRACTED]
  diagram-export-14-04-2026-17_59_19.png → ARCHITECTURE.md
- `ml.serving.predictor.ArbitragePredictor` --references--> `XGBoost Arbitrage Classifier`  [EXTRACTED]
  tests/test_pipeline.py → ARCHITECTURE.md

## Hyperedges (group relationships)
- **End-to-End ML Training Pipeline (Features → Labels → Train → Evaluate → Log)** — features_feature_store_load_feature_store, training_label_generator_generate_labels, training_train_xgboost_train, evaluation_metrics_compute_classifier_metrics, mlflow_setup_get_or_create_experiment [INFERRED 0.90]
- **Multi-Model Arbitrage Inference Pipeline (IsolationForest + GARCH + LSTM + XGBoost)** — serving_predictor_arbitrage_predictor, artifacts_isolation_forest_pkl, artifacts_garch_symbol_pkl, artifacts_lstm_price_direction_pt, artifacts_xgboost_arbitrage_pkl [EXTRACTED 1.00]
- **MLflow Experiment Tracking (Setup → Training Scripts → Model Registry)** — mlflow_setup_mlflow_setup, training_train_xgboost_train, training_train_lstm_train, training_train_isolation_forest_train, training_train_garch_train, serving_model_registry_model_registry [EXTRACTED 0.95]
- **Exchange WebSocket Producer -> Kafka Ingestion Pipeline** — binance_producer_BinanceProducer, coinbase_producer_CoinbaseProducer, kraken_producer_KrakenProducer, kafka_utils_KafkaProducerWrapper, config_KafkaTopicConstants [EXTRACTED 1.00]
- **Medallion Architecture Bronze-Silver-Gold Data Flow** — medallion_BronzeLayer, medallion_SilverLayer, medallion_GoldLayer, delta_writer_DeltaWriter, delta_utils_DeltaLakeManager [INFERRED 0.90]
- **Gold Layer Analytics: Arbitrage + VWAP + Volume Aggregation** — spark_streaming_GoldPipeline, arbitrage_detect_arbitrage_opportunities, aggregations_calculate_vwap, aggregations_aggregate_volume, schemas_ArbitrageSchema [INFERRED 0.85]
- **Domain Router-Schema-Reader Serving Pattern** — api_routes_prices_router, api_schemas_prices_priceresponse, api_dependencies_deltareader [INFERRED 0.90]
- **FastAPI App Assembly via Config and Routers** — api_main_fastapiapp, serving_config_servingconfig, api_routes_health_router [EXTRACTED 1.00]
- **ML Inference Pipeline via Predictor and Registry** — api_routes_ml_router, ml_serving_arbitragepredictor, ml_serving_modelregistry [EXTRACTED 1.00]
- **Delta Lake Read Pipeline (Reader + Cache + Models)** — delta_reader_DeltaReader, pandas_delta_reader_PandasDeltaReader, cache_DataCache, models_PriceData, models_VWAPData, models_VolumeData, models_LiquidityData, models_ArbitrageData [INFERRED 0.90]
- **Reusable Dashboard Component System** — components_metrics, components_charts, components_tables, components_filters, dashboard_config_DashboardConfig [EXTRACTED 0.95]
- **Streamlit Pages Polling REST API Pattern** — page_live_prices, page_arbitrage_alerts, page_ml_insights, dashboard_config_DashboardConfig [INFERRED 0.85]
- **Medallion Pipeline: Kafka → Spark → Bronze/Silver/Gold → Delta Lake** — architecture_kafka_broker, architecture_spark_streaming, architecture_bronze_layer, architecture_silver_layer, architecture_gold_layer, architecture_delta_lake [EXTRACTED 1.00]
- **ML Training + Validation: Feature Extractor → Label Generator → Walk-Forward CV** — ml_features_extractor, ml_training_label_generator, ml_evaluation_walk_forward_cv [EXTRACTED 0.95]
- **Serving Stack: Delta Lake → FastAPI → Streamlit Dashboard** — architecture_delta_lake, architecture_pandas_delta_reader, architecture_fastapi, architecture_streamlit_dashboard [EXTRACTED 1.00]

## Communities

### Community 0 - "Data Cache & Storage Layer"
Cohesion: 0.06
Nodes (84): DataCache, Thread-safe TTL cache for data access layer., Initialize cache.          Args:             ttl: Time-to-live in seconds (defau, Get value from cache.          Args:             key: Cache key          Returns, Set value in cache.          Args:             key: Cache key             value:, Delete a specific key from cache.          Args:             key: Cache key to d, Clear all cached entries., Get current cache size. (+76 more)

### Community 1 - "Exchange WebSocket Producers"
Cohesion: 0.03
Nodes (67): ABC, BaseProducer, get_kafka_topic(), get_subscribe_message(), get_websocket_url(), parse_message(), Base WebSocket producer for crypto exchanges., Get Kafka topic based on message type.          Args:             message_type: (+59 more)

### Community 2 - "Streamlit Dashboard Pages"
Cohesion: 0.03
Nodes (87): Live Prices Page - Poll API for latest prices., Arbitrage Alerts Page - Poll ML-enriched signals., ML Insights Page - Model performance metrics and feature importance., check_api_health(), main(), Streamlit dashboard main application., Check API health status., Main dashboard application. (+79 more)

### Community 3 - "API Response Models & Schemas"
Cohesion: 0.05
Nodes (73): BaseModel, APIResponse, ErrorDetail, ErrorResponse, MetaInfo, PaginatedResponse, PaginationMeta, Common API response schemas. (+65 more)

### Community 4 - "ML API Routes & Endpoints"
Cohesion: 0.05
Nodes (46): Dataset, anomalies_recent(), arbitrage_live(), ml.evaluation.walk_forward_cv.walk_forward_split, ml.features.feature_extractor (compute_rolling_volatility, add_time_features), _get_predictor(), model_performance(), predictions_symbol() (+38 more)

### Community 5 - "FastAPI Price Routes"
Cohesion: 0.05
Nodes (59): aggregate_volume(), calculate_liquidity_metrics(), calculate_multi_window_volume(), calculate_multi_window_vwap(), calculate_vwap(), Aggregation transformations for Gold layer analytics., Aggregate trading volume across exchanges with market share calculations.      A, Calculate liquidity metrics from orderbook data.      Metrics include:     - Bid (+51 more)

### Community 6 - "ML Training Pipeline"
Cohesion: 0.06
Nodes (47): Bidirectional LSTM Price Direction Model, Bronze Layer (raw JSON Delta tables), Crypto Data Platform Architecture, Delta Lake Storage (ACID, mergeSchema, checkpoints), Exchange WebSocket Producers (Binance, Coinbase, Kraken), FastAPI REST API (port 8000, 15 endpoints), Feature Store (feature_store.parquet), GARCH(1,1) Volatility Models (5 symbols) (+39 more)

### Community 7 - "Spark Streaming Processing"
Cohesion: 0.07
Nodes (38): Artifact: garch_<symbol>.pkl (per-symbol), Artifact: isolation_forest.pkl, Artifact: lstm_price_direction.pt, Artifact: online_learner.pkl, Artifact: xgboost_arbitrage.pkl, Data: feature_store.parquet (Cached Feature Store), Data: Gold Delta Table (Spreads), Data: Silver Delta Table (Prices) (+30 more)

### Community 8 - "Feature Extraction & Store"
Cohesion: 0.08
Nodes (35): DataCache Singleton, Dependency Injection (DeltaReader/Cache), SparkSession Singleton, API Module Init, FastAPI Application (main.py), Arbitrage Router, Health Router, Liquidity Router (+27 more)

### Community 9 - "GARCH Volatility Models"
Cohesion: 0.16
Nodes (26): DataCache, cached(), get_cache(), TTL-based caching layer for serving module., Decorator for caching function results.      Args:         cache: DataCache inst, Get or create global cache instance.      Args:         ttl: Time-to-live in sec, Chart Components (Plotly), Sidebar Filter Components (+18 more)

### Community 10 - "Exchange List & Volume Aggregates"
Cohesion: 0.17
Nodes (26): ExchangeListResponse, Response containing list of symbols., Response containing list of exchanges., SymbolListResponse, compare_prices(), Config, get_exchanges(), get_price_history() (+18 more)

### Community 11 - "Medallion Layer Coordinator"
Cohesion: 0.08
Nodes (16): BronzeLayer, GoldLayer, Medallion architecture implementation for Bronze/Silver/Gold layers., Silver Layer: Cleaned and normalized data.      Responsibilities:     - Read fro, Initialize Silver Layer.          Args:             spark: Active Spark session, Read streaming data from Bronze Delta table.          Args:             data_typ, Clean and normalize data.          Args:             df: Input DataFrame, Gold Layer: Analytics and business logic.      Responsibilities:     - Read from (+8 more)

### Community 12 - "Spark Streaming Core"
Cohesion: 0.18
Nodes (7): CryptoStreamingApp, _ensure_dirs(), _load_config(), main(), Main Spark Structured Streaming application for crypto data pipeline.  Reads fro, Orchestrates Bronze -> Silver -> Gold medallion pipeline in local mode., Wait until the Silver prices Delta table has data.

### Community 13 - "Delta Lake Writer"
Cohesion: 0.12
Nodes (9): DeltaWriter, Delta Lake writer utilities for streaming data., Write streaming DataFrame to Gold layer.          Args:             df: Streamin, Utility class for writing data to Delta Lake., Initialize Delta Writer.          Args:             base_path: Base path for Del, Write batch DataFrame to Delta Lake.          Args:             df: Batch DataFr, Create necessary directory paths if they don't exist., Write streaming DataFrame to Bronze layer.          Args:             df: Stream (+1 more)

### Community 14 - "Symbol Normalizer"
Cohesion: 0.13
Nodes (15): Symbol Mapping Dict (normalizer), add_data_quality_score(), detect_outliers(), extract_currency_pair(), normalize_prices(), normalize_symbol(), normalize_symbol_udf(), Symbol and price normalization transformations. (+7 more)

### Community 15 - "BaseProducer Abstract Class"
Cohesion: 0.22
Nodes (15): BaseProducer Abstract Class, Dead Letter Queue Pattern, Exponential Backoff Reconnect, BinanceProducer Class, CoinbaseProducer Class, Exchange Config Module, Exchange Credentials Config, Kafka Topic Constants (+7 more)

### Community 16 - "Online Learner"
Cohesion: 0.21
Nodes (5): OnlineLearner, Online learning with River's AdaptiveRandomForestClassifier., Adaptive online learner for streaming arbitrage prediction., Update the model with a single observation., Predict class for a single observation.

### Community 17 - "Feature Extractor"
Cohesion: 0.23
Nodes (11): add_time_features(), compute_rolling_volatility(), extract_features(), load_silver_prices(), load_spreads(), Extract features from Delta Lake tables for ML training., Load spreads from Gold Delta table., Load silver prices from Delta table. (+3 more)

### Community 18 - "Health Check Endpoints"
Cohesion: 0.24
Nodes (11): HealthResponse, Health check response., backend_info(), health_check(), liveness_check(), Health check endpoints., Basic health check endpoint.      Returns:         HealthResponse with service s, Readiness probe - checks Delta Lake connectivity.      Returns:         Dict wit (+3 more)

### Community 19 - "Kafka Utilities"
Cohesion: 0.17
Nodes (7): KafkaConsumerWrapper, Kafka utility functions and wrappers., Initialize Kafka consumer.          Args:             topics: List of topics to, Consume messages from subscribed topics.          Args:             timeout_ms:, Close the consumer connection., Close the producer connection., Wrapper for Kafka consumer with built-in error handling and deserialization.

### Community 20 - "ML Baseline Models"
Cohesion: 0.32
Nodes (5): evaluate_baseline(), Rule-based baseline for arbitrage classification., Predict 1 if spread_pct > threshold, else 0., Evaluate the rule-based baseline on labelled data., RuleBasedBaseline

### Community 21 - "API Dependencies"
Cohesion: 0.39
Nodes (7): cache_dependency(), get_backend_info(), get_data_cache(), get_delta_reader(), get_spark_session(), reader_dependency(), shutdown()

### Community 22 - "Model Registry"
Cohesion: 0.4
Nodes (5): get_all_latest_metrics(), get_latest_run(), Model registry helpers backed by MLflow., Get metrics from the latest run of an experiment., Get latest metrics for all experiments.

### Community 23 - "Feature Store"
Cohesion: 0.4
Nodes (5): get_features_for_symbol(), load_feature_store(), Feature store utilities — load cached features., Load the feature store parquet file., Load features filtered by symbol.

### Community 24 - "Label Generator"
Cohesion: 0.5
Nodes (3): generate_labels(), Generate labels for arbitrage classifier.  For each row at time T, look ahead by, Add a `label` column to the feature DataFrame.      Args:         df: Feature Da

### Community 25 - "Walk-Forward CV"
Cohesion: 0.5
Nodes (3): Walk-forward cross-validation for time-series data., Split data chronologically into expanding train/val/test windows.      Args:, walk_forward_split()

### Community 26 - "Delta Table Optimizer"
Cohesion: 0.5
Nodes (2): Optimize Delta table (compaction and optional Z-ordering).          Args:, Write DataFrame to Delta Lake.          Args:             df: DataFrame to write

### Community 27 - "Isolation Forest Training"
Cohesion: 0.67
Nodes (1): Train Isolation Forest for anomaly detection on price data.

### Community 28 - "GARCH Training"
Cohesion: 0.67
Nodes (1): Train GARCH(1,1) volatility models per symbol.

### Community 29 - "Volume Analysis Dashboard"
Cohesion: 0.67
Nodes (1): Volume Analysis Page - Read Silver prices, compute rolling volume.

### Community 30 - "Exchange Comparison Dashboard"
Cohesion: 0.67
Nodes (1): Exchange Comparison Page - Cross-exchange spread analysis.

### Community 31 - "VWAP Analysis Dashboard"
Cohesion: 0.67
Nodes (1): VWAP Analysis Page - Read from Gold Delta table.

### Community 32 - "API Schema Definitions"
Cohesion: 0.67
Nodes (3): APIResponse Schema, ErrorResponse Schema, MetaInfo Schema

### Community 33 - "Medallion Architecture Docs"
Cohesion: 0.67
Nodes (3): Medallion Architecture (Bronze/Silver/Gold), Citation: Armbrust et al. Delta Lake VLDB 2020, Rationale: Medallion Architecture on Single Node

### Community 34 - "Rolling Accuracy Tracker"
Cohesion: 1.0
Nodes (1): Rolling accuracy over last 500 samples.

### Community 35 - "Pipeline Startup Scripts"
Cohesion: 1.0
Nodes (0): 

### Community 36 - "Cache Key Generator"
Cohesion: 1.0
Nodes (1): Generate a cache key from arguments.          Args:             *args: Positiona

### Community 37 - "Path Utilities"
Cohesion: 1.0
Nodes (1): Create directory path if it doesn't exist.          Args:             path: Dire

### Community 38 - "Stream Query Manager"
Cohesion: 1.0
Nodes (1): Stop a streaming query gracefully.          Args:             query: StreamingQu

### Community 39 - "Query Termination Handler"
Cohesion: 1.0
Nodes (1): Wait for all streaming queries to terminate.          Args:             queries:

### Community 40 - "Kafka Consumer Wrapper"
Cohesion: 1.0
Nodes (1): KafkaConsumerWrapper Class

### Community 41 - "OrderBook Data Model"
Cohesion: 1.0
Nodes (1): OrderBookData Model

## Knowledge Gaps
- **235 isolated node(s):** `MLflow experiment tracking setup.`, `Get or create an MLflow experiment by name and return its ID.`, `Online learning with River's AdaptiveRandomForestClassifier.`, `Adaptive online learner for streaming arbitrage prediction.`, `Update the model with a single observation.` (+230 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Rolling Accuracy Tracker`** (1 nodes): `Rolling accuracy over last 500 samples.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Pipeline Startup Scripts`** (1 nodes): `start-pipeline.ps1`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Cache Key Generator`** (1 nodes): `Generate a cache key from arguments.          Args:             *args: Positiona`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Path Utilities`** (1 nodes): `Create directory path if it doesn't exist.          Args:             path: Dire`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Stream Query Manager`** (1 nodes): `Stop a streaming query gracefully.          Args:             query: StreamingQu`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Query Termination Handler`** (1 nodes): `Wait for all streaming queries to terminate.          Args:             queries:`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Kafka Consumer Wrapper`** (1 nodes): `KafkaConsumerWrapper Class`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `OrderBook Data Model`** (1 nodes): `OrderBookData Model`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Machine learning layer for crypto arbitrage detection and price prediction.` connect `Data Cache & Storage Layer` to `Exchange WebSocket Producers`, `Streamlit Dashboard Pages`, `API Response Models & Schemas`, `Medallion Layer Coordinator`, `Spark Streaming Core`, `Delta Lake Writer`, `Health Check Endpoints`, `Kafka Utilities`?**
  _High betweenness centrality (0.241) - this node is a cross-community bridge._
- **Are the 72 inferred relationships involving `ServingConfig` (e.g. with `Machine learning layer for crypto arbitrage detection and price prediction.` and `DeltaReader`) actually correct?**
  _`ServingConfig` has 72 INFERRED edges - model-reasoned connections that need verification._
- **Are the 54 inferred relationships involving `DataCache` (e.g. with `DeltaReader` and `Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations`) actually correct?**
  _`DataCache` has 54 INFERRED edges - model-reasoned connections that need verification._
- **Are the 39 inferred relationships involving `BaseProducer` (e.g. with `KafkaProducerWrapper` and `Machine learning layer for crypto arbitrage detection and price prediction.`) actually correct?**
  _`BaseProducer` has 39 INFERRED edges - model-reasoned connections that need verification._
- **Are the 46 inferred relationships involving `PriceData` (e.g. with `DeltaReader` and `Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations`) actually correct?**
  _`PriceData` has 46 INFERRED edges - model-reasoned connections that need verification._
- **Are the 46 inferred relationships involving `VWAPData` (e.g. with `DeltaReader` and `Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations`) actually correct?**
  _`VWAPData` has 46 INFERRED edges - model-reasoned connections that need verification._
- **What connects `MLflow experiment tracking setup.`, `Get or create an MLflow experiment by name and return its ID.`, `Online learning with River's AdaptiveRandomForestClassifier.` to the rest of the system?**
  _235 weakly-connected nodes found - possible documentation gaps or missing edges._