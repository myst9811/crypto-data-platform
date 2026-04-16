---
type: community
cohesion: 0.07
members: 39
---

# Spark Streaming Processing

**Cohesion:** 0.07 - loosely connected
**Members:** 39 nodes

## Members
- [[4-Stage Inference Pipeline (IsolationForest→GARCH→LSTM→XGBoost)]] - code - ml/serving/predictor.py
- [[ArbitragePredictor Class]] - code - ml/serving/predictor.py
- [[Artifact garch_symbol.pkl (per-symbol)]] - document - ml/training/train_garch.py
- [[Artifact isolation_forest.pkl]] - document - ml/training/train_isolation_forest.py
- [[Artifact lstm_price_direction.pt]] - document - ml/training/train_lstm.py
- [[Artifact online_learner.pkl]] - document - ml/serving/online_learner.py
- [[Artifact xgboost_arbitrage.pkl]] - document - ml/training/train_xgboost.py
- [[Data Gold Delta Table (Spreads)]] - document - ml/features/feature_extractor.py
- [[Data Silver Delta Table (Prices)]] - document - ml/features/feature_extractor.py
- [[Data feature_store.parquet (Cached Feature Store)]] - document - ml/features/feature_store.py
- [[GARCH(1,1) Per-Symbol Volatility Training Script]] - code - ml/training/train_garch.py
- [[Get or create an MLflow experiment by name and return its ID.]] - rationale - ml/mlflow_setup.py
- [[Isolation Forest Training Script]] - code - ml/training/train_isolation_forest.py
- [[LSTM Training Script]] - code - ml/training/train_lstm.py
- [[MLflow Experiment Tracking Setup]] - code - ml/mlflow_setup.py
- [[MLflow Experiments Registry (arbitrage_classifier, price_direction_lstm, anomaly_detection)]] - code - ml/mlflow_setup.py
- [[MLflow experiment tracking setup.]] - rationale - ml/mlflow_setup.py
- [[MLflow-backed Model Registry Helpers]] - code - ml/serving/model_registry.py
- [[OnlineLearner Class (AdaptiveRandomForest)]] - code - ml/serving/online_learner.py
- [[PriceDirectionLSTM Model Class (Bidirectional)]] - code - ml/training/train_lstm.py
- [[RuleBasedBaseline Class (Spread Threshold)]] - code - ml/evaluation/baseline.py
- [[XGBoost Arbitrage Classifier Training Script]] - code - ml/training/train_xgboost.py
- [[add_time_features Function (Cyclical Encoding)]] - code - ml/features/feature_extractor.py
- [[compute_classifier_metrics Function]] - code - ml/evaluation/metrics.py
- [[compute_regression_metrics Function]] - code - ml/evaluation/metrics.py
- [[compute_rolling_volatility Function]] - code - ml/features/feature_extractor.py
- [[evaluate_baseline Function]] - code - ml/evaluation/baseline.py
- [[extract_features Function (Feature Store Builder)]] - code - ml/features/feature_extractor.py
- [[generate_labels Function (Look-ahead Labelling)]] - code - ml/training/label_generator.py
- [[get_all_latest_metrics Function]] - code - ml/serving/model_registry.py
- [[get_features_for_symbol Function]] - code - ml/features/feature_store.py
- [[get_latest_run Function]] - code - ml/serving/model_registry.py
- [[get_or_create_experiment()]] - code - ml/mlflow_setup.py
- [[load_feature_store Function (Parquet Cache)]] - code - ml/features/feature_store.py
- [[load_silver_prices Function (Silver Delta Table)]] - code - ml/features/feature_extractor.py
- [[load_spreads Function (Gold Delta Table)]] - code - ml/features/feature_extractor.py
- [[mlflow_setup.py]] - code - ml/mlflow_setup.py
- [[prepare_data Function (Sequence Builder)]] - code - ml/training/train_lstm.py
- [[walk_forward_split Function (Expanding Window CV)]] - code - ml/evaluation/walk_forward_cv.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Spark_Streaming_Processing
SORT file.name ASC
```
