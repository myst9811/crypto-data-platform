---
type: community
cohesion: 0.05
members: 62
---

# ML API Routes & Endpoints

**Cohesion:** 0.05 - loosely connected
**Members:** 62 nodes

## Members
- [[.__getitem__()]] - code - ml/training/train_lstm.py
- [[.__init__()]] - code - ml/serving/predictor.py
- [[.__init__()_2]] - code - ml/training/train_lstm.py
- [[.__init__()_3]] - code - ml/training/train_lstm.py
- [[.__len__()]] - code - ml/training/train_lstm.py
- [[._load_garch_models()]] - code - ml/serving/predictor.py
- [[._load_lstm()]] - code - ml/serving/predictor.py
- [[.forward()]] - code - ml/training/train_lstm.py
- [[.is_loaded()]] - code - ml/serving/predictor.py
- [[.predict()]] - code - ml/serving/predictor.py
- [[ArbitragePredictor]] - code - ml/serving/predictor.py
- [[ArbitragePredictor should initialize even when no model files exist.]] - rationale - tests/test_pipeline.py
- [[ArbitragePredictor — loads all models and runs inference pipeline.]] - rationale - ml/serving/predictor.py
- [[Check if at least the core models are loaded.]] - rationale - ml/serving/predictor.py
- [[Convert NaNInf to None for JSON serialization.]] - rationale - src/serving/api/routes/ml.py
- [[Dataset]] - code
- [[Feature extractor should produce expected columns from sample data.]] - rationale - tests/test_pipeline.py
- [[LSTM direction forecast for a symbol.]] - rationale - src/serving/api/routes/ml.py
- [[Labels at time T should only use data from time = T (spread_pct shift).]] - rationale - tests/test_pipeline.py
- [[Latest GARCH conditional variance forecast.]] - rationale - src/serving/api/routes/ml.py
- [[Latest arbitrage signals with ML predictions.]] - rationale - src/serving/api/routes/ml.py
- [[Latest model metrics from MLflow.]] - rationale - src/serving/api/routes/ml.py
- [[Loads trained models and produces arbitrage predictions.      Pipeline order]] - rationale - ml/serving/predictor.py
- [[ML prediction endpoints.]] - rationale - src/serving/api/routes/ml.py
- [[Pipeline tests predictor loading, feature extraction, label generation, walk-fo]] - rationale - tests/test_pipeline.py
- [[Prepare sequences and labels from silver prices.]] - rationale - ml/training/train_lstm.py
- [[PriceDirectionDataset]] - code - ml/training/train_lstm.py
- [[PriceDirectionLSTM]] - code - ml/training/train_lstm.py
- [[Rationale Adaptive Percentile Label Generation for Imbalanced Data]] - document - REPORT_BRIEF.md
- [[Read a Delta table, return list of dicts or empty list.]] - rationale - src/serving/api/routes/ml.py
- [[Recent price records flagged by IsolationForest.]] - rationale - src/serving/api/routes/ml.py
- [[Run the full prediction pipeline.          Args             features_dict Dict]] - rationale - ml/serving/predictor.py
- [[Test Feature Extractor Output Columns]] - code - tests/test_pipeline.py
- [[Test Label Generator No Future Leakage]] - code - tests/test_pipeline.py
- [[Test Walk-Forward Splits Chronological and Non-Overlapping]] - code - tests/test_pipeline.py
- [[Train XGBoost arbitrage classifier.]] - rationale - ml/training/train_xgboost.py
- [[Train bidirectional LSTM for price direction prediction.]] - rationale - ml/training/train_lstm.py
- [[Walk-forward splits must be chronological and non-overlapping.]] - rationale - tests/test_pipeline.py
- [[_get_predictor()]] - code - src/serving/api/routes/ml.py
- [[_load_pickle()]] - code - ml/serving/predictor.py
- [[_read_delta_safe()]] - code - src/serving/api/routes/ml.py
- [[_sanitize_value()]] - code - src/serving/api/routes/ml.py
- [[anomalies_recent()]] - code - src/serving/api/routes/ml.py
- [[arbitrage_live()]] - code - src/serving/api/routes/ml.py
- [[ml.evaluation.walk_forward_cv.walk_forward_split]] - code - tests/test_pipeline.py
- [[ml.features.feature_extractor (compute_rolling_volatility, add_time_features)]] - code - tests/test_pipeline.py
- [[ml.py]] - code - src/serving/api/routes/ml.py
- [[ml.training.label_generator.generate_labels]] - code - tests/test_pipeline.py
- [[model_performance()]] - code - src/serving/api/routes/ml.py
- [[predictions_symbol()]] - code - src/serving/api/routes/ml.py
- [[predictor.py]] - code - ml/serving/predictor.py
- [[prepare_data()]] - code - ml/training/train_lstm.py
- [[test_feature_extractor_output_columns()]] - code - tests/test_pipeline.py
- [[test_label_generator_no_future_leakage()]] - code - tests/test_pipeline.py
- [[test_pipeline.py]] - code - tests/test_pipeline.py
- [[test_predictor_loads_with_empty_artifacts()]] - code - tests/test_pipeline.py
- [[test_walk_forward_splits_chronological()]] - code - tests/test_pipeline.py
- [[train()_2]] - code - ml/training/train_lstm.py
- [[train()_3]] - code - ml/training/train_xgboost.py
- [[train_lstm.py]] - code - ml/training/train_lstm.py
- [[train_xgboost.py]] - code - ml/training/train_xgboost.py
- [[volatility_symbol()]] - code - src/serving/api/routes/ml.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/ML_API_Routes_&_Endpoints
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_ML Training Pipeline]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]

## Top bridge nodes
- [[ml.py]] - degree 10, connects to 1 community
- [[test_pipeline.py]] - degree 10, connects to 1 community
- [[ml.features.feature_extractor (compute_rolling_volatility, add_time_features)]] - degree 2, connects to 1 community