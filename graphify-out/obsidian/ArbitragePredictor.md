---
source_file: "ml/serving/predictor.py"
type: "code"
community: "ML API Routes & Endpoints"
location: "L12"
tags:
  - graphify/code
  - graphify/INFERRED
  - community/ML_API_Routes_&_Endpoints
---

# ArbitragePredictor

## Connections
- [[.__init__()]] - `method` [EXTRACTED]
- [[._load_garch_models()]] - `method` [EXTRACTED]
- [[._load_lstm()]] - `method` [EXTRACTED]
- [[.is_loaded()]] - `method` [EXTRACTED]
- [[.predict()]] - `method` [EXTRACTED]
- [[ArbitragePredictor should initialize even when no model files exist.]] - `uses` [INFERRED]
- [[Convert NaNInf to None for JSON serialization.]] - `uses` [INFERRED]
- [[Feature extractor should produce expected columns from sample data.]] - `uses` [INFERRED]
- [[LSTM direction forecast for a symbol.]] - `uses` [INFERRED]
- [[Labels at time T should only use data from time = T (spread_pct shift).]] - `uses` [INFERRED]
- [[Latest GARCH conditional variance forecast.]] - `uses` [INFERRED]
- [[Latest arbitrage signals with ML predictions.]] - `uses` [INFERRED]
- [[Latest model metrics from MLflow.]] - `uses` [INFERRED]
- [[Loads trained models and produces arbitrage predictions.      Pipeline order]] - `rationale_for` [EXTRACTED]
- [[ML prediction endpoints.]] - `uses` [INFERRED]
- [[Pipeline tests predictor loading, feature extraction, label generation, walk-fo]] - `uses` [INFERRED]
- [[PriceDirectionLSTM]] - `uses` [INFERRED]
- [[Read a Delta table, return list of dicts or empty list.]] - `uses` [INFERRED]
- [[Recent price records flagged by IsolationForest.]] - `uses` [INFERRED]
- [[Walk-forward splits must be chronological and non-overlapping.]] - `uses` [INFERRED]
- [[predictor.py]] - `contains` [EXTRACTED]

#graphify/code #graphify/INFERRED #community/ML_API_Routes_&_Endpoints