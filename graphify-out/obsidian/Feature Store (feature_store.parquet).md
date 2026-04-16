---
source_file: "ARCHITECTURE.md"
type: "document"
community: "ML Training Pipeline"
location: "ML Layer block"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/ML_Training_Pipeline
---

# Feature Store (feature_store.parquet)

## Connections
- [[Bidirectional LSTM Price Direction Model]] - `calls` [EXTRACTED]
- [[Delta Lake Storage (ACID, mergeSchema, checkpoints)]] - `calls` [EXTRACTED]
- [[GARCH(1,1) Volatility Models (5 symbols)]] - `calls` [EXTRACTED]
- [[Isolation Forest Anomaly Detector]] - `calls` [EXTRACTED]
- [[XGBoost Arbitrage Classifier]] - `calls` [EXTRACTED]
- [[ml.features.feature_extractor (compute_rolling_volatility, add_time_features)]] - `implements` [INFERRED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline