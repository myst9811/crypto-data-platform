---
source_file: "ml/serving/predictor.py"
type: "rationale"
community: "ML API Routes & Endpoints"
location: "L13"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ML_API_Routes_&_Endpoints
---

# Loads trained models and produces arbitrage predictions.      Pipeline order:

## Connections
- [[ArbitragePredictor]] - `rationale_for` [EXTRACTED]
- [[PriceDirectionLSTM]] - `uses` [INFERRED]

#graphify/rationale #graphify/EXTRACTED #community/ML_API_Routes_&_Endpoints