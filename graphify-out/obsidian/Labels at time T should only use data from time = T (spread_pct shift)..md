---
source_file: "tests/test_pipeline.py"
type: "rationale"
community: "ML API Routes & Endpoints"
location: "L65"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/ML_API_Routes_&_Endpoints
---

# Labels at time T should only use data from time <= T (spread_pct shift).

## Connections
- [[ArbitragePredictor]] - `uses` [INFERRED]
- [[test_label_generator_no_future_leakage()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/ML_API_Routes_&_Endpoints