---
source_file: "src/serving/api/routes/arbitrage.py"
type: "rationale"
community: "FastAPI Price Routes"
location: "L131"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/FastAPI_Price_Routes
---

# Get historical arbitrage opportunities.

## Connections
- [[ActiveArbitrageResponse]] - `uses` [INFERRED]
- [[ArbitrageHistoryResponse]] - `uses` [INFERRED]
- [[ArbitrageListResponse]] - `uses` [INFERRED]
- [[ArbitrageResponse]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[get_arbitrage_history()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/FastAPI_Price_Routes