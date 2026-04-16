---
source_file: "src/serving/api/routes/arbitrage.py"
type: "rationale"
community: "FastAPI Price Routes"
location: "L28"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/FastAPI_Price_Routes
---

# Get arbitrage opportunities.      Returns detected cross-exchange arbitrage oppo

## Connections
- [[ActiveArbitrageResponse]] - `uses` [INFERRED]
- [[ArbitrageHistoryResponse]] - `uses` [INFERRED]
- [[ArbitrageListResponse]] - `uses` [INFERRED]
- [[ArbitrageResponse]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[get_arbitrage()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/FastAPI_Price_Routes