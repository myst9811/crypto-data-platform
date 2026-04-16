---
source_file: "src/serving/api/routes/arbitrage.py"
type: "rationale"
community: "FastAPI Price Routes"
location: "L81"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/FastAPI_Price_Routes
---

# Get currently viable arbitrage opportunities.      Returns opportunities that ar

## Connections
- [[ActiveArbitrageResponse]] - `uses` [INFERRED]
- [[ArbitrageHistoryResponse]] - `uses` [INFERRED]
- [[ArbitrageListResponse]] - `uses` [INFERRED]
- [[ArbitrageResponse]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[get_active_arbitrage()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/FastAPI_Price_Routes