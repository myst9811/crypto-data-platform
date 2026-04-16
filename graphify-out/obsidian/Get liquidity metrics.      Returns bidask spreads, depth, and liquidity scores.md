---
source_file: "src/serving/api/routes/liquidity.py"
type: "rationale"
community: "API Response Models & Schemas"
location: "L23"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/API_Response_Models_&_Schemas
---

# Get liquidity metrics.      Returns bid/ask spreads, depth, and liquidity scores

## Connections
- [[LiquidityListResponse]] - `uses` [INFERRED]
- [[LiquidityRankingResponse]] - `uses` [INFERRED]
- [[LiquidityResponse]] - `uses` [INFERRED]
- [[get_liquidity()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/API_Response_Models_&_Schemas