---
source_file: "src/serving/api/routes/vwap.py"
type: "rationale"
community: "API Response Models & Schemas"
location: "L28"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/API_Response_Models_&_Schemas
---

# Get VWAP metrics.      Returns Volume Weighted Average Price data from the Gold

## Connections
- [[VWAPHistoryResponse]] - `uses` [INFERRED]
- [[VWAPListResponse]] - `uses` [INFERRED]
- [[VWAPResponse]] - `uses` [INFERRED]
- [[WindowDurationListResponse]] - `uses` [INFERRED]
- [[get_vwap()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/API_Response_Models_&_Schemas