---
source_file: "src/serving/api/routes/vwap.py"
type: "rationale"
community: "API Response Models & Schemas"
location: "L70"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/API_Response_Models_&_Schemas
---

# Get list of available window durations.

## Connections
- [[VWAPHistoryResponse]] - `uses` [INFERRED]
- [[VWAPListResponse]] - `uses` [INFERRED]
- [[VWAPResponse]] - `uses` [INFERRED]
- [[WindowDurationListResponse]] - `uses` [INFERRED]
- [[get_windows()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/API_Response_Models_&_Schemas