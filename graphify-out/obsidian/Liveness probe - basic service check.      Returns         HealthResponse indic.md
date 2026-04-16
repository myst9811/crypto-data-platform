---
source_file: "src/serving/api/routes/health.py"
type: "rationale"
community: "Health Check Endpoints"
location: "L63"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/Health_Check_Endpoints
---

# Liveness probe - basic service check.      Returns:         HealthResponse indic

## Connections
- [[HealthResponse]] - `uses` [INFERRED]
- [[liveness_check()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/Health_Check_Endpoints