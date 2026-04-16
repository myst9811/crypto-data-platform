---
source_file: "src/serving/api/routes/health.py"
type: "rationale"
community: "Health Check Endpoints"
location: "L13"
tags:
  - graphify/rationale
  - graphify/EXTRACTED
  - community/Health_Check_Endpoints
---

# Basic health check endpoint.      Returns:         HealthResponse with service s

## Connections
- [[HealthResponse]] - `uses` [INFERRED]
- [[health_check()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/EXTRACTED #community/Health_Check_Endpoints