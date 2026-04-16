---
type: community
cohesion: 0.24
members: 12
---

# Health Check Endpoints

**Cohesion:** 0.24 - loosely connected
**Members:** 12 nodes

## Members
- [[Basic health check endpoint.      Returns         HealthResponse with service s]] - rationale - src/serving/api/routes/health.py
- [[Get information about the data backend.      Returns         Dict with backend]] - rationale - src/serving/api/routes/health.py
- [[Health check endpoints.]] - rationale - src/serving/api/routes/health.py
- [[Health check response.]] - rationale - src/serving/api/schemas/common.py
- [[HealthResponse]] - code - src/serving/api/schemas/common.py
- [[Liveness probe - basic service check.      Returns         HealthResponse indic]] - rationale - src/serving/api/routes/health.py
- [[Readiness probe - checks Delta Lake connectivity.      Returns         Dict wit]] - rationale - src/serving/api/routes/health.py
- [[backend_info()]] - code - src/serving/api/routes/health.py
- [[health.py]] - code - src/serving/api/routes/health.py
- [[health_check()]] - code - src/serving/api/routes/health.py
- [[liveness_check()]] - code - src/serving/api/routes/health.py
- [[readiness_check()]] - code - src/serving/api/routes/health.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Health_Check_Endpoints
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_API Response Models & Schemas]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[HealthResponse]] - degree 9, connects to 2 communities
- [[health.py]] - degree 6, connects to 1 community