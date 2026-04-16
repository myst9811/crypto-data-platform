---
type: community
cohesion: 0.67
members: 3
---

# VWAP Analysis Dashboard

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[2_VWAP_Analysis.py]] - code - src/serving/dashboard/pages/2_VWAP_Analysis.py
- [[VWAP Analysis Page - Read from Gold Delta table.]] - rationale - src/serving/dashboard/pages/2_VWAP_Analysis.py
- [[load_vwap()]] - code - src/serving/dashboard/pages/2_VWAP_Analysis.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/VWAP_Analysis_Dashboard
SORT file.name ASC
```
