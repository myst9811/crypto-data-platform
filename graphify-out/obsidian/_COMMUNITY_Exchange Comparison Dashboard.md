---
type: community
cohesion: 0.67
members: 3
---

# Exchange Comparison Dashboard

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[6_Exchange_Comparison.py]] - code - src/serving/dashboard/pages/6_Exchange_Comparison.py
- [[Exchange Comparison Page - Cross-exchange spread analysis.]] - rationale - src/serving/dashboard/pages/6_Exchange_Comparison.py
- [[load_spreads()_1]] - code - src/serving/dashboard/pages/6_Exchange_Comparison.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Exchange_Comparison_Dashboard
SORT file.name ASC
```
