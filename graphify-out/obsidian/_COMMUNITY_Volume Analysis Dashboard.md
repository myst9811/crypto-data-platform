---
type: community
cohesion: 0.67
members: 3
---

# Volume Analysis Dashboard

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[4_Volume_Analysis.py]] - code - src/serving/dashboard/pages/4_Volume_Analysis.py
- [[Volume Analysis Page - Read Silver prices, compute rolling volume.]] - rationale - src/serving/dashboard/pages/4_Volume_Analysis.py
- [[load_prices()]] - code - src/serving/dashboard/pages/4_Volume_Analysis.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Volume_Analysis_Dashboard
SORT file.name ASC
```
