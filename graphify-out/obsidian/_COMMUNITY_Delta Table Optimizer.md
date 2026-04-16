---
type: community
cohesion: 0.50
members: 4
---

# Delta Table Optimizer

**Cohesion:** 0.50 - moderately connected
**Members:** 4 nodes

## Members
- [[.optimize_table()]] - code - src/utils/delta_utils.py
- [[.write_to_delta()]] - code - src/utils/delta_utils.py
- [[Optimize Delta table (compaction and optional Z-ordering).          Args]] - rationale - src/utils/delta_utils.py
- [[Write DataFrame to Delta Lake.          Args             df DataFrame to write]] - rationale - src/utils/delta_utils.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Delta_Table_Optimizer
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[.optimize_table()]] - degree 3, connects to 1 community
- [[.write_to_delta()]] - degree 3, connects to 1 community