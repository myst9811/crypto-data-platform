---
type: community
cohesion: 0.67
members: 3
---

# API Schema Definitions

**Cohesion:** 0.67 - moderately connected
**Members:** 3 nodes

## Members
- [[APIResponse Schema]] - code - src/serving/api/schemas/common.py
- [[ErrorResponse Schema]] - code - src/serving/api/schemas/common.py
- [[MetaInfo Schema]] - code - src/serving/api/schemas/common.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/API_Schema_Definitions
SORT file.name ASC
```
