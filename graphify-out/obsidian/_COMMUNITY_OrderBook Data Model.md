---
type: community
cohesion: 1.00
members: 1
---

# OrderBook Data Model

**Cohesion:** 1.00 - tightly connected
**Members:** 1 nodes

## Members
- [[OrderBookData Model]] - code - src/serving/data_access/models.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/OrderBook_Data_Model
SORT file.name ASC
```
