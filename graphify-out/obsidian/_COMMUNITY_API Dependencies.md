---
type: community
cohesion: 0.39
members: 8
---

# API Dependencies

**Cohesion:** 0.39 - loosely connected
**Members:** 8 nodes

## Members
- [[cache_dependency()]] - code - src/serving/api/dependencies.py
- [[dependencies.py]] - code - src/serving/api/dependencies.py
- [[get_backend_info()]] - code - src/serving/api/dependencies.py
- [[get_data_cache()]] - code - src/serving/api/dependencies.py
- [[get_delta_reader()]] - code - src/serving/api/dependencies.py
- [[get_spark_session()]] - code - src/serving/api/dependencies.py
- [[reader_dependency()]] - code - src/serving/api/dependencies.py
- [[shutdown()]] - code - src/serving/api/dependencies.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/API_Dependencies
SORT file.name ASC
```

## Connections to other communities
- 8 edges to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[dependencies.py]] - degree 8, connects to 1 community
- [[get_delta_reader()]] - degree 5, connects to 1 community
- [[get_data_cache()]] - degree 4, connects to 1 community
- [[cache_dependency()]] - degree 3, connects to 1 community
- [[get_spark_session()]] - degree 3, connects to 1 community