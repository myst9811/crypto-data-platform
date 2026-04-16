---
type: community
cohesion: 0.12
members: 18
---

# Delta Lake Writer

**Cohesion:** 0.12 - loosely connected
**Members:** 18 nodes

## Members
- [[.__init__()_19]] - code - src/storage/delta_writer.py
- [[._ensure_paths_exist()]] - code - src/storage/delta_writer.py
- [[.write_batch_to_delta()]] - code - src/storage/delta_writer.py
- [[.write_to_bronze()]] - code - src/storage/delta_writer.py
- [[.write_to_gold()]] - code - src/storage/delta_writer.py
- [[.write_to_silver()]] - code - src/storage/delta_writer.py
- [[Create necessary directory paths if they don't exist.]] - rationale - src/storage/delta_writer.py
- [[Delta Lake writer utilities for streaming data.]] - rationale - src/storage/delta_writer.py
- [[DeltaWriter]] - code - src/storage/delta_writer.py
- [[Initialize Delta Writer.          Args             base_path Base path for Del]] - rationale - src/storage/delta_writer.py
- [[Utility class for writing data to Delta Lake.]] - rationale - src/storage/delta_writer.py
- [[Write batch DataFrame to Delta Lake.          Args             df Batch DataFr]] - rationale - src/storage/delta_writer.py
- [[Write streaming DataFrame to Bronze layer.          Args             df Stream]] - rationale - src/storage/delta_writer.py
- [[Write streaming DataFrame to Gold layer.          Args             df Streamin]] - rationale - src/storage/delta_writer.py
- [[Write streaming DataFrame to Silver layer.          Args             df Stream]] - rationale - src/storage/delta_writer.py
- [[await_termination()]] - code - src/storage/delta_writer.py
- [[delta_writer.py]] - code - src/storage/delta_writer.py
- [[stop_query()]] - code - src/storage/delta_writer.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Delta_Lake_Writer
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[DeltaWriter]] - degree 9, connects to 1 community
- [[delta_writer.py]] - degree 5, connects to 1 community