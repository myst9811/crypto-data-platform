---
type: community
cohesion: 0.18
members: 20
---

# Spark Streaming Core

**Cohesion:** 0.18 - loosely connected
**Members:** 20 nodes

## Members
- [[.__init__()_12]] - code - src/processing/spark_streaming.py
- [[._create_spark_session()_1]] - code - src/processing/spark_streaming.py
- [[._read_kafka()]] - code - src/processing/spark_streaming.py
- [[._setup_signal_handlers()]] - code - src/processing/spark_streaming.py
- [[._start_bronze()]] - code - src/processing/spark_streaming.py
- [[._start_gold()]] - code - src/processing/spark_streaming.py
- [[._start_silver()]] - code - src/processing/spark_streaming.py
- [[._wait_for_silver()]] - code - src/processing/spark_streaming.py
- [[.get_query_status()]] - code - src/processing/spark_streaming.py
- [[.start()_1]] - code - src/processing/spark_streaming.py
- [[.stop()_1]] - code - src/processing/spark_streaming.py
- [[CryptoStreamingApp]] - code - src/processing/spark_streaming.py
- [[Main Spark Structured Streaming application for crypto data pipeline.  Reads fro]] - rationale - src/processing/spark_streaming.py
- [[Orchestrates Bronze - Silver - Gold medallion pipeline in local mode.]] - rationale - src/processing/spark_streaming.py
- [[Wait until the Silver prices Delta table has data.]] - rationale - src/processing/spark_streaming.py
- [[_ensure_dirs()]] - code - src/processing/spark_streaming.py
- [[_load_config()]] - code - src/processing/spark_streaming.py
- [[_normalise_symbol()]] - code - src/processing/spark_streaming.py
- [[main()_4]] - code - src/processing/spark_streaming.py
- [[spark_streaming.py]] - code - src/processing/spark_streaming.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Spark_Streaming_Core
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[CryptoStreamingApp]] - degree 15, connects to 1 community
- [[spark_streaming.py]] - degree 7, connects to 1 community