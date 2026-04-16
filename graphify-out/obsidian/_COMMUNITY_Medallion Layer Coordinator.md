---
type: community
cohesion: 0.08
members: 26
---

# Medallion Layer Coordinator

**Cohesion:** 0.08 - loosely connected
**Members:** 26 nodes

## Members
- [[.__init__()_16]] - code - src/storage/medallion.py
- [[.__init__()_18]] - code - src/storage/medallion.py
- [[.__init__()_17]] - code - src/storage/medallion.py
- [[.apply_watermark()]] - code - src/storage/medallion.py
- [[.clean_and_normalize()]] - code - src/storage/medallion.py
- [[.parse_messages()]] - code - src/storage/medallion.py
- [[.read_from_bronze()]] - code - src/storage/medallion.py
- [[.read_from_kafka()]] - code - src/storage/medallion.py
- [[.read_from_silver()]] - code - src/storage/medallion.py
- [[Apply watermark for handling late data.          Args             df Input Dat]] - rationale - src/storage/medallion.py
- [[Bronze Layer Raw data ingestion from Kafka.      Responsibilities     - Read f]] - rationale - src/storage/medallion.py
- [[BronzeLayer]] - code - src/storage/medallion.py
- [[Clean and normalize data.          Args             df Input DataFrame]] - rationale - src/storage/medallion.py
- [[Gold Layer Analytics and business logic.      Responsibilities     - Read from]] - rationale - src/storage/medallion.py
- [[GoldLayer]] - code - src/storage/medallion.py
- [[Initialize Bronze Layer.          Args             spark Active Spark session]] - rationale - src/storage/medallion.py
- [[Initialize Gold Layer.          Args             spark Active Spark session]] - rationale - src/storage/medallion.py
- [[Initialize Silver Layer.          Args             spark Active Spark session]] - rationale - src/storage/medallion.py
- [[Medallion architecture implementation for BronzeSilverGold layers.]] - rationale - src/storage/medallion.py
- [[Parse Kafka messages and separate by topic.          Args             df Raw K]] - rationale - src/storage/medallion.py
- [[Read streaming data from Bronze Delta table.          Args             data_typ]] - rationale - src/storage/medallion.py
- [[Read streaming data from Kafka topics.          Returns             Streaming D]] - rationale - src/storage/medallion.py
- [[Read streaming data from Silver Delta table.          Args             data_typ]] - rationale - src/storage/medallion.py
- [[Silver Layer Cleaned and normalized data.      Responsibilities     - Read fro]] - rationale - src/storage/medallion.py
- [[SilverLayer]] - code - src/storage/medallion.py
- [[medallion.py]] - code - src/storage/medallion.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Medallion_Layer_Coordinator
SORT file.name ASC
```

## Connections to other communities
- 3 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]

## Top bridge nodes
- [[BronzeLayer]] - degree 6, connects to 1 community
- [[GoldLayer]] - degree 6, connects to 1 community
- [[SilverLayer]] - degree 6, connects to 1 community
- [[medallion.py]] - degree 5, connects to 1 community