---
type: community
cohesion: 0.17
members: 12
---

# Kafka Utilities

**Cohesion:** 0.17 - loosely connected
**Members:** 12 nodes

## Members
- [[.__init__()_15]] - code - src/utils/kafka_utils.py
- [[.close()_3]] - code - src/utils/kafka_utils.py
- [[.close()_2]] - code - src/utils/kafka_utils.py
- [[.consume()]] - code - src/utils/kafka_utils.py
- [[Close the consumer connection.]] - rationale - src/utils/kafka_utils.py
- [[Close the producer connection.]] - rationale - src/utils/kafka_utils.py
- [[Consume messages from subscribed topics.          Args             timeout_ms]] - rationale - src/utils/kafka_utils.py
- [[Initialize Kafka consumer.          Args             topics List of topics to]] - rationale - src/utils/kafka_utils.py
- [[Kafka utility functions and wrappers.]] - rationale - src/utils/kafka_utils.py
- [[KafkaConsumerWrapper]] - code - src/utils/kafka_utils.py
- [[Wrapper for Kafka consumer with built-in error handling and deserialization.]] - rationale - src/utils/kafka_utils.py
- [[kafka_utils.py]] - code - src/utils/kafka_utils.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Kafka_Utilities
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_Exchange WebSocket Producers]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_Data Cache & Storage Layer]]

## Top bridge nodes
- [[kafka_utils.py]] - degree 4, connects to 2 communities
- [[KafkaConsumerWrapper]] - degree 6, connects to 1 community
- [[.close()_2]] - degree 3, connects to 1 community