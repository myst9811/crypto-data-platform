---
type: community
cohesion: 0.22
members: 16
---

# BaseProducer Abstract Class

**Cohesion:** 0.22 - loosely connected
**Members:** 16 nodes

## Members
- [[BaseProducer Abstract Class]] - code - src/ingestion/base_producer.py
- [[BinanceProducer Class]] - code - src/ingestion/binance_producer.py
- [[Centralized logging configuration for the crypto data platform.]] - rationale - src/utils/logging_config.py
- [[CoinbaseProducer Class]] - code - src/ingestion/coinbase_producer.py
- [[Configure logging for the application.      Args         log_level Logging lev]] - rationale - src/utils/logging_config.py
- [[Dead Letter Queue Pattern]] - code - src/ingestion/base_producer.py
- [[Exchange Config Module]] - code - src/ingestion/config.py
- [[Exchange Credentials Config]] - code - src/ingestion/config.py
- [[Exponential Backoff Reconnect]] - code - src/ingestion/base_producer.py
- [[Get a logger instance with the given name.      Args         name Logger name]] - rationale - src/utils/logging_config.py
- [[Kafka Topic Constants]] - code - src/ingestion/config.py
- [[KafkaProducerWrapper Class]] - code - src/utils/kafka_utils.py
- [[KrakenProducer Class]] - code - src/ingestion/kraken_producer.py
- [[get_logger()]] - code - src/utils/logging_config.py
- [[logging_config.py]] - code - src/utils/logging_config.py
- [[setup_logging()]] - code - src/utils/logging_config.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/BaseProducer_Abstract_Class
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]

## Top bridge nodes
- [[logging_config.py]] - degree 4, connects to 1 community