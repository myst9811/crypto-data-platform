---
source_file: "requirements/ingestion.txt"
type: "document"
community: "ML Training Pipeline"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/ML_Training_Pipeline
---

# Ingestion Requirements (kafka-python, confluent-kafka, websockets)

## Connections
- [[All Requirements (full installation aggregator)]] - `references` [EXTRACTED]
- [[Base Requirements (pandas, numpy, pyarrow, pydantic, deltalake)]] - `references` [EXTRACTED]
- [[Kafka Broker (raw-trades, raw-ticker, raw-orderbook)]] - `implements` [INFERRED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline