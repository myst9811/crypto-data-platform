---
source_file: "ARCHITECTURE.md"
type: "document"
community: "ML Training Pipeline"
location: "Architecture Diagram block"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/ML_Training_Pipeline
---

# Spark Structured Streaming (local[2], 10s micro-batches)

## Connections
- [[Architecture Diagram (Eraser.io Export)]] - `references` [EXTRACTED]
- [[Bronze Layer (raw JSON Delta tables)]] - `implements` [EXTRACTED]
- [[Delta Lake Storage (ACID, mergeSchema, checkpoints)]] - `calls` [EXTRACTED]
- [[Gold Layer (VWAP, spreads, arbitrage signals)]] - `implements` [EXTRACTED]
- [[Kafka Broker (raw-trades, raw-ticker, raw-orderbook)]] - `calls` [EXTRACTED]
- [[Silver Layer (parsed, symbol-normalised prices)]] - `implements` [EXTRACTED]
- [[Spark Requirements (pyspark==3.5.0, delta-spark==3.0.0)]] - `implements` [INFERRED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline