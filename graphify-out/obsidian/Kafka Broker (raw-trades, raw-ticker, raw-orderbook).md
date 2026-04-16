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

# Kafka Broker (raw-trades, raw-ticker, raw-orderbook)

## Connections
- [[Architecture Diagram (Eraser.io Export)]] - `references` [EXTRACTED]
- [[Exchange WebSocket Producers (Binance, Coinbase, Kraken)]] - `calls` [EXTRACTED]
- [[Ingestion Requirements (kafka-python, confluent-kafka, websockets)]] - `implements` [INFERRED]
- [[PowerShell Pipeline Startup Script (Windows)]] - `calls` [EXTRACTED]
- [[Spark Structured Streaming (local2, 10s micro-batches)]] - `calls` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline