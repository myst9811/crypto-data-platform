---
source_file: "ARCHITECTURE.md"
type: "document"
community: "ML Training Pipeline"
location: "Medallion Layers table"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/ML_Training_Pipeline
---

# Silver Layer (parsed, symbol-normalised prices)

## Connections
- [[Bronze Layer (raw JSON Delta tables)]] - `calls` [EXTRACTED]
- [[Gold Layer (VWAP, spreads, arbitrage signals)]] - `calls` [EXTRACTED]
- [[Spark Structured Streaming (local2, 10s micro-batches)]] - `implements` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline