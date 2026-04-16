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

# Bronze Layer (raw JSON Delta tables)

## Connections
- [[Silver Layer (parsed, symbol-normalised prices)]] - `calls` [EXTRACTED]
- [[Spark Structured Streaming (local2, 10s micro-batches)]] - `implements` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline