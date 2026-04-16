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

# Delta Lake Storage (ACID, mergeSchema, checkpoints)

## Connections
- [[FastAPI REST API (port 8000, 15 endpoints)]] - `calls` [EXTRACTED]
- [[Feature Store (feature_store.parquet)]] - `calls` [EXTRACTED]
- [[Spark Requirements (pyspark==3.5.0, delta-spark==3.0.0)]] - `implements` [INFERRED]
- [[Spark Structured Streaming (local2, 10s micro-batches)]] - `calls` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline