---
source_file: "ARCHITECTURE.md"
type: "document"
community: "ML Training Pipeline"
location: "API Endpoints table"
tags:
  - graphify/document
  - graphify/EXTRACTED
  - community/ML_Training_Pipeline
---

# FastAPI REST API (port 8000, 15 endpoints)

## Connections
- [[API Requirements (fastapi, uvicorn, python-socketio)]] - `implements` [INFERRED]
- [[Delta Lake Storage (ACID, mergeSchema, checkpoints)]] - `calls` [EXTRACTED]
- [[PandasDeltaReader (no-JVM Delta access)]] - `implements` [EXTRACTED]
- [[Streamlit Dashboard (port 8501, 6 pages)]] - `calls` [EXTRACTED]
- [[XGBoost Arbitrage Classifier]] - `calls` [EXTRACTED]

#graphify/document #graphify/EXTRACTED #community/ML_Training_Pipeline