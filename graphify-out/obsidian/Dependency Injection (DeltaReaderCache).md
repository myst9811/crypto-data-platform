---
source_file: "src/serving/api/dependencies.py"
type: "code"
community: "Feature Extraction & Store"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Feature_Extraction_&_Store
---

# Dependency Injection (DeltaReader/Cache)

## Connections
- [[Arbitrage Router]] - `calls` [EXTRACTED]
- [[DataCache Singleton]] - `shares_data_with` [EXTRACTED]
- [[FastAPI Application (main.py)]] - `calls` [EXTRACTED]
- [[Health Router]] - `calls` [EXTRACTED]
- [[Liquidity Router]] - `calls` [EXTRACTED]
- [[Prices Router]] - `calls` [EXTRACTED]
- [[ServingConfig_1]] - `conceptually_related_to` [INFERRED]
- [[SparkSession Singleton]] - `calls` [EXTRACTED]
- [[VWAP Router]] - `calls` [EXTRACTED]
- [[Volume Router]] - `calls` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Feature_Extraction_&_Store