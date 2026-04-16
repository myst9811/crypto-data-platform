---
source_file: "src/serving/data_access/pandas_delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L1"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Delta Lake reader using deltalake package (no Spark required).

## Connections
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]
- [[pandas_delta_reader.py]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer