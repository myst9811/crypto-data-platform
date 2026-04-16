---
source_file: "src/serving/data_access/pandas_delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L506"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Check connectivity to Delta Lake tables.

## Connections
- [[.health_check()_1]] - `rationale_for` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer