---
source_file: "src/serving/data_access/delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L589"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Get list of available trading symbols.

## Connections
- [[.get_available_symbols()]] - `rationale_for` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaLakeManager]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer