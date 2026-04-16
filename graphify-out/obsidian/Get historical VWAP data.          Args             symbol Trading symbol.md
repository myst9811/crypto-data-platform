---
source_file: "src/serving/data_access/delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L254"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Get historical VWAP data.          Args:             symbol: Trading symbol

## Connections
- [[.get_vwap_history()]] - `rationale_for` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaLakeManager]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer