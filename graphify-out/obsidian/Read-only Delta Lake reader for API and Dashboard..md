---
source_file: "src/serving/data_access/delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L31"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Read-only Delta Lake reader for API and Dashboard.

## Connections
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaLakeManager]] - `uses` [INFERRED]
- [[DeltaReader]] - `rationale_for` [EXTRACTED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer