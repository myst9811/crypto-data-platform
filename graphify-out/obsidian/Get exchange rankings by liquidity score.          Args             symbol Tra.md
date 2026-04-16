---
source_file: "src/serving/data_access/delta_reader.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L427"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Get exchange rankings by liquidity score.          Args:             symbol: Tra

## Connections
- [[.get_liquidity_rankings()]] - `rationale_for` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaLakeManager]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer