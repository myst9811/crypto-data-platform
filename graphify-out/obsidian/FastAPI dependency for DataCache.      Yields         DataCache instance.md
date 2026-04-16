---
source_file: "src/serving/api/dependencies.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L146"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# FastAPI dependency for DataCache.      Yields:         DataCache instance

## Connections
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[cache_dependency()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer