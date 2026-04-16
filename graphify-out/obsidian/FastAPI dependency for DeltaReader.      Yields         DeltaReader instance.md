---
source_file: "src/serving/api/dependencies.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L136"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# FastAPI dependency for DeltaReader.      Yields:         DeltaReader instance

## Connections
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[reader_dependency()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer