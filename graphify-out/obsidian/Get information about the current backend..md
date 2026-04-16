---
source_file: "src/serving/api/dependencies.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L175"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Get information about the current backend.

## Connections
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[get_backend_info()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer