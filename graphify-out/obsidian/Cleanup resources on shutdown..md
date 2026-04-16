---
source_file: "src/serving/api/dependencies.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L156"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Cleanup resources on shutdown.

## Connections
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[shutdown()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer