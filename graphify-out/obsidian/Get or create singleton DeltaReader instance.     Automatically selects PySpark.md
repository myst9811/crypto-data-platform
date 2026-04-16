---
source_file: "src/serving/api/dependencies.py"
type: "rationale"
community: "Data Cache & Storage Layer"
location: "L102"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# Get or create singleton DeltaReader instance.     Automatically selects PySpark

## Connections
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[get_delta_reader()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Data_Cache_&_Storage_Layer