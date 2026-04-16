---
type: community
cohesion: 0.40
members: 6
---

# Feature Store

**Cohesion:** 0.40 - moderately connected
**Members:** 6 nodes

## Members
- [[Feature store utilities — load cached features.]] - rationale - ml/features/feature_store.py
- [[Load features filtered by symbol.]] - rationale - ml/features/feature_store.py
- [[Load the feature store parquet file.]] - rationale - ml/features/feature_store.py
- [[feature_store.py]] - code - ml/features/feature_store.py
- [[get_features_for_symbol()]] - code - ml/features/feature_store.py
- [[load_feature_store()]] - code - ml/features/feature_store.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Feature_Store
SORT file.name ASC
```
