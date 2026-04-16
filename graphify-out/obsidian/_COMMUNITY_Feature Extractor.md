---
type: community
cohesion: 0.23
members: 12
---

# Feature Extractor

**Cohesion:** 0.23 - loosely connected
**Members:** 12 nodes

## Members
- [[Add cyclical time features (sincos of hour).]] - rationale - ml/features/feature_extractor.py
- [[Build the full feature DataFrame and save to feature store.      Columns spread]] - rationale - ml/features/feature_extractor.py
- [[Compute rolling volatility at 15s and 60s windows.]] - rationale - ml/features/feature_extractor.py
- [[Extract features from Delta Lake tables for ML training.]] - rationale - ml/features/feature_extractor.py
- [[Load silver prices from Delta table.]] - rationale - ml/features/feature_extractor.py
- [[Load spreads from Gold Delta table.]] - rationale - ml/features/feature_extractor.py
- [[add_time_features()]] - code - ml/features/feature_extractor.py
- [[compute_rolling_volatility()]] - code - ml/features/feature_extractor.py
- [[extract_features()]] - code - ml/features/feature_extractor.py
- [[feature_extractor.py]] - code - ml/features/feature_extractor.py
- [[load_silver_prices()]] - code - ml/features/feature_extractor.py
- [[load_spreads()]] - code - ml/features/feature_extractor.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Feature_Extractor
SORT file.name ASC
```
