---
type: community
cohesion: 0.40
members: 6
---

# Model Registry

**Cohesion:** 0.40 - moderately connected
**Members:** 6 nodes

## Members
- [[Get latest metrics for all experiments.]] - rationale - ml/serving/model_registry.py
- [[Get metrics from the latest run of an experiment.]] - rationale - ml/serving/model_registry.py
- [[Model registry helpers backed by MLflow.]] - rationale - ml/serving/model_registry.py
- [[get_all_latest_metrics()]] - code - ml/serving/model_registry.py
- [[get_latest_run()]] - code - ml/serving/model_registry.py
- [[model_registry.py]] - code - ml/serving/model_registry.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Model_Registry
SORT file.name ASC
```
