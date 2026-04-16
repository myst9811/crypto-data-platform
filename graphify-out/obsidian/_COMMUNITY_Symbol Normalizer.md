---
type: community
cohesion: 0.13
members: 16
---

# Symbol Normalizer

**Cohesion:** 0.13 - loosely connected
**Members:** 16 nodes

## Members
- [[Add data quality score based on various checks.      Quality factors     - Non-]] - rationale - src/processing/transformations/normalizer.py
- [[Create UDF to normalize symbol based on exchange.      Args         exchange E]] - rationale - src/processing/transformations/normalizer.py
- [[Detect outliers using standard deviation method.      Args         df Input Da]] - rationale - src/processing/transformations/normalizer.py
- [[Extract base and quote currency from standard symbol.      Args         df Dat]] - rationale - src/processing/transformations/normalizer.py
- [[Normalize price data (ensure consistent decimal precision, handle outliers).]] - rationale - src/processing/transformations/normalizer.py
- [[Normalize symbols across all exchanges to standard format.      Args         df]] - rationale - src/processing/transformations/normalizer.py
- [[Normalized Price Schema (Silver)]] - code - src/processing/schemas.py
- [[Symbol Mapping Dict (normalizer)]] - code - src/processing/transformations/normalizer.py
- [[Symbol and price normalization transformations.]] - rationale - src/processing/transformations/normalizer.py
- [[add_data_quality_score()]] - code - src/processing/transformations/normalizer.py
- [[detect_outliers()]] - code - src/processing/transformations/normalizer.py
- [[extract_currency_pair()]] - code - src/processing/transformations/normalizer.py
- [[normalize_prices()]] - code - src/processing/transformations/normalizer.py
- [[normalize_symbol()]] - code - src/processing/transformations/normalizer.py
- [[normalize_symbol_udf()]] - code - src/processing/transformations/normalizer.py
- [[normalizer.py]] - code - src/processing/transformations/normalizer.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Symbol_Normalizer
SORT file.name ASC
```

## Connections to other communities
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_FastAPI Price Routes]]

## Top bridge nodes
- [[normalizer.py]] - degree 8, connects to 1 community
- [[Symbol Mapping Dict (normalizer)]] - degree 2, connects to 1 community