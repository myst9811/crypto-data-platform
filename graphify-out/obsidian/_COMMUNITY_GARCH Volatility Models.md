---
type: community
cohesion: 0.16
members: 28
---

# GARCH Volatility Models

**Cohesion:** 0.16 - loosely connected
**Members:** 28 nodes

## Members
- [[Arbitrage Alerts Page]] - code - src/serving/dashboard/pages/3_Arbitrage_Alerts.py
- [[ArbitrageData Model]] - code - src/serving/data_access/models.py
- [[Chart Components (Plotly)]] - code - src/serving/dashboard/components/charts.py
- [[Dashboard Components __init__]] - code - src/serving/dashboard/components/__init__.py
- [[Dashboard Main App]] - code - src/serving/dashboard/app.py
- [[DashboardConfig_1]] - code - src/serving/dashboard/config.py
- [[Data Access Layer __init__]] - code - src/serving/data_access/__init__.py
- [[DataCache_1]] - code - src/serving/data_access/cache.py
- [[Decorator for caching function results.      Args         cache DataCache inst]] - rationale - src/serving/data_access/cache.py
- [[DeltaReader (Spark)]] - code - src/serving/data_access/delta_reader.py
- [[Exchange Comparison Page]] - code - src/serving/dashboard/pages/6_Exchange_Comparison.py
- [[Get or create global cache instance.      Args         ttl Time-to-live in sec]] - rationale - src/serving/data_access/cache.py
- [[LiquidityData Model]] - code - src/serving/data_access/models.py
- [[Live Prices Page]] - code - src/serving/dashboard/pages/1_Live_Prices.py
- [[ML Insights Page]] - code - src/serving/dashboard/pages/5_ML_Insights.py
- [[Metric Display Components]] - code - src/serving/dashboard/components/metrics.py
- [[PandasDeltaReader (Spark-free)]] - code - src/serving/data_access/pandas_delta_reader.py
- [[PriceData Model]] - code - src/serving/data_access/models.py
- [[Sidebar Filter Components]] - code - src/serving/dashboard/components/filters.py
- [[TTL-based caching layer for serving module.]] - rationale - src/serving/data_access/cache.py
- [[Table Display Components]] - code - src/serving/dashboard/components/tables.py
- [[VWAPData Model]] - code - src/serving/data_access/models.py
- [[Volume Analysis Page]] - code - src/serving/dashboard/pages/4_Volume_Analysis.py
- [[VolumeData Model]] - code - src/serving/data_access/models.py
- [[cache.py]] - code - src/serving/data_access/cache.py
- [[cached()]] - code - src/serving/data_access/cache.py
- [[get_cache()]] - code - src/serving/data_access/cache.py
- [[make_key()]] - code - src/serving/data_access/cache.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/GARCH_Volatility_Models
SORT file.name ASC
```

## Connections to other communities
- 2 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]

## Top bridge nodes
- [[cache.py]] - degree 6, connects to 2 communities
- [[get_cache()]] - degree 5, connects to 1 community