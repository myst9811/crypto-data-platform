---
type: community
cohesion: 0.08
members: 35
---

# Feature Extraction & Store

**Cohesion:** 0.08 - loosely connected
**Members:** 35 nodes

## Members
- [[API Module Init]] - code - src/serving/api/__init__.py
- [[ActiveArbitrageResponse Schema]] - code - src/serving/api/schemas/arbitrage.py
- [[Arbitrage Router]] - code - src/serving/api/routes/arbitrage.py
- [[ArbitrageHistoryResponse Schema]] - code - src/serving/api/schemas/arbitrage.py
- [[ArbitrageListResponse Schema]] - code - src/serving/api/schemas/arbitrage.py
- [[ArbitragePredictor (ML)]] - code - src/serving/api/routes/ml.py
- [[ArbitrageResponse Schema]] - code - src/serving/api/schemas/arbitrage.py
- [[DataCache Singleton]] - code - src/serving/api/dependencies.py
- [[Dependency Injection (DeltaReaderCache)]] - code - src/serving/api/dependencies.py
- [[FastAPI Application (main.py)]] - code - src/serving/api/main.py
- [[Health Router]] - code - src/serving/api/routes/health.py
- [[HealthResponse Schema]] - code - src/serving/api/schemas/common.py
- [[Liquidity Router]] - code - src/serving/api/routes/liquidity.py
- [[LiquidityListResponse Schema]] - code - src/serving/api/schemas/liquidity.py
- [[LiquidityRankingResponse Schema]] - code - src/serving/api/schemas/liquidity.py
- [[LiquidityResponse Schema]] - code - src/serving/api/schemas/liquidity.py
- [[ML Model Registry]] - code - src/serving/api/routes/ml.py
- [[ML Router]] - code - src/serving/api/routes/ml.py
- [[MarketShareListResponse Schema]] - code - src/serving/api/schemas/volume.py
- [[PriceComparisonResponse Schema]] - code - src/serving/api/schemas/prices.py
- [[PriceHistoryResponse Schema]] - code - src/serving/api/schemas/prices.py
- [[PriceListResponse Schema]] - code - src/serving/api/schemas/prices.py
- [[PriceResponse Schema]] - code - src/serving/api/schemas/prices.py
- [[Prices Router]] - code - src/serving/api/routes/prices.py
- [[Serving Module Init]] - code - src/serving/__init__.py
- [[ServingConfig_1]] - code - src/serving/config.py
- [[SparkSession Singleton]] - code - src/serving/api/dependencies.py
- [[VWAP Router]] - code - src/serving/api/routes/vwap.py
- [[VWAPHistoryResponse Schema]] - code - src/serving/api/schemas/vwap.py
- [[VWAPListResponse Schema]] - code - src/serving/api/schemas/vwap.py
- [[VWAPResponse Schema]] - code - src/serving/api/schemas/vwap.py
- [[Volume Router]] - code - src/serving/api/routes/volume.py
- [[VolumeListResponse Schema]] - code - src/serving/api/schemas/volume.py
- [[VolumeRankingResponse Schema]] - code - src/serving/api/schemas/volume.py
- [[VolumeResponse Schema]] - code - src/serving/api/schemas/volume.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Feature_Extraction_&_Store
SORT file.name ASC
```
