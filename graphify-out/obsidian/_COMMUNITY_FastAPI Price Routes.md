---
type: community
cohesion: 0.05
members: 61
---

# FastAPI Price Routes

**Cohesion:** 0.05 - loosely connected
**Members:** 61 nodes

## Members
- [[Active arbitrage opportunities response.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[ActiveArbitrageResponse]] - code - src/serving/api/schemas/arbitrage.py
- [[Aggregate trading volume across exchanges with market share calculations.      A]] - rationale - src/processing/transformations/aggregations.py
- [[Aggregation transformations for Gold layer analytics.]] - rationale - src/processing/transformations/aggregations.py
- [[Arbitrage Schema (Gold)]] - code - src/processing/schemas.py
- [[Arbitrage endpoint response schemas.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[ArbitrageHistoryResponse]] - code - src/serving/api/schemas/arbitrage.py
- [[ArbitrageListResponse]] - code - src/serving/api/schemas/arbitrage.py
- [[ArbitrageResponse]] - code - src/serving/api/schemas/arbitrage.py
- [[ArbitrageSummary]] - code - src/serving/api/schemas/arbitrage.py
- [[Bronze Layer Streaming Pipeline]] - code - src/processing/spark_streaming.py
- [[BronzeLayer Class]] - code - src/storage/medallion.py
- [[Calculate VWAP for multiple time windows simultaneously.      Args         df]] - rationale - src/processing/transformations/aggregations.py
- [[Calculate Volume-Weighted Average Price (VWAP) over time windows.      VWAP = Su]] - rationale - src/processing/transformations/aggregations.py
- [[Calculate liquidity metrics from orderbook data.      Metrics include     - Bid]] - rationale - src/processing/transformations/aggregations.py
- [[Calculate price spread across exchanges for each trading pair.      Args]] - rationale - src/processing/transformations/arbitrage.py
- [[Calculate trading and withdrawal fees for arbitrage.      Args         df Data]] - rationale - src/processing/transformations/arbitrage.py
- [[Calculate volume aggregations for multiple time windows simultaneously.      Arg]] - rationale - src/processing/transformations/aggregations.py
- [[Config_3]] - code - src/serving/api/schemas/arbitrage.py
- [[Cross-Exchange Spread Computation]] - code - src/processing/spark_streaming.py
- [[CryptoStreamingApp Orchestrator]] - code - src/processing/spark_streaming.py
- [[DeltaLakeManager Class]] - code - src/utils/delta_utils.py
- [[DeltaWriter Class]] - code - src/storage/delta_writer.py
- [[Detect arbitrage opportunities across exchanges.      Algorithm     1. Window t]] - rationale - src/processing/transformations/arbitrage.py
- [[Exchange Fee Structure Map]] - code - src/processing/transformations/arbitrage.py
- [[Filter arbitrage opportunities by liquidity score.      Args         df DataFr]] - rationale - src/processing/transformations/arbitrage.py
- [[Get arbitrage opportunities for a specific trading pair.]] - rationale - src/serving/api/routes/arbitrage.py
- [[Get arbitrage opportunities.      Returns detected cross-exchange arbitrage oppo]] - rationale - src/serving/api/routes/arbitrage.py
- [[Get currently viable arbitrage opportunities.      Returns opportunities that ar]] - rationale - src/serving/api/routes/arbitrage.py
- [[Get historical arbitrage opportunities._1]] - rationale - src/serving/api/routes/arbitrage.py
- [[Gold Layer Streaming Pipeline]] - code - src/processing/spark_streaming.py
- [[GoldLayer Class]] - code - src/storage/medallion.py
- [[Historical arbitrage data response.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[Inline Arbitrage Signal Detection]] - code - src/processing/spark_streaming.py
- [[Liquidity Metric Schema (Gold)]] - code - src/processing/schemas.py
- [[Orderbook Schema (Spark)]] - code - src/processing/schemas.py
- [[Response containing list of arbitrage opportunities.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[Silver Layer Streaming Pipeline]] - code - src/processing/spark_streaming.py
- [[SilverLayer Class]] - code - src/storage/medallion.py
- [[Single arbitrage opportunity.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[Summary of arbitrage activity.]] - rationale - src/serving/api/schemas/arbitrage.py
- [[Symbol Map (spark_streaming)]] - code - src/processing/spark_streaming.py
- [[Ticker Schema (Spark)]] - code - src/processing/schemas.py
- [[Trade Schema (Spark)]] - code - src/processing/schemas.py
- [[VWAP Schema (Gold)]] - code - src/processing/schemas.py
- [[Volume Aggregate Schema (Gold)]] - code - src/processing/schemas.py
- [[aggregate_volume()]] - code - src/processing/transformations/aggregations.py
- [[aggregations.py]] - code - src/processing/transformations/aggregations.py
- [[arbitrage.py]] - code - src/serving/api/schemas/arbitrage.py
- [[calculate_fees()]] - code - src/processing/transformations/arbitrage.py
- [[calculate_liquidity_metrics()]] - code - src/processing/transformations/aggregations.py
- [[calculate_multi_window_volume()]] - code - src/processing/transformations/aggregations.py
- [[calculate_multi_window_vwap()]] - code - src/processing/transformations/aggregations.py
- [[calculate_spread()]] - code - src/processing/transformations/arbitrage.py
- [[calculate_vwap()]] - code - src/processing/transformations/aggregations.py
- [[detect_arbitrage_opportunities()]] - code - src/processing/transformations/arbitrage.py
- [[filter_liquidity()]] - code - src/processing/transformations/arbitrage.py
- [[get_active_arbitrage()]] - code - src/serving/api/routes/arbitrage.py
- [[get_arbitrage()]] - code - src/serving/api/routes/arbitrage.py
- [[get_arbitrage_history()]] - code - src/serving/api/routes/arbitrage.py
- [[get_symbol_arbitrage()]] - code - src/serving/api/routes/arbitrage.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/FastAPI_Price_Routes
SORT file.name ASC
```

## Connections to other communities
- 5 edges to [[_COMMUNITY_API Response Models & Schemas]]
- 4 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 2 edges to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 1 edge to [[_COMMUNITY_Symbol Normalizer]]

## Top bridge nodes
- [[arbitrage.py]] - degree 16, connects to 1 community
- [[aggregations.py]] - degree 7, connects to 1 community
- [[ActiveArbitrageResponse]] - degree 7, connects to 1 community
- [[ArbitrageHistoryResponse]] - degree 7, connects to 1 community
- [[ArbitrageListResponse]] - degree 7, connects to 1 community