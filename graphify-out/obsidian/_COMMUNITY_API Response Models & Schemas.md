---
type: community
cohesion: 0.05
members: 78
---

# API Response Models & Schemas

**Cohesion:** 0.05 - loosely connected
**Members:** 78 nodes

## Members
- [[APIResponse]] - code - src/serving/api/schemas/common.py
- [[BaseModel]] - code
- [[Common API response schemas.]] - rationale - src/serving/api/schemas/common.py
- [[Config_1]] - code - src/serving/api/schemas/liquidity.py
- [[Config]] - code - src/serving/data_access/models.py
- [[Config_4]] - code - src/serving/api/schemas/volume.py
- [[Config_2]] - code - src/serving/api/schemas/vwap.py
- [[DepthListResponse]] - code - src/serving/api/schemas/liquidity.py
- [[DepthResponse]] - code - src/serving/api/schemas/liquidity.py
- [[Error response schema.]] - rationale - src/serving/api/schemas/common.py
- [[ErrorDetail]] - code - src/serving/api/schemas/common.py
- [[ErrorResponse]] - code - src/serving/api/schemas/common.py
- [[Exchange volume rankings response.]] - rationale - src/serving/api/schemas/volume.py
- [[Generic API response wrapper.]] - rationale - src/serving/api/schemas/common.py
- [[Get VWAP metrics for a specific symbol.]] - rationale - src/serving/api/routes/vwap.py
- [[Get VWAP metrics.      Returns Volume Weighted Average Price data from the Gold]] - rationale - src/serving/api/routes/vwap.py
- [[Get exchange rankings by liquidity score.      Returns exchanges ranked by liqui]] - rationale - src/serving/api/routes/liquidity.py
- [[Get exchange rankings by volume for a symbol.      Returns exchanges ranked by t]] - rationale - src/serving/api/routes/volume.py
- [[Get historical VWAP data for a symbol.]] - rationale - src/serving/api/routes/vwap.py
- [[Get liquidity metrics for a specific symbol.]] - rationale - src/serving/api/routes/liquidity.py
- [[Get liquidity metrics.      Returns bidask spreads, depth, and liquidity scores]] - rationale - src/serving/api/routes/liquidity.py
- [[Get list of available window durations._2]] - rationale - src/serving/api/routes/vwap.py
- [[Get market share by exchange for a symbol.      Returns percentage of total volu]] - rationale - src/serving/api/routes/volume.py
- [[Get volume aggregates for a specific symbol.]] - rationale - src/serving/api/routes/volume.py
- [[Get volume aggregates.      Returns volume data from the Gold layer.]] - rationale - src/serving/api/routes/volume.py
- [[Historical VWAP data response.]] - rationale - src/serving/api/schemas/vwap.py
- [[Liquidity endpoint response schemas.]] - rationale - src/serving/api/schemas/liquidity.py
- [[Liquidity rankings by exchange.]] - rationale - src/serving/api/schemas/liquidity.py
- [[LiquidityListResponse]] - code - src/serving/api/schemas/liquidity.py
- [[LiquidityRankingResponse]] - code - src/serving/api/schemas/liquidity.py
- [[LiquidityResponse]] - code - src/serving/api/schemas/liquidity.py
- [[Market share by exchange.]] - rationale - src/serving/api/schemas/volume.py
- [[MarketShareListResponse]] - code - src/serving/api/schemas/volume.py
- [[MarketShareResponse]] - code - src/serving/api/schemas/volume.py
- [[MetaInfo]] - code - src/serving/api/schemas/common.py
- [[Metadata for API responses.]] - rationale - src/serving/api/schemas/common.py
- [[Order book depth data.]] - rationale - src/serving/api/schemas/liquidity.py
- [[OrderBookData]] - code - src/serving/data_access/models.py
- [[OrderLevel]] - code - src/serving/data_access/models.py
- [[Paginated API response.]] - rationale - src/serving/api/schemas/common.py
- [[PaginatedResponse]] - code - src/serving/api/schemas/common.py
- [[PaginationMeta]] - code - src/serving/api/schemas/common.py
- [[Pydantic models for data access layer - mirrors Spark schemas.]] - rationale - src/serving/data_access/models.py
- [[Response containing depth data.]] - rationale - src/serving/api/schemas/liquidity.py
- [[Response containing list of VWAP data.]] - rationale - src/serving/api/schemas/vwap.py
- [[Response containing list of liquidity data.]] - rationale - src/serving/api/schemas/liquidity.py
- [[Response containing list of volume data.]] - rationale - src/serving/api/schemas/volume.py
- [[Response containing list of window durations.]] - rationale - src/serving/api/schemas/common.py
- [[Response containing market share data.]] - rationale - src/serving/api/schemas/volume.py
- [[Single VWAP data point.]] - rationale - src/serving/api/schemas/vwap.py
- [[Single liquidity metrics data point.]] - rationale - src/serving/api/schemas/liquidity.py
- [[Single order book level.]] - rationale - src/serving/data_access/models.py
- [[Single volume aggregate data point.]] - rationale - src/serving/api/schemas/volume.py
- [[VWAP endpoint response schemas.]] - rationale - src/serving/api/schemas/vwap.py
- [[VWAPHistoryResponse]] - code - src/serving/api/schemas/vwap.py
- [[VWAPListResponse]] - code - src/serving/api/schemas/vwap.py
- [[VWAPResponse]] - code - src/serving/api/schemas/vwap.py
- [[Volume endpoint response schemas.]] - rationale - src/serving/api/schemas/volume.py
- [[VolumeListResponse]] - code - src/serving/api/schemas/volume.py
- [[VolumeRankingResponse]] - code - src/serving/api/schemas/volume.py
- [[VolumeResponse]] - code - src/serving/api/schemas/volume.py
- [[WindowDurationListResponse]] - code - src/serving/api/schemas/common.py
- [[common.py]] - code - src/serving/api/schemas/common.py
- [[get_liquidity()]] - code - src/serving/api/routes/liquidity.py
- [[get_liquidity_rankings()]] - code - src/serving/api/routes/liquidity.py
- [[get_market_share()]] - code - src/serving/api/routes/volume.py
- [[get_symbol_liquidity()]] - code - src/serving/api/routes/liquidity.py
- [[get_symbol_volume()]] - code - src/serving/api/routes/volume.py
- [[get_symbol_vwap()]] - code - src/serving/api/routes/vwap.py
- [[get_volume()]] - code - src/serving/api/routes/volume.py
- [[get_volume_rankings()]] - code - src/serving/api/routes/volume.py
- [[get_vwap()]] - code - src/serving/api/routes/vwap.py
- [[get_vwap_history()]] - code - src/serving/api/routes/vwap.py
- [[get_windows()]] - code - src/serving/api/routes/vwap.py
- [[liquidity.py]] - code - src/serving/api/schemas/liquidity.py
- [[models.py]] - code - src/serving/data_access/models.py
- [[volume.py]] - code - src/serving/api/schemas/volume.py
- [[vwap.py]] - code - src/serving/api/schemas/vwap.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/API_Response_Models_&_Schemas
SORT file.name ASC
```

## Connections to other communities
- 14 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 8 edges to [[_COMMUNITY_Exchange List & Volume Aggregates]]
- 5 edges to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 5 edges to [[_COMMUNITY_FastAPI Price Routes]]
- 2 edges to [[_COMMUNITY_Health Check Endpoints]]

## Top bridge nodes
- [[BaseModel]] - degree 37, connects to 4 communities
- [[common.py]] - degree 12, connects to 3 communities
- [[models.py]] - degree 10, connects to 2 communities
- [[volume.py]] - degree 12, connects to 1 community
- [[liquidity.py]] - degree 11, connects to 1 community