---
type: community
cohesion: 0.17
members: 27
---

# Exchange List & Volume Aggregates

**Cohesion:** 0.17 - loosely connected
**Members:** 27 nodes

## Members
- [[Compare prices across all exchanges for a symbol.      Returns the latest price]] - rationale - src/serving/api/routes/prices.py
- [[Config_5]] - code - src/serving/api/schemas/prices.py
- [[ExchangeListResponse]] - code - src/serving/api/schemas/common.py
- [[Get historical prices for a symbol._1]] - rationale - src/serving/api/routes/prices.py
- [[Get latest prices for a specific symbol.]] - rationale - src/serving/api/routes/prices.py
- [[Get latest prices.      Returns normalized price data from the Silver layer.]] - rationale - src/serving/api/routes/prices.py
- [[Get list of available exchanges._2]] - rationale - src/serving/api/routes/prices.py
- [[Get list of available trading symbols._2]] - rationale - src/serving/api/routes/prices.py
- [[Historical price data response.]] - rationale - src/serving/api/schemas/prices.py
- [[Price comparison across exchanges.]] - rationale - src/serving/api/schemas/prices.py
- [[Price endpoint response schemas.]] - rationale - src/serving/api/schemas/prices.py
- [[PriceComparisonResponse]] - code - src/serving/api/schemas/prices.py
- [[PriceHistoryResponse]] - code - src/serving/api/schemas/prices.py
- [[PriceListResponse]] - code - src/serving/api/schemas/prices.py
- [[PriceResponse]] - code - src/serving/api/schemas/prices.py
- [[Response containing list of exchanges.]] - rationale - src/serving/api/schemas/common.py
- [[Response containing list of prices.]] - rationale - src/serving/api/schemas/prices.py
- [[Response containing list of symbols.]] - rationale - src/serving/api/schemas/common.py
- [[Single price data point.]] - rationale - src/serving/api/schemas/prices.py
- [[SymbolListResponse]] - code - src/serving/api/schemas/common.py
- [[compare_prices()]] - code - src/serving/api/routes/prices.py
- [[get_exchanges()]] - code - src/serving/api/routes/prices.py
- [[get_price_history()]] - code - src/serving/api/routes/prices.py
- [[get_prices()]] - code - src/serving/api/routes/prices.py
- [[get_symbol_prices()]] - code - src/serving/api/routes/prices.py
- [[get_symbols()]] - code - src/serving/api/routes/prices.py
- [[prices.py]] - code - src/serving/api/schemas/prices.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Exchange_List_&_Volume_Aggregates
SORT file.name ASC
```

## Connections to other communities
- 8 edges to [[_COMMUNITY_API Response Models & Schemas]]
- 6 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 1 edge to [[_COMMUNITY_Streamlit Dashboard Pages]]

## Top bridge nodes
- [[prices.py]] - degree 13, connects to 1 community
- [[ExchangeListResponse]] - degree 9, connects to 1 community
- [[SymbolListResponse]] - degree 9, connects to 1 community
- [[PriceComparisonResponse]] - degree 9, connects to 1 community
- [[PriceHistoryResponse]] - degree 9, connects to 1 community