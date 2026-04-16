---
source_file: "src/serving/api/routes/prices.py"
type: "rationale"
community: "Exchange List & Volume Aggregates"
location: "L74"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Exchange_List_&_Volume_Aggregates
---

# Compare prices across all exchanges for a symbol.      Returns the latest price

## Connections
- [[ExchangeListResponse]] - `uses` [INFERRED]
- [[PriceComparisonResponse]] - `uses` [INFERRED]
- [[PriceHistoryResponse]] - `uses` [INFERRED]
- [[PriceListResponse]] - `uses` [INFERRED]
- [[PriceResponse]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[SymbolListResponse]] - `uses` [INFERRED]
- [[compare_prices()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Exchange_List_&_Volume_Aggregates