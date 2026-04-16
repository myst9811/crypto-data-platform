---
source_file: "src/serving/api/routes/prices.py"
type: "rationale"
community: "Exchange List & Volume Aggregates"
location: "L127"
tags:
  - graphify/rationale
  - graphify/INFERRED
  - community/Exchange_List_&_Volume_Aggregates
---

# Get latest prices for a specific symbol.

## Connections
- [[ExchangeListResponse]] - `uses` [INFERRED]
- [[PriceComparisonResponse]] - `uses` [INFERRED]
- [[PriceHistoryResponse]] - `uses` [INFERRED]
- [[PriceListResponse]] - `uses` [INFERRED]
- [[PriceResponse]] - `uses` [INFERRED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[SymbolListResponse]] - `uses` [INFERRED]
- [[get_symbol_prices()]] - `rationale_for` [EXTRACTED]

#graphify/rationale #graphify/INFERRED #community/Exchange_List_&_Volume_Aggregates