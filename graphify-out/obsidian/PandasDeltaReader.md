---
source_file: "src/serving/data_access/pandas_delta_reader.py"
type: "code"
community: "Data Cache & Storage Layer"
location: "L32"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Data_Cache_&_Storage_Layer
---

# PandasDeltaReader

## Connections
- [[.__init__()_11]] - `method` [EXTRACTED]
- [[._read_delta()]] - `method` [EXTRACTED]
- [[._table_exists()_1]] - `method` [EXTRACTED]
- [[.close()_1]] - `method` [EXTRACTED]
- [[.get_active_arbitrage()_1]] - `method` [EXTRACTED]
- [[.get_arbitrage_history()_1]] - `method` [EXTRACTED]
- [[.get_arbitrage_opportunities()_1]] - `method` [EXTRACTED]
- [[.get_available_exchanges()_1]] - `method` [EXTRACTED]
- [[.get_available_symbols()_1]] - `method` [EXTRACTED]
- [[.get_available_windows()_1]] - `method` [EXTRACTED]
- [[.get_latest_prices()_1]] - `method` [EXTRACTED]
- [[.get_liquidity_metrics()_1]] - `method` [EXTRACTED]
- [[.get_liquidity_rankings()_1]] - `method` [EXTRACTED]
- [[.get_price_comparison()_1]] - `method` [EXTRACTED]
- [[.get_price_history()_1]] - `method` [EXTRACTED]
- [[.get_volume_aggregates()_1]] - `method` [EXTRACTED]
- [[.get_volume_rankings()_1]] - `method` [EXTRACTED]
- [[.get_vwap()_1]] - `method` [EXTRACTED]
- [[.get_vwap_history()_1]] - `method` [EXTRACTED]
- [[.health_check()_1]] - `method` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[Cleanup resources on shutdown.]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[FastAPI dependency for DataCache.      Yields         DataCache instance]] - `uses` [INFERRED]
- [[FastAPI dependency for DeltaReader.      Yields         DeltaReader instance]] - `uses` [INFERRED]
- [[FastAPI dependency injection for API routes.]] - `uses` [INFERRED]
- [[Get information about the current backend.]] - `uses` [INFERRED]
- [[Get or create singleton DeltaReader instance.     Automatically selects PySpark]] - `uses` [INFERRED]
- [[Get or create singleton Spark session.     Returns None if PySpark is not availa]] - `uses` [INFERRED]
- [[Get or create singleton cache instance.      Returns         DataCache instance]] - `uses` [INFERRED]
- [[LiquidityData]] - `uses` [INFERRED]
- [[Machine learning layer for crypto arbitrage detection and price prediction.]] - `uses` [INFERRED]
- [[PriceData]] - `uses` [INFERRED]
- [[Read-only Delta Lake reader using pandas (no Spark required).]] - `rationale_for` [EXTRACTED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]
- [[pandas_delta_reader.py]] - `contains` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Data_Cache_&_Storage_Layer