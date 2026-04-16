---
source_file: "src/serving/data_access/delta_reader.py"
type: "code"
community: "Data Cache & Storage Layer"
location: "L30"
tags:
  - graphify/code
  - graphify/EXTRACTED
  - community/Data_Cache_&_Storage_Layer
---

# DeltaReader

## Connections
- [[.__init__()_9]] - `method` [EXTRACTED]
- [[._create_spark_session()]] - `method` [EXTRACTED]
- [[._table_exists()]] - `method` [EXTRACTED]
- [[._to_pandas()]] - `method` [EXTRACTED]
- [[.close()]] - `method` [EXTRACTED]
- [[.get_active_arbitrage()]] - `method` [EXTRACTED]
- [[.get_arbitrage_history()]] - `method` [EXTRACTED]
- [[.get_arbitrage_opportunities()]] - `method` [EXTRACTED]
- [[.get_available_exchanges()]] - `method` [EXTRACTED]
- [[.get_available_symbols()]] - `method` [EXTRACTED]
- [[.get_available_windows()]] - `method` [EXTRACTED]
- [[.get_latest_prices()]] - `method` [EXTRACTED]
- [[.get_liquidity_metrics()]] - `method` [EXTRACTED]
- [[.get_liquidity_rankings()]] - `method` [EXTRACTED]
- [[.get_price_comparison()]] - `method` [EXTRACTED]
- [[.get_price_history()]] - `method` [EXTRACTED]
- [[.get_volume_aggregates()]] - `method` [EXTRACTED]
- [[.get_volume_rankings()]] - `method` [EXTRACTED]
- [[.get_vwap()]] - `method` [EXTRACTED]
- [[.get_vwap_history()]] - `method` [EXTRACTED]
- [[.health_check()]] - `method` [EXTRACTED]
- [[ArbitrageData]] - `uses` [INFERRED]
- [[Cleanup resources on shutdown.]] - `uses` [INFERRED]
- [[DataCache]] - `uses` [INFERRED]
- [[DeltaLakeManager]] - `uses` [INFERRED]
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
- [[Read-only Delta Lake reader for API and Dashboard.]] - `rationale_for` [EXTRACTED]
- [[ServingConfig]] - `uses` [INFERRED]
- [[VWAPData]] - `uses` [INFERRED]
- [[VolumeData]] - `uses` [INFERRED]
- [[delta_reader.py]] - `contains` [EXTRACTED]

#graphify/code #graphify/EXTRACTED #community/Data_Cache_&_Storage_Layer