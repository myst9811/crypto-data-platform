---
source_file: "src/serving/data_access/cache.py"
type: "code"
community: "Data Cache & Storage Layer"
location: "L16"
tags:
  - graphify/code
  - graphify/INFERRED
  - community/Data_Cache_&_Storage_Layer
---

# DataCache

## Connections
- [[.__init__()_10]] - `method` [EXTRACTED]
- [[.clear()]] - `method` [EXTRACTED]
- [[.delete()]] - `method` [EXTRACTED]
- [[.get()]] - `method` [EXTRACTED]
- [[.set()]] - `method` [EXTRACTED]
- [[.size()]] - `method` [EXTRACTED]
- [[Check connectivity to Delta Lake tables.]] - `uses` [INFERRED]
- [[Check connectivity to Delta Lake tables.          Returns             Dict with]] - `uses` [INFERRED]
- [[Check if Delta table exists.]] - `uses` [INFERRED]
- [[Check if Delta table exists._1]] - `uses` [INFERRED]
- [[Cleanup (no-op for pandas reader).]] - `uses` [INFERRED]
- [[Cleanup resources on shutdown.]] - `uses` [INFERRED]
- [[Convert Spark DataFrame to Pandas.]] - `uses` [INFERRED]
- [[Create Spark session optimized for reading.]] - `uses` [INFERRED]
- [[Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations]] - `uses` [INFERRED]
- [[Delta Lake reader using deltalake package (no Spark required).]] - `uses` [INFERRED]
- [[DeltaReader]] - `uses` [INFERRED]
- [[FastAPI dependency for DataCache.      Yields         DataCache instance]] - `uses` [INFERRED]
- [[FastAPI dependency for DeltaReader.      Yields         DeltaReader instance]] - `uses` [INFERRED]
- [[FastAPI dependency injection for API routes.]] - `uses` [INFERRED]
- [[Get VWAP metrics.          Args             symbol Filter by symbol]] - `uses` [INFERRED]
- [[Get arbitrage opportunities.]] - `uses` [INFERRED]
- [[Get arbitrage opportunities.          Args             symbol Filter by tradin]] - `uses` [INFERRED]
- [[Get currently viable arbitrage opportunities.]] - `uses` [INFERRED]
- [[Get currently viable arbitrage opportunities.          Args             min_pro]] - `uses` [INFERRED]
- [[Get exchange rankings by liquidity score.]] - `uses` [INFERRED]
- [[Get exchange rankings by liquidity score.          Args             symbol Tra]] - `uses` [INFERRED]
- [[Get exchange rankings by volume for a symbol.]] - `uses` [INFERRED]
- [[Get exchange rankings by volume for a symbol.          Args             symbol]] - `uses` [INFERRED]
- [[Get historical VWAP data.]] - `uses` [INFERRED]
- [[Get historical VWAP data.          Args             symbol Trading symbol]] - `uses` [INFERRED]
- [[Get historical arbitrage opportunities.]] - `uses` [INFERRED]
- [[Get historical arbitrage opportunities.          Args             start Start]] - `uses` [INFERRED]
- [[Get historical prices for a symbol.]] - `uses` [INFERRED]
- [[Get historical prices for a symbol.          Args             symbol Trading s]] - `uses` [INFERRED]
- [[Get information about the current backend.]] - `uses` [INFERRED]
- [[Get latest prices across all exchanges for comparison.]] - `uses` [INFERRED]
- [[Get latest prices across all exchanges for comparison.          Args]] - `uses` [INFERRED]
- [[Get latest prices from normalized_prices table.]] - `uses` [INFERRED]
- [[Get latest prices from normalized_prices table.          Args             symbo]] - `uses` [INFERRED]
- [[Get liquidity metrics.]] - `uses` [INFERRED]
- [[Get liquidity metrics.          Args             symbol Filter by symbol]] - `uses` [INFERRED]
- [[Get list of available exchanges.]] - `uses` [INFERRED]
- [[Get list of available exchanges._1]] - `uses` [INFERRED]
- [[Get list of available trading symbols.]] - `uses` [INFERRED]
- [[Get list of available trading symbols._1]] - `uses` [INFERRED]
- [[Get list of available window durations.]] - `uses` [INFERRED]
- [[Get list of available window durations._1]] - `uses` [INFERRED]
- [[Get or create singleton DeltaReader instance.     Automatically selects PySpark]] - `uses` [INFERRED]
- [[Get or create singleton Spark session.     Returns None if PySpark is not availa]] - `uses` [INFERRED]
- [[Get or create singleton cache instance.      Returns         DataCache instance]] - `uses` [INFERRED]
- [[Get volume aggregates.]] - `uses` [INFERRED]
- [[Get volume aggregates.          Args             symbol Filter by symbol]] - `uses` [INFERRED]
- [[Initialize Delta reader.          Args             cache Optional cache instan]] - `uses` [INFERRED]
- [[Initialize Delta reader.          Args             spark Optional Spark sessio]] - `uses` [INFERRED]
- [[Machine learning layer for crypto arbitrage detection and price prediction.]] - `uses` [INFERRED]
- [[PandasDeltaReader]] - `uses` [INFERRED]
- [[Read Delta table to pandas DataFrame with column harmonization.]] - `uses` [INFERRED]
- [[Read-only Delta Lake reader for API and Dashboard.]] - `uses` [INFERRED]
- [[Read-only Delta Lake reader using pandas (no Spark required).]] - `uses` [INFERRED]
- [[Thread-safe TTL cache for data access layer.]] - `rationale_for` [EXTRACTED]
- [[cache.py]] - `contains` [EXTRACTED]
- [[get_cache()]] - `calls` [EXTRACTED]

#graphify/code #graphify/INFERRED #community/Data_Cache_&_Storage_Layer