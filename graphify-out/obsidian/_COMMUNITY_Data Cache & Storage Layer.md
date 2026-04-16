---
type: community
cohesion: 0.06
members: 138
---

# Data Cache & Storage Layer

**Cohesion:** 0.06 - loosely connected
**Members:** 138 nodes

## Members
- [[.__init__()_10]] - code - src/serving/data_access/cache.py
- [[.__init__()_9]] - code - src/serving/data_access/delta_reader.py
- [[.__init__()_13]] - code - src/utils/delta_utils.py
- [[.__init__()_11]] - code - src/serving/data_access/pandas_delta_reader.py
- [[._create_spark_session()]] - code - src/serving/data_access/delta_reader.py
- [[._read_delta()]] - code - src/serving/data_access/pandas_delta_reader.py
- [[._table_exists()]] - code - src/serving/data_access/delta_reader.py
- [[._table_exists()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[._to_pandas()]] - code - src/serving/data_access/delta_reader.py
- [[.clear()]] - code - src/serving/data_access/cache.py
- [[.close()]] - code - src/serving/data_access/delta_reader.py
- [[.close()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.delete()]] - code - src/serving/data_access/cache.py
- [[.get()]] - code - src/serving/data_access/cache.py
- [[.get_active_arbitrage()]] - code - src/serving/data_access/delta_reader.py
- [[.get_active_arbitrage()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_arbitrage_history()]] - code - src/serving/data_access/delta_reader.py
- [[.get_arbitrage_history()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_arbitrage_opportunities()]] - code - src/serving/data_access/delta_reader.py
- [[.get_arbitrage_opportunities()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_available_exchanges()]] - code - src/serving/data_access/delta_reader.py
- [[.get_available_exchanges()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_available_symbols()]] - code - src/serving/data_access/delta_reader.py
- [[.get_available_symbols()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_available_windows()]] - code - src/serving/data_access/delta_reader.py
- [[.get_available_windows()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_latest_prices()]] - code - src/serving/data_access/delta_reader.py
- [[.get_latest_prices()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_liquidity_metrics()]] - code - src/serving/data_access/delta_reader.py
- [[.get_liquidity_metrics()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_liquidity_rankings()]] - code - src/serving/data_access/delta_reader.py
- [[.get_liquidity_rankings()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_price_comparison()]] - code - src/serving/data_access/delta_reader.py
- [[.get_price_comparison()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_price_history()]] - code - src/serving/data_access/delta_reader.py
- [[.get_price_history()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_table_history()]] - code - src/utils/delta_utils.py
- [[.get_volume_aggregates()]] - code - src/serving/data_access/delta_reader.py
- [[.get_volume_aggregates()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_volume_rankings()]] - code - src/serving/data_access/delta_reader.py
- [[.get_volume_rankings()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_vwap()]] - code - src/serving/data_access/delta_reader.py
- [[.get_vwap()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.get_vwap_history()]] - code - src/serving/data_access/delta_reader.py
- [[.get_vwap_history()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.health_check()]] - code - src/serving/data_access/delta_reader.py
- [[.health_check()_1]] - code - src/serving/data_access/pandas_delta_reader.py
- [[.merge_data()]] - code - src/utils/delta_utils.py
- [[.read_from_delta()]] - code - src/utils/delta_utils.py
- [[.set()]] - code - src/serving/data_access/cache.py
- [[.size()]] - code - src/serving/data_access/cache.py
- [[.table_exists()]] - code - src/utils/delta_utils.py
- [[.vacuum_table()]] - code - src/utils/delta_utils.py
- [[Arbitrage opportunity from Gold layer.]] - rationale - src/serving/data_access/models.py
- [[ArbitrageData]] - code - src/serving/data_access/models.py
- [[Check connectivity to Delta Lake tables.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Check connectivity to Delta Lake tables.          Returns             Dict with]] - rationale - src/serving/data_access/delta_reader.py
- [[Check if Delta table exists.]] - rationale - src/serving/data_access/delta_reader.py
- [[Check if Delta table exists._1]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Check if Delta table exists.          Args             path Delta table path]] - rationale - src/utils/delta_utils.py
- [[Cleanup (no-op for pandas reader).]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Cleanup resources on shutdown.]] - rationale - src/serving/api/dependencies.py
- [[Clear all cached entries.]] - rationale - src/serving/data_access/cache.py
- [[Configuration for the serving layer.]] - rationale - src/serving/config.py
- [[Convert Spark DataFrame to Pandas.]] - rationale - src/serving/data_access/delta_reader.py
- [[Create Spark session optimized for reading.]] - rationale - src/serving/data_access/delta_reader.py
- [[DataCache]] - code - src/serving/data_access/cache.py
- [[Delete a specific key from cache.          Args             key Cache key to d]] - rationale - src/serving/data_access/cache.py
- [[Delta Lake reader for serving layer - wraps DeltaLakeManager for read operations]] - rationale - src/serving/data_access/delta_reader.py
- [[Delta Lake reader using deltalake package (no Spark required).]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[DeltaLakeManager]] - code - src/utils/delta_utils.py
- [[DeltaReader]] - code - src/serving/data_access/delta_reader.py
- [[FastAPI dependency for DataCache.      Yields         DataCache instance]] - rationale - src/serving/api/dependencies.py
- [[FastAPI dependency for DeltaReader.      Yields         DeltaReader instance]] - rationale - src/serving/api/dependencies.py
- [[FastAPI dependency injection for API routes.]] - rationale - src/serving/api/dependencies.py
- [[Get Delta table history.          Args             path Delta table path]] - rationale - src/utils/delta_utils.py
- [[Get VWAP metrics.          Args             symbol Filter by symbol]] - rationale - src/serving/data_access/delta_reader.py
- [[Get arbitrage opportunities.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get arbitrage opportunities.          Args             symbol Filter by tradin]] - rationale - src/serving/data_access/delta_reader.py
- [[Get color for exchange.]] - rationale - src/serving/dashboard/config.py
- [[Get current cache size.]] - rationale - src/serving/data_access/cache.py
- [[Get currently viable arbitrage opportunities.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get currently viable arbitrage opportunities.          Args             min_pro]] - rationale - src/serving/data_access/delta_reader.py
- [[Get display name for exchange.]] - rationale - src/serving/dashboard/config.py
- [[Get exchange rankings by liquidity score.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get exchange rankings by liquidity score.          Args             symbol Tra]] - rationale - src/serving/data_access/delta_reader.py
- [[Get exchange rankings by volume for a symbol.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get exchange rankings by volume for a symbol.          Args             symbol]] - rationale - src/serving/data_access/delta_reader.py
- [[Get historical VWAP data.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get historical VWAP data.          Args             symbol Trading symbol]] - rationale - src/serving/data_access/delta_reader.py
- [[Get historical arbitrage opportunities.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get historical arbitrage opportunities.          Args             start Start]] - rationale - src/serving/data_access/delta_reader.py
- [[Get historical prices for a symbol.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get historical prices for a symbol.          Args             symbol Trading s]] - rationale - src/serving/data_access/delta_reader.py
- [[Get information about the current backend.]] - rationale - src/serving/api/dependencies.py
- [[Get latest prices across all exchanges for comparison.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get latest prices across all exchanges for comparison.          Args]] - rationale - src/serving/data_access/delta_reader.py
- [[Get latest prices from normalized_prices table.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get latest prices from normalized_prices table.          Args             symbo]] - rationale - src/serving/data_access/delta_reader.py
- [[Get liquidity metrics.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get liquidity metrics.          Args             symbol Filter by symbol]] - rationale - src/serving/data_access/delta_reader.py
- [[Get list of available exchanges.]] - rationale - src/serving/data_access/delta_reader.py
- [[Get list of available exchanges._1]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get list of available trading symbols.]] - rationale - src/serving/data_access/delta_reader.py
- [[Get list of available trading symbols._1]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get list of available window durations.]] - rationale - src/serving/data_access/delta_reader.py
- [[Get list of available window durations._1]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get or create singleton DeltaReader instance.     Automatically selects PySpark]] - rationale - src/serving/api/dependencies.py
- [[Get or create singleton Spark session.     Returns None if PySpark is not availa]] - rationale - src/serving/api/dependencies.py
- [[Get or create singleton cache instance.      Returns         DataCache instance]] - rationale - src/serving/api/dependencies.py
- [[Get value from cache.          Args             key Cache key          Returns]] - rationale - src/serving/data_access/cache.py
- [[Get volume aggregates.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Get volume aggregates.          Args             symbol Filter by symbol]] - rationale - src/serving/data_access/delta_reader.py
- [[Initialize Delta Lake manager.          Args             spark Active Spark se]] - rationale - src/utils/delta_utils.py
- [[Initialize Delta reader.          Args             cache Optional cache instan]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Initialize Delta reader.          Args             spark Optional Spark sessio]] - rationale - src/serving/data_access/delta_reader.py
- [[Initialize cache.          Args             ttl Time-to-live in seconds (defau]] - rationale - src/serving/data_access/cache.py
- [[Liquidity metrics from Gold layer.]] - rationale - src/serving/data_access/models.py
- [[LiquidityData]] - code - src/serving/data_access/models.py
- [[Machine learning layer for crypto arbitrage detection and price prediction.]] - rationale - ml/__init__.py
- [[Manager for Delta Lake operations.]] - rationale - src/utils/delta_utils.py
- [[Normalized price data from Silver layer.]] - rationale - src/serving/data_access/models.py
- [[PandasDeltaReader]] - code - src/serving/data_access/pandas_delta_reader.py
- [[Perform MERGE operation (upsert) on Delta table.          Args             targ]] - rationale - src/utils/delta_utils.py
- [[PriceData]] - code - src/serving/data_access/models.py
- [[Read DataFrame from Delta Lake.          Args             path Delta table pat]] - rationale - src/utils/delta_utils.py
- [[Read Delta table to pandas DataFrame with column harmonization.]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[Read-only Delta Lake reader for API and Dashboard.]] - rationale - src/serving/data_access/delta_reader.py
- [[Read-only Delta Lake reader using pandas (no Spark required).]] - rationale - src/serving/data_access/pandas_delta_reader.py
- [[ServingConfig]] - code - src/serving/config.py
- [[Set value in cache.          Args             key Cache key             value]] - rationale - src/serving/data_access/cache.py
- [[Thread-safe TTL cache for data access layer.]] - rationale - src/serving/data_access/cache.py
- [[VWAP metrics from Gold layer.]] - rationale - src/serving/data_access/models.py
- [[VWAPData]] - code - src/serving/data_access/models.py
- [[Vacuum Delta table to remove old files.          Args             path Delta t]] - rationale - src/utils/delta_utils.py
- [[Volume aggregates from Gold layer.]] - rationale - src/serving/data_access/models.py
- [[VolumeData]] - code - src/serving/data_access/models.py
- [[pandas_delta_reader.py]] - code - src/serving/data_access/pandas_delta_reader.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Data_Cache_&_Storage_Layer
SORT file.name ASC
```

## Connections to other communities
- 14 edges to [[_COMMUNITY_API Response Models & Schemas]]
- 12 edges to [[_COMMUNITY_Streamlit Dashboard Pages]]
- 8 edges to [[_COMMUNITY_API Dependencies]]
- 6 edges to [[_COMMUNITY_Exchange List & Volume Aggregates]]
- 5 edges to [[_COMMUNITY_Exchange WebSocket Producers]]
- 4 edges to [[_COMMUNITY_FastAPI Price Routes]]
- 3 edges to [[_COMMUNITY_Medallion Layer Coordinator]]
- 2 edges to [[_COMMUNITY_GARCH Volatility Models]]
- 2 edges to [[_COMMUNITY_Delta Table Optimizer]]
- 1 edge to [[_COMMUNITY_Health Check Endpoints]]
- 1 edge to [[_COMMUNITY_Spark Streaming Core]]
- 1 edge to [[_COMMUNITY_Kafka Utilities]]
- 1 edge to [[_COMMUNITY_Delta Lake Writer]]

## Top bridge nodes
- [[Machine learning layer for crypto arbitrage detection and price prediction.]] - degree 27, connects to 8 communities
- [[ServingConfig]] - degree 74, connects to 3 communities
- [[DeltaLakeManager]] - degree 34, connects to 2 communities
- [[DataCache]] - degree 63, connects to 1 community
- [[ArbitrageData]] - degree 49, connects to 1 community