---
type: community
cohesion: 0.03
members: 108
---

# Streamlit Dashboard Pages

**Cohesion:** 0.03 - loosely connected
**Members:** 108 nodes

## Members
- [[1_Live_Prices.py]] - code - src/serving/dashboard/pages/1_Live_Prices.py
- [[3_Arbitrage_Alerts.py]] - code - src/serving/dashboard/pages/3_Arbitrage_Alerts.py
- [[5_ML_Insights.py]] - code - src/serving/dashboard/pages/5_ML_Insights.py
- [[Application lifespan handler for startup and shutdown.]] - rationale - src/serving/api/main.py
- [[Arbitrage Alerts Page - Poll ML-enriched signals.]] - rationale - src/serving/dashboard/pages/3_Arbitrage_Alerts.py
- [[Chart components for dashboard using Plotly.]] - rationale - src/serving/dashboard/components/charts.py
- [[Check API health status.]] - rationale - src/serving/dashboard/app.py
- [[Compute RMSE, MAE, and directional accuracy.]] - rationale - ml/evaluation/metrics.py
- [[Compute precision, recall, F1, and AUC-ROC.]] - rationale - ml/evaluation/metrics.py
- [[Configuration for Streamlit dashboard.]] - rationale - src/serving/dashboard/config.py
- [[Configuration for exchange ingestion.]] - rationale - src/ingestion/config.py
- [[Create a VWAP chart with optional standard deviation bands.      Args         d]] - rationale - src/serving/dashboard/components/charts.py
- [[Create a market share pie chart.      Args         df DataFrame with exchange]] - rationale - src/serving/dashboard/components/charts.py
- [[Create a price line chart.      Args         df DataFrame with price data]] - rationale - src/serving/dashboard/components/charts.py
- [[Create a profit threshold slider.      Args         default Default threshold]] - rationale - src/serving/dashboard/components/filters.py
- [[Create a quick time range selector with preset options.      Args         key]] - rationale - src/serving/dashboard/components/filters.py
- [[Create a radar chart comparing exchanges.      Args         metrics Dict of ex]] - rationale - src/serving/dashboard/components/charts.py
- [[Create a refresh rate selector.      Args         default Default refresh inte]] - rationale - src/serving/dashboard/components/filters.py
- [[Create a symbol selector.      Args         default Default selected symbol]] - rationale - src/serving/dashboard/components/filters.py
- [[Create a time range selector.      Args         default_hours Default time ran]] - rationale - src/serving/dashboard/components/filters.py
- [[Create a volume bar chart.      Args         df DataFrame with volume data]] - rationale - src/serving/dashboard/components/charts.py
- [[Create a window duration selector.      Args         default Default window du]] - rationale - src/serving/dashboard/components/filters.py
- [[Create an arbitrage opportunities scatter chart.      Args         df DataFram]] - rationale - src/serving/dashboard/components/charts.py
- [[Create an exchange selector.      Args         multi Allow multiple selections]] - rationale - src/serving/dashboard/components/filters.py
- [[Create an order book depth chart.      Args         bids List of bid levels wi]] - rationale - src/serving/dashboard/components/charts.py
- [[DashboardConfig]] - code - src/serving/dashboard/config.py
- [[Delta Lake utility functions and managers.]] - rationale - src/utils/delta_utils.py
- [[Display a KPI card with title, value, and optional subtitle.      Args]] - rationale - src/serving/dashboard/components/metrics.py
- [[Display a VWAP data table.      Args         df DataFrame with VWAP data]] - rationale - src/serving/dashboard/components/tables.py
- [[Display a liquidity metrics table.      Args         df DataFrame with liquidi]] - rationale - src/serving/dashboard/components/tables.py
- [[Display a price data table with formatting.      Args         df DataFrame wit]] - rationale - src/serving/dashboard/components/tables.py
- [[Display a spread metric card.      Args         exchange Exchange name]] - rationale - src/serving/dashboard/components/metrics.py
- [[Display a status indicator.      Args         label Status label         statu]] - rationale - src/serving/dashboard/components/metrics.py
- [[Display a styled dataframe with custom configuration.      Args         df Dat]] - rationale - src/serving/dashboard/components/tables.py
- [[Display a volume metric card.      Args         symbol Trading symbol]] - rationale - src/serving/dashboard/components/metrics.py
- [[Display a volume rankings table.      Args         df DataFrame with volume da]] - rationale - src/serving/dashboard/components/tables.py
- [[Display an arbitrage opportunities table with color coding.      Args         d]] - rationale - src/serving/dashboard/components/tables.py
- [[Display an arbitrage opportunity metric.      Args         buy_exchange Exchan]] - rationale - src/serving/dashboard/components/metrics.py
- [[Display multiple metrics in a row.      Args         metrics List of metric di]] - rationale - src/serving/dashboard/components/metrics.py
- [[Evaluation metrics for classifiers and regressors.]] - rationale - ml/evaluation/metrics.py
- [[FastAPI application entry point.]] - rationale - src/serving/api/main.py
- [[Get Spark schema by data type name.      Args         data_type Type of data (]] - rationale - src/processing/schemas.py
- [[Get configuration for a specific exchange.      Args         exchange_name Nam]] - rationale - src/ingestion/config.py
- [[Get list of standard trading pairs.      Returns         List of standard tradi]] - rationale - src/ingestion/config.py
- [[Get symbol mapping for all exchanges.      Returns         Symbol mapping dicti]] - rationale - src/ingestion/config.py
- [[Live Prices Page - Poll API for latest prices.]] - rationale - src/serving/dashboard/pages/1_Live_Prices.py
- [[ML Insights Page - Model performance metrics and feature importance.]] - rationale - src/serving/dashboard/pages/5_ML_Insights.py
- [[Main dashboard application.]] - rationale - src/serving/dashboard/app.py
- [[Root endpoint - redirects to API documentation.]] - rationale - src/serving/api/main.py
- [[Sidebar filter components for dashboard.]] - rationale - src/serving/dashboard/components/filters.py
- [[Spark schema definitions for crypto market data.]] - rationale - src/processing/schemas.py
- [[Streamlit dashboard main application.]] - rationale - src/serving/dashboard/app.py
- [[Table components for dashboard.]] - rationale - src/serving/dashboard/components/tables.py
- [[__init__.py]] - code - tests/__init__.py
- [[app.py]] - code - src/serving/dashboard/app.py
- [[arbitrage_metric()]] - code - src/serving/dashboard/components/metrics.py
- [[arbitrage_table()]] - code - src/serving/dashboard/components/tables.py
- [[charts.py]] - code - src/serving/dashboard/components/charts.py
- [[check_api_health()]] - code - src/serving/dashboard/app.py
- [[color_probability()]] - code - src/serving/dashboard/pages/3_Arbitrage_Alerts.py
- [[compute_classifier_metrics()]] - code - ml/evaluation/metrics.py
- [[compute_regression_metrics()]] - code - ml/evaluation/metrics.py
- [[config.py]] - code - src/ingestion/config.py
- [[create_arbitrage_chart()]] - code - src/serving/dashboard/components/charts.py
- [[create_depth_chart()]] - code - src/serving/dashboard/components/charts.py
- [[create_exchange_radar()]] - code - src/serving/dashboard/components/charts.py
- [[create_market_share_pie()]] - code - src/serving/dashboard/components/charts.py
- [[create_path_if_not_exists()]] - code - src/utils/delta_utils.py
- [[create_price_chart()]] - code - src/serving/dashboard/components/charts.py
- [[create_volume_chart()]] - code - src/serving/dashboard/components/charts.py
- [[create_vwap_chart()]] - code - src/serving/dashboard/components/charts.py
- [[delta_reader.py]] - code - src/serving/data_access/delta_reader.py
- [[delta_utils.py]] - code - src/utils/delta_utils.py
- [[exchange_filter()]] - code - src/serving/dashboard/components/filters.py
- [[fetch_arbitrage()]] - code - src/serving/dashboard/pages/3_Arbitrage_Alerts.py
- [[fetch_prices()]] - code - src/serving/dashboard/pages/1_Live_Prices.py
- [[filters.py]] - code - src/serving/dashboard/components/filters.py
- [[get_delta_paths()]] - code - src/serving/config.py
- [[get_exchange_color()]] - code - src/serving/dashboard/config.py
- [[get_exchange_config()]] - code - src/ingestion/config.py
- [[get_exchange_name()]] - code - src/serving/dashboard/config.py
- [[get_schema_by_type()]] - code - src/processing/schemas.py
- [[get_standard_pairs()]] - code - src/ingestion/config.py
- [[get_symbol_mapping()]] - code - src/ingestion/config.py
- [[kpi_card()]] - code - src/serving/dashboard/components/metrics.py
- [[lifespan()]] - code - src/serving/api/main.py
- [[liquidity_table()]] - code - src/serving/dashboard/components/tables.py
- [[main()_3]] - code - src/serving/dashboard/app.py
- [[main.py]] - code - src/serving/api/main.py
- [[metrics.py]] - code - ml/evaluation/metrics.py
- [[multi_metric_row()]] - code - src/serving/dashboard/components/metrics.py
- [[price_metric()]] - code - src/serving/dashboard/components/metrics.py
- [[price_table()]] - code - src/serving/dashboard/components/tables.py
- [[profit_threshold_filter()]] - code - src/serving/dashboard/components/filters.py
- [[quick_time_range_filter()]] - code - src/serving/dashboard/components/filters.py
- [[refresh_rate_filter()]] - code - src/serving/dashboard/components/filters.py
- [[root()]] - code - src/serving/api/main.py
- [[schemas.py]] - code - src/processing/schemas.py
- [[spread_metric()]] - code - src/serving/dashboard/components/metrics.py
- [[status_indicator()]] - code - src/serving/dashboard/components/metrics.py
- [[styled_dataframe()]] - code - src/serving/dashboard/components/tables.py
- [[symbol_filter()]] - code - src/serving/dashboard/components/filters.py
- [[tables.py]] - code - src/serving/dashboard/components/tables.py
- [[time_range_filter()]] - code - src/serving/dashboard/components/filters.py
- [[volume_metric()]] - code - src/serving/dashboard/components/metrics.py
- [[volume_rankings_table()]] - code - src/serving/dashboard/components/tables.py
- [[vwap_table()]] - code - src/serving/dashboard/components/tables.py
- [[window_duration_filter()]] - code - src/serving/dashboard/components/filters.py

## Live Query (requires Dataview plugin)

```dataview
TABLE source_file, type FROM #community/Streamlit_Dashboard_Pages
SORT file.name ASC
```

## Connections to other communities
- 12 edges to [[_COMMUNITY_Data Cache & Storage Layer]]
- 5 edges to [[_COMMUNITY_API Response Models & Schemas]]
- 4 edges to [[_COMMUNITY_Exchange WebSocket Producers]]
- 2 edges to [[_COMMUNITY_FastAPI Price Routes]]
- 1 edge to [[_COMMUNITY_GARCH Volatility Models]]
- 1 edge to [[_COMMUNITY_Health Check Endpoints]]
- 1 edge to [[_COMMUNITY_Exchange List & Volume Aggregates]]
- 1 edge to [[_COMMUNITY_ML API Routes & Endpoints]]
- 1 edge to [[_COMMUNITY_Spark Streaming Core]]
- 1 edge to [[_COMMUNITY_Symbol Normalizer]]
- 1 edge to [[_COMMUNITY_BaseProducer Abstract Class]]
- 1 edge to [[_COMMUNITY_Kafka Utilities]]
- 1 edge to [[_COMMUNITY_Delta Lake Writer]]
- 1 edge to [[_COMMUNITY_Medallion Layer Coordinator]]

## Top bridge nodes
- [[__init__.py]] - degree 32, connects to 14 communities
- [[DashboardConfig]] - degree 40, connects to 1 community
- [[config.py]] - degree 10, connects to 1 community
- [[delta_utils.py]] - degree 4, connects to 1 community
- [[delta_reader.py]] - degree 3, connects to 1 community