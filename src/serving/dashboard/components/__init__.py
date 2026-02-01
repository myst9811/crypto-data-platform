"""Dashboard reusable components."""

from .filters import (
    symbol_filter,
    exchange_filter,
    time_range_filter,
    window_duration_filter,
    profit_threshold_filter,
    refresh_rate_filter,
    quick_time_range_filter,
)
from .metrics import (
    price_metric,
    volume_metric,
    spread_metric,
    arbitrage_metric,
    multi_metric_row,
    status_indicator,
    kpi_card,
)
from .charts import (
    create_price_chart,
    create_vwap_chart,
    create_volume_chart,
    create_depth_chart,
    create_arbitrage_chart,
    create_exchange_radar,
    create_market_share_pie,
)
from .tables import (
    price_table,
    arbitrage_table,
    volume_rankings_table,
    liquidity_table,
    vwap_table,
    styled_dataframe,
)

__all__ = [
    # Filters
    "symbol_filter",
    "exchange_filter",
    "time_range_filter",
    "window_duration_filter",
    "profit_threshold_filter",
    "refresh_rate_filter",
    "quick_time_range_filter",
    # Metrics
    "price_metric",
    "volume_metric",
    "spread_metric",
    "arbitrage_metric",
    "multi_metric_row",
    "status_indicator",
    "kpi_card",
    # Charts
    "create_price_chart",
    "create_vwap_chart",
    "create_volume_chart",
    "create_depth_chart",
    "create_arbitrage_chart",
    "create_exchange_radar",
    "create_market_share_pie",
    # Tables
    "price_table",
    "arbitrage_table",
    "volume_rankings_table",
    "liquidity_table",
    "vwap_table",
    "styled_dataframe",
]
