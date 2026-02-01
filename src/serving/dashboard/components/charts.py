"""Chart components for dashboard using Plotly."""

from typing import Dict, List, Optional
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.serving.dashboard.config import DashboardConfig


def create_price_chart(
    df: pd.DataFrame,
    symbol: str,
    x_col: str = "timestamp",
    y_col: str = "price",
    color_col: Optional[str] = "exchange",
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create a price line chart.

    Args:
        df: DataFrame with price data
        symbol: Trading symbol
        x_col: X-axis column name
        y_col: Y-axis column name
        color_col: Column to use for color grouping
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    if color_col and color_col in df.columns:
        for exchange in df[color_col].unique():
            exchange_data = df[df[color_col] == exchange]
            color = DashboardConfig.get_exchange_color(exchange)
            name = DashboardConfig.get_exchange_name(exchange)
            fig.add_trace(
                go.Scatter(
                    x=exchange_data[x_col],
                    y=exchange_data[y_col],
                    mode="lines",
                    name=name,
                    line=dict(color=color),
                )
            )
    else:
        fig.add_trace(
            go.Scatter(
                x=df[x_col],
                y=df[y_col],
                mode="lines",
                name=symbol,
                line=dict(color=DashboardConfig.COLORS["primary"]),
            )
        )

    fig.update_layout(
        title=f"{symbol} Price",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=height,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_vwap_chart(
    df: pd.DataFrame,
    symbol: str,
    with_bands: bool = True,
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create a VWAP chart with optional standard deviation bands.

    Args:
        df: DataFrame with VWAP data
        symbol: Trading symbol
        with_bands: Include standard deviation bands
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.7, 0.3],
        subplot_titles=(f"{symbol} VWAP", "Volume"),
    )

    # Group by exchange if present
    if "exchange" in df.columns:
        for exchange in df["exchange"].unique():
            exchange_data = df[df["exchange"] == exchange].sort_values("window_start")
            color = DashboardConfig.get_exchange_color(exchange)
            name = DashboardConfig.get_exchange_name(exchange)

            # VWAP line
            fig.add_trace(
                go.Scatter(
                    x=exchange_data["window_start"],
                    y=exchange_data["vwap"],
                    mode="lines",
                    name=f"{name} VWAP",
                    line=dict(color=color),
                ),
                row=1,
                col=1,
            )

            # Volume bars
            fig.add_trace(
                go.Bar(
                    x=exchange_data["window_start"],
                    y=exchange_data["total_volume"],
                    name=f"{name} Volume",
                    marker_color=color,
                    opacity=0.7,
                ),
                row=2,
                col=1,
            )

            # Standard deviation bands
            if with_bands and "std_dev_price" in exchange_data.columns:
                std_dev = exchange_data["std_dev_price"].fillna(0)
                upper = exchange_data["vwap"] + std_dev
                lower = exchange_data["vwap"] - std_dev

                fig.add_trace(
                    go.Scatter(
                        x=exchange_data["window_start"],
                        y=upper,
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )
                fig.add_trace(
                    go.Scatter(
                        x=exchange_data["window_start"],
                        y=lower,
                        mode="lines",
                        line=dict(width=0),
                        fill="tonexty",
                        fillcolor=f"rgba({int(color[1:3], 16)}, {int(color[3:5], 16)}, {int(color[5:7], 16)}, 0.1)",
                        showlegend=False,
                    ),
                    row=1,
                    col=1,
                )
    else:
        df_sorted = df.sort_values("window_start")
        fig.add_trace(
            go.Scatter(
                x=df_sorted["window_start"],
                y=df_sorted["vwap"],
                mode="lines",
                name="VWAP",
                line=dict(color=DashboardConfig.COLORS["primary"]),
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=df_sorted["window_start"],
                y=df_sorted["total_volume"],
                name="Volume",
                marker_color=DashboardConfig.COLORS["secondary"],
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        height=height,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="VWAP (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig


def create_volume_chart(
    df: pd.DataFrame,
    symbol: str,
    stacked: bool = True,
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create a volume bar chart.

    Args:
        df: DataFrame with volume data
        symbol: Trading symbol
        stacked: Stack bars by exchange
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    if "exchange" in df.columns:
        for exchange in df["exchange"].unique():
            exchange_data = df[df["exchange"] == exchange].sort_values("window_start")
            color = DashboardConfig.get_exchange_color(exchange)
            name = DashboardConfig.get_exchange_name(exchange)

            fig.add_trace(
                go.Bar(
                    x=exchange_data["window_start"],
                    y=exchange_data["total_volume"],
                    name=name,
                    marker_color=color,
                )
            )
    else:
        df_sorted = df.sort_values("window_start")
        fig.add_trace(
            go.Bar(
                x=df_sorted["window_start"],
                y=df_sorted["total_volume"],
                name="Volume",
                marker_color=DashboardConfig.COLORS["primary"],
            )
        )

    barmode = "stack" if stacked else "group"
    fig.update_layout(
        title=f"{symbol} Trading Volume",
        xaxis_title="Time",
        yaxis_title="Volume",
        barmode=barmode,
        height=height,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_depth_chart(
    bids: List[Dict],
    asks: List[Dict],
    symbol: str,
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create an order book depth chart.

    Args:
        bids: List of bid levels with price and volume
        asks: List of ask levels with price and volume
        symbol: Trading symbol
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    if bids:
        bid_prices = [b["price"] for b in bids]
        bid_volumes = [b["volume"] for b in bids]

        # Cumulative bid depth
        cumulative_bids = []
        total = 0
        for vol in bid_volumes:
            total += vol
            cumulative_bids.append(total)

        fig.add_trace(
            go.Scatter(
                x=bid_prices,
                y=cumulative_bids,
                fill="tozeroy",
                name="Bids",
                line=dict(color=DashboardConfig.COLORS["buy"]),
                fillcolor=f"rgba(44, 160, 44, 0.3)",
            )
        )

    if asks:
        ask_prices = [a["price"] for a in asks]
        ask_volumes = [a["volume"] for a in asks]

        # Cumulative ask depth
        cumulative_asks = []
        total = 0
        for vol in ask_volumes:
            total += vol
            cumulative_asks.append(total)

        fig.add_trace(
            go.Scatter(
                x=ask_prices,
                y=cumulative_asks,
                fill="tozeroy",
                name="Asks",
                line=dict(color=DashboardConfig.COLORS["sell"]),
                fillcolor=f"rgba(214, 39, 40, 0.3)",
            )
        )

    fig.update_layout(
        title=f"{symbol} Order Book Depth",
        xaxis_title="Price (USD)",
        yaxis_title="Cumulative Volume",
        height=height,
        hovermode="x unified",
    )

    return fig


def create_arbitrage_chart(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create an arbitrage opportunities scatter chart.

    Args:
        df: DataFrame with arbitrage data
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    if "trading_pair" in df.columns:
        for pair in df["trading_pair"].unique():
            pair_data = df[df["trading_pair"] == pair]
            fig.add_trace(
                go.Scatter(
                    x=pair_data["detection_timestamp"],
                    y=pair_data["net_profit_percent"],
                    mode="markers",
                    name=pair,
                    marker=dict(
                        size=10,
                        color=pair_data["net_profit_percent"],
                        colorscale="RdYlGn",
                        showscale=True,
                    ),
                    text=[
                        f"{row['buy_exchange']} → {row['sell_exchange']}"
                        for _, row in pair_data.iterrows()
                    ],
                    hovertemplate="<b>%{text}</b><br>Profit: %{y:.2f}%<extra></extra>",
                )
            )
    else:
        fig.add_trace(
            go.Scatter(
                x=df["detection_timestamp"],
                y=df["net_profit_percent"],
                mode="markers",
                name="Opportunities",
                marker=dict(
                    size=10,
                    color=df["net_profit_percent"],
                    colorscale="RdYlGn",
                    showscale=True,
                ),
            )
        )

    fig.update_layout(
        title="Arbitrage Opportunities Over Time",
        xaxis_title="Time",
        yaxis_title="Net Profit %",
        height=height,
        hovermode="closest",
    )

    return fig


def create_exchange_radar(
    metrics: Dict[str, Dict[str, float]],
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create a radar chart comparing exchanges.

    Args:
        metrics: Dict of exchange -> metric_name -> value
        height: Chart height

    Returns:
        Plotly figure
    """
    fig = go.Figure()

    categories = list(list(metrics.values())[0].keys()) if metrics else []

    for exchange, values in metrics.items():
        color = DashboardConfig.get_exchange_color(exchange)
        name = DashboardConfig.get_exchange_name(exchange)
        r_values = [values.get(cat, 0) for cat in categories]
        r_values.append(r_values[0])  # Close the polygon

        fig.add_trace(
            go.Scatterpolar(
                r=r_values,
                theta=categories + [categories[0]],
                fill="toself",
                name=name,
                line=dict(color=color),
            )
        )

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
        showlegend=True,
        height=height,
        title="Exchange Comparison",
    )

    return fig


def create_market_share_pie(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_CHART_HEIGHT,
) -> go.Figure:
    """
    Create a market share pie chart.

    Args:
        df: DataFrame with exchange and market_share columns
        height: Chart height

    Returns:
        Plotly figure
    """
    colors = [
        DashboardConfig.get_exchange_color(e) for e in df["exchange"]
    ]
    names = [
        DashboardConfig.get_exchange_name(e) for e in df["exchange"]
    ]

    fig = go.Figure(
        data=[
            go.Pie(
                labels=names,
                values=df["market_share"],
                marker=dict(colors=colors),
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Market Share: %{percent}<br>Volume: %{value:.2f}<extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title="Market Share by Exchange",
        height=height,
    )

    return fig
