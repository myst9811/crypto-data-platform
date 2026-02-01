"""Metric display components for dashboard."""

from typing import Dict, List, Optional, Union
import streamlit as st

from src.serving.dashboard.config import DashboardConfig


def price_metric(
    symbol: str,
    price: float,
    change: Optional[float] = None,
    exchange: Optional[str] = None,
    label: Optional[str] = None,
) -> None:
    """
    Display a price metric card.

    Args:
        symbol: Trading symbol
        price: Current price
        change: Price change (percentage or absolute)
        exchange: Exchange name
        label: Custom label
    """
    display_label = label or symbol
    if exchange:
        display_label = f"{display_label} ({DashboardConfig.get_exchange_name(exchange)})"

    delta = None
    delta_color = "normal"
    if change is not None:
        delta = f"{change:+.2f}%"
        delta_color = "normal"  # Streamlit handles color automatically

    st.metric(
        label=display_label,
        value=f"${price:,.2f}",
        delta=delta,
        delta_color=delta_color,
    )


def volume_metric(
    symbol: str,
    volume: float,
    change: Optional[float] = None,
    label: Optional[str] = None,
) -> None:
    """
    Display a volume metric card.

    Args:
        symbol: Trading symbol
        volume: Volume value
        change: Volume change percentage
        label: Custom label
    """
    display_label = label or f"{symbol} Volume"

    # Format volume with appropriate suffix
    if volume >= 1_000_000:
        formatted = f"{volume / 1_000_000:.2f}M"
    elif volume >= 1_000:
        formatted = f"{volume / 1_000:.2f}K"
    else:
        formatted = f"{volume:.2f}"

    delta = f"{change:+.1f}%" if change is not None else None

    st.metric(
        label=display_label,
        value=formatted,
        delta=delta,
    )


def spread_metric(
    exchange: str,
    spread_percent: float,
    spread_absolute: Optional[float] = None,
    label: Optional[str] = None,
) -> None:
    """
    Display a spread metric card.

    Args:
        exchange: Exchange name
        spread_percent: Spread as percentage
        spread_absolute: Absolute spread value
        label: Custom label
    """
    display_label = label or f"{DashboardConfig.get_exchange_name(exchange)} Spread"

    # Color based on spread (lower is better)
    help_text = None
    if spread_absolute:
        help_text = f"Absolute: ${spread_absolute:.4f}"

    st.metric(
        label=display_label,
        value=f"{spread_percent:.3f}%",
        help=help_text,
    )


def arbitrage_metric(
    buy_exchange: str,
    sell_exchange: str,
    profit_percent: float,
    spread_absolute: Optional[float] = None,
    label: Optional[str] = None,
) -> None:
    """
    Display an arbitrage opportunity metric.

    Args:
        buy_exchange: Exchange to buy from
        sell_exchange: Exchange to sell on
        profit_percent: Net profit percentage
        spread_absolute: Absolute spread
        label: Custom label
    """
    buy_name = DashboardConfig.get_exchange_name(buy_exchange)
    sell_name = DashboardConfig.get_exchange_name(sell_exchange)
    display_label = label or f"{buy_name} → {sell_name}"

    help_text = None
    if spread_absolute:
        help_text = f"Spread: ${spread_absolute:.2f}"

    # Use positive delta to show profit
    st.metric(
        label=display_label,
        value=f"{profit_percent:.2f}%",
        delta="Profit" if profit_percent > 0 else "Loss",
        delta_color="normal" if profit_percent > 0 else "inverse",
        help=help_text,
    )


def multi_metric_row(
    metrics: List[Dict],
    columns: int = 4,
) -> None:
    """
    Display multiple metrics in a row.

    Args:
        metrics: List of metric dictionaries with keys:
            - type: "price", "volume", "spread", or "arbitrage"
            - Plus type-specific keys
        columns: Number of columns
    """
    cols = st.columns(columns)

    for i, metric in enumerate(metrics):
        with cols[i % columns]:
            metric_type = metric.get("type", "price")

            if metric_type == "price":
                price_metric(
                    symbol=metric.get("symbol", ""),
                    price=metric.get("price", 0),
                    change=metric.get("change"),
                    exchange=metric.get("exchange"),
                    label=metric.get("label"),
                )
            elif metric_type == "volume":
                volume_metric(
                    symbol=metric.get("symbol", ""),
                    volume=metric.get("volume", 0),
                    change=metric.get("change"),
                    label=metric.get("label"),
                )
            elif metric_type == "spread":
                spread_metric(
                    exchange=metric.get("exchange", ""),
                    spread_percent=metric.get("spread_percent", 0),
                    spread_absolute=metric.get("spread_absolute"),
                    label=metric.get("label"),
                )
            elif metric_type == "arbitrage":
                arbitrage_metric(
                    buy_exchange=metric.get("buy_exchange", ""),
                    sell_exchange=metric.get("sell_exchange", ""),
                    profit_percent=metric.get("profit_percent", 0),
                    spread_absolute=metric.get("spread_absolute"),
                    label=metric.get("label"),
                )


def status_indicator(
    label: str,
    status: bool,
    true_text: str = "Active",
    false_text: str = "Inactive",
) -> None:
    """
    Display a status indicator.

    Args:
        label: Status label
        status: True for active/healthy
        true_text: Text when status is True
        false_text: Text when status is False
    """
    color = "green" if status else "red"
    text = true_text if status else false_text
    st.markdown(f"**{label}:** :{color}[{text}]")


def kpi_card(
    title: str,
    value: Union[str, float, int],
    subtitle: Optional[str] = None,
    icon: Optional[str] = None,
) -> None:
    """
    Display a KPI card with title, value, and optional subtitle.

    Args:
        title: Card title
        value: Main value to display
        subtitle: Optional subtitle or description
        icon: Optional emoji icon
    """
    if icon:
        st.markdown(f"### {icon} {title}")
    else:
        st.markdown(f"### {title}")

    if isinstance(value, float):
        st.markdown(f"# {value:,.2f}")
    elif isinstance(value, int):
        st.markdown(f"# {value:,}")
    else:
        st.markdown(f"# {value}")

    if subtitle:
        st.caption(subtitle)
