"""Sidebar filter components for dashboard."""

from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Union
import streamlit as st

from src.serving.dashboard.config import DashboardConfig


def symbol_filter(
    default: str = "BTC/USD",
    key: str = "symbol_filter",
    label: str = "Trading Symbol",
) -> str:
    """
    Create a symbol selector.

    Args:
        default: Default selected symbol
        key: Unique key for the widget
        label: Label for the selector

    Returns:
        Selected symbol
    """
    symbols = DashboardConfig.TRADING_PAIRS
    default_index = symbols.index(default) if default in symbols else 0
    return st.selectbox(
        label,
        options=symbols,
        index=default_index,
        key=key,
    )


def exchange_filter(
    multi: bool = False,
    include_all: bool = True,
    key: str = "exchange_filter",
    label: str = "Exchange",
) -> Union[str, List[str], None]:
    """
    Create an exchange selector.

    Args:
        multi: Allow multiple selections
        include_all: Include "All" option
        key: Unique key for the widget
        label: Label for the selector

    Returns:
        Selected exchange(s) or None for "All"
    """
    exchanges = DashboardConfig.EXCHANGES.copy()
    display_names = [DashboardConfig.get_exchange_name(e) for e in exchanges]

    if multi:
        selected_names = st.multiselect(
            label,
            options=display_names,
            default=display_names,
            key=key,
        )
        # Convert display names back to exchange keys
        return [
            exchanges[display_names.index(name)]
            for name in selected_names
            if name in display_names
        ]
    else:
        if include_all:
            options = ["All"] + display_names
            selected = st.selectbox(label, options=options, key=key)
            if selected == "All":
                return None
            return exchanges[display_names.index(selected)]
        else:
            selected = st.selectbox(label, options=display_names, key=key)
            return exchanges[display_names.index(selected)]


def time_range_filter(
    default_hours: int = 1,
    key_prefix: str = "time_range",
) -> Tuple[datetime, datetime]:
    """
    Create a time range selector.

    Args:
        default_hours: Default time range in hours
        key_prefix: Prefix for widget keys

    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    col1, col2 = st.columns(2)

    end_date = datetime.now()
    start_date = end_date - timedelta(hours=default_hours)

    with col1:
        start = st.date_input(
            "Start Date",
            value=start_date.date(),
            key=f"{key_prefix}_start_date",
        )
        start_time = st.time_input(
            "Start Time",
            value=start_date.time(),
            key=f"{key_prefix}_start_time",
        )

    with col2:
        end = st.date_input(
            "End Date",
            value=end_date.date(),
            key=f"{key_prefix}_end_date",
        )
        end_time = st.time_input(
            "End Time",
            value=end_date.time(),
            key=f"{key_prefix}_end_time",
        )

    start_datetime = datetime.combine(start, start_time)
    end_datetime = datetime.combine(end, end_time)

    return start_datetime, end_datetime


def window_duration_filter(
    default: str = "1min",
    key: str = "window_filter",
    label: str = "Window Duration",
) -> str:
    """
    Create a window duration selector.

    Args:
        default: Default window duration
        key: Unique key for the widget
        label: Label for the selector

    Returns:
        Selected window duration
    """
    windows = DashboardConfig.WINDOW_DURATIONS
    display_labels = {
        "1min": "1 Minute",
        "5min": "5 Minutes",
        "15min": "15 Minutes",
        "1h": "1 Hour",
    }
    display_options = [display_labels.get(w, w) for w in windows]
    default_index = windows.index(default) if default in windows else 0

    selected_display = st.selectbox(
        label,
        options=display_options,
        index=default_index,
        key=key,
    )

    # Convert back to window key
    return windows[display_options.index(selected_display)]


def profit_threshold_filter(
    default: float = 0.5,
    min_value: float = 0.0,
    max_value: float = 5.0,
    step: float = 0.1,
    key: str = "profit_filter",
    label: str = "Min Profit %",
) -> float:
    """
    Create a profit threshold slider.

    Args:
        default: Default threshold
        min_value: Minimum value
        max_value: Maximum value
        step: Step size
        key: Unique key for the widget
        label: Label for the slider

    Returns:
        Selected profit threshold
    """
    return st.slider(
        label,
        min_value=min_value,
        max_value=max_value,
        value=default,
        step=step,
        key=key,
        format="%.1f%%",
    )


def refresh_rate_filter(
    default: int = 10,
    key: str = "refresh_filter",
    label: str = "Auto-refresh (seconds)",
) -> int:
    """
    Create a refresh rate selector.

    Args:
        default: Default refresh interval
        key: Unique key for the widget
        label: Label for the selector

    Returns:
        Selected refresh interval in seconds
    """
    options = DashboardConfig.REFRESH_OPTIONS
    default_index = options.index(default) if default in options else 0

    return st.selectbox(
        label,
        options=options,
        index=default_index,
        key=key,
        format_func=lambda x: f"{x}s",
    )


def quick_time_range_filter(key: str = "quick_time") -> Tuple[datetime, datetime]:
    """
    Create a quick time range selector with preset options.

    Args:
        key: Unique key for the widget

    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    options = {
        "Last 15 minutes": timedelta(minutes=15),
        "Last 1 hour": timedelta(hours=1),
        "Last 4 hours": timedelta(hours=4),
        "Last 24 hours": timedelta(hours=24),
        "Last 7 days": timedelta(days=7),
    }

    selected = st.selectbox(
        "Time Range",
        options=list(options.keys()),
        index=1,  # Default to 1 hour
        key=key,
    )

    end_datetime = datetime.now()
    start_datetime = end_datetime - options[selected]

    return start_datetime, end_datetime
