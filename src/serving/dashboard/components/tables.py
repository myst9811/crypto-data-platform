"""Table components for dashboard."""

from typing import Dict, List, Optional, Callable
import pandas as pd
import streamlit as st

from src.serving.dashboard.config import DashboardConfig


def price_table(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display a price data table with formatting.

    Args:
        df: DataFrame with price data
        height: Table height
    """
    if df.empty:
        st.info("No price data available")
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Rename columns for display
    column_map = {
        "standard_symbol": "Symbol",
        "symbol": "Symbol",
        "exchange": "Exchange",
        "price": "Price",
        "volume": "Volume",
        "side": "Side",
        "timestamp": "Time",
        "data_quality_score": "Quality",
    }
    display_df = display_df.rename(columns=column_map)

    # Format exchange names
    if "Exchange" in display_df.columns:
        display_df["Exchange"] = display_df["Exchange"].apply(
            DashboardConfig.get_exchange_name
        )

    # Select and order columns
    display_cols = [c for c in ["Symbol", "Exchange", "Price", "Volume", "Side", "Time", "Quality"] if c in display_df.columns]
    display_df = display_df[display_cols]

    # Format numeric columns
    if "Price" in display_df.columns:
        display_df["Price"] = display_df["Price"].apply(lambda x: f"${x:,.2f}")
    if "Volume" in display_df.columns:
        display_df["Volume"] = display_df["Volume"].apply(lambda x: f"{x:,.4f}")
    if "Quality" in display_df.columns:
        display_df["Quality"] = display_df["Quality"].apply(
            lambda x: f"{x:.2%}" if pd.notna(x) else "-"
        )

    st.dataframe(display_df, height=height, use_container_width=True)


def arbitrage_table(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display an arbitrage opportunities table with color coding.

    Args:
        df: DataFrame with arbitrage data
        height: Table height
    """
    if df.empty:
        st.info("No arbitrage opportunities available")
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Rename columns for display
    column_map = {
        "trading_pair": "Pair",
        "buy_exchange": "Buy From",
        "sell_exchange": "Sell On",
        "buy_price": "Buy Price",
        "sell_price": "Sell Price",
        "spread_percent": "Spread %",
        "net_profit_percent": "Net Profit %",
        "recommended_action": "Action",
        "detection_timestamp": "Detected",
    }
    display_df = display_df.rename(columns=column_map)

    # Format exchange names
    for col in ["Buy From", "Sell On"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                DashboardConfig.get_exchange_name
            )

    # Select and order columns
    display_cols = [
        c for c in ["Pair", "Buy From", "Buy Price", "Sell On", "Sell Price", "Spread %", "Net Profit %", "Action", "Detected"]
        if c in display_df.columns
    ]
    display_df = display_df[display_cols]

    # Format numeric columns
    for col in ["Buy Price", "Sell Price"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    for col in ["Spread %", "Net Profit %"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")

    # Apply color styling based on action
    def highlight_action(val):
        if val == "execute":
            return "background-color: #d4edda"  # Green
        elif val == "monitor":
            return "background-color: #fff3cd"  # Yellow
        else:
            return "background-color: #f8d7da"  # Red

    styled_df = display_df.style
    if "Action" in display_df.columns:
        styled_df = styled_df.applymap(highlight_action, subset=["Action"])

    st.dataframe(styled_df, height=height, use_container_width=True)


def volume_rankings_table(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display a volume rankings table.

    Args:
        df: DataFrame with volume data
        height: Table height
    """
    if df.empty:
        st.info("No volume data available")
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Rename columns for display
    column_map = {
        "standard_symbol": "Symbol",
        "symbol": "Symbol",
        "exchange": "Exchange",
        "total_volume": "Volume",
        "buy_volume": "Buy Vol",
        "sell_volume": "Sell Vol",
        "num_trades": "Trades",
        "volume_rank": "Rank",
        "exchange_market_share": "Market Share",
    }
    display_df = display_df.rename(columns=column_map)

    # Format exchange names
    if "Exchange" in display_df.columns:
        display_df["Exchange"] = display_df["Exchange"].apply(
            DashboardConfig.get_exchange_name
        )

    # Select and order columns
    display_cols = [
        c for c in ["Rank", "Exchange", "Volume", "Buy Vol", "Sell Vol", "Trades", "Market Share"]
        if c in display_df.columns
    ]
    display_df = display_df[display_cols]

    # Format numeric columns
    for col in ["Volume", "Buy Vol", "Sell Vol"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.2f}" if pd.notna(x) else "-"
            )
    if "Market Share" in display_df.columns:
        display_df["Market Share"] = display_df["Market Share"].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) else "-"
        )

    st.dataframe(display_df, height=height, use_container_width=True)


def liquidity_table(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display a liquidity metrics table.

    Args:
        df: DataFrame with liquidity data
        height: Table height
    """
    if df.empty:
        st.info("No liquidity data available")
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Rename columns for display
    column_map = {
        "standard_symbol": "Symbol",
        "symbol": "Symbol",
        "exchange": "Exchange",
        "bid_price": "Bid",
        "ask_price": "Ask",
        "spread_percent": "Spread %",
        "bid_depth": "Bid Depth",
        "ask_depth": "Ask Depth",
        "liquidity_score": "Liquidity Score",
        "timestamp": "Time",
    }
    display_df = display_df.rename(columns=column_map)

    # Format exchange names
    if "Exchange" in display_df.columns:
        display_df["Exchange"] = display_df["Exchange"].apply(
            DashboardConfig.get_exchange_name
        )

    # Select and order columns
    display_cols = [
        c for c in ["Symbol", "Exchange", "Bid", "Ask", "Spread %", "Bid Depth", "Ask Depth", "Liquidity Score", "Time"]
        if c in display_df.columns
    ]
    display_df = display_df[display_cols]

    # Format numeric columns
    for col in ["Bid", "Ask"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
    if "Spread %" in display_df.columns:
        display_df["Spread %"] = display_df["Spread %"].apply(lambda x: f"{x:.3f}%")
    for col in ["Bid Depth", "Ask Depth", "Liquidity Score"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x:,.2f}" if pd.notna(x) else "-"
            )

    st.dataframe(display_df, height=height, use_container_width=True)


def vwap_table(
    df: pd.DataFrame,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display a VWAP data table.

    Args:
        df: DataFrame with VWAP data
        height: Table height
    """
    if df.empty:
        st.info("No VWAP data available")
        return

    # Prepare display dataframe
    display_df = df.copy()

    # Rename columns for display
    column_map = {
        "standard_symbol": "Symbol",
        "symbol": "Symbol",
        "exchange": "Exchange",
        "vwap": "VWAP",
        "total_volume": "Volume",
        "num_trades": "Trades",
        "min_price": "Min",
        "max_price": "Max",
        "std_dev_price": "Std Dev",
        "window_start": "Window Start",
        "window_end": "Window End",
    }
    display_df = display_df.rename(columns=column_map)

    # Format exchange names
    if "Exchange" in display_df.columns:
        display_df["Exchange"] = display_df["Exchange"].apply(
            DashboardConfig.get_exchange_name
        )

    # Select and order columns
    display_cols = [
        c for c in ["Symbol", "Exchange", "VWAP", "Volume", "Trades", "Min", "Max", "Std Dev", "Window Start"]
        if c in display_df.columns
    ]
    display_df = display_df[display_cols]

    # Format numeric columns
    for col in ["VWAP", "Min", "Max"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "-")
    if "Volume" in display_df.columns:
        display_df["Volume"] = display_df["Volume"].apply(lambda x: f"{x:,.4f}")
    if "Std Dev" in display_df.columns:
        display_df["Std Dev"] = display_df["Std Dev"].apply(
            lambda x: f"${x:.4f}" if pd.notna(x) else "-"
        )

    st.dataframe(display_df, height=height, use_container_width=True)


def styled_dataframe(
    df: pd.DataFrame,
    style_config: Optional[Dict] = None,
    height: int = DashboardConfig.DEFAULT_TABLE_HEIGHT,
) -> None:
    """
    Display a styled dataframe with custom configuration.

    Args:
        df: DataFrame to display
        style_config: Dictionary with styling options
        height: Table height
    """
    if df.empty:
        st.info("No data available")
        return

    if style_config is None:
        st.dataframe(df, height=height, use_container_width=True)
        return

    styled = df.style

    # Apply column formatting
    if "format" in style_config:
        styled = styled.format(style_config["format"])

    # Apply conditional formatting
    if "highlight_max" in style_config:
        styled = styled.highlight_max(
            subset=style_config["highlight_max"],
            color="lightgreen",
        )
    if "highlight_min" in style_config:
        styled = styled.highlight_min(
            subset=style_config["highlight_min"],
            color="lightcoral",
        )

    # Apply background gradients
    if "gradient" in style_config:
        for col, cmap in style_config["gradient"].items():
            if col in df.columns:
                styled = styled.background_gradient(cmap=cmap, subset=[col])

    st.dataframe(styled, height=height, use_container_width=True)
