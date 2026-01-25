"""Aggregation transformations for Gold layer analytics."""

import logging
from typing import Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, window, sum as spark_sum, count, avg, min as spark_min,
    max as spark_max, stddev, when, lit, dense_rank,
    slice as spark_slice, aggregate
)
from pyspark.sql.types import DoubleType

logger = logging.getLogger(__name__)


def calculate_vwap(
    df: DataFrame,
    window_duration: str = "1 minute",
    slide_duration: Optional[str] = None,
    watermark_delay: str = "30 seconds"
) -> DataFrame:
    """
    Calculate Volume-Weighted Average Price (VWAP) over time windows.

    VWAP = Sum(Price * Volume) / Sum(Volume)

    Args:
        df: Input DataFrame with columns: standard_symbol, exchange, price,
            volume, timestamp
        window_duration: Window duration (e.g., "1 minute", "5 minutes")
        slide_duration: Optional slide interval for sliding windows
        watermark_delay: Watermark delay for late data handling

    Returns:
        DataFrame with VWAP_SCHEMA columns:
        - standard_symbol, exchange, vwap, total_volume, total_value,
          num_trades, window_duration, window_start, window_end,
          min_price, max_price, avg_price, std_dev_price
    """
    logger.info(f"Calculating VWAP with window: {window_duration}")

    # Apply watermark for late data handling
    df = df.withWatermark("timestamp", watermark_delay)

    # Calculate price * volume for VWAP numerator
    df = df.withColumn("price_volume", col("price") * col("volume"))

    # Determine window specification
    if slide_duration:
        window_spec = window(col("timestamp"), window_duration, slide_duration)
    else:
        window_spec = window(col("timestamp"), window_duration)

    # Group by window, symbol, and exchange
    vwap_df = df.groupBy(
        window_spec.alias("window"),
        col("standard_symbol"),
        col("exchange")
    ).agg(
        (spark_sum("price_volume") / spark_sum("volume")).alias("vwap"),
        spark_sum("volume").alias("total_volume"),
        spark_sum("price_volume").alias("total_value"),
        count("*").alias("num_trades"),
        spark_min("price").alias("min_price"),
        spark_max("price").alias("max_price"),
        avg("price").alias("avg_price"),
        stddev("price").alias("std_dev_price")
    )

    # Extract window timestamps and add metadata
    vwap_df = vwap_df.withColumn("window_start", col("window.start"))
    vwap_df = vwap_df.withColumn("window_end", col("window.end"))
    vwap_df = vwap_df.withColumn("window_duration", lit(window_duration))

    # Select final columns matching VWAP_SCHEMA
    vwap_df = vwap_df.select(
        "standard_symbol",
        "exchange",
        "vwap",
        "total_volume",
        "total_value",
        "num_trades",
        "window_duration",
        "window_start",
        "window_end",
        "min_price",
        "max_price",
        "avg_price",
        "std_dev_price"
    )

    logger.info("VWAP calculation complete")
    return vwap_df


def aggregate_volume(
    df: DataFrame,
    window_duration: str = "1 minute",
    slide_duration: Optional[str] = None,
    watermark_delay: str = "30 seconds"
) -> DataFrame:
    """
    Aggregate trading volume across exchanges with market share calculations.

    Args:
        df: Input DataFrame with columns: standard_symbol, exchange, volume,
            side, timestamp
        window_duration: Window duration for aggregation
        slide_duration: Optional slide interval for sliding windows
        watermark_delay: Watermark delay for late data handling

    Returns:
        DataFrame with VOLUME_AGGREGATE_SCHEMA columns:
        - standard_symbol, exchange, total_volume, buy_volume, sell_volume,
          num_trades, window_duration, window_start, window_end,
          volume_rank, exchange_market_share
    """
    logger.info(f"Aggregating volume with window: {window_duration}")

    # Apply watermark
    df = df.withWatermark("timestamp", watermark_delay)

    # Determine window specification
    if slide_duration:
        window_spec = window(col("timestamp"), window_duration, slide_duration)
    else:
        window_spec = window(col("timestamp"), window_duration)

    # Calculate buy and sell volumes
    df = df.withColumn(
        "buy_vol",
        when(col("side") == "buy", col("volume")).otherwise(0.0)
    )
    df = df.withColumn(
        "sell_vol",
        when(col("side") == "sell", col("volume")).otherwise(0.0)
    )

    # Aggregate by window, symbol, and exchange
    vol_df = df.groupBy(
        window_spec.alias("window"),
        col("standard_symbol"),
        col("exchange")
    ).agg(
        spark_sum("volume").alias("total_volume"),
        spark_sum("buy_vol").alias("buy_volume"),
        spark_sum("sell_vol").alias("sell_volume"),
        count("*").alias("num_trades")
    )

    # Calculate total volume per symbol-window for market share
    window_total = Window.partitionBy("window", "standard_symbol")
    vol_df = vol_df.withColumn(
        "symbol_total_volume",
        spark_sum("total_volume").over(window_total)
    )

    # Calculate market share percentage
    vol_df = vol_df.withColumn(
        "exchange_market_share",
        when(col("symbol_total_volume") > 0,
             (col("total_volume") / col("symbol_total_volume")) * 100
        ).otherwise(0.0)
    )

    # Calculate volume rank within each window-symbol group
    rank_window = Window.partitionBy("window", "standard_symbol").orderBy(
        col("total_volume").desc()
    )
    vol_df = vol_df.withColumn("volume_rank", dense_rank().over(rank_window))

    # Extract window timestamps
    vol_df = vol_df.withColumn("window_start", col("window.start"))
    vol_df = vol_df.withColumn("window_end", col("window.end"))
    vol_df = vol_df.withColumn("window_duration", lit(window_duration))

    # Select final columns matching VOLUME_AGGREGATE_SCHEMA
    vol_df = vol_df.select(
        "standard_symbol",
        "exchange",
        "total_volume",
        "buy_volume",
        "sell_volume",
        "num_trades",
        "window_duration",
        "window_start",
        "window_end",
        "volume_rank",
        "exchange_market_share"
    )

    logger.info("Volume aggregation complete")
    return vol_df


def calculate_liquidity_metrics(
    df: DataFrame,
    depth_levels: int = 10,
    watermark_delay: str = "30 seconds"
) -> DataFrame:
    """
    Calculate liquidity metrics from orderbook data.

    Metrics include:
    - Bid/ask spread (absolute and percentage)
    - Order book depth (bid and ask sides)
    - Depth imbalance
    - Liquidity score (custom metric: total_depth / spread_pct)

    Args:
        df: Input DataFrame with orderbook data including:
            standard_symbol, exchange, bids (array), asks (array), timestamp
        depth_levels: Number of price levels to consider for depth calculation
        watermark_delay: Watermark delay for late data handling

    Returns:
        DataFrame with LIQUIDITY_METRIC_SCHEMA columns:
        - standard_symbol, exchange, bid_price, ask_price, spread_absolute,
          spread_percent, bid_depth, ask_depth, total_depth, depth_imbalance,
          liquidity_score, orderbook_levels, timestamp, window_start, window_end
    """
    logger.info(f"Calculating liquidity metrics (depth levels: {depth_levels})")

    # Apply watermark
    df = df.withWatermark("timestamp", watermark_delay)

    # Extract best bid (first element in bids array - highest price)
    # Bids array structure: [{price: X, volume: Y}, ...]
    df = df.withColumn(
        "bid_price",
        col("bids").getItem(0).getField("price")
    )

    # Extract best ask (first element in asks array - lowest price)
    df = df.withColumn(
        "ask_price",
        col("asks").getItem(0).getField("price")
    )

    # Calculate spread
    df = df.withColumn(
        "spread_absolute",
        col("ask_price") - col("bid_price")
    )

    df = df.withColumn(
        "spread_percent",
        when(col("bid_price") > 0,
             (col("spread_absolute") / col("bid_price")) * 100
        ).otherwise(0.0)
    )

    # Calculate bid depth (sum of volumes in top N bid levels)
    df = df.withColumn(
        "bid_depth",
        aggregate(
            spark_slice(col("bids"), 1, depth_levels),
            lit(0.0).cast(DoubleType()),
            lambda acc, x: acc + x.getField("volume")
        )
    )

    # Calculate ask depth
    df = df.withColumn(
        "ask_depth",
        aggregate(
            spark_slice(col("asks"), 1, depth_levels),
            lit(0.0).cast(DoubleType()),
            lambda acc, x: acc + x.getField("volume")
        )
    )

    # Total depth
    df = df.withColumn(
        "total_depth",
        col("bid_depth") + col("ask_depth")
    )

    # Depth imbalance: (bid_depth - ask_depth) / total_depth
    df = df.withColumn(
        "depth_imbalance",
        when(col("total_depth") > 0,
             (col("bid_depth") - col("ask_depth")) / col("total_depth")
        ).otherwise(0.0)
    )

    # Liquidity score: higher depth and lower spread = higher score
    df = df.withColumn(
        "liquidity_score",
        when(col("spread_percent") > 0,
             col("total_depth") / col("spread_percent")
        ).otherwise(0.0)
    )

    # Count orderbook levels
    df = df.withColumn(
        "orderbook_levels",
        (col("bids").size() + col("asks").size()).cast("int")
    )

    # Add window columns for consistency (using timestamp as point-in-time)
    df = df.withColumn("window_start", col("timestamp"))
    df = df.withColumn("window_end", col("timestamp"))

    # Select final columns matching LIQUIDITY_METRIC_SCHEMA
    liquidity_df = df.select(
        "standard_symbol",
        "exchange",
        "bid_price",
        "ask_price",
        "spread_absolute",
        "spread_percent",
        "bid_depth",
        "ask_depth",
        "total_depth",
        "depth_imbalance",
        "liquidity_score",
        "orderbook_levels",
        "timestamp",
        "window_start",
        "window_end"
    )

    logger.info("Liquidity metrics calculation complete")
    return liquidity_df


def calculate_multi_window_vwap(
    df: DataFrame,
    windows: list,
    watermark_delay: str = "30 seconds"
) -> DataFrame:
    """
    Calculate VWAP for multiple time windows simultaneously.

    Args:
        df: Input DataFrame with trade data
        windows: List of window configs [{"duration": "1 minute", "slide": "30 seconds"}, ...]
        watermark_delay: Watermark delay for late data handling

    Returns:
        Union of VWAP DataFrames for all windows
    """
    logger.info(f"Calculating VWAP for {len(windows)} windows")

    result_dfs = []
    for window_config in windows:
        duration = window_config.get("duration", "1 minute")
        slide = window_config.get("slide")

        vwap_df = calculate_vwap(
            df=df,
            window_duration=duration,
            slide_duration=slide,
            watermark_delay=watermark_delay
        )
        result_dfs.append(vwap_df)

    # Union all window results
    if result_dfs:
        result = result_dfs[0]
        for additional_df in result_dfs[1:]:
            result = result.union(additional_df)
        return result

    return df


def calculate_multi_window_volume(
    df: DataFrame,
    windows: list,
    watermark_delay: str = "30 seconds"
) -> DataFrame:
    """
    Calculate volume aggregations for multiple time windows simultaneously.

    Args:
        df: Input DataFrame with trade data
        windows: List of window configs [{"duration": "1 minute", "slide": "30 seconds"}, ...]
        watermark_delay: Watermark delay for late data handling

    Returns:
        Union of volume aggregate DataFrames for all windows
    """
    logger.info(f"Calculating volume aggregations for {len(windows)} windows")

    result_dfs = []
    for window_config in windows:
        duration = window_config.get("duration", "1 minute")
        slide = window_config.get("slide")

        vol_df = aggregate_volume(
            df=df,
            window_duration=duration,
            slide_duration=slide,
            watermark_delay=watermark_delay
        )
        result_dfs.append(vol_df)

    # Union all window results
    if result_dfs:
        result = result_dfs[0]
        for additional_df in result_dfs[1:]:
            result = result.union(additional_df)
        return result

    return df
