"""Pydantic models for data access layer - mirrors Spark schemas."""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class PriceData(BaseModel):
    """Normalized price data from Silver layer."""

    standard_symbol: str = Field(..., description="Standardized symbol (e.g., BTC/USD)")
    exchange: str = Field(..., description="Exchange name")
    price: float = Field(..., description="Trade price")
    volume: float = Field(..., description="Trade volume")
    side: Optional[str] = Field(None, description="Trade side (buy/sell)")
    timestamp: datetime = Field(..., description="Trade timestamp")
    processing_timestamp: datetime = Field(..., description="Processing timestamp")
    data_quality_score: Optional[float] = Field(None, ge=0, le=1)
    is_outlier: Optional[bool] = Field(None)
    original_symbol: Optional[str] = Field(None)
    price_usd: Optional[float] = Field(None)
    base_currency: Optional[str] = Field(None)
    quote_currency: Optional[str] = Field(None)

    class Config:
        from_attributes = True


class VWAPData(BaseModel):
    """VWAP metrics from Gold layer."""

    standard_symbol: str = Field(..., description="Standardized symbol")
    exchange: str = Field(..., description="Exchange name")
    vwap: float = Field(..., description="Volume Weighted Average Price")
    total_volume: float = Field(..., description="Total trading volume")
    total_value: float = Field(..., description="Total value (price * volume)")
    num_trades: int = Field(..., description="Number of trades")
    window_duration: str = Field(..., description="Window duration (1min, 5min, etc.)")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    min_price: Optional[float] = Field(None)
    max_price: Optional[float] = Field(None)
    avg_price: Optional[float] = Field(None)
    std_dev_price: Optional[float] = Field(None)

    class Config:
        from_attributes = True


class VolumeData(BaseModel):
    """Volume aggregates from Gold layer."""

    standard_symbol: str = Field(..., description="Standardized symbol")
    exchange: Optional[str] = Field(None, description="Exchange name (null for cross-exchange)")
    total_volume: float = Field(..., description="Total volume")
    buy_volume: Optional[float] = Field(None)
    sell_volume: Optional[float] = Field(None)
    num_trades: int = Field(..., description="Number of trades")
    window_duration: str = Field(..., description="Window duration")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    volume_rank: Optional[int] = Field(None, description="Rank by volume")
    exchange_market_share: Optional[float] = Field(None, description="Market share percentage")

    class Config:
        from_attributes = True


class LiquidityData(BaseModel):
    """Liquidity metrics from Gold layer."""

    standard_symbol: str = Field(..., description="Standardized symbol")
    exchange: str = Field(..., description="Exchange name")
    bid_price: float = Field(..., description="Best bid price")
    ask_price: float = Field(..., description="Best ask price")
    spread_absolute: float = Field(..., description="Absolute spread")
    spread_percent: float = Field(..., description="Spread as percentage")
    bid_depth: Optional[float] = Field(None, description="Total bid depth")
    ask_depth: Optional[float] = Field(None, description="Total ask depth")
    total_depth: Optional[float] = Field(None, description="Total orderbook depth")
    depth_imbalance: Optional[float] = Field(None, description="Depth imbalance ratio")
    liquidity_score: Optional[float] = Field(None, description="Liquidity score")
    orderbook_levels: Optional[int] = Field(None, description="Number of price levels")
    timestamp: datetime = Field(..., description="Measurement timestamp")
    window_start: Optional[datetime] = Field(None)
    window_end: Optional[datetime] = Field(None)

    class Config:
        from_attributes = True


class ArbitrageData(BaseModel):
    """Arbitrage opportunity from Gold layer."""

    trading_pair: str = Field(..., description="Trading pair (e.g., BTC/USD)")
    buy_exchange: str = Field(..., description="Exchange to buy from")
    buy_price: float = Field(..., description="Buy price")
    sell_exchange: str = Field(..., description="Exchange to sell on")
    sell_price: float = Field(..., description="Sell price")
    spread_percent: float = Field(..., description="Spread percentage")
    spread_absolute: float = Field(..., description="Absolute spread")
    estimated_profit_percent: float = Field(..., description="Estimated profit before fees")
    buy_fee_percent: Optional[float] = Field(None)
    sell_fee_percent: Optional[float] = Field(None)
    withdrawal_fee: Optional[float] = Field(None)
    net_profit_percent: Optional[float] = Field(None, description="Net profit after fees")
    min_volume: Optional[float] = Field(None)
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    detection_timestamp: datetime = Field(..., description="When opportunity was detected")
    liquidity_check_passed: Optional[bool] = Field(None)
    recommended_action: Optional[str] = Field(None, description="Recommended action")

    class Config:
        from_attributes = True


class OrderLevel(BaseModel):
    """Single order book level."""

    price: float
    volume: float


class OrderBookData(BaseModel):
    """Order book snapshot."""

    symbol: str
    exchange: str
    bids: List[OrderLevel]
    asks: List[OrderLevel]
    timestamp: datetime

    class Config:
        from_attributes = True
