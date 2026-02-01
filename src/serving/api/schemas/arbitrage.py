"""Arbitrage endpoint response schemas."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class ArbitrageResponse(BaseModel):
    """Single arbitrage opportunity."""

    trading_pair: str = Field(..., description="Trading pair")
    buy_exchange: str = Field(..., description="Exchange to buy from")
    buy_price: float = Field(..., description="Buy price")
    sell_exchange: str = Field(..., description="Exchange to sell on")
    sell_price: float = Field(..., description="Sell price")
    spread_percent: float = Field(..., description="Spread percentage")
    spread_absolute: float = Field(..., description="Absolute spread")
    estimated_profit_percent: float = Field(..., description="Estimated profit before fees")
    buy_fee_percent: Optional[float] = Field(None, description="Buy fee percentage")
    sell_fee_percent: Optional[float] = Field(None, description="Sell fee percentage")
    withdrawal_fee: Optional[float] = Field(None, description="Withdrawal fee")
    net_profit_percent: Optional[float] = Field(None, description="Net profit after fees")
    min_volume: Optional[float] = Field(None, description="Minimum volume")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    detection_timestamp: datetime = Field(..., description="Detection timestamp")
    recommended_action: Optional[str] = Field(
        None, description="Recommended action (execute/monitor/ignore)"
    )

    class Config:
        from_attributes = True


class ArbitrageListResponse(BaseModel):
    """Response containing list of arbitrage opportunities."""

    data: List[ArbitrageResponse]
    count: int
    timestamp: datetime = Field(default_factory=datetime.now)


class ActiveArbitrageResponse(BaseModel):
    """Active arbitrage opportunities response."""

    opportunities: List[ArbitrageResponse]
    count: int
    min_profit_threshold: float
    max_age_seconds: int
    timestamp: datetime = Field(default_factory=datetime.now)


class ArbitrageHistoryResponse(BaseModel):
    """Historical arbitrage data response."""

    trading_pair: Optional[str] = None
    start: datetime
    end: datetime
    data: List[ArbitrageResponse]
    count: int
    total_opportunities: int


class ArbitrageSummary(BaseModel):
    """Summary of arbitrage activity."""

    total_opportunities: int
    avg_profit_percent: float
    max_profit_percent: float
    most_common_buy_exchange: Optional[str] = None
    most_common_sell_exchange: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)
