"""Liquidity endpoint response schemas."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class LiquidityResponse(BaseModel):
    """Single liquidity metrics data point."""

    standard_symbol: str = Field(..., alias="symbol", description="Trading symbol")
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

    class Config:
        populate_by_name = True
        from_attributes = True


class LiquidityListResponse(BaseModel):
    """Response containing list of liquidity data."""

    data: List[LiquidityResponse]
    count: int
    timestamp: datetime = Field(default_factory=datetime.now)


class LiquidityRankingResponse(BaseModel):
    """Liquidity rankings by exchange."""

    symbol: str
    rankings: List[LiquidityResponse]
    best_exchange: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class DepthResponse(BaseModel):
    """Order book depth data."""

    symbol: str
    exchange: str
    bid_depth: float
    ask_depth: float
    total_depth: float
    depth_imbalance: float
    levels: int
    timestamp: datetime


class DepthListResponse(BaseModel):
    """Response containing depth data."""

    symbol: str
    data: List[DepthResponse]
    timestamp: datetime = Field(default_factory=datetime.now)
