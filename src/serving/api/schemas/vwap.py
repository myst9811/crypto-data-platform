"""VWAP endpoint response schemas."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class VWAPResponse(BaseModel):
    """Single VWAP data point."""

    standard_symbol: str = Field(..., alias="symbol", description="Trading symbol")
    exchange: str = Field(..., description="Exchange name")
    vwap: float = Field(..., description="Volume Weighted Average Price")
    total_volume: float = Field(..., description="Total trading volume")
    total_value: float = Field(..., description="Total value traded")
    num_trades: int = Field(..., description="Number of trades")
    window_duration: str = Field(..., description="Window duration")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    min_price: Optional[float] = Field(None, description="Minimum price in window")
    max_price: Optional[float] = Field(None, description="Maximum price in window")
    avg_price: Optional[float] = Field(None, description="Average price")
    std_dev_price: Optional[float] = Field(None, description="Price standard deviation")

    class Config:
        populate_by_name = True
        from_attributes = True


class VWAPListResponse(BaseModel):
    """Response containing list of VWAP data."""

    data: List[VWAPResponse]
    count: int
    timestamp: datetime = Field(default_factory=datetime.now)


class VWAPHistoryResponse(BaseModel):
    """Historical VWAP data response."""

    symbol: str
    exchange: Optional[str] = None
    window_duration: str
    start: datetime
    end: datetime
    data: List[VWAPResponse]
    count: int
