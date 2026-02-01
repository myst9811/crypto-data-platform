"""Volume endpoint response schemas."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class VolumeResponse(BaseModel):
    """Single volume aggregate data point."""

    standard_symbol: str = Field(..., alias="symbol", description="Trading symbol")
    exchange: Optional[str] = Field(None, description="Exchange name")
    total_volume: float = Field(..., description="Total volume")
    buy_volume: Optional[float] = Field(None, description="Buy volume")
    sell_volume: Optional[float] = Field(None, description="Sell volume")
    num_trades: int = Field(..., description="Number of trades")
    window_duration: str = Field(..., description="Window duration")
    window_start: datetime = Field(..., description="Window start time")
    window_end: datetime = Field(..., description="Window end time")
    volume_rank: Optional[int] = Field(None, description="Volume rank")
    exchange_market_share: Optional[float] = Field(
        None, description="Market share percentage"
    )

    class Config:
        populate_by_name = True
        from_attributes = True


class VolumeListResponse(BaseModel):
    """Response containing list of volume data."""

    data: List[VolumeResponse]
    count: int
    timestamp: datetime = Field(default_factory=datetime.now)


class VolumeRankingResponse(BaseModel):
    """Exchange volume rankings response."""

    symbol: str
    window_duration: str
    rankings: List[VolumeResponse]
    total_volume: float
    timestamp: datetime = Field(default_factory=datetime.now)


class MarketShareResponse(BaseModel):
    """Market share by exchange."""

    exchange: str
    market_share: float = Field(..., ge=0, le=100)
    volume: float


class MarketShareListResponse(BaseModel):
    """Response containing market share data."""

    symbol: str
    window_duration: str
    data: List[MarketShareResponse]
    timestamp: datetime = Field(default_factory=datetime.now)
