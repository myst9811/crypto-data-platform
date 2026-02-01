"""Price endpoint response schemas."""

from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field


class PriceResponse(BaseModel):
    """Single price data point."""

    standard_symbol: str = Field(..., alias="symbol", description="Trading symbol")
    exchange: str = Field(..., description="Exchange name")
    price: float = Field(..., description="Trade price")
    volume: float = Field(..., description="Trade volume")
    side: Optional[str] = Field(None, description="Trade side (buy/sell)")
    timestamp: datetime = Field(..., description="Trade timestamp")
    data_quality_score: Optional[float] = Field(None, ge=0, le=1)

    class Config:
        populate_by_name = True
        from_attributes = True


class PriceListResponse(BaseModel):
    """Response containing list of prices."""

    data: List[PriceResponse]
    count: int
    timestamp: datetime = Field(default_factory=datetime.now)


class PriceComparisonResponse(BaseModel):
    """Price comparison across exchanges."""

    symbol: str
    prices: List[PriceResponse]
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    spread_percent: Optional[float] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class PriceHistoryResponse(BaseModel):
    """Historical price data response."""

    symbol: str
    exchange: Optional[str] = None
    start: datetime
    end: datetime
    data: List[PriceResponse]
    count: int
