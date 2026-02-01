"""Common API response schemas."""

from datetime import datetime
from typing import Any, Dict, Generic, List, Optional, TypeVar
from pydantic import BaseModel, Field

T = TypeVar("T")


class MetaInfo(BaseModel):
    """Metadata for API responses."""

    count: int = Field(..., description="Number of items returned")
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Response timestamp"
    )
    cached: bool = Field(default=False, description="Whether data was from cache")
    cache_age_seconds: Optional[float] = Field(
        None, description="Age of cached data in seconds"
    )


class PaginationMeta(MetaInfo):
    """Pagination metadata."""

    page: int = Field(1, ge=1)
    page_size: int = Field(100, ge=1, le=1000)
    total_pages: int = Field(1, ge=1)
    total_count: int = Field(0, ge=0)


class ErrorDetail(BaseModel):
    """Error detail schema."""

    code: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")


class APIResponse(BaseModel, Generic[T]):
    """Generic API response wrapper."""

    success: bool = Field(True, description="Whether the request was successful")
    data: T = Field(..., description="Response data")
    meta: Optional[MetaInfo] = Field(None, description="Response metadata")
    error: Optional[ErrorDetail] = Field(None, description="Error details if failed")


class ErrorResponse(BaseModel):
    """Error response schema."""

    success: bool = Field(False)
    data: None = None
    error: ErrorDetail


class PaginatedResponse(APIResponse[List[T]], Generic[T]):
    """Paginated API response."""

    meta: PaginationMeta


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(default_factory=datetime.now)
    version: str = Field("1.0.0", description="API version")
    components: Optional[Dict[str, bool]] = Field(
        None, description="Component health status"
    )


class SymbolListResponse(BaseModel):
    """Response containing list of symbols."""

    symbols: List[str] = Field(..., description="Available trading symbols")


class ExchangeListResponse(BaseModel):
    """Response containing list of exchanges."""

    exchanges: List[str] = Field(..., description="Available exchanges")


class WindowDurationListResponse(BaseModel):
    """Response containing list of window durations."""

    windows: List[str] = Field(..., description="Available window durations")
