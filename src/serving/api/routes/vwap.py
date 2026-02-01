"""VWAP endpoints."""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.dependencies import reader_dependency
from src.serving.api.schemas.vwap import (
    VWAPResponse,
    VWAPListResponse,
    VWAPHistoryResponse,
)
from src.serving.api.schemas.common import WindowDurationListResponse
from src.serving.data_access.delta_reader import DeltaReader

router = APIRouter()


@router.get("", response_model=VWAPListResponse)
async def get_vwap(
    symbol: Optional[str] = Query(None, description="Filter by trading symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    window: Optional[str] = Query(
        None, description="Window duration (1min, 5min, 15min, 1h)"
    ),
    limit: int = Query(100, ge=1, le=1000),
    reader: DeltaReader = Depends(reader_dependency),
) -> VWAPListResponse:
    """
    Get VWAP metrics.

    Returns Volume Weighted Average Price data from the Gold layer.
    """
    try:
        vwap_data = reader.get_vwap(
            symbol=symbol,
            exchange=exchange,
            window_duration=window,
            limit=limit,
        )

        return VWAPListResponse(
            data=[
                VWAPResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    vwap=v.vwap,
                    total_volume=v.total_volume,
                    total_value=v.total_value,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    min_price=v.min_price,
                    max_price=v.max_price,
                    avg_price=v.avg_price,
                    std_dev_price=v.std_dev_price,
                )
                for v in vwap_data
            ],
            count=len(vwap_data),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/windows", response_model=WindowDurationListResponse)
async def get_windows(
    reader: DeltaReader = Depends(reader_dependency),
) -> WindowDurationListResponse:
    """Get list of available window durations."""
    return WindowDurationListResponse(windows=reader.get_available_windows())


@router.get("/{symbol}", response_model=VWAPListResponse)
async def get_symbol_vwap(
    symbol: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    window: Optional[str] = Query(
        None, description="Window duration (1min, 5min, 15min, 1h)"
    ),
    limit: int = Query(100, ge=1, le=1000),
    reader: DeltaReader = Depends(reader_dependency),
) -> VWAPListResponse:
    """Get VWAP metrics for a specific symbol."""
    try:
        vwap_data = reader.get_vwap(
            symbol=symbol,
            exchange=exchange,
            window_duration=window,
            limit=limit,
        )

        if not vwap_data:
            raise HTTPException(status_code=404, detail=f"No VWAP data found for {symbol}")

        return VWAPListResponse(
            data=[
                VWAPResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    vwap=v.vwap,
                    total_volume=v.total_volume,
                    total_value=v.total_value,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    min_price=v.min_price,
                    max_price=v.max_price,
                    avg_price=v.avg_price,
                    std_dev_price=v.std_dev_price,
                )
                for v in vwap_data
            ],
            count=len(vwap_data),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}/history", response_model=VWAPHistoryResponse)
async def get_vwap_history(
    symbol: str,
    start: datetime = Query(..., description="Start datetime"),
    end: datetime = Query(..., description="End datetime"),
    window: str = Query("1min", description="Window duration"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    reader: DeltaReader = Depends(reader_dependency),
) -> VWAPHistoryResponse:
    """Get historical VWAP data for a symbol."""
    try:
        vwap_data = reader.get_vwap_history(
            symbol=symbol,
            start=start,
            end=end,
            window_duration=window,
            exchange=exchange,
        )

        return VWAPHistoryResponse(
            symbol=symbol,
            exchange=exchange,
            window_duration=window,
            start=start,
            end=end,
            data=[
                VWAPResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    vwap=v.vwap,
                    total_volume=v.total_volume,
                    total_value=v.total_value,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    min_price=v.min_price,
                    max_price=v.max_price,
                    avg_price=v.avg_price,
                    std_dev_price=v.std_dev_price,
                )
                for v in vwap_data
            ],
            count=len(vwap_data),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
