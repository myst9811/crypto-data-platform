"""Volume endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.dependencies import reader_dependency
from src.serving.api.schemas.volume import (
    VolumeResponse,
    VolumeListResponse,
    VolumeRankingResponse,
    MarketShareResponse,
    MarketShareListResponse,
)

router = APIRouter()


@router.get("", response_model=VolumeListResponse)
async def get_volume(
    symbol: Optional[str] = Query(None, description="Filter by trading symbol"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    window: Optional[str] = Query(None, description="Window duration"),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> VolumeListResponse:
    """
    Get volume aggregates.

    Returns volume data from the Gold layer.
    """
    try:
        volume_data = reader.get_volume_aggregates(
            symbol=symbol,
            exchange=exchange,
            window_duration=window,
            limit=limit,
        )

        return VolumeListResponse(
            data=[
                VolumeResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    total_volume=v.total_volume,
                    buy_volume=v.buy_volume,
                    sell_volume=v.sell_volume,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    volume_rank=v.volume_rank,
                    exchange_market_share=v.exchange_market_share,
                )
                for v in volume_data
            ],
            count=len(volume_data),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rankings", response_model=VolumeRankingResponse)
async def get_volume_rankings(
    symbol: str = Query(..., description="Trading symbol"),
    window: str = Query("1min", description="Window duration"),
    reader=Depends(reader_dependency),
) -> VolumeRankingResponse:
    """
    Get exchange rankings by volume for a symbol.

    Returns exchanges ranked by trading volume.
    """
    try:
        rankings = reader.get_volume_rankings(symbol=symbol, window_duration=window)

        if not rankings:
            raise HTTPException(
                status_code=404, detail=f"No volume rankings found for {symbol}"
            )

        total_volume = sum(r.total_volume for r in rankings)

        return VolumeRankingResponse(
            symbol=symbol,
            window_duration=window,
            rankings=[
                VolumeResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    total_volume=v.total_volume,
                    buy_volume=v.buy_volume,
                    sell_volume=v.sell_volume,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    volume_rank=v.volume_rank,
                    exchange_market_share=v.exchange_market_share,
                )
                for v in rankings
            ],
            total_volume=total_volume,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market-share", response_model=MarketShareListResponse)
async def get_market_share(
    symbol: str = Query(..., description="Trading symbol"),
    window: str = Query("1min", description="Window duration"),
    reader=Depends(reader_dependency),
) -> MarketShareListResponse:
    """
    Get market share by exchange for a symbol.

    Returns percentage of total volume per exchange.
    """
    try:
        rankings = reader.get_volume_rankings(symbol=symbol, window_duration=window)

        if not rankings:
            raise HTTPException(
                status_code=404, detail=f"No market share data found for {symbol}"
            )

        total_volume = sum(r.total_volume for r in rankings)

        market_share_data = [
            MarketShareResponse(
                exchange=r.exchange,
                market_share=(
                    (r.total_volume / total_volume * 100) if total_volume > 0 else 0
                ),
                volume=r.total_volume,
            )
            for r in rankings
            if r.exchange
        ]

        return MarketShareListResponse(
            symbol=symbol,
            window_duration=window,
            data=market_share_data,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{symbol}", response_model=VolumeListResponse)
async def get_symbol_volume(
    symbol: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    window: Optional[str] = Query(None, description="Window duration"),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> VolumeListResponse:
    """Get volume aggregates for a specific symbol."""
    try:
        volume_data = reader.get_volume_aggregates(
            symbol=symbol,
            exchange=exchange,
            window_duration=window,
            limit=limit,
        )

        if not volume_data:
            raise HTTPException(
                status_code=404, detail=f"No volume data found for {symbol}"
            )

        return VolumeListResponse(
            data=[
                VolumeResponse(
                    symbol=v.standard_symbol,
                    exchange=v.exchange,
                    total_volume=v.total_volume,
                    buy_volume=v.buy_volume,
                    sell_volume=v.sell_volume,
                    num_trades=v.num_trades,
                    window_duration=v.window_duration,
                    window_start=v.window_start,
                    window_end=v.window_end,
                    volume_rank=v.volume_rank,
                    exchange_market_share=v.exchange_market_share,
                )
                for v in volume_data
            ],
            count=len(volume_data),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
