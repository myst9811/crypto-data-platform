"""Liquidity endpoints."""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from src.serving.api.validators import SYMBOL_PATTERN

from src.serving.api.dependencies import reader_dependency
from src.serving.api.schemas.liquidity import (
    LiquidityResponse,
    LiquidityListResponse,
    LiquidityRankingResponse,
)

router = APIRouter()


@router.get("", response_model=LiquidityListResponse)
async def get_liquidity(
    symbol: Optional[str] = Query(None, description="Filter by trading symbol", pattern=SYMBOL_PATTERN),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> LiquidityListResponse:
    """

import logging

logger = logging.getLogger(__name__)
    Get liquidity metrics.

    Returns bid/ask spreads, depth, and liquidity scores from the Gold layer.
    """
    try:
        liquidity_data = reader.get_liquidity_metrics(
            symbol=symbol,
            exchange=exchange,
            limit=limit,
        )

        return LiquidityListResponse(
            data=[
                LiquidityResponse(
                    symbol=lq.standard_symbol,
                    exchange=lq.exchange,
                    bid_price=lq.bid_price,
                    ask_price=lq.ask_price,
                    spread_absolute=lq.spread_absolute,
                    spread_percent=lq.spread_percent,
                    bid_depth=lq.bid_depth,
                    ask_depth=lq.ask_depth,
                    total_depth=lq.total_depth,
                    depth_imbalance=lq.depth_imbalance,
                    liquidity_score=lq.liquidity_score,
                    orderbook_levels=lq.orderbook_levels,
                    timestamp=lq.timestamp,
                )
                for lq in liquidity_data
            ],
            count=len(liquidity_data),
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/rankings", response_model=LiquidityRankingResponse)
async def get_liquidity_rankings(
    symbol: str = Query(..., description="Trading symbol", pattern=SYMBOL_PATTERN),
    reader=Depends(reader_dependency),
) -> LiquidityRankingResponse:
    """
    Get exchange rankings by liquidity score.

    Returns exchanges ranked by liquidity (higher score = better liquidity).
    """
    try:
        rankings = reader.get_liquidity_rankings(symbol=symbol)

        if not rankings:
            raise HTTPException(
                status_code=404, detail=f"No liquidity rankings found for {symbol}"
            )

        best_exchange = rankings[0].exchange if rankings else None

        return LiquidityRankingResponse(
            symbol=symbol,
            rankings=[
                LiquidityResponse(
                    symbol=lq.standard_symbol,
                    exchange=lq.exchange,
                    bid_price=lq.bid_price,
                    ask_price=lq.ask_price,
                    spread_absolute=lq.spread_absolute,
                    spread_percent=lq.spread_percent,
                    bid_depth=lq.bid_depth,
                    ask_depth=lq.ask_depth,
                    total_depth=lq.total_depth,
                    depth_imbalance=lq.depth_imbalance,
                    liquidity_score=lq.liquidity_score,
                    orderbook_levels=lq.orderbook_levels,
                    timestamp=lq.timestamp,
                )
                for lq in rankings
            ],
            best_exchange=best_exchange,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{symbol}", response_model=LiquidityListResponse)
async def get_symbol_liquidity(
    symbol: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> LiquidityListResponse:
    """Get liquidity metrics for a specific symbol."""
    try:
        liquidity_data = reader.get_liquidity_metrics(
            symbol=symbol,
            exchange=exchange,
            limit=limit,
        )

        if not liquidity_data:
            raise HTTPException(
                status_code=404, detail=f"No liquidity data found for {symbol}"
            )

        return LiquidityListResponse(
            data=[
                LiquidityResponse(
                    symbol=lq.standard_symbol,
                    exchange=lq.exchange,
                    bid_price=lq.bid_price,
                    ask_price=lq.ask_price,
                    spread_absolute=lq.spread_absolute,
                    spread_percent=lq.spread_percent,
                    bid_depth=lq.bid_depth,
                    ask_depth=lq.ask_depth,
                    total_depth=lq.total_depth,
                    depth_imbalance=lq.depth_imbalance,
                    liquidity_score=lq.liquidity_score,
                    orderbook_levels=lq.orderbook_levels,
                    timestamp=lq.timestamp,
                )
                for lq in liquidity_data
            ],
            count=len(liquidity_data),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")
