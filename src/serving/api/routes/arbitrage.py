"""Arbitrage endpoints."""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from src.serving.api.dependencies import reader_dependency
from src.serving.api.schemas.arbitrage import (
    ArbitrageResponse,
    ArbitrageListResponse,
    ActiveArbitrageResponse,
    ArbitrageHistoryResponse,
)
from src.serving.config import ServingConfig

router = APIRouter()


@router.get("", response_model=ArbitrageListResponse)
async def get_arbitrage(
    symbol: Optional[str] = Query(None, description="Filter by trading pair"),
    min_profit: Optional[float] = Query(
        None, ge=0, description="Minimum net profit percentage"
    ),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> ArbitrageListResponse:
    """

import logging

logger = logging.getLogger(__name__)
    Get arbitrage opportunities.

    Returns detected cross-exchange arbitrage opportunities from the Gold layer.
    """
    try:
        opportunities = reader.get_arbitrage_opportunities(
            symbol=symbol,
            min_profit=min_profit,
            limit=limit,
        )

        return ArbitrageListResponse(
            data=[
                ArbitrageResponse(
                    trading_pair=arb.trading_pair,
                    buy_exchange=arb.buy_exchange,
                    buy_price=arb.buy_price,
                    sell_exchange=arb.sell_exchange,
                    sell_price=arb.sell_price,
                    spread_percent=arb.spread_percent,
                    spread_absolute=arb.spread_absolute,
                    estimated_profit_percent=arb.estimated_profit_percent,
                    buy_fee_percent=arb.buy_fee_percent,
                    sell_fee_percent=arb.sell_fee_percent,
                    withdrawal_fee=arb.withdrawal_fee,
                    net_profit_percent=arb.net_profit_percent,
                    min_volume=arb.min_volume,
                    window_start=arb.window_start,
                    window_end=arb.window_end,
                    detection_timestamp=arb.detection_timestamp,
                    recommended_action=arb.recommended_action,
                )
                for arb in opportunities
            ],
            count=len(opportunities),
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/active", response_model=ActiveArbitrageResponse)
async def get_active_arbitrage(
    min_profit: float = Query(
        ServingConfig.ARBITRAGE_MIN_PROFIT,
        ge=0,
        description="Minimum net profit percentage",
    ),
    max_age_seconds: int = Query(
        60, ge=1, le=300, description="Maximum age in seconds"
    ),
    reader=Depends(reader_dependency),
) -> ActiveArbitrageResponse:
    """
    Get currently viable arbitrage opportunities.

    Returns opportunities that are recent enough to potentially be actionable.
    """
    try:
        opportunities = reader.get_active_arbitrage(
            min_profit=min_profit,
            max_age_seconds=max_age_seconds,
        )

        return ActiveArbitrageResponse(
            opportunities=[
                ArbitrageResponse(
                    trading_pair=arb.trading_pair,
                    buy_exchange=arb.buy_exchange,
                    buy_price=arb.buy_price,
                    sell_exchange=arb.sell_exchange,
                    sell_price=arb.sell_price,
                    spread_percent=arb.spread_percent,
                    spread_absolute=arb.spread_absolute,
                    estimated_profit_percent=arb.estimated_profit_percent,
                    buy_fee_percent=arb.buy_fee_percent,
                    sell_fee_percent=arb.sell_fee_percent,
                    withdrawal_fee=arb.withdrawal_fee,
                    net_profit_percent=arb.net_profit_percent,
                    min_volume=arb.min_volume,
                    window_start=arb.window_start,
                    window_end=arb.window_end,
                    detection_timestamp=arb.detection_timestamp,
                    recommended_action=arb.recommended_action,
                )
                for arb in opportunities
            ],
            count=len(opportunities),
            min_profit_threshold=min_profit,
            max_age_seconds=max_age_seconds,
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/history", response_model=ArbitrageHistoryResponse)
async def get_arbitrage_history(
    start: datetime = Query(..., description="Start datetime"),
    end: datetime = Query(..., description="End datetime"),
    symbol: Optional[str] = Query(None, description="Filter by trading pair"),
    limit: int = Query(1000, ge=1, le=10000),
    reader=Depends(reader_dependency),
) -> ArbitrageHistoryResponse:
    """Get historical arbitrage opportunities."""
    try:
        opportunities = reader.get_arbitrage_history(
            start=start,
            end=end,
            symbol=symbol,
            limit=limit,
        )

        return ArbitrageHistoryResponse(
            trading_pair=symbol,
            start=start,
            end=end,
            data=[
                ArbitrageResponse(
                    trading_pair=arb.trading_pair,
                    buy_exchange=arb.buy_exchange,
                    buy_price=arb.buy_price,
                    sell_exchange=arb.sell_exchange,
                    sell_price=arb.sell_price,
                    spread_percent=arb.spread_percent,
                    spread_absolute=arb.spread_absolute,
                    estimated_profit_percent=arb.estimated_profit_percent,
                    buy_fee_percent=arb.buy_fee_percent,
                    sell_fee_percent=arb.sell_fee_percent,
                    withdrawal_fee=arb.withdrawal_fee,
                    net_profit_percent=arb.net_profit_percent,
                    min_volume=arb.min_volume,
                    window_start=arb.window_start,
                    window_end=arb.window_end,
                    detection_timestamp=arb.detection_timestamp,
                    recommended_action=arb.recommended_action,
                )
                for arb in opportunities
            ],
            count=len(opportunities),
            total_opportunities=len(opportunities),
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{symbol}", response_model=ArbitrageListResponse)
async def get_symbol_arbitrage(
    symbol: str,
    min_profit: Optional[float] = Query(None, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> ArbitrageListResponse:
    """Get arbitrage opportunities for a specific trading pair."""
    try:
        opportunities = reader.get_arbitrage_opportunities(
            symbol=symbol,
            min_profit=min_profit,
            limit=limit,
        )

        if not opportunities:
            raise HTTPException(
                status_code=404,
                detail=f"No arbitrage opportunities found for {symbol}",
            )

        return ArbitrageListResponse(
            data=[
                ArbitrageResponse(
                    trading_pair=arb.trading_pair,
                    buy_exchange=arb.buy_exchange,
                    buy_price=arb.buy_price,
                    sell_exchange=arb.sell_exchange,
                    sell_price=arb.sell_price,
                    spread_percent=arb.spread_percent,
                    spread_absolute=arb.spread_absolute,
                    estimated_profit_percent=arb.estimated_profit_percent,
                    buy_fee_percent=arb.buy_fee_percent,
                    sell_fee_percent=arb.sell_fee_percent,
                    withdrawal_fee=arb.withdrawal_fee,
                    net_profit_percent=arb.net_profit_percent,
                    min_volume=arb.min_volume,
                    window_start=arb.window_start,
                    window_end=arb.window_end,
                    detection_timestamp=arb.detection_timestamp,
                    recommended_action=arb.recommended_action,
                )
                for arb in opportunities
            ],
            count=len(opportunities),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")
