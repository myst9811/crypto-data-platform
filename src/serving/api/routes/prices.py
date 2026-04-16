"""Price endpoints."""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from src.serving.api.validators import SYMBOL_PATTERN

from src.serving.api.dependencies import reader_dependency
from src.serving.api.schemas.prices import (
    PriceResponse,
    PriceListResponse,
    PriceComparisonResponse,
    PriceHistoryResponse,
)
from src.serving.api.schemas.common import SymbolListResponse, ExchangeListResponse
from src.serving.config import ServingConfig

router = APIRouter()


@router.get("", response_model=PriceListResponse)
async def get_prices(
    symbol: Optional[str] = Query(None, description="Filter by trading symbol (e.g., BTC/USD)", pattern=SYMBOL_PATTERN),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    reader=Depends(reader_dependency),
) -> PriceListResponse:
    """

import logging

logger = logging.getLogger(__name__)
    Get latest prices.

    Returns normalized price data from the Silver layer.
    """
    try:
        prices = reader.get_latest_prices(symbol=symbol, exchange=exchange, limit=limit)
        return PriceListResponse(
            data=[
                PriceResponse(
                    symbol=p.standard_symbol,
                    exchange=p.exchange,
                    price=p.price,
                    volume=p.volume,
                    side=p.side,
                    timestamp=p.timestamp,
                    data_quality_score=p.data_quality_score,
                )
                for p in prices
            ],
            count=len(prices),
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/symbols", response_model=SymbolListResponse)
async def get_symbols(
    reader=Depends(reader_dependency),
) -> SymbolListResponse:
    """Get list of available trading symbols."""
    return SymbolListResponse(symbols=reader.get_available_symbols())


@router.get("/exchanges", response_model=ExchangeListResponse)
async def get_exchanges(
    reader=Depends(reader_dependency),
) -> ExchangeListResponse:
    """Get list of available exchanges."""
    return ExchangeListResponse(exchanges=reader.get_available_exchanges())


@router.get("/compare", response_model=PriceComparisonResponse)
async def compare_prices(
    symbol: str = Query(..., description="Trading symbol to compare", pattern=SYMBOL_PATTERN),
    reader=Depends(reader_dependency),
) -> PriceComparisonResponse:
    """
    Compare prices across all exchanges for a symbol.

    Returns the latest price from each exchange with spread analysis.
    """
    try:
        prices = reader.get_price_comparison(symbol)

        if not prices:
            raise HTTPException(status_code=404, detail=f"No prices found for {symbol}")

        price_responses = [
            PriceResponse(
                symbol=p.standard_symbol,
                exchange=p.exchange,
                price=p.price,
                volume=p.volume,
                side=p.side,
                timestamp=p.timestamp,
                data_quality_score=p.data_quality_score,
            )
            for p in prices
        ]

        price_values = [p.price for p in prices]
        min_price = min(price_values) if price_values else None
        max_price = max(price_values) if price_values else None
        spread_percent = (
            ((max_price - min_price) / min_price * 100)
            if min_price and max_price and min_price > 0
            else None
        )

        return PriceComparisonResponse(
            symbol=symbol,
            prices=price_responses,
            min_price=min_price,
            max_price=max_price,
            spread_percent=spread_percent,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{symbol}", response_model=PriceListResponse)
async def get_symbol_prices(
    symbol: str,
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, ge=1, le=1000),
    reader=Depends(reader_dependency),
) -> PriceListResponse:
    """Get latest prices for a specific symbol."""
    try:
        prices = reader.get_latest_prices(symbol=symbol, exchange=exchange, limit=limit)

        if not prices:
            raise HTTPException(status_code=404, detail=f"No prices found for {symbol}")

        return PriceListResponse(
            data=[
                PriceResponse(
                    symbol=p.standard_symbol,
                    exchange=p.exchange,
                    price=p.price,
                    volume=p.volume,
                    side=p.side,
                    timestamp=p.timestamp,
                    data_quality_score=p.data_quality_score,
                )
                for p in prices
            ],
            count=len(prices),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{symbol}/history", response_model=PriceHistoryResponse)
async def get_price_history(
    symbol: str,
    start: datetime = Query(..., description="Start datetime"),
    end: datetime = Query(..., description="End datetime"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(1000, ge=1, le=10000),
    reader=Depends(reader_dependency),
) -> PriceHistoryResponse:
    """Get historical prices for a symbol."""
    try:
        prices = reader.get_price_history(
            symbol=symbol,
            start=start,
            end=end,
            exchange=exchange,
            limit=limit,
        )

        return PriceHistoryResponse(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=end,
            data=[
                PriceResponse(
                    symbol=p.standard_symbol,
                    exchange=p.exchange,
                    price=p.price,
                    volume=p.volume,
                    side=p.side,
                    timestamp=p.timestamp,
                    data_quality_score=p.data_quality_score,
                )
                for p in prices
            ],
            count=len(prices),
        )
    except Exception as e:
        logger.exception("internal error"); raise HTTPException(status_code=500, detail="Internal server error")
