"""API route modules."""

from .health import router as health_router
from .prices import router as prices_router
from .vwap import router as vwap_router
from .volume import router as volume_router
from .liquidity import router as liquidity_router
from .arbitrage import router as arbitrage_router

__all__ = [
    "health_router",
    "prices_router",
    "vwap_router",
    "volume_router",
    "liquidity_router",
    "arbitrage_router",
]
