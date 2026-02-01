"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.serving.config import ServingConfig
from src.serving.api.routes import health_router
from src.serving.api.dependencies import shutdown

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown."""
    logger.info("Starting Crypto Data Platform API")
    yield
    logger.info("Shutting down Crypto Data Platform API")
    shutdown()


# Create FastAPI application
app = FastAPI(
    title="Crypto Data Platform API",
    description="""
    REST API for cryptocurrency market analytics.

    ## Features
    - Real-time and historical price data
    - VWAP (Volume Weighted Average Price) metrics
    - Volume aggregations and market share analysis
    - Liquidity metrics and orderbook depth
    - Arbitrage opportunity detection

    ## Data Sources
    - Binance
    - Coinbase
    - Kraken

    ## Trading Pairs
    - BTC/USD, ETH/USD, BNB/USD, SOL/USD, XRP/USD
    """,
    version="1.0.0",
    docs_url=f"{ServingConfig.API_PREFIX}/docs",
    redoc_url=f"{ServingConfig.API_PREFIX}/redoc",
    openapi_url=f"{ServingConfig.API_PREFIX}/openapi.json",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health_router, prefix=ServingConfig.API_PREFIX)

# Import and include additional routers (will be added in next phase)
# from src.serving.api.routes import prices_router, vwap_router, volume_router, liquidity_router, arbitrage_router
# app.include_router(prices_router, prefix=f"{ServingConfig.API_PREFIX}/prices", tags=["Prices"])
# app.include_router(vwap_router, prefix=f"{ServingConfig.API_PREFIX}/vwap", tags=["VWAP"])
# app.include_router(volume_router, prefix=f"{ServingConfig.API_PREFIX}/volume", tags=["Volume"])
# app.include_router(liquidity_router, prefix=f"{ServingConfig.API_PREFIX}/liquidity", tags=["Liquidity"])
# app.include_router(arbitrage_router, prefix=f"{ServingConfig.API_PREFIX}/arbitrage", tags=["Arbitrage"])


@app.get("/")
async def root():
    """Root endpoint - redirects to API documentation."""
    return {
        "message": "Crypto Data Platform API",
        "docs": f"{ServingConfig.API_PREFIX}/docs",
        "health": f"{ServingConfig.API_PREFIX}/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.serving.api.main:app",
        host=ServingConfig.API_HOST,
        port=ServingConfig.API_PORT,
        reload=True,
    )
