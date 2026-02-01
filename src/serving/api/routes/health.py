"""Health check endpoints."""

from datetime import datetime
from fastapi import APIRouter, Depends
from src.serving.api.schemas.common import HealthResponse
from src.serving.api.dependencies import reader_dependency
from src.serving.data_access.delta_reader import DeltaReader

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Basic health check endpoint.

    Returns:
        HealthResponse with service status
    """
    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(),
        version="1.0.0",
    )


@router.get("/health/ready", response_model=HealthResponse)
async def readiness_check(
    reader: DeltaReader = Depends(reader_dependency),
) -> HealthResponse:
    """
    Readiness probe - checks Delta Lake connectivity.

    Returns:
        HealthResponse with component status
    """
    try:
        components = reader.health_check()
        all_healthy = any(components.values())  # At least one table should exist

        return HealthResponse(
            status="ready" if all_healthy else "degraded",
            timestamp=datetime.now(),
            version="1.0.0",
            components=components,
        )
    except Exception as e:
        return HealthResponse(
            status="unhealthy",
            timestamp=datetime.now(),
            version="1.0.0",
            components={"error": str(e)},
        )


@router.get("/health/live", response_model=HealthResponse)
async def liveness_check() -> HealthResponse:
    """
    Liveness probe - basic service check.

    Returns:
        HealthResponse indicating service is alive
    """
    return HealthResponse(
        status="alive",
        timestamp=datetime.now(),
        version="1.0.0",
    )
