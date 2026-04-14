"""Health check endpoints."""

from datetime import datetime
from fastapi import APIRouter, Depends
from src.serving.api.schemas.common import HealthResponse
from src.serving.api.dependencies import reader_dependency, get_backend_info

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


@router.get("/health/ready")
async def readiness_check(
    reader=Depends(reader_dependency),
) -> dict:
    """
    Readiness probe - checks Delta Lake connectivity.

    Returns:
        Dict with component status
    """
    try:
        components = reader.health_check()
        backend = get_backend_info()
        all_healthy = any(
            v for k, v in components.items()
            if isinstance(v, bool) and v
        )

        return {
            "status": "ready" if all_healthy else "degraded",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "components": components,
            "backend": backend["active_backend"],
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "components": {},
            "error": str(e),
        }


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


@router.get("/health/backend")
async def backend_info() -> dict:
    """
    Get information about the data backend.

    Returns:
        Dict with backend availability info
    """
    return get_backend_info()
