import logging

from fastapi import APIRouter, Response, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from subscriptions_service.api.deps import SessionFactoryDep

router = APIRouter(prefix="/health", tags=["health"])
logger = logging.getLogger(__name__)


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(session_factory: SessionFactoryDep, response: Response) -> dict[str, str]:
    """Ready when the database answers. RabbitMQ is deliberately not checked."""
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError) as e:
        logger.warning("readiness check failed", extra={"error": str(e)})
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable"}
    return {"status": "ok"}
