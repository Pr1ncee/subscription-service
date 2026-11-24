from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from subscriptions_service.domain.errors import ConflictError, NotFoundError


async def _not_found(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def _conflict(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


def register_error_handlers(app: FastAPI) -> None:
    """Map domain errors to HTTP: `NotFoundError` → 404, `ConflictError` → 409."""
    app.add_exception_handler(NotFoundError, _not_found)
    app.add_exception_handler(ConflictError, _conflict)
