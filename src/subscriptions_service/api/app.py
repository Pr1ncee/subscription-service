from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.body_limit import RequestBodyLimitMiddleware

from subscriptions_service.api.errors import register_error_handlers
from subscriptions_service.api.routers import health, payments, subscriptions
from subscriptions_service.config import Settings
from subscriptions_service.db.engine import create_engine, create_session_factory
from subscriptions_service.logs import configure_logging

MAX_BODY_SIZE = 64 * 1024


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    if app.state.settings is None:
        app.state.settings = Settings()
    settings: Settings = app.state.settings
    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    app.state.session_factory = create_session_factory(engine)
    try:
        yield
    finally:
        await engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the API. Without explicit settings they are read from the env at startup."""
    app = FastAPI(title="Subscriptions service", lifespan=lifespan)
    app.state.settings = settings
    # FastAPI 0.162 does not forward `max_body_size` to Starlette, so add the middleware directly.
    app.add_middleware(RequestBodyLimitMiddleware, max_body_size=MAX_BODY_SIZE)
    register_error_handlers(app)
    app.include_router(subscriptions.router)
    app.include_router(payments.router)
    app.include_router(health.router)
    return app


app = create_app()
