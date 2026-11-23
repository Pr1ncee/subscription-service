from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime

import httpx
import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer

from subscriptions_service.api.app import create_app
from subscriptions_service.api.deps import get_clock, get_session_factory
from subscriptions_service.config import Settings
from subscriptions_service.db.engine import create_session_factory
from subscriptions_service.repositories.uow import UnitOfWork, UnitOfWorkFactory
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.fakes import FakeClock
from tests.support import alembic_config

TABLES = "subscriptions, notifications, payments, outbox, processed_events"


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        url = pg.get_connection_url()
        command.upgrade(alembic_config(url), "head")
        yield url


@pytest.fixture
async def session_factory(pg_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(pg_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} CASCADE"))
    yield create_session_factory(engine)
    await engine.dispose()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(datetime(2026, 1, 1, 12, tzinfo=UTC))


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    return lambda: UnitOfWork(session_factory)


@pytest.fixture
def subs(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> SubscriptionService:
    return SubscriptionService(uow_factory, clock, [3, 1, 0])


UNREACHABLE_BROKER = "amqp://guest:guest@127.0.0.1:1/"


@pytest.fixture
async def api_client(
    pg_url: str, session_factory: async_sessionmaker[AsyncSession], clock: FakeClock
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(Settings(database_url=pg_url, rabbitmq_url=UNREACHABLE_BROKER))
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.dependency_overrides[get_clock] = lambda: clock
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
