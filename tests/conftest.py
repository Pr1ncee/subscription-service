from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from alembic import command
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from subscriptions_service.api.app import create_app
from subscriptions_service.api.deps import get_clock, get_session_factory
from subscriptions_service.config import Settings
from subscriptions_service.db.engine import create_session_factory
from subscriptions_service.repositories.uow import UnitOfWork, UnitOfWorkFactory
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.fakes import FakeClock
from tests.support import alembic_config

TABLES = "subscriptions, notifications, payments, outbox, processed_events"
# Same exact tag as docker-compose: delivery-limit semantics changed in RabbitMQ 4.3.
RABBITMQ_IMAGE = "rabbitmq:4.3.8-management-alpine"
RABBITMQ_MANAGEMENT_PORT = 15672


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


@pytest.fixture
async def psvc(
    uow_factory: UnitOfWorkFactory, clock: FakeClock, subs: SubscriptionService
) -> PaymentService:
    """Payment service with subscription `s1` (day_count 30) already created."""
    await subs.upsert("s1", "u1", 30, clock.now() + timedelta(days=10))
    return PaymentService(uow_factory, clock)


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


@pytest.fixture(scope="session")
def rabbit_container() -> Iterator[RabbitMqContainer]:
    with RabbitMqContainer(RABBITMQ_IMAGE).with_exposed_ports(RABBITMQ_MANAGEMENT_PORT) as rabbit:
        yield rabbit


@pytest.fixture(scope="session")
def rabbit_url(rabbit_container: RabbitMqContainer) -> str:
    host = rabbit_container.get_container_host_ip()
    port = rabbit_container.get_exposed_port(rabbit_container.port)
    return f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
def rabbit_management(rabbit_container: RabbitMqContainer) -> str:
    host = rabbit_container.get_container_host_ip()
    port = rabbit_container.get_exposed_port(RABBITMQ_MANAGEMENT_PORT)
    return f"http://{host}:{port}/api"


async def _delete_all_queues(management_url: str) -> None:
    async with httpx.AsyncClient(auth=("guest", "guest"), timeout=10) as client:
        for queue in (await client.get(f"{management_url}/queues/%2F")).json():
            await client.delete(f"{management_url}/queues/%2F/{queue['name']}")


@pytest.fixture
async def rabbit_broker(rabbit_url: str, rabbit_management: str) -> AsyncIterator[RabbitBroker]:
    broker = RabbitBroker(rabbit_url, default_channel=Channel(on_return_raises=True))
    await broker.connect()
    yield broker
    await broker.stop()
    # Tests declare queues with different x-delivery-limit values; drop them so they don't collide.
    await _delete_all_queues(rabbit_management)
