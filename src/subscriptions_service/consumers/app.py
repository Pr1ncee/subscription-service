"""Composition root for the consumers process."""

from faststream import FastStream
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from subscriptions_service.adapters.clock import SystemClock
from subscriptions_service.adapters.logging_sender import LoggingSender
from subscriptions_service.adapters.rabbit_topology import declare_topology
from subscriptions_service.config import Settings
from subscriptions_service.consumers.handlers import ConsumerServices, register_handlers
from subscriptions_service.db.engine import create_engine, create_session_factory
from subscriptions_service.repositories.uow import UnitOfWork
from subscriptions_service.services.consumers import (
    DeadLetterHandler,
    NotificationDelivery,
    PaymentResultNotifier,
    SubscriptionExtender,
)
from subscriptions_service.services.ports import Clock, NotificationSender


def create_app(
    settings: Settings,
    *,
    sender: NotificationSender | None = None,
    clock: Clock | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastStream:
    engine: AsyncEngine | None = None
    if session_factory is None:
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
    factory = session_factory
    clock = clock or SystemClock()

    def uow_factory() -> UnitOfWork:
        return UnitOfWork(factory)

    services = ConsumerServices(
        notifier=PaymentResultNotifier(uow_factory, clock),
        extender=SubscriptionExtender(uow_factory, clock, settings.reminder_offsets_days),
        delivery=NotificationDelivery(uow_factory, clock, sender or LoggingSender()),
        dead_letters=DeadLetterHandler(uow_factory, clock),
    )
    broker = RabbitBroker(settings.rabbitmq_url)
    register_handlers(broker, services, settings.consumer_delivery_limit)
    app = FastStream(broker)

    @app.on_startup
    async def connect_and_declare() -> None:
        # FastStream runs on_startup before connecting; declarations need a channel.
        await broker.connect()
        await declare_topology(broker, settings.consumer_delivery_limit)

    @app.after_shutdown
    async def dispose_engine() -> None:
        if engine is not None:
            await engine.dispose()

    return app
