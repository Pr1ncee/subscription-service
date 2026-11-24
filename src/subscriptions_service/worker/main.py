"""Composition root for the worker process: scheduler + outbox relay."""

import asyncio
import logging

from faststream.rabbit import Channel, RabbitBroker

from subscriptions_service.adapters.clock import SystemClock
from subscriptions_service.adapters.rabbit_publisher import RabbitPublisher
from subscriptions_service.adapters.rabbit_topology import declare_topology
from subscriptions_service.config import Settings
from subscriptions_service.db.engine import create_engine, create_session_factory
from subscriptions_service.logs import configure_logging
from subscriptions_service.repositories.uow import UnitOfWork
from subscriptions_service.services.relay import OutboxRelay
from subscriptions_service.services.scheduler import NotificationScheduler
from subscriptions_service.worker.loop import run_periodic
from subscriptions_service.worker.utils import stop_on_signals

logger = logging.getLogger(__name__)


async def run_worker(settings: Settings, stop: asyncio.Event | None = None) -> None:
    """Run until SIGTERM/SIGINT (or until `stop` is set, for tests).

    An unreachable broker at startup raises: the process exits non-zero and the
    supervisor (compose `restart: unless-stopped`) restarts it. The broker is connected
    before the topology is declared, because declarations need an open channel.
    """
    stop = stop or stop_on_signals()
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    broker = RabbitBroker(settings.rabbitmq_url, default_channel=Channel(on_return_raises=True))
    clock = SystemClock()

    def uow_factory() -> UnitOfWork:
        return UnitOfWork(session_factory)

    try:
        await broker.connect()
        await declare_topology(broker, settings.consumer_delivery_limit)
        scheduler = NotificationScheduler(uow_factory, clock, settings.scheduler_batch_size)
        relay = OutboxRelay(
            uow_factory,
            RabbitPublisher(broker, settings.publish_timeout_s),
            clock,
            settings.relay_batch_size,
            settings.relay_max_backoff_s,
        )
        logger.info("worker started")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                run_periodic(
                    scheduler.run_once,
                    interval_s=settings.scheduler_poll_interval_s,
                    name="scheduler",
                    stop=stop,
                )
            )
            tg.create_task(
                run_periodic(
                    relay.run_once,
                    interval_s=settings.relay_poll_interval_s,
                    name="relay",
                    stop=stop,
                )
            )
        logger.info("worker stopped")
    finally:
        await broker.stop()
        await engine.dispose()


def main() -> None:
    """Entry point: read settings from the environment and run the worker."""
    settings = Settings()
    configure_logging(settings.log_level)
    asyncio.run(run_worker(settings))
