import asyncio

import pytest
from faststream.rabbit import RabbitBroker, RabbitQueue

from subscriptions_service.config import Settings
from subscriptions_service.services.dto import RecordPaymentResult
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.worker.main import run_worker
from tests.conftest import UNREACHABLE_BROKER
from tests.factories import CMD


async def _published(psvc: PaymentService, pay: RecordPaymentResult) -> bool:
    return (await psvc.get_status(pay.view.payment_id)).event.published


async def test_worker_publishes_recorded_payment(
    pg_url: str, rabbit_url: str, rabbit_broker: RabbitBroker, psvc: PaymentService
) -> None:
    pay = await psvc.record(CMD)
    settings = Settings(
        database_url=pg_url,
        rabbitmq_url=rabbit_url,
        relay_poll_interval_s=0.05,
        scheduler_poll_interval_s=0.05,
    )
    stop = asyncio.Event()
    worker = asyncio.create_task(run_worker(settings, stop))
    try:
        async with asyncio.timeout(15):
            while not await _published(psvc, pay):  # noqa: ASYNC110
                await asyncio.sleep(0.05)
    finally:
        stop.set()
        await asyncio.wait_for(worker, 10)

    queue = await rabbit_broker.declare_queue(RabbitQueue("payments.notify", declare=False))
    msg = await queue.get(timeout=5)
    assert msg is not None
    assert msg.message_id == str((await psvc.get_status(pay.view.payment_id)).event.event_id)


async def test_worker_exits_when_broker_unreachable(pg_url: str) -> None:
    """An unreachable broker at startup fails fast with ConnectionError instead of
    hanging until `wait_for` times out.
    """
    settings = Settings(database_url=pg_url, rabbitmq_url=UNREACHABLE_BROKER)
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(run_worker(settings, asyncio.Event()), 30)
