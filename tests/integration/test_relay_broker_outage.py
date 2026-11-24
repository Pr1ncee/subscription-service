import time
from dataclasses import replace
from datetime import timedelta

import pytest
from faststream.rabbit import RabbitBroker, RabbitQueue
from testcontainers.community.rabbitmq import RabbitMqContainer

from subscriptions_service.adapters.rabbit_publisher import RabbitPublisher
from subscriptions_service.adapters.rabbit_topology import declare_topology
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.relay import OutboxRelay
from tests.factories import CMD
from tests.fakes import FakeClock


@pytest.mark.slow
async def test_relay_keeps_rows_pending_while_broker_paused_then_publishes(
    uow_factory: UnitOfWorkFactory,
    psvc: PaymentService,
    clock: FakeClock,
    rabbit_broker: RabbitBroker,
    rabbit_container: RabbitMqContainer,
) -> None:
    """Review Focus 2: while the broker is paused a batch costs one publish timeout
    (not five) and every row stays pending. Past the first backoff the same relay and broker
    objects publish all five, with no restart.
    """
    await declare_topology(rabbit_broker, delivery_limit=5)
    pays = [await psvc.record(replace(CMD, provider_payment=f"pp-{i}")) for i in range(5)]
    relay = OutboxRelay(
        uow_factory,
        RabbitPublisher(rabbit_broker, timeout_s=1),
        clock,
        batch_size=100,
        max_backoff_s=60,
    )

    rabbit_container.get_wrapped_container().pause()
    try:
        started = time.monotonic()
        assert await relay.run_once() == 0
        assert time.monotonic() - started < 2.5
    finally:
        rabbit_container.get_wrapped_container().unpause()

    events = [(await psvc.get_status(p.view.payment_id)).event for p in pays]
    assert all(not e.published for e in events)
    assert sorted(e.attempts for e in events) == [0, 0, 0, 0, 1]

    clock.advance(timedelta(seconds=1))
    assert await relay.run_once() == 5
    events = [(await psvc.get_status(p.view.payment_id)).event for p in pays]
    assert all(e.published for e in events)

    queue = await rabbit_broker.declare_queue(RabbitQueue("payments.notify", declare=False))
    seen = set()
    while (msg := await queue.get(timeout=5, fail=False)) is not None:
        await msg.ack()
        seen.add(msg.message_id)
    assert seen >= {str(e.event_id) for e in events}
