from datetime import UTC, datetime

import httpx
import pytest
from faststream.rabbit import RabbitBroker, RabbitQueue
from testcontainers.community.rabbitmq import RabbitMqContainer

from subscriptions_service.adapters.rabbit_publisher import RabbitPublisher
from subscriptions_service.adapters.rabbit_topology import (
    DLQ_NAME,
    EVENTS_EXCHANGE,
    QUEUE_SPECS,
    declare_topology,
)
from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.services.ports import PublisherUnavailable

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


async def _get_one(broker: RabbitBroker, queue: str) -> tuple[str | None, bytes]:
    q = await broker.declare_queue(RabbitQueue(queue, declare=False))
    msg = await q.get(timeout=5)
    assert msg is not None
    await msg.ack()
    return msg.message_id, msg.body


async def test_event_retained_without_consumers(rabbit_broker: RabbitBroker) -> None:
    await declare_topology(rabbit_broker, delivery_limit=5)
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1"}, NOW)
    await RabbitPublisher(rabbit_broker, timeout_s=5).publish(env)
    for name in ("payments.notify", "payments.extend-subscription"):
        message_id, body = await _get_one(rabbit_broker, name)
        assert message_id == str(env.event_id)
        assert EventEnvelope.from_json(body) == env


async def test_routing_by_event_type(rabbit_broker: RabbitBroker) -> None:
    await declare_topology(rabbit_broker, delivery_limit=5)
    publisher = RabbitPublisher(rabbit_broker, timeout_s=5)
    failed = EventEnvelope.new(EventType.PAYMENT_FAILED, {"payment_id": "p2"}, NOW)
    due = EventEnvelope.new(EventType.NOTIFICATION_DUE, {"notification_id": "n1"}, NOW)
    await publisher.publish(failed)
    await publisher.publish(due)
    assert (await _get_one(rabbit_broker, "payments.notify"))[0] == str(failed.event_id)
    assert (await _get_one(rabbit_broker, "notifications.send"))[0] == str(due.event_id)
    extend = await rabbit_broker.declare_queue(
        RabbitQueue("payments.extend-subscription", declare=False)
    )
    assert await extend.get(timeout=5, fail=False) is None  # payment.failed does not extend


async def test_declare_topology_is_idempotent(rabbit_broker: RabbitBroker) -> None:
    await declare_topology(rabbit_broker, delivery_limit=5)
    await declare_topology(rabbit_broker, delivery_limit=5)


async def test_queue_arguments_declared(
    rabbit_broker: RabbitBroker, rabbit_management: str
) -> None:
    await declare_topology(rabbit_broker, delivery_limit=5)
    async with httpx.AsyncClient(auth=("guest", "guest"), timeout=10) as client:
        for spec in QUEUE_SPECS:
            queue = (await client.get(f"{rabbit_management}/queues/%2F/{spec.name}")).json()
            assert queue["durable"] is True
            assert queue["arguments"] == {
                "x-queue-type": "quorum",
                "x-delivery-limit": 5,
                "x-dead-letter-exchange": "events.dlx",
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
            }
        dlq = (await client.get(f"{rabbit_management}/queues/%2F/{DLQ_NAME}")).json()
        assert dlq["arguments"] == {"x-queue-type": "quorum", "x-delivery-limit": -1}
        bindings = (
            await client.get(f"{rabbit_management}/exchanges/%2F/events.dlx/bindings/source")
        ).json()
        assert [b["destination"] for b in bindings] == [DLQ_NAME]


async def test_unroutable_publish_raises(rabbit_broker: RabbitBroker) -> None:
    await rabbit_broker.declare_exchange(EVENTS_EXCHANGE)  # exchange only, no queues bound
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1"}, NOW)
    with pytest.raises(Exception):  # noqa: B017 - any failure is acceptable, silence is not
        await RabbitPublisher(rabbit_broker, timeout_s=5).publish(env)


@pytest.mark.slow
async def test_publish_recovers_after_broker_pause(
    rabbit_broker: RabbitBroker, rabbit_container: RabbitMqContainer
) -> None:
    await declare_topology(rabbit_broker, 5)
    pub = RabbitPublisher(rabbit_broker, timeout_s=2)
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1"}, NOW)
    rabbit_container.get_wrapped_container().pause()
    try:
        with pytest.raises(PublisherUnavailable, match="not confirmed within 2"):
            await pub.publish(env)
    finally:
        rabbit_container.get_wrapped_container().unpause()
    await pub.publish(env)  # same broker object; connection recovered
    message_id, _ = await _get_one(rabbit_broker, "payments.notify")
    assert message_id == str(env.event_id)
