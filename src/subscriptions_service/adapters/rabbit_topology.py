"""RabbitMQ topology shared by every process that touches the broker.

Declaring from the same functions everywhere keeps queue arguments identical, so a
redeclare never fails with PRECONDITION_FAILED.
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

EVENTS_EXCHANGE = RabbitExchange("events", type=ExchangeType.TOPIC, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("events.dlx", type=ExchangeType.FANOUT, durable=True)
DLQ_NAME = "events.dlq"


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """A consumer queue and the routing keys it is bound with on the events exchange."""

    name: str
    routing_keys: tuple[str, ...]


PAYMENTS_NOTIFY = QueueSpec("payments.notify", ("payment.*",))
PAYMENTS_EXTEND = QueueSpec("payments.extend-subscription", ("payment.succeeded",))
NOTIFICATIONS_SEND = QueueSpec("notifications.send", ("notification.due",))
QUEUE_SPECS: tuple[QueueSpec, ...] = (PAYMENTS_NOTIFY, PAYMENTS_EXTEND, NOTIFICATIONS_SEND)


def rabbit_queue(spec: QueueSpec, delivery_limit: int) -> RabbitQueue:
    """Quorum queue that dead-letters to `events.dlx` after `delivery_limit` deliveries.

    At-least-once dead-lettering keeps the message until the DLQ confirms it; RabbitMQ
    requires `reject-publish` overflow for this strategy.
    """
    return RabbitQueue(
        spec.name,
        queue_type=QueueType.QUORUM,
        durable=True,
        arguments={
            "x-delivery-limit": delivery_limit,
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


def dead_letter_queue() -> RabbitQueue:
    """Quorum queue for dead letters, with the delivery limit switched off.

    Quorum queues default to a delivery limit of 20 in RabbitMQ 4.x; the DLQ has no
    dead-letter exchange of its own, so a message past that limit would be dropped.
    """
    return RabbitQueue(
        DLQ_NAME, queue_type=QueueType.QUORUM, durable=True, arguments={"x-delivery-limit": -1}
    )


async def declare_topology(broker: RabbitBroker, delivery_limit: int) -> None:
    """Declare exchanges, queues and bindings. Idempotent; the broker must be connected."""
    events = await broker.declare_exchange(EVENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)
    await (await broker.declare_queue(dead_letter_queue())).bind(dlx, routing_key="")
    for spec in QUEUE_SPECS:
        queue = await broker.declare_queue(rabbit_queue(spec, delivery_limit))
        for routing_key in spec.routing_keys:
            await queue.bind(events, routing_key=routing_key)
