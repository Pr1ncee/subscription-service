"""Thin FastStream subscribers: decode, call the service, settle the message."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage

from subscriptions_service.adapters.rabbit_topology import (
    DLQ_NAME,
    NOTIFICATIONS_SEND,
    PAYMENTS_EXTEND,
    PAYMENTS_NOTIFY,
    QueueSpec,
    dead_letter_queue,
    rabbit_queue,
)
from subscriptions_service.config import DLQ_RETRY_DELAY_S
from subscriptions_service.consumers import acking
from subscriptions_service.consumers.enums import Outcome
from subscriptions_service.consumers.utils import raw_body, retry_delay, wait_out_db_outages
from subscriptions_service.domain.events import EventEnvelope, InvalidEnvelopeError
from subscriptions_service.services.consumers import (
    DeadLetterHandler,
    NotificationDelivery,
    PaymentResultNotifier,
    SubscriptionExtender,
)

logger = logging.getLogger(__name__)

Handler = Callable[[bytes, RabbitMessage], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ConsumerServices:
    """The services the subscribers dispatch to, built by the composition root."""

    notifier: PaymentResultNotifier
    extender: SubscriptionExtender
    delivery: NotificationDelivery
    dead_letters: DeadLetterHandler


def register_handlers(
    broker: RabbitBroker, services: ConsumerServices, delivery_limit: int
) -> None:
    """One subscriber per queue. Bindings belong to `declare_topology`, so no exchange here."""
    routes: list[tuple[QueueSpec, Callable[[EventEnvelope], Awaitable[None]]]] = [
        (PAYMENTS_NOTIFY, services.notifier.handle),
        (PAYMENTS_EXTEND, services.extender.handle),
        (NOTIFICATIONS_SEND, services.delivery.handle),
    ]
    for spec, handle in routes:
        broker.subscriber(
            rabbit_queue(spec, delivery_limit), ack_policy=AckPolicy.MANUAL, decoder=raw_body
        )(_event_handler(spec.name, handle))
    broker.subscriber(dead_letter_queue(), ack_policy=AckPolicy.MANUAL, decoder=raw_body)(
        _dead_letter_handler(services.dead_letters)
    )


def _event_handler(queue: str, handle: Callable[[EventEnvelope], Awaitable[None]]) -> Handler:
    """Build the subscriber for one event queue; every path settles the message exactly once.

    - undecodable body or invalid payload: poison, dead-lettered immediately;
    - database unreachable: waited out in-process without spending the delivery limit;
    - any other failure: backoff, then requeue; past the delivery limit RabbitMQ dead-letters it.
    """

    async def handler(body: bytes, message: RabbitMessage) -> None:
        try:
            envelope = EventEnvelope.from_json(body)
        except Exception as e:
            logger.error("malformed event dead-lettered", extra={"queue": queue, "error": str(e)})
            await acking.settle(message, Outcome.DEAD_LETTER, queue=queue)
            return
        try:
            await wait_out_db_outages(handle, envelope, queue)
        except InvalidEnvelopeError as e:
            logger.error(
                "event with invalid payload dead-lettered",
                extra={"queue": queue, "event_id": str(envelope.event_id), "error": str(e)},
            )
            await acking.settle(message, Outcome.DEAD_LETTER, queue=queue)
            return
        except Exception:
            logger.exception(
                "event handling failed; will retry",
                extra={"queue": queue, "event_id": str(envelope.event_id)},
            )
            await asyncio.sleep(retry_delay(message))
            await acking.settle(message, Outcome.RETRY, queue=queue)
            return
        await acking.settle(message, Outcome.ACK, queue=queue)

    handler.__name__ = f"handle_{queue.replace('.', '_').replace('-', '_')}"
    return handler


def _dead_letter_handler(dead_letters: DeadLetterHandler) -> Handler:
    """Build the `events.dlq` subscriber.

    An undecodable dead letter can only be logged, so it is acked. A dead letter that
    cannot be recorded (e.g. Postgres down) is retried: the DLQ has no delivery limit,
    so it is never discarded.
    """

    async def handle_dead_letter(body: bytes, message: RabbitMessage) -> None:
        try:
            envelope = EventEnvelope.from_json(body)
        except Exception as e:
            logger.error("undecodable dead letter", extra={"error": str(e), "body": body[:200]})
            await acking.settle(message, Outcome.ACK, queue=DLQ_NAME)
            return
        reason = str((message.headers or {}).get("x-first-death-reason", "unknown"))
        try:
            await dead_letters.handle(envelope, reason)
        except Exception:
            logger.exception(
                "dead letter not recorded; will retry", extra={"event_id": str(envelope.event_id)}
            )
            await asyncio.sleep(DLQ_RETRY_DELAY_S)
            await acking.settle(message, Outcome.RETRY, queue=DLQ_NAME)
            return
        await acking.settle(message, Outcome.ACK, queue=DLQ_NAME)

    return handle_dead_letter
