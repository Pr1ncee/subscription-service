"""Helpers for the FastStream subscribers: body decoding, retry timing, DB-outage waits."""

import asyncio
import logging
from collections.abc import Awaitable, Callable

from faststream.message import StreamMessage
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy.exc import InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from subscriptions_service.config import (
    DB_RETRY_BASE_S,
    DB_RETRY_MAX_S,
    RETRY_BASE_S,
    RETRY_MAX_S,
)
from subscriptions_service.domain.events import EventEnvelope

logger = logging.getLogger(__name__)

DB_UNAVAILABLE_ERRORS: tuple[type[BaseException], ...] = (
    OperationalError,
    InterfaceError,
    PoolTimeoutError,
    ConnectionError,
)
"""Errors meaning the database is unreachable, as opposed to a problem with the message."""


async def raw_body(message: StreamMessage[object]) -> bytes:
    """Decoder that hands the handler the raw bytes.

    FastStream's JSON decoding would fail on a malformed body before the handler
    could dead-letter it.
    """
    return message.body


async def wait_out_db_outages(
    handle: Callable[[EventEnvelope], Awaitable[None]], envelope: EventEnvelope, queue: str
) -> None:
    """Run `handle`, retrying in-process with backoff while the database is unreachable.

    Every message needs the database, so spending the delivery limit on an outage would
    dead-letter healthy events during a brief blip. Other errors propagate to the caller.
    """
    attempt = 0
    while True:
        try:
            await handle(envelope)
            return
        except DB_UNAVAILABLE_ERRORS as e:
            delay = min(DB_RETRY_BASE_S * 2**attempt, DB_RETRY_MAX_S)
            attempt += 1
            logger.warning(
                "database unavailable; waiting before retrying the same delivery",
                extra={
                    "queue": queue,
                    "event_id": str(envelope.event_id),
                    "delay_s": delay,
                    "error": str(e),
                },
            )
            await asyncio.sleep(delay)


def retry_delay(message: RabbitMessage) -> float:
    """Backoff before requeueing, growing with the message's `x-delivery-count`.

    A requeue counts toward the delivery limit, so backing off first keeps the limit
    from being spent within milliseconds.
    """
    try:
        deliveries = int((message.headers or {}).get("x-delivery-count", 0))
    except (TypeError, ValueError):
        deliveries = 0
    return float(min(RETRY_BASE_S * 2 ** min(deliveries, 16), RETRY_MAX_S))
