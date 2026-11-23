"""The one place that decides how a consumed message is settled with RabbitMQ."""

import logging
from enum import StrEnum

from faststream.rabbit.annotations import RabbitMessage

logger = logging.getLogger(__name__)


class Outcome(StrEnum):
    ACK = "ack"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


async def settle(message: RabbitMessage, outcome: Outcome, *, queue: str) -> None:
    """Ack only after the handler's transaction committed.

    Retries use `basic.reject(requeue=True)`, never `basic.nack`: since RabbitMQ 4.3 a
    requeued nack does not count toward the quorum queue's `x-delivery-limit`, so a
    poison message would never reach the dead-letter queue.
    """
    match outcome:
        case Outcome.ACK:
            await message.ack()
        case Outcome.RETRY:
            await message.reject(requeue=True)
        case Outcome.DEAD_LETTER:
            await message.reject(requeue=False)
    logger.debug("message settled", extra={"queue": queue, "outcome": outcome.value})
