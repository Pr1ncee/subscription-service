import logging
from datetime import timedelta

from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.ports import Clock, EventPublisher

logger = logging.getLogger(__name__)

_BASE_DELAY_S = 0.5
_MAX_EXPONENT = 32  # 0.5 * 2**32 s is far beyond any sane cap; avoids float overflow
_MAX_ERROR_LEN = 1000


def backoff_delay(attempts: int, max_backoff_s: float) -> timedelta:
    """Exponential backoff after `attempts` failed tries: 0.5s, 1s, 2s, … capped."""
    exponent = min(max(attempts - 1, 0), _MAX_EXPONENT)
    return timedelta(seconds=min(max_backoff_s, _BASE_DELAY_S * 2**exponent))


class OutboxRelay:
    """Publishes pending outbox rows at-least-once, in `created_at` order."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        publisher: EventPublisher,
        clock: Clock,
        batch_size: int,
        max_backoff_s: float,
    ) -> None:
        self._uow_factory = uow_factory
        self._publisher = publisher
        self._clock = clock
        self._batch_size = batch_size
        self._max_backoff_s = max_backoff_s

    async def run_once(self) -> int:
        """Publish one batch; returns how many rows were published.

        A failure to commit the marks propagates: the batch stays pending and is
        republished later with the same event ids, which consumers deduplicate.
        """
        published = 0
        async with self._uow_factory() as uow:
            rows = await uow.outbox.lock_pending(self._clock.now(), self._batch_size)
            for row in rows:
                try:
                    envelope = EventEnvelope(
                        event_id=row.id,
                        event_type=EventType(row.event_type),
                        occurred_at=row.created_at,
                        payload=row.payload,
                    )
                    await self._publisher.publish(envelope)
                except Exception as e:
                    attempts = row.attempts + 1
                    next_attempt_at = self._clock.now() + backoff_delay(
                        attempts, self._max_backoff_s
                    )
                    await uow.outbox.mark_failed(
                        row, f"{type(e).__name__}: {e}"[:_MAX_ERROR_LEN], next_attempt_at
                    )
                    logger.warning(
                        "outbox publish failed",
                        extra={
                            "event_id": str(row.id),
                            "event_type": row.event_type,
                            "attempts": attempts,
                            "error": str(e),
                        },
                    )
                    continue
                await uow.outbox.mark_published(row, self._clock.now())
                published += 1
        return published
