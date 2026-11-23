from datetime import datetime, timedelta

from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> None:
        self._now += delta


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[EventEnvelope] = []

    async def publish(self, envelope: EventEnvelope) -> None:
        self.published.append(envelope)


class FailingPublisher:
    """Fails for `fail_types` (every type when None) and records the rest."""

    def __init__(self, fail_types: set[EventType] | None = None, error: str = "boom") -> None:
        self._fail_types = fail_types
        self._error = error
        self.published: list[EventEnvelope] = []

    async def publish(self, envelope: EventEnvelope) -> None:
        if self._fail_types is None or envelope.event_type in self._fail_types:
            raise RuntimeError(self._error)
        self.published.append(envelope)
