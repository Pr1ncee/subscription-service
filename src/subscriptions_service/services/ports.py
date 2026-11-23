from datetime import datetime
from typing import Protocol

from subscriptions_service.domain.events import EventEnvelope


class Clock(Protocol):
    def now(self) -> datetime: ...


class EventPublisher(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None:
        """Publish and wait for the broker's confirm; raise if it is not confirmed."""
        ...
