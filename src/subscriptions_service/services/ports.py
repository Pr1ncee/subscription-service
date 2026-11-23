from datetime import datetime
from typing import Protocol

from subscriptions_service.domain.events import EventEnvelope


class PublisherUnavailable(Exception):  # noqa: N818 - reads as a state, like TimeoutError
    """The broker cannot be reached or did not answer in time.

    Distinct from a per-message rejection: retrying the next message now is pointless.
    """


class Clock(Protocol):
    def now(self) -> datetime: ...


class EventPublisher(Protocol):
    async def publish(self, envelope: EventEnvelope) -> None:
        """Publish and wait for the broker's confirm; raise if it is not confirmed.

        Raises `PublisherUnavailable` when the broker is unreachable or unresponsive.
        """
        ...
