import uuid
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class OutgoingNotification:
    notification_id: uuid.UUID
    user_id: str
    subscription_id: str
    event_name: str
    expected_expires_on: datetime


class PermanentSendError(Exception):
    """The notification can never be delivered (e.g. invalid address); do not retry."""


class NotificationSender(Protocol):
    async def send(self, message: OutgoingNotification) -> None:
        """Deliver to the user. Raise `PermanentSendError` for non-retryable failures;
        any other exception is treated as transient and the message is redelivered."""
        ...
