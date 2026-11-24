import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from subscriptions_service.domain.events import EventEnvelope


class PublisherUnavailable(Exception):  # noqa: N818
    """The broker cannot be reached or did not answer in time.

    Distinct from a per-message rejection: retrying the next message now is pointless.
    Named as a state, like `TimeoutError`, rather than with an `Error` suffix.
    """


class Clock(Protocol):
    """Source of the current time; injected so tests control it."""

    def now(self) -> datetime:
        """The current time, timezone-aware (UTC)."""
        ...


class EventPublisher(Protocol):
    """Publishes outbox events to the broker; used only by the relay."""

    async def publish(self, envelope: EventEnvelope) -> None:
        """Publish and wait for the broker's confirm; raise if it is not confirmed.

        Raises `PublisherUnavailable` when the broker is unreachable or unresponsive.
        """
        ...


@dataclass(frozen=True, slots=True)
class OutgoingNotification:
    """What the sender needs to notify a user."""

    notification_id: uuid.UUID
    user_id: str
    subscription_id: str
    event_name: str
    expected_expires_on: datetime


class PermanentSendError(Exception):
    """The notification can never be delivered (e.g. invalid address); do not retry."""


class TransientSendError(RuntimeError):
    """The sender failed in a way that may succeed on retry (wraps the original error)."""


class NotificationSender(Protocol):
    """Delivers notifications to users (email, push, ...)."""

    async def send(self, message: OutgoingNotification) -> None:
        """Deliver to the user.

        Raise `PermanentSendError` for non-retryable failures; any other exception is
        treated as transient and the message is redelivered.
        """
        ...
