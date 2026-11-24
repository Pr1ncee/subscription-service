import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from subscriptions_service.domain.enums import NotificationStatus, PaymentStatus


@dataclass(frozen=True, slots=True)
class NotificationView:
    """A scheduled, sent, cancelled or failed notification."""

    id: uuid.UUID
    event_name: str
    scheduled_for: datetime
    sent_at: datetime | None
    status: NotificationStatus


@dataclass(frozen=True, slots=True)
class SubscriptionView:
    """A subscription with its notification history."""

    subscription_id: str
    user_id: str
    day_count: int
    expected_expires_on: datetime
    notifications: tuple[NotificationView, ...]


@dataclass(frozen=True, slots=True)
class UpsertResult:
    """Outcome of an upsert: the subscription and whether it was created."""

    view: SubscriptionView
    created: bool


@dataclass(frozen=True, slots=True)
class RecordPaymentCommand:
    """A payment to record, as reported by the payment provider."""

    subscription_id: str
    provider_payment: str
    amount: Decimal
    currency: str
    status: PaymentStatus


@dataclass(frozen=True, slots=True)
class PaymentView:
    """A recorded payment, including the period it bought."""

    payment_id: uuid.UUID
    subscription_id: str
    provider_payment: str
    amount: Decimal
    currency: str
    status: PaymentStatus
    period_days: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RecordPaymentResult:
    """Outcome of recording a payment: the payment and whether it was created."""

    view: PaymentView
    created: bool


@dataclass(frozen=True, slots=True)
class EventDeliveryView:
    """Delivery state of an outbox event."""

    event_id: uuid.UUID
    event_type: str
    published_at: datetime | None
    attempts: int
    last_error: str | None

    @property
    def published(self) -> bool:
        """Whether the broker confirmed the event."""
        return self.published_at is not None


@dataclass(frozen=True, slots=True)
class NotificationDeliveryView:
    """Delivery state of a payment-result notification."""

    status: NotificationStatus
    sent_at: datetime | None


@dataclass(frozen=True, slots=True)
class PaymentStatusView:
    """A payment with the delivery state of its event and its result notification."""

    payment: PaymentView
    event: EventDeliveryView
    notification: NotificationDeliveryView | None
