import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from subscriptions_service.domain.enums import NotificationStatus, PaymentStatus


@dataclass(frozen=True, slots=True)
class NotificationView:
    id: uuid.UUID
    event_name: str
    scheduled_for: datetime
    sent_at: datetime | None
    status: NotificationStatus


@dataclass(frozen=True, slots=True)
class SubscriptionView:
    subscription_id: str
    user_id: str
    day_count: int
    expected_expires_on: datetime
    notifications: tuple[NotificationView, ...]


@dataclass(frozen=True, slots=True)
class UpsertResult:
    view: SubscriptionView
    created: bool


@dataclass(frozen=True, slots=True)
class RecordPaymentCommand:
    subscription_id: str
    provider_payment: str
    amount: Decimal
    currency: str
    status: PaymentStatus


@dataclass(frozen=True, slots=True)
class PaymentView:
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
    view: PaymentView
    created: bool


@dataclass(frozen=True, slots=True)
class EventDeliveryView:
    event_id: uuid.UUID
    event_type: str
    published_at: datetime | None
    attempts: int
    last_error: str | None

    @property
    def published(self) -> bool:
        return self.published_at is not None


@dataclass(frozen=True, slots=True)
class NotificationDeliveryView:
    status: NotificationStatus
    sent_at: datetime | None


@dataclass(frozen=True, slots=True)
class PaymentStatusView:
    payment: PaymentView
    event: EventDeliveryView
    notification: NotificationDeliveryView | None
