import uuid
from dataclasses import dataclass
from datetime import datetime

from subscriptions_service.domain.enums import NotificationStatus


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
