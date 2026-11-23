import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from subscriptions_service.domain.enums import NotificationStatus, PaymentStatus


class SubscriptionUpsertIn(BaseModel):
    user_id: str = Field(min_length=1, max_length=255)
    day_count: int = Field(gt=0, le=3650)
    expected_expires_on: AwareDatetime


class NotificationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_name: str
    scheduled_for: datetime
    sent_at: datetime | None
    status: NotificationStatus


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    subscription_id: str
    user_id: str
    day_count: int
    expected_expires_on: datetime
    notifications: list[NotificationOut]


class PaymentIn(BaseModel):
    subscription_id: str = Field(min_length=1, max_length=255)
    provider_payment: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0, max_digits=19, decimal_places=4)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    status: PaymentStatus


class PaymentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID
    subscription_id: str
    provider_payment: str
    amount: Decimal
    currency: str
    status: PaymentStatus
    created_at: datetime


class EventDeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: uuid.UUID
    event_type: str
    published: bool
    published_at: datetime | None
    attempts: int
    last_error: str | None


class NotificationDeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    status: NotificationStatus
    sent_at: datetime | None


class PaymentStatusOut(PaymentOut):
    event: EventDeliveryOut
    notification: NotificationDeliveryOut | None
