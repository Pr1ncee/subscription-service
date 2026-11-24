"""Request and response models of the HTTP API."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Self

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field

from subscriptions_service.api.utils import bounded_utc
from subscriptions_service.config import (
    CURRENCY_PATTERN,
    MAX_DAY_COUNT,
    MAX_TEXT_ID_LENGTH,
    NO_NUL_PATTERN,
)
from subscriptions_service.domain.enums import NotificationStatus, PaymentStatus
from subscriptions_service.services.dto import PaymentStatusView

TextId = Annotated[str, Field(min_length=1, max_length=MAX_TEXT_ID_LENGTH, pattern=NO_NUL_PATTERN)]
ExpiryDatetime = Annotated[AwareDatetime, AfterValidator(bounded_utc)]


class SubscriptionUpsertIn(BaseModel):
    """Body of `PUT /subscriptions/{subscription_id}`."""

    user_id: TextId
    day_count: int = Field(gt=0, le=MAX_DAY_COUNT)
    expected_expires_on: ExpiryDatetime


class NotificationOut(BaseModel):
    """A scheduled or sent notification of a subscription."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_name: str
    scheduled_for: datetime
    sent_at: datetime | None
    status: NotificationStatus


class SubscriptionOut(BaseModel):
    """A subscription with its notification history."""

    model_config = ConfigDict(from_attributes=True)

    subscription_id: str
    user_id: str
    day_count: int
    expected_expires_on: datetime
    notifications: list[NotificationOut]


class PaymentIn(BaseModel):
    """Body of `POST /payments`."""

    subscription_id: TextId
    provider_payment: TextId
    amount: Decimal = Field(gt=0, max_digits=19, decimal_places=4)
    currency: str = Field(pattern=CURRENCY_PATTERN)
    status: PaymentStatus


class PaymentOut(BaseModel):
    """A recorded payment."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID
    subscription_id: str
    provider_payment: str
    amount: Decimal
    currency: str
    status: PaymentStatus
    created_at: datetime


class EventDeliveryOut(BaseModel):
    """Delivery state of a payment's outbox event."""

    model_config = ConfigDict(from_attributes=True)

    event_id: uuid.UUID
    event_type: str
    published: bool
    published_at: datetime | None
    attempts: int
    last_error: str | None


class NotificationDeliveryOut(BaseModel):
    """Delivery state of a payment's result notification."""

    model_config = ConfigDict(from_attributes=True)

    status: NotificationStatus
    sent_at: datetime | None


class PaymentStatusOut(PaymentOut):
    """A payment with the delivery state of its event and its result notification."""

    event: EventDeliveryOut
    notification: NotificationDeliveryOut | None

    @classmethod
    def from_view(cls, view: PaymentStatusView) -> Self:
        """Map the service's view to the response model."""
        payment = PaymentOut.model_validate(view.payment, from_attributes=True)
        return cls(
            **payment.model_dump(),
            event=EventDeliveryOut.model_validate(view.event, from_attributes=True),
            notification=(
                None
                if view.notification is None
                else NotificationDeliveryOut.model_validate(view.notification, from_attributes=True)
            ),
        )
