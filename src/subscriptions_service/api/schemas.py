import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field

from subscriptions_service.domain.enums import NotificationStatus, PaymentStatus

# Postgres `text` cannot store NUL; reject it here instead of failing in the driver.
NO_NUL_PATTERN = r"^[^\x00]*$"
TextId = Annotated[str, Field(min_length=1, max_length=255, pattern=NO_NUL_PATTERN)]

# Leaves headroom for reminder offsets and paid extensions without leaving datetime's range.
MIN_EXPIRY_YEAR, MAX_EXPIRY_YEAR = 1970, 9000


def _bounded_utc(value: datetime) -> datetime:
    try:
        utc = value.astimezone(UTC)
    except OverflowError as e:
        raise ValueError("datetime is out of range") from e
    if not MIN_EXPIRY_YEAR <= utc.year < MAX_EXPIRY_YEAR:
        raise ValueError(f"year must be in [{MIN_EXPIRY_YEAR}, {MAX_EXPIRY_YEAR})")
    return utc


ExpiryDatetime = Annotated[AwareDatetime, AfterValidator(_bounded_utc)]


class SubscriptionUpsertIn(BaseModel):
    user_id: TextId
    day_count: int = Field(gt=0, le=3650)
    expected_expires_on: ExpiryDatetime


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
    subscription_id: TextId
    provider_payment: TextId
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
