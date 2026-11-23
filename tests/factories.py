import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from subscriptions_service.db.tables import NotificationRow, PaymentRow, SubscriptionRow

BASE_TIME = datetime(2026, 1, 1, 12, tzinfo=UTC)


def make_subscription(
    subscription_id: str = "s1",
    user_id: str = "u1",
    day_count: int = 30,
    expected_expires_on: datetime = BASE_TIME + timedelta(days=10),
) -> SubscriptionRow:
    return SubscriptionRow(
        subscription_id=subscription_id,
        user_id=user_id,
        day_count=day_count,
        expected_expires_on=expected_expires_on,
    )


def make_payment(
    subscription_id: str = "s1",
    provider_payment: str = "pp-1",
    amount: Decimal = Decimal("10.00"),
    status: str = "succeeded",
) -> PaymentRow:
    return PaymentRow(
        id=uuid.uuid4(),
        subscription_id=subscription_id,
        provider_payment=provider_payment,
        amount=amount,
        currency="USD",
        status=status,
        period_days=30,
    )


def make_notification(
    subscription_id: str = "s1",
    scheduled_for: datetime = BASE_TIME,
    status: str = "scheduled",
    event_name: str = "subscription.expiring",
    payment_id: uuid.UUID | None = None,
) -> NotificationRow:
    return NotificationRow(
        id=uuid.uuid4(),
        subscription_id=subscription_id,
        payment_id=payment_id,
        event_name=event_name,
        scheduled_for=scheduled_for,
        status=status,
    )
