"""Helpers shared by the services: event payload parsing and row → DTO mapping."""

import uuid
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from subscriptions_service.db.tables import NotificationRow, PaymentRow, SubscriptionRow
from subscriptions_service.domain.enums import AggregateType, NotificationStatus, PaymentStatus
from subscriptions_service.domain.events import InvalidEnvelopeError
from subscriptions_service.repositories.uow import UnitOfWork
from subscriptions_service.services.dto import (
    EventDeliveryView,
    NotificationDeliveryView,
    NotificationView,
    PaymentStatusView,
    PaymentView,
    RecordPaymentCommand,
    SubscriptionView,
)

AMOUNT_QUANTUM = Decimal("0.0001")
"""Scale of amounts in event payloads, matching the `numeric(19, 4)` column."""


def payload_text(payload: Mapping[str, Any], key: str) -> str:
    """Read a required non-empty string from an event payload.

    A malformed payload is poison: `InvalidEnvelopeError` dead-letters it instead of retrying.
    """
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise InvalidEnvelopeError(f"payload.{key} must be a non-empty string")
    return value


def payload_uuid(payload: Mapping[str, Any], key: str) -> uuid.UUID:
    """Read a required UUID from an event payload; raises `InvalidEnvelopeError` if malformed."""
    try:
        return uuid.UUID(payload_text(payload, key))
    except ValueError as e:
        raise InvalidEnvelopeError(f"payload.{key} must be a UUID") from e


def is_identical_payment(existing: PaymentRow, cmd: RecordPaymentCommand) -> bool:
    """Whether a retried payment matches the recorded one.

    Amounts compare as decimals (10 == 10.00). `period_days` is not compared: the client
    never sends it.
    """
    return (
        existing.subscription_id == cmd.subscription_id
        and existing.currency == cmd.currency
        and existing.status == cmd.status.value
        and existing.amount == cmd.amount
    )


def payment_event_payload(payment: PaymentRow) -> dict[str, object]:
    """Payload of the `payment.*` event, including the period snapshotted at recording time."""
    return {
        "payment_id": str(payment.id),
        "subscription_id": payment.subscription_id,
        "provider_payment": payment.provider_payment,
        "amount": str(payment.amount.quantize(AMOUNT_QUANTUM)),
        "currency": payment.currency,
        "status": payment.status,
        "period_days": payment.period_days,
    }


def payment_view(payment: PaymentRow) -> PaymentView:
    """Map a payment row to its DTO."""
    return PaymentView(
        payment_id=payment.id,
        subscription_id=payment.subscription_id,
        provider_payment=payment.provider_payment,
        amount=payment.amount,
        currency=payment.currency,
        status=PaymentStatus(payment.status),
        period_days=payment.period_days,
        created_at=payment.created_at,
    )


async def payment_status_view(uow: UnitOfWork, payment: PaymentRow) -> PaymentStatusView:
    """A payment with the delivery state of its outbox event and its result notification.

    The event always exists: it is written in the same transaction as the payment.
    """
    event = await uow.outbox.get_for_aggregate(AggregateType.PAYMENT, str(payment.id))
    if event is None:  # pragma: no cover
        raise RuntimeError(f"payment {payment.id} has no outbox event")
    notification = await uow.notifications.get_by_payment(payment.id)
    return PaymentStatusView(
        payment=payment_view(payment),
        event=EventDeliveryView(
            event_id=event.id,
            event_type=event.event_type,
            published_at=event.published_at,
            attempts=event.attempts,
            last_error=event.last_error,
        ),
        notification=(
            None
            if notification is None
            else NotificationDeliveryView(
                status=NotificationStatus(notification.status), sent_at=notification.sent_at
            )
        ),
    )


def notification_view(row: NotificationRow) -> NotificationView:
    """Map a notification row to its DTO."""
    return NotificationView(
        id=row.id,
        event_name=row.event_name,
        scheduled_for=row.scheduled_for,
        sent_at=row.sent_at,
        status=NotificationStatus(row.status),
    )


async def subscription_view(uow: UnitOfWork, subscription: SubscriptionRow) -> SubscriptionView:
    """A subscription with its full notification history."""
    notifications = await uow.notifications.list_for_subscription(subscription.subscription_id)
    return SubscriptionView(
        subscription_id=subscription.subscription_id,
        user_id=subscription.user_id,
        day_count=subscription.day_count,
        expected_expires_on=subscription.expected_expires_on,
        notifications=tuple(notification_view(n) for n in notifications),
    )
