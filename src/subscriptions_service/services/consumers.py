"""Event handlers behind the FastStream subscribers.

Each handler records the event in the inbox in the same transaction as its side
effects, so a redelivered event (at-least-once) is applied exactly once. Any
exception rolls the whole transaction back, inbox row included, so the redelivery
is processed again.
"""

import logging
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from subscriptions_service.db.tables import NotificationRow
from subscriptions_service.domain.enums import EventType, NotificationEvent, NotificationStatus
from subscriptions_service.domain.events import EventEnvelope, InvalidEnvelopeError
from subscriptions_service.domain.schedule import extend_expiry
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.ports import (
    Clock,
    NotificationSender,
    OutgoingNotification,
    PermanentSendError,
    TransientSendError,
)
from subscriptions_service.services.reminders import reschedule_reminders
from subscriptions_service.services.utils import payload_text, payload_uuid

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _PaymentEvent:
    payment_id: uuid.UUID
    subscription_id: str
    event_name: NotificationEvent
    period_days: int

    @classmethod
    def parse(cls, payload: Mapping[str, Any]) -> "_PaymentEvent":
        status = payload_text(payload, "status")
        try:
            event_name = NotificationEvent(f"payment.{status}")
        except ValueError as e:
            raise InvalidEnvelopeError(f"unknown payment status {status!r}") from e
        period_days = payload.get("period_days")
        if not isinstance(period_days, int) or isinstance(period_days, bool) or period_days <= 0:
            raise InvalidEnvelopeError("payload.period_days must be a positive integer")
        return cls(
            payload_uuid(payload, "payment_id"),
            payload_text(payload, "subscription_id"),
            event_name,
            period_days,
        )


class PaymentResultNotifier:
    """`payment.*` → a payment-result notification, sent later by the scheduler."""

    CONSUMER_NAME: ClassVar[str] = "payment-notifier"

    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def handle(self, envelope: EventEnvelope) -> None:
        event = _PaymentEvent.parse(envelope.payload)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            if not await uow.inbox.try_mark_processed(self.CONSUMER_NAME, envelope.event_id, now):
                return
            await uow.notifications.insert_payment_notification(
                NotificationRow(
                    subscription_id=event.subscription_id,
                    payment_id=event.payment_id,
                    event_name=event.event_name.value,
                    scheduled_for=now,
                    status=NotificationStatus.SCHEDULED.value,
                    created_at=now,
                    updated_at=now,
                )
            )


class SubscriptionExtender:
    """`payment.succeeded` → extend by the period the payment bought, reschedule reminders."""

    CONSUMER_NAME: ClassVar[str] = "subscription-extender"

    def __init__(
        self, uow_factory: UnitOfWorkFactory, clock: Clock, reminder_offsets_days: Sequence[int]
    ) -> None:
        self._uow_factory = uow_factory
        self._clock = clock
        self._offsets = tuple(reminder_offsets_days)

    async def handle(self, envelope: EventEnvelope) -> None:
        if envelope.event_type is not EventType.PAYMENT_SUCCEEDED:
            logger.warning("extender ignored event", extra={"event_type": envelope.event_type})
            return
        event = _PaymentEvent.parse(envelope.payload)
        now = self._clock.now()
        async with self._uow_factory() as uow:
            if not await uow.inbox.try_mark_processed(self.CONSUMER_NAME, envelope.event_id, now):
                return
            # Lock order: subscription → notification (reschedule touches notifications).
            subscription = await uow.subscriptions.get_for_update(event.subscription_id)
            if subscription is None:
                raise LookupError(f"subscription {event.subscription_id!r} not found")
            # The period comes from the event (snapshotted at payment time), never from
            # the subscription's current day_count.
            subscription.expected_expires_on = extend_expiry(
                subscription.expected_expires_on, event.period_days, now
            )
            subscription.updated_at = now
            await reschedule_reminders(uow, subscription, self._offsets, now)


class NotificationDelivery:
    """`notification.due` → send it, unless it is no longer queued or is a stale reminder."""

    CONSUMER_NAME: ClassVar[str] = "notification-sender"

    def __init__(
        self, uow_factory: UnitOfWorkFactory, clock: Clock, sender: NotificationSender
    ) -> None:
        self._uow_factory = uow_factory
        self._clock = clock
        self._sender = sender

    async def handle(self, envelope: EventEnvelope) -> None:
        notification_id = payload_uuid(envelope.payload, "notification_id")
        now = self._clock.now()
        async with self._uow_factory() as uow:
            if not await uow.inbox.try_mark_processed(self.CONSUMER_NAME, envelope.event_id, now):
                return
            peek = await uow.notifications.get(notification_id)
            if peek is None:
                logger.warning(
                    "notification not found", extra={"notification_id": str(notification_id)}
                )
                return
            # Lock order: subscription (FOR SHARE) → notification (FOR UPDATE). The extender
            # holds the subscription FOR UPDATE, so no extension can commit between the
            # stale check below and the send.
            subscription = await uow.subscriptions.get_for_share(peek.subscription_id)
            notification = await uow.notifications.get_for_update(notification_id)
            if subscription is None or notification is None:  # pragma: no cover - FK-protected
                return
            if notification.status != NotificationStatus.QUEUED:
                return
            is_reminder = notification.payment_id is None
            if is_reminder and notification.target_expires_on != subscription.expected_expires_on:
                notification.status = NotificationStatus.CANCELLED.value
                notification.updated_at = now
                return
            try:
                await self._sender.send(
                    OutgoingNotification(
                        notification_id=notification.id,
                        user_id=subscription.user_id,
                        subscription_id=subscription.subscription_id,
                        event_name=notification.event_name,
                        expected_expires_on=subscription.expected_expires_on,
                    )
                )
            except PermanentSendError as e:
                notification.status = NotificationStatus.FAILED.value
                notification.last_error = str(e)
                notification.updated_at = now
                return
            except Exception as e:
                # Anything else from the sender is transient: roll back and let it retry.
                raise TransientSendError(f"send failed: {e}") from e
            notification.status = NotificationStatus.SENT.value
            notification.sent_at = now
            notification.updated_at = now


class DeadLetterHandler:
    """Records dead-lettered events so nothing sits in `queued` forever."""

    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def handle(self, envelope: EventEnvelope, reason: str) -> None:
        logger.error(
            "event dead-lettered",
            extra={
                "event_id": str(envelope.event_id),
                "event_type": envelope.event_type.value,
                "reason": reason,
            },
        )
        if envelope.event_type is not EventType.NOTIFICATION_DUE:
            return
        try:
            notification_id = payload_uuid(envelope.payload, "notification_id")
        except InvalidEnvelopeError as e:
            # Unrecordable: retrying cannot help, and the DLQ has no delivery limit.
            logger.error("dead letter has an invalid payload", extra={"error": str(e)})
            return
        now = self._clock.now()
        async with self._uow_factory() as uow:
            notification = await uow.notifications.get_for_update(notification_id)
            if notification is not None and notification.status == NotificationStatus.QUEUED:
                notification.status = NotificationStatus.FAILED.value
                notification.last_error = f"dead-lettered: {reason}"
                notification.updated_at = now
