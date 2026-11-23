import uuid
from decimal import Decimal

from subscriptions_service.db.tables import PaymentRow
from subscriptions_service.domain.enums import EventType, NotificationStatus, PaymentStatus
from subscriptions_service.domain.errors import ConflictError, NotFoundError
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWork, UnitOfWorkFactory
from subscriptions_service.services.dto import (
    EventDeliveryView,
    NotificationDeliveryView,
    PaymentStatusView,
    PaymentView,
    RecordPaymentCommand,
    RecordPaymentResult,
)
from subscriptions_service.services.ports import Clock

PAYMENT_AGGREGATE = "payment"
_AMOUNT_QUANTUM = Decimal("0.0001")


class PaymentService:
    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def record(self, cmd: RecordPaymentCommand) -> RecordPaymentResult:
        """Store the payment and its outbox event in one transaction (idempotent on provider id)."""
        now = self._clock.now()
        async with self._uow_factory() as uow:
            subscription = await uow.subscriptions.get(cmd.subscription_id)
            if subscription is None:
                raise NotFoundError(f"subscription {cmd.subscription_id!r} not found")

            inserted = await uow.payments.insert_if_absent(
                PaymentRow(
                    id=uuid.uuid4(),
                    subscription_id=cmd.subscription_id,
                    provider_payment=cmd.provider_payment,
                    amount=cmd.amount,
                    currency=cmd.currency,
                    status=cmd.status.value,
                    period_days=subscription.day_count,
                    created_at=now,
                )
            )
            if inserted is None:
                # A concurrent duplicate blocked on the unique key until its winner
                # committed, so the existing row is visible here.
                existing = await uow.payments.get_by_provider(cmd.provider_payment)
                if existing is None:  # pragma: no cover - conflict implies the row exists
                    raise ConflictError(f"payment {cmd.provider_payment!r} is being recorded")
                if not _is_identical(existing, cmd):
                    raise ConflictError(
                        f"payment {cmd.provider_payment!r} already recorded with a different payload"
                    )
                return RecordPaymentResult(view=_payment_view(existing), created=False)

            event_type = (
                EventType.PAYMENT_SUCCEEDED
                if cmd.status is PaymentStatus.SUCCEEDED
                else EventType.PAYMENT_FAILED
            )
            await uow.outbox.add(
                EventEnvelope.new(event_type, _event_payload(inserted), now),
                PAYMENT_AGGREGATE,
                str(inserted.id),
            )
            view = _payment_view(inserted)
        return RecordPaymentResult(view=view, created=True)

    async def get_status(self, payment_id: uuid.UUID) -> PaymentStatusView:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                raise NotFoundError(f"payment {payment_id} not found")
            return await _status_view(uow, payment)

    async def get_status_by_provider(self, provider_payment: str) -> PaymentStatusView:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_by_provider(provider_payment)
            if payment is None:
                raise NotFoundError(f"payment {provider_payment!r} not found")
            return await _status_view(uow, payment)


def _is_identical(existing: PaymentRow, cmd: RecordPaymentCommand) -> bool:
    # period_days is not compared: the client never sends it.
    return (
        existing.subscription_id == cmd.subscription_id
        and existing.currency == cmd.currency
        and existing.status == cmd.status.value
        and existing.amount == cmd.amount  # Decimal equality: 10 == 10.00
    )


def _event_payload(payment: PaymentRow) -> dict[str, object]:
    return {
        "payment_id": str(payment.id),
        "subscription_id": payment.subscription_id,
        "provider_payment": payment.provider_payment,
        "amount": str(payment.amount.quantize(_AMOUNT_QUANTUM)),
        "currency": payment.currency,
        "status": payment.status,
        "period_days": payment.period_days,
    }


def _payment_view(payment: PaymentRow) -> PaymentView:
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


async def _status_view(uow: UnitOfWork, payment: PaymentRow) -> PaymentStatusView:
    event = await uow.outbox.get_for_aggregate(PAYMENT_AGGREGATE, str(payment.id))
    if event is None:  # pragma: no cover - written in the same transaction as the payment
        raise RuntimeError(f"payment {payment.id} has no outbox event")
    notification = await uow.notifications.get_by_payment(payment.id)
    return PaymentStatusView(
        payment=_payment_view(payment),
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
