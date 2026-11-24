import uuid

from subscriptions_service.db.tables import PaymentRow
from subscriptions_service.domain.enums import AggregateType, EventType, PaymentStatus
from subscriptions_service.domain.errors import ConflictError, NotFoundError
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.dto import (
    PaymentStatusView,
    RecordPaymentCommand,
    RecordPaymentResult,
)
from subscriptions_service.services.ports import Clock
from subscriptions_service.services.utils import (
    is_identical_payment,
    payment_event_payload,
    payment_status_view,
    payment_view,
)


class PaymentService:
    """Records payments with their outbox event and reports their delivery state."""

    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock) -> None:
        self._uow_factory = uow_factory
        self._clock = clock

    async def record(self, cmd: RecordPaymentCommand) -> RecordPaymentResult:
        """Store the payment and its outbox event in one transaction (idempotent on provider id).

        The payment snapshots the subscription's current `day_count` as the period it buys.
        A concurrent duplicate blocks on the unique key until the first insert commits, so
        the existing row is visible when the insert is skipped; an identical retry returns
        it, a different payload is a `ConflictError`.
        """
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
                existing = await uow.payments.get_by_provider(cmd.provider_payment)
                if existing is None:  # pragma: no cover
                    raise ConflictError(f"payment {cmd.provider_payment!r} is being recorded")
                if not is_identical_payment(existing, cmd):
                    raise ConflictError(
                        f"payment {cmd.provider_payment!r} already recorded with a different payload"
                    )
                return RecordPaymentResult(view=payment_view(existing), created=False)

            event_type = (
                EventType.PAYMENT_SUCCEEDED
                if cmd.status is PaymentStatus.SUCCEEDED
                else EventType.PAYMENT_FAILED
            )
            await uow.outbox.add(
                EventEnvelope.new(event_type, payment_event_payload(inserted), now),
                AggregateType.PAYMENT,
                str(inserted.id),
            )
            view = payment_view(inserted)
        return RecordPaymentResult(view=view, created=True)

    async def get_status(self, payment_id: uuid.UUID) -> PaymentStatusView:
        """The payment with its event and notification state; `NotFoundError` if unknown."""
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
            if payment is None:
                raise NotFoundError(f"payment {payment_id} not found")
            return await payment_status_view(uow, payment)

    async def get_status_by_provider(self, provider_payment: str) -> PaymentStatusView:
        """Same as `get_status`, looked up by the provider's payment id."""
        async with self._uow_factory() as uow:
            payment = await uow.payments.get_by_provider(provider_payment)
            if payment is None:
                raise NotFoundError(f"payment {provider_payment!r} not found")
            return await payment_status_view(uow, payment)
