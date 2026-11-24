import asyncio
import uuid
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from subscriptions_service.domain.enums import PaymentStatus
from subscriptions_service.domain.errors import ConflictError, NotFoundError
from subscriptions_service.repositories.outbox import OutboxRepository
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.factories import CMD, make_notification
from tests.fakes import FakeClock

D = timedelta(days=1)


async def _outbox_count(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> int:
    async with uow_factory() as uow:
        return len(await uow.outbox.lock_pending(clock.now() + 365 * D, limit=100))


async def test_record_writes_payment_and_outbox_event(
    psvc: PaymentService, uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    res = await psvc.record(CMD)
    assert res.created is True
    assert (res.view.subscription_id, res.view.provider_payment, res.view.currency) == (
        "s1",
        "pp-1",
        "USD",
    )
    assert res.view.amount == Decimal("10.00")
    assert res.view.status is PaymentStatus.SUCCEEDED
    assert res.view.created_at == clock.now()

    status = await psvc.get_status(res.view.payment_id)
    assert status.payment == res.view
    assert status.event.event_type == "payment.succeeded"
    assert status.event.published is False and status.event.published_at is None
    assert status.event.attempts == 0 and status.event.last_error is None
    assert status.notification is None

    async with uow_factory() as uow:
        row = await uow.outbox.get_for_aggregate("payment", str(res.view.payment_id))
    assert row is not None and row.id == status.event.event_id
    assert row.payload == {
        "payment_id": str(res.view.payment_id),
        "subscription_id": "s1",
        "provider_payment": "pp-1",
        "amount": "10.0000",
        "currency": "USD",
        "status": "succeeded",
        "period_days": 30,
    }
    assert res.view.period_days == 30


async def test_failed_payment_emits_payment_failed(psvc: PaymentService) -> None:
    res = await psvc.record(replace(CMD, status=PaymentStatus.FAILED))
    assert (await psvc.get_status(res.view.payment_id)).event.event_type == "payment.failed"


async def test_period_is_snapshotted_at_record_time(
    psvc: PaymentService, subs: SubscriptionService, clock: FakeClock
) -> None:
    """The payment keeps s1's day_count (30) from when it was recorded; a later PUT
    changing the period does not alter it.
    """
    res = await psvc.record(CMD)
    await subs.upsert("s1", "u1", 60, clock.now() + 10 * D)
    assert (await psvc.get_status(res.view.payment_id)).payment.period_days == 30


async def test_outbox_failure_rolls_back_payment(
    psvc: PaymentService, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(OutboxRepository, "add", AsyncMock(side_effect=RuntimeError("db down")))
    with pytest.raises(RuntimeError):
        await psvc.record(CMD)
    monkeypatch.undo()
    with pytest.raises(NotFoundError):
        await psvc.get_status_by_provider("pp-1")


async def test_identical_duplicate_returns_existing(
    psvc: PaymentService, uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    first = await psvc.record(CMD)
    second = await psvc.record(CMD)
    assert second.created is False
    assert second.view == first.view
    assert await _outbox_count(uow_factory, clock) == 1


async def test_duplicate_with_equal_amount_other_scale_is_identical(psvc: PaymentService) -> None:
    await psvc.record(CMD)
    assert (await psvc.record(replace(CMD, amount=Decimal("10")))).created is False


@pytest.mark.parametrize(
    "change",
    [
        {"amount": Decimal("11")},
        {"currency": "EUR"},
        {"status": PaymentStatus.FAILED},
    ],
)
async def test_duplicate_with_different_payload_conflicts(
    psvc: PaymentService, change: dict[str, object]
) -> None:
    await psvc.record(CMD)
    with pytest.raises(ConflictError):
        await psvc.record(replace(CMD, **change))  # type: ignore


async def test_duplicate_for_other_subscription_conflicts(
    psvc: PaymentService, subs: SubscriptionService, clock: FakeClock
) -> None:
    await subs.upsert("s2", "u1", 30, clock.now() + 10 * D)
    await psvc.record(CMD)
    with pytest.raises(ConflictError):
        await psvc.record(replace(CMD, subscription_id="s2"))


async def test_concurrent_duplicates_single_payment(
    psvc: PaymentService, uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    results = await asyncio.gather(psvc.record(CMD), psvc.record(CMD))
    assert sorted(r.created for r in results) == [False, True]
    assert results[0].view.payment_id == results[1].view.payment_id
    assert await _outbox_count(uow_factory, clock) == 1


async def test_unknown_subscription_not_found(psvc: PaymentService) -> None:
    with pytest.raises(NotFoundError):
        await psvc.record(replace(CMD, subscription_id="nope"))


async def test_get_status_unknown_not_found(psvc: PaymentService) -> None:
    with pytest.raises(NotFoundError):
        await psvc.get_status(uuid.uuid4())
    with pytest.raises(NotFoundError):
        await psvc.get_status_by_provider("nope")


async def test_get_status_reports_notification(
    psvc: PaymentService, uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    res = await psvc.record(CMD)
    async with uow_factory() as uow:
        await uow.notifications.insert_payment_notification(
            make_notification(event_name="payment.succeeded", payment_id=res.view.payment_id)
        )
    status = await psvc.get_status_by_provider("pp-1")
    assert status.payment.payment_id == res.view.payment_id
    assert status.notification is not None
    assert (status.notification.status, status.notification.sent_at) == ("scheduled", None)
