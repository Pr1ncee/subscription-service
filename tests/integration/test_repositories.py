import uuid
from datetime import timedelta

import pytest

from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from tests.factories import make_notification, make_payment, make_subscription
from tests.fakes import FakeClock

D = timedelta(days=1)


async def test_uow_commits_on_clean_exit(uow_factory: UnitOfWorkFactory) -> None:
    async with uow_factory() as uow:
        assert await uow.subscriptions.insert_if_absent(make_subscription("s1")) is True
    async with uow_factory() as uow:
        assert await uow.subscriptions.get("s1") is not None


async def test_uow_rolls_back_on_exception(uow_factory: UnitOfWorkFactory) -> None:
    with pytest.raises(RuntimeError):
        async with uow_factory() as uow:
            await uow.subscriptions.insert_if_absent(make_subscription("s1"))
            raise RuntimeError
    async with uow_factory() as uow:
        assert await uow.subscriptions.get("s1") is None


async def test_subscription_insert_if_absent_false_on_duplicate(
    uow_factory: UnitOfWorkFactory,
) -> None:
    async with uow_factory() as uow:
        assert await uow.subscriptions.insert_if_absent(make_subscription("s1")) is True
        assert await uow.subscriptions.insert_if_absent(make_subscription("s1", "u2")) is False
        row = await uow.subscriptions.get_for_update("s1")
        assert row is not None and row.user_id == "u1"


async def test_payment_insert_if_absent_returns_none_on_duplicate(
    uow_factory: UnitOfWorkFactory,
) -> None:
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        first = await uow.payments.insert_if_absent(make_payment(provider_payment="pp-1"))
        assert first is not None and first.created_at is not None
        assert await uow.payments.insert_if_absent(make_payment(provider_payment="pp-1")) is None
        existing = await uow.payments.get_by_provider("pp-1")
        assert existing is not None and existing.id == first.id
        assert await uow.payments.get(first.id) is not None


async def test_insert_payment_notification_is_idempotent(uow_factory: UnitOfWorkFactory) -> None:
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        payment = await uow.payments.insert_if_absent(make_payment())
        assert payment is not None
        note = make_notification(event_name="payment.succeeded", payment_id=payment.id)
        assert await uow.notifications.insert_payment_notification(note) is True
        again = make_notification(event_name="payment.succeeded", payment_id=payment.id)
        assert await uow.notifications.insert_payment_notification(again) is False
        found = await uow.notifications.get_by_payment(payment.id)
        assert found is not None and found.id == note.id


async def test_lock_due_skips_rows_locked_by_another_tx(
    uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    """`lock_due` takes the earliest due row first, ignores rows not yet due, and
    skips rows another transaction has locked.
    """
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        await uow.notifications.add_many(
            [
                make_notification(scheduled_for=clock.now() - D),
                make_notification(scheduled_for=clock.now() - 2 * D),
                make_notification(scheduled_for=clock.now() + D),
            ]
        )
    async with uow_factory() as a:
        locked_a = await a.notifications.lock_due(clock.now(), limit=1)
        assert len(locked_a) == 1
        assert locked_a[0].scheduled_for == clock.now() - 2 * D
        async with uow_factory() as b:
            locked_b = await b.notifications.lock_due(clock.now(), limit=10)
            assert [n.id for n in locked_b] != [locked_a[0].id]
            assert len(locked_b) == 1


async def test_cancel_scheduled_reminders_ignores_payment_and_queued(
    uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        payment = await uow.payments.insert_if_absent(make_payment())
        assert payment is not None
        await uow.notifications.add_many(
            [
                make_notification(status="scheduled"),
                make_notification(status="queued", scheduled_for=clock.now() + D),
                make_notification(event_name="payment.succeeded", payment_id=payment.id),
            ]
        )
    async with uow_factory() as uow:
        assert await uow.notifications.cancel_scheduled_reminders("s1", clock.now()) == 1
    async with uow_factory() as uow:
        rows = await uow.notifications.list_for_subscription("s1")
        assert sorted(r.status for r in rows) == ["cancelled", "queued", "scheduled"]
        assert [r.scheduled_for for r in rows] == sorted(r.scheduled_for for r in rows)


async def test_outbox_lock_pending_ignores_future_next_attempt_and_published(
    uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    envs = [EventEnvelope.new(EventType.NOTIFICATION_DUE, {"n": i}, clock.now()) for i in range(3)]
    async with uow_factory() as uow:
        for env in envs:
            await uow.outbox.add(env, "notification", str(uuid.uuid4()))
    async with uow_factory() as uow:
        rows = {r.id: r for r in await uow.outbox.lock_pending(clock.now(), limit=10)}
        assert set(rows) == {e.event_id for e in envs}
        await uow.outbox.mark_published(rows[envs[0].event_id], clock.now())
        await uow.outbox.mark_failed(rows[envs[1].event_id], "boom", clock.now() + D)
    async with uow_factory() as uow:
        pending = await uow.outbox.lock_pending(clock.now(), limit=10)
        assert [r.id for r in pending] == [envs[2].event_id]
        later = await uow.outbox.lock_pending(clock.now() + D, limit=10)
        assert {r.id for r in later} == {envs[1].event_id, envs[2].event_id}
        failed = next(r for r in later if r.id == envs[1].event_id)
        assert (failed.attempts, failed.last_error) == (1, "boom")


async def test_outbox_get_for_aggregate(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> None:
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"a": 1}, clock.now())
    async with uow_factory() as uow:
        await uow.outbox.add(env, "payment", "p-1")
    async with uow_factory() as uow:
        row = await uow.outbox.get_for_aggregate("payment", "p-1")
        assert row is not None
        assert (row.id, row.event_type, row.payload, row.attempts) == (
            env.event_id,
            "payment.succeeded",
            {"a": 1},
            0,
        )
        assert await uow.outbox.get_for_aggregate("payment", "other") is None


async def test_inbox_try_mark_processed_true_then_false(
    uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    event_id = uuid.uuid4()
    async with uow_factory() as uow:
        assert await uow.inbox.try_mark_processed("c1", event_id, clock.now()) is True
    async with uow_factory() as uow:
        assert await uow.inbox.try_mark_processed("c1", event_id, clock.now()) is False
        assert await uow.inbox.try_mark_processed("c2", event_id, clock.now()) is True


async def test_locking_lookups_return_rows_or_none(uow_factory: UnitOfWorkFactory) -> None:
    note = make_notification()
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        await uow.notifications.add_many([note])
    async with uow_factory() as uow:
        sub = await uow.subscriptions.get_for_share("s1")
        assert sub is not None and sub.subscription_id == "s1"
        assert await uow.subscriptions.get_for_share("nope") is None
        locked = await uow.notifications.get_for_update(note.id)
        assert locked is not None and locked.id == note.id
        assert await uow.notifications.get_for_update(uuid.uuid4()) is None
