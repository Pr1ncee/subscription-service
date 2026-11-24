import asyncio
from datetime import timedelta

import pytest

from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.scheduler import NotificationScheduler
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.factories import make_notification, make_subscription
from tests.fakes import FakeClock

D = timedelta(days=1)
FAR_FUTURE = 3650 * D


@pytest.fixture
def sched(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> NotificationScheduler:
    return NotificationScheduler(uow_factory, clock, batch_size=100)


async def _seed_due(uow_factory: UnitOfWorkFactory, clock: FakeClock, count: int) -> None:
    async with uow_factory() as uow:
        await uow.subscriptions.insert_if_absent(make_subscription("s1"))
        await uow.notifications.add_many(
            [
                make_notification(scheduled_for=clock.now() - timedelta(minutes=i))
                for i in range(count)
            ]
        )


async def _due_events(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> list[dict[str, object]]:
    async with uow_factory() as uow:
        rows = await uow.outbox.lock_pending(clock.now() + FAR_FUTURE, limit=1000)
    assert all(r.event_type == "notification.due" for r in rows)
    assert all(
        (r.aggregate_type, r.aggregate_id) == ("notification", r.payload["notification_id"])
        for r in rows
    )
    return [r.payload for r in rows]


async def test_queues_due_reminder_and_writes_outbox(
    sched: NotificationScheduler,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    """Once the 3-day reminder is due it is queued with an outbox event; a second run
    finds nothing left to queue.
    """
    await subs.upsert("s1", "u1", 30, clock.now() + 10 * D)
    clock.advance(7 * D)
    assert await sched.run_once() == 1

    view = await subs.get("s1")
    queued = [n for n in view.notifications if n.status == "queued"]
    assert len(queued) == 1
    assert queued[0].scheduled_for == clock.now()
    assert [n.status for n in view.notifications].count("scheduled") == 2
    assert await _due_events(uow_factory, clock) == [{"notification_id": str(queued[0].id)}]
    assert await sched.run_once() == 0


async def test_nothing_due_returns_zero(
    sched: NotificationScheduler,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    await subs.upsert("s1", "u1", 30, clock.now() + 10 * D)
    assert await sched.run_once() == 0
    assert await _due_events(uow_factory, clock) == []


async def test_respects_batch_size(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> None:
    await _seed_due(uow_factory, clock, 5)
    sched = NotificationScheduler(uow_factory, clock, batch_size=2)
    assert [await sched.run_once() for _ in range(4)] == [2, 2, 1, 0]


async def test_concurrent_schedulers_do_not_double_queue(
    uow_factory: UnitOfWorkFactory, clock: FakeClock
) -> None:
    await _seed_due(uow_factory, clock, 50)
    a = NotificationScheduler(uow_factory, clock, batch_size=30)
    b = NotificationScheduler(uow_factory, clock, batch_size=30)
    total = 0
    for _ in range(2):
        total += sum(await asyncio.gather(a.run_once(), b.run_once()))
    assert total == 50
    payloads = await _due_events(uow_factory, clock)
    assert len(payloads) == 50
    assert len({p["notification_id"] for p in payloads}) == 50
