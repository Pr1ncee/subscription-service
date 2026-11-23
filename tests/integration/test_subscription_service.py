import asyncio
from collections.abc import Sequence
from datetime import timedelta

import pytest

from subscriptions_service.domain.enums import NotificationStatus
from subscriptions_service.domain.errors import ConflictError, NotFoundError
from subscriptions_service.services.dto import NotificationView
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.fakes import FakeClock

D = timedelta(days=1)


@pytest.fixture
def svc(subs: SubscriptionService) -> SubscriptionService:
    return subs


def _statuses(notifications: Sequence[NotificationView]) -> list[str]:
    return sorted(n.status.value for n in notifications)


async def test_create_schedules_three_reminders(svc: SubscriptionService, clock: FakeClock) -> None:
    exp = clock.now() + 10 * D
    res = await svc.upsert("s1", "u1", 30, exp)
    assert res.created is True
    assert (res.view.subscription_id, res.view.user_id, res.view.day_count) == ("s1", "u1", 30)
    assert res.view.expected_expires_on == exp
    assert [
        (n.event_name, n.scheduled_for, n.status, n.sent_at) for n in res.view.notifications
    ] == [
        ("subscription.expiring", exp - 3 * D, NotificationStatus.SCHEDULED, None),
        ("subscription.expiring", exp - D, NotificationStatus.SCHEDULED, None),
        ("subscription.expired", exp, NotificationStatus.SCHEDULED, None),
    ]
    assert await svc.get("s1") == res.view


async def test_later_expiry_cancels_and_reschedules(
    svc: SubscriptionService, clock: FakeClock
) -> None:
    exp = clock.now() + 10 * D
    first = await svc.upsert("s1", "u1", 30, exp)
    res = await svc.upsert("s1", "u1", 30, exp + 5 * D)
    assert res.created is False
    assert res.view.expected_expires_on == exp + 5 * D
    assert _statuses(res.view.notifications) == ["cancelled"] * 3 + ["scheduled"] * 3
    old_ids = {n.id for n in first.view.notifications}
    cancelled = {n.id for n in res.view.notifications if n.status == "cancelled"}
    assert cancelled == old_ids
    scheduled = [n.scheduled_for for n in res.view.notifications if n.status == "scheduled"]
    assert scheduled == [exp + 2 * D, exp + 4 * D, exp + 5 * D]


async def test_earlier_expiry_is_ignored_but_day_count_updates(
    svc: SubscriptionService, clock: FakeClock
) -> None:
    exp = clock.now() + 10 * D
    await svc.upsert("s1", "u1", 30, exp)
    res = await svc.upsert("s1", "u1", 60, exp - 5 * D)
    assert res.view.expected_expires_on == exp
    assert res.view.day_count == 60
    assert _statuses(res.view.notifications) == ["scheduled"] * 3


async def test_same_expiry_leaves_reminders_untouched(
    svc: SubscriptionService, clock: FakeClock
) -> None:
    exp = clock.now() + 10 * D
    first = await svc.upsert("s1", "u1", 30, exp)
    second = await svc.upsert("s1", "u1", 30, exp)
    assert [n.id for n in second.view.notifications] == [n.id for n in first.view.notifications]
    assert _statuses(second.view.notifications) == ["scheduled"] * 3


async def test_user_mismatch_conflicts(svc: SubscriptionService, clock: FakeClock) -> None:
    await svc.upsert("s1", "u1", 30, clock.now() + 10 * D)
    with pytest.raises(ConflictError):
        await svc.upsert("s1", "u2", 30, clock.now() + 20 * D)
    assert (await svc.get("s1")).user_id == "u1"


async def test_past_expiry_saves_without_reminders(
    svc: SubscriptionService, clock: FakeClock
) -> None:
    res = await svc.upsert("s1", "u1", 30, clock.now() - D)
    assert res.created and res.view.notifications == ()
    assert (await svc.get("s1")).expected_expires_on == clock.now() - D


async def test_get_unknown_raises_not_found(svc: SubscriptionService) -> None:
    with pytest.raises(NotFoundError):
        await svc.get("nope")


async def test_concurrent_creates_produce_one_subscription(
    svc: SubscriptionService, clock: FakeClock
) -> None:
    results = await asyncio.gather(
        *(svc.upsert("s1", "u1", 30, clock.now() + 10 * D) for _ in range(2))
    )
    assert sorted(r.created for r in results) == [False, True]
    assert len((await svc.get("s1")).notifications) == 3
