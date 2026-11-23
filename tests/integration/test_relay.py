from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from subscriptions_service.domain.enums import EventType, PaymentStatus
from subscriptions_service.repositories.uow import UnitOfWork, UnitOfWorkFactory
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.ports import EventPublisher
from subscriptions_service.services.relay import OutboxRelay
from tests.factories import CMD
from tests.fakes import FailingPublisher, FakeClock, RecordingPublisher

RelayFactory = Callable[[EventPublisher], OutboxRelay]


@pytest.fixture
def relay_factory(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> RelayFactory:
    return lambda publisher: OutboxRelay(
        uow_factory, publisher, clock, batch_size=10, max_backoff_s=60
    )


async def test_publishes_pending_and_marks_published(
    relay_factory: RelayFactory, psvc: PaymentService, clock: FakeClock
) -> None:
    pay = await psvc.record(CMD)
    pub = RecordingPublisher()
    relay = relay_factory(pub)
    assert await relay.run_once() == 1
    env = pub.published[0]
    assert env.event_type == "payment.succeeded"
    assert env.occurred_at == clock.now()
    assert env.payload["payment_id"] == str(pay.view.payment_id)
    status = await psvc.get_status(pay.view.payment_id)
    assert status.event.published and status.event.attempts == 1
    assert status.event.published_at == clock.now()
    assert env.event_id == status.event.event_id
    assert await relay.run_once() == 0  # not republished


async def test_failure_records_attempt_and_backs_off(
    relay_factory: RelayFactory, psvc: PaymentService, clock: FakeClock
) -> None:
    pay = await psvc.record(CMD)
    assert await relay_factory(FailingPublisher()).run_once() == 0
    ev = (await psvc.get_status(pay.view.payment_id)).event
    assert (ev.published, ev.attempts) == (False, 1)
    assert ev.last_error is not None and "boom" in ev.last_error
    pub = RecordingPublisher()
    assert await relay_factory(pub).run_once() == 0  # not yet due (next_attempt_at = now + 0.5s)
    clock.advance(timedelta(seconds=1))
    assert await relay_factory(pub).run_once() == 1
    ev = (await psvc.get_status(pay.view.payment_id)).event
    assert (ev.published, ev.attempts) == (True, 2)


async def test_one_failing_event_does_not_block_others(
    relay_factory: RelayFactory, psvc: PaymentService
) -> None:
    failed = await psvc.record(replace(CMD, provider_payment="pp-f", status=PaymentStatus.FAILED))
    ok = await psvc.record(replace(CMD, provider_payment="pp-ok"))
    pub = FailingPublisher(fail_types={EventType.PAYMENT_FAILED})
    assert await relay_factory(pub).run_once() == 1
    assert [e.payload["provider_payment"] for e in pub.published] == ["pp-ok"]
    assert (await psvc.get_status(ok.view.payment_id)).event.published is True
    assert (await psvc.get_status(failed.view.payment_id)).event.published is False


async def test_publishes_in_created_at_order(
    relay_factory: RelayFactory, psvc: PaymentService, clock: FakeClock
) -> None:
    clock.advance(timedelta(seconds=2))
    await psvc.record(replace(CMD, provider_payment="pp-late"))
    clock.advance(timedelta(seconds=-1))
    await psvc.record(replace(CMD, provider_payment="pp-early"))
    clock.advance(timedelta(seconds=1))
    pub = RecordingPublisher()
    assert await relay_factory(pub).run_once() == 2
    assert [e.payload["provider_payment"] for e in pub.published] == ["pp-early", "pp-late"]


async def test_respects_batch_size(
    uow_factory: UnitOfWorkFactory, psvc: PaymentService, clock: FakeClock
) -> None:
    for i in range(3):
        await psvc.record(replace(CMD, provider_payment=f"pp-{i}"))
    pub = RecordingPublisher()
    relay = OutboxRelay(uow_factory, pub, clock, batch_size=2, max_backoff_s=60)
    assert [await relay.run_once() for _ in range(3)] == [2, 1, 0]


async def test_commit_failure_after_publish_republishes_same_event_id(
    session_factory: async_sessionmaker[AsyncSession], psvc: PaymentService, clock: FakeClock
) -> None:
    # crash window: broker confirmed, DB commit of published_at lost
    pay = await psvc.record(CMD)
    pub = RecordingPublisher()

    class CommitFails(UnitOfWork):
        async def commit(self) -> None:
            raise OperationalError("commit", {}, Exception("connection lost"))

    crashing = OutboxRelay(
        lambda: CommitFails(session_factory), pub, clock, batch_size=10, max_backoff_s=60
    )
    with pytest.raises(OperationalError):
        await crashing.run_once()
    assert len(pub.published) == 1  # it did reach the broker
    ev = (await psvc.get_status(pay.view.payment_id)).event
    assert (ev.published, ev.attempts) == (False, 0)  # marking rolled back; row still pending

    healthy = OutboxRelay(
        lambda: UnitOfWork(session_factory), pub, clock, batch_size=10, max_backoff_s=60
    )
    assert await healthy.run_once() == 1
    assert len(pub.published) == 2
    assert pub.published[0].event_id == pub.published[1].event_id == ev.event_id
    assert pub.published[0] == pub.published[1]  # identical envelope (same occurred_at, payload)
    assert (await psvc.get_status(pay.view.payment_id)).event.published is True
