import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import httpx
import pytest
from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from subscriptions_service.adapters.rabbit_publisher import RabbitPublisher
from subscriptions_service.adapters.rabbit_topology import DLQ_NAME, EVENTS_EXCHANGE, QUEUE_SPECS
from subscriptions_service.config import Settings
from subscriptions_service.consumers import acking
from subscriptions_service.consumers.acking import Outcome
from subscriptions_service.consumers.app import create_app
from subscriptions_service.db.tables import ProcessedEventRow
from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.ports import NotificationSender
from subscriptions_service.services.relay import OutboxRelay
from subscriptions_service.services.scheduler import NotificationScheduler
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.factories import CMD
from tests.fakes import FakeClock, RecordingSender

D = timedelta(days=1)
EXTEND_QUEUE = "payments.extend-subscription"
SEND_QUEUE = "notifications.send"


async def wait_until(predicate: Callable[[], Awaitable[bool]], limit_s: float = 10) -> None:
    async with asyncio.timeout(limit_s):
        while not await predicate():  # noqa: ASYNC110 - polls DB / broker state
            await asyncio.sleep(0.05)


# --- settle spy ---------------------------------------------------------------------


@dataclass
class Settled:
    queue: str
    outcome: Outcome
    redelivered: bool
    delivery_count: Any


@dataclass
class SettleSpy:
    real: Callable[..., Awaitable[None]]
    records: list[Settled] = field(default_factory=list)
    real_acks: list[str] = field(default_factory=list)
    _drop_ack: set[str] = field(default_factory=set)

    def drop_first_ack(self, queue: str) -> None:
        self._drop_ack.add(queue)

    async def __call__(self, message: RabbitMessage, outcome: Outcome, *, queue: str) -> None:
        raw = message.raw_message
        self.records.append(
            Settled(queue, outcome, raw.redelivered, (raw.headers or {}).get("x-delivery-count"))
        )
        if outcome is Outcome.ACK and queue in self._drop_ack:
            self._drop_ack.discard(queue)  # simulate an ack lost after the commit
            return
        if outcome is Outcome.ACK:
            self.real_acks.append(queue)
        await self.real(message, outcome, queue=queue)

    def deliveries(self, queue: str) -> list[Settled]:
        return [r for r in self.records if r.queue == queue]

    def acked(self, queue: str) -> int:
        return self.real_acks.count(queue)


@pytest.fixture
def settle_spy(monkeypatch: pytest.MonkeyPatch) -> SettleSpy:
    spy = SettleSpy(acking.settle)
    monkeypatch.setattr(acking, "settle", spy)
    return spy


# --- app lifecycle ------------------------------------------------------------------


@dataclass
class StackFactory:
    pg_url: str
    rabbit_url: str
    clock: FakeClock
    session_factory: async_sessionmaker[AsyncSession]
    running: list[FastStream] = field(default_factory=list)

    async def start(
        self,
        *,
        delivery_limit: int = 5,
        sender: NotificationSender | None = None,
        session_factory: Any = None,
    ) -> FastStream:
        settings = Settings(
            database_url=self.pg_url,
            rabbitmq_url=self.rabbit_url,
            consumer_delivery_limit=delivery_limit,
        )
        app = create_app(
            settings,
            sender=sender or RecordingSender(),
            clock=self.clock,
            session_factory=session_factory or self.session_factory,
        )
        await app.start()  # real lifecycle: on_startup (connect + declare) → subscribers
        self.running.append(app)
        return app

    async def stop(self, app: FastStream) -> None:
        await app.stop()
        self.running.remove(app)


@pytest.fixture
async def stack_factory(
    pg_url: str,
    rabbit_url: str,
    rabbit_broker: RabbitBroker,  # requested first so queues are deleted after the apps stop
    clock: FakeClock,
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[StackFactory]:
    factory = StackFactory(pg_url, rabbit_url, clock, session_factory)
    yield factory
    for app in list(factory.running):
        await factory.stop(app)


@pytest.fixture
def relay(
    uow_factory: UnitOfWorkFactory, rabbit_broker: RabbitBroker, clock: FakeClock
) -> OutboxRelay:
    return OutboxRelay(
        uow_factory, RabbitPublisher(rabbit_broker, timeout_s=5), clock, 100, max_backoff_s=60
    )


@pytest.fixture
def sched(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> NotificationScheduler:
    return NotificationScheduler(uow_factory, clock, batch_size=100)


async def _queue_reminder(
    subs: SubscriptionService, sched: NotificationScheduler, relay: OutboxRelay, clock: FakeClock
) -> str:
    """s1 expires in 10 days; 7 days later its 3-day reminder is queued and published."""
    await subs.upsert("s1", "u1", 30, clock.now() + 10 * D)
    clock.advance(7 * D)
    assert await sched.run_once() == 1
    assert await relay.run_once() == 1
    return next(str(n.id) for n in (await subs.get("s1")).notifications if n.status == "queued")


async def _note_row(uow_factory: UnitOfWorkFactory, note_id: str) -> Any:
    import uuid

    async with uow_factory() as uow:
        return await uow.notifications.get(uuid.UUID(note_id))


# --- tests --------------------------------------------------------------------------


async def test_app_startup_declares_topology_on_fresh_broker(
    stack_factory: StackFactory, rabbit_management: str
) -> None:
    await stack_factory.start()

    async def all_queues_consumed() -> bool:
        async with httpx.AsyncClient(auth=("guest", "guest"), timeout=10) as client:
            queues = {
                q["name"]: q for q in (await client.get(f"{rabbit_management}/queues/%2F")).json()
            }
        names = [s.name for s in QUEUE_SPECS] + [DLQ_NAME]
        return all(n in queues and queues[n].get("consumers", 0) >= 1 for n in names)

    await wait_until(all_queues_consumed)


async def test_payment_flow_through_broker(
    stack_factory: StackFactory,
    psvc: PaymentService,
    subs: SubscriptionService,
    relay: OutboxRelay,
    sched: NotificationScheduler,
    clock: FakeClock,
) -> None:
    sender = RecordingSender()
    await stack_factory.start(sender=sender)
    pay = await psvc.record(CMD)  # s1 expires now+10D, period 30
    assert await relay.run_once() == 1

    async def extended() -> bool:
        return (await subs.get("s1")).expected_expires_on == clock.now() + 40 * D

    async def payment_notification_created() -> bool:
        return (await psvc.get_status(pay.view.payment_id)).notification is not None

    await wait_until(extended)
    await wait_until(payment_notification_created)
    assert await sched.run_once() == 1
    assert await relay.run_once() == 1

    async def sent() -> bool:
        note = (await psvc.get_status(pay.view.payment_id)).notification
        return note is not None and note.status == "sent"

    await wait_until(sent)
    assert [m.event_name for m in sender.sent] == ["payment.succeeded"]


async def test_malformed_message_goes_to_dlq(  # Review Focus 3
    stack_factory: StackFactory, rabbit_broker: RabbitBroker, settle_spy: SettleSpy
) -> None:
    await stack_factory.start()
    await rabbit_broker.publish(
        b"garbage", exchange=EVENTS_EXCHANGE, routing_key="payment.succeeded"
    )

    async def dead_lettered_and_recorded() -> bool:
        return settle_spy.acked(DLQ_NAME) >= 2  # routed to two queues → two dead letters

    await wait_until(dead_lettered_and_recorded)
    outcomes = {(r.queue, r.outcome) for r in settle_spy.records if r.queue != DLQ_NAME}
    assert outcomes == {
        ("payments.notify", Outcome.DEAD_LETTER),
        (EXTEND_QUEUE, Outcome.DEAD_LETTER),
    }


async def test_delivery_limit_dead_letters_and_marks_failed(  # Review Focus 2
    stack_factory: StackFactory,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    relay: OutboxRelay,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    sender = RecordingSender(RuntimeError("down"))
    await stack_factory.start(delivery_limit=2, sender=sender)
    note_id = await _queue_reminder(subs, sched, relay, clock)

    async def failed() -> bool:
        return (await _note_row(uow_factory, note_id)).status == "failed"

    await wait_until(failed, limit_s=20)
    row = await _note_row(uow_factory, note_id)
    assert sender.attempts >= 2
    assert row.last_error.startswith("dead-lettered")


class FlakySessionFactory:
    """Raises OperationalError for the first `failures` sessions, as if Postgres were down."""

    def __init__(self, real: async_sessionmaker[AsyncSession], failures: int) -> None:
        self._real = real
        self.failures_left = failures

    def __call__(self) -> AsyncSession:
        if self.failures_left > 0:
            self.failures_left -= 1
            raise OperationalError("connect", {}, ConnectionRefusedError("db down"))
        return self._real()


async def test_dlq_handler_keeps_dead_letter_while_db_down(
    stack_factory: StackFactory,
    session_factory: async_sessionmaker[AsyncSession],
    subs: SubscriptionService,
    sched: NotificationScheduler,
    relay: OutboxRelay,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
    settle_spy: SettleSpy,
) -> None:
    flaky = FlakySessionFactory(session_factory, failures=0)

    class ArmsOutageOnLastAttempt(RecordingSender):
        async def send(self, message: Any) -> None:
            self.attempts += 1
            if self.attempts == 2:  # delivery_limit=1 → 2nd attempt is the last one
                flaky.failures_left = 3  # Postgres "goes down" just as it is dead-lettered
            raise RuntimeError("down")

    await stack_factory.start(
        delivery_limit=1, sender=ArmsOutageOnLastAttempt(), session_factory=flaky
    )
    note_id = await _queue_reminder(subs, sched, relay, clock)

    async def failed() -> bool:
        return (await _note_row(uow_factory, note_id)).status == "failed"

    await wait_until(failed, limit_s=20)  # recorded after "recovery", not lost
    dlq = settle_spy.deliveries(DLQ_NAME)
    assert Outcome.RETRY in {r.outcome for r in dlq}  # the DB-down path was exercised
    assert dlq[-1].outcome is Outcome.ACK


async def test_retry_uses_reject_not_nack(
    stack_factory: StackFactory,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    relay: OutboxRelay,
    clock: FakeClock,
    settle_spy: SettleSpy,
) -> None:
    await stack_factory.start(delivery_limit=5, sender=RecordingSender(RuntimeError("down")))
    await _queue_reminder(subs, sched, relay, clock)

    async def redelivered_twice() -> bool:
        return len(settle_spy.deliveries(SEND_QUEUE)) >= 3

    await wait_until(redelivered_twice)
    deliveries = settle_spy.deliveries(SEND_QUEUE)
    assert {r.outcome for r in deliveries} == {Outcome.RETRY}
    # reject(requeue=True) counts toward x-delivery-limit on RabbitMQ 4.3; a requeued nack would not.
    assert deliveries[1].delivery_count is not None and int(deliveries[1].delivery_count) >= 1
    assert int(deliveries[2].delivery_count) > int(deliveries[1].delivery_count)


async def test_lost_ack_redelivery_has_no_second_side_effect(
    stack_factory: StackFactory,
    psvc: PaymentService,
    subs: SubscriptionService,
    relay: OutboxRelay,
    session_factory: async_sessionmaker[AsyncSession],
    clock: FakeClock,
    settle_spy: SettleSpy,
) -> None:
    pay = await psvc.record(CMD)  # s1 expires now+10D, period_days = 30
    settle_spy.drop_first_ack(EXTEND_QUEUE)  # the first ACK never reaches the broker
    app = await stack_factory.start()
    assert await relay.run_once() == 1

    async def extended() -> bool:
        return (await subs.get("s1")).expected_expires_on == clock.now() + 40 * D

    await wait_until(extended)  # extender committed
    await wait_until(lambda: _settled(settle_spy, EXTEND_QUEUE, 1))
    await stack_factory.stop(app)  # connection closes with the message unacked → requeued

    await stack_factory.start()  # a fresh connection receives the redelivery

    async def acked_once() -> bool:
        return settle_spy.acked(EXTEND_QUEUE) == 1

    await wait_until(acked_once)
    deliveries = settle_spy.deliveries(EXTEND_QUEUE)
    assert len(deliveries) == 2
    assert deliveries[-1].redelivered is True  # AMQP redelivered flag
    assert (await subs.get("s1")).expected_expires_on == clock.now() + 40 * D  # +30, not +60
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(ProcessedEventRow)
            .where(ProcessedEventRow.consumer == "subscription-extender")
        )
    assert count == 1
    assert pay.created


async def _settled(spy: SettleSpy, queue: str, count: int) -> bool:
    return len(spy.deliveries(queue)) >= count


async def test_db_blip_does_not_dead_letter_payment_extension(  # review C1
    stack_factory: StackFactory,
    session_factory: async_sessionmaker[AsyncSession],
    psvc: PaymentService,
    subs: SubscriptionService,
    relay: OutboxRelay,
    clock: FakeClock,
    settle_spy: SettleSpy,
) -> None:
    pay = await psvc.record(CMD)  # s1 expires now+10D, period 30
    flaky = FlakySessionFactory(session_factory, failures=12)  # more than 2 queues x 3 attempts
    await stack_factory.start(delivery_limit=2, session_factory=flaky)
    assert await relay.run_once() == 1

    async def extended() -> bool:
        return (await subs.get("s1")).expected_expires_on == clock.now() + 40 * D

    async def notified() -> bool:
        return (await psvc.get_status(pay.view.payment_id)).notification is not None

    await wait_until(extended, limit_s=30)
    await wait_until(notified, limit_s=30)
    assert flaky.failures_left == 0  # the outage really happened
    assert settle_spy.deliveries(DLQ_NAME) == []
    assert {r.outcome for r in settle_spy.records} == {Outcome.ACK}


async def _publish_raw(broker: RabbitBroker, body: bytes, routing_key: str) -> None:
    await broker.publish(body, exchange=EVENTS_EXCHANGE, routing_key=routing_key)


async def test_bad_payload_is_dead_lettered_once_and_acked_in_dlq(  # review I2
    stack_factory: StackFactory,
    rabbit_broker: RabbitBroker,
    settle_spy: SettleSpy,
    clock: FakeClock,
) -> None:
    await stack_factory.start()
    env = EventEnvelope.new(EventType.NOTIFICATION_DUE, {"notification_id": "nope"}, clock.now())
    await _publish_raw(rabbit_broker, env.to_json(), "notification.due")

    async def recorded() -> bool:
        return settle_spy.acked(DLQ_NAME) >= 1

    await wait_until(recorded)
    await asyncio.sleep(1.5)  # a looping DLQ handler would retry every second
    assert [r.outcome for r in settle_spy.deliveries(SEND_QUEUE)] == [Outcome.DEAD_LETTER]
    assert [r.outcome for r in settle_spy.deliveries(DLQ_NAME)] == [Outcome.ACK]


async def test_envelope_with_non_string_event_id_is_settled(  # review I3
    stack_factory: StackFactory, rabbit_broker: RabbitBroker, settle_spy: SettleSpy
) -> None:
    await stack_factory.start()
    body = (
        b'{"event_id": 123, "event_type": "notification.due",'
        b' "occurred_at": "2026-01-01T12:00:00+00:00", "payload": {}}'
    )
    await _publish_raw(rabbit_broker, body, "notification.due")

    async def dead_lettered_and_acked() -> bool:
        return settle_spy.acked(DLQ_NAME) >= 1

    await wait_until(dead_lettered_and_acked)
    assert [r.outcome for r in settle_spy.deliveries(SEND_QUEUE)] == [Outcome.DEAD_LETTER]


async def test_consumers_use_bounded_prefetch(  # review M5 (re-graded Important)
    stack_factory: StackFactory, rabbit_management: str
) -> None:
    await stack_factory.start()

    async def consumers_prefetch() -> set[int]:
        async with httpx.AsyncClient(auth=("guest", "guest"), timeout=10) as client:
            rows = (await client.get(f"{rabbit_management}/consumers/%2F")).json()
        return {row["prefetch_count"] for row in rows}

    async def registered() -> bool:
        return len(await consumers_prefetch()) > 0

    await wait_until(registered)
    assert await consumers_prefetch() == {10}
