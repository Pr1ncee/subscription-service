import asyncio
import logging
import uuid
from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest

from subscriptions_service.domain.enums import EventType, PaymentStatus
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.consumers import (
    DeadLetterHandler,
    NotificationDelivery,
    PaymentResultNotifier,
    SubscriptionExtender,
)
from subscriptions_service.services.dto import NotificationView, RecordPaymentResult
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.ports import OutgoingNotification, PermanentSendError
from subscriptions_service.services.scheduler import NotificationScheduler
from subscriptions_service.services.subscriptions import SubscriptionService
from tests.factories import CMD
from tests.fakes import FakeClock, RecordingSender

D = timedelta(days=1)


# --- fixtures -------------------------------------------------------------------------


@pytest.fixture
def payments(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> PaymentService:
    return PaymentService(uow_factory, clock)


@pytest.fixture
def sched(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> NotificationScheduler:
    return NotificationScheduler(uow_factory, clock, batch_size=100)


@pytest.fixture
def notifier(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> PaymentResultNotifier:
    return PaymentResultNotifier(uow_factory, clock)


@pytest.fixture
def extender(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> SubscriptionExtender:
    return SubscriptionExtender(uow_factory, clock, [3, 1, 0])


@pytest.fixture
def sender() -> RecordingSender:
    return RecordingSender()


@pytest.fixture
def delivery(
    uow_factory: UnitOfWorkFactory, clock: FakeClock, sender: RecordingSender
) -> NotificationDelivery:
    return NotificationDelivery(uow_factory, clock, sender)


@pytest.fixture
def dead_letters(uow_factory: UnitOfWorkFactory, clock: FakeClock) -> DeadLetterHandler:
    return DeadLetterHandler(uow_factory, clock)


# --- helpers --------------------------------------------------------------------------


async def _envelope(
    uow_factory: UnitOfWorkFactory, aggregate: str, aggregate_id: str
) -> EventEnvelope:
    """The event exactly as the relay would publish it."""
    async with uow_factory() as uow:
        row = await uow.outbox.get_for_aggregate(aggregate, aggregate_id)
    assert row is not None
    return EventEnvelope(row.id, EventType(row.event_type), row.created_at, row.payload)


async def _payment_env(uow_factory: UnitOfWorkFactory, pay: RecordPaymentResult) -> EventEnvelope:
    return await _envelope(uow_factory, "payment", str(pay.view.payment_id))


async def _note(subs: SubscriptionService, note_id: uuid.UUID) -> NotificationView:
    return next(n for n in (await subs.get("s1")).notifications if n.id == note_id)


async def _queue_three_day_reminder(
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> tuple[uuid.UUID, EventEnvelope]:
    """s1 expires in 10 days; 7 days later the 3-day reminder is queued."""
    clock.advance(7 * D)
    assert await sched.run_once() == 1
    queued = [n for n in (await subs.get("s1")).notifications if n.status == "queued"]
    assert len(queued) == 1
    return queued[0].id, await _envelope(uow_factory, "notification", str(queued[0].id))


@pytest.fixture
async def s1(subs: SubscriptionService, clock: FakeClock) -> None:
    await subs.upsert("s1", "u1", 30, clock.now() + 10 * D)


# --- PaymentResultNotifier ------------------------------------------------------------


@pytest.mark.usefixtures("s1")
async def test_payment_notifier_creates_one_notification(
    notifier: PaymentResultNotifier,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    pay = await payments.record(CMD)
    env = await _payment_env(uow_factory, pay)
    await notifier.handle(env)
    await notifier.handle(env)
    notes = [n for n in (await subs.get("s1")).notifications if n.event_name == "payment.succeeded"]
    assert len(notes) == 1
    assert (notes[0].scheduled_for, notes[0].status) == (clock.now(), "scheduled")
    status = await payments.get_status(pay.view.payment_id)
    assert status.notification is not None and status.notification.status == "scheduled"


@pytest.mark.usefixtures("s1")
async def test_payment_notifier_failed_payment_event_name(
    notifier: PaymentResultNotifier,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
) -> None:
    pay = await payments.record(replace(CMD, status=PaymentStatus.FAILED))
    await notifier.handle(await _payment_env(uow_factory, pay))
    names = [n.event_name for n in (await subs.get("s1")).notifications]
    assert names.count("payment.failed") == 1


# --- SubscriptionExtender -------------------------------------------------------------


@pytest.mark.usefixtures("s1")
async def test_extender_applies_once_per_payment(
    extender: SubscriptionExtender,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    pay = await payments.record(CMD)
    env = await _payment_env(uow_factory, pay)
    await extender.handle(env)
    await extender.handle(env)
    view = await subs.get("s1")
    assert view.expected_expires_on == clock.now() + 40 * D
    assert Counter(str(n.status) for n in view.notifications) == {"cancelled": 3, "scheduled": 3}
    scheduled = [n.scheduled_for for n in view.notifications if n.status == "scheduled"]
    assert scheduled[-1] == clock.now() + 40 * D


@pytest.mark.usefixtures("s1")
async def test_extender_uses_period_from_event_not_current_day_count(
    extender: SubscriptionExtender,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    pay = await payments.record(CMD)  # period_days = 30
    await subs.upsert("s1", "u1", 60, clock.now() + 10 * D)  # later PUT changes the period
    await extender.handle(await _payment_env(uow_factory, pay))
    assert (await subs.get("s1")).expected_expires_on == clock.now() + 40 * D


async def test_extender_on_lapsed_subscription_starts_from_now(
    extender: SubscriptionExtender,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    await subs.upsert("s1", "u1", 30, clock.now() - 10 * D)
    pay = await payments.record(CMD)
    await extender.handle(await _payment_env(uow_factory, pay))
    assert (await subs.get("s1")).expected_expires_on == clock.now() + 30 * D


@pytest.mark.usefixtures("s1")
async def test_two_payments_extend_twice(
    extender: SubscriptionExtender,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    first = await payments.record(CMD)
    second = await payments.record(replace(CMD, provider_payment="pp-2"))
    await extender.handle(await _payment_env(uow_factory, first))
    await extender.handle(await _payment_env(uow_factory, second))
    assert (await subs.get("s1")).expected_expires_on == clock.now() + 70 * D


# --- NotificationDelivery -------------------------------------------------------------


@pytest.mark.usefixtures("s1")
async def test_delivery_sends_and_marks_sent(
    delivery: NotificationDelivery,
    sender: RecordingSender,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    clock.advance(timedelta(minutes=1))
    await delivery.handle(env)
    assert sender.sent == [
        OutgoingNotification(
            notification_id=note_id,
            user_id="u1",
            subscription_id="s1",
            event_name="subscription.expiring",
            expected_expires_on=clock.now() - timedelta(minutes=1) + 3 * D,
        )
    ]
    note = await _note(subs, note_id)
    assert (note.status, note.sent_at) == ("sent", clock.now())


@pytest.mark.usefixtures("s1")
async def test_delivery_duplicate_message_sends_once(
    delivery: NotificationDelivery,
    sender: RecordingSender,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    _, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    await delivery.handle(env)
    await delivery.handle(env)
    assert len(sender.sent) == 1


@pytest.mark.usefixtures("s1")
async def test_delivery_skips_stale_reminder_after_extension(  # Review Focus 1
    delivery: NotificationDelivery,
    extender: SubscriptionExtender,
    sender: RecordingSender,
    payments: PaymentService,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, due_env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    pay = await payments.record(CMD)  # the user pays after the reminder was queued
    await extender.handle(await _payment_env(uow_factory, pay))
    await delivery.handle(due_env)
    assert sender.sent == []
    assert (await _note(subs, note_id)).status == "cancelled"


@pytest.mark.usefixtures("s1")
async def test_delivery_sends_payment_notification_without_stale_check(
    delivery: NotificationDelivery,
    notifier: PaymentResultNotifier,
    extender: SubscriptionExtender,
    sender: RecordingSender,
    payments: PaymentService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
) -> None:
    pay = await payments.record(CMD)
    env = await _payment_env(uow_factory, pay)
    await notifier.handle(env)
    await extender.handle(env)  # expiry changes; payment notifications are not reminders
    assert await sched.run_once() == 1
    status = await payments.get_status(pay.view.payment_id)
    async with uow_factory() as uow:
        note = await uow.notifications.get_by_payment(pay.view.payment_id)
    assert note is not None
    await delivery.handle(await _envelope(uow_factory, "notification", str(note.id)))
    assert [m.event_name for m in sender.sent] == ["payment.succeeded"]
    assert status.notification is not None


@pytest.mark.usefixtures("s1")
async def test_delivery_skips_cancelled_notification(
    delivery: NotificationDelivery,
    sender: RecordingSender,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    async with uow_factory() as uow:
        row = await uow.notifications.get_for_update(note_id)
        assert row is not None
        row.status = "cancelled"
    await delivery.handle(env)
    assert sender.sent == []
    assert (await _note(subs, note_id)).status == "cancelled"


async def test_delivery_unknown_notification_is_ignored(
    delivery: NotificationDelivery, sender: RecordingSender, clock: FakeClock
) -> None:
    env = EventEnvelope.new(
        EventType.NOTIFICATION_DUE, {"notification_id": str(uuid.uuid4())}, clock.now()
    )
    await delivery.handle(env)
    assert sender.sent == []


@pytest.mark.usefixtures("s1")
async def test_delivery_permanent_error_marks_failed(
    delivery: NotificationDelivery,
    sender: RecordingSender,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    sender.error = PermanentSendError("bad address")
    await delivery.handle(env)  # no raise: the message is done
    async with uow_factory() as uow:
        row = await uow.notifications.get_for_update(note_id)
    assert row is not None and (row.status, row.last_error) == ("failed", "bad address")


@pytest.mark.usefixtures("s1")
async def test_delivery_transient_error_rolls_back_and_retries(  # Review Focus 5
    delivery: NotificationDelivery,
    sender: RecordingSender,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    sender.error = RuntimeError("smtp timeout")
    with pytest.raises(RuntimeError):
        await delivery.handle(env)
    assert (await _note(subs, note_id)).status == "queued"
    sender.error = None
    await delivery.handle(env)  # redelivery: the inbox row was rolled back too
    assert (await _note(subs, note_id)).status == "sent"
    assert len(sender.sent) == 1


class BlockingSender:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.sent: list[OutgoingNotification] = []

    async def send(self, message: OutgoingNotification) -> None:
        self.entered.set()
        await self.release.wait()
        self.sent.append(message)


@pytest.mark.usefixtures("s1")
async def test_extension_waits_for_in_flight_delivery(  # interleaved race (spec §7.5)
    extender: SubscriptionExtender,
    payments: PaymentService,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    pay = await payments.record(CMD)
    payment_env = await _payment_env(uow_factory, pay)
    note_id, due_env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    expiry_before = (await subs.get("s1")).expected_expires_on

    blocking = BlockingSender()
    delivery = NotificationDelivery(uow_factory, clock, blocking)
    t_deliver = asyncio.create_task(delivery.handle(due_env))
    await asyncio.wait_for(blocking.entered.wait(), 5)  # delivery holds subscription FOR SHARE
    t_extend = asyncio.create_task(extender.handle(payment_env))
    await asyncio.sleep(0.3)
    assert not t_extend.done()  # extension blocked on the subscription lock
    blocking.release.set()
    await asyncio.wait_for(asyncio.gather(t_deliver, t_extend), 5)

    view = await subs.get("s1")
    assert (await _note(subs, note_id)).status == "sent"  # it was valid when sent
    assert len(blocking.sent) == 1
    assert view.expected_expires_on == expiry_before + 30 * D
    scheduled = [n.scheduled_for for n in view.notifications if n.status == "scheduled"]
    assert scheduled == [view.expected_expires_on - o * D for o in (3, 1, 0)]


# --- DeadLetterHandler ----------------------------------------------------------------


@pytest.mark.usefixtures("s1")
async def test_dead_letter_marks_queued_notification_failed(
    dead_letters: DeadLetterHandler,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    await dead_letters.handle(env, "delivery_limit")
    async with uow_factory() as uow:
        row = await uow.notifications.get_for_update(note_id)
    assert row is not None and row.status == "failed"
    assert row.last_error == "dead-lettered: delivery_limit"


@pytest.mark.usefixtures("s1")
async def test_dead_letter_leaves_sent_notification(
    dead_letters: DeadLetterHandler,
    delivery: NotificationDelivery,
    subs: SubscriptionService,
    sched: NotificationScheduler,
    uow_factory: UnitOfWorkFactory,
    clock: FakeClock,
) -> None:
    note_id, env = await _queue_three_day_reminder(subs, sched, uow_factory, clock)
    await delivery.handle(env)
    await dead_letters.handle(env, "delivery_limit")
    assert (await _note(subs, note_id)).status == "sent"


@pytest.mark.usefixtures("s1")
async def test_dead_letter_payment_event_only_logs(
    dead_letters: DeadLetterHandler,
    payments: PaymentService,
    subs: SubscriptionService,
    uow_factory: UnitOfWorkFactory,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pay = await payments.record(CMD)
    before = await subs.get("s1")
    with caplog.at_level(logging.ERROR):
        await dead_letters.handle(await _payment_env(uow_factory, pay), "rejected")
    assert any(
        r.levelno == logging.ERROR and "dead-lettered" in r.getMessage() for r in caplog.records
    )
    assert await subs.get("s1") == before
