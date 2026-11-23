from datetime import UTC, datetime, timedelta

from subscriptions_service.domain.enums import NotificationEvent
from subscriptions_service.domain.schedule import (
    ReminderSlot,
    extend_expiry,
    merge_expiry,
    reminder_slots,
)

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
D = timedelta(days=1)


def test_reminder_slots_default_offsets() -> None:
    exp = NOW + 10 * D
    assert reminder_slots(exp, [3, 1, 0], NOW) == [
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRING, exp - 3 * D),
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRING, exp - D),
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRED, exp),
    ]


def test_reminder_slots_sorted_regardless_of_offset_order() -> None:
    exp = NOW + 10 * D
    slots = reminder_slots(exp, [0, 3, 1], NOW)
    assert [s.scheduled_for for s in slots] == [exp - 3 * D, exp - D, exp]


def test_reminder_slots_skip_past() -> None:
    slots = reminder_slots(NOW + 2 * D, [3, 1, 0], NOW)
    assert [s.scheduled_for for s in slots] == [NOW + D, NOW + 2 * D]


def test_reminder_slot_exactly_now_is_skipped() -> None:
    assert len(reminder_slots(NOW + 3 * D, [3, 1, 0], NOW)) == 2


def test_reminder_slots_all_past_is_empty() -> None:
    assert reminder_slots(NOW - timedelta(hours=1), [3, 1, 0], NOW) == []


def test_merge_expiry_never_moves_backwards() -> None:
    assert merge_expiry(NOW + 5 * D, NOW + D) == NOW + 5 * D
    assert merge_expiry(NOW + D, NOW + 5 * D) == NOW + 5 * D


def test_extend_expiry_from_future_expiry() -> None:
    assert extend_expiry(NOW + 5 * D, 30, NOW) == NOW + 35 * D


def test_extend_expiry_when_lapsed_starts_from_now() -> None:
    assert extend_expiry(NOW - 10 * D, 30, NOW) == NOW + 30 * D
