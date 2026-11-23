from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from subscriptions_service.domain.enums import NotificationEvent


@dataclass(frozen=True, slots=True)
class ReminderSlot:
    event: NotificationEvent
    scheduled_for: datetime


def reminder_slots(
    expires_on: datetime, offsets_days: Sequence[int], now: datetime
) -> list[ReminderSlot]:
    """Reminder slots before `expires_on`, strictly after `now`, earliest first.

    Offset 0 is the expiry itself (`subscription.expired`); any other offset is
    an advance warning (`subscription.expiring`).
    """
    slots = [
        ReminderSlot(
            (
                NotificationEvent.SUBSCRIPTION_EXPIRED
                if offset == 0
                else NotificationEvent.SUBSCRIPTION_EXPIRING
            ),
            expires_on - timedelta(days=offset),
        )
        for offset in offsets_days
    ]
    return sorted((s for s in slots if s.scheduled_for > now), key=lambda s: s.scheduled_for)


def merge_expiry(current: datetime, requested: datetime) -> datetime:
    """Expiry never moves backwards."""
    return max(current, requested)


def extend_expiry(current: datetime, day_count: int, now: datetime) -> datetime:
    """A paid period starts at the current expiry, or now if it has already lapsed."""
    return max(current, now) + timedelta(days=day_count)
