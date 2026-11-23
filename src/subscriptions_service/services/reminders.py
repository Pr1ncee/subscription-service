from collections.abc import Sequence
from datetime import datetime

from subscriptions_service.db.tables import NotificationRow, SubscriptionRow
from subscriptions_service.domain.enums import NotificationStatus
from subscriptions_service.domain.schedule import reminder_slots
from subscriptions_service.repositories.uow import UnitOfWork


async def reschedule_reminders(
    uow: UnitOfWork, subscription: SubscriptionRow, offsets_days: Sequence[int], now: datetime
) -> None:
    """Cancel still-scheduled reminders and schedule fresh ones for the current expiry.

    The caller must hold the subscription row lock (lock order: subscription → notification).
    """
    await uow.notifications.cancel_scheduled_reminders(subscription.subscription_id, now)
    await uow.notifications.add_many(
        [
            NotificationRow(
                subscription_id=subscription.subscription_id,
                event_name=slot.event.value,
                scheduled_for=slot.scheduled_for,
                target_expires_on=subscription.expected_expires_on,
                status=NotificationStatus.SCHEDULED.value,
                created_at=now,
                updated_at=now,
            )
            for slot in reminder_slots(subscription.expected_expires_on, offsets_days, now)
        ]
    )
