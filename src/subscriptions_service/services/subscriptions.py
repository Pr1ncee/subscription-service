from collections.abc import Sequence
from datetime import datetime

from subscriptions_service.db.tables import NotificationRow, SubscriptionRow
from subscriptions_service.domain.enums import NotificationStatus
from subscriptions_service.domain.errors import ConflictError, NotFoundError
from subscriptions_service.domain.schedule import merge_expiry
from subscriptions_service.repositories.uow import UnitOfWork, UnitOfWorkFactory
from subscriptions_service.services.dto import NotificationView, SubscriptionView, UpsertResult
from subscriptions_service.services.ports import Clock
from subscriptions_service.services.reminders import reschedule_reminders


class SubscriptionService:
    def __init__(
        self, uow_factory: UnitOfWorkFactory, clock: Clock, reminder_offsets_days: Sequence[int]
    ) -> None:
        self._uow_factory = uow_factory
        self._clock = clock
        self._offsets = tuple(reminder_offsets_days)

    async def upsert(
        self, subscription_id: str, user_id: str, day_count: int, expected_expires_on: datetime
    ) -> UpsertResult:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            # Insert-first: a concurrent creator blocks on the primary key until we commit,
            # then sees our row and takes the update path.
            created = await uow.subscriptions.insert_if_absent(
                SubscriptionRow(
                    subscription_id=subscription_id,
                    user_id=user_id,
                    day_count=day_count,
                    expected_expires_on=expected_expires_on,
                    created_at=now,
                    updated_at=now,
                )
            )
            subscription = await uow.subscriptions.get_for_update(subscription_id)
            if subscription is None:  # pragma: no cover - the row was just inserted or existed
                raise NotFoundError(f"subscription {subscription_id!r} not found")

            if created:
                await reschedule_reminders(uow, subscription, self._offsets, now)
            else:
                if subscription.user_id != user_id:
                    raise ConflictError(
                        f"subscription {subscription_id!r} belongs to a different user"
                    )
                subscription.day_count = day_count
                subscription.updated_at = now
                new_expiry = merge_expiry(subscription.expected_expires_on, expected_expires_on)
                if new_expiry != subscription.expected_expires_on:
                    subscription.expected_expires_on = new_expiry
                    await reschedule_reminders(uow, subscription, self._offsets, now)

            view = await _view(uow, subscription)
        return UpsertResult(view=view, created=created)

    async def get(self, subscription_id: str) -> SubscriptionView:
        async with self._uow_factory() as uow:
            subscription = await uow.subscriptions.get(subscription_id)
            if subscription is None:
                raise NotFoundError(f"subscription {subscription_id!r} not found")
            return await _view(uow, subscription)


async def _view(uow: UnitOfWork, subscription: SubscriptionRow) -> SubscriptionView:
    notifications = await uow.notifications.list_for_subscription(subscription.subscription_id)
    return SubscriptionView(
        subscription_id=subscription.subscription_id,
        user_id=subscription.user_id,
        day_count=subscription.day_count,
        expected_expires_on=subscription.expected_expires_on,
        notifications=tuple(_notification_view(n) for n in notifications),
    )


def _notification_view(row: NotificationRow) -> NotificationView:
    return NotificationView(
        id=row.id,
        event_name=row.event_name,
        scheduled_for=row.scheduled_for,
        sent_at=row.sent_at,
        status=NotificationStatus(row.status),
    )
