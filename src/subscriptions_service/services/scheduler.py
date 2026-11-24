from subscriptions_service.domain.enums import AggregateType, EventType, NotificationStatus
from subscriptions_service.domain.events import EventEnvelope
from subscriptions_service.repositories.uow import UnitOfWorkFactory
from subscriptions_service.services.ports import Clock


class NotificationScheduler:
    """Moves due notifications to `queued` and emits a `notification.due` outbox event for each."""

    def __init__(self, uow_factory: UnitOfWorkFactory, clock: Clock, batch_size: int) -> None:
        self._uow_factory = uow_factory
        self._clock = clock
        self._batch_size = batch_size

    async def run_once(self) -> int:
        """Queue one batch of due notifications; returns how many were queued.

        Rows are locked with `SKIP LOCKED`, so concurrent schedulers take disjoint batches.
        Each status change and its outbox event commit in one transaction.
        """
        now = self._clock.now()
        async with self._uow_factory() as uow:
            due = await uow.notifications.lock_due(now, self._batch_size)
            for notification in due:
                notification.status = NotificationStatus.QUEUED.value
                notification.updated_at = now
                await uow.outbox.add(
                    EventEnvelope.new(
                        EventType.NOTIFICATION_DUE, {"notification_id": str(notification.id)}, now
                    ),
                    AggregateType.NOTIFICATION,
                    str(notification.id),
                )
            return len(due)
