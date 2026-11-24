import uuid
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import NotificationRow
from subscriptions_service.domain.enums import NotificationStatus
from subscriptions_service.repositories._rows import insert_values


class NotificationRepository:
    """Queries and updates on `notifications`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list_for_subscription(self, subscription_id: str) -> list[NotificationRow]:
        """All notifications of a subscription, by `scheduled_for` then `created_at`, reloaded."""
        stmt = (
            select(NotificationRow)
            .where(NotificationRow.subscription_id == subscription_id)
            .order_by(NotificationRow.scheduled_for, NotificationRow.created_at)
            .execution_options(populate_existing=True)
        )
        return list(await self._session.scalars(stmt))

    async def add_many(self, rows: Sequence[NotificationRow]) -> None:
        """Insert the rows and flush."""
        self._session.add_all(rows)
        await self._session.flush()

    async def cancel_scheduled_reminders(self, subscription_id: str, now: datetime) -> int:
        """Cancel the subscription's still-`scheduled` reminders; returns how many.

        Payment-result notifications (`payment_id` set) are left alone.
        """
        stmt = (
            update(NotificationRow)
            .where(
                NotificationRow.subscription_id == subscription_id,
                NotificationRow.status == NotificationStatus.SCHEDULED,
                NotificationRow.payment_id.is_(None),
            )
            .values(status=NotificationStatus.CANCELLED, updated_at=now)
            .execution_options(synchronize_session=False)
        )
        result = await self._session.execute(stmt)
        return int(result.rowcount)  # type: ignore[attr-defined]

    async def insert_payment_notification(self, row: NotificationRow) -> bool:
        """Insert a payment-result notification; False if the payment already has one."""
        stmt = (
            insert(NotificationRow)
            .values(insert_values(row))
            .on_conflict_do_nothing(
                index_elements=[NotificationRow.payment_id],
                index_where=text("payment_id IS NOT NULL"),
            )
            .returning(NotificationRow.id)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def lock_due(self, now: datetime, limit: int) -> list[NotificationRow]:
        """Lock up to `limit` `scheduled` notifications due by `now`, earliest first.

        `FOR UPDATE SKIP LOCKED`: concurrent schedulers take disjoint batches.
        """
        stmt = (
            select(NotificationRow)
            .where(
                NotificationRow.status == NotificationStatus.SCHEDULED,
                NotificationRow.scheduled_for <= now,
            )
            .order_by(NotificationRow.scheduled_for)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(stmt))

    async def get(self, notification_id: uuid.UUID) -> NotificationRow | None:
        """The notification by id, without locking."""
        return await self._session.get(NotificationRow, notification_id)

    async def get_for_update(self, notification_id: uuid.UUID) -> NotificationRow | None:
        """The notification by id, locked `FOR UPDATE` and reloaded from the database."""
        stmt = (
            select(NotificationRow)
            .where(NotificationRow.id == notification_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get_by_payment(self, payment_id: uuid.UUID) -> NotificationRow | None:
        """The result notification of a payment, if it was created yet."""
        stmt = select(NotificationRow).where(NotificationRow.payment_id == payment_id)
        return (await self._session.scalars(stmt)).one_or_none()
