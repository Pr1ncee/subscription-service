from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import SubscriptionRow
from subscriptions_service.repositories._rows import insert_values


class SubscriptionRepository:
    """Queries and inserts on `subscriptions`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, subscription_id: str) -> SubscriptionRow | None:
        """The subscription by id, without locking."""
        return await self._session.get(SubscriptionRow, subscription_id)

    async def get_for_update(self, subscription_id: str) -> SubscriptionRow | None:
        """The subscription locked `FOR UPDATE` (writers: upsert, extension), reloaded."""
        return await self._get_locked(subscription_id, read=False)

    async def get_for_share(self, subscription_id: str) -> SubscriptionRow | None:
        """The subscription locked `FOR SHARE`, reloaded.

        Delivery holds it while sending, so no extension can commit in between.
        """
        return await self._get_locked(subscription_id, read=True)

    async def insert_if_absent(self, row: SubscriptionRow) -> bool:
        """Insert the subscription; False if the id already exists.

        A concurrent insert of the same id blocks on the primary key until the first
        transaction ends.
        """
        stmt = (
            insert(SubscriptionRow)
            .values(insert_values(row))
            .on_conflict_do_nothing(index_elements=[SubscriptionRow.subscription_id])
            .returning(SubscriptionRow.subscription_id)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None

    async def _get_locked(self, subscription_id: str, *, read: bool) -> SubscriptionRow | None:
        stmt = (
            select(SubscriptionRow)
            .where(SubscriptionRow.subscription_id == subscription_id)
            .with_for_update(read=read)
            .execution_options(populate_existing=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()
