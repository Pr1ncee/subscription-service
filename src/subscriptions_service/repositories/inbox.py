import uuid
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import ProcessedEventRow


class InboxRepository:
    """`processed_events`: which consumer has already handled which event (idempotency)."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def try_mark_processed(self, consumer: str, event_id: uuid.UUID, at: datetime) -> bool:
        """Record that `consumer` handled `event_id`; False if it already had.

        Called first in the consumer's transaction, so the inbox row commits or rolls back
        together with the side effects and a redelivered event is skipped.
        """
        stmt = (
            insert(ProcessedEventRow)
            .values(consumer=consumer, event_id=event_id, processed_at=at)
            .on_conflict_do_nothing()
            .returning(ProcessedEventRow.event_id)
        )
        return (await self._session.execute(stmt)).scalar_one_or_none() is not None
