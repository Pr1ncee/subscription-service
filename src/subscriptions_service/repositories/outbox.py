from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import OutboxRow
from subscriptions_service.domain.events import EventEnvelope


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, envelope: EventEnvelope, aggregate_type: str, aggregate_id: str) -> None:
        # Timestamps come from the envelope (i.e. the injected clock), not the DB's now(),
        # so the relay's `next_attempt_at <= now` check uses one time source.
        self._session.add(
            OutboxRow(
                id=envelope.event_id,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                event_type=envelope.event_type.value,
                payload=dict(envelope.payload),
                created_at=envelope.occurred_at,
                next_attempt_at=envelope.occurred_at,
                attempts=0,
            )
        )
        await self._session.flush()

    async def lock_pending(self, now: datetime, limit: int) -> list[OutboxRow]:
        stmt = (
            select(OutboxRow)
            .where(OutboxRow.published_at.is_(None), OutboxRow.next_attempt_at <= now)
            .order_by(OutboxRow.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(stmt))

    async def mark_published(self, row: OutboxRow, at: datetime) -> None:
        row.published_at = at
        row.attempts += 1
        await self._session.flush()

    async def mark_failed(self, row: OutboxRow, error: str, next_attempt_at: datetime) -> None:
        row.attempts += 1
        row.last_error = error
        row.next_attempt_at = next_attempt_at
        await self._session.flush()

    async def get_for_aggregate(self, aggregate_type: str, aggregate_id: str) -> OutboxRow | None:
        stmt = (
            select(OutboxRow)
            .where(OutboxRow.aggregate_type == aggregate_type)
            .where(OutboxRow.aggregate_id == aggregate_id)
            .order_by(OutboxRow.created_at)
            .limit(1)
        )
        return (await self._session.scalars(stmt)).one_or_none()
