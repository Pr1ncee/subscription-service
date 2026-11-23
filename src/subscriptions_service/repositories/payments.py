import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import PaymentRow
from subscriptions_service.repositories._rows import insert_values


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, row: PaymentRow) -> PaymentRow | None:
        stmt = (
            insert(PaymentRow)
            .values(insert_values(row))
            .on_conflict_do_nothing(index_elements=[PaymentRow.provider_payment])
            .returning(PaymentRow)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: uuid.UUID) -> PaymentRow | None:
        return await self._session.get(PaymentRow, payment_id)

    async def get_by_provider(self, provider_payment: str) -> PaymentRow | None:
        stmt = select(PaymentRow).where(PaymentRow.provider_payment == provider_payment)
        return (await self._session.scalars(stmt)).one_or_none()
