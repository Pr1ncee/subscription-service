import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from subscriptions_service.db.tables import PaymentRow
from subscriptions_service.repositories._rows import insert_values


class PaymentRepository:
    """Queries and inserts on `payments`."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, row: PaymentRow) -> PaymentRow | None:
        """Insert the payment; None if `provider_payment` is already recorded.

        A concurrent insert of the same `provider_payment` blocks on the unique key until
        the first transaction ends.
        """
        stmt = (
            insert(PaymentRow)
            .values(insert_values(row))
            .on_conflict_do_nothing(index_elements=[PaymentRow.provider_payment])
            .returning(PaymentRow)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: uuid.UUID) -> PaymentRow | None:
        """The payment by id."""
        return await self._session.get(PaymentRow, payment_id)

    async def get_by_provider(self, provider_payment: str) -> PaymentRow | None:
        """The payment by the provider's payment id."""
        stmt = select(PaymentRow).where(PaymentRow.provider_payment == provider_payment)
        return (await self._session.scalars(stmt)).one_or_none()
