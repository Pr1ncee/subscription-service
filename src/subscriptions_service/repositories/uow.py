from collections.abc import Callable
from types import TracebackType
from typing import Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from subscriptions_service.repositories.inbox import InboxRepository
from subscriptions_service.repositories.notifications import NotificationRepository
from subscriptions_service.repositories.outbox import OutboxRepository
from subscriptions_service.repositories.payments import PaymentRepository
from subscriptions_service.repositories.subscriptions import SubscriptionRepository


class UnitOfWork:
    """One database transaction: commits on clean exit, rolls back on exception."""

    subscriptions: SubscriptionRepository
    notifications: NotificationRepository
    payments: PaymentRepository
    outbox: OutboxRepository
    inbox: InboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        session = self._session_factory()
        self._session = session
        self.subscriptions = SubscriptionRepository(session)
        self.notifications = NotificationRepository(session)
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)
        self.inbox = InboxRepository(session)
        return self

    async def commit(self) -> None:
        assert self._session is not None, "UnitOfWork used outside `async with`"  # noqa: S101
        await self._session.commit()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._session is not None  # noqa: S101
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self._session.rollback()
        finally:
            await self._session.close()
            self._session = None


UnitOfWorkFactory = Callable[[], UnitOfWork]
