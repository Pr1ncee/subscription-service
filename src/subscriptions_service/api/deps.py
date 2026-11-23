"""Composition root for the HTTP API: the only place that wires adapters into services."""

from typing import Annotated, cast

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from subscriptions_service.adapters.clock import SystemClock
from subscriptions_service.config import Settings
from subscriptions_service.repositories.uow import UnitOfWork
from subscriptions_service.services.payments import PaymentService
from subscriptions_service.services.ports import Clock
from subscriptions_service.services.subscriptions import SubscriptionService


def get_settings(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], request.app.state.session_factory)


def get_clock() -> Clock:
    return SystemClock()


SettingsDep = Annotated[Settings, Depends(get_settings)]
SessionFactoryDep = Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)]
ClockDep = Annotated[Clock, Depends(get_clock)]


def get_subscription_service(
    session_factory: SessionFactoryDep, clock: ClockDep, settings: SettingsDep
) -> SubscriptionService:
    return SubscriptionService(
        lambda: UnitOfWork(session_factory), clock, settings.reminder_offsets_days
    )


def get_payment_service(session_factory: SessionFactoryDep, clock: ClockDep) -> PaymentService:
    return PaymentService(lambda: UnitOfWork(session_factory), clock)


SubscriptionServiceDep = Annotated[SubscriptionService, Depends(get_subscription_service)]
PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
