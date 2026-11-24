import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import CHAR, CheckConstraint, ForeignKey, Index, func, text
from sqlalchemy.orm import Mapped, mapped_column

from subscriptions_service.db.base import Base
from subscriptions_service.db.utils import one_of
from subscriptions_service.domain.enums import NotificationEvent, NotificationStatus, PaymentStatus


class SubscriptionRow(Base):
    __tablename__ = "subscriptions"
    __table_args__ = (CheckConstraint("day_count > 0", name="day_count_positive"),)

    subscription_id: Mapped[str] = mapped_column(primary_key=True)
    user_id: Mapped[str]
    day_count: Mapped[int]
    expected_expires_on: Mapped[datetime]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="currency_iso4217"),
        CheckConstraint(one_of("status", PaymentStatus), name="status_valid"),
        CheckConstraint("period_days > 0", name="period_days_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    subscription_id: Mapped[str] = mapped_column(
        ForeignKey("subscriptions.subscription_id"), index=True
    )
    provider_payment: Mapped[str] = mapped_column(unique=True)
    amount: Mapped[Decimal]
    currency: Mapped[str] = mapped_column(CHAR(3))
    status: Mapped[str]
    period_days: Mapped[int]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())


class NotificationRow(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint(one_of("status", NotificationStatus), name="status_valid"),
        CheckConstraint(one_of("event_name", NotificationEvent), name="event_name_valid"),
        Index(
            "ix_notifications_due",
            "scheduled_for",
            postgresql_where=text("status = 'scheduled'"),
        ),
        Index("ix_notifications_subscription_id_scheduled_for", "subscription_id", "scheduled_for"),
        Index(
            "uq_notifications_payment_id",
            "payment_id",
            unique=True,
            postgresql_where=text("payment_id IS NOT NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    subscription_id: Mapped[str] = mapped_column(ForeignKey("subscriptions.subscription_id"))
    payment_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("payments.id"))
    event_name: Mapped[str]
    scheduled_for: Mapped[datetime]
    target_expires_on: Mapped[datetime | None]
    status: Mapped[str]
    last_error: Mapped[str | None]
    sent_at: Mapped[datetime | None]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class OutboxRow(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("ix_outbox_aggregate_type_aggregate_id", "aggregate_type", "aggregate_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    aggregate_type: Mapped[str]
    aggregate_id: Mapped[str]
    event_type: Mapped[str]
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    published_at: Mapped[datetime | None]
    attempts: Mapped[int] = mapped_column(default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(server_default=func.now())
    last_error: Mapped[str | None]


class ProcessedEventRow(Base):
    __tablename__ = "processed_events"

    consumer: Mapped[str] = mapped_column(primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(server_default=func.now())
