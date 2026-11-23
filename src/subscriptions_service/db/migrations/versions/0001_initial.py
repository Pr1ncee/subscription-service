"""initial

Revision ID: 0001
Revises:
Create Date: 2026-11-23 15:58:24.558653
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column("aggregate_id", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_aggregate_type_aggregate_id",
        "outbox",
        ["aggregate_type", "aggregate_id"],
        unique=False,
    )
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["next_attempt_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "processed_events",
        sa.Column("consumer", sa.Text(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("consumer", "event_id", name=op.f("pk_processed_events")),
    )
    op.create_table(
        "subscriptions",
        sa.Column("subscription_id", sa.Text(), nullable=False),
        sa.Column("user_id", sa.Text(), nullable=False),
        sa.Column("day_count", sa.Integer(), nullable=False),
        sa.Column("expected_expires_on", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("day_count > 0", name=op.f("ck_subscriptions_day_count_positive")),
        sa.PrimaryKeyConstraint("subscription_id", name=op.f("pk_subscriptions")),
    )
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("subscription_id", sa.Text(), nullable=False),
        sa.Column("provider_payment", sa.Text(), nullable=False),
        sa.Column("amount", sa.Numeric(precision=19, scale=4), nullable=False),
        sa.Column("currency", sa.CHAR(length=3), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("period_days", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f("ck_payments_currency_iso4217")),
        sa.CheckConstraint(
            "status IN ('succeeded', 'failed')", name=op.f("ck_payments_status_valid")
        ),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_amount_positive")),
        sa.CheckConstraint("period_days > 0", name=op.f("ck_payments_period_days_positive")),
        sa.ForeignKeyConstraint(
            ["subscription_id"],
            ["subscriptions.subscription_id"],
            name=op.f("fk_payments_subscription_id_subscriptions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("provider_payment", name=op.f("uq_payments_provider_payment")),
    )
    op.create_index(
        op.f("ix_payments_subscription_id"), "payments", ["subscription_id"], unique=False
    )
    op.create_table(
        "notifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("subscription_id", sa.Text(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=True),
        sa.Column("event_name", sa.Text(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("target_expires_on", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "event_name IN ('subscription.expiring', 'subscription.expired', 'payment.succeeded', 'payment.failed')",
            name=op.f("ck_notifications_event_name_valid"),
        ),
        sa.CheckConstraint(
            "status IN ('scheduled', 'queued', 'sent', 'cancelled', 'failed')",
            name=op.f("ck_notifications_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["payment_id"], ["payments.id"], name=op.f("fk_notifications_payment_id_payments")
        ),
        sa.ForeignKeyConstraint(
            ["subscription_id"],
            ["subscriptions.subscription_id"],
            name=op.f("fk_notifications_subscription_id_subscriptions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
    )
    op.create_index(
        "ix_notifications_due",
        "notifications",
        ["scheduled_for"],
        unique=False,
        postgresql_where=sa.text("status = 'scheduled'"),
    )
    op.create_index(
        "ix_notifications_subscription_id_scheduled_for",
        "notifications",
        ["subscription_id", "scheduled_for"],
        unique=False,
    )
    op.create_index(
        "uq_notifications_payment_id",
        "notifications",
        ["payment_id"],
        unique=True,
        postgresql_where=sa.text("payment_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_notifications_payment_id",
        table_name="notifications",
        postgresql_where=sa.text("payment_id IS NOT NULL"),
    )
    op.drop_index("ix_notifications_subscription_id_scheduled_for", table_name="notifications")
    op.drop_index(
        "ix_notifications_due",
        table_name="notifications",
        postgresql_where=sa.text("status = 'scheduled'"),
    )
    op.drop_table("notifications")
    op.drop_index(op.f("ix_payments_subscription_id"), table_name="payments")
    op.drop_table("payments")
    op.drop_table("subscriptions")
    op.drop_table("processed_events")
    op.drop_index(
        "ix_outbox_pending", table_name="outbox", postgresql_where=sa.text("published_at IS NULL")
    )
    op.drop_index("ix_outbox_aggregate_type_aggregate_id", table_name="outbox")
    op.drop_table("outbox")
