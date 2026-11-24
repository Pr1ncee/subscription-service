from enum import StrEnum


class NotificationStatus(StrEnum):
    """Lifecycle of a notification: scheduled → queued → sent, cancelled or failed."""

    SCHEDULED = "scheduled"
    QUEUED = "queued"
    SENT = "sent"
    CANCELLED = "cancelled"
    FAILED = "failed"


class PaymentStatus(StrEnum):
    """Result of a payment as reported by the provider."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class NotificationEvent(StrEnum):
    """What a notification tells the user."""

    SUBSCRIPTION_EXPIRING = "subscription.expiring"
    SUBSCRIPTION_EXPIRED = "subscription.expired"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"


class EventType(StrEnum):
    """Routing key of an event published to the events exchange."""

    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    NOTIFICATION_DUE = "notification.due"


class AggregateType(StrEnum):
    """What an outbox row's `aggregate_id` refers to."""

    PAYMENT = "payment"
    NOTIFICATION = "notification"
