from enum import StrEnum


class NotificationStatus(StrEnum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    SENT = "sent"
    CANCELLED = "cancelled"
    FAILED = "failed"


class PaymentStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class NotificationEvent(StrEnum):
    SUBSCRIPTION_EXPIRING = "subscription.expiring"
    SUBSCRIPTION_EXPIRED = "subscription.expired"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"


class EventType(StrEnum):
    PAYMENT_SUCCEEDED = "payment.succeeded"
    PAYMENT_FAILED = "payment.failed"
    NOTIFICATION_DUE = "notification.due"


class AggregateType(StrEnum):
    """What an outbox row's `aggregate_id` refers to."""

    PAYMENT = "payment"
    NOTIFICATION = "notification"
