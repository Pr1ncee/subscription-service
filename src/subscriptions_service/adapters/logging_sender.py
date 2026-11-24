import logging

from subscriptions_service.services.ports import OutgoingNotification

logger = logging.getLogger(__name__)


class LoggingSender:
    """`NotificationSender` that only logs: stands in for a real email/SMS/push channel."""

    async def send(self, message: OutgoingNotification) -> None:
        """Log the notification as sent."""
        logger.info(
            "notification sent",
            extra={
                "notification_id": str(message.notification_id),
                "user_id": message.user_id,
                "subscription_id": message.subscription_id,
                "event_name": message.event_name,
                "expected_expires_on": message.expected_expires_on.isoformat(),
            },
        )
