import logging
import uuid
from datetime import UTC, datetime

import pytest

from subscriptions_service.adapters.logging_sender import LoggingSender
from subscriptions_service.services.ports import OutgoingNotification


async def test_logging_sender_logs_notification_at_info(caplog: pytest.LogCaptureFixture) -> None:
    message = OutgoingNotification(
        notification_id=uuid.uuid4(),
        user_id="u1",
        subscription_id="s1",
        event_name="payment.succeeded",
        expected_expires_on=datetime(2026, 2, 1, tzinfo=UTC),
    )
    with caplog.at_level(logging.INFO):
        await LoggingSender().send(message)
    [record] = [
        r for r in caplog.records if r.name == "subscriptions_service.adapters.logging_sender"
    ]
    assert record.levelno == logging.INFO
    assert record.__dict__["user_id"] == "u1"
    assert record.__dict__["notification_id"] == str(message.notification_id)
    assert record.__dict__["event_name"] == "payment.succeeded"
