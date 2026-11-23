from datetime import UTC, datetime

import pytest

from subscriptions_service.domain.enums import EventType
from subscriptions_service.domain.events import EventEnvelope, InvalidEnvelopeError

NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)


def test_envelope_roundtrip() -> None:
    env = EventEnvelope.new(
        EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1", "amount": "10.0000"}, NOW
    )
    assert EventEnvelope.from_json(env.to_json()) == env


def test_envelope_new_generates_unique_ids() -> None:
    a = EventEnvelope.new(EventType.NOTIFICATION_DUE, {}, NOW)
    b = EventEnvelope.new(EventType.NOTIFICATION_DUE, {}, NOW)
    assert a.event_id != b.event_id


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"{}",
        b'{"event_id": "x"}',
        b"[]",
        b'{"event_id": "8f0e5b8e-6c1f-4f5e-9a51-2a8f0f6f2d11", "event_type": "nope",'
        b' "occurred_at": "2026-01-01T12:00:00+00:00", "payload": {}}',
        b'{"event_id": "8f0e5b8e-6c1f-4f5e-9a51-2a8f0f6f2d11", "event_type": "payment.failed",'
        b' "occurred_at": "2026-01-01T12:00:00+00:00", "payload": []}',
        b'{"event_id": 123, "event_type": "payment.failed",'
        b' "occurred_at": "2026-01-01T12:00:00+00:00", "payload": {}}',
        b'{"event_id": "8f0e5b8e-6c1f-4f5e-9a51-2a8f0f6f2d11", "event_type": "payment.failed",'
        b' "occurred_at": 5, "payload": {}}',
    ],
)
def test_invalid_envelope_raises(raw: bytes) -> None:
    with pytest.raises(InvalidEnvelopeError):
        EventEnvelope.from_json(raw)
