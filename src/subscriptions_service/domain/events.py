import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from subscriptions_service.domain.enums import EventType


class InvalidEnvelopeError(ValueError):
    """Raised when a message body is not a valid event envelope."""


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event_id: UUID
    event_type: EventType
    occurred_at: datetime
    payload: Mapping[str, Any]

    @classmethod
    def new(
        cls, event_type: EventType, payload: Mapping[str, Any], occurred_at: datetime
    ) -> "EventEnvelope":
        return cls(uuid.uuid4(), event_type, occurred_at, payload)

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "event_id": str(self.event_id),
                "event_type": self.event_type.value,
                "occurred_at": self.occurred_at.isoformat(),
                "payload": dict(self.payload),
            }
        ).encode()

    @classmethod
    def from_json(cls, data: bytes) -> "EventEnvelope":
        try:
            raw = json.loads(data)
            if not isinstance(raw, dict):
                raise InvalidEnvelopeError("envelope must be a JSON object")
            payload = raw["payload"]
            if not isinstance(payload, dict):
                raise InvalidEnvelopeError("payload must be a JSON object")
            return cls(
                event_id=UUID(raw["event_id"]),
                event_type=EventType(raw["event_type"]),
                occurred_at=datetime.fromisoformat(raw["occurred_at"]),
                payload=payload,
            )
        except InvalidEnvelopeError:
            raise
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, AttributeError) as e:
            raise InvalidEnvelopeError(f"invalid event envelope: {e}") from e
