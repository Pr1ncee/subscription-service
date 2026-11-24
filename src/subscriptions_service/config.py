"""Service configuration: environment settings and fixed limits.

`Settings` holds values that differ per deployment and are read from the environment.
The module-level constants are limits and tunings that are part of the service's
contract; they are read only by the outer layers (`api`, `worker`, `consumers`).
"""

from typing import Annotated, Any, Final

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

MAX_BODY_SIZE: Final = 64 * 1024
"""Largest accepted HTTP request body, in bytes; larger requests get `413`."""

NO_NUL_PATTERN: Final = r"^[^\x00]*$"
"""Rejects NUL bytes, which Postgres `text` cannot store, before they reach the driver."""

MAX_TEXT_ID_LENGTH: Final = 255
"""Longest accepted `subscription_id`, `user_id` or `provider_payment`."""

MAX_DAY_COUNT: Final = 3650
"""Longest accepted billing period, in days."""

CURRENCY_PATTERN: Final = r"^[A-Z]{3}$"
"""ISO 4217 alphabetic currency code."""

MIN_EXPIRY_YEAR: Final = 1970
"""Earliest accepted expiry year (inclusive)."""

MAX_EXPIRY_YEAR: Final = 9000
"""Latest accepted expiry year (exclusive).

Leaves headroom for reminder offsets and paid extensions without leaving `datetime`'s range.
"""

CONSUMER_PREFETCH: Final = 10
"""Unacked messages per consumer channel.

A backlog stays in RabbitMQ instead of in the process's memory, and extra replicas share it.
"""

DB_RETRY_BASE_S: Final = 0.2
"""First wait before retrying a delivery whose handler found the database unreachable."""

DB_RETRY_MAX_S: Final = 5.0
"""Cap on the exponential wait while the database is unreachable."""

RETRY_BASE_S: Final = 0.5
"""First backoff before a failed delivery is requeued (`reject(requeue=True)`)."""

RETRY_MAX_S: Final = 10.0
"""Cap on the requeue backoff, so the delivery limit is not spent within milliseconds."""

DLQ_RETRY_DELAY_S: Final = 1.0
"""Wait before retrying a dead letter that could not be recorded (e.g. Postgres down)."""


class Settings(BaseSettings):
    """Deployment settings, read from environment variables and validated at startup."""

    model_config = SettingsConfigDict(extra="ignore")

    database_url: str
    rabbitmq_url: str
    reminder_offsets_days: Annotated[list[int], NoDecode] = [3, 1, 0]
    relay_poll_interval_s: float = 0.5
    relay_batch_size: int = 100
    scheduler_poll_interval_s: float = 1.0
    scheduler_batch_size: int = 100
    relay_max_backoff_s: float = 60
    consumer_delivery_limit: int = 5
    publish_timeout_s: float = 5.0
    log_level: str = "INFO"

    @field_validator("reminder_offsets_days", mode="before")
    @classmethod
    def _parse_offsets(cls, value: Any) -> Any:
        """Accept a comma-separated string such as `3,1,0` from the environment."""
        if isinstance(value, str):
            return [int(part) for part in value.split(",") if part.strip()]
        return value

    @field_validator("reminder_offsets_days")
    @classmethod
    def _validate_offsets(cls, value: list[int]) -> list[int]:
        """Offsets are days before expiry: they must be non-negative and unique."""
        if any(offset < 0 for offset in value):
            raise ValueError("reminder offsets must be non-negative")
        if len(set(value)) != len(value):
            raise ValueError("reminder offsets must be unique")
        return value
