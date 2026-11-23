from typing import Annotated, Any

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
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
        if isinstance(value, str):
            return [int(part) for part in value.split(",") if part.strip()]
        return value

    @field_validator("reminder_offsets_days")
    @classmethod
    def _validate_offsets(cls, value: list[int]) -> list[int]:
        if any(offset < 0 for offset in value):
            raise ValueError("reminder offsets must be non-negative")
        if len(set(value)) != len(value):
            raise ValueError("reminder offsets must be unique")
        return value
