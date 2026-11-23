import pytest
from pydantic import ValidationError

from subscriptions_service.config import Settings


@pytest.fixture(autouse=True)
def required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://g:g@h/")
    monkeypatch.delenv("REMINDER_OFFSETS_DAYS", raising=False)


def test_reminder_offsets_parsed_from_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REMINDER_OFFSETS_DAYS", "7,1,0")
    s = Settings()
    assert s.reminder_offsets_days == [7, 1, 0]
    assert (s.relay_poll_interval_s, s.relay_batch_size) == (0.5, 100)
    assert (s.scheduler_poll_interval_s, s.scheduler_batch_size) == (1.0, 100)
    assert (s.relay_max_backoff_s, s.consumer_delivery_limit, s.publish_timeout_s) == (60, 5, 5.0)
    assert s.log_level == "INFO"


def test_default_offsets() -> None:
    assert Settings().reminder_offsets_days == [3, 1, 0]


@pytest.mark.parametrize("raw", ["3,-1", "3,1,1", "a,b"])
def test_invalid_offsets_rejected(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("REMINDER_OFFSETS_DAYS", raw)
    with pytest.raises(ValidationError):
        Settings()
