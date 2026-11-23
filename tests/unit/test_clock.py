from datetime import UTC, datetime, timedelta

from subscriptions_service.adapters.clock import SystemClock


def test_system_clock_returns_aware_utc_now() -> None:
    now = SystemClock().now()
    assert now.tzinfo is UTC
    assert abs(datetime.now(UTC) - now) < timedelta(seconds=5)
