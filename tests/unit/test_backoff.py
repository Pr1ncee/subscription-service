from datetime import timedelta

import pytest

from subscriptions_service.services.relay import backoff_delay


@pytest.mark.parametrize(
    ("attempts", "seconds"), [(1, 0.5), (2, 1.0), (3, 2.0), (8, 60.0), (10, 60.0), (500, 60.0)]
)
def test_backoff_delay(attempts: int, seconds: float) -> None:
    assert backoff_delay(attempts, 60) == timedelta(seconds=seconds)
