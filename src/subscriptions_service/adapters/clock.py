from datetime import UTC, datetime


class SystemClock:
    """`Clock` backed by the system clock; the only caller of `datetime.now()`."""

    def now(self) -> datetime:
        """Current time, timezone-aware in UTC."""
        return datetime.now(UTC)
