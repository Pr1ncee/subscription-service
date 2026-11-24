"""Helpers for request validation and response mapping."""

from datetime import UTC, datetime

from subscriptions_service.config import MAX_EXPIRY_YEAR, MIN_EXPIRY_YEAR


def bounded_utc(value: datetime) -> datetime:
    """Normalise an aware datetime to UTC and require its year in the accepted expiry range.

    Raises `ValueError` (reported by Pydantic as `422`) when the value is out of range.
    """
    try:
        utc = value.astimezone(UTC)
    except OverflowError as e:
        raise ValueError("datetime is out of range") from e
    if not MIN_EXPIRY_YEAR <= utc.year < MAX_EXPIRY_YEAR:
        raise ValueError(f"year must be in [{MIN_EXPIRY_YEAR}, {MAX_EXPIRY_YEAR})")
    return utc
