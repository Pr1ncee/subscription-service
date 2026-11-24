"""Helpers for table definitions."""

from enum import StrEnum


def one_of(column: str, enum: type[StrEnum]) -> str:
    """SQL check expression restricting `column` to the values of `enum`."""
    values = ", ".join(f"'{member.value}'" for member in enum)
    return f"{column} IN ({values})"
