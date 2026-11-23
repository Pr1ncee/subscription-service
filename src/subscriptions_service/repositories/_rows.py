from typing import Any

from sqlalchemy import inspect

from subscriptions_service.db.base import Base


def insert_values(row: Base) -> dict[str, Any]:
    """Column values set on a transient row; unset columns fall back to their defaults."""
    return {
        attr.key: getattr(row, attr.key)
        for attr in inspect(type(row)).column_attrs
        if getattr(row, attr.key) is not None
    }
