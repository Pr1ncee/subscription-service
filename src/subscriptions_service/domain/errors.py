class DomainError(Exception):
    """Base class for business-rule violations."""


class NotFoundError(DomainError):
    """The requested entity does not exist."""


class ConflictError(DomainError):
    """The request conflicts with the current state."""
