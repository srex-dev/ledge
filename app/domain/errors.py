class DomainError(Exception):
    """Base domain error."""


class ValidationError(DomainError):
    """Input validation failed."""


class NotFoundError(DomainError):
    """Requested resource was not found."""


class InsufficientFundsError(DomainError):
    """Account has insufficient funds."""


class IdempotencyConflictError(DomainError):
    """Idempotency key reused with a different payload."""
