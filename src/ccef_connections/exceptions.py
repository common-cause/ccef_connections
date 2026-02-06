"""
Custom exceptions for CCEF connections library.

This module defines a hierarchy of exceptions used throughout the library
to provide clear, actionable error messages for connection failures.
"""


class CCEFConnectionError(Exception):
    """Base exception for all CCEF connection errors."""

    pass


class CredentialError(CCEFConnectionError):
    """Raised when credentials are missing or invalid."""

    pass


class ConnectionError(CCEFConnectionError):
    """Raised when a connection cannot be established."""

    pass


class AuthenticationError(CCEFConnectionError):
    """Raised when authentication fails due to invalid credentials."""

    pass


class RateLimitError(CCEFConnectionError):
    """Raised when an API rate limit is exceeded."""

    def __init__(self, message: str, retry_after: int = None) -> None:
        """
        Initialize RateLimitError.

        Args:
            message: Error message
            retry_after: Seconds to wait before retrying (if provided by API)
        """
        super().__init__(message)
        self.retry_after = retry_after


class ConfigurationError(CCEFConnectionError):
    """Raised when configuration is invalid or missing."""

    pass


class QueryError(CCEFConnectionError):
    """Raised when a query fails (BigQuery or Airtable)."""

    pass


class WriteError(CCEFConnectionError):
    """Raised when a write operation fails (BigQuery or Airtable)."""

    pass
