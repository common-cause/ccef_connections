"""Core functionality for CCEF connections."""

from .base import BaseConnection
from .credentials import CredentialManager, get_credential
from .retry import (
    retry_with_backoff,
    retry_airtable_operation,
    retry_openai_operation,
    retry_google_operation,
    retry_helpscout_operation,
    retry_zoom_operation,
)

__all__ = [
    "BaseConnection",
    "CredentialManager",
    "get_credential",
    "retry_with_backoff",
    "retry_airtable_operation",
    "retry_openai_operation",
    "retry_google_operation",
    "retry_helpscout_operation",
    "retry_zoom_operation",
]
