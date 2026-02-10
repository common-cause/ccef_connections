"""
CCEF Connections - Reusable connection library for CCEF data integrations.

This library provides unified connection management for Airtable, OpenAI,
Google Sheets, and BigQuery with Civis credential compatibility.
"""

from .config import ConfigManager
from .connectors.action_network import ActionNetworkConnector
from .connectors.airtable import AirtableConnector
from .connectors.bigquery import BigQueryConnector
from .connectors.helpscout import HelpScoutConnector
from .connectors.openai import OpenAIConnector
from .connectors.sheets import SheetsConnector
from .connectors.zoom import ZoomConnector
from .core.credentials import CredentialManager, get_credential
from .exceptions import (
    CCEFConnectionError,
    CredentialError,
    ConnectionError,
    AuthenticationError,
    RateLimitError,
    ConfigurationError,
    QueryError,
    WriteError,
)

__version__ = "0.1.0"

__all__ = [
    # Main connectors
    "ActionNetworkConnector",
    "AirtableConnector",
    "BigQueryConnector",
    "HelpScoutConnector",
    "OpenAIConnector",
    "SheetsConnector",
    "ZoomConnector",
    # Configuration
    "ConfigManager",
    # Credentials
    "CredentialManager",
    "get_credential",
    # Exceptions
    "CCEFConnectionError",
    "CredentialError",
    "ConnectionError",
    "AuthenticationError",
    "RateLimitError",
    "ConfigurationError",
    "QueryError",
    "WriteError",
]
