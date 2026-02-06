"""
Base connection class for all CCEF connectors.

This module defines the abstract base class that all connectors must implement,
ensuring a consistent interface across different services.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Optional

from .credentials import CredentialManager

logger = logging.getLogger(__name__)


class BaseConnection(ABC):
    """
    Abstract base class for all CCEF connectors.

    All connectors (Airtable, OpenAI, Google Sheets, BigQuery) inherit from
    this class and implement the required methods.

    Attributes:
        _client: The underlying API client instance
        _is_connected: Flag indicating connection status
        _credential_manager: Shared credential manager instance
    """

    def __init__(self) -> None:
        """Initialize the base connection."""
        self._client: Optional[Any] = None
        self._is_connected: bool = False
        self._credential_manager = CredentialManager()
        logger.debug(f"Initialized {self.__class__.__name__}")

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the service.

        This method must be implemented by all subclasses to handle
        service-specific connection logic.

        Raises:
            ConnectionError: If connection fails
            CredentialError: If credentials are invalid
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the service.

        This method must be implemented by all subclasses to handle
        cleanup and resource release.
        """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        Check if the connection is healthy and functional.

        Returns:
            True if the connection is healthy, False otherwise
        """
        pass

    def is_connected(self) -> bool:
        """
        Check if currently connected to the service.

        Returns:
            True if connected, False otherwise
        """
        return self._is_connected

    def __enter__(self) -> "BaseConnection":
        """
        Context manager entry.

        Returns:
            self

        Examples:
            >>> with connector as conn:
            ...     # Use the connection
            ...     pass
        """
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Context manager exit.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        self.disconnect()

    def __repr__(self) -> str:
        """
        String representation of the connection.

        Returns:
            String representation
        """
        status = "connected" if self._is_connected else "disconnected"
        return f"<{self.__class__.__name__} status={status}>"
