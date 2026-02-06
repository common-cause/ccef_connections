"""
Airtable connector for CCEF connections library.

This module provides a wrapper around pyairtable with automatic credential
management, retry logic, and convenient methods for common operations.
"""

import logging
from typing import Any, Dict, List, Optional

from pyairtable import Api, Table

from ..core.base import BaseConnection
from ..core.retry import retry_airtable_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)


class AirtableConnector(BaseConnection):
    """
    Airtable connector with automatic credential management.

    This connector wraps the pyairtable library and provides convenient
    methods for working with Airtable bases and tables.

    Examples:
        >>> connector = AirtableConnector()
        >>> table = connector.get_table('appXXX', 'TableName')
        >>> records = table.all(formula="{status} = 'pending'")
        >>>
        >>> # Or use as context manager
        >>> with AirtableConnector() as conn:
        ...     table = conn.get_table('appXXX', 'TableName')
        ...     records = table.all()
    """

    def __init__(self) -> None:
        """Initialize the Airtable connector."""
        super().__init__()
        self._api: Optional[Api] = None

    def connect(self) -> None:
        """
        Establish connection to Airtable using credentials.

        Raises:
            CredentialError: If Airtable API key is missing
            ConnectionError: If connection fails
        """
        try:
            api_key = self._credential_manager.get_airtable_key()
            self._api = Api(api_key)
            self._is_connected = True
            logger.info("Successfully connected to Airtable")
        except CredentialError:
            logger.error("Failed to connect to Airtable: missing credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Airtable: {str(e)}")
            raise ConnectionError(f"Failed to connect to Airtable: {str(e)}") from e

    def disconnect(self) -> None:
        """Close the Airtable connection."""
        self._api = None
        self._is_connected = False
        logger.debug("Disconnected from Airtable")

    def health_check(self) -> bool:
        """
        Check if the Airtable connection is healthy.

        Returns:
            True if connected and API object exists, False otherwise
        """
        return self._is_connected and self._api is not None

    def get_table(self, base_id: str, table_name: str) -> Table:
        """
        Get a table instance for operations.

        Args:
            base_id: The Airtable base ID (e.g., 'appXXX')
            table_name: The table name

        Returns:
            pyairtable.Table instance

        Raises:
            ConnectionError: If not connected

        Examples:
            >>> connector = AirtableConnector()
            >>> table = connector.get_table('appSBBlMCcLRWd2bk', 'Test Input')
            >>> records = table.all()
        """
        if not self._is_connected or self._api is None:
            self.connect()

        if self._api is None:
            raise ConnectionError("Not connected to Airtable")

        logger.debug(f"Getting table: {base_id}/{table_name}")
        return self._api.table(base_id, table_name)

    @retry_airtable_operation
    def get_records(
        self,
        base_id: str,
        table_name: str,
        formula: Optional[str] = None,
        max_records: Optional[int] = None,
        view: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get records from a table with retry logic.

        Args:
            base_id: The Airtable base ID
            table_name: The table name
            formula: Optional filter formula
            max_records: Maximum number of records to return
            view: Optional view name to use

        Returns:
            List of records

        Examples:
            >>> connector = AirtableConnector()
            >>> records = connector.get_records(
            ...     'appXXX', 'TableName',
            ...     formula="{status} = 'pending'",
            ...     max_records=100
            ... )
        """
        table = self.get_table(base_id, table_name)
        return table.all(formula=formula, max_records=max_records, view=view)

    @retry_airtable_operation
    def update_record(
        self, base_id: str, table_name: str, record_id: str, fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a single record with retry logic.

        Args:
            base_id: The Airtable base ID
            table_name: The table name
            record_id: The record ID to update
            fields: Dictionary of field names and values to update

        Returns:
            Updated record

        Examples:
            >>> connector = AirtableConnector()
            >>> updated = connector.update_record(
            ...     'appXXX', 'TableName', 'recXXX',
            ...     {'Status': 'processed', 'Summary': 'Done'}
            ... )
        """
        table = self.get_table(base_id, table_name)
        return table.update(record_id, fields)

    @retry_airtable_operation
    def batch_update(
        self, base_id: str, table_name: str, records: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Update multiple records in batch with retry logic.

        Args:
            base_id: The Airtable base ID
            table_name: The table name
            records: List of records to update (each with 'id' and 'fields')

        Returns:
            List of updated records

        Examples:
            >>> connector = AirtableConnector()
            >>> records_to_update = [
            ...     {'id': 'recXXX', 'fields': {'Status': 'processed'}},
            ...     {'id': 'recYYY', 'fields': {'Status': 'processed'}},
            ... ]
            >>> updated = connector.batch_update('appXXX', 'TableName', records_to_update)
        """
        table = self.get_table(base_id, table_name)
        return table.batch_update(records)

    @retry_airtable_operation
    def create_record(
        self, base_id: str, table_name: str, fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new record with retry logic.

        Args:
            base_id: The Airtable base ID
            table_name: The table name
            fields: Dictionary of field names and values

        Returns:
            Created record

        Examples:
            >>> connector = AirtableConnector()
            >>> new_record = connector.create_record(
            ...     'appXXX', 'TableName',
            ...     {'Name': 'John Doe', 'Email': 'john@example.com'}
            ... )
        """
        table = self.get_table(base_id, table_name)
        return table.create(fields)
