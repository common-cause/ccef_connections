"""
Google Sheets connector for CCEF connections library.

This module provides READ-ONLY access to Google Sheets for configuration
management and data retrieval using service account authentication.
"""

import logging
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

from ..core.base import BaseConnection
from ..core.retry import retry_google_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)


class SheetsConnector(BaseConnection):
    """
    Google Sheets connector with service account authentication.

    This connector provides READ-ONLY access to Google Sheets for reading
    configuration data and other information. It uses service account
    credentials for authentication.

    Examples:
        >>> connector = SheetsConnector()
        >>> data = connector.get_range('SPREADSHEET_ID', 'Sheet1!A1:B10')
        >>>
        >>> # Or use as context manager
        >>> with SheetsConnector() as conn:
        ...     worksheet = conn.get_worksheet('SPREADSHEET_ID', 'Config')
        ...     values = worksheet.get_all_values()
    """

    # Google Sheets API scopes
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    def __init__(self) -> None:
        """Initialize the Google Sheets connector."""
        super().__init__()
        self._credentials: Optional[Credentials] = None

    def connect(self) -> None:
        """
        Establish connection to Google Sheets using service account credentials.

        Raises:
            CredentialError: If Google Sheets credentials are missing or invalid
            ConnectionError: If connection fails
        """
        try:
            creds_dict = self._credential_manager.get_google_sheets_credentials()

            # Create credentials from service account info
            self._credentials = Credentials.from_service_account_info(
                creds_dict, scopes=self.SCOPES
            )

            # Create gspread client
            self._client = gspread.authorize(self._credentials)
            self._is_connected = True
            logger.info("Successfully connected to Google Sheets")
        except CredentialError:
            logger.error("Failed to connect to Google Sheets: missing credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {str(e)}")
            raise ConnectionError(f"Failed to connect to Google Sheets: {str(e)}") from e

    def disconnect(self) -> None:
        """Close the Google Sheets connection."""
        self._client = None
        self._credentials = None
        self._is_connected = False
        logger.debug("Disconnected from Google Sheets")

    def health_check(self) -> bool:
        """
        Check if the Google Sheets connection is healthy.

        Returns:
            True if connected and client exists, False otherwise
        """
        return self._is_connected and self._client is not None

    @retry_google_operation
    def get_spreadsheet(self, spreadsheet_id: str) -> Any:
        """
        Get a spreadsheet by ID.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID

        Returns:
            gspread.Spreadsheet instance

        Raises:
            ConnectionError: If not connected

        Examples:
            >>> connector = SheetsConnector()
            >>> sheet = connector.get_spreadsheet('1ABC...')
            >>> print(sheet.title)
        """
        if not self._is_connected or self._client is None:
            self.connect()

        if self._client is None:
            raise ConnectionError("Not connected to Google Sheets")

        logger.debug(f"Getting spreadsheet: {spreadsheet_id}")
        return self._client.open_by_key(spreadsheet_id)

    @retry_google_operation
    def get_worksheet(self, spreadsheet_id: str, worksheet_name: str) -> Any:
        """
        Get a worksheet by spreadsheet ID and worksheet name.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_name: The worksheet name

        Returns:
            gspread.Worksheet instance

        Examples:
            >>> connector = SheetsConnector()
            >>> worksheet = connector.get_worksheet('1ABC...', 'Config')
            >>> values = worksheet.get_all_values()
        """
        spreadsheet = self.get_spreadsheet(spreadsheet_id)
        logger.debug(f"Getting worksheet: {worksheet_name}")
        return spreadsheet.worksheet(worksheet_name)

    @retry_google_operation
    def get_range(
        self, spreadsheet_id: str, range_name: str
    ) -> List[List[Any]]:
        """
        Get values from a specific range in A1 notation.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            range_name: Range in A1 notation (e.g., 'Sheet1!A1:B10')

        Returns:
            List of lists containing cell values

        Examples:
            >>> connector = SheetsConnector()
            >>> data = connector.get_range('1ABC...', 'Config!A1:D100')
            >>> for row in data:
            ...     print(row)
        """
        spreadsheet = self.get_spreadsheet(spreadsheet_id)
        logger.debug(f"Getting range: {range_name}")
        return spreadsheet.values_get(range_name).get("values", [])

    @retry_google_operation
    def get_all_values(
        self, spreadsheet_id: str, worksheet_name: str
    ) -> List[List[Any]]:
        """
        Get all values from a worksheet.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_name: The worksheet name

        Returns:
            List of lists containing all cell values

        Examples:
            >>> connector = SheetsConnector()
            >>> data = connector.get_all_values('1ABC...', 'Config')
        """
        worksheet = self.get_worksheet(spreadsheet_id, worksheet_name)
        logger.debug(f"Getting all values from worksheet: {worksheet_name}")
        return worksheet.get_all_values()

    @retry_google_operation
    def get_range_as_dicts(
        self, spreadsheet_id: str, range_name: str, header_row: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get values from a range as a list of dictionaries.

        Uses the specified row (default: first row) as headers and returns
        each subsequent row as a dictionary with those headers as keys.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            range_name: Range in A1 notation
            header_row: Index of the header row (default: 0)

        Returns:
            List of dictionaries

        Examples:
            >>> connector = SheetsConnector()
            >>> config = connector.get_range_as_dicts('1ABC...', 'Config!A1:D100')
            >>> for item in config:
            ...     print(item['Section'], item['Key'], item['Value'])
        """
        data = self.get_range(spreadsheet_id, range_name)

        if not data or len(data) <= header_row:
            return []

        headers = data[header_row]
        result = []

        for row in data[header_row + 1:]:
            # Pad row with empty strings if it's shorter than headers
            padded_row = row + [""] * (len(headers) - len(row))
            result.append(dict(zip(headers, padded_row)))

        logger.debug(f"Converted {len(result)} rows to dictionaries")
        return result

    @retry_google_operation
    def get_worksheet_as_dicts(
        self, spreadsheet_id: str, worksheet_name: str, header_row: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get all values from a worksheet as a list of dictionaries.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_name: The worksheet name
            header_row: Index of the header row (default: 0)

        Returns:
            List of dictionaries

        Examples:
            >>> connector = SheetsConnector()
            >>> config = connector.get_worksheet_as_dicts('1ABC...', 'Config')
        """
        data = self.get_all_values(spreadsheet_id, worksheet_name)

        if not data or len(data) <= header_row:
            return []

        headers = data[header_row]
        result = []

        for row in data[header_row + 1:]:
            # Pad row with empty strings if it's shorter than headers
            padded_row = row + [""] * (len(headers) - len(row))
            result.append(dict(zip(headers, padded_row)))

        logger.debug(f"Converted {len(result)} rows to dictionaries")
        return result
