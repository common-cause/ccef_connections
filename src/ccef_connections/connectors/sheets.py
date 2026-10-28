"""
Google Sheets connector for CCEF connections library.

This module provides READ-ONLY access to Google Sheets for configuration
management and data retrieval using service account authentication.
"""

import logging
from typing import Any, Dict, List, NamedTuple, Optional, Sequence

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import absolute_range_name, fill_gaps

from ..core.base import BaseConnection
from ..core.retry import retry_google_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)


class SheetsWorkbook(NamedTuple):
    """One spreadsheet read in two requests: its properties, its full tab list,
    and the values of the tabs that were asked for.

    `tab_names` is every tab in the file, not just the ones read -- it is what
    lets a caller detect a tab that was added or renamed without reading it.
    """

    spreadsheet_id: str
    title: str
    timezone: Optional[str]
    tab_names: List[str]
    values: Dict[str, List[List[Any]]]


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
    def get_workbook(
        self,
        spreadsheet_id: str,
        worksheet_names: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
    ) -> SheetsWorkbook:
        """
        Read one spreadsheet -- properties, tab list and tab values -- in TWO
        Sheets requests, however many tabs are wanted.

        This is the method to reach for when a job reads several tabs of a file
        and also records what it saw (title, time zone, the full tab list).
        Assembling the same picture from get_spreadsheet() + worksheets() +
        get_all_values() per tab costs 2 + 3N requests against a 60/minute
        ceiling; this costs 2. See get_tabs_values() for the values-only form
        and the read-budget rationale.

        `timezone` is the file's own setting, which is NOT always the zone its
        timestamps are written in -- an integration can write local wall-clock
        times into a file still set to GMT. Callers that parse dates should let
        a per-source override win over this.

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_names: Worksheet titles to read; None reads every tab
            missing_ok: If True, a requested tab that does not exist is omitted
                from `values` instead of raising. It is still absent from
                `tab_names` only if it genuinely isn't in the file.

        Returns:
            SheetsWorkbook(spreadsheet_id, title, timezone, tab_names, values)

        Raises:
            ValueError: If a requested worksheet does not exist (unless missing_ok)
            ConnectionError: If not connected, or if Sheets returns a response
                that cannot be mapped back to the requested tabs

        Examples:
            >>> connector = SheetsConnector()
            >>> wb = connector.get_workbook('1ABC...', ['Week 2', 'Week 3'])
            >>> wb.title, len(wb.tab_names), len(wb.values['Week 2'])
        """
        spreadsheet = self.get_spreadsheet(spreadsheet_id)

        # The one metadata request. Titles come from here rather than being
        # trusted, so a typo fails loudly instead of as an opaque API 400 on a
        # range, and so a caller can ask for "every tab".
        existing = [ws.title for ws in spreadsheet.worksheets()]
        wanted = list(existing if worksheet_names is None else worksheet_names)

        missing = [name for name in wanted if name not in existing]
        if missing and not missing_ok:
            raise ValueError(
                f"worksheet(s) {missing} not found in spreadsheet "
                f"{spreadsheet_id}; tabs are {existing}"
            )
        wanted = [name for name in wanted if name in existing]

        # title/timezone are free here: gspread's Spreadsheet.__init__ has
        # already fetched the file's properties.
        values = self._batch_get(spreadsheet, wanted) if wanted else {}
        return SheetsWorkbook(
            spreadsheet_id=spreadsheet_id,
            title=spreadsheet.title,
            timezone=getattr(spreadsheet, "timezone", None),
            tab_names=existing,
            values=values,
        )

    def _batch_get(
        self, spreadsheet: Any, worksheet_names: Sequence[str]
    ) -> Dict[str, List[List[Any]]]:
        """One values:batchGet for `worksheet_names`, padded like
        get_all_values(). Titles must already be known to exist."""
        # absolute_range_name quotes the title and doubles any internal single
        # quote. A bare title breaks A1 notation on anything with a space in it,
        # which is most real tabs ('Week 2', 'Los Angeles', 'Form Responses 1').
        ranges = [absolute_range_name(name) for name in worksheet_names]
        logger.debug(
            f"Batch-getting {len(ranges)} worksheet(s) from {spreadsheet.id}"
        )
        response = spreadsheet.values_batch_get(ranges)
        value_ranges = response.get("valueRanges", [])

        # The API answers in request order, so zip is the mapping. Guard the
        # length anyway: a short response would otherwise shift every tab's
        # values onto the wrong name, which no caller could detect.
        if len(value_ranges) != len(worksheet_names):
            raise ConnectionError(
                f"Sheets returned {len(value_ranges)} range(s) for "
                f"{len(worksheet_names)} requested worksheet(s) in "
                f"{spreadsheet.id}"
            )

        result: Dict[str, List[List[Any]]] = {}
        for name, value_range in zip(worksheet_names, value_ranges):
            values = value_range.get("values", [])
            # fill_gaps([]) returns [[]], not [] -- an empty tab would arrive as
            # one blank row and a caller counting rows would see 1.
            result[name] = fill_gaps(values) if values else []
        return result

    def get_tabs_values(
        self,
        spreadsheet_id: str,
        worksheet_names: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
    ) -> Dict[str, List[List[Any]]]:
        """
        Get all values from SEVERAL worksheets of one spreadsheet in two requests.

        The values-only form of get_workbook(); use that one if you also want
        the file's title, time zone or full tab list, so you don't pay for the
        metadata twice.

        Reading N tabs with get_all_values() costs 3N Sheets requests, because
        gspread caches no metadata: both get_worksheet() and the Worksheet
        lookup inside it issue their own spreadsheets.get, and then the values
        read is a third. This method spends ONE metadata request (to resolve the
        tab list) plus ONE values:batchGet for every range, so the cost is 2 per
        FILE however many tabs are read.

        That matters because the Sheets read quota is 60 requests per MINUTE per
        USER, and "user" is the service account -- one bucket shared by every job
        running as it. A job looping over tabs reaches the ceiling at about 20
        tabs and the symptom is a 429 on an unrelated job (see ep-syncs'
        misc_jobs/sheet_volunteer_intake.py, which hit it on 2026-10-27).

        Values are padded to a rectangle, exactly as get_all_values() does
        (gspread's own fill_gaps, widest-row semantics), so this is a drop-in
        replacement for calling it per tab. A tab that is empty comes back as [].

        Args:
            spreadsheet_id: The Google Sheets spreadsheet ID
            worksheet_names: Worksheet titles to read; None reads every tab
            missing_ok: If True, a requested tab that does not exist is omitted
                from the result instead of raising

        Returns:
            Dict of worksheet name -> list of row lists, in the order requested

        Raises:
            ValueError: If a requested worksheet does not exist (unless missing_ok)
            ConnectionError: If not connected

        Examples:
            >>> connector = SheetsConnector()
            >>> tabs = connector.get_tabs_values('1ABC...', ['Week 2', 'Week 3'])
            >>> for name, rows in tabs.items():
            ...     print(name, len(rows))
        """
        return self.get_workbook(
            spreadsheet_id, worksheet_names, missing_ok=missing_ok
        ).values

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
