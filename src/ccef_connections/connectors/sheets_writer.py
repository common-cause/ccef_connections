"""
Google Sheets write connector for CCEF connections library.

Provides read/write access to Google Sheets for creating and updating
spreadsheets using service account authentication.
"""

import logging
from typing import Any, List, Optional

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as build_service

from ..core.base import BaseConnection
from ..core.retry import retry_google_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)


class SheetsWriterConnector(BaseConnection):
    """
    Google Sheets connector with read/write access.

    Uses the same GOOGLE_SHEETS_CREDENTIALS_PASSWORD credential as
    SheetsConnector but with write scopes. Provides helpers for
    creating/updating spreadsheets in bulk.

    Examples:
        >>> writer = SheetsWriterConnector()
        >>> ss = writer.get_or_create_spreadsheet("MA Precinct Demographics 2024")
        >>> writer.write_worksheet(ss, "Raw Data", [["col1", "col2"], [1, 2]])
        >>> print(ss.url)
    """

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self) -> None:
        super().__init__()
        self._credentials: Optional[Credentials] = None

    def connect(self) -> None:
        """
        Establish connection using service account credentials.

        Raises:
            CredentialError: If credentials are missing or invalid
            ConnectionError: If connection fails
        """
        try:
            creds_dict = self._credential_manager.get_google_sheets_credentials()
            self._credentials = Credentials.from_service_account_info(
                creds_dict, scopes=self.SCOPES
            )
            self._client = gspread.authorize(self._credentials)
            self._is_connected = True
            logger.info("Successfully connected to Google Sheets (write)")
        except CredentialError:
            logger.error("Failed to connect to Google Sheets: missing credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {str(e)}")
            raise ConnectionError(f"Failed to connect to Google Sheets: {str(e)}") from e

    def disconnect(self) -> None:
        """Close the connection."""
        self._client = None
        self._credentials = None
        self._is_connected = False
        logger.debug("Disconnected from Google Sheets (write)")

    def health_check(self) -> bool:
        return self._is_connected and self._client is not None

    def _ensure_connected(self) -> None:
        if not self._is_connected or self._client is None:
            self.connect()

    @retry_google_operation
    def get_or_create_spreadsheet(
        self, title: str, folder_id: Optional[str] = None
    ) -> gspread.Spreadsheet:
        """
        Open an existing spreadsheet by title, or create it if not found.

        When folder_id is provided, both the lookup and creation happen within
        that Drive folder — the service account's personal Drive quota is never
        touched.  Without folder_id, falls back to the service account's root
        (requires available SA quota).

        Args:
            title: Spreadsheet title to look up or create
            folder_id: Google Drive folder ID to scope the lookup/creation

        Returns:
            gspread.Spreadsheet instance
        """
        self._ensure_connected()

        if folder_id:
            # Search within the target folder so we never write to SA's root
            drive = build_service("drive", "v3", credentials=self._credentials)
            safe_title = title.replace("'", "\\'")
            results = drive.files().list(
                q=(
                    f"name = '{safe_title}'"
                    f" and '{folder_id}' in parents"
                    f" and mimeType = 'application/vnd.google-apps.spreadsheet'"
                    f" and trashed = false"
                ),
                fields="files(id, name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            ).execute()
            files = results.get("files", [])
            if files:
                ss = self._client.open_by_key(files[0]["id"])
                logger.info(f"Opened existing spreadsheet in folder: {title}")
                return ss
            # Create directly in the folder — bypasses SA quota
            file_meta = {
                "name": title,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            }
            new_file = drive.files().create(
                body=file_meta, fields="id", supportsAllDrives=True
            ).execute()
            ss = self._client.open_by_key(new_file["id"])
            logger.info(f"Created new spreadsheet in folder: {title}")
            return ss

        # Fallback: no folder specified — use SA's own Drive
        try:
            ss = self._client.open(title)
            logger.info(f"Opened existing spreadsheet: {title}")
            return ss
        except gspread.SpreadsheetNotFound:
            ss = self._client.create(title)
            logger.info(f"Created new spreadsheet: {title}")
            return ss

    @retry_google_operation
    def get_or_add_worksheet(
        self, spreadsheet: gspread.Spreadsheet, title: str
    ) -> gspread.Worksheet:
        """
        Return an existing worksheet by title, or add it if missing.

        Args:
            spreadsheet: gspread.Spreadsheet to look in
            title: Worksheet tab name

        Returns:
            gspread.Worksheet instance
        """
        try:
            ws = spreadsheet.worksheet(title)
            logger.debug(f"Found existing worksheet: {title}")
            return ws
        except gspread.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=title, rows=1, cols=1)
            logger.info(f"Added new worksheet: {title}")
            return ws

    @retry_google_operation
    def write_worksheet(
        self,
        spreadsheet: gspread.Spreadsheet,
        worksheet_name: str,
        data: List[List[Any]],
        value_input_option: str = "RAW",
    ) -> None:
        """
        Clear a worksheet and write a 2D list of values.

        Row 0 of data becomes the header row. Existing content is erased first.

        Args:
            spreadsheet: Target spreadsheet
            worksheet_name: Tab name to write to
            data: List of rows; each row is a list of cell values
            value_input_option: "RAW" (literal values) or "USER_ENTERED"
                (Sheets parses formulas, dates, numbers). Use USER_ENTERED
                whenever data contains formula strings starting with "=".
        """
        ws = self.get_or_add_worksheet(spreadsheet, worksheet_name)
        ws.clear()
        if data:
            ws.resize(rows=max(len(data), 1), cols=max(len(data[0]), 1))
            ws.update(
                range_name="A1",
                values=data,
                value_input_option=value_input_option,
            )
        logger.info(
            f"Wrote {len(data)} rows to '{worksheet_name}' in '{spreadsheet.title}'"
        )

    @retry_google_operation
    def delete_worksheet_if_exists(
        self, spreadsheet: gspread.Spreadsheet, title: str
    ) -> None:
        """
        Delete a worksheet by title if it exists (silently skips if not found).

        Args:
            spreadsheet: Target spreadsheet
            title: Tab name to delete
        """
        try:
            ws = spreadsheet.worksheet(title)
            spreadsheet.del_worksheet(ws)
            logger.info(f"Deleted worksheet: {title}")
        except gspread.WorksheetNotFound:
            pass

    @retry_google_operation
    def format_header_row(
        self, spreadsheet: gspread.Spreadsheet, worksheet_name: str
    ) -> None:
        """
        Bold and freeze row 1 of the named worksheet.

        Args:
            spreadsheet: Target spreadsheet
            worksheet_name: Tab name to format
        """
        ws = spreadsheet.worksheet(worksheet_name)
        ws.freeze(rows=1)
        ws.format("1:1", {"textFormat": {"bold": True}})
        logger.debug(f"Formatted header row in '{worksheet_name}'")

    @retry_google_operation
    def move_to_folder(
        self, spreadsheet: gspread.Spreadsheet, folder_id: str
    ) -> None:
        """
        Move a spreadsheet into a specific Google Drive folder.

        Safe to call on re-runs — if the file is already in the target folder,
        this is a no-op. Removes the file from any other parent folders.

        Args:
            spreadsheet: Spreadsheet to move
            folder_id: Google Drive folder ID (from the folder's URL)
        """
        self._ensure_connected()
        drive = build_service("drive", "v3", credentials=self._credentials)
        file_id = spreadsheet.id

        # Check current parents to avoid a redundant move
        meta = drive.files().get(fileId=file_id, fields="parents").execute()
        current_parents = meta.get("parents", [])

        if folder_id in current_parents:
            logger.debug(f"'{spreadsheet.title}' already in folder {folder_id}")
            return

        drive.files().update(
            fileId=file_id,
            addParents=folder_id,
            removeParents=",".join(current_parents),
            fields="id, parents",
        ).execute()
        logger.info(f"Moved '{spreadsheet.title}' to folder {folder_id}")
