"""Tests for the Google Sheets write connector."""

from unittest.mock import MagicMock, patch

import gspread
import pytest

from ccef_connections.connectors.sheets_writer import SheetsWriterConnector
from ccef_connections.exceptions import ConnectionError, CredentialError


# -- Fixtures ----------------------------------------------------------------


FAKE_SERVICE_ACCOUNT = {
    "type": "service_account",
    "project_id": "test-project",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}

SAMPLE_SPREADSHEET_ID = "1aBcDeFgHiJkLmNoPqRsTuVwXyZ"
SAMPLE_FOLDER_ID = "0ABcDeFgHiJkLmNoPqRs"


@pytest.fixture
def connector():
    """Create a SheetsWriterConnector with mocked credentials."""
    with patch.object(
        SheetsWriterConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_google_sheets_credentials.return_value = FAKE_SERVICE_ACCOUNT
        c = SheetsWriterConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """A connector already 'connected' with a fake gspread client."""
    connector._client = MagicMock()
    connector._credentials = MagicMock()
    connector._is_connected = True
    return connector


@pytest.fixture
def spreadsheet():
    """A stand-in gspread.Spreadsheet."""
    ss = MagicMock()
    ss.title = "Test Sheet"
    ss.id = SAMPLE_SPREADSHEET_ID
    return ss


def _worksheet_not_found():
    """gspread.WorksheetNotFound, constructed the way gspread raises it."""
    return gspread.WorksheetNotFound("Missing")


# -- Initialization ----------------------------------------------------------


class TestInit:
    def test_initial_state(self):
        connector = SheetsWriterConnector()
        assert connector._client is None
        assert connector._credentials is None
        assert not connector.is_connected()

    def test_scopes_are_read_write(self):
        """Unlike SheetsConnector, these scopes are not '.readonly'."""
        assert SheetsWriterConnector.SCOPES == [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

    def test_repr_disconnected(self):
        assert (
            repr(SheetsWriterConnector())
            == "<SheetsWriterConnector status=disconnected>"
        )

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<SheetsWriterConnector status=connected>"


# -- Connect / Disconnect ----------------------------------------------------


class TestConnect:
    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_connect_success(self, mock_from_sa, mock_authorize, connector):
        mock_creds = MagicMock()
        mock_from_sa.return_value = mock_creds
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client

        connector.connect()

        assert connector.is_connected()
        assert connector._client is mock_client
        assert connector._credentials is mock_creds
        mock_from_sa.assert_called_once_with(
            FAKE_SERVICE_ACCOUNT, scopes=SheetsWriterConnector.SCOPES
        )
        mock_authorize.assert_called_once_with(mock_creds)

    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_connect_credential_error_reraises(
        self, mock_from_sa, mock_authorize, connector
    ):
        """CredentialError passes through rather than becoming ConnectionError."""
        connector._credential_manager.get_google_sheets_credentials.side_effect = (
            CredentialError("missing credentials")
        )

        with pytest.raises(CredentialError, match="missing credentials"):
            connector.connect()

        assert not connector.is_connected()
        mock_from_sa.assert_not_called()
        mock_authorize.assert_not_called()

    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_connect_authorize_failure_raises_connection_error(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_from_sa.return_value = MagicMock()
        mock_authorize.side_effect = Exception("auth failed")

        with pytest.raises(ConnectionError, match="Failed to connect to Google Sheets"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_connect_bad_key_raises_connection_error(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_from_sa.side_effect = ValueError("Invalid key")

        with pytest.raises(ConnectionError, match="Failed to connect to Google Sheets"):
            connector.connect()

        assert not connector.is_connected()


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()

        assert connected_connector._client is None
        assert connected_connector._credentials is None
        assert not connected_connector.is_connected()

    def test_disconnect_is_idempotent(self, connector):
        connector.disconnect()
        connector.disconnect()

        assert not connector.is_connected()


class TestHealthCheck:
    def test_healthy_when_connected_with_client(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_unhealthy_when_disconnected(self, connector):
        assert connector.health_check() is False

    def test_unhealthy_when_client_is_none(self, connected_connector):
        connected_connector._client = None
        assert connected_connector.health_check() is False


class TestEnsureConnected:
    def test_connects_when_not_connected(self, connector):
        with patch.object(connector, "connect") as mock_connect:
            connector._ensure_connected()

        mock_connect.assert_called_once()

    def test_no_op_when_already_connected(self, connected_connector):
        with patch.object(connected_connector, "connect") as mock_connect:
            connected_connector._ensure_connected()

        mock_connect.assert_not_called()

    def test_reconnects_when_client_went_none(self, connected_connector):
        """_is_connected True but no client still triggers a reconnect."""
        connected_connector._client = None

        with patch.object(connected_connector, "connect") as mock_connect:
            connected_connector._ensure_connected()

        mock_connect.assert_called_once()


# -- get_or_create_spreadsheet -----------------------------------------------


class TestGetOrCreateSpreadsheet:
    def test_opens_existing_by_title(self, connected_connector, spreadsheet):
        connected_connector._client.open.return_value = spreadsheet

        result = connected_connector.get_or_create_spreadsheet("Test Sheet")

        assert result is spreadsheet
        connected_connector._client.open.assert_called_once_with("Test Sheet")
        connected_connector._client.create.assert_not_called()

    def test_creates_when_title_not_found(self, connected_connector, spreadsheet):
        connected_connector._client.open.side_effect = gspread.SpreadsheetNotFound(
            "nope"
        )
        connected_connector._client.create.return_value = spreadsheet

        result = connected_connector.get_or_create_spreadsheet("Test Sheet")

        assert result is spreadsheet
        connected_connector._client.create.assert_called_once_with("Test Sheet")

    def test_auto_connects(self, connector, spreadsheet):
        """The method connects itself rather than requiring an explicit connect()."""

        def fake_connect():
            connector._client = MagicMock()
            connector._client.open.return_value = spreadsheet
            connector._is_connected = True

        with patch.object(connector, "connect", side_effect=fake_connect):
            result = connector.get_or_create_spreadsheet("Test Sheet")

        assert result is spreadsheet

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_folder_scoped_lookup_returns_existing(
        self, mock_build, connected_connector, spreadsheet
    ):
        """With folder_id, an existing file is opened by key, not by title."""
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.list.return_value.execute.return_value = {
            "files": [{"id": "file-1", "name": "Test Sheet"}]
        }
        connected_connector._client.open_by_key.return_value = spreadsheet

        result = connected_connector.get_or_create_spreadsheet(
            "Test Sheet", folder_id=SAMPLE_FOLDER_ID
        )

        assert result is spreadsheet
        connected_connector._client.open_by_key.assert_called_once_with("file-1")
        # Never falls back to the service account's own Drive
        connected_connector._client.open.assert_not_called()
        connected_connector._client.create.assert_not_called()
        drive.files.return_value.create.assert_not_called()

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_folder_scoped_query_is_correctly_constrained(
        self, mock_build, connected_connector, spreadsheet
    ):
        """The Drive query must pin folder, mime type, and trashed=false.

        A query missing any of these could match a same-named file elsewhere in
        Drive and silently write to the wrong spreadsheet.
        """
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.list.return_value.execute.return_value = {"files": []}
        drive.files.return_value.create.return_value.execute.return_value = {
            "id": "new-file"
        }
        connected_connector._client.open_by_key.return_value = spreadsheet

        connected_connector.get_or_create_spreadsheet(
            "Test Sheet", folder_id=SAMPLE_FOLDER_ID
        )

        kwargs = drive.files.return_value.list.call_args.kwargs
        query = kwargs["q"]
        assert "name = 'Test Sheet'" in query
        assert f"'{SAMPLE_FOLDER_ID}' in parents" in query
        assert "mimeType = 'application/vnd.google-apps.spreadsheet'" in query
        assert "trashed = false" in query
        # Shared drives must be searchable, or the lookup misses and we create a dupe
        assert kwargs["includeItemsFromAllDrives"] is True
        assert kwargs["supportsAllDrives"] is True

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_folder_scoped_creation_targets_the_folder(
        self, mock_build, connected_connector, spreadsheet
    ):
        """Creation goes through Drive with the folder as parent.

        Creating via gspread instead would land the file in the service
        account's own Drive and consume its quota.
        """
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.list.return_value.execute.return_value = {"files": []}
        drive.files.return_value.create.return_value.execute.return_value = {
            "id": "new-file"
        }
        connected_connector._client.open_by_key.return_value = spreadsheet

        result = connected_connector.get_or_create_spreadsheet(
            "Test Sheet", folder_id=SAMPLE_FOLDER_ID
        )

        assert result is spreadsheet
        create_kwargs = drive.files.return_value.create.call_args.kwargs
        assert create_kwargs["body"] == {
            "name": "Test Sheet",
            "mimeType": "application/vnd.google-apps.spreadsheet",
            "parents": [SAMPLE_FOLDER_ID],
        }
        assert create_kwargs["supportsAllDrives"] is True
        connected_connector._client.open_by_key.assert_called_once_with("new-file")
        connected_connector._client.create.assert_not_called()

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_apostrophe_in_title_is_escaped_for_the_drive_query(
        self, mock_build, connected_connector, spreadsheet
    ):
        """An unescaped apostrophe would terminate the query string early."""
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.list.return_value.execute.return_value = {
            "files": [{"id": "file-1"}]
        }
        connected_connector._client.open_by_key.return_value = spreadsheet

        connected_connector.get_or_create_spreadsheet(
            "Rob's Sheet", folder_id=SAMPLE_FOLDER_ID
        )

        query = drive.files.return_value.list.call_args.kwargs["q"]
        assert "name = 'Rob\\'s Sheet'" in query

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.get_or_create_spreadsheet, "retry")


# -- open_spreadsheet --------------------------------------------------------


class TestOpenSpreadsheet:
    def test_opens_by_key(self, connected_connector, spreadsheet):
        connected_connector._client.open_by_key.return_value = spreadsheet

        result = connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert result is spreadsheet
        connected_connector._client.open_by_key.assert_called_once_with(
            SAMPLE_SPREADSHEET_ID
        )

    def test_auto_connects(self, connector, spreadsheet):
        def fake_connect():
            connector._client = MagicMock()
            connector._client.open_by_key.return_value = spreadsheet
            connector._is_connected = True

        with patch.object(connector, "connect", side_effect=fake_connect):
            result = connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert result is spreadsheet

    def test_propagates_not_found(self, connected_connector):
        connected_connector._client.open_by_key.side_effect = (
            gspread.SpreadsheetNotFound("nope")
        )

        with pytest.raises(gspread.SpreadsheetNotFound):
            connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.open_spreadsheet, "retry")


# -- get_range ---------------------------------------------------------------


class TestGetRange:
    def test_reads_range_from_named_worksheet(self, connected_connector, spreadsheet):
        ws = MagicMock()
        ws.get.return_value = [["a", "b"], ["c", "d"]]
        spreadsheet.worksheet.return_value = ws

        result = connected_connector.get_range(spreadsheet, "Data", "A1:B2")

        assert result == [["a", "b"], ["c", "d"]]
        spreadsheet.worksheet.assert_called_once_with("Data")
        ws.get.assert_called_once_with("A1:B2")

    def test_short_rows_are_returned_as_is(self, connected_connector, spreadsheet):
        """Sheets truncates trailing empties; the connector must not pad."""
        ws = MagicMock()
        ws.get.return_value = [["a", "b"], ["c"]]
        spreadsheet.worksheet.return_value = ws

        assert connected_connector.get_range(spreadsheet, "Data", "A1:B2") == [
            ["a", "b"],
            ["c"],
        ]

    def test_empty_range_returns_empty_list(self, connected_connector, spreadsheet):
        ws = MagicMock()
        ws.get.return_value = []
        spreadsheet.worksheet.return_value = ws

        assert connected_connector.get_range(spreadsheet, "Data", "Z1:Z9") == []

    def test_propagates_worksheet_not_found(self, connected_connector, spreadsheet):
        spreadsheet.worksheet.side_effect = _worksheet_not_found()

        with pytest.raises(gspread.WorksheetNotFound):
            connected_connector.get_range(spreadsheet, "Missing", "A1:B2")

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.get_range, "retry")


# -- update_cell -------------------------------------------------------------


class TestUpdateCell:
    def test_updates_single_cell(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        result = connected_connector.update_cell(spreadsheet, "Data", 4, 2, "hello")

        assert result is None
        spreadsheet.worksheet.assert_called_once_with("Data")
        ws.update_cell.assert_called_once_with(4, 2, "hello")

    def test_does_not_clear_the_worksheet(self, connected_connector, spreadsheet):
        """This is the whole point of update_cell vs write_worksheet."""
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.update_cell(spreadsheet, "Data", 1, 1, "x")

        ws.clear.assert_not_called()
        ws.resize.assert_not_called()

    def test_propagates_worksheet_not_found(self, connected_connector, spreadsheet):
        spreadsheet.worksheet.side_effect = _worksheet_not_found()

        with pytest.raises(gspread.WorksheetNotFound):
            connected_connector.update_cell(spreadsheet, "Missing", 1, 1, "x")

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.update_cell, "retry")


# -- get_or_add_worksheet ----------------------------------------------------


class TestGetOrAddWorksheet:
    def test_returns_existing_worksheet(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        result = connected_connector.get_or_add_worksheet(spreadsheet, "Data")

        assert result is ws
        spreadsheet.add_worksheet.assert_not_called()

    def test_adds_worksheet_when_missing(self, connected_connector, spreadsheet):
        new_ws = MagicMock()
        spreadsheet.worksheet.side_effect = _worksheet_not_found()
        spreadsheet.add_worksheet.return_value = new_ws

        result = connected_connector.get_or_add_worksheet(spreadsheet, "Data")

        assert result is new_ws
        spreadsheet.add_worksheet.assert_called_once_with(title="Data", rows=1, cols=1)

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.get_or_add_worksheet, "retry")


# -- write_worksheet ---------------------------------------------------------


class TestWriteWorksheet:
    def test_clears_resizes_and_writes(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws
        data = [["h1", "h2"], [1, 2], [3, 4]]

        connected_connector.write_worksheet(spreadsheet, "Data", data)

        ws.clear.assert_called_once()
        ws.resize.assert_called_once_with(rows=3, cols=2)
        ws.update.assert_called_once_with(
            range_name="A1", values=data, value_input_option="RAW"
        )

    def test_defaults_to_raw_input(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.write_worksheet(spreadsheet, "Data", [["=A1+1"]])

        assert ws.update.call_args.kwargs["value_input_option"] == "RAW"

    def test_user_entered_is_passed_through(self, connected_connector, spreadsheet):
        """USER_ENTERED is what makes formula strings evaluate."""
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.write_worksheet(
            spreadsheet, "Data", [["=A1+1"]], value_input_option="USER_ENTERED"
        )

        assert ws.update.call_args.kwargs["value_input_option"] == "USER_ENTERED"

    def test_empty_data_clears_without_writing(self, connected_connector, spreadsheet):
        """No rows means clear the tab and stop — resize(rows=0) would raise."""
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.write_worksheet(spreadsheet, "Data", [])

        ws.clear.assert_called_once()
        ws.resize.assert_not_called()
        ws.update.assert_not_called()

    def test_single_empty_row_resizes_to_at_least_one_column(
        self, connected_connector, spreadsheet
    ):
        """A row with no cells must not resize to 0 columns."""
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.write_worksheet(spreadsheet, "Data", [[]])

        ws.resize.assert_called_once_with(rows=1, cols=1)

    def test_creates_the_worksheet_when_missing(self, connected_connector, spreadsheet):
        new_ws = MagicMock()
        spreadsheet.worksheet.side_effect = _worksheet_not_found()
        spreadsheet.add_worksheet.return_value = new_ws

        connected_connector.write_worksheet(spreadsheet, "New Tab", [["a"]])

        spreadsheet.add_worksheet.assert_called_once_with(
            title="New Tab", rows=1, cols=1
        )
        new_ws.clear.assert_called_once()
        new_ws.update.assert_called_once()

    def test_column_count_comes_from_the_header_row(
        self, connected_connector, spreadsheet
    ):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.write_worksheet(
            spreadsheet, "Data", [["a", "b", "c"], ["1"]]
        )

        ws.resize.assert_called_once_with(rows=2, cols=3)

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.write_worksheet, "retry")


# -- delete_worksheet_if_exists ----------------------------------------------


class TestDeleteWorksheetIfExists:
    def test_deletes_existing_worksheet(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.delete_worksheet_if_exists(spreadsheet, "Data")

        spreadsheet.del_worksheet.assert_called_once_with(ws)

    def test_silently_skips_when_missing(self, connected_connector, spreadsheet):
        spreadsheet.worksheet.side_effect = _worksheet_not_found()

        connected_connector.delete_worksheet_if_exists(spreadsheet, "Missing")

        spreadsheet.del_worksheet.assert_not_called()

    def test_other_errors_are_not_swallowed(self, connected_connector, spreadsheet):
        """Only WorksheetNotFound is tolerated; a real failure must surface."""
        spreadsheet.worksheet.return_value = MagicMock()
        spreadsheet.del_worksheet.side_effect = RuntimeError("permission denied")

        with pytest.raises(RuntimeError, match="permission denied"):
            connected_connector.delete_worksheet_if_exists(spreadsheet, "Data")

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.delete_worksheet_if_exists, "retry")


# -- format_header_row -------------------------------------------------------


class TestFormatHeaderRow:
    def test_freezes_and_bolds_row_one(self, connected_connector, spreadsheet):
        ws = MagicMock()
        spreadsheet.worksheet.return_value = ws

        connected_connector.format_header_row(spreadsheet, "Data")

        ws.freeze.assert_called_once_with(rows=1)
        ws.format.assert_called_once_with("1:1", {"textFormat": {"bold": True}})

    def test_propagates_worksheet_not_found(self, connected_connector, spreadsheet):
        spreadsheet.worksheet.side_effect = _worksheet_not_found()

        with pytest.raises(gspread.WorksheetNotFound):
            connected_connector.format_header_row(spreadsheet, "Missing")

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.format_header_row, "retry")


# -- move_to_folder ----------------------------------------------------------


class TestMoveToFolder:
    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_moves_and_strips_previous_parents(
        self, mock_build, connected_connector, spreadsheet
    ):
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.get.return_value.execute.return_value = {
            "parents": ["old-1", "old-2"]
        }

        connected_connector.move_to_folder(spreadsheet, SAMPLE_FOLDER_ID)

        kwargs = drive.files.return_value.update.call_args.kwargs
        assert kwargs["fileId"] == SAMPLE_SPREADSHEET_ID
        assert kwargs["addParents"] == SAMPLE_FOLDER_ID
        assert kwargs["removeParents"] == "old-1,old-2"

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_no_op_when_already_in_target_folder(
        self, mock_build, connected_connector, spreadsheet
    ):
        """Safe on re-runs — this is what makes the sync idempotent."""
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.get.return_value.execute.return_value = {
            "parents": [SAMPLE_FOLDER_ID, "other"]
        }

        connected_connector.move_to_folder(spreadsheet, SAMPLE_FOLDER_ID)

        drive.files.return_value.update.assert_not_called()

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_handles_file_with_no_parents(
        self, mock_build, connected_connector, spreadsheet
    ):
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.get.return_value.execute.return_value = {}

        connected_connector.move_to_folder(spreadsheet, SAMPLE_FOLDER_ID)

        assert drive.files.return_value.update.call_args.kwargs["removeParents"] == ""

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_auto_connects(self, mock_build, connector, spreadsheet):
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.get.return_value.execute.return_value = {"parents": []}

        def fake_connect():
            connector._client = MagicMock()
            connector._credentials = MagicMock()
            connector._is_connected = True

        with patch.object(connector, "connect", side_effect=fake_connect) as mock_conn:
            connector.move_to_folder(spreadsheet, SAMPLE_FOLDER_ID)

        mock_conn.assert_called_once()

    @patch("ccef_connections.connectors.sheets_writer.build_service")
    def test_builds_drive_client_with_the_write_credentials(
        self, mock_build, connected_connector, spreadsheet
    ):
        drive = MagicMock()
        mock_build.return_value = drive
        drive.files.return_value.get.return_value.execute.return_value = {"parents": []}

        connected_connector.move_to_folder(spreadsheet, SAMPLE_FOLDER_ID)

        mock_build.assert_called_once_with(
            "drive", "v3", credentials=connected_connector._credentials
        )

    def test_has_retry_decorator(self):
        assert hasattr(SheetsWriterConnector.move_to_folder, "retry")


# -- Retry behavior ----------------------------------------------------------


class TestRetry:
    """These methods share retry_google_operation, which retries 429 only.

    Writes here are not idempotent — get_or_create_spreadsheet can create a
    duplicate spreadsheet — so anything other than a rate limit must surface on
    the first attempt rather than be replayed.
    """

    @staticmethod
    def _api_error(status_code):
        response = MagicMock()
        response.status_code = status_code
        response.json.return_value = {
            "error": {"code": status_code, "message": "boom", "status": "ERROR"}
        }
        return gspread.exceptions.APIError(response)

    @patch("tenacity.nap.time.sleep")
    def test_retries_gspread_429(self, mock_sleep, connected_connector, spreadsheet):
        connected_connector._client.open_by_key.side_effect = [
            self._api_error(429),
            spreadsheet,
        ]

        result = connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert result is spreadsheet
        assert connected_connector._client.open_by_key.call_count == 2

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_gspread_500(self, mock_sleep, connected_connector):
        """A 5xx leaves the write in an unknown state — do not replay it."""
        connected_connector._client.open_by_key.side_effect = self._api_error(500)

        with pytest.raises(gspread.exceptions.APIError):
            connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert connected_connector._client.open_by_key.call_count == 1
        assert not mock_sleep.called

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_gspread_404(self, mock_sleep, connected_connector):
        connected_connector._client.open_by_key.side_effect = self._api_error(404)

        with pytest.raises(gspread.exceptions.APIError):
            connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert connected_connector._client.open_by_key.call_count == 1

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_worksheet_not_found(
        self, mock_sleep, connected_connector, spreadsheet
    ):
        spreadsheet.worksheet.side_effect = _worksheet_not_found()

        with pytest.raises(gspread.WorksheetNotFound):
            connected_connector.get_range(spreadsheet, "Missing", "A1:B2")

        assert spreadsheet.worksheet.call_count == 1
        assert not mock_sleep.called

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_generic_exception(self, mock_sleep, connected_connector):
        """Regression guard: this decorator once retried bare Exception."""
        connected_connector._client.open_by_key.side_effect = RuntimeError("bug")

        with pytest.raises(RuntimeError, match="bug"):
            connected_connector.open_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert connected_connector._client.open_by_key.call_count == 1
        assert not mock_sleep.called


# -- Context manager ---------------------------------------------------------


class TestContextManager:
    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_connects_and_disconnects(self, mock_from_sa, mock_authorize, connector):
        mock_from_sa.return_value = MagicMock()
        mock_authorize.return_value = MagicMock()

        with connector as conn:
            assert conn.is_connected()

        assert not connector.is_connected()
        assert connector._client is None

    @patch("ccef_connections.connectors.sheets_writer.gspread.authorize")
    @patch(
        "ccef_connections.connectors.sheets_writer.Credentials.from_service_account_info"
    )
    def test_disconnects_on_exception(self, mock_from_sa, mock_authorize, connector):
        mock_from_sa.return_value = MagicMock()
        mock_authorize.return_value = MagicMock()

        with pytest.raises(ValueError):
            with connector:
                raise ValueError("boom")

        assert not connector.is_connected()


# -- Write-surface guard -----------------------------------------------------


class TestWriteSurface:
    def test_write_worksheet_is_destructive_by_design(self):
        """Documented contract: write_worksheet clears the tab first.

        Callers that must preserve neighbouring content use update_cell. If
        write_worksheet ever stops clearing, that is a behavior change callers
        depend on, not a fix.
        """
        import inspect

        source = inspect.getsource(SheetsWriterConnector.write_worksheet)
        assert "ws.clear()" in source

    def test_all_public_api_methods_carry_retry(self):
        """Every method that hits the Sheets/Drive API must be decorated."""
        expected = [
            "get_or_create_spreadsheet",
            "open_spreadsheet",
            "get_range",
            "update_cell",
            "get_or_add_worksheet",
            "write_worksheet",
            "delete_worksheet_if_exists",
            "format_header_row",
            "move_to_folder",
        ]
        undecorated = [
            name
            for name in expected
            if not hasattr(getattr(SheetsWriterConnector, name), "retry")
        ]
        assert undecorated == []
