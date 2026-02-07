"""Tests for the Google Sheets connector."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ccef_connections.connectors.sheets import SheetsConnector
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


@pytest.fixture
def connector():
    """Create a SheetsConnector with mocked credentials."""
    with patch.object(
        SheetsConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_google_sheets_credentials.return_value = FAKE_SERVICE_ACCOUNT
        c = SheetsConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a fake gspread client."""
    connector._client = MagicMock()
    connector._credentials = MagicMock()
    connector._is_connected = True
    return connector


# -- Initialization -----------------------------------------------------------


class TestInit:
    def test_initial_state(self):
        connector = SheetsConnector()
        assert connector._client is None
        assert connector._credentials is None
        assert not connector.is_connected()

    def test_scopes(self):
        assert SheetsConnector.SCOPES == [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]

    def test_repr_disconnected(self):
        connector = SheetsConnector()
        assert repr(connector) == "<SheetsConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<SheetsConnector status=connected>"


# -- Connect / Disconnect ----------------------------------------------------


class TestConnect:
    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
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
            FAKE_SERVICE_ACCOUNT, scopes=SheetsConnector.SCOPES
        )
        mock_authorize.assert_called_once_with(mock_creds)

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_connect_credential_error_reraises(
        self, mock_from_sa, mock_authorize, connector
    ):
        """CredentialError from credential manager is re-raised as-is."""
        connector._credential_manager.get_google_sheets_credentials.side_effect = (
            CredentialError("missing credentials")
        )

        with pytest.raises(CredentialError, match="missing credentials"):
            connector.connect()

        assert not connector.is_connected()
        mock_from_sa.assert_not_called()
        mock_authorize.assert_not_called()

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_connect_authorize_failure_raises_connection_error(
        self, mock_from_sa, mock_authorize, connector
    ):
        """Generic exception during authorize is wrapped in ConnectionError."""
        mock_from_sa.return_value = MagicMock()
        mock_authorize.side_effect = Exception("auth failed")

        with pytest.raises(ConnectionError, match="Failed to connect to Google Sheets"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_connect_from_service_account_info_failure(
        self, mock_from_sa, mock_authorize, connector
    ):
        """Exception during Credentials creation is wrapped in ConnectionError."""
        mock_from_sa.side_effect = ValueError("Invalid key")

        with pytest.raises(ConnectionError, match="Failed to connect to Google Sheets"):
            connector.connect()

        assert not connector.is_connected()


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._client is None
        assert connected_connector._credentials is None

    def test_disconnect_when_already_disconnected(self, connector):
        """Disconnect on an already-disconnected connector does not raise."""
        connector.disconnect()

        assert not connector.is_connected()
        assert connector._client is None
        assert connector._credentials is None


# -- Health Check -------------------------------------------------------------


class TestHealthCheck:
    def test_health_check_not_connected(self, connector):
        assert connector.health_check() is False

    def test_health_check_connected(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_health_check_connected_but_no_client(self, connector):
        """Edge case: _is_connected is True but _client is None."""
        connector._is_connected = True
        connector._client = None
        assert connector.health_check() is False

    def test_health_check_client_but_not_connected_flag(self, connector):
        """Edge case: _client exists but _is_connected is False."""
        connector._is_connected = False
        connector._client = MagicMock()
        assert connector.health_check() is False


# -- get_spreadsheet ----------------------------------------------------------


class TestGetSpreadsheet:
    def test_get_spreadsheet_success(self, connected_connector):
        mock_spreadsheet = MagicMock()
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert result is mock_spreadsheet
        connected_connector._client.open_by_key.assert_called_once_with(
            SAMPLE_SPREADSHEET_ID
        )

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_get_spreadsheet_auto_connects(
        self, mock_from_sa, mock_authorize, connector
    ):
        """get_spreadsheet auto-connects when not connected."""
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_client.open_by_key.return_value = mock_spreadsheet

        result = connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)

        assert result is mock_spreadsheet
        assert connector.is_connected()
        mock_authorize.assert_called_once()

    def test_get_spreadsheet_raises_when_client_none_after_connect(self, connector):
        """If connect() leaves _client as None, raises ConnectionError."""
        # Simulate connect() setting _is_connected but not _client
        with patch.object(connector, "connect") as mock_connect:
            mock_connect.side_effect = lambda: setattr(
                connector, "_is_connected", True
            )
            # _client remains None
            with pytest.raises(ConnectionError, match="Not connected to Google Sheets"):
                connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)

    def test_get_spreadsheet_open_by_key_error(self, connected_connector):
        """Exception from gspread propagates (tenacity may retry)."""
        connected_connector._client.open_by_key.side_effect = Exception(
            "Spreadsheet not found"
        )

        with pytest.raises(Exception, match="Spreadsheet not found"):
            connected_connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)


# -- get_worksheet ------------------------------------------------------------


class TestGetWorksheet:
    def test_get_worksheet_success(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet(SAMPLE_SPREADSHEET_ID, "Config")

        assert result is mock_worksheet
        connected_connector._client.open_by_key.assert_called_once_with(
            SAMPLE_SPREADSHEET_ID
        )
        mock_spreadsheet.worksheet.assert_called_once_with("Config")

    def test_get_worksheet_not_found(self, connected_connector):
        """If the worksheet name does not exist, gspread raises."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.side_effect = Exception(
            "Worksheet 'Missing' not found"
        )
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        with pytest.raises(Exception, match="Worksheet 'Missing' not found"):
            connected_connector.get_worksheet(SAMPLE_SPREADSHEET_ID, "Missing")


# -- get_range ----------------------------------------------------------------


class TestGetRange:
    def test_get_range_success(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [["Name", "Age"], ["Alice", "30"], ["Bob", "25"]]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:B3"
        )

        assert result == [["Name", "Age"], ["Alice", "30"], ["Bob", "25"]]
        mock_spreadsheet.values_get.assert_called_once_with("Sheet1!A1:B3")

    def test_get_range_empty(self, connected_connector):
        """values_get with no 'values' key returns empty list."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {}
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:B3"
        )

        assert result == []

    def test_get_range_empty_values_key(self, connected_connector):
        """values_get with empty 'values' list returns empty list."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {"values": []}
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:B3"
        )

        assert result == []


# -- get_all_values -----------------------------------------------------------


class TestGetAllValues:
    def test_get_all_values_success(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ["Name", "Email"],
            ["Alice", "alice@example.com"],
            ["Bob", "bob@example.com"],
        ]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_all_values(
            SAMPLE_SPREADSHEET_ID, "Config"
        )

        assert len(result) == 3
        assert result[0] == ["Name", "Email"]
        assert result[1] == ["Alice", "alice@example.com"]
        mock_spreadsheet.worksheet.assert_called_once_with("Config")
        mock_worksheet.get_all_values.assert_called_once()

    def test_get_all_values_empty(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = []
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_all_values(
            SAMPLE_SPREADSHEET_ID, "Empty"
        )

        assert result == []


# -- get_range_as_dicts -------------------------------------------------------


class TestGetRangeAsDicts:
    def test_normal_case(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [
                ["Name", "Age", "City"],
                ["Alice", "30", "NYC"],
                ["Bob", "25", "LA"],
            ]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:C3"
        )

        assert len(result) == 2
        assert result[0] == {"Name": "Alice", "Age": "30", "City": "NYC"}
        assert result[1] == {"Name": "Bob", "Age": "25", "City": "LA"}

    def test_empty_data(self, connected_connector):
        """Empty response returns empty list."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {}
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:C3"
        )

        assert result == []

    def test_only_header_row(self, connected_connector):
        """Data with only a header row and no data rows returns empty list."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [["Name", "Age", "City"]]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:C1"
        )

        assert result == []

    def test_short_rows_padded(self, connected_connector):
        """Rows shorter than the header are padded with empty strings."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [
                ["Name", "Age", "City"],
                ["Alice"],
                ["Bob", "25"],
            ]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:C3"
        )

        assert len(result) == 2
        assert result[0] == {"Name": "Alice", "Age": "", "City": ""}
        assert result[1] == {"Name": "Bob", "Age": "25", "City": ""}

    def test_custom_header_row(self, connected_connector):
        """Custom header_row skips rows before the header."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [
                ["Title line - ignore"],
                ["Name", "Age"],
                ["Alice", "30"],
                ["Bob", "25"],
            ]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:B4", header_row=1
        )

        assert len(result) == 2
        assert result[0] == {"Name": "Alice", "Age": "30"}
        assert result[1] == {"Name": "Bob", "Age": "25"}

    def test_custom_header_row_beyond_data(self, connected_connector):
        """header_row beyond data length returns empty list."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [["Name", "Age"]]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:B1", header_row=5
        )

        assert result == []

    def test_single_column(self, connected_connector):
        """Single-column data works correctly."""
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {
            "values": [
                ["ID"],
                ["1"],
                ["2"],
                ["3"],
            ]
        }
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_range_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1!A1:A4"
        )

        assert len(result) == 3
        assert result[0] == {"ID": "1"}
        assert result[2] == {"ID": "3"}


# -- get_worksheet_as_dicts ---------------------------------------------------


class TestGetWorksheetAsDicts:
    def test_normal_case(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ["Section", "Key", "Value"],
            ["general", "name", "CCEF"],
            ["general", "version", "1.0"],
        ]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Config"
        )

        assert len(result) == 2
        assert result[0] == {
            "Section": "general",
            "Key": "name",
            "Value": "CCEF",
        }
        assert result[1] == {
            "Section": "general",
            "Key": "version",
            "Value": "1.0",
        }

    def test_empty_worksheet(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = []
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Empty"
        )

        assert result == []

    def test_only_header_row(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [["A", "B", "C"]]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet_as_dicts(
            SAMPLE_SPREADSHEET_ID, "HeaderOnly"
        )

        assert result == []

    def test_short_rows_padded(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ["A", "B", "C"],
            ["1"],
            ["2", "3"],
        ]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1"
        )

        assert result[0] == {"A": "1", "B": "", "C": ""}
        assert result[1] == {"A": "2", "B": "3", "C": ""}

    def test_custom_header_row(self, connected_connector):
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ["Skip me"],
            ["Skip me too"],
            ["Name", "Role"],
            ["Alice", "Admin"],
        ]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        connected_connector._client.open_by_key.return_value = mock_spreadsheet

        result = connected_connector.get_worksheet_as_dicts(
            SAMPLE_SPREADSHEET_ID, "Sheet1", header_row=2
        )

        assert len(result) == 1
        assert result[0] == {"Name": "Alice", "Role": "Admin"}


# -- Auto-connect behavior ---------------------------------------------------


class TestAutoConnect:
    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_get_spreadsheet_auto_connects(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()

        assert not connector.is_connected()
        connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)
        assert connector.is_connected()

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_get_worksheet_auto_connects(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_client = MagicMock()
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()

        assert not connector.is_connected()
        connector.get_worksheet(SAMPLE_SPREADSHEET_ID, "Sheet1")
        assert connector.is_connected()

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_get_range_auto_connects(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_client = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.values_get.return_value = {"values": [["a"]]}
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()

        assert not connector.is_connected()
        connector.get_range(SAMPLE_SPREADSHEET_ID, "Sheet1!A1")
        assert connector.is_connected()

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_get_all_values_auto_connects(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_client = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = []
        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()

        assert not connector.is_connected()
        connector.get_all_values(SAMPLE_SPREADSHEET_ID, "Sheet1")
        assert connector.is_connected()

    def test_no_reconnect_when_already_connected(self, connected_connector):
        """If already connected, connect() is not called again."""
        with patch.object(connected_connector, "connect") as mock_connect:
            connected_connector.get_spreadsheet(SAMPLE_SPREADSHEET_ID)
            mock_connect.assert_not_called()


# -- Context Manager ----------------------------------------------------------


class TestContextManager:
    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_context_manager_connects_and_disconnects(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_authorize.return_value = MagicMock()
        mock_from_sa.return_value = MagicMock()

        with connector as c:
            assert c.is_connected()
            assert c is connector

        assert not connector.is_connected()
        assert connector._client is None
        assert connector._credentials is None

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_context_manager_disconnects_on_exception(
        self, mock_from_sa, mock_authorize, connector
    ):
        mock_authorize.return_value = MagicMock()
        mock_from_sa.return_value = MagicMock()

        with pytest.raises(RuntimeError):
            with connector as c:
                assert c.is_connected()
                raise RuntimeError("something broke")

        assert not connector.is_connected()
        assert connector._client is None

    @patch("ccef_connections.connectors.sheets.gspread.authorize")
    @patch("ccef_connections.connectors.sheets.Credentials.from_service_account_info")
    def test_context_manager_full_workflow(
        self, mock_from_sa, mock_authorize, connector
    ):
        """Full workflow: context manager -> get data -> auto-cleanup."""
        mock_client = MagicMock()
        mock_spreadsheet = MagicMock()
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ["Key", "Value"],
            ["env", "prod"],
        ]
        mock_spreadsheet.worksheet.return_value = mock_worksheet
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_authorize.return_value = mock_client
        mock_from_sa.return_value = MagicMock()

        with connector as c:
            result = c.get_worksheet_as_dicts(SAMPLE_SPREADSHEET_ID, "Config")

        assert result == [{"Key": "env", "Value": "prod"}]
        assert not connector.is_connected()
