"""Tests for the Airtable connector."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from ccef_connections.connectors.airtable import AirtableConnector
from ccef_connections.exceptions import ConnectionError, CredentialError


# -- Fixtures ----------------------------------------------------------------


FAKE_API_KEY = "patFAKEKEY123.abc"


@pytest.fixture
def connector():
    """Create an AirtableConnector with mocked credentials."""
    with patch.object(
        AirtableConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_airtable_key.return_value = FAKE_API_KEY
        c = AirtableConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a mock Api."""
    mock_api = MagicMock()
    connector._api = mock_api
    connector._is_connected = True
    return connector


# -- Initialization ----------------------------------------------------------


class TestInit:
    def test_initial_state(self):
        connector = AirtableConnector()
        assert connector._api is None
        assert not connector.is_connected()
        assert connector._is_connected is False

    def test_repr_disconnected(self):
        connector = AirtableConnector()
        assert repr(connector) == "<AirtableConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<AirtableConnector status=connected>"

    def test_inherits_base_connection(self):
        from ccef_connections.core.base import BaseConnection

        connector = AirtableConnector()
        assert isinstance(connector, BaseConnection)


# -- Connect / Disconnect ----------------------------------------------------


class TestConnect:
    @patch("ccef_connections.connectors.airtable.Api")
    def test_connect_success(self, mock_api_cls, connector):
        mock_api_instance = MagicMock()
        mock_api_cls.return_value = mock_api_instance

        connector.connect()

        assert connector.is_connected()
        assert connector._api is mock_api_instance
        mock_api_cls.assert_called_once_with(FAKE_API_KEY)
        connector._credential_manager.get_airtable_key.assert_called_once()

    def test_connect_missing_credentials(self):
        connector = AirtableConnector()
        connector._credential_manager.get_airtable_key = MagicMock(
            side_effect=CredentialError("Airtable API key not found")
        )

        with pytest.raises(CredentialError, match="Airtable API key not found"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.airtable.Api")
    def test_connect_api_construction_error(self, mock_api_cls, connector):
        """Non-CredentialError exceptions are wrapped in ConnectionError."""
        mock_api_cls.side_effect = RuntimeError("unexpected failure")

        with pytest.raises(ConnectionError, match="Failed to connect to Airtable"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.airtable.Api")
    def test_connect_wraps_generic_exception(self, mock_api_cls, connector):
        """Verify the original exception is chained via __cause__."""
        original = ValueError("bad value")
        mock_api_cls.side_effect = original

        with pytest.raises(ConnectionError) as exc_info:
            connector.connect()

        assert exc_info.value.__cause__ is original

    @patch("ccef_connections.connectors.airtable.Api")
    def test_connect_sets_connected_flag(self, mock_api_cls, connector):
        mock_api_cls.return_value = MagicMock()

        connector.connect()

        assert connector._is_connected is True


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        assert connected_connector.is_connected()
        assert connected_connector._api is not None

        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._api is None
        assert connected_connector._is_connected is False

    def test_disconnect_when_already_disconnected(self, connector):
        """Disconnect on a never-connected connector should not raise."""
        connector.disconnect()

        assert not connector.is_connected()
        assert connector._api is None


# -- Health Check ------------------------------------------------------------


class TestHealthCheck:
    def test_health_check_when_connected(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_health_check_when_not_connected(self, connector):
        assert connector.health_check() is False

    def test_health_check_connected_flag_but_no_api(self, connector):
        """Edge case: _is_connected is True but _api is None."""
        connector._is_connected = True
        connector._api = None

        assert connector.health_check() is False

    def test_health_check_api_exists_but_flag_false(self, connector):
        """Edge case: _api is set but _is_connected is False."""
        connector._api = MagicMock()
        connector._is_connected = False

        assert connector.health_check() is False

    def test_health_check_after_disconnect(self, connected_connector):
        connected_connector.disconnect()

        assert connected_connector.health_check() is False


# -- get_table ---------------------------------------------------------------


class TestGetTable:
    def test_get_table_returns_table(self, connected_connector):
        mock_table = MagicMock()
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.get_table("appABC123", "MyTable")

        assert result is mock_table
        connected_connector._api.table.assert_called_once_with("appABC123", "MyTable")

    @patch("ccef_connections.connectors.airtable.Api")
    def test_get_table_auto_connects(self, mock_api_cls, connector):
        """get_table should call connect() when not yet connected."""
        mock_api_instance = MagicMock()
        mock_table = MagicMock()
        mock_api_cls.return_value = mock_api_instance
        mock_api_instance.table.return_value = mock_table

        result = connector.get_table("appABC123", "MyTable")

        assert connector.is_connected()
        assert result is mock_table
        mock_api_cls.assert_called_once_with(FAKE_API_KEY)
        mock_api_instance.table.assert_called_once_with("appABC123", "MyTable")

    def test_get_table_does_not_reconnect_if_already_connected(self, connected_connector):
        """Should not call connect() again when already connected."""
        mock_table = MagicMock()
        connected_connector._api.table.return_value = mock_table

        with patch.object(connected_connector, "connect") as mock_connect:
            result = connected_connector.get_table("appABC123", "MyTable")

        mock_connect.assert_not_called()
        assert result is mock_table

    def test_get_table_raises_when_connect_fails(self, connector):
        """If auto-connect fails, the error should propagate."""
        connector._credential_manager.get_airtable_key.side_effect = CredentialError(
            "no key"
        )

        with pytest.raises(CredentialError, match="no key"):
            connector.get_table("appABC123", "MyTable")

    def test_get_table_passes_correct_base_and_table(self, connected_connector):
        connected_connector.get_table("appXYZ789", "Contacts")

        connected_connector._api.table.assert_called_once_with("appXYZ789", "Contacts")


# -- get_records -------------------------------------------------------------


class TestGetRecords:
    def test_get_records_basic(self, connected_connector):
        mock_table = MagicMock()
        expected_records = [
            {"id": "rec1", "fields": {"Name": "Alice"}},
            {"id": "rec2", "fields": {"Name": "Bob"}},
        ]
        mock_table.all.return_value = expected_records
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.get_records("appABC123", "People")

        assert result == expected_records
        mock_table.all.assert_called_once_with(
            formula=None, max_records=None, view=None
        )

    def test_get_records_with_formula(self, connected_connector):
        mock_table = MagicMock()
        mock_table.all.return_value = [{"id": "rec1", "fields": {"Status": "active"}}]
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.get_records(
            "appABC123", "Tasks", formula="{Status} = 'active'"
        )

        mock_table.all.assert_called_once_with(
            formula="{Status} = 'active'", max_records=None, view=None
        )
        assert len(result) == 1

    def test_get_records_with_max_records(self, connected_connector):
        mock_table = MagicMock()
        mock_table.all.return_value = [{"id": "rec1", "fields": {}}]
        connected_connector._api.table.return_value = mock_table

        connected_connector.get_records("appABC123", "Tasks", max_records=50)

        mock_table.all.assert_called_once_with(
            formula=None, max_records=50, view=None
        )

    def test_get_records_with_view(self, connected_connector):
        mock_table = MagicMock()
        mock_table.all.return_value = []
        connected_connector._api.table.return_value = mock_table

        connected_connector.get_records(
            "appABC123", "Tasks", view="Grid view"
        )

        mock_table.all.assert_called_once_with(
            formula=None, max_records=None, view="Grid view"
        )

    def test_get_records_with_all_params(self, connected_connector):
        mock_table = MagicMock()
        mock_table.all.return_value = []
        connected_connector._api.table.return_value = mock_table

        connected_connector.get_records(
            "appABC123",
            "Tasks",
            formula="{Done} = 1",
            max_records=10,
            view="Kanban",
        )

        mock_table.all.assert_called_once_with(
            formula="{Done} = 1", max_records=10, view="Kanban"
        )

    def test_get_records_empty_result(self, connected_connector):
        mock_table = MagicMock()
        mock_table.all.return_value = []
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.get_records("appABC123", "EmptyTable")

        assert result == []

    @patch("ccef_connections.connectors.airtable.Api")
    def test_get_records_auto_connects(self, mock_api_cls, connector):
        """get_records should auto-connect via get_table if not connected."""
        mock_api_instance = MagicMock()
        mock_table = MagicMock()
        mock_table.all.return_value = [{"id": "rec1"}]
        mock_api_cls.return_value = mock_api_instance
        mock_api_instance.table.return_value = mock_table

        result = connector.get_records("appABC123", "MyTable")

        assert connector.is_connected()
        assert result == [{"id": "rec1"}]


# -- update_record -----------------------------------------------------------


class TestUpdateRecord:
    def test_update_record_success(self, connected_connector):
        mock_table = MagicMock()
        updated = {"id": "recXXX", "fields": {"Status": "done"}}
        mock_table.update.return_value = updated
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.update_record(
            "appABC123", "Tasks", "recXXX", {"Status": "done"}
        )

        assert result == updated
        mock_table.update.assert_called_once_with("recXXX", {"Status": "done"})

    def test_update_record_passes_fields_correctly(self, connected_connector):
        mock_table = MagicMock()
        mock_table.update.return_value = {}
        connected_connector._api.table.return_value = mock_table

        fields = {"Name": "Updated Name", "Email": "new@example.com", "Score": 42}
        connected_connector.update_record("appABC123", "People", "rec123", fields)

        mock_table.update.assert_called_once_with("rec123", fields)

    def test_update_record_calls_get_table_with_correct_args(self, connected_connector):
        mock_table = MagicMock()
        mock_table.update.return_value = {}
        connected_connector._api.table.return_value = mock_table

        connected_connector.update_record(
            "appBASEID", "TargetTable", "recID", {"field": "value"}
        )

        connected_connector._api.table.assert_called_once_with(
            "appBASEID", "TargetTable"
        )


# -- batch_update ------------------------------------------------------------


class TestBatchUpdate:
    def test_batch_update_success(self, connected_connector):
        mock_table = MagicMock()
        records = [
            {"id": "rec1", "fields": {"Status": "done"}},
            {"id": "rec2", "fields": {"Status": "done"}},
        ]
        mock_table.batch_update.return_value = records
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.batch_update("appABC123", "Tasks", records)

        assert result == records
        mock_table.batch_update.assert_called_once_with(records)

    def test_batch_update_empty_list(self, connected_connector):
        mock_table = MagicMock()
        mock_table.batch_update.return_value = []
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.batch_update("appABC123", "Tasks", [])

        assert result == []
        mock_table.batch_update.assert_called_once_with([])

    def test_batch_update_multiple_records(self, connected_connector):
        mock_table = MagicMock()
        records = [
            {"id": f"rec{i}", "fields": {"Index": i}} for i in range(10)
        ]
        mock_table.batch_update.return_value = records
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.batch_update("appABC123", "Data", records)

        assert len(result) == 10
        mock_table.batch_update.assert_called_once_with(records)

    def test_batch_update_calls_get_table_correctly(self, connected_connector):
        mock_table = MagicMock()
        mock_table.batch_update.return_value = []
        connected_connector._api.table.return_value = mock_table

        connected_connector.batch_update("appXYZ", "BatchTable", [])

        connected_connector._api.table.assert_called_once_with("appXYZ", "BatchTable")


# -- create_record -----------------------------------------------------------


class TestCreateRecord:
    def test_create_record_success(self, connected_connector):
        mock_table = MagicMock()
        created = {"id": "recNEW", "fields": {"Name": "Alice", "Email": "a@b.com"}}
        mock_table.create.return_value = created
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.create_record(
            "appABC123", "People", {"Name": "Alice", "Email": "a@b.com"}
        )

        assert result == created
        mock_table.create.assert_called_once_with(
            {"Name": "Alice", "Email": "a@b.com"}
        )

    def test_create_record_with_empty_fields(self, connected_connector):
        mock_table = MagicMock()
        mock_table.create.return_value = {"id": "recNEW", "fields": {}}
        connected_connector._api.table.return_value = mock_table

        result = connected_connector.create_record("appABC123", "Tasks", {})

        assert result["id"] == "recNEW"
        mock_table.create.assert_called_once_with({})

    def test_create_record_calls_get_table_correctly(self, connected_connector):
        mock_table = MagicMock()
        mock_table.create.return_value = {"id": "recNEW", "fields": {}}
        connected_connector._api.table.return_value = mock_table

        connected_connector.create_record("appBASE", "NewTable", {"Key": "Val"})

        connected_connector._api.table.assert_called_once_with("appBASE", "NewTable")

    @patch("ccef_connections.connectors.airtable.Api")
    def test_create_record_auto_connects(self, mock_api_cls, connector):
        mock_api_instance = MagicMock()
        mock_table = MagicMock()
        mock_table.create.return_value = {"id": "recNEW", "fields": {"X": 1}}
        mock_api_cls.return_value = mock_api_instance
        mock_api_instance.table.return_value = mock_table

        result = connector.create_record("appABC123", "T", {"X": 1})

        assert connector.is_connected()
        assert result["id"] == "recNEW"


# -- Context Manager ---------------------------------------------------------


class TestContextManager:
    @patch("ccef_connections.connectors.airtable.Api")
    def test_context_manager_connects_and_disconnects(self, mock_api_cls, connector):
        mock_api_cls.return_value = MagicMock()

        with connector as c:
            assert c.is_connected()
            assert c._api is not None

        assert not c.is_connected()
        assert c._api is None

    @patch("ccef_connections.connectors.airtable.Api")
    def test_context_manager_returns_self(self, mock_api_cls, connector):
        mock_api_cls.return_value = MagicMock()

        with connector as c:
            assert c is connector

    @patch("ccef_connections.connectors.airtable.Api")
    def test_context_manager_disconnects_on_exception(self, mock_api_cls, connector):
        mock_api_cls.return_value = MagicMock()

        with pytest.raises(ValueError, match="test error"):
            with connector as c:
                assert c.is_connected()
                raise ValueError("test error")

        assert not connector.is_connected()
        assert connector._api is None

    @patch("ccef_connections.connectors.airtable.Api")
    def test_context_manager_allows_operations(self, mock_api_cls, connector):
        """Full round-trip: enter context, call get_table, exit."""
        mock_api_instance = MagicMock()
        mock_table = MagicMock()
        mock_api_cls.return_value = mock_api_instance
        mock_api_instance.table.return_value = mock_table

        with connector as c:
            table = c.get_table("appABC123", "MyTable")
            assert table is mock_table

        assert not connector.is_connected()


# -- Retry decorator integration ---------------------------------------------


class TestRetryDecoratorIntegration:
    """Verify that the retry decorator is applied to the right methods."""

    def test_get_records_has_retry(self):
        """get_records should be wrapped by tenacity retry."""
        assert hasattr(AirtableConnector.get_records, "retry")

    def test_update_record_has_retry(self):
        assert hasattr(AirtableConnector.update_record, "retry")

    def test_batch_update_has_retry(self):
        assert hasattr(AirtableConnector.batch_update, "retry")

    def test_create_record_has_retry(self):
        assert hasattr(AirtableConnector.create_record, "retry")

    def test_get_table_has_no_retry(self):
        """get_table should NOT have retry logic."""
        assert not hasattr(AirtableConnector.get_table, "retry")
