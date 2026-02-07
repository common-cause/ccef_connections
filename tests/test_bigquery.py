"""Tests for the BigQuery connector."""

from unittest.mock import MagicMock, PropertyMock, patch, call

import pytest
from tenacity import stop_after_attempt

from ccef_connections.connectors.bigquery import BigQueryConnector
from ccef_connections.exceptions import (
    ConnectionError,
    CredentialError,
    QueryError,
    WriteError,
)


# Disable tenacity retries on all @retry_google_operation-decorated methods
# so that tests fail fast instead of retrying with exponential backoff.
for _method_name in ("query", "query_to_dataframe", "table_exists",
                      "insert_rows", "load_dataframe", "execute_dml"):
    _method = getattr(BigQueryConnector, _method_name)
    if hasattr(_method, "retry"):
        _method.retry.stop = stop_after_attempt(1)


# -- Fixtures ----------------------------------------------------------------


FAKE_CREDS_DICT = {
    "type": "service_account",
    "project_id": "test-project-from-creds",
    "private_key_id": "key123",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "123456789",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


@pytest.fixture
def connector():
    """Create a BigQueryConnector with mocked credentials (no project_id)."""
    with patch.object(
        BigQueryConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_bigquery_credentials.return_value = FAKE_CREDS_DICT.copy()
        c = BigQueryConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connector_with_project():
    """Create a BigQueryConnector with explicit project_id and mocked credentials."""
    with patch.object(
        BigQueryConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_bigquery_credentials.return_value = FAKE_CREDS_DICT.copy()
        c = BigQueryConnector(project_id="my-explicit-project")
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector_with_project):
    """Create a connector that is already 'connected' with a mock client."""
    mock_client = MagicMock()
    connector_with_project._client = mock_client
    connector_with_project._is_connected = True
    connector_with_project._credentials = MagicMock()
    return connector_with_project


# -- Initialization -----------------------------------------------------------


class TestInit:
    def test_init_without_project_id(self):
        c = BigQueryConnector()
        assert c._project_id is None
        assert c._credentials is None
        assert c._client is None
        assert not c.is_connected()

    def test_init_with_project_id(self):
        c = BigQueryConnector(project_id="my-project")
        assert c._project_id == "my-project"
        assert c._credentials is None
        assert c._client is None
        assert not c.is_connected()

    def test_repr_disconnected(self):
        c = BigQueryConnector()
        assert repr(c) == "<BigQueryConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<BigQueryConnector status=connected>"


# -- Connect / Disconnect ----------------------------------------------------


class TestConnect:
    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_connect_success_with_explicit_project(
        self, mock_from_sa, mock_client_cls, connector_with_project
    ):
        mock_creds_obj = MagicMock()
        mock_from_sa.return_value = mock_creds_obj
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance

        connector_with_project.connect()

        assert connector_with_project.is_connected()
        assert connector_with_project._client is mock_client_instance
        assert connector_with_project._credentials is mock_creds_obj
        assert connector_with_project._project_id == "my-explicit-project"
        mock_from_sa.assert_called_once_with(FAKE_CREDS_DICT)
        mock_client_cls.assert_called_once_with(
            credentials=mock_creds_obj, project="my-explicit-project"
        )

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_connect_success_project_from_creds(
        self, mock_from_sa, mock_client_cls, connector
    ):
        """When no project_id is given, it should be pulled from credentials dict."""
        mock_from_sa.return_value = MagicMock()
        mock_client_cls.return_value = MagicMock()

        connector.connect()

        assert connector.is_connected()
        assert connector._project_id == "test-project-from-creds"
        mock_client_cls.assert_called_once_with(
            credentials=mock_from_sa.return_value,
            project="test-project-from-creds",
        )

    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_connect_missing_project_id_raises_credential_error(
        self, mock_from_sa
    ):
        """When project_id is None and creds dict has no project_id, raise CredentialError."""
        creds_without_project = FAKE_CREDS_DICT.copy()
        del creds_without_project["project_id"]

        c = BigQueryConnector()
        c._credential_manager = MagicMock()
        c._credential_manager.get_bigquery_credentials.return_value = creds_without_project
        mock_from_sa.return_value = MagicMock()

        with pytest.raises(CredentialError, match="Project ID must be provided"):
            c.connect()

    def test_connect_missing_credentials_raises_credential_error(self):
        """When credential manager raises CredentialError, it propagates."""
        c = BigQueryConnector()
        c._credential_manager = MagicMock()
        c._credential_manager.get_bigquery_credentials.side_effect = CredentialError(
            "missing creds"
        )

        with pytest.raises(CredentialError, match="missing creds"):
            c.connect()

        assert not c.is_connected()

    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_connect_generic_exception_raises_connection_error(
        self, mock_from_sa, connector_with_project
    ):
        """Any non-CredentialError exception is wrapped in ConnectionError."""
        mock_from_sa.side_effect = ValueError("bad key format")

        with pytest.raises(ConnectionError, match="Failed to connect to BigQuery"):
            connector_with_project.connect()


class TestDisconnect:
    def test_disconnect_calls_client_close(self, connected_connector):
        mock_client = connected_connector._client

        connected_connector.disconnect()

        mock_client.close.assert_called_once()
        assert connected_connector._client is None
        assert connected_connector._credentials is None
        assert not connected_connector.is_connected()

    def test_disconnect_when_no_client(self, connector):
        """Disconnecting when no client exists should not raise."""
        connector._client = None

        connector.disconnect()

        assert connector._client is None
        assert not connector.is_connected()


# -- Health Check -------------------------------------------------------------


class TestHealthCheck:
    def test_health_check_when_connected(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_health_check_when_not_connected(self, connector):
        assert connector.health_check() is False

    def test_health_check_connected_but_no_client(self, connector):
        """Edge case: _is_connected is True but _client is None."""
        connector._is_connected = True
        connector._client = None
        assert connector.health_check() is False

    def test_health_check_has_client_but_not_connected_flag(self, connector):
        """Edge case: _client exists but _is_connected is False."""
        connector._client = MagicMock()
        connector._is_connected = False
        assert connector.health_check() is False


# -- Context Manager ----------------------------------------------------------


class TestContextManager:
    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_context_manager(self, mock_from_sa, mock_client_cls, connector_with_project):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance

        with connector_with_project as c:
            assert c.is_connected()

        assert not c.is_connected()
        mock_client_instance.close.assert_called_once()


# -- Query --------------------------------------------------------------------


class TestQuery:
    def test_query_success(self, connected_connector):
        mock_job = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 5
        mock_job.result.return_value = mock_results
        connected_connector._client.query.return_value = mock_job

        result = connected_connector.query("SELECT * FROM dataset.table")

        assert result is mock_results
        connected_connector._client.query.assert_called_once()
        call_kwargs = connected_connector._client.query.call_args
        assert call_kwargs[0][0] == "SELECT * FROM dataset.table"
        assert call_kwargs[1]["timeout"] is None

    @patch("ccef_connections.connectors.bigquery.bigquery.QueryJobConfig")
    def test_query_with_params_and_timeout(
        self, mock_job_config_cls, connected_connector
    ):
        mock_job_config = MagicMock()
        mock_job_config_cls.return_value = mock_job_config

        mock_job = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 3
        mock_job.result.return_value = mock_results
        connected_connector._client.query.return_value = mock_job

        params = [MagicMock()]  # bigquery ScalarQueryParameter or similar
        result = connected_connector.query(
            "SELECT * FROM dataset.table WHERE id = @id",
            params=params,
            timeout=30.0,
        )

        assert result is mock_results
        call_kwargs = connected_connector._client.query.call_args
        assert call_kwargs[1]["timeout"] == 30.0
        # Verify job_config was passed and params were set on it
        assert call_kwargs[1]["job_config"] is mock_job_config
        assert mock_job_config.query_parameters == params

    def test_query_failure_raises_query_error(self, connected_connector):
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("Syntax error in SQL")
        connected_connector._client.query.return_value = mock_job

        with pytest.raises(QueryError, match="Query failed.*Syntax error"):
            connected_connector.query("SELECT * FROM bad_sql")

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_query_auto_connects(self, mock_from_sa, mock_client_cls, connector_with_project):
        """Query should auto-connect if not connected."""
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance

        mock_job = MagicMock()
        mock_results = MagicMock()
        mock_results.total_rows = 0
        mock_job.result.return_value = mock_results
        mock_client_instance.query.return_value = mock_job

        result = connector_with_project.query("SELECT 1")

        assert result is mock_results
        assert connector_with_project.is_connected()


# -- Query to DataFrame -------------------------------------------------------


class TestQueryToDataframe:
    @patch("ccef_connections.connectors.bigquery.BigQueryConnector.query")
    def test_query_to_dataframe_success(self, mock_query, connected_connector):
        mock_results = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=10)
        mock_results.to_dataframe.return_value = mock_df
        mock_query.return_value = mock_results

        result = connected_connector.query_to_dataframe("SELECT * FROM dataset.table")

        assert result is mock_df
        mock_query.assert_called_once_with("SELECT * FROM dataset.table", None, None)
        mock_results.to_dataframe.assert_called_once()

    @patch("ccef_connections.connectors.bigquery.BigQueryConnector.query")
    def test_query_to_dataframe_with_params(self, mock_query, connected_connector):
        mock_results = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=5)
        mock_results.to_dataframe.return_value = mock_df
        mock_query.return_value = mock_results

        params = [MagicMock()]
        result = connected_connector.query_to_dataframe(
            "SELECT * FROM dataset.table WHERE x = @x",
            params=params,
            timeout=60.0,
        )

        assert result is mock_df
        mock_query.assert_called_once_with(
            "SELECT * FROM dataset.table WHERE x = @x", params, 60.0
        )

    def test_query_to_dataframe_no_pandas_raises_import_error(
        self, connected_connector
    ):
        """If pandas is not available, an ImportError should be raised."""
        with patch.dict("sys.modules", {"pandas": None}):
            with pytest.raises(ImportError, match="pandas is required"):
                connected_connector.query_to_dataframe("SELECT 1")


# -- Table Exists -------------------------------------------------------------


class TestTableExists:
    def test_table_exists_true(self, connected_connector):
        connected_connector._client.get_table.return_value = MagicMock()

        result = connected_connector.table_exists("dataset.my_table")

        assert result is True
        connected_connector._client.get_table.assert_called_once_with(
            "my-explicit-project.dataset.my_table"
        )

    def test_table_exists_false(self, connected_connector):
        from google.cloud.exceptions import NotFound

        connected_connector._client.get_table.side_effect = NotFound("not found")

        result = connected_connector.table_exists("dataset.my_table")

        assert result is False

    def test_table_exists_with_full_table_id(self, connected_connector):
        connected_connector._client.get_table.return_value = MagicMock()

        result = connected_connector.table_exists("other-project.dataset.my_table")

        assert result is True
        connected_connector._client.get_table.assert_called_once_with(
            "other-project.dataset.my_table"
        )

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_table_exists_auto_connects(
        self, mock_from_sa, mock_client_cls, connector_with_project
    ):
        """table_exists should auto-connect if not connected."""
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance
        mock_client_instance.get_table.return_value = MagicMock()

        result = connector_with_project.table_exists("dataset.table")

        assert result is True
        assert connector_with_project.is_connected()


# -- Insert Rows --------------------------------------------------------------


class TestInsertRows:
    def test_insert_rows_success(self, connected_connector):
        mock_table = MagicMock()
        connected_connector._client.get_table.return_value = mock_table
        connected_connector._client.insert_rows_json.return_value = []  # no errors

        rows = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        connected_connector.insert_rows("dataset.users", rows)

        connected_connector._client.get_table.assert_called_once_with(
            "my-explicit-project.dataset.users"
        )
        connected_connector._client.insert_rows_json.assert_called_once_with(
            mock_table, rows
        )

    def test_insert_rows_with_errors_raises_write_error(self, connected_connector):
        mock_table = MagicMock()
        connected_connector._client.get_table.return_value = mock_table
        connected_connector._client.insert_rows_json.return_value = [
            {"index": 0, "errors": [{"reason": "invalid", "message": "bad data"}]}
        ]

        rows = [{"name": "bad_row"}]
        with pytest.raises(WriteError, match="Insert failed with errors"):
            connected_connector.insert_rows("dataset.users", rows)

    def test_insert_rows_exception_raises_write_error(self, connected_connector):
        connected_connector._client.get_table.side_effect = Exception("Table not found")

        with pytest.raises(WriteError, match="Insert failed.*Table not found"):
            connected_connector.insert_rows("dataset.users", [{"a": 1}])

    def test_insert_rows_with_full_table_id(self, connected_connector):
        mock_table = MagicMock()
        connected_connector._client.get_table.return_value = mock_table
        connected_connector._client.insert_rows_json.return_value = []

        connected_connector.insert_rows("other-project.dataset.users", [{"a": 1}])

        connected_connector._client.get_table.assert_called_once_with(
            "other-project.dataset.users"
        )

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_insert_rows_auto_connects(
        self, mock_from_sa, mock_client_cls, connector_with_project
    ):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance
        mock_client_instance.get_table.return_value = MagicMock()
        mock_client_instance.insert_rows_json.return_value = []

        connector_with_project.insert_rows("dataset.users", [{"a": 1}])

        assert connector_with_project.is_connected()


# -- Load DataFrame -----------------------------------------------------------


class TestLoadDataframe:
    @patch("ccef_connections.connectors.bigquery.bigquery.LoadJobConfig")
    def test_load_dataframe_append(self, mock_job_config_cls, connected_connector):
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=5)
        mock_job = MagicMock()
        connected_connector._client.load_table_from_dataframe.return_value = mock_job

        connected_connector.load_dataframe(mock_df, "dataset.table", if_exists="append")

        connected_connector._client.load_table_from_dataframe.assert_called_once()
        call_args = connected_connector._client.load_table_from_dataframe.call_args
        assert call_args[0][0] is mock_df
        assert call_args[0][1] == "my-explicit-project.dataset.table"
        mock_job.result.assert_called_once()

    @patch("ccef_connections.connectors.bigquery.bigquery.LoadJobConfig")
    @patch("ccef_connections.connectors.bigquery.bigquery.WriteDisposition")
    def test_load_dataframe_replace(
        self, mock_write_disp, mock_job_config_cls, connected_connector
    ):
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=3)
        mock_job = MagicMock()
        connected_connector._client.load_table_from_dataframe.return_value = mock_job

        connected_connector.load_dataframe(
            mock_df, "dataset.table", if_exists="replace"
        )

        connected_connector._client.load_table_from_dataframe.assert_called_once()
        mock_job.result.assert_called_once()

    @patch("ccef_connections.connectors.bigquery.bigquery.LoadJobConfig")
    @patch("ccef_connections.connectors.bigquery.bigquery.WriteDisposition")
    def test_load_dataframe_fail_if_exists(
        self, mock_write_disp, mock_job_config_cls, connected_connector
    ):
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=2)
        mock_job = MagicMock()
        connected_connector._client.load_table_from_dataframe.return_value = mock_job

        connected_connector.load_dataframe(
            mock_df, "dataset.table", if_exists="fail_if_exists"
        )

        connected_connector._client.load_table_from_dataframe.assert_called_once()
        mock_job.result.assert_called_once()

    def test_load_dataframe_failure_raises_write_error(self, connected_connector):
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        connected_connector._client.load_table_from_dataframe.side_effect = Exception(
            "Schema mismatch"
        )

        with pytest.raises(WriteError, match="Load failed.*Schema mismatch"):
            connected_connector.load_dataframe(mock_df, "dataset.table")

    def test_load_dataframe_no_pandas_raises_import_error(self, connected_connector):
        mock_df = MagicMock()
        with patch.dict("sys.modules", {"pandas": None}):
            with pytest.raises(ImportError, match="pandas is required"):
                connected_connector.load_dataframe(mock_df, "dataset.table")

    @patch("ccef_connections.connectors.bigquery.bigquery.LoadJobConfig")
    def test_load_dataframe_with_full_table_id(
        self, mock_job_config_cls, connected_connector
    ):
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_job = MagicMock()
        connected_connector._client.load_table_from_dataframe.return_value = mock_job

        connected_connector.load_dataframe(
            mock_df, "other-project.dataset.table", if_exists="append"
        )

        call_args = connected_connector._client.load_table_from_dataframe.call_args
        assert call_args[0][1] == "other-project.dataset.table"

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    @patch("ccef_connections.connectors.bigquery.bigquery.LoadJobConfig")
    def test_load_dataframe_auto_connects(
        self, mock_job_config_cls, mock_from_sa, mock_client_cls, connector_with_project
    ):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance
        mock_job = MagicMock()
        mock_client_instance.load_table_from_dataframe.return_value = mock_job

        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        connector_with_project.load_dataframe(mock_df, "dataset.table")

        assert connector_with_project.is_connected()
        mock_job.result.assert_called_once()


# -- Execute DML --------------------------------------------------------------


class TestExecuteDml:
    def test_execute_dml_success(self, connected_connector):
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 42
        connected_connector._client.query.return_value = mock_job

        result = connected_connector.execute_dml(
            "UPDATE dataset.table SET status = 'done' WHERE id = 1"
        )

        assert result == 42
        connected_connector._client.query.assert_called_once_with(
            "UPDATE dataset.table SET status = 'done' WHERE id = 1"
        )
        mock_job.result.assert_called_once()

    def test_execute_dml_zero_rows(self, connected_connector):
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 0
        connected_connector._client.query.return_value = mock_job

        result = connected_connector.execute_dml("DELETE FROM dataset.table WHERE 1=0")

        assert result == 0

    def test_execute_dml_none_rows_returns_zero(self, connected_connector):
        """When num_dml_affected_rows is None, should return 0."""
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = None
        connected_connector._client.query.return_value = mock_job

        result = connected_connector.execute_dml("UPDATE dataset.table SET x = 1")

        assert result == 0

    def test_execute_dml_failure_raises_query_error(self, connected_connector):
        mock_job = MagicMock()
        mock_job.result.side_effect = Exception("Permission denied")
        connected_connector._client.query.return_value = mock_job

        with pytest.raises(QueryError, match="DML failed.*Permission denied"):
            connected_connector.execute_dml("DROP TABLE dataset.table")

    def test_execute_dml_client_query_raises(self, connected_connector):
        connected_connector._client.query.side_effect = Exception("Network error")

        with pytest.raises(QueryError, match="DML failed.*Network error"):
            connected_connector.execute_dml("UPDATE dataset.table SET x = 1")

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_execute_dml_auto_connects(
        self, mock_from_sa, mock_client_cls, connector_with_project
    ):
        mock_from_sa.return_value = MagicMock()
        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance
        mock_job = MagicMock()
        mock_job.num_dml_affected_rows = 1
        mock_client_instance.query.return_value = mock_job

        result = connector_with_project.execute_dml("UPDATE dataset.table SET x = 1")

        assert result == 1
        assert connector_with_project.is_connected()


# -- _get_full_table_id -------------------------------------------------------


class TestGetFullTableId:
    def test_two_part_id_adds_project(self, connected_connector):
        result = connected_connector._get_full_table_id("dataset.table")
        assert result == "my-explicit-project.dataset.table"

    def test_three_part_id_unchanged(self, connected_connector):
        result = connected_connector._get_full_table_id(
            "other-project.dataset.table"
        )
        assert result == "other-project.dataset.table"

    def test_two_part_id_with_different_project(self):
        c = BigQueryConnector(project_id="another-project")
        result = c._get_full_table_id("my_dataset.my_table")
        assert result == "another-project.my_dataset.my_table"

    def test_single_part_id_returned_as_is(self, connected_connector):
        """A single-part ID (no dots) is returned unchanged since len(parts) != 2."""
        result = connected_connector._get_full_table_id("just_a_table")
        assert result == "just_a_table"

    def test_four_part_id_returned_as_is(self, connected_connector):
        """A four-part ID is returned unchanged since len(parts) != 2."""
        result = connected_connector._get_full_table_id("a.b.c.d")
        assert result == "a.b.c.d"


# -- project_id property ------------------------------------------------------


class TestProjectIdProperty:
    def test_project_id_none_initially(self):
        c = BigQueryConnector()
        assert c.project_id is None

    def test_project_id_from_constructor(self):
        c = BigQueryConnector(project_id="my-project")
        assert c.project_id == "my-project"

    @patch("ccef_connections.connectors.bigquery.bigquery.Client")
    @patch("ccef_connections.connectors.bigquery.Credentials.from_service_account_info")
    def test_project_id_set_after_connect(self, mock_from_sa, mock_client_cls, connector):
        """After connect(), project_id should be set from credentials if not provided."""
        mock_from_sa.return_value = MagicMock()
        mock_client_cls.return_value = MagicMock()

        connector.connect()

        assert connector.project_id == "test-project-from-creds"
