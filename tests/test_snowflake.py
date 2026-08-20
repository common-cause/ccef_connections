"""Tests for the Snowflake connector."""

from unittest.mock import MagicMock, patch

import pytest
from tenacity import stop_after_attempt

from ccef_connections.connectors.snowflake import DEFAULTS, SnowflakeConnector
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    QueryError,
)


# Disable tenacity retries so tests fail fast instead of backing off.
for _method_name in ("query",):
    _method = getattr(SnowflakeConnector, _method_name)
    if hasattr(_method, "retry"):
        _method.retry.stop = stop_after_attempt(1)


FULL_CREDS = {
    "password": "hunter2",
    "account": "acct-from-env",
    "user": "JRUPP",
    "role": "PUBLIC",
    "warehouse": "READER_WH",
    "database": "CMNC_DATA",
    "schema": "ROI",
}


# -- Fixtures ----------------------------------------------------------------


def _make(creds=None, **kwargs):
    """Build a connector whose credential manager returns ``creds``."""
    connector = SnowflakeConnector(**kwargs)
    mock_cm = MagicMock()
    mock_cm.get_snowflake_credentials.return_value = dict(
        FULL_CREDS if creds is None else creds
    )
    connector._credential_manager = mock_cm
    return connector


@pytest.fixture
def connector():
    return _make()


@pytest.fixture
def connected(connector):
    """A connector with a mock client already in place."""
    connector._client = MagicMock()
    connector._is_connected = True
    connector._config = connector._resolve_config()
    return connector


def _cursor_of(connected_connector):
    return connected_connector._client.cursor.return_value


# -- Configuration resolution ------------------------------------------------


def test_resolves_settings_from_credentials():
    cfg = _make()._resolve_config()
    assert cfg["account"] == "acct-from-env"
    assert cfg["user"] == "JRUPP"
    assert cfg["password"] == "hunter2"
    assert cfg["schema"] == "ROI"


def test_explicit_kwargs_win_over_environment():
    """Precedence is explicit keyword > environment > default."""
    cfg = _make(schema="DATA_MODEL", warehouse="BIG_WH")._resolve_config()
    assert cfg["schema"] == "DATA_MODEL"
    assert cfg["warehouse"] == "BIG_WH"
    assert cfg["account"] == "acct-from-env"  # untouched


def test_defaults_apply_when_environment_is_silent():
    creds = {"password": "p", "account": "a", "user": "u", "role": "PUBLIC"}
    cfg = _make(creds)._resolve_config()
    assert cfg["warehouse"] == DEFAULTS["warehouse"] == "READER_WH"
    assert cfg["database"] == DEFAULTS["database"] == "CMNC_DATA"
    assert cfg["schema"] == DEFAULTS["schema"] == "ROI"


@pytest.mark.parametrize("missing", ["account", "user", "role"])
def test_required_settings_raise_naming_the_env_var(missing):
    """A missing required setting names itself and its env var, not a driver error."""
    creds = {k: v for k, v in FULL_CREDS.items() if k != missing}
    with pytest.raises(CredentialError) as exc:
        _make(creds)._resolve_config()
    assert missing in str(exc.value)
    assert f"SNOWFLAKE_{missing.upper()}" in str(exc.value)


def test_role_has_no_default():
    """Role is deliberately not defaulted — an unset role silently mis-resolves."""
    assert "role" not in DEFAULTS


def test_timeout_sets_session_parameter_only_when_given():
    assert "session_parameters" not in _make()._resolve_config()
    cfg = _make(timeout=30)._resolve_config()
    assert cfg["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 30


def test_config_property_redacts_the_password(connector):
    assert connector.config["password"] == "***"
    assert connector.config["account"] == "acct-from-env"


# -- Connect / disconnect ----------------------------------------------------


@patch("ccef_connections.connectors.snowflake.snowflake.connector.connect")
def test_connect_passes_resolved_config(mock_connect, connector):
    connector.connect()
    assert connector.is_connected()
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["account"] == "acct-from-env"
    assert kwargs["password"] == "hunter2"


@patch("ccef_connections.connectors.snowflake.snowflake.connector.connect")
def test_connect_propagates_credential_error(mock_connect):
    c = _make({"password": "p"})  # no account/user/role
    with pytest.raises(CredentialError):
        c.connect()
    mock_connect.assert_not_called()


def test_disconnect_is_safe_when_close_raises(connected):
    connected._client.close.side_effect = RuntimeError("already gone")
    connected.disconnect()
    assert not connected.is_connected()
    assert connected._client is None


def test_context_manager_connects_and_closes(connector):
    with patch(
        "ccef_connections.connectors.snowflake.snowflake.connector.connect"
    ) as mock_connect:
        with connector as conn:
            assert conn.is_connected()
        mock_connect.return_value.close.assert_called_once()


# -- Queries -----------------------------------------------------------------


def test_query_returns_columns_and_rows(connected):
    """(columns, rows) is the shim-compatible shape callers depend on."""
    cursor = _cursor_of(connected)
    cursor.description = [("ROI_ID",), ("FULL_NAME",)]
    cursor.fetchall.return_value = [(1, "A"), (2, "B")]

    cols, rows = connected.query("SELECT ROI_ID, FULL_NAME FROM T")

    assert cols == ["ROI_ID", "FULL_NAME"]
    assert rows == [(1, "A"), (2, "B")]
    cursor.close.assert_called_once()


def test_query_connects_lazily(connector):
    with patch(
        "ccef_connections.connectors.snowflake.snowflake.connector.connect"
    ) as mock_connect:
        cursor = mock_connect.return_value.cursor.return_value
        cursor.description = [("N",)]
        cursor.fetchall.return_value = [(1,)]
        connector.query("SELECT 1")
        mock_connect.assert_called_once()


def test_query_closes_cursor_on_failure(connected):
    cursor = _cursor_of(connected)
    cursor.execute.side_effect = RuntimeError("boom")
    with pytest.raises(QueryError):
        connected.query("SELECT 1")
    cursor.close.assert_called_once()


def test_query_handles_null_description(connected):
    cursor = _cursor_of(connected)
    cursor.description = None
    cursor.fetchall.return_value = []
    cols, rows = connected.query("SELECT 1")
    assert cols == []
    assert rows == []


def test_query_dicts_zips_columns(connected):
    cursor = _cursor_of(connected)
    cursor.description = [("A",), ("B",)]
    cursor.fetchall.return_value = [(1, 2), (3, 4)]
    assert connected.query_dicts("SELECT A, B FROM T") == [
        {"A": 1, "B": 2},
        {"A": 3, "B": 4},
    ]


def test_query_to_dataframe(connected):
    pd = pytest.importorskip("pandas")
    cursor = _cursor_of(connected)
    cursor.description = [("A",)]
    cursor.fetchall.return_value = [(1,), (2,)]
    df = connected.query_to_dataframe("SELECT A FROM T")
    assert list(df.columns) == ["A"]
    assert len(df) == 2


def test_list_tables_filters_by_schema(connected):
    cursor = _cursor_of(connected)
    cursor.description = [("TABLE_NAME",)]
    cursor.fetchall.return_value = [("ACCOUNT_PROFILE",), ("V_CAMPAIGN_SOURCE",)]
    assert connected.list_tables() == ["ACCOUNT_PROFILE", "V_CAMPAIGN_SOURCE"]
    sql = cursor.execute.call_args.args[0]
    assert "INFORMATION_SCHEMA.TABLES" in sql


# -- health_check ------------------------------------------------------------


def test_health_check_true_on_rows(connected):
    cursor = _cursor_of(connected)
    cursor.description = [("1",)]
    cursor.fetchall.return_value = [(1,)]
    assert connected.health_check() is True


def test_health_check_false_on_failure(connected):
    _cursor_of(connected).execute.side_effect = RuntimeError("session closed")
    assert connected.health_check() is False


# -- Error translation -------------------------------------------------------
#
# The hints are the point of this connector; these lock them in.


def _err(message, errno=None):
    e = RuntimeError(message)
    if errno is not None:
        e.errno = errno  # type: ignore[attr-defined]
    return e


def test_ip_allowlist_is_not_reported_as_a_bad_credential(connected):
    """The replica is IP-allowlisted; that failure must not read as auth."""
    _cursor_of(connected).execute.side_effect = _err(
        "Incoming request with IP 1.2.3.4 is not allowed to access Snowflake"
    )
    with pytest.raises(ConnectionError) as exc:
        connected.query("SELECT 1")
    assert "IP-allowlisted" in str(exc.value)
    assert "not a bad credential" in str(exc.value)


def test_ip_allowlist_matches_on_errno_too(connected):
    _cursor_of(connected).execute.side_effect = _err("opaque failure", errno=250001)
    with pytest.raises(ConnectionError):
        connected.query("SELECT 1")


def test_bad_password_raises_authentication_error(connected):
    _cursor_of(connected).execute.side_effect = _err(
        "Incorrect username or password was specified"
    )
    with pytest.raises(AuthenticationError) as exc:
        connected.query("SELECT 1")
    assert "SNOWFLAKE_CREDENTIALS_PASSWORD" in str(exc.value)


def test_statement_timeout_explains_the_warehouse_ceiling(connected):
    _cursor_of(connected).execute.side_effect = _err(
        "Statement reached its statement or warehouse timeout"
    )
    with pytest.raises(QueryError) as exc:
        connected.query("SELECT COUNT(*) FROM HUGE")
    msg = str(exc.value)
    assert "WAREHOUSE level" in msg
    assert "cannot be raised by PUBLIC" in msg


def test_read_only_violation_is_explained(connected):
    _cursor_of(connected).execute.side_effect = _err(
        "Cannot perform CREATE TABLE. This session is read-only."
    )
    with pytest.raises(QueryError) as exc:
        connected.query("CREATE TEMPORARY TABLE T AS SELECT 1")
    assert "read-only" in str(exc.value)


def test_missing_object_points_at_the_role_first(connected):
    """A role that cannot see a schema reports a missing object, not a denial."""
    _cursor_of(connected).execute.side_effect = _err(
        "Object 'CMNC_DATA.BQ_HUSTLE.X' does not exist or not authorized."
    )
    with pytest.raises(QueryError) as exc:
        connected.query("SELECT 1 FROM CMNC_DATA.BQ_HUSTLE.X")
    assert "SNOWFLAKE_ROLE" in str(exc.value)


@patch("ccef_connections.connectors.snowflake.snowflake.connector.connect")
def test_connect_failure_becomes_connection_error(mock_connect, connector):
    mock_connect.side_effect = _err("host unreachable")
    with pytest.raises(ConnectionError):
        connector.connect()


# -- Retry policy ------------------------------------------------------------


@patch("tenacity.nap.time.sleep")
def test_timeouts_are_not_retried(mock_sleep):
    """
    A statement timeout is deterministic on READER_WH, so retrying only wastes
    minutes and disguises a hard failure as a flaky one.
    """
    from ccef_connections.core.retry import retry_snowflake_operation

    calls = []

    @retry_snowflake_operation
    def always_times_out():
        calls.append(1)
        raise QueryError("Statement reached its statement or warehouse timeout")

    with pytest.raises(QueryError):
        always_times_out()
    assert len(calls) == 1
    assert not mock_sleep.called


@patch("tenacity.nap.time.sleep")
def test_connection_errors_are_not_retried(mock_sleep):
    """
    Per the library retry policy, ConnectionError is permanent until a human acts
    — for Snowflake it is usually the IP allowlist, which no amount of retrying
    fixes. The driver has already retried genuine transport blips internally.
    """
    from ccef_connections.core.retry import retry_snowflake_operation

    calls = []

    @retry_snowflake_operation
    def blocked():
        calls.append(1)
        raise ConnectionError("not allowed to access Snowflake")

    with pytest.raises(ConnectionError):
        blocked()
    assert len(calls) == 1
    assert not mock_sleep.called


@patch("tenacity.nap.time.sleep")
def test_rate_limits_are_retried(mock_sleep):
    """Rate limiting is the one safe thing to replay: nothing was applied."""
    from ccef_connections.core.retry import retry_snowflake_operation
    from ccef_connections.exceptions import RateLimitError

    calls = []

    @retry_snowflake_operation
    def throttled():
        calls.append(1)
        if len(calls) < 3:
            raise RateLimitError("throttled", retry_after=1)
        return "ok"

    assert throttled() == "ok"
    assert len(calls) == 3
    assert mock_sleep.called


# -- Public API --------------------------------------------------------------


def test_exported_lazily_from_package_root():
    import ccef_connections

    assert "SnowflakeConnector" in ccef_connections.__all__
    assert ccef_connections.SnowflakeConnector is SnowflakeConnector
