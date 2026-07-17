"""Tests for the Asana connector."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.asana import (
    ASANA_API_BASE,
    DEFAULT_TASK_FIELDS,
    AsanaConnector,
)
from ccef_connections.core.retry import _wait_for_asana_rate_limit
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


def _page(items, next_offset=None):
    """Build an Asana list-response body with the data/next_page envelope."""
    body = {"data": items}
    if next_offset:
        body["next_page"] = {
            "offset": next_offset,
            "path": "/tasks?limit=100&offset=" + next_offset,
            "uri": ASANA_API_BASE + "/tasks?limit=100&offset=" + next_offset,
        }
    else:
        body["next_page"] = None
    return body


FAKE_PAT = "test-asana-pat"
USERS_ME = {"data": {"gid": "9999", "name": "EP Sync Bot"}}


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def connector():
    """Create an AsanaConnector with a mocked credential manager."""
    with patch.object(AsanaConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_asana_api_key.return_value = FAKE_PAT
        c = AsanaConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """Return a connector that is already connected, with a mock session."""
    connector._api_key = FAKE_PAT
    connector._session = MagicMock()
    connector._is_connected = True
    return connector


# ── Initialization ─────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = AsanaConnector()
        assert c._api_key is None
        assert c._session is None
        assert c._user_gid is None
        assert not c.is_connected()

    def test_inherits_base_connection(self):
        from ccef_connections.core.base import BaseConnection

        assert isinstance(AsanaConnector(), BaseConnection)

    def test_repr(self):
        assert "AsanaConnector" in repr(AsanaConnector())
        assert "disconnected" in repr(AsanaConnector())

    def test_default_task_fields_contents(self):
        for field in ("custom_fields", "assignee.email", "memberships.section.name",
                      "permalink_url", "modified_at"):
            assert field in DEFAULT_TASK_FIELDS


# ── Connect / Disconnect ───────────────────────────────────────────────


class TestConnect:
    @patch("ccef_connections.connectors.asana.requests.Session")
    def test_connect_validates_via_users_me(self, mock_session_cls, connector):
        session = mock_session_cls.return_value
        session.request.return_value = _make_response(200, USERS_ME)

        connector.connect()

        assert connector.is_connected()
        assert connector._user_gid == "9999"
        assert connector._user_name == "EP Sync Bot"
        args = session.request.call_args
        assert args[0][0] == "GET"
        assert args[0][1] == f"{ASANA_API_BASE}/users/me"

    @patch("ccef_connections.connectors.asana.requests.Session")
    def test_connect_sets_bearer_header_on_session(self, mock_session_cls, connector):
        session = mock_session_cls.return_value
        session.request.return_value = _make_response(200, USERS_ME)

        connector.connect()

        headers = session.headers.update.call_args[0][0]
        assert headers["Authorization"] == f"Bearer {FAKE_PAT}"

    def test_connect_reraises_missing_credential(self, connector):
        connector._credential_manager.get_asana_api_key.side_effect = CredentialError(
            "ASANA_API_KEY_PASSWORD not set"
        )
        with pytest.raises(CredentialError, match="ASANA_API_KEY_PASSWORD"):
            connector.connect()
        assert not connector.is_connected()

    @patch("ccef_connections.connectors.asana.requests.Session")
    def test_connect_rejected_pat_raises_authentication_error(
        self, mock_session_cls, connector
    ):
        session = mock_session_cls.return_value
        session.request.return_value = _make_response(
            401, json_data={"errors": [{"message": "Not Authorized"}]}
        )

        with pytest.raises(AuthenticationError, match="Not Authorized"):
            connector.connect()
        assert not connector.is_connected()
        assert connector._session is None

    @patch("ccef_connections.connectors.asana.requests.Session")
    def test_connect_wraps_network_failure(self, mock_session_cls, connector):
        session = mock_session_cls.return_value
        session.request.side_effect = requests.RequestException("timeout")

        with pytest.raises(ConnectionError, match="Failed to connect to Asana"):
            connector.connect()
        assert not connector.is_connected()
        assert connector._session is None


class TestDisconnect:
    def test_disconnect_clears_state(self, connected):
        session = connected._session
        connected.disconnect()
        session.close.assert_called_once()
        assert connected._session is None
        assert connected._api_key is None
        assert connected._user_gid is None
        assert not connected.is_connected()


# ── Health check ───────────────────────────────────────────────────────


class TestHealthCheck:
    def test_returns_true_when_connected(self, connected):
        connected._session.request.return_value = _make_response(200, USERS_ME)
        assert connected.health_check() is True

    def test_returns_false_when_disconnected(self, connector):
        assert connector.health_check() is False

    def test_returns_false_when_api_errors(self, connected):
        connected._session.request.return_value = _make_response(
            500, text="Server Error"
        )
        assert connected.health_check() is False


# ── Context manager ────────────────────────────────────────────────────


class TestContextManager:
    @patch("ccef_connections.connectors.asana.requests.Session")
    def test_enter_connects_and_exit_disconnects(self, mock_session_cls, connector):
        session = mock_session_cls.return_value
        session.request.return_value = _make_response(200, USERS_ME)

        with connector as c:
            assert c.is_connected()
        assert not connector.is_connected()


# ── _request ───────────────────────────────────────────────────────────


class TestRequest:
    def test_returns_full_body_with_envelope(self, connected):
        body = _page([{"gid": "1"}], next_offset="tok123")
        connected._session.request.return_value = _make_response(200, body)

        result = connected._request("GET", "/tasks")

        assert result["data"] == [{"gid": "1"}]
        assert result["next_page"]["offset"] == "tok123"

    def test_builds_url_and_timeout(self, connected):
        connected._session.request.return_value = _make_response(200, {"data": []})

        connected._request("GET", "/workspaces", params={"limit": 100})

        args = connected._session.request.call_args
        assert args[0][0] == "GET"
        assert args[0][1] == f"{ASANA_API_BASE}/workspaces"
        assert args.kwargs["params"] == {"limit": 100}
        assert args.kwargs["timeout"] == 30

    def test_401_raises_authentication_error(self, connected):
        connected._session.request.return_value = _make_response(
            401, json_data={"errors": [{"message": "Not Authorized"}]}
        )
        with pytest.raises(AuthenticationError, match="Not Authorized"):
            connected._request("GET", "/users/me")

    def test_402_raises_non_retryable_paid_tier_error(self, connected):
        connected._session.request.return_value = _make_response(
            402,
            json_data={"errors": [{"message": "Payment Required"}]},
        )
        # ConnectionError is never retried by retry_asana_operation, so a
        # free-workspace failure surfaces immediately with a clear message.
        with pytest.raises(ConnectionError, match="paid-tier Asana feature"):
            connected._request("GET", "/tasks")

    def test_429_raises_rate_limit_error(self, connected):
        connected._session.request.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "30"}
        )
        # Call _request directly rather than a public method: public methods
        # carry @retry_asana_operation, which would sleep through real waits
        # on a persistent 429. The successful-retry path is covered in
        # TestRetry with a patched sleep.
        with pytest.raises(RateLimitError, match="retry after 30s") as exc_info:
            connected._request("GET", "/tasks")
        assert exc_info.value.retry_after == 30

    def test_429_defaults_to_60s_without_header(self, connected):
        connected._session.request.return_value = _make_response(
            429, text="Too Many Requests"
        )
        with pytest.raises(RateLimitError) as exc_info:
            connected._request("GET", "/tasks")
        assert exc_info.value.retry_after == 60

    def test_429_defaults_to_60s_on_unparseable_header(self, connected):
        connected._session.request.return_value = _make_response(
            429, headers={"Retry-After": "soon"}
        )
        with pytest.raises(RateLimitError) as exc_info:
            connected._request("GET", "/tasks")
        assert exc_info.value.retry_after == 60

    def test_error_message_extracted_from_errors_envelope(self, connected):
        connected._session.request.return_value = _make_response(
            400,
            json_data={
                "errors": [
                    {"message": "project: Not a valid GID", "help": "See docs"}
                ]
            },
        )
        with pytest.raises(ConnectionError, match="Not a valid GID"):
            connected._request("GET", "/tasks")

    def test_error_message_falls_back_to_text(self, connected):
        resp = _make_response(502, text="Bad Gateway")
        resp.json.side_effect = ValueError("not json")
        connected._session.request.return_value = resp
        with pytest.raises(ConnectionError, match="Bad Gateway"):
            connected._request("GET", "/tasks")

    def test_network_error_raises_connection_error(self, connected):
        connected._session.request.side_effect = requests.RequestException("timeout")
        with pytest.raises(ConnectionError, match="Asana API request failed"):
            connected._request("GET", "/tasks")

    def test_auto_connects_when_not_connected(self, connector):
        def fake_connect():
            connector._session = MagicMock()
            connector._session.request.return_value = _make_response(
                200, {"data": []}
            )
            connector._is_connected = True

        with patch.object(
            connector, "connect", side_effect=fake_connect
        ) as mock_connect:
            connector._request("GET", "/workspaces")
        mock_connect.assert_called_once()

    def test_raises_when_connect_leaves_no_session(self, connector):
        with patch.object(connector, "connect"):
            with pytest.raises(ConnectionError, match="not connected"):
                connector._request("GET", "/workspaces")


# ── _paginate ──────────────────────────────────────────────────────────


class TestPaginate:
    def test_follows_offset_chain_until_null(self, connected):
        connected._session.request.side_effect = [
            _make_response(200, _page([{"gid": "1"}], next_offset="off1")),
            _make_response(200, _page([{"gid": "2"}], next_offset="off2")),
            _make_response(200, _page([{"gid": "3"}])),
        ]

        results = connected._paginate("/tasks", params={"project": "77"})

        assert [r["gid"] for r in results] == ["1", "2", "3"]
        assert connected._session.request.call_count == 3
        second = connected._session.request.call_args_list[1]
        third = connected._session.request.call_args_list[2]
        assert second.kwargs["params"]["offset"] == "off1"
        assert third.kwargs["params"]["offset"] == "off2"

    def test_injects_limit_100(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected._paginate("/workspaces")

        params = connected._session.request.call_args.kwargs["params"]
        assert params["limit"] == 100

    def test_respects_caller_limit(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected._paginate("/workspaces", params={"limit": 5})

        params = connected._session.request.call_args.kwargs["params"]
        assert params["limit"] == 5

    def test_does_not_mutate_caller_params(self, connected):
        connected._session.request.side_effect = [
            _make_response(200, _page([{"gid": "1"}], next_offset="off1")),
            _make_response(200, _page([{"gid": "2"}])),
        ]
        params = {"project": "77"}

        connected._paginate("/tasks", params=params)

        assert params == {"project": "77"}

    def test_empty_page_returns_empty_list(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))
        assert connected._paginate("/tasks") == []

    def test_missing_data_key_returns_empty_list(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"next_page": None}
        )
        assert connected._paginate("/tasks") == []


# ── Workspaces ─────────────────────────────────────────────────────────


class TestGetWorkspaces:
    def test_lists_workspaces(self, connected):
        connected._session.request.return_value = _make_response(
            200, _page([{"gid": "ws1", "name": "Common Cause NM"}])
        )

        results = connected.get_workspaces()

        assert results == [{"gid": "ws1", "name": "Common Cause NM"}]
        assert (
            connected._session.request.call_args[0][1]
            == f"{ASANA_API_BASE}/workspaces"
        )


# ── Projects ───────────────────────────────────────────────────────────


class TestGetProjects:
    def test_filters_by_workspace(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_projects("ws1")

        params = connected._session.request.call_args.kwargs["params"]
        assert params["workspace"] == "ws1"
        assert "archived" not in params

    @pytest.mark.parametrize("archived,expected", [(True, "true"), (False, "false")])
    def test_archived_passthrough(self, connected, archived, expected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_projects("ws1", archived=archived)

        params = connected._session.request.call_args.kwargs["params"]
        assert params["archived"] == expected


class TestGetProject:
    def test_gets_single_project_unwrapped(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"data": {"gid": "p1", "name": "EP Volunteers"}}
        )

        result = connected.get_project("p1")

        assert result == {"gid": "p1", "name": "EP Volunteers"}
        args = connected._session.request.call_args
        assert args[0][1] == f"{ASANA_API_BASE}/projects/p1"
        assert args.kwargs["params"] is None

    def test_opt_fields_passthrough(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"data": {"gid": "p1"}}
        )

        connected.get_project("p1", opt_fields="name,custom_field_settings")

        params = connected._session.request.call_args.kwargs["params"]
        assert params == {"opt_fields": "name,custom_field_settings"}


# ── Sections ───────────────────────────────────────────────────────────


class TestGetSections:
    def test_lists_project_sections(self, connected):
        connected._session.request.return_value = _make_response(
            200, _page([{"gid": "s1", "name": "Onboarding"}])
        )

        results = connected.get_sections("p1")

        assert results == [{"gid": "s1", "name": "Onboarding"}]
        assert (
            connected._session.request.call_args[0][1]
            == f"{ASANA_API_BASE}/projects/p1/sections"
        )


# ── Tasks ──────────────────────────────────────────────────────────────


class TestGetProjectTasks:
    def test_lists_tasks_with_default_fields(self, connected):
        connected._session.request.return_value = _make_response(
            200, _page([{"gid": "t1"}])
        )

        results = connected.get_project_tasks("p1")

        assert results == [{"gid": "t1"}]
        args = connected._session.request.call_args
        assert args[0][1] == f"{ASANA_API_BASE}/tasks"
        params = args.kwargs["params"]
        assert params["project"] == "p1"
        assert params["opt_fields"] == DEFAULT_TASK_FIELDS

    def test_opt_fields_override(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_project_tasks("p1", opt_fields="name,completed")

        params = connected._session.request.call_args.kwargs["params"]
        assert params["opt_fields"] == "name,completed"

    def test_modified_since_string_passthrough(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_project_tasks("p1", modified_since="2026-07-01T00:00:00Z")

        params = connected._session.request.call_args.kwargs["params"]
        assert params["modified_since"] == "2026-07-01T00:00:00Z"

    def test_datetime_filters_serialized_iso(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_project_tasks(
            "p1",
            modified_since=datetime(2026, 7, 1, 8, 30),
            completed_since=datetime(2026, 6, 15),
        )

        params = connected._session.request.call_args.kwargs["params"]
        assert params["modified_since"] == "2026-07-01T08:30:00"
        assert params["completed_since"] == "2026-06-15T00:00:00"

    def test_filters_absent_by_default(self, connected):
        connected._session.request.return_value = _make_response(200, _page([]))

        connected.get_project_tasks("p1")

        params = connected._session.request.call_args.kwargs["params"]
        assert "modified_since" not in params
        assert "completed_since" not in params


class TestGetTask:
    def test_gets_single_task_unwrapped(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"data": {"gid": "t1", "name": "Call volunteer"}}
        )

        result = connected.get_task("t1")

        assert result == {"gid": "t1", "name": "Call volunteer"}
        args = connected._session.request.call_args
        assert args[0][1] == f"{ASANA_API_BASE}/tasks/t1"
        assert args.kwargs["params"] == {"opt_fields": DEFAULT_TASK_FIELDS}


class TestGetSubtasks:
    def test_lists_subtasks(self, connected):
        connected._session.request.return_value = _make_response(
            200, _page([{"gid": "sub1"}])
        )

        results = connected.get_subtasks("t1")

        assert results == [{"gid": "sub1"}]
        args = connected._session.request.call_args
        assert args[0][1] == f"{ASANA_API_BASE}/tasks/t1/subtasks"
        assert args.kwargs["params"]["opt_fields"] == DEFAULT_TASK_FIELDS


# ── Retry behavior ─────────────────────────────────────────────────────


class TestRetry:
    @pytest.mark.parametrize(
        "method",
        [
            "get_workspaces",
            "get_projects",
            "get_project",
            "get_sections",
            "get_project_tasks",
            "get_task",
            "get_subtasks",
        ],
    )
    def test_public_methods_have_retry_decorator(self, method):
        assert hasattr(getattr(AsanaConnector, method), "retry")

    def test_wait_honors_retry_after_plus_buffer(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = RateLimitError(
            "limited", retry_after=30
        )
        assert _wait_for_asana_rate_limit(retry_state) == 32.0

    def test_wait_defaults_without_retry_after(self):
        retry_state = MagicMock()
        retry_state.outcome.exception.return_value = RateLimitError("limited")
        assert _wait_for_asana_rate_limit(retry_state) == 5.0

    @patch("tenacity.nap.time.sleep")
    def test_429_then_success_retries_and_returns(self, mock_sleep, connected):
        connected._session.request.side_effect = [
            _make_response(429, headers={"Retry-After": "7"}),
            _make_response(200, _page([{"gid": "ws1"}])),
        ]

        result = connected.get_workspaces()

        assert result == [{"gid": "ws1"}]
        assert connected._session.request.call_count == 2
        mock_sleep.assert_called_once_with(9.0)

    @patch("tenacity.nap.time.sleep")
    def test_persistent_429_reraises_after_five_attempts(self, mock_sleep, connected):
        connected._session.request.side_effect = [
            _make_response(429, headers={"Retry-After": "1"}) for _ in range(5)
        ]

        with pytest.raises(RateLimitError):
            connected.get_workspaces()

        assert connected._session.request.call_count == 5
        assert mock_sleep.call_count == 4

    def test_402_is_not_retried(self, connected):
        connected._session.request.return_value = _make_response(
            402, json_data={"errors": [{"message": "Payment Required"}]}
        )

        with pytest.raises(ConnectionError, match="paid-tier"):
            connected.get_project_tasks("p1")

        assert connected._session.request.call_count == 1


# ── Credentials ────────────────────────────────────────────────────────


class TestCredentials:
    def test_get_asana_api_key(self):
        from ccef_connections.core.credentials import CredentialManager

        cm = object.__new__(CredentialManager)
        cm._credentials_cache = {}
        cm._env_loaded = True
        with patch.dict("os.environ", {"ASANA_API_KEY_PASSWORD": "test-pat"}):
            assert cm.get_asana_api_key() == "test-pat"

    def test_get_asana_api_key_missing(self):
        from ccef_connections.core.credentials import CredentialManager

        cm = object.__new__(CredentialManager)
        cm._credentials_cache = {}
        cm._env_loaded = True
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CredentialError, match="ASANA_API_KEY"):
                cm.get_asana_api_key()


# ── Lazy import registration ───────────────────────────────────────────


class TestLazyImport:
    def test_importable_from_package_root(self):
        from ccef_connections import AsanaConnector as FromRoot

        assert FromRoot is AsanaConnector

    def test_importable_from_connectors_package(self):
        from ccef_connections.connectors import AsanaConnector as FromConnectors

        assert FromConnectors is AsanaConnector

    def test_listed_in_dir(self):
        import ccef_connections
        import ccef_connections.connectors as connectors_pkg

        assert "AsanaConnector" in dir(ccef_connections)
        assert "AsanaConnector" in dir(connectors_pkg)
