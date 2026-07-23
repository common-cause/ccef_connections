"""Tests for the Hex connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.hex import HEX_API_BASE, HexConnector, _parse_retry_after
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# -- Fixtures ----------------------------------------------------------------


FAKE_TOKEN = "hex_pat_fake_token_value"
PROJECT_ID = "019f8c28-79e4-769b-93d1-5f434fdc9310"
CELL_ID = "019f8c29-166b-765d-b7cd-7a3d9b679825"


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


def _page(values, after=None):
    """Build a Hex cursor-paginated list response."""
    return _make_response(200, {"values": values, "pagination": {"before": None, "after": after}})


def _sql_cell(cell_id=CELL_ID, source="SELECT 1"):
    return {
        "id": cell_id,
        "staticId": "static-" + cell_id,
        "cellType": "SQL",
        "label": "test_sql",
        "dataConnectionId": None,
        "contents": {
            "codeCell": None,
            "sqlCell": {"source": source, "outputDataframe": "df"},
            "markdownCell": None,
        },
        "projectId": PROJECT_ID,
    }


@pytest.fixture
def connector():
    """Create a HexConnector with mocked credentials."""
    with patch.object(HexConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_hex_api_key.return_value = FAKE_TOKEN
        c = HexConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """Create a connector that is already 'connected' with a fake token."""
    connector._token = FAKE_TOKEN
    connector._is_connected = True
    return connector


# -- Initialization / connection ----------------------------------------------


class TestInit:
    def test_default_credential_name(self, connector):
        assert connector._credential_name == "HEX_API_KEY"

    def test_custom_credential_name(self):
        c = HexConnector(credential_name="OTHER_HEX_KEY")
        assert c._credential_name == "OTHER_HEX_KEY"

    def test_custom_base_url_strips_trailing_slash(self):
        c = HexConnector(base_url="https://single-tenant.hex.tech/api/v1/")
        assert c._base_url == "https://single-tenant.hex.tech/api/v1"

    def test_default_base_url(self, connector):
        assert connector._base_url == HEX_API_BASE


class TestConnect:
    def test_connect_loads_token(self, connector):
        connector.connect()
        assert connector.is_connected()
        assert connector._token == FAKE_TOKEN

    def test_connect_missing_credential(self, connector):
        connector._credential_manager.get_hex_api_key.side_effect = CredentialError("missing")
        with pytest.raises(CredentialError):
            connector.connect()

    def test_disconnect_clears_token(self, connected):
        connected.disconnect()
        assert not connected.is_connected()
        assert connected._token is None


# -- Error mapping (via _request to bypass retry waits) ------------------------


class TestErrorMapping:
    @patch("ccef_connections.connectors.hex.requests.request")
    def test_401_raises_authentication_error(self, mock_req, connected):
        mock_req.return_value = _make_response(401, text="bad token")
        with pytest.raises(AuthenticationError):
            connected._request("GET", "/projects")

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_403_raises_authentication_error(self, mock_req, connected):
        mock_req.return_value = _make_response(403, text="no access")
        with pytest.raises(AuthenticationError):
            connected._request("GET", "/projects")

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_429_raises_rate_limit_error_with_retry_after(self, mock_req, connected):
        mock_req.return_value = _make_response(429, headers={"Retry-After": "13"})
        with pytest.raises(RateLimitError) as exc_info:
            connected._request("GET", "/projects")
        assert exc_info.value.retry_after == 13

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_500_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(500, text="boom")
        with pytest.raises(ConnectionError):
            connected._request("GET", "/projects")

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_404_returns_none(self, mock_req, connected):
        mock_req.return_value = _make_response(404)
        assert connected._request("GET", "/projects/nope") is None

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_network_failure_raises_connection_error(self, mock_req, connected):
        mock_req.side_effect = requests.RequestException("dns fail")
        with pytest.raises(ConnectionError):
            connected._request("GET", "/projects")

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_auth_header_sent(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"values": []})
        connected._request("GET", "/projects")
        headers = mock_req.call_args.kwargs["headers"]
        assert headers["Authorization"] == f"Bearer {FAKE_TOKEN}"


# -- Pagination -----------------------------------------------------------------


class TestPagination:
    @patch("ccef_connections.connectors.hex.requests.request")
    def test_follows_after_cursor(self, mock_req, connected):
        mock_req.side_effect = [
            _page([{"id": "p1"}, {"id": "p2"}], after="cursor1"),
            _page([{"id": "p3"}], after=None),
        ]
        projects = connected.list_projects()
        assert [p["id"] for p in projects] == ["p1", "p2", "p3"]
        # Second call must pass the cursor back as `after`.
        second_params = mock_req.call_args_list[1].kwargs["params"]
        assert second_params["after"] == "cursor1"

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_single_page(self, mock_req, connected):
        mock_req.return_value = _page([{"id": "p1"}])
        assert len(connected.list_projects()) == 1


# -- Cells ------------------------------------------------------------------------


class TestCells:
    @patch("ccef_connections.connectors.hex.requests.request")
    def test_list_cells_passes_project_id(self, mock_req, connected):
        mock_req.return_value = _page([_sql_cell()])
        cells = connected.list_cells(PROJECT_ID)
        assert cells[0]["cellType"] == "SQL"
        assert mock_req.call_args_list[0].kwargs["params"]["projectId"] == PROJECT_ID

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_create_cell_payload(self, mock_req, connected):
        mock_req.return_value = _make_response(200, _sql_cell())
        contents = {"sqlCell": {"source": "SELECT 1", "outputDataframe": "df"}}
        connected.create_cell(PROJECT_ID, "SQL", contents, label="test_sql")
        body = mock_req.call_args.kwargs["json"]
        assert body["projectId"] == PROJECT_ID
        assert body["cellType"] == "SQL"
        assert body["label"] == "test_sql"
        assert body["contents"] == contents

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_create_cell_bad_response_raises(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"whoops": True})
        with pytest.raises(ConnectionError):
            connected.create_cell(PROJECT_ID, "SQL", {"sqlCell": {"source": "x"}})

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_update_cell_sends_contents_only(self, mock_req, connected):
        mock_req.return_value = _make_response(200, _sql_cell(source="SELECT 2"))
        contents = {"sqlCell": {"source": "SELECT 2", "outputDataframe": "df"}}
        result = connected.update_cell(CELL_ID, contents)
        assert result["contents"]["sqlCell"]["source"] == "SELECT 2"
        assert mock_req.call_args.kwargs["json"] == {"contents": contents}

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_update_cell_attaches_data_connection(self, mock_req, connected):
        mock_req.return_value = _make_response(200, _sql_cell())
        connected.update_cell(CELL_ID, data_connection_id="conn-1")
        assert mock_req.call_args.kwargs["json"] == {"dataConnectionId": "conn-1"}

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_update_cell_contents_and_connection_together(self, mock_req, connected):
        mock_req.return_value = _make_response(200, _sql_cell())
        contents = {"sqlCell": {"source": "SELECT 2", "outputDataframe": "df"}}
        connected.update_cell(CELL_ID, contents=contents, data_connection_id="conn-1")
        assert mock_req.call_args.kwargs["json"] == {
            "contents": contents,
            "dataConnectionId": "conn-1",
        }

    def test_update_cell_requires_something(self, connected):
        with pytest.raises(ConnectionError, match="needs contents"):
            connected.update_cell(CELL_ID)

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_delete_cell_true_on_confirmation(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"cellId": CELL_ID})
        assert connected.delete_cell(CELL_ID) is True

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_delete_cell_false_on_404(self, mock_req, connected):
        mock_req.return_value = _make_response(404)
        assert connected.delete_cell(CELL_ID) is False


# -- Data connections ---------------------------------------------------------------


class TestDataConnections:
    @patch("ccef_connections.connectors.hex.requests.request")
    def test_list_data_connections(self, mock_req, connected):
        mock_req.return_value = _page(
            [{"id": "conn-1", "name": "COM Service Account", "type": "bigquery"}]
        )
        conns = connected.list_data_connections()
        assert conns[0]["type"] == "bigquery"


# -- Runs ---------------------------------------------------------------------------


class TestRuns:
    @patch("ccef_connections.connectors.hex.requests.request")
    def test_run_project_posts_options(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"runId": "r1"})
        result = connected.run_project(PROJECT_ID, options={"updatePublishedResults": False})
        assert result["runId"] == "r1"
        assert mock_req.call_args.kwargs["json"] == {"updatePublishedResults": False}

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_run_project_empty_response_raises(self, mock_req, connected):
        mock_req.return_value = _make_response(404)
        with pytest.raises(ConnectionError):
            connected.run_project(PROJECT_ID)


# -- Helpers --------------------------------------------------------------------------


class TestParseRetryAfter:
    def test_parses_header(self):
        assert _parse_retry_after({"Retry-After": "42"}) == 42

    def test_defaults_to_60(self):
        assert _parse_retry_after({}) == 60

    def test_unparseable_defaults_to_60(self):
        assert _parse_retry_after({"Retry-After": "soon"}) == 60


# -- Health check -----------------------------------------------------------------------


class TestHealthCheck:
    def test_not_connected_returns_false(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_healthy(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"values": []})
        assert connected.health_check() is True

    @patch("ccef_connections.connectors.hex.requests.request")
    def test_unhealthy_on_auth_failure(self, mock_req, connected):
        mock_req.return_value = _make_response(401)
        assert connected.health_check() is False
