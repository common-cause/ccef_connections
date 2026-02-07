"""Tests for the Zoom connector."""

import base64
import json
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.zoom import (
    ZOOM_API_BASE,
    ZOOM_TOKEN_URL,
    ZoomConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Fixtures ──────────────────────────────────────────────────────────

FAKE_CREDS = {
    "account_id": "test-account",
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
}

TOKEN_RESPONSE = {
    "access_token": "fake-zoom-token",
    "token_type": "bearer",
    "expires_in": 3600,
    "scope": "meeting:read webinar:read report:read",
}


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    return resp


@pytest.fixture
def connector():
    """Create a ZoomConnector with mocked credentials."""
    with patch.object(ZoomConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_zoom_credentials.return_value = FAKE_CREDS
        c = ZoomConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a fake token."""
    connector._access_token = "fake-zoom-token"
    connector._token_expires_at = time.time() + 3000
    connector._is_connected = True
    return connector


# ── Initialization ────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        connector = ZoomConnector()
        assert connector._access_token is None
        assert connector._token_expires_at == 0.0
        assert not connector.is_connected()

    def test_repr_disconnected(self):
        connector = ZoomConnector()
        assert repr(connector) == "<ZoomConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<ZoomConnector status=connected>"


# ── Connect / Disconnect ─────────────────────────────────────────────


class TestConnect:
    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_connect_success(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        assert connector.is_connected()
        assert connector._access_token == "fake-zoom-token"
        # Verify Basic auth header was sent
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"].startswith("Basic ")

    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_connect_sends_correct_basic_auth(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        call_kwargs = mock_post.call_args
        expected_basic = base64.b64encode(b"test-client-id:test-client-secret").decode()
        assert call_kwargs.kwargs["headers"]["Authorization"] == f"Basic {expected_basic}"

    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_connect_sends_account_credentials_grant(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["data"]["grant_type"] == "account_credentials"
        assert call_kwargs.kwargs["data"]["account_id"] == "test-account"

    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_connect_auth_failure(self, mock_post, connector):
        mock_post.return_value = _make_response(401, text="Unauthorized")

        with pytest.raises(AuthenticationError, match="401"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_connect_network_error(self, mock_post, connector):
        mock_post.side_effect = requests.ConnectionError("DNS failure")

        with pytest.raises(ConnectionError, match="Failed to connect to Zoom"):
            connector.connect()

    def test_connect_missing_credentials(self):
        connector = ZoomConnector()
        connector._credential_manager.get_zoom_credentials = MagicMock(
            side_effect=CredentialError("missing")
        )

        with pytest.raises(ConnectionError, match="missing"):
            connector.connect()

    def test_disconnect(self, connected_connector):
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._access_token is None
        assert connected_connector._token_expires_at == 0.0


# ── Health Check ──────────────────────────────────────────────────────


class TestHealthCheck:
    def test_health_check_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_health_check_success(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"id": "abc", "email": "a@b.com"})

        assert connected_connector.health_check() is True

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_health_check_failure(self, mock_request, connected_connector):
        mock_request.side_effect = requests.ConnectionError("timeout")

        assert connected_connector.health_check() is False


# ── Context Manager ───────────────────────────────────────────────────


class TestContextManager:
    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_context_manager(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with connector as c:
            assert c.is_connected()

        assert not c.is_connected()


# ── Token Refresh ─────────────────────────────────────────────────────


class TestTokenRefresh:
    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_refresh_when_expired(self, mock_post, connected_connector):
        mock_post.return_value = _make_response(
            200, {"access_token": "new-token", "expires_in": 3600}
        )
        connected_connector._token_expires_at = time.time() - 1  # expired

        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer new-token"
        mock_post.assert_called_once()

    def test_no_refresh_when_valid(self, connected_connector):
        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer fake-zoom-token"
        assert headers["Content-Type"] == "application/json"


# ── _request internals ────────────────────────────────────────────────


class TestRequest:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_request(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"id": 1})

        result = connected_connector._request("GET", "/users/me")

        assert result == {"id": 1}
        mock_request.assert_called_once()

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_post_request_with_body(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._request(
            "POST", "/meetings/1/registrants", json_body={"email": "a@b.com"}
        )

        assert result is None

    @patch("ccef_connections.connectors.zoom.requests.request")
    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_401_triggers_refresh_and_retry(
        self, mock_post, mock_request, connected_connector
    ):
        mock_request.side_effect = [
            _make_response(401),
            _make_response(200, {"ok": True}),
        ]
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 3600}
        )

        result = connected_connector._request("GET", "/users/me")

        assert result == {"ok": True}
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.zoom.requests.request")
    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_401_after_refresh_raises(self, mock_post, mock_request, connected_connector):
        mock_request.return_value = _make_response(401)
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 3600}
        )

        with pytest.raises(AuthenticationError, match="after token refresh"):
            connected_connector._request("GET", "/users/me")

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_429_raises_rate_limit(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(429, headers={"Retry-After": "5"})

        with pytest.raises(RateLimitError, match="retry after 5s") as exc_info:
            connected_connector._request("GET", "/users/me")

        assert exc_info.value.retry_after == 5

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_500_raises_connection_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(500, text="Internal Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector._request("GET", "/users/me")

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_network_error_raises_connection_error(self, mock_request, connected_connector):
        mock_request.side_effect = requests.ConnectionError("timeout")

        with pytest.raises(ConnectionError, match="request failed"):
            connected_connector._request("GET", "/users/me")

    @patch("ccef_connections.connectors.zoom.requests.post")
    def test_auto_connect_on_request(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with patch("ccef_connections.connectors.zoom.requests.request") as mock_request:
            mock_request.return_value = _make_response(200, {"data": "ok"})
            result = connector._request("GET", "/users/me")

        assert result == {"data": "ok"}
        assert connector.is_connected()


# ── Pagination ────────────────────────────────────────────────────────


class TestPagination:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_single_page(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {"meetings": [{"id": 1}, {"id": 2}], "next_page_token": ""},
        )

        result = connected_connector._paginate("/users/me/meetings", resource_key="meetings")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_multi_page(self, mock_request, connected_connector):
        page1 = _make_response(
            200,
            {"meetings": [{"id": 1}], "next_page_token": "token123"},
        )
        page2 = _make_response(
            200,
            {"meetings": [{"id": 2}], "next_page_token": ""},
        )
        mock_request.side_effect = [page1, page2]

        result = connected_connector._paginate("/users/me/meetings", resource_key="meetings")

        assert result == [{"id": 1}, {"id": 2}]
        assert mock_request.call_count == 2
        # Second call should include next_page_token
        second_call_params = mock_request.call_args_list[1].kwargs["params"]
        assert second_call_params["next_page_token"] == "token123"

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_pagination_includes_page_size(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"id": 1}], "next_page_token": ""}
        )

        connected_connector._paginate("/items", resource_key="items", page_size=100)

        assert mock_request.call_args.kwargs["params"]["page_size"] == 100

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_pagination_empty_response(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._paginate("/users/me/meetings", resource_key="meetings")

        assert result == []

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_pagination_missing_resource_key(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"other_key": [{"id": 1}], "next_page_token": ""}
        )

        result = connected_connector._paginate("/items", resource_key="meetings")

        assert result == []


# ── Users ─────────────────────────────────────────────────────────────


class TestGetUser:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_user_me(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": "abc", "email": "user@example.com", "type": 2}
        )

        result = connected_connector.get_user("me")

        assert result["email"] == "user@example.com"
        assert "/users/me" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_user_by_id(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": "xyz", "email": "other@example.com"}
        )

        result = connected_connector.get_user("xyz")

        assert "/users/xyz" in mock_request.call_args[0][1]


# ── Meetings ──────────────────────────────────────────────────────────


class TestListMeetings:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_list_meetings_default(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "meetings": [
                    {"id": 111, "topic": "Team Standup"},
                    {"id": 222, "topic": "All Hands"},
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.list_meetings("me")

        assert len(result) == 2
        assert result[0]["topic"] == "Team Standup"
        params = mock_request.call_args.kwargs["params"]
        assert params["type"] == "scheduled"

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_list_meetings_previous(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"meetings": [], "next_page_token": ""}
        )

        connected_connector.list_meetings("me", meeting_type="previous_meetings")

        params = mock_request.call_args.kwargs["params"]
        assert params["type"] == "previous_meetings"


class TestGetMeeting:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_meeting(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": 111, "topic": "Team Standup", "duration": 60}
        )

        result = connected_connector.get_meeting(111)

        assert result["id"] == 111
        assert "/meetings/111" in mock_request.call_args[0][1]


class TestGetPastMeetingParticipants:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_participants(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "participants": [
                    {
                        "name": "Alice",
                        "user_email": "alice@example.com",
                        "duration": 3600,
                        "join_time": "2025-01-01T10:00:00Z",
                        "leave_time": "2025-01-01T11:00:00Z",
                    },
                    {
                        "name": "Bob",
                        "user_email": "bob@example.com",
                        "duration": 1800,
                        "join_time": "2025-01-01T10:30:00Z",
                        "leave_time": "2025-01-01T11:00:00Z",
                    },
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.get_past_meeting_participants("12345678901")

        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["user_email"] == "bob@example.com"
        assert "/report/meetings/12345678901/participants" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_participants_paginated(self, mock_request, connected_connector):
        page1 = _make_response(
            200,
            {
                "participants": [{"name": "Alice"}],
                "next_page_token": "tok123",
            },
        )
        page2 = _make_response(
            200,
            {
                "participants": [{"name": "Bob"}],
                "next_page_token": "",
            },
        )
        mock_request.side_effect = [page1, page2]

        result = connected_connector.get_past_meeting_participants("12345")

        assert len(result) == 2
        assert result[0]["name"] == "Alice"
        assert result[1]["name"] == "Bob"


# ── Webinars ──────────────────────────────────────────────────────────


class TestListWebinars:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_list_webinars(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "webinars": [
                    {"id": 999, "topic": "Member Town Hall"},
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.list_webinars("me")

        assert len(result) == 1
        assert result[0]["topic"] == "Member Town Hall"


class TestGetWebinar:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_webinar(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": 999, "topic": "Member Town Hall", "duration": 90}
        )

        result = connected_connector.get_webinar(999)

        assert result["id"] == 999
        assert "/webinars/999" in mock_request.call_args[0][1]


class TestGetWebinarRegistrants:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_registrants_default_approved(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "registrants": [
                    {"first_name": "Alice", "email": "alice@example.com"},
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.get_webinar_registrants(999)

        assert len(result) == 1
        params = mock_request.call_args.kwargs["params"]
        assert params["status"] == "approved"

    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_registrants_pending(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"registrants": [], "next_page_token": ""}
        )

        connected_connector.get_webinar_registrants(999, status="pending")

        params = mock_request.call_args.kwargs["params"]
        assert params["status"] == "pending"


class TestGetPastWebinarParticipants:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_past_webinar_participants(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "participants": [
                    {
                        "name": "Alice",
                        "user_email": "alice@example.com",
                        "duration": 5400,
                    },
                    {
                        "name": "Bob",
                        "user_email": "bob@example.com",
                        "duration": 5400,
                    },
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.get_past_webinar_participants("999")

        assert len(result) == 2
        assert "/report/webinars/999/participants" in mock_request.call_args[0][1]


class TestGetWebinarAbsentees:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_absentees(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "registrants": [
                    {"first_name": "NoShow", "email": "noshow@example.com"},
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.get_webinar_absentees("uuid-abc")

        assert len(result) == 1
        assert "/past_webinars/uuid-abc/absentees" in mock_request.call_args[0][1]


# ── Meeting Registrants ──────────────────────────────────────────────


class TestGetMeetingRegistrants:
    @patch("ccef_connections.connectors.zoom.requests.request")
    def test_get_meeting_registrants(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "registrants": [
                    {"first_name": "Alice", "email": "alice@example.com"},
                ],
                "next_page_token": "",
            },
        )

        result = connected_connector.get_meeting_registrants(111)

        assert len(result) == 1
        assert "/meetings/111/registrants" in mock_request.call_args[0][1]
        assert mock_request.call_args.kwargs["params"]["status"] == "approved"


# ── Credentials ───────────────────────────────────────────────────────


class TestCredentials:
    def _make_manager(self):
        """Create a fresh CredentialManager instance, bypassing the singleton."""
        from ccef_connections.core.credentials import CredentialManager

        mgr = object.__new__(CredentialManager)
        mgr._credentials_cache = {}
        mgr._env_loaded = True
        return mgr

    def test_get_zoom_credentials_success(self):
        creds_json = json.dumps(FAKE_CREDS)
        with patch.dict("os.environ", {"ZOOM_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            result = cm.get_zoom_credentials()

        assert result["account_id"] == "test-account"
        assert result["client_id"] == "test-client-id"
        assert result["client_secret"] == "test-client-secret"

    def test_get_zoom_credentials_missing_keys(self):
        creds_json = json.dumps({"account_id": "only-account"})
        with patch.dict("os.environ", {"ZOOM_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="client_id"):
                cm.get_zoom_credentials()

    def test_get_zoom_credentials_invalid_json(self):
        with patch.dict("os.environ", {"ZOOM_CREDENTIALS_PASSWORD": "bad-json"}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="valid JSON"):
                cm.get_zoom_credentials()

    def test_get_zoom_credentials_missing_env(self):
        with patch.dict("os.environ", {}, clear=True):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="ZOOM_CREDENTIALS_PASSWORD"):
                cm.get_zoom_credentials()
