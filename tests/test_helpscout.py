"""Tests for the HelpScout connector."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.helpscout import (
    HELPSCOUT_API_BASE,
    HELPSCOUT_TOKEN_URL,
    HelpScoutConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Fixtures ──────────────────────────────────────────────────────────


FAKE_CREDS = {"app_id": "test-id", "app_secret": "test-secret"}

TOKEN_RESPONSE = {
    "access_token": "fake-token-abc",
    "expires_in": 172800,
    "token_type": "bearer",
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
    """Create a HelpScoutConnector with mocked credentials."""
    with patch.object(
        HelpScoutConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_helpscout_credentials.return_value = FAKE_CREDS
        c = HelpScoutConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a fake token."""
    connector._access_token = "fake-token-abc"
    connector._token_expires_at = time.time() + 86400
    connector._is_connected = True
    return connector


# ── Initialization ────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        connector = HelpScoutConnector()
        assert connector._access_token is None
        assert connector._token_expires_at == 0.0
        assert not connector.is_connected()

    def test_repr_disconnected(self):
        connector = HelpScoutConnector()
        assert repr(connector) == "<HelpScoutConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<HelpScoutConnector status=connected>"


# ── Connect / Disconnect ─────────────────────────────────────────────


class TestConnect:
    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_connect_success(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        assert connector.is_connected()
        assert connector._access_token == "fake-token-abc"
        mock_post.assert_called_once_with(
            HELPSCOUT_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": "test-id",
                "client_secret": "test-secret",
            },
            timeout=30,
        )

    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_connect_auth_failure(self, mock_post, connector):
        mock_post.return_value = _make_response(403, text="Forbidden")

        with pytest.raises(AuthenticationError, match="403"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_connect_network_error(self, mock_post, connector):
        mock_post.side_effect = requests.ConnectionError("DNS failure")

        with pytest.raises(ConnectionError, match="Failed to connect to HelpScout"):
            connector.connect()

    def test_connect_missing_credentials(self):
        connector = HelpScoutConnector()
        connector._credential_manager.get_helpscout_credentials = MagicMock(
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

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_health_check_success(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"id": 1, "email": "a@b.com"})

        assert connected_connector.health_check() is True
        mock_request.assert_called_once()

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_health_check_failure(self, mock_request, connected_connector):
        mock_request.side_effect = requests.ConnectionError("timeout")

        assert connected_connector.health_check() is False


# ── Context Manager ───────────────────────────────────────────────────


class TestContextManager:
    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_context_manager(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with connector as c:
            assert c.is_connected()

        assert not c.is_connected()


# ── Token Refresh ─────────────────────────────────────────────────────


class TestTokenRefresh:
    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_refresh_when_expired(self, mock_post, connected_connector):
        mock_post.return_value = _make_response(
            200, {"access_token": "new-token", "expires_in": 172800}
        )
        connected_connector._token_expires_at = time.time() - 1  # expired

        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer new-token"
        mock_post.assert_called_once()

    def test_no_refresh_when_valid(self, connected_connector):
        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer fake-token-abc"
        assert headers["Content-Type"] == "application/json"


# ── _request internals ────────────────────────────────────────────────


class TestRequest:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_get_request(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"id": 1})

        result = connected_connector._request("GET", "/mailboxes")

        assert result == {"id": 1}
        mock_request.assert_called_once_with(
            "GET",
            f"{HELPSCOUT_API_BASE}/mailboxes",
            headers=connected_connector._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_post_request_with_body(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._request(
            "POST", "/conversations/1/reply", json_body={"text": "hi"}
        )

        assert result is None
        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["json"] == {"text": "hi"}

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_201_returns_none(self, mock_request, connected_connector):
        """Write operations (reply, note) return 201 with no body."""
        mock_request.return_value = _make_response(201)

        result = connected_connector._request(
            "POST", "/conversations/1/notes", json_body={"text": "note"}
        )

        assert result is None

    @patch("ccef_connections.connectors.helpscout.requests.request")
    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_401_triggers_refresh_and_retry(
        self, mock_post, mock_request, connected_connector
    ):
        """First call returns 401, token refresh happens, retry succeeds."""
        mock_request.side_effect = [
            _make_response(401),
            _make_response(200, {"ok": True}),
        ]
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 172800}
        )

        result = connected_connector._request("GET", "/users/me")

        assert result == {"ok": True}
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.helpscout.requests.request")
    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_401_after_refresh_raises(self, mock_post, mock_request, connected_connector):
        """Both calls return 401 -> AuthenticationError."""
        mock_request.return_value = _make_response(401)
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 172800}
        )

        with pytest.raises(AuthenticationError, match="after token refresh"):
            connected_connector._request("GET", "/users/me")

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_429_raises_rate_limit(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            429, headers={"X-RateLimit-Retry-After": "30"}
        )

        with pytest.raises(RateLimitError, match="retry after 30s") as exc_info:
            connected_connector._request("GET", "/mailboxes")

        assert exc_info.value.retry_after == 30

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_429_default_retry_after(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(429, headers={})

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector._request("GET", "/mailboxes")

        assert exc_info.value.retry_after == 10

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_500_raises_connection_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(500, text="Internal Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector._request("GET", "/mailboxes")

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_network_error_raises_connection_error(
        self, mock_request, connected_connector
    ):
        mock_request.side_effect = requests.ConnectionError("timeout")

        with pytest.raises(ConnectionError, match="request failed"):
            connected_connector._request("GET", "/mailboxes")

    @patch("ccef_connections.connectors.helpscout.requests.post")
    def test_auto_connect_on_request(self, mock_post, connector):
        """_request auto-connects when not connected and no token."""
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with patch(
            "ccef_connections.connectors.helpscout.requests.request"
        ) as mock_request:
            mock_request.return_value = _make_response(200, {"data": "ok"})
            result = connector._request("GET", "/mailboxes")

        assert result == {"data": "ok"}
        assert connector.is_connected()


# ── Pagination ────────────────────────────────────────────────────────


class TestPagination:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_single_page(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "_embedded": {"mailboxes": [{"id": 1}, {"id": 2}]},
                "_links": {},
            },
        )

        result = connected_connector._paginate("/mailboxes", resource_key="mailboxes")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_multi_page(self, mock_request, connected_connector):
        page1 = _make_response(
            200,
            {
                "_embedded": {"mailboxes": [{"id": 1}]},
                "_links": {
                    "next": {"href": f"{HELPSCOUT_API_BASE}/mailboxes?page=2"}
                },
            },
        )
        page2 = _make_response(
            200,
            {
                "_embedded": {"mailboxes": [{"id": 2}]},
                "_links": {},
            },
        )
        mock_request.side_effect = [page1, page2]

        result = connected_connector._paginate("/mailboxes", resource_key="mailboxes")

        assert result == [{"id": 1}, {"id": 2}]
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_pagination_without_resource_key(self, mock_request, connected_connector):
        """Falls back to first key in _embedded when no resource_key given."""
        mock_request.return_value = _make_response(
            200,
            {
                "_embedded": {"items": [{"id": 10}]},
                "_links": {},
            },
        )

        result = connected_connector._paginate("/items")

        assert result == [{"id": 10}]

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_pagination_empty_response(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._paginate("/mailboxes", resource_key="mailboxes")

        assert result == []

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_pagination_strips_base_url_from_next(self, mock_request, connected_connector):
        """next link with full URL is reduced to a path."""
        page1 = _make_response(
            200,
            {
                "_embedded": {"mailboxes": [{"id": 1}]},
                "_links": {
                    "next": {"href": f"{HELPSCOUT_API_BASE}/mailboxes?page=2"}
                },
            },
        )
        page2 = _make_response(
            200,
            {"_embedded": {"mailboxes": [{"id": 2}]}, "_links": {}},
        )
        mock_request.side_effect = [page1, page2]

        connected_connector._paginate("/mailboxes", resource_key="mailboxes")

        # Second call should use the path, not full URL
        second_call_url = mock_request.call_args_list[1].kwargs.get(
            "params"
        ) or mock_request.call_args_list[1][1].get("params")
        # params should be None on the second call (encoded in the URL path)
        assert second_call_url is None


# ── Mailboxes ─────────────────────────────────────────────────────────


class TestListMailboxes:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_list_mailboxes(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "_embedded": {
                    "mailboxes": [
                        {"id": 1, "name": "Support", "email": "support@example.com"},
                        {"id": 2, "name": "Sales", "email": "sales@example.com"},
                    ]
                },
                "_links": {},
            },
        )

        result = connected_connector.list_mailboxes()

        assert len(result) == 2
        assert result[0]["name"] == "Support"
        assert result[1]["name"] == "Sales"


# ── Conversations (read) ─────────────────────────────────────────────


class TestListConversations:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_list_conversations_basic(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "_embedded": {
                    "conversations": [
                        {"id": 100, "subject": "Help!"},
                        {"id": 101, "subject": "Question"},
                    ]
                },
                "_links": {},
            },
        )

        result = connected_connector.list_conversations(mailbox_id=1)

        assert len(result) == 2
        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["params"] == {"mailbox": 1}

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_list_conversations_with_filters(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {"_embedded": {"conversations": [{"id": 100}]}, "_links": {}},
        )

        connected_connector.list_conversations(
            mailbox_id=1, status="active", tag="urgent"
        )

        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["params"] == {
            "mailbox": 1,
            "status": "active",
            "tag": "urgent",
        }

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_list_conversations_with_kwargs(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {"_embedded": {"conversations": []}, "_links": {}},
        )

        connected_connector.list_conversations(
            mailbox_id=1, sortField="createdAt"
        )

        call_kwargs = mock_request.call_args
        assert call_kwargs.kwargs["params"]["sortField"] == "createdAt"


class TestGetConversation:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_get_conversation(self, mock_request, connected_connector):
        conversation_data = {"id": 100, "subject": "Help!", "status": "active"}
        mock_request.return_value = _make_response(200, conversation_data)

        result = connected_connector.get_conversation(100)

        assert result == conversation_data
        mock_request.assert_called_once()
        assert "/conversations/100" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_get_conversation_returns_empty_on_204(
        self, mock_request, connected_connector
    ):
        mock_request.return_value = _make_response(204)

        result = connected_connector.get_conversation(999)

        assert result == {}


class TestListThreads:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_list_threads(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "_embedded": {
                    "threads": [
                        {"id": 1, "body": "Hello"},
                        {"id": 2, "body": "Reply"},
                    ]
                },
                "_links": {},
            },
        )

        result = connected_connector.list_threads(100)

        assert len(result) == 2
        assert result[0]["body"] == "Hello"
        assert "/conversations/100/threads" in mock_request.call_args[0][1]


# ── Conversations (write) ────────────────────────────────────────────


class TestReplyToConversation:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_reply_basic(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201)

        connected_connector.reply_to_conversation(100, "Thanks for writing in!", customer_id=42)

        mock_request.assert_called_once()
        call_args = mock_request.call_args
        assert call_args[0][0] == "POST"
        assert "/conversations/100/reply" in call_args[0][1]
        body = call_args.kwargs["json"]
        assert body["text"] == "Thanks for writing in!"
        assert body["customer"] == {"id": 42}
        assert body["draft"] is False

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_reply_as_draft(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201)

        connected_connector.reply_to_conversation(
            100, "Draft reply", customer_id=42, draft=True
        )

        assert mock_request.call_args.kwargs["json"]["draft"] is True

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_reply_with_kwargs(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201)

        connected_connector.reply_to_conversation(
            100, "Hello", customer_id=42, cc=["cc@example.com"], bcc=["bcc@example.com"]
        )

        body = mock_request.call_args.kwargs["json"]
        assert body["cc"] == ["cc@example.com"]
        assert body["bcc"] == ["bcc@example.com"]


class TestAddNote:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_add_note(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201)

        connected_connector.add_note(100, "Internal note here")

        call_args = mock_request.call_args
        assert call_args[0][0] == "POST"
        assert "/conversations/100/notes" in call_args[0][1]
        assert call_args.kwargs["json"] == {"text": "Internal note here"}


class TestUpdateConversationStatus:
    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_update_status_closed(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        connected_connector.update_conversation_status(100, "closed")

        call_args = mock_request.call_args
        assert call_args[0][0] == "PATCH"
        assert "/conversations/100" in call_args[0][1]
        assert call_args.kwargs["json"]["value"] == "closed"

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_update_status_active(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        connected_connector.update_conversation_status(100, "active")

        assert mock_request.call_args.kwargs["json"]["value"] == "active"

    @patch("ccef_connections.connectors.helpscout.requests.request")
    def test_update_status_pending(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        connected_connector.update_conversation_status(100, "pending")

        assert mock_request.call_args.kwargs["json"]["value"] == "pending"

    def test_update_status_invalid(self, connected_connector):
        with pytest.raises(ValueError, match="Invalid status 'spam'"):
            connected_connector.update_conversation_status(100, "spam")


# ── Credentials ───────────────────────────────────────────────────────


class TestCredentials:
    """Test get_helpscout_credentials on a fresh CredentialManager (bypass singleton)."""

    def _make_manager(self):
        """Create a fresh CredentialManager instance, bypassing the singleton."""
        from ccef_connections.core.credentials import CredentialManager

        mgr = object.__new__(CredentialManager)
        mgr._credentials_cache = {}
        mgr._env_loaded = True  # skip dotenv reload
        return mgr

    def test_get_helpscout_credentials_success(self):
        creds_json = json.dumps(FAKE_CREDS)
        with patch.dict("os.environ", {"HELPSCOUT_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            result = cm.get_helpscout_credentials()

        assert result["app_id"] == "test-id"
        assert result["app_secret"] == "test-secret"

    def test_get_helpscout_credentials_missing_keys(self):
        creds_json = json.dumps({"app_id": "only-id"})
        with patch.dict("os.environ", {"HELPSCOUT_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="app_secret"):
                cm.get_helpscout_credentials()

    def test_get_helpscout_credentials_invalid_json(self):
        with patch.dict("os.environ", {"HELPSCOUT_CREDENTIALS_PASSWORD": "not-json"}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="valid JSON"):
                cm.get_helpscout_credentials()

    def test_get_helpscout_credentials_missing_env(self):
        with patch.dict("os.environ", {}, clear=True):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="HELPSCOUT_CREDENTIALS_PASSWORD"):
                cm.get_helpscout_credentials()
