"""Tests for the transactional-email (Resend) connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.email_connector import (
    RESEND_API_BASE,
    EmailConnector,
)
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


FAKE_API_KEY = "test-resend-api-key"
SEND_RESPONSE = {"id": "49a3999c-0ce1-4ea6-ab68-afcd6dc2e794"}
FROM = "EP Roving Review <auth@mail.commoncause.org>"


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def connector():
    """Create an EmailConnector with a mocked credential manager."""
    with patch.object(EmailConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_resend_api_key.return_value = FAKE_API_KEY
        c = EmailConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Return a connector that is already connected."""
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


# ── Initialization ─────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = EmailConnector()
        assert c._api_key is None
        assert not c.is_connected()

    def test_inherits_base_connection(self):
        from ccef_connections.core.base import BaseConnection

        assert isinstance(EmailConnector(), BaseConnection)


# ── Connect / Disconnect ───────────────────────────────────────────────


class TestConnect:
    def test_connect_loads_api_key(self, connector):
        connector.connect()
        assert connector._api_key == FAKE_API_KEY
        assert connector.is_connected()

    def test_connect_calls_credential_manager(self, connector):
        connector.connect()
        connector._credential_manager.get_resend_api_key.assert_called_once()

    def test_connect_raises_connection_error_on_failure(self, connector):
        connector._credential_manager.get_resend_api_key.side_effect = CredentialError(
            "Missing key"
        )
        with pytest.raises(ConnectionError, match="Failed to connect to Resend"):
            connector.connect()

    def test_connect_wraps_credential_error(self, connector):
        original = CredentialError("env var not set")
        connector._credential_manager.get_resend_api_key.side_effect = original
        with pytest.raises(ConnectionError) as exc_info:
            connector.connect()
        assert exc_info.value.__cause__ is original


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()
        assert connected_connector._api_key is None
        assert not connected_connector.is_connected()


# ── Health check ───────────────────────────────────────────────────────


class TestHealthCheck:
    def test_returns_true_when_connected(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_returns_false_when_disconnected(self, connector):
        assert connector.health_check() is False

    def test_returns_false_when_key_is_none(self, connected_connector):
        connected_connector._api_key = None
        assert connected_connector.health_check() is False


# ── Context manager ────────────────────────────────────────────────────


class TestContextManager:
    def test_enter_connects(self, connector):
        with connector as c:
            assert c.is_connected()

    def test_exit_disconnects(self, connector):
        with connector as c:
            pass
        assert not c.is_connected()


# ── send ──────────────────────────────────────────────────────────────


class TestSend:
    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_posts_to_emails_endpoint(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        connected_connector.send(
            to="a@b.org", subject="Hi", html="<p>x</p>", from_addr=FROM
        )

        assert mock_req.call_args[0][0] == "POST"
        assert mock_req.call_args[0][1] == f"{RESEND_API_BASE}/emails"

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_uses_bearer_auth(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        connected_connector.send(
            to="a@b.org", subject="Hi", text="x", from_addr=FROM
        )

        headers = mock_req.call_args.kwargs["headers"]
        assert headers["Authorization"] == f"Bearer {FAKE_API_KEY}"

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_normalizes_single_recipient_to_list(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        connected_connector.send(
            to="a@b.org", subject="Hi", html="<p>x</p>", from_addr=FROM
        )

        body = mock_req.call_args.kwargs["json"]
        assert body["to"] == ["a@b.org"]
        assert body["from"] == FROM
        assert body["subject"] == "Hi"
        assert body["html"] == "<p>x</p>"

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_passes_list_of_recipients(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        connected_connector.send(
            to=["a@b.org", "c@d.org"], subject="Hi", text="x", from_addr=FROM
        )

        assert mock_req.call_args.kwargs["json"]["to"] == ["a@b.org", "c@d.org"]

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_returns_response(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        result = connected_connector.send(
            to="a@b.org", subject="Hi", html="<p>x</p>", from_addr=FROM
        )

        assert result == SEND_RESPONSE

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_send_uses_env_from_when_not_given(self, mock_req, connected_connector, monkeypatch):
        monkeypatch.setenv("RESEND_FROM_EMAIL", "env@x.org")
        mock_req.return_value = _make_response(200, SEND_RESPONSE)

        connected_connector.send(to="a@b.org", subject="Hi", text="x")

        assert mock_req.call_args.kwargs["json"]["from"] == "env@x.org"

    def test_send_requires_a_sender(self, connected_connector, monkeypatch):
        monkeypatch.delenv("RESEND_FROM_EMAIL", raising=False)
        with pytest.raises(ValueError, match="from_addr is required"):
            connected_connector.send(to="a@b.org", subject="Hi", text="x")

    def test_send_requires_a_body(self, connected_connector):
        with pytest.raises(ValueError, match="html= or text="):
            connected_connector.send(to="a@b.org", subject="Hi", from_addr=FROM)

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_401_raises_authentication_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(401, text="Unauthorized")
        with pytest.raises(AuthenticationError):
            connected_connector.send(to="a@b.org", subject="Hi", text="x", from_addr=FROM)

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_429_raises_rate_limit_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "12"}
        )
        with pytest.raises(RateLimitError) as exc_info:
            connected_connector.send(to="a@b.org", subject="Hi", text="x", from_addr=FROM)
        assert exc_info.value.retry_after == 12

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_422_raises_connection_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(422, text="Unprocessable Entity")
        with pytest.raises(ConnectionError, match="422"):
            connected_connector.send(to="a@b.org", subject="Hi", text="x", from_addr=FROM)

    @patch("ccef_connections.connectors.email_connector.requests.request")
    def test_network_error_raises_connection_error(self, mock_req, connected_connector):
        mock_req.side_effect = requests.RequestException("timeout")
        with pytest.raises(ConnectionError, match="Resend API request failed"):
            connected_connector.send(to="a@b.org", subject="Hi", text="x", from_addr=FROM)

    def test_send_has_retry_decorator(self):
        assert hasattr(EmailConnector.send, "retry")
