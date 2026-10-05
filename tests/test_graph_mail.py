"""Tests for the Microsoft Graph mail connector. All HTTP is mocked."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.graph_mail import (
    GRAPH_API_BASE,
    GraphMailConnector,
)
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

POST = "ccef_connections.connectors.graph_mail.requests.post"

FAKE_CREDS = {
    "tenant_id": "tenant-123",
    "client_id": "client-456",
    "client_secret": "secret-789",
    "default_sender": "grants@example.org",
}


def _resp(status_code=200, json_data=None, text="", headers=None):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


TOKEN = _resp(200, {"access_token": "tok-1", "expires_in": 3599})


def _token_then(*send_responses):
    """requests.post side effect: the token call, then each send in turn."""
    return [TOKEN, *send_responses]


@pytest.fixture
def connector(monkeypatch):
    monkeypatch.delenv("SEND_EMAIL_SENDER", raising=False)
    with patch.object(GraphMailConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_graph_mail_credentials.return_value = dict(FAKE_CREDS)
        c = GraphMailConnector()
        c._credential_manager = mock_cm
        yield c


# ── Connect / health ───────────────────────────────────────────────────


class TestConnect:
    def test_initial_state(self):
        c = GraphMailConnector()
        assert not c.is_connected()
        assert c.health_check() is False

    def test_connect_loads_credentials_without_a_network_call(self, connector):
        with patch(POST) as mock_post:
            connector.connect()
        assert connector.health_check() is True
        mock_post.assert_not_called()

    def test_credential_error_is_not_wrapped(self, connector):
        original = CredentialError("missing")
        connector._credential_manager.get_graph_mail_credentials.side_effect = original
        with pytest.raises(CredentialError) as exc_info:
            connector.connect()
        assert exc_info.value is original

    def test_unexpected_failure_is_wrapped(self, connector):
        connector._credential_manager.get_graph_mail_credentials.side_effect = RuntimeError("odd")
        with pytest.raises(ConnectionError, match="Failed to connect to Graph mail"):
            connector.connect()

    def test_disconnect_clears_token(self, connector):
        connector.connect()
        connector._token = "tok"
        connector.disconnect()
        assert connector._token is None
        assert not connector.is_connected()


# ── Sender resolution ──────────────────────────────────────────────────


class TestResolveSender:
    def test_argument_wins(self, connector, monkeypatch):
        monkeypatch.setenv("SEND_EMAIL_SENDER", "env@example.org")
        assert connector.resolve_sender("arg@example.org") == "arg@example.org"

    def test_env_beats_default_sender(self, connector, monkeypatch):
        monkeypatch.setenv("SEND_EMAIL_SENDER", " dataops@example.org ")
        assert connector.resolve_sender() == "dataops@example.org"

    def test_falls_back_to_default_sender(self, connector):
        assert connector.resolve_sender() == "grants@example.org"

    def test_no_sender_anywhere_raises(self, connector):
        creds = dict(FAKE_CREDS)
        del creds["default_sender"]
        connector._credential_manager.get_graph_mail_credentials.return_value = creds
        with pytest.raises(ValueError, match="No sender"):
            connector.resolve_sender()


# ── send ───────────────────────────────────────────────────────────────


class TestSend:
    def test_token_request_uses_client_credentials(self, connector):
        with patch(POST, side_effect=_token_then(_resp(202))) as mock_post:
            connector.send("a@example.org", "Hi", text="x")
        url = mock_post.call_args_list[0].args[0]
        data = mock_post.call_args_list[0].kwargs["data"]
        assert "tenant-123" in url
        assert data["grant_type"] == "client_credentials"
        assert data["client_id"] == "client-456"
        assert data["scope"] == "https://graph.microsoft.com/.default"

    def test_posts_sendmail_as_sender(self, connector):
        with patch(POST, side_effect=_token_then(_resp(202))) as mock_post:
            status = connector.send("a@example.org", "Hi", html="<p>x</p>",
                                    sender="dataops@example.org")
        assert status == 202
        call = mock_post.call_args_list[1]
        assert call.args[0] == f"{GRAPH_API_BASE}/users/dataops@example.org/sendMail"
        assert call.kwargs["headers"]["Authorization"] == "Bearer tok-1"
        body = call.kwargs["json"]
        assert body["saveToSentItems"] is True
        msg = body["message"]
        assert msg["subject"] == "Hi"
        assert msg["body"] == {"contentType": "HTML", "content": "<p>x</p>"}
        assert msg["toRecipients"] == [{"emailAddress": {"address": "a@example.org"}}]
        assert "ccRecipients" not in msg

    def test_text_body_when_no_html(self, connector):
        with patch(POST, side_effect=_token_then(_resp(202))) as mock_post:
            connector.send("a@example.org", "Hi", text="plain")
        msg = mock_post.call_args_list[1].kwargs["json"]["message"]
        assert msg["body"] == {"contentType": "Text", "content": "plain"}

    def test_lists_and_optional_recipients(self, connector):
        with patch(POST, side_effect=_token_then(_resp(202))) as mock_post:
            connector.send(["a@example.org", "b@example.org"], "Hi", text="x",
                           cc="c@example.org", bcc=["d@example.org"],
                           reply_to="r@example.org")
        msg = mock_post.call_args_list[1].kwargs["json"]["message"]
        assert [r["emailAddress"]["address"] for r in msg["toRecipients"]] == [
            "a@example.org", "b@example.org"]
        assert msg["ccRecipients"] == [{"emailAddress": {"address": "c@example.org"}}]
        assert msg["bccRecipients"] == [{"emailAddress": {"address": "d@example.org"}}]
        assert msg["replyTo"] == [{"emailAddress": {"address": "r@example.org"}}]

    def test_token_is_reused_across_sends(self, connector):
        with patch(POST, side_effect=_token_then(_resp(202), _resp(202))) as mock_post:
            connector.send("a@example.org", "1", text="x")
            connector.send("a@example.org", "2", text="x")
        assert mock_post.call_count == 3  # one token, two sends

    def test_requires_body(self, connector):
        with pytest.raises(ValueError, match="html= or text="):
            connector.send("a@example.org", "Hi")

    def test_requires_recipient(self, connector):
        with pytest.raises(ValueError, match="recipient"):
            connector.send([], "Hi", text="x")

    def test_rejected_token_raises_authentication_error(self, connector):
        bad = _resp(401, text='{"error":"invalid_client"}')
        with patch(POST, side_effect=[bad]):
            with pytest.raises(AuthenticationError, match="invalid_client"):
                connector.send("a@example.org", "Hi", text="x")

    def test_403_names_the_access_policy(self, connector):
        with patch(POST, side_effect=_token_then(_resp(403, text="ErrorAccessDenied"))):
            with pytest.raises(AuthenticationError, match="ApplicationAccessPolicy"):
                connector.send("a@example.org", "Hi", text="x", sender="new@example.org")

    def test_401_drops_cached_token(self, connector):
        with patch(POST, side_effect=_token_then(_resp(401, text="expired"))):
            with pytest.raises(AuthenticationError):
                connector.send("a@example.org", "Hi", text="x")
        assert connector._token is None

    def test_429_raises_rate_limit_error(self, connector):
        # Call the undecorated function: send() would back off through real sleeps.
        throttled = _resp(429, text="slow down", headers={"Retry-After": "7"})
        with patch(POST, side_effect=_token_then(throttled)):
            with pytest.raises(RateLimitError) as exc_info:
                GraphMailConnector.send.__wrapped__(connector, "a@example.org", "Hi", text="x")
        assert exc_info.value.retry_after == 7

    def test_5xx_is_not_retried(self, connector):
        with patch(POST, side_effect=_token_then(_resp(503, text="unavailable"))) as mock_post:
            with pytest.raises(ConnectionError, match="503"):
                connector.send("a@example.org", "Hi", text="x")
        assert mock_post.call_count == 2

    def test_network_error_raises_connection_error(self, connector):
        with patch(POST, side_effect=[TOKEN, requests.RequestException("timeout")]):
            with pytest.raises(ConnectionError, match="sendMail request failed"):
                connector.send("a@example.org", "Hi", text="x")

    def test_send_has_retry_decorator(self):
        assert hasattr(GraphMailConnector.send, "retry")


# ── Credential getter ──────────────────────────────────────────────────


class TestCredentials:
    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        CredentialManager._credentials_cache.pop("SEND_EMAIL_CREDENTIALS", None)
        yield
        CredentialManager._credentials_cache.pop("SEND_EMAIL_CREDENTIALS", None)

    def test_parses_json(self, monkeypatch):
        monkeypatch.setenv(
            "SEND_EMAIL_CREDENTIALS_PASSWORD",
            '{"tenant_id":"t","client_id":"c","client_secret":"s","default_sender":"g@x.org"}',
        )
        assert CredentialManager().get_graph_mail_credentials() == {
            "tenant_id": "t", "client_id": "c", "client_secret": "s",
            "default_sender": "g@x.org"}

    def test_default_sender_is_optional(self, monkeypatch):
        monkeypatch.setenv(
            "SEND_EMAIL_CREDENTIALS_PASSWORD",
            '{"tenant_id":"t","client_id":"c","client_secret":"s"}',
        )
        assert "default_sender" not in CredentialManager().get_graph_mail_credentials()

    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.setenv("SEND_EMAIL_CREDENTIALS_PASSWORD", '{"tenant_id":"t"}')
        with pytest.raises(CredentialError, match="client_secret"):
            CredentialManager().get_graph_mail_credentials()


def test_lazy_export():
    import ccef_connections

    assert ccef_connections.GraphMailConnector is GraphMailConnector
