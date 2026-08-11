"""Tests for the Tatango (MomoGood) connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.tatango import (
    TATANGO_API_BASE,
    TatangoConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# -- helpers ----------------------------------------------------------------

FAKE_EMAIL = "api-user@example.org"
FAKE_API_KEY = "fake-tatango-api-key-12345"
LIST_ID = "1061380"


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if json_data is None and status_code < 400:
        resp.json.return_value = {}
    else:
        resp.json.return_value = json_data
    return resp


# -- fixtures ---------------------------------------------------------------


@pytest.fixture
def connector():
    """Create a TatangoConnector with mocked credentials and no pacing."""
    with patch.object(
        TatangoConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_tatango_credentials.return_value = {
            "email": FAKE_EMAIL,
            "api_key": FAKE_API_KEY,
        }
        c = TatangoConnector(default_list_id=LIST_ID, min_request_interval=0)
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """Return a connector that is already connected."""
    connector._login_email = FAKE_EMAIL
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


# ==========================================================================
# Initialization
# ==========================================================================


class TestInit:
    def test_initial_state(self):
        c = TatangoConnector()
        assert c._api_key is None
        assert c._login_email is None
        assert c._default_list_id is None
        assert not c.is_connected()

    def test_default_list_id_coerced_to_str(self):
        c = TatangoConnector(default_list_id=1061380)
        assert c._default_list_id == "1061380"

    def test_repr_disconnected(self):
        c = TatangoConnector()
        assert repr(c) == "<TatangoConnector status=disconnected>"

    def test_repr_connected(self, connected):
        assert repr(connected) == "<TatangoConnector status=connected>"


# ==========================================================================
# Connect / Disconnect
# ==========================================================================


class TestConnect:
    def test_connect_success(self, connector):
        connector.connect()
        assert connector.is_connected()
        assert connector._login_email == FAKE_EMAIL
        assert connector._api_key == FAKE_API_KEY

    def test_connect_missing_credentials(self):
        c = TatangoConnector()
        c._credential_manager.get_tatango_credentials = MagicMock(
            side_effect=CredentialError("missing")
        )
        with pytest.raises(ConnectionError, match="missing"):
            c.connect()

    def test_disconnect(self, connected):
        connected.disconnect()
        assert not connected.is_connected()
        assert connected._api_key is None
        assert connected._login_email is None


# ==========================================================================
# Health Check
# ==========================================================================


class TestHealthCheck:
    def test_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_success_reads_default_list(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"list": {"id": LIST_ID}})
        assert connected.health_check() is True
        assert mock_req.call_args[0][1] == f"{TATANGO_API_BASE}/lists/{LIST_ID}"

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_success_without_default_list(self, mock_req):
        c = TatangoConnector(min_request_interval=0)
        c._login_email = FAKE_EMAIL
        c._api_key = FAKE_API_KEY
        c._is_connected = True
        mock_req.return_value = _make_response(200, {"lists": []})
        assert c.health_check() is True
        assert mock_req.call_args[0][1] == f"{TATANGO_API_BASE}/lists"

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_failure(self, mock_req, connected):
        mock_req.side_effect = requests.ConnectionError("down")
        assert connected.health_check() is False


# ==========================================================================
# _request – HTTP layer
# ==========================================================================


class TestRequest:
    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_get_success_uses_basic_auth(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"ok": True})
        result = connected._request("GET", f"/lists/{LIST_ID}")
        assert result == {"ok": True}
        mock_req.assert_called_once_with(
            "GET",
            f"{TATANGO_API_BASE}/lists/{LIST_ID}",
            auth=(FAKE_EMAIL, FAKE_API_KEY),
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_auto_connect(self, mock_req, connector):
        mock_req.return_value = _make_response(200, {"ok": True})
        connector._request("GET", "/lists")
        assert connector.is_connected()

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_401_raises_authentication_error(self, mock_req, connected):
        mock_req.return_value = _make_response(401, text="bad key")
        with pytest.raises(AuthenticationError, match="authentication failed"):
            connected._request("GET", "/lists")

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_429_raises_rate_limit_error(self, mock_req, connected):
        mock_req.return_value = _make_response(429, headers={"Retry-After": "7"})
        with pytest.raises(RateLimitError) as exc_info:
            connected._request("GET", "/lists")
        assert exc_info.value.retry_after == 7

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_403_waf_block_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(403, text="<html>blocked</html>")
        with pytest.raises(ConnectionError, match="403"):
            connected._request("POST", f"/lists/{LIST_ID}/webhooks")

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_204_returns_none(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        assert connected._request("DELETE", "/lists/1/webhooks/9") is None

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_network_error_raises_connection_error(self, mock_req, connected):
        mock_req.side_effect = requests.Timeout("timeout")
        with pytest.raises(ConnectionError, match="request failed"):
            connected._request("GET", "/lists")

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_non_json_2xx_raises_connection_error(self, mock_req, connected):
        resp = _make_response(200, text="<html>not json</html>")
        resp.json.side_effect = ValueError("no json")
        mock_req.return_value = resp
        with pytest.raises(ConnectionError, match="non-JSON"):
            connected._request("GET", "/lists")


# ==========================================================================
# Pacing throttle
# ==========================================================================


class TestThrottle:
    @patch("ccef_connections.connectors.tatango.time.sleep")
    @patch("ccef_connections.connectors.tatango.time.monotonic")
    def test_first_request_does_not_sleep(self, mock_mono, mock_sleep, connected):
        connected._min_request_interval = 3.0
        mock_mono.return_value = 100.0
        connected._throttle()
        mock_sleep.assert_not_called()

    @patch("ccef_connections.connectors.tatango.time.sleep")
    @patch("ccef_connections.connectors.tatango.time.monotonic")
    def test_rapid_second_request_sleeps_remainder(
        self, mock_mono, mock_sleep, connected
    ):
        connected._min_request_interval = 3.0
        connected._last_request_at = 100.0
        mock_mono.return_value = 101.0  # 1s elapsed of a 3s floor
        connected._throttle()
        mock_sleep.assert_called_once_with(2.0)

    @patch("ccef_connections.connectors.tatango.time.sleep")
    @patch("ccef_connections.connectors.tatango.time.monotonic")
    def test_slow_second_request_does_not_sleep(
        self, mock_mono, mock_sleep, connected
    ):
        connected._min_request_interval = 3.0
        connected._last_request_at = 100.0
        mock_mono.return_value = 104.5
        connected._throttle()
        mock_sleep.assert_not_called()

    @patch("ccef_connections.connectors.tatango.time.sleep")
    def test_zero_interval_never_sleeps(self, mock_sleep, connected):
        connected._throttle()
        connected._throttle()
        mock_sleep.assert_not_called()


# ==========================================================================
# List id resolution
# ==========================================================================


class TestListResolution:
    def test_uses_default(self, connected):
        assert connected._resolve_list_id(None) == LIST_ID

    def test_per_call_overrides_default(self, connected):
        assert connected._resolve_list_id("999") == "999"

    def test_per_call_int_coerced(self, connected):
        assert connected._resolve_list_id(999) == "999"

    def test_no_list_raises_configuration_error(self):
        c = TatangoConnector(min_request_interval=0)
        with pytest.raises(ConfigurationError, match="list id"):
            c._resolve_list_id(None)


# ==========================================================================
# Subscribers
# ==========================================================================


class TestSubscribers:
    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_get_subscriber(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, {"subscriber": {"phone_number": "3125550123"}}
        )
        result = connected.get_subscriber("3125550123")
        assert result["subscriber"]["phone_number"] == "3125550123"
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/subscribers/3125550123"
        )

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_add_subscriber_minimal_no_bypass(self, mock_req, connected):
        mock_req.return_value = _make_response(201, {"status": "pending confirmation"})
        connected.add_subscriber("3125550123", first_name="Jane")
        body = mock_req.call_args[1]["json"]
        assert body == {
            "subscriber": {"phone_number": "3125550123", "first_name": "Jane"}
        }

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_add_subscriber_bypass_flags(self, mock_req, connected):
        mock_req.return_value = _make_response(201, {"status": "added"})
        connected.add_subscriber(
            "3125550123",
            bypass_opt_in_process=True,
            bypass_opt_in_response=True,
        )
        sub = mock_req.call_args[1]["json"]["subscriber"]
        assert sub["bypass_opt_in_process"] is True
        assert sub["bypass_opt_in_response"] is True

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_add_subscriber_welcome_text_combo(self, mock_req, connected):
        """bypass process but NOT response = instant subscribe + welcome text."""
        mock_req.return_value = _make_response(201, {"status": "added"})
        connected.add_subscriber(
            "3125550123",
            bypass_opt_in_process=True,
            bypass_opt_in_response=False,
        )
        sub = mock_req.call_args[1]["json"]["subscriber"]
        assert sub["bypass_opt_in_process"] is True
        assert sub["bypass_opt_in_response"] is False

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_add_subscriber_custom_fields_ride_flat(self, mock_req, connected):
        mock_req.return_value = _make_response(201, {"status": "added"})
        connected.add_subscriber(
            "3125550123",
            email="jane@example.org",
            zip_code="20005",
            custom_fields={"membership_status": "Active", "mrc_amount": "25"},
        )
        sub = mock_req.call_args[1]["json"]["subscriber"]
        assert sub["membership_status"] == "Active"
        assert sub["mrc_amount"] == "25"
        assert sub["email"] == "jane@example.org"
        assert sub["zip_code"] == "20005"
        assert "custom_fields" not in sub

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_add_subscriber_refused_add_returns_body(self, mock_req, connected):
        """A refused add is HTTP 201 — the connector must NOT raise; the
        caller reads the status string."""
        refused = {"status": "security timeout ... wait 47 hours and 55 minutes"}
        mock_req.return_value = _make_response(201, refused)
        result = connected.add_subscriber("3125550123")
        assert result == refused

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_update_subscriber_put_in_place(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"status": "updated"})
        connected.update_subscriber(
            "3125550123", {"membership_status": "Lapsed", "mrc_date": "2026-07-15"}
        )
        assert mock_req.call_args[0][0] == "PUT"
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/subscribers/3125550123"
        )
        body = mock_req.call_args[1]["json"]
        assert body == {
            "subscriber": {
                "phone_number": "3125550123",
                "membership_status": "Lapsed",
                "mrc_date": "2026-07-15",
            }
        }

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_delete_subscriber(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, {"status": "successfully unsubscribed"}
        )
        result = connected.delete_subscriber("3125550123")
        assert result["status"] == "successfully unsubscribed"
        assert mock_req.call_args[0][0] == "DELETE"
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/subscribers/3125550123"
        )

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_list_subscribers_explicit_list_id(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"subscribers": []})
        connected.list_subscribers(list_id="777", page=2)
        assert mock_req.call_args[0][1] == f"{TATANGO_API_BASE}/lists/777/subscribers"
        assert mock_req.call_args[1]["params"] == {"page": 2}


# ==========================================================================
# Custom fields
# ==========================================================================


class TestCustomFields:
    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_list_custom_fields(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"custom_fields": []})
        connected.list_custom_fields()
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/custom_fields"
        )

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_create_custom_field_plural_path_and_max_length(
        self, mock_req, connected
    ):
        mock_req.return_value = _make_response(201, {"custom_field": {"id": 1}})
        connected.create_custom_field("mrc_date", "MRC Date", "datetime")
        # plural path — the documented singular path 404s
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/custom_fields"
        )
        field = mock_req.call_args[1]["json"]["custom_field"]
        assert field == {
            "key": "mrc_date",
            "label": "MRC Date",
            "content_type": "datetime",
            # required despite docs calling it optional (422 without it)
            "max_length": 100,
        }


# ==========================================================================
# Webhooks
# ==========================================================================


class TestWebhooks:
    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_list_webhooks(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"webhooks": []})
        connected.list_webhooks()
        assert (
            mock_req.call_args[0][1] == f"{TATANGO_API_BASE}/lists/{LIST_ID}/webhooks"
        )

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_create_webhook_defaults(self, mock_req, connected):
        mock_req.return_value = _make_response(201, {"webhook": {"id": 9}})
        connected.create_webhook("https://example.org/hook?secret=abc")
        body = mock_req.call_args[1]["json"]["webhook"]
        assert body == {
            "callback_url": "https://example.org/hook?secret=abc",
            "subscribe": True,
            "unsubscribe": True,
            "message_sent": False,
            "cleaned": True,
            "reply_received": False,
        }

    @patch("ccef_connections.connectors.tatango.requests.request")
    def test_delete_webhook(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        assert connected.delete_webhook("9") is None
        assert (
            mock_req.call_args[0][1]
            == f"{TATANGO_API_BASE}/lists/{LIST_ID}/webhooks/9"
        )
