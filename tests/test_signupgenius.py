"""Tests for the SignUpGenius connector. All data is fabricated."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.signupgenius import (
    SIGNUPGENIUS_API_BASE,
    SignUpGeniusConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

REQ = "ccef_connections.connectors.signupgenius.requests.get"
FAKE_KEY = "test-sug-key"


def _resp(status_code=200, json_data=None, text="", headers=None, bad_json=False):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if bad_json:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_data if json_data is not None else {}
    return resp


def _ok(data):
    return _resp(200, {"success": True, "message": [], "data": data})


SIGNUPS = [
    {"signupid": 111, "title": "Example County Early Vote 2026", "group": "Example Group",
     "groupid": 9001, "signupurl": "https://www.signupgenius.com/go/ABC-111-example"},
    {"signupid": 222, "title": "Other County Early Vote 2026", "group": "Example Group",
     "groupid": 9002, "signupurl": "https://www.signupgenius.com/go/ABC-222-other"},
]

SLOTS = [
    {"slotitemid": 5001, "itemmemberid": 7001, "item": "Poll Monitors",
     "firstname": "Pat", "lastname": "Example", "email": "pat@example.org",
     "startdate": 1791000000, "enddate": 1791009000, "offset": "GMT-4:00",
     "myqty": 1, "status": "", "waitlist": 0},
]


@pytest.fixture
def connector():
    c = SignUpGeniusConnector()
    c._credential_manager = MagicMock()
    c._credential_manager.get_signupgenius_key.return_value = FAKE_KEY
    return c


class TestConnect:
    def test_initial_state(self):
        c = SignUpGeniusConnector()
        assert c._api_key is None
        assert not c.is_connected()

    def test_connect_loads_key(self, connector):
        connector.connect()
        assert connector._api_key == FAKE_KEY
        assert connector.health_check()

    def test_missing_credential_reraised_as_is(self, connector):
        connector._credential_manager.get_signupgenius_key.side_effect = CredentialError("missing")
        with pytest.raises(CredentialError):
            connector.connect()

    def test_other_failure_wrapped(self, connector):
        connector._credential_manager.get_signupgenius_key.side_effect = RuntimeError("boom")
        with pytest.raises(ConnectionError, match="Failed to connect to SignUpGenius"):
            connector.connect()

    def test_disconnect(self, connector):
        connector.connect()
        connector.disconnect()
        assert connector._api_key is None
        assert not connector.health_check()


class TestRequest:
    @patch(REQ)
    def test_fresh_connector_sends_key(self, mock_get, connector):
        # Never connect()-ed: the key must still go out (Geocodio regression).
        mock_get.return_value = _ok({"memberid": 1})
        connector.get_profile()
        _, kwargs = mock_get.call_args
        assert kwargs["params"] == {"user_key": FAKE_KEY}
        assert mock_get.call_args[0][0] == f"{SIGNUPGENIUS_API_BASE}/user/profile/"

    @patch(REQ)
    def test_bad_key_403_text_body(self, mock_get, connector):
        mock_get.return_value = _resp(403, text="Authentication failed", bad_json=True)
        with pytest.raises(AuthenticationError, match="403"):
            connector.get_profile()
        assert mock_get.call_count == 1

    @patch(REQ)
    def test_404_is_connection_error(self, mock_get, connector):
        mock_get.return_value = _resp(404, text="No Mapping Rule matched", bad_json=True)
        with pytest.raises(ConnectionError, match="404"):
            connector.get_profile()

    @patch(REQ)
    def test_success_false_access_denied_raises_not_empty(self, mock_get, connector):
        # Live behaviour: HTTP 200 with success:false. Must not become [].
        mock_get.return_value = _resp(
            200, {"success": False, "message": ["access denied"], "data": {}}
        )
        with pytest.raises(AuthenticationError, match="access denied"):
            connector.get_report(1, "filled")

    @patch(REQ)
    def test_success_false_other_message(self, mock_get, connector):
        mock_get.return_value = _resp(200, {"success": False, "message": ["weird"], "data": {}})
        with pytest.raises(ConnectionError, match="weird"):
            connector.list_signups()

    @patch(REQ)
    def test_non_json_200(self, mock_get, connector):
        mock_get.return_value = _resp(200, text="<html>", bad_json=True)
        with pytest.raises(ConnectionError, match="non-JSON"):
            connector.get_profile()

    @patch(REQ)
    def test_network_failure(self, mock_get, connector):
        mock_get.side_effect = requests.ConnectionError("down")
        with pytest.raises(ConnectionError, match="request failed"):
            connector.get_profile()

    @patch(REQ)
    @patch("tenacity.nap.time.sleep")
    def test_429_retried_then_raised(self, mock_sleep, mock_get, connector):
        mock_get.return_value = _resp(429, headers={"Retry-After": "7"})
        with pytest.raises(RateLimitError) as exc:
            connector.get_profile()
        assert exc.value.retry_after == 7
        assert mock_get.call_count == 4

    @patch(REQ)
    @patch("tenacity.nap.time.sleep")
    def test_auth_error_not_retried(self, mock_sleep, mock_get, connector):
        mock_get.return_value = _resp(403, text="Authentication failed")
        with pytest.raises(AuthenticationError):
            connector.list_signups()
        assert mock_get.call_count == 1


class TestSignups:
    @patch(REQ)
    def test_list_signups_default_active(self, mock_get, connector):
        mock_get.return_value = _ok(SIGNUPS)
        assert connector.list_signups() == SIGNUPS
        assert mock_get.call_args[0][0].endswith("/signups/created/active/")

    @pytest.mark.parametrize("status", ["active", "expired", "all"])
    @patch(REQ)
    def test_list_signups_statuses(self, mock_get, status, connector):
        mock_get.return_value = _ok([])
        assert connector.list_signups(status) == []
        assert mock_get.call_args[0][0].endswith(f"/signups/created/{status}/")

    def test_list_signups_bad_status(self, connector):
        with pytest.raises(ValueError):
            connector.list_signups("current")

    @patch(REQ)
    def test_null_data_is_empty_list(self, mock_get, connector):
        mock_get.return_value = _ok(None)
        assert connector.list_signups() == []


class TestReport:
    @pytest.mark.parametrize("report", ["filled", "available", "all"])
    @patch(REQ)
    def test_report_path_and_unwrap(self, mock_get, report, connector):
        mock_get.return_value = _ok({"signup": SLOTS})
        assert connector.get_report(111, report) == SLOTS
        assert mock_get.call_args[0][0].endswith(f"/signups/report/{report}/111/")

    @patch(REQ)
    def test_report_empty(self, mock_get, connector):
        mock_get.return_value = _ok({"signup": []})
        assert connector.get_report(111) == []

    def test_report_bad_type(self, connector):
        with pytest.raises(ValueError):
            connector.get_report(111, "unfilled")

    def test_retry_decorators_present(self):
        for m in ("get_profile", "list_signups", "get_report"):
            assert hasattr(getattr(SignUpGeniusConnector, m), "retry")


def test_top_level_export():
    import ccef_connections
    assert ccef_connections.SignUpGeniusConnector is SignUpGeniusConnector
    assert "SignUpGeniusConnector" in ccef_connections.__all__
