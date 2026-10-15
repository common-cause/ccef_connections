"""Tests for the CallHub connector."""

import csv
import io
import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.callhub import (
    CALLHUB_DEFAULT_DOMAIN,
    CallHubConnector,
    callhub_id,
)
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

FAKE_KEY = "0123456789abcdef0123456789abcdef01234567"
DOMAIN = "https://api-na1.callhub.io"
PB_ID = "4008573011220235837"


def _resp(status=200, json_data=None, text="", headers=None, content=b"x"):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.text = text or (json.dumps(json_data) if json_data is not None else "")
    r.headers = headers or {}
    r.content = content if status != 204 else b""
    if json_data is None:
        r.json.side_effect = ValueError("no json")
    else:
        r.json.return_value = json_data
    return r


@pytest.fixture
def connected():
    """A connected CallHubConnector with no pacing and a mocked session."""
    with patch.object(CallHubConnector, "_credential_manager", create=True) as cm:
        cm.get_callhub_credentials.return_value = {"api_key": FAKE_KEY, "api_domain": DOMAIN}
        c = CallHubConnector(read_interval=0, write_interval=0, bulk_interval=0, bulk_per_hour=0)
        c._credential_manager = cm
        c.connect()
        c._session = MagicMock()
        yield c


def _calls(c):
    return c._session.request.call_args_list


# ==========================================================================
# Credentials
# ==========================================================================


class TestCredentials:
    @pytest.fixture(autouse=True)
    def _fresh_cache(self):
        CredentialManager()._credentials_cache.clear()
        yield
        CredentialManager()._credentials_cache.clear()

    def test_bare_key(self, monkeypatch):
        monkeypatch.setenv("CALLHUB_API_KEY_PASSWORD", FAKE_KEY)
        assert CredentialManager().get_callhub_credentials() == {
            "api_key": FAKE_KEY, "api_domain": None,
        }

    def test_json_key(self, monkeypatch):
        monkeypatch.setenv(
            "CALLHUB_API_KEY_PASSWORD",
            json.dumps({"api_key": FAKE_KEY, "api_domain": DOMAIN + "/"}),
        )
        creds = CredentialManager().get_callhub_credentials()
        assert creds["api_key"] == FAKE_KEY
        assert creds["api_domain"] == DOMAIN + "/"

    def test_json_without_key_raises(self, monkeypatch):
        monkeypatch.setenv("CALLHUB_API_KEY_PASSWORD", json.dumps({"api_domain": DOMAIN}))
        with pytest.raises(CredentialError):
            CredentialManager().get_callhub_credentials()


# ==========================================================================
# Connect
# ==========================================================================


class TestConnect:
    def _make(self, creds, **kw):
        with patch.object(CallHubConnector, "_credential_manager", create=True) as cm:
            cm.get_callhub_credentials.return_value = creds
            c = CallHubConnector(**kw)
            c._credential_manager = cm
            c.connect()
            return c

    def test_domain_from_credential_trailing_slash_stripped(self):
        c = self._make({"api_key": FAKE_KEY, "api_domain": DOMAIN + "/"})
        assert c._base == DOMAIN
        assert c._session.headers["Authorization"] == f"Token {FAKE_KEY}"
        assert c.is_connected()

    def test_missing_domain_falls_back_with_warning(self, caplog):
        c = self._make({"api_key": FAKE_KEY, "api_domain": None})
        assert c._base == CALLHUB_DEFAULT_DOMAIN
        assert "api_domain" in caplog.text

    def test_constructor_domain_wins(self):
        c = self._make({"api_key": FAKE_KEY, "api_domain": DOMAIN}, api_domain="api-app.callhub.io")
        assert c._base == "https://api-app.callhub.io"

    def test_deprecated_host_refused(self):
        with pytest.raises(ConfigurationError):
            self._make({"api_key": FAKE_KEY, "api_domain": "https://api.callhub.io/"})

    def test_missing_credential_reraised(self):
        with patch.object(CallHubConnector, "_credential_manager", create=True) as cm:
            cm.get_callhub_credentials.side_effect = CredentialError("missing")
            c = CallHubConnector()
            c._credential_manager = cm
            with pytest.raises(CredentialError):
                c.connect()


# ==========================================================================
# _request error mapping
# ==========================================================================


class TestRequest:
    def test_trailing_slash_added_and_host_rebased(self, connected):
        connected._session.request.return_value = _resp(200, {"ok": 1})
        connected._request("GET", "https://api.callhub.io/v1/phonebooks?page=2")
        args = _calls(connected)[0][0]
        assert args == ("GET", f"{DOMAIN}/v1/phonebooks/?page=2")

    def test_403_invalid_token_is_auth_error(self, connected):
        connected._session.request.return_value = _resp(403, {"detail": "Invalid token."})
        with pytest.raises(AuthenticationError):
            connected._request("GET", "/v1/users/")

    def test_403_other_is_connection_error(self, connected):
        connected._session.request.return_value = _resp(
            403, {"error_message": "Endpoint is deprecated"}
        )
        with pytest.raises(ConnectionError, match="deprecated"):
            connected._request("GET", "/v1/users/")

    def test_429_header_retry_after(self, connected):
        connected._session.request.return_value = _resp(
            429, {"detail": "Request was throttled. Expected available in 60 seconds."},
            headers={"Retry-After": "60"},
        )
        with pytest.raises(RateLimitError) as ei:
            connected._request("GET", "/v1/users/")
        assert ei.value.retry_after == 60

    def test_429_body_retry_after(self, connected):
        connected._session.request.return_value = _resp(
            429, {"detail": "Request was throttled. Expected available in 3540 seconds."}
        )
        with pytest.raises(RateLimitError) as ei:
            connected._request("GET", "/v1/users/")
        assert ei.value.retry_after == 3540

    def test_400_carries_detail(self, connected):
        connected._session.request.return_value = _resp(400, {"detail": "low_credit"})
        with pytest.raises(ConnectionError, match="low_credit"):
            connected._request("POST", "/v1/phonebooks/", json_body={})

    def test_204_returns_none(self, connected):
        connected._session.request.return_value = _resp(204)
        assert connected._request("DELETE", f"/v1/phonebooks/{PB_ID}/") is None

    def test_network_error_wrapped(self, connected):
        connected._session.request.side_effect = requests.ConnectionError("boom")
        with pytest.raises(ConnectionError):
            connected._request("GET", "/v1/users/")

    @patch("time.sleep")
    def test_decorated_method_retries_429_then_succeeds(self, _sleep, connected):
        connected._session.request.side_effect = [
            _resp(429, {"detail": "throttled"}, headers={"Retry-After": "1"}),
            _resp(201, {"id": 1, "pk_str": "1", "name": "x"}),
        ]
        assert connected.create_phonebook("x")["pk_str"] == "1"
        assert len(_calls(connected)) == 2

    def test_decorated_method_does_not_retry_500(self, connected):
        connected._session.request.return_value = _resp(500, text="oops")
        with pytest.raises(ConnectionError):
            connected.create_phonebook("x")
        assert len(_calls(connected)) == 1


# ==========================================================================
# Pacing
# ==========================================================================


class TestPacing:
    @patch("ccef_connections.connectors.callhub.time.sleep")
    @patch("ccef_connections.connectors.callhub.time.monotonic")
    def test_rapid_reads_sleep_remainder(self, mono, sleep, connected):
        connected._intervals["read"] = 0.5
        mono.side_effect = [100.0, 100.0, 100.2, 100.5]
        connected._pace("read")
        connected._pace("read")
        sleep.assert_called_once()
        assert sleep.call_args[0][0] == pytest.approx(0.3)

    @patch("ccef_connections.connectors.callhub.time.sleep")
    @patch("ccef_connections.connectors.callhub.time.monotonic")
    def test_bulk_hourly_window(self, mono, sleep, connected):
        connected._bulk_per_hour = 2
        connected._bulk_history.extend([10.0, 20.0])
        mono.side_effect = [30.0, 3611.0, 3611.0]
        connected._pace("bulk")
        assert sleep.call_args[0][0] == pytest.approx(3600 - 20 + 1)


# ==========================================================================
# Pagination / ids
# ==========================================================================


class TestPaginate:
    def test_follows_next_rebased(self, connected):
        connected._session.request.side_effect = [
            _resp(200, {"count": 3, "next": "https://other.callhub.io/v1/phonebooks/?page=2",
                        "results": [{"id": 1}, {"id": 2}]}),
            _resp(200, {"count": 3, "next": None, "results": [{"id": 3}]}),
        ]
        assert [p["id"] for p in connected.list_phonebooks()] == [1, 2, 3]
        assert _calls(connected)[1][0][1] == f"{DOMAIN}/v1/phonebooks/?page=2"
        assert _calls(connected)[0][1]["params"]["page_size"] == 1000

    def test_bare_list(self, connected):
        connected._session.request.return_value = _resp(200, [{"id": 30178, "name": "rank"}])
        assert connected.list_custom_fields()[0]["name"] == "rank"

    def test_limit(self, connected):
        connected._session.request.return_value = _resp(
            200, {"next": "x?page=2", "results": [{"id": i} for i in range(5)]}
        )
        assert len(connected.list_phonebooks(limit=2)) == 2
        assert len(_calls(connected)) == 1

    def test_callhub_id(self):
        assert callhub_id({"pk_str": "4008573011220235837", "id": 4008573011220235837}) == "4008573011220235837"
        assert callhub_id({"id": 19}) == "19"
        assert callhub_id({"url": "https://api-na1.callhub.io/v1/dnc_lists/387/"}) == "387"
        with pytest.raises(ValueError):
            callhub_id({})


# ==========================================================================
# Phonebooks / fields
# ==========================================================================


class TestPhonebooks:
    def test_create(self, connected):
        connected._session.request.return_value = _resp(201, {"pk_str": PB_ID, "name": "W1"})
        connected.create_phonebook("W1", "desc")
        args, kw = _calls(connected)[0]
        assert args == ("POST", f"{DOMAIN}/v1/phonebooks/")
        assert kw["json"] == {"name": "W1", "description": "desc"}

    def test_find_phonebook(self, connected):
        connected._session.request.return_value = _resp(
            200, {"next": None, "results": [{"name": "A"}, {"name": "B"}]}
        )
        assert connected.find_phonebook("B") == {"name": "B"}
        assert connected.find_phonebook("C") is None

    def test_find_phonebook_ambiguous(self, connected):
        connected._session.request.return_value = _resp(
            200, {"next": None, "results": [{"name": "A"}, {"name": "A"}]}
        )
        with pytest.raises(ConfigurationError):
            connected.find_phonebook("A")

    def test_count(self, connected):
        connected._session.request.return_value = _resp(
            200, {"phonenumber_count": 2, "mobilenumber_count": 2}
        )
        assert connected.phonebook_count(PB_ID) == {"phonenumber_count": 2, "mobilenumber_count": 2}
        assert _calls(connected)[0][0][1].endswith(f"/v1/phonebooks/{PB_ID}/numbers_count/")

    def test_field_id_map(self, connected):
        connected._session.request.return_value = _resp(200, {"count": 3, "results": [
            {"id": 0, "name": "Calling Number*"}, {"id": 16, "name": "Tags"},
            {"id": 19, "name": "List_Rank"},
        ]})
        assert connected.field_id_map() == {"calling number*": "0", "tags": "16", "list_rank": "19"}

    def test_create_custom_field_multichoice(self, connected):
        connected._session.request.return_value = _resp(201, {"id": 1})
        connected.create_custom_field("tier", "Multi-choice", ["A", "B"])
        assert _calls(connected)[0][1]["json"] == {
            "name": "tier", "field_type": "Multi-choice", "choice": ["A", "B"],
        }


# ==========================================================================
# bulk import
# ==========================================================================


class TestBulkCreate:
    def test_rows_get_header_and_string_mapping(self, connected):
        connected._session.request.return_value = _resp(200, {"message": "ok", "job_id": "j"})
        out = connected.bulk_create_contacts(
            PB_ID, header=["phone", "first"], rows=[["4045550123", "Ada"], ["4045550124", None]],
            mapping={0: 0, "1": 0, "3": 1},
        )
        assert out["job_id"] == "j"
        args, kw = _calls(connected)[0]
        assert args == ("POST", f"{DOMAIN}/v1/contacts/bulk_create/")
        assert json.loads(kw["data"]["mapping"]) == {"0": "0", "1": "0", "3": "1"}
        assert isinstance(kw["data"]["mapping"], str)
        assert kw["data"]["phonebook_id"] == PB_ID
        assert kw["data"]["country_iso"] == "US" and kw["data"]["country_choice"] == "custom"
        name, payload, ctype = kw["files"]["contacts_csv"]
        assert " " not in name and ctype == "text/csv"
        rows = list(csv.reader(io.StringIO(payload.decode())))
        assert rows == [["phone", "first"], ["4045550123", "Ada"], ["4045550124", ""]]

    def test_rows_without_header_refused(self, connected):
        with pytest.raises(ConfigurationError, match="header"):
            connected.bulk_create_contacts(PB_ID, rows=[["1"]], mapping={"0": 0})

    def test_needs_exactly_one_source(self, connected):
        with pytest.raises(ConfigurationError):
            connected.bulk_create_contacts(PB_ID, mapping={"0": 0})
        with pytest.raises(ConfigurationError):
            connected.bulk_create_contacts(PB_ID, mapping={"0": 0}, rows=[], header=["p"], csv_bytes=b"p\n")

    def test_needs_phone_mapping(self, connected):
        with pytest.raises(ConfigurationError, match="phone"):
            connected.bulk_create_contacts(PB_ID, mapping={"3": 1}, csv_bytes=b"h\n")

    def test_uses_bulk_bucket(self, connected):
        connected._session.request.return_value = _resp(200, {"message": "ok"})
        with patch.object(connected, "_pace") as pace:
            connected.bulk_create_contacts(PB_ID, mapping={"0": 0}, csv_bytes=b"p\n1\n")
        pace.assert_called_once_with("bulk")


class TestWaitForImport:
    @patch("ccef_connections.connectors.callhub.time.sleep")
    def test_returns_when_expected_reached(self, _sleep, connected):
        with patch.object(connected, "phonebook_count", side_effect=[
            {"phonenumber_count": 0}, {"phonenumber_count": 5000}, {"phonenumber_count": 7000},
        ]):
            assert connected.wait_for_import(PB_ID, expected=7000) == 7000

    @patch("ccef_connections.connectors.callhub.time.sleep")
    @patch("ccef_connections.connectors.callhub.time.monotonic")
    def test_returns_on_stall(self, mono, _sleep, connected):
        mono.side_effect = [0, 0, 10, 400]
        with patch.object(connected, "phonebook_count", side_effect=[
            {"phonenumber_count": 98}, {"phonenumber_count": 98}, {"phonenumber_count": 98},
        ]):
            assert connected.wait_for_import(PB_ID, expected=100, stall_after=300) == 98

    @patch("ccef_connections.connectors.callhub.time.sleep")
    @patch("ccef_connections.connectors.callhub.time.monotonic")
    def test_timeout_when_nothing_lands(self, mono, _sleep, connected):
        mono.side_effect = [0, 0, 2000]
        with patch.object(connected, "phonebook_count", return_value={"phonenumber_count": 0}):
            with pytest.raises(ConnectionError, match="Timed out"):
                connected.wait_for_import(PB_ID, expected=10, timeout=1800)


# ==========================================================================
# DNC / read-only listings
# ==========================================================================


class TestReads:
    def test_dnc_phone_set_normalises(self, connected):
        connected._session.request.return_value = _resp(200, {"next": None, "results": [
            {"phone_number": "15555550123"}, {"phone_number": "+1 (555) 555-0124"},
            {"phone_number": "5555550125"}, {"phone_number": "44207"},
        ]})
        assert connected.dnc_phone_set() == {"5555550123", "5555550124", "5555550125"}

    def test_agents_pending_param(self, connected):
        connected._session.request.return_value = _resp(200, {"next": None, "results": []})
        connected.list_agents(include_pending=True)
        assert _calls(connected)[0][1]["params"]["include_pending"] == "true"

    def test_no_campaign_mutators(self):
        public = {n for n in dir(CallHubConnector) if not n.startswith("_")}
        assert not {n for n in public if "campaign" in n and not n.startswith("list_")}
