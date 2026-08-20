"""Tests for the Protect the Vote (PTV) connector.

All fixture data here is fabricated. PTV's real payloads are volunteer PII
(names, emails, phone numbers), which never lands in this repo — see the PII
policy in CLAUDE.md.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.ptv import (
    PTV_API_BASE,
    PTV_DEFAULT_USERNAME,
    PTVConnector,
    _ENDPOINT_SHIFT_VOLUNTEERS,
    _ENDPOINT_STATE_SHIFTS,
    _ENDPOINT_USERS,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _make_response(status_code=200, text="", headers=None):
    """Create a mock requests.Response. PTV returns CSV text, not JSON."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    return resp


FAKE_API_KEY = "test-ptv-api-key"

# The API returns this JSON body instead of CSV when a state has no data.
NOT_FOUND_BODY = '{"errors":{"detail":"Not Found"}}'

SHIFT_VOLUNTEERS_CSV = (
    "shift_id,inserted_at,date,start_time,end_time,timezone,locations,county,"
    "first_name,last_name,phone_number,email,role,source\n"
    "1,2026-08-01T10:00:00Z,2026-11-03,08:00,12:00,America/New_York,Site A,"
    "Testshire,Ada,Fabricated,555-0100,volunteer1@example.org,Poll Monitor,web\n"
    "2,2026-08-02T11:00:00Z,2026-11-03,12:00,16:00,America/New_York,Site B,"
    "Testshire,Bo,Fabricated,555-0101,volunteer2@example.org,Hotline,web\n"
)

USERS_CSV = (
    "id,email,join_date,phone_number,first_name,last_name,county,zip_code,"
    "source_code,regional_admin,shifted,training,role\n"
    "101,volunteer1@example.org,2026-07-01,555-0100,Ada,Fabricated,Testshire,"
    "00001,web,false,true,true,Poll Monitor\n"
)

STATE_SHIFTS_CSV = (
    "id,date,start_time,end_time,locations_string,volunteers,filled\n"
    "1,2026-11-03,08:00,12:00,Site A,4,true\n"
    "2,2026-11-03,12:00,16:00,Site B,1,false\n"
)


@pytest.fixture
def connector():
    """A PTVConnector with a mocked credential manager."""
    with patch.object(PTVConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_ptv_api_key.return_value = FAKE_API_KEY
        c = PTVConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """A connector already holding an API key."""
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


# ── Initialization ─────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = PTVConnector()
        assert c._api_key is None
        assert not c.is_connected()

    def test_default_username(self):
        assert PTVConnector()._username == PTV_DEFAULT_USERNAME
        assert PTV_DEFAULT_USERNAME == "colab"

    def test_custom_username(self):
        assert PTVConnector(username="other")._username == "other"

    def test_endpoints_are_built_from_the_api_base(self):
        assert _ENDPOINT_SHIFT_VOLUNTEERS == f"{PTV_API_BASE}/shift_volunteers_csv"
        assert _ENDPOINT_USERS == f"{PTV_API_BASE}/users_csv"
        assert _ENDPOINT_STATE_SHIFTS == f"{PTV_API_BASE}/state_shifts_csv"

    def test_repr_disconnected(self):
        assert repr(PTVConnector()) == "<PTVConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<PTVConnector status=connected>"


# ── Connect / Disconnect ───────────────────────────────────────────────


class TestConnect:
    def test_connect_loads_api_key(self, connector):
        connector.connect()

        assert connector.is_connected()
        assert connector._api_key == FAKE_API_KEY
        connector._credential_manager.get_ptv_api_key.assert_called_once()

    def test_missing_credential_raises_credential_error(self, connector):
        """CredentialError is re-raised as-is, matching the house pattern.

        A missing credential is not a connection failure. It used to be wrapped
        in ConnectionError, which told the caller to check the network when the
        fix is to set PTV_API_KEY_PASSWORD.
        """
        connector._credential_manager.get_ptv_api_key.side_effect = CredentialError(
            "PTV_API_KEY_PASSWORD not set"
        )

        with pytest.raises(CredentialError, match="PTV_API_KEY_PASSWORD not set"):
            connector.connect()

        assert not connector.is_connected()

    def test_credential_error_is_not_wrapped_in_connection_error(self, connector):
        """Regression guard: the two are sibling classes, not parent/child.

        CredentialError and ConnectionError both descend from
        CCEFConnectionError but neither subclasses the other, so wrapping meant
        `except CredentialError` could never fire for a missing PTV key.
        """
        connector._credential_manager.get_ptv_api_key.side_effect = CredentialError(
            "PTV_API_KEY_PASSWORD not set"
        )

        with pytest.raises(CredentialError) as exc_info:
            connector.connect()

        assert not isinstance(exc_info.value, ConnectionError)

    def test_unexpected_failure_still_becomes_connection_error(self, connector):
        """Anything that isn't a CredentialError is still wrapped."""
        connector._credential_manager.get_ptv_api_key.side_effect = RuntimeError(
            "something odd"
        )

        with pytest.raises(ConnectionError, match="Failed to connect to PTV") as exc_info:
            connector.connect()

        assert "something odd" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert not connector.is_connected()


class TestDisconnect:
    def test_disconnect_clears_the_key(self, connected_connector):
        connected_connector.disconnect()

        assert connected_connector._api_key is None
        assert not connected_connector.is_connected()

    def test_disconnect_is_idempotent(self, connector):
        connector.disconnect()
        connector.disconnect()

        assert not connector.is_connected()


class TestHealthCheck:
    def test_healthy_when_connected_with_key(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_unhealthy_when_disconnected(self, connector):
        assert connector.health_check() is False

    def test_unhealthy_when_key_is_none(self, connected_connector):
        connected_connector._api_key = None
        assert connected_connector.health_check() is False


# ── _fetch_csv ─────────────────────────────────────────────────────────


class TestFetchCsv:
    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_sends_key_as_param_and_basic_auth(self, mock_get, connected_connector):
        """PTV wants the key in BOTH the query string and as the basic-auth password."""
        mock_get.return_value = _make_response(200, text=STATE_SHIFTS_CSV)

        connected_connector._fetch_csv(_ENDPOINT_STATE_SHIFTS, "PA")

        args, kwargs = mock_get.call_args
        assert args[0] == _ENDPOINT_STATE_SHIFTS
        assert kwargs["params"] == {"key": FAKE_API_KEY, "state_code": "PA"}
        assert kwargs["auth"] == (PTV_DEFAULT_USERNAME, FAKE_API_KEY)
        assert kwargs["timeout"] == 60

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_returns_raw_csv_text(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=USERS_CSV)

        assert connected_connector._fetch_csv(_ENDPOINT_USERS, "PA") == USERS_CSV

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_auto_connects_when_not_connected(self, mock_get, connector):
        mock_get.return_value = _make_response(200, text=USERS_CSV)

        connector._fetch_csv(_ENDPOINT_USERS, "PA")

        assert connector.is_connected()
        connector._credential_manager.get_ptv_api_key.assert_called_once()

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_not_found_marker_returns_empty_string(self, mock_get, connected_connector):
        """A state with no data gets a JSON error body, not CSV.

        Returning "" keeps the contract that callers always get parseable CSV,
        so one dataless state doesn't abort a 50-state pull.
        """
        mock_get.return_value = _make_response(200, text=NOT_FOUND_BODY)

        assert connected_connector._fetch_csv(_ENDPOINT_USERS, "WY") == ""

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_401_raises_authentication_error(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(401, text="Unauthorized")

        with pytest.raises(AuthenticationError, match="PTV authentication failed"):
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_429_raises_rate_limit_error_with_retry_after(
        self, mock_get, connected_connector
    ):
        mock_get.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "45"}
        )

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")

        assert exc_info.value.retry_after == 45

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_429_without_header_defaults_retry_after_to_one(
        self, mock_get, connected_connector
    ):
        mock_get.return_value = _make_response(429, text="Too Many Requests")

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")

        assert exc_info.value.retry_after == 1

    @patch("ccef_connections.connectors.ptv.requests.get")
    @pytest.mark.parametrize("status_code", [400, 403, 404, 422, 500, 503])
    def test_other_error_statuses_raise_connection_error(
        self, mock_get, connected_connector, status_code
    ):
        mock_get.return_value = _make_response(status_code, text="boom")

        with pytest.raises(ConnectionError, match=str(status_code)):
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_transport_failure_raises_connection_error(
        self, mock_get, connected_connector
    ):
        mock_get.side_effect = requests.ConnectionError("dns went away")

        with pytest.raises(ConnectionError, match="PTV API request failed"):
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_timeout_raises_connection_error(self, mock_get, connected_connector):
        mock_get.side_effect = requests.Timeout("too slow")

        with pytest.raises(ConnectionError, match="PTV API request failed"):
            connected_connector._fetch_csv(_ENDPOINT_USERS, "PA")


# ── _parse_csv ─────────────────────────────────────────────────────────


class TestParseCsv:
    def test_parses_rows_into_dicts(self, connector):
        rows = connector._parse_csv(STATE_SHIFTS_CSV)

        assert len(rows) == 2
        assert rows[0]["id"] == "1"
        assert rows[0]["locations_string"] == "Site A"
        assert rows[1]["filled"] == "false"

    def test_empty_string_returns_empty_list(self, connector):
        assert connector._parse_csv("") == []

    def test_whitespace_only_returns_empty_list(self, connector):
        assert connector._parse_csv("   \n  \n") == []

    def test_header_only_returns_empty_list(self, connector):
        assert connector._parse_csv("id,date,filled\n") == []

    def test_all_values_are_strings(self, connector):
        """csv.DictReader does no type coercion — callers must cast."""
        rows = connector._parse_csv(STATE_SHIFTS_CSV)

        assert all(isinstance(v, str) for v in rows[0].values())


# ── Single-state fetches ───────────────────────────────────────────────


class TestGetShiftVolunteers:
    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_returns_parsed_rows(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=SHIFT_VOLUNTEERS_CSV)

        rows = connected_connector.get_shift_volunteers("PA")

        assert len(rows) == 2
        assert rows[0]["shift_id"] == "1"
        assert rows[0]["role"] == "Poll Monitor"

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_hits_the_shift_volunteers_endpoint(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=SHIFT_VOLUNTEERS_CSV)

        connected_connector.get_shift_volunteers("PA")

        assert mock_get.call_args.args[0] == _ENDPOINT_SHIFT_VOLUNTEERS

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_dataless_state_returns_empty_list(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=NOT_FOUND_BODY)

        assert connected_connector.get_shift_volunteers("WY") == []

    def test_has_retry_decorator(self):
        assert hasattr(PTVConnector.get_shift_volunteers, "retry")


class TestGetUsers:
    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_returns_parsed_rows(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=USERS_CSV)

        rows = connected_connector.get_users("PA")

        assert len(rows) == 1
        assert rows[0]["id"] == "101"
        assert rows[0]["regional_admin"] == "false"

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_hits_the_users_endpoint(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=USERS_CSV)

        connected_connector.get_users("PA")

        assert mock_get.call_args.args[0] == _ENDPOINT_USERS

    def test_has_retry_decorator(self):
        assert hasattr(PTVConnector.get_users, "retry")


class TestGetStateShifts:
    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_returns_parsed_rows(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=STATE_SHIFTS_CSV)

        rows = connected_connector.get_state_shifts("PA")

        assert len(rows) == 2
        assert rows[1]["volunteers"] == "1"

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_hits_the_state_shifts_endpoint(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=STATE_SHIFTS_CSV)

        connected_connector.get_state_shifts("PA")

        assert mock_get.call_args.args[0] == _ENDPOINT_STATE_SHIFTS

    def test_has_retry_decorator(self):
        assert hasattr(PTVConnector.get_state_shifts, "retry")


# ── Multi-state fetches ────────────────────────────────────────────────


class TestGetAllStates:
    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_adds_state_key_to_every_row(self, mock_get, connected_connector):
        """The 'state' key is the only way to tell rows apart once combined."""
        mock_get.return_value = _make_response(200, text=STATE_SHIFTS_CSV)

        rows = connected_connector.get_all_state_shifts(["PA", "GA"])

        assert len(rows) == 4
        assert [r["state"] for r in rows] == ["PA", "PA", "GA", "GA"]

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_queries_each_state_once(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=USERS_CSV)

        connected_connector.get_all_users(["PA", "GA", "AZ"])

        assert mock_get.call_count == 3
        requested = [c.kwargs["params"]["state_code"] for c in mock_get.call_args_list]
        assert requested == ["PA", "GA", "AZ"]

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_empty_state_list_makes_no_requests(self, mock_get, connected_connector):
        assert connected_connector.get_all_users([]) == []
        mock_get.assert_not_called()

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_dataless_states_contribute_nothing(self, mock_get, connected_connector):
        """One state with no data must not abort or pollute a multi-state pull."""
        mock_get.side_effect = [
            _make_response(200, text=USERS_CSV),
            _make_response(200, text=NOT_FOUND_BODY),
            _make_response(200, text=USERS_CSV),
        ]

        rows = connected_connector.get_all_users(["PA", "WY", "GA"])

        assert len(rows) == 2
        assert [r["state"] for r in rows] == ["PA", "GA"]

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_all_shift_volunteers_combines_states(self, mock_get, connected_connector):
        mock_get.return_value = _make_response(200, text=SHIFT_VOLUNTEERS_CSV)

        rows = connected_connector.get_all_shift_volunteers(["PA", "GA"])

        assert len(rows) == 4
        assert {r["state"] for r in rows} == {"PA", "GA"}

    @patch("ccef_connections.connectors.ptv.requests.get")
    def test_an_error_on_one_state_propagates(self, mock_get, connected_connector):
        """No partial-success swallowing — a hard failure surfaces.

        A silently-short pull would look like a state with low turnout.
        """
        mock_get.side_effect = [
            _make_response(200, text=USERS_CSV),
            _make_response(500, text="server error"),
        ]

        with pytest.raises(ConnectionError, match="500"):
            connected_connector.get_all_users(["PA", "GA"])

    def test_multi_state_wrappers_are_not_decorated(self):
        """Retry belongs on the single-state call, not the loop.

        Decorating the wrapper too would re-fetch every state already pulled
        when one late state rate-limits, and nest 5x5 attempts.
        """
        for name in (
            "get_all_shift_volunteers",
            "get_all_users",
            "get_all_state_shifts",
        ):
            assert not hasattr(getattr(PTVConnector, name), "retry"), name


# ── Retry behavior ─────────────────────────────────────────────────────


class TestRetry:
    """retry_ptv_operation retries 429 and nothing else.

    It previously also retried the library's ConnectionError, which wraps both
    a genuine transport failure AND any 4xx/5xx response — so a PTV 404 cost
    five attempts and ~30s of backoff before surfacing.
    """

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_retries_429_then_succeeds(
        self, mock_sleep, mock_get, connected_connector
    ):
        mock_get.side_effect = [
            _make_response(429, text="slow down", headers={"Retry-After": "1"}),
            _make_response(200, text=USERS_CSV),
        ]

        rows = connected_connector.get_users("PA")

        assert len(rows) == 1
        assert mock_get.call_count == 2

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_persistent_429_reraises_after_five_attempts(
        self, mock_sleep, mock_get, connected_connector
    ):
        mock_get.return_value = _make_response(
            429, text="slow down", headers={"Retry-After": "1"}
        )

        with pytest.raises(RateLimitError):
            connected_connector.get_users("PA")

        assert mock_get.call_count == 5

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_404(self, mock_sleep, mock_get, connected_connector):
        """A missing state will not appear on attempt 2."""
        mock_get.return_value = _make_response(404, text="Not Found")

        with pytest.raises(ConnectionError, match="404"):
            connected_connector.get_users("XX")

        assert mock_get.call_count == 1
        assert not mock_sleep.called

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_500(self, mock_sleep, mock_get, connected_connector):
        mock_get.return_value = _make_response(500, text="server error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector.get_users("PA")

        assert mock_get.call_count == 1

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_transport_failure(
        self, mock_sleep, mock_get, connected_connector
    ):
        """A transport failure is wrapped in ConnectionError, same as a 4xx.

        The connector cannot distinguish the two after wrapping, and the
        library-wide rule is 429-only, so this surfaces immediately. Matches
        the ten other narrowed decorators.
        """
        mock_get.side_effect = requests.ConnectionError("dns went away")

        with pytest.raises(ConnectionError, match="PTV API request failed"):
            connected_connector.get_users("PA")

        assert mock_get.call_count == 1
        assert not mock_sleep.called

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_401(self, mock_sleep, mock_get, connected_connector):
        """A bad API key is fixed in the credential, not by retrying."""
        mock_get.return_value = _make_response(401, text="Unauthorized")

        with pytest.raises(AuthenticationError):
            connected_connector.get_users("PA")

        assert mock_get.call_count == 1
        assert not mock_sleep.called

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_generic_exception(
        self, mock_sleep, mock_get, connected_connector
    ):
        """Regression guard for the bare-Exception predicate class of bug."""
        mock_get.side_effect = RuntimeError("bug in our code")

        with pytest.raises(RuntimeError, match="bug in our code"):
            connected_connector.get_users("PA")

        assert mock_get.call_count == 1
        assert not mock_sleep.called

    @patch("ccef_connections.connectors.ptv.requests.get")
    @patch("tenacity.nap.time.sleep")
    def test_missing_credential_is_not_retried(
        self, mock_sleep, mock_get, connector
    ):
        """A missing credential must fail fast, not five times.

        It reaches the caller through a decorated method, so this covers both
        halves of the fix: connect() now re-raises CredentialError instead of
        wrapping it, and the predicate wouldn't retry it either way.
        """
        connector._credential_manager.get_ptv_api_key.side_effect = CredentialError(
            "PTV_API_KEY_PASSWORD not set"
        )

        with pytest.raises(CredentialError, match="PTV_API_KEY_PASSWORD not set"):
            connector.get_users("PA")

        assert connector._credential_manager.get_ptv_api_key.call_count == 1
        mock_get.assert_not_called()
        assert not mock_sleep.called


# ── Context manager ────────────────────────────────────────────────────


class TestContextManager:
    def test_connects_and_disconnects(self, connector):
        with connector as conn:
            assert conn.is_connected()
            assert conn._api_key == FAKE_API_KEY

        assert not connector.is_connected()
        assert connector._api_key is None

    def test_disconnects_on_exception(self, connector):
        with pytest.raises(ValueError):
            with connector:
                raise ValueError("boom")

        assert not connector.is_connected()
