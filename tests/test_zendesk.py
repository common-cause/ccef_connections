"""Tests for the Zendesk connector."""

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.zendesk import (
    ZENDESK_DEFAULT_SCOPE,
    ZendeskConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Fixtures ──────────────────────────────────────────────────────────


SUBDOMAIN = "testsub"
FAKE_CREDS = {"client_id": "test-identifier", "client_secret": "test-secret"}

TOKEN_RESPONSE = {
    "access_token": "fake-token-abc",
    "expires_in": 1800,
    "token_type": "bearer",
    "scope": "read",
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
    """A ZendeskConnector with mocked credentials and throttling disabled."""
    c = ZendeskConnector(subdomain=SUBDOMAIN, max_requests_per_minute=0)
    mock_cm = MagicMock()
    mock_cm.get_zendesk_credentials.return_value = FAKE_CREDS
    c._credential_manager = mock_cm
    return c


@pytest.fixture
def connected_connector(connector):
    """A connector already 'connected' with a fake, unexpired token."""
    connector._access_token = "fake-token-abc"
    connector._token_expires_at = time.time() + 1800
    connector._is_connected = True
    return connector


# ── Initialization ────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = ZendeskConnector(subdomain=SUBDOMAIN)
        assert c._access_token is None
        assert c._token_expires_at == 0.0
        assert not c.is_connected()

    def test_defaults_to_read_scope(self):
        """Read-only by default -- writing requires a separate OAuth client."""
        assert ZendeskConnector(subdomain=SUBDOMAIN).scope == ZENDESK_DEFAULT_SCOPE
        assert ZENDESK_DEFAULT_SCOPE == "read"

    def test_urls_derive_from_subdomain(self, connector):
        assert connector.api_base == f"https://{SUBDOMAIN}.zendesk.com/api/v2"
        # The token endpoint is NOT under /api/v2.
        assert connector.token_url == f"https://{SUBDOMAIN}.zendesk.com/oauth/tokens"

    def test_missing_subdomain_raises(self, monkeypatch):
        monkeypatch.delenv("ZENDESK_SUBDOMAIN", raising=False)
        with pytest.raises(ConfigurationError, match="subdomain"):
            ZendeskConnector().api_base

    def test_subdomain_falls_back_to_env(self, monkeypatch):
        monkeypatch.setenv("ZENDESK_SUBDOMAIN", "fromenv")
        assert ZendeskConnector().api_base == "https://fromenv.zendesk.com/api/v2"

    def test_throttle_interval_from_rpm(self):
        c = ZendeskConnector(subdomain=SUBDOMAIN, max_requests_per_minute=120)
        assert c._min_interval == pytest.approx(0.5)


# ── Token management ──────────────────────────────────────────────────


class TestConnect:
    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_connect_success(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)
        connector.connect()

        assert connector.is_connected()
        assert connector._access_token == "fake-token-abc"

        # client_credentials grant, no refresh token involved.
        body = mock_post.call_args.kwargs["json"]
        assert body["grant_type"] == "client_credentials"
        assert body["client_id"] == "test-identifier"
        assert body["scope"] == "read"

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_expiry_uses_buffer(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)
        before = time.time()
        connector.connect()
        # 1800s lifetime minus the 60s early-refresh buffer.
        assert connector._token_expires_at == pytest.approx(before + 1740, abs=5)

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_invalid_client_hints_at_identifier_and_confidential(
        self, mock_post, connector
    ):
        """The two mistakes people actually make should be named in the error."""
        mock_post.return_value = _make_response(
            401, text='{"error":"invalid_client"}'
        )
        with pytest.raises(AuthenticationError) as exc:
            connector.connect()
        assert "Identifier" in str(exc.value)
        assert "Confidential" in str(exc.value)

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_invalid_scope_hints_at_ceiling(self, mock_post, connector):
        mock_post.return_value = _make_response(400, text='{"error":"invalid_scope"}')
        with pytest.raises(AuthenticationError, match="Allowed scopes"):
            connector.connect()

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_unreachable_token_endpoint(self, mock_post, connector):
        mock_post.side_effect = requests.RequestException("boom")
        with pytest.raises(ConnectionError, match="token endpoint"):
            connector.connect()

    def test_missing_credential_keys_raise(self):
        c = ZendeskConnector(subdomain=SUBDOMAIN)
        mock_cm = MagicMock()
        mock_cm.get_zendesk_credentials.side_effect = CredentialError("missing keys")
        c._credential_manager = mock_cm
        with pytest.raises(ConnectionError):
            c.connect()

    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()
        assert connected_connector._access_token is None
        assert connected_connector._token_expires_at == 0.0
        assert not connected_connector.is_connected()

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_expired_token_is_re_requested(self, mock_post, connected_connector):
        """No refresh token exists for client_credentials -- re-request instead."""
        mock_post.return_value = _make_response(
            200, {**TOKEN_RESPONSE, "access_token": "second-token"}
        )
        connected_connector._token_expires_at = time.time() - 1

        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer second-token"
        assert mock_post.call_count == 1


# ── Requests ──────────────────────────────────────────────────────────


class TestRequest:
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_bearer_auth_and_url(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ok": True})
        connected_connector._request("GET", "/groups.json")

        args, kwargs = mock_request.call_args
        assert args[0] == "GET"
        assert args[1] == f"https://{SUBDOMAIN}.zendesk.com/api/v2/groups.json"
        assert kwargs["headers"]["Authorization"] == "Bearer fake-token-abc"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_absolute_url_passed_through(self, mock_request, connected_connector):
        """Pagination links are absolute and must not be re-prefixed."""
        mock_request.return_value = _make_response(200, {"ok": True})
        url = "https://testsub.zendesk.com/api/v2/groups.json?page=2"
        connected_connector._request("GET", url)
        assert mock_request.call_args[0][1] == url

    @patch("ccef_connections.connectors.zendesk.requests.post")
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_401_re_requests_token_and_retries_once(
        self, mock_request, mock_post, connected_connector
    ):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)
        mock_request.side_effect = [
            _make_response(401),
            _make_response(200, {"ok": True}),
        ]

        result = connected_connector._request("GET", "/groups.json")

        assert result == {"ok": True}
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.zendesk.requests.post")
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_persistent_401_raises(self, mock_request, mock_post, connected_connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)
        mock_request.return_value = _make_response(401)
        with pytest.raises(AuthenticationError):
            connected_connector._request("GET", "/groups.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_429_uses_retry_after_header(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(429, headers={"Retry-After": "42"})
        with pytest.raises(RateLimitError) as exc:
            connected_connector._request("GET", "/groups.json")
        assert exc.value.retry_after == 42

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_429_without_header_defaults(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(429)
        with pytest.raises(RateLimitError) as exc:
            connected_connector._request("GET", "/groups.json")
        assert exc.value.retry_after == 10

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_204_returns_none(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)
        assert connected_connector._request("DELETE", "/x.json") is None

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_error_status_raises(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(422, text="unprocessable")
        with pytest.raises(ConnectionError, match="422"):
            connected_connector._request("GET", "/groups.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_rate_limit_headers_captured(self, mock_request, connected_connector):
        """Both the standard and legacy header spellings are recorded."""
        mock_request.return_value = _make_response(
            200,
            {"ok": True},
            headers={
                "ratelimit-limit": "400",
                "ratelimit-remaining": "399",
                "x-rate-limit": "400",
            },
        )
        connected_connector._request("GET", "/groups.json")

        status = connected_connector.rate_limit_status
        assert status["ratelimit-limit"] == "400"
        assert status["ratelimit-remaining"] == "399"
        assert status["x-rate-limit"] == "400"


class TestThrottle:
    @patch("ccef_connections.connectors.zendesk.time.sleep")
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_throttle_sleeps_between_requests(
        self, mock_request, mock_sleep, connected_connector
    ):
        mock_request.return_value = _make_response(200, {"ok": True})
        connected_connector._min_interval = 0.5
        connected_connector._last_request_at = time.time()

        connected_connector._request("GET", "/groups.json")

        mock_sleep.assert_called_once()
        assert 0 < mock_sleep.call_args[0][0] <= 0.5

    @patch("ccef_connections.connectors.zendesk.time.sleep")
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_no_throttle_when_disabled(
        self, mock_request, mock_sleep, connected_connector
    ):
        mock_request.return_value = _make_response(200, {"ok": True})
        connected_connector._request("GET", "/groups.json")
        mock_sleep.assert_not_called()


# ── Pagination ────────────────────────────────────────────────────────


class TestPagination:
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_cursor_pagination(self, mock_request, connected_connector):
        """Cursor style: meta.has_more plus links.next."""
        page2 = "https://testsub.zendesk.com/api/v2/groups.json?page[after]=xyz"
        mock_request.side_effect = [
            _make_response(
                200,
                {
                    "groups": [{"id": 1}],
                    "meta": {"has_more": True, "after_cursor": "xyz"},
                    "links": {"next": page2},
                },
            ),
            _make_response(
                200, {"groups": [{"id": 2}], "meta": {"has_more": False}, "links": {}}
            ),
        ]

        result = connected_connector._paginate("/groups.json", resource_key="groups")

        assert result == [{"id": 1}, {"id": 2}]
        assert mock_request.call_args_list[1][0][1] == page2

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_offset_pagination(self, mock_request, connected_connector):
        """Offset style: next_page. Some config endpoints still use this."""
        page2 = "https://testsub.zendesk.com/api/v2/triggers.json?page=2"
        mock_request.side_effect = [
            _make_response(200, {"triggers": [{"id": 1}], "next_page": page2}),
            _make_response(200, {"triggers": [{"id": 2}], "next_page": None}),
        ]

        result = connected_connector._paginate("/triggers.json", resource_key="triggers")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_single_page(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"groups": [{"id": 1}]})
        assert connected_connector._paginate("/groups.json", resource_key="groups") == [
            {"id": 1}
        ]
        assert mock_request.call_count == 1

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_resource_key_inferred(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"views": [{"id": 7}], "count": 1})
        assert connected_connector._paginate("/views.json") == [{"id": 7}]

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_params_only_sent_on_first_page(self, mock_request, connected_connector):
        """Subsequent pages come from absolute links that already carry params."""
        page2 = "https://testsub.zendesk.com/api/v2/users.json?page=2"
        mock_request.side_effect = [
            _make_response(200, {"users": [{"id": 1}], "next_page": page2}),
            _make_response(200, {"users": [{"id": 2}], "next_page": None}),
        ]

        connected_connector._paginate(
            "/users.json", params={"role": "agent"}, resource_key="users"
        )

        assert mock_request.call_args_list[0][1]["params"] == {"role": "agent"}
        assert mock_request.call_args_list[1][1]["params"] is None

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_page_size_not_injected(self, mock_request, connected_connector):
        """Not every endpoint accepts page[size]; sending it can 400."""
        mock_request.return_value = _make_response(200, {"groups": []})
        connected_connector._paginate("/groups.json", resource_key="groups")
        assert mock_request.call_args[1]["params"] is None


# ── Read surface ──────────────────────────────────────────────────────


class TestReads:
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_me_unwraps_user(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"user": {"id": 1, "role": "admin"}}
        )
        assert connected_connector.get_me() == {"id": 1, "role": "admin"}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_account_settings_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"settings": {"tickets": {}}})
        assert connected_connector.get_account_settings() == {"tickets": {}}

    @pytest.mark.parametrize(
        "method,path,key",
        [
            ("list_groups", "/groups.json", "groups"),
            ("list_ticket_forms", "/ticket_forms.json", "ticket_forms"),
            ("list_ticket_fields", "/ticket_fields.json", "ticket_fields"),
            ("list_views", "/views.json", "views"),
            ("list_triggers", "/triggers.json", "triggers"),
            ("list_automations", "/automations.json", "automations"),
            ("list_macros", "/macros.json", "macros"),
            ("list_sla_policies", "/slas/policies.json", "sla_policies"),
            ("list_brands", "/brands.json", "brands"),
            ("list_custom_roles", "/custom_roles.json", "custom_roles"),
            ("list_organizations", "/organizations.json", "organizations"),
        ],
    )
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_config_readers_hit_expected_endpoint(
        self, mock_request, connected_connector, method, path, key
    ):
        mock_request.return_value = _make_response(200, {key: [{"id": 1}]})
        assert getattr(connected_connector, method)() == [{"id": 1}]
        assert mock_request.call_args[0][1].endswith(path)

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_custom_roles_empty_is_not_an_error(self, mock_request, connected_connector):
        """Suite Growth lacks custom agent roles -- empty is the expected answer."""
        mock_request.return_value = _make_response(200, {"custom_roles": []})
        assert connected_connector.list_custom_roles() == []

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_list_users_passes_role_filter(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"users": []})
        connected_connector.list_users(role="agent")
        assert mock_request.call_args[1]["params"] == {"role": "agent"}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_list_agents_covers_admins_too(self, mock_request, connected_connector):
        """Admins consume agent seats but report role 'admin'."""
        mock_request.side_effect = [
            _make_response(200, {"users": [{"id": 1, "role": "agent"}]}),
            _make_response(200, {"users": [{"id": 2, "role": "admin"}]}),
        ]
        result = connected_connector.list_agents()
        assert [u["id"] for u in result] == [1, 2]

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_ticket_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket": {"id": 5}})
        assert connected_connector.get_ticket(5) == {"id": 5}
        assert mock_request.call_args[0][1].endswith("/tickets/5.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_list_group_tickets(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"tickets": [{"id": 9}]})
        assert connected_connector.list_group_tickets(3) == [{"id": 9}]
        assert mock_request.call_args[0][1].endswith("/groups/3/tickets.json")


# ── Write surface ─────────────────────────────────────────────────────


class TestWrites:
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_ticket(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket": {"id": 11}})
        result = connected_connector.create_ticket({"subject": "hi"})

        assert result == {"id": 11}
        assert mock_request.call_args[1]["json"] == {"ticket": {"subject": "hi"}}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_ticket(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket": {"id": 11}})
        connected_connector.update_ticket(11, {"status": "solved"})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[1]["json"] == {"ticket": {"status": "solved"}}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_many_returns_job_status(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"job_status": {"id": "abc", "status": "queued"}}
        )
        result = connected_connector.create_many_tickets([{"subject": "a"}])

        assert result["id"] == "abc"
        assert mock_request.call_args[0][1].endswith("/tickets/create_many.json")

    def test_create_many_rejects_over_100(self, connected_connector):
        with pytest.raises(ValueError, match="100"):
            connected_connector.create_many_tickets([{"subject": "x"}] * 101)

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_job_status(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"job_status": {"id": "abc", "status": "completed"}}
        )
        assert connected_connector.get_job_status("abc")["status"] == "completed"

    def test_no_config_mutators_exposed(self, connector):
        """Config-object writes belong with the reviewed config-as-code script.

        Guards against someone adding trigger/view/group mutation here, where it
        would run against IT's shared instance without the namespacing and
        non-destructive guarantees that plan carries.
        """
        forbidden = [
            name
            for name in dir(connector)
            if not name.startswith("_")
            and any(name.startswith(v) for v in ("create_", "update_", "delete_"))
            and not name.endswith(("ticket", "tickets"))
        ]
        assert forbidden == []


# ── Health check ──────────────────────────────────────────────────────


class TestHealthCheck:
    def test_false_when_disconnected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_true_when_token_valid(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"user": {"id": 1}})
        assert connected_connector.health_check() is True

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_false_on_error(self, mock_request, connected_connector):
        mock_request.side_effect = requests.RequestException("boom")
        assert connected_connector.health_check() is False


# ── Registration ──────────────────────────────────────────────────────


class TestRegistration:
    def test_lazy_export(self):
        """Importable from the connectors package (PEP 562 lazy import)."""
        from ccef_connections import connectors

        assert connectors.ZendeskConnector is ZendeskConnector
        assert "ZendeskConnector" in connectors.__all__
