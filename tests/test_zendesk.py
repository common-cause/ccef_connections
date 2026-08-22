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
        """CredentialError is re-raised as-is, not wrapped in ConnectionError."""
        c = ZendeskConnector(subdomain=SUBDOMAIN)
        mock_cm = MagicMock()
        mock_cm.get_zendesk_credentials.side_effect = CredentialError("missing keys")
        c._credential_manager = mock_cm
        with pytest.raises(CredentialError, match="missing keys") as exc_info:
            c.connect()

        assert not isinstance(exc_info.value, ConnectionError)

    def test_unexpected_failure_still_wraps(self):
        c = ZendeskConnector(subdomain=SUBDOMAIN)
        mock_cm = MagicMock()
        mock_cm.get_zendesk_credentials.side_effect = RuntimeError("something odd")
        c._credential_manager = mock_cm
        with pytest.raises(ConnectionError, match="Failed to connect to Zendesk"):
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

    def test_write_surface_is_an_explicit_allowlist(self, connector):
        """The set of mutators is pinned, so adding one takes a deliberate edit.

        Single-object config writes are supported (they back reviewed
        config-as-code), but this connector runs against IT's SHARED instance.
        Widening the write surface -- especially to anything bulk or
        reconciling -- must be a conscious change to this list, not a drive-by.
        """
        allowed = {
            # tickets
            "create_ticket",
            "update_ticket",
            "create_many_tickets",
            # config objects, one at a time, by explicit id
            "create_ticket_field",
            "update_ticket_field",
            "update_ticket_form",
            "create_trigger",
            "update_trigger",
            "create_trigger_category",
            "update_trigger_category",
            "create_view",
            "update_view",
            "create_sla_policy",
            "update_sla_policy",
            "create_macro",
            "update_macro",
            "create_help_center_article",
            "update_help_center_article",
            "update_help_center_article_translation",
        }
        actual = {
            name
            for name in dir(connector)
            if not name.startswith("_")
            and any(name.startswith(v) for v in ("create_", "update_", "delete_"))
        }
        assert actual == allowed

    def test_no_delete_methods_at_all(self, connector):
        """Nothing in this connector may destroy a config object or a ticket.

        Deletion on a shared instance is the one mistake with no cheap undo, so
        the capability simply is not exposed.
        """
        assert [n for n in dir(connector) if n.startswith("delete")] == []

    def test_no_bulk_config_mutators(self, connector):
        """No 'update everything' / reconcile helpers.

        An enumerate-config-and-write-it-back helper is what would let a bug
        clobber IT's production triggers, macros and views. Bulk writes are
        limited to tickets (create_many_tickets), which are our own objects.
        """
        bulk = [
            n
            for n in dir(connector)
            if not n.startswith("_")
            and any(k in n for k in ("_many", "_all", "sync_", "reconcile", "apply_"))
            and n != "create_many_tickets"
        ]
        assert bulk == []


# ── Config-object writes ──────────────────────────────────────────────


class TestConfigWrites:
    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_ticket_field(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket_field": {"id": 99}})
        field = {"type": "tagger", "title": "Campaigns - Request Type"}
        result = connected_connector.create_ticket_field(field)

        assert result == {"id": 99}
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith("/ticket_fields.json")
        assert mock_request.call_args[1]["json"] == {"ticket_field": field}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_ticket_field_targets_one_id(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket_field": {"id": 99}})
        connected_connector.update_ticket_field(99, {"required": False})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/ticket_fields/99.json")
        assert mock_request.call_args[1]["json"] == {"ticket_field": {"required": False}}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_ticket_field_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"ticket_field": {"id": 99, "title": "x"}}
        )
        assert connected_connector.get_ticket_field(99)["title"] == "x"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_ticket_form_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"ticket_form": {"id": 5, "name": "Campaigns"}}
        )
        assert connected_connector.get_ticket_form(5)["name"] == "Campaigns"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_ticket_form(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ticket_form": {"id": 5}})
        body = {"ticket_field_ids": [1, 2], "agent_conditions": []}
        connected_connector.update_ticket_form(5, body)

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/ticket_forms/5.json")
        assert mock_request.call_args[1]["json"] == {"ticket_form": body}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_trigger(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"trigger": {"id": 7}})
        result = connected_connector.create_trigger({"title": "Campaigns: set category"})

        assert result == {"id": 7}
        assert mock_request.call_args[0][1].endswith("/triggers.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_trigger(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"trigger": {"id": 7}})
        connected_connector.update_trigger(7, {"active": False})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/triggers/7.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_trigger_category(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"trigger_category": {"id": "12", "name": "Campaigns"}}
        )
        result = connected_connector.create_trigger_category(
            {"name": "Campaigns", "position": 7}
        )

        assert result["name"] == "Campaigns"
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith("/trigger_categories")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_trigger_category_uses_patch(self, mock_request, connected_connector):
        """The trigger-categories endpoint takes PATCH, not PUT, unlike the rest."""
        mock_request.return_value = _make_response(200, {"trigger_category": {"id": "12"}})
        connected_connector.update_trigger_category(12, {"position": 8})

        assert mock_request.call_args[0][0] == "PATCH"
        assert mock_request.call_args[0][1].endswith("/trigger_categories/12")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_view(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"view": {"id": 21}})
        body = {"title": "Campaigns: Open by due date", "active": True}
        result = connected_connector.create_view(body)

        assert result == {"id": 21}
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith("/views.json")
        assert mock_request.call_args[1]["json"] == {"view": body}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_view_targets_one_id(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"view": {"id": 21}})
        connected_connector.update_view(21, {"active": False})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/views/21.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_view_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"view": {"id": 21, "title": "Campaigns: Day-of"}}
        )
        assert connected_connector.get_view(21)["title"] == "Campaigns: Day-of"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_sla_policy(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"sla_policy": {"id": 31}})
        body = {
            "title": "Campaigns: request turnaround",
            "filter": {"all": [], "any": []},
            "policy_metrics": [],
        }
        result = connected_connector.create_sla_policy(body)

        assert result == {"id": 31}
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith("/slas/policies.json")
        assert mock_request.call_args[1]["json"] == {"sla_policy": body}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_sla_policy_targets_one_id(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"sla_policy": {"id": 31}})
        connected_connector.update_sla_policy(31, {"position": 2})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/slas/policies/31.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_sla_policy_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"sla_policy": {"id": 31, "title": "Campaigns"}}
        )
        assert connected_connector.get_sla_policy(31)["title"] == "Campaigns"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_macro(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"macro": {"id": 41}})
        body = {"title": "Campaigns: Ask for more information", "actions": []}
        result = connected_connector.create_macro(body)

        assert result == {"id": 41}
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith("/macros.json")
        assert mock_request.call_args[1]["json"] == {"macro": body}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_macro_targets_one_id(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"macro": {"id": 41}})
        connected_connector.update_macro(41, {"active": False})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/macros/41.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_create_article_posts_to_its_section(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"article": {"id": 51}})
        body = {"title": "Which request type?", "body": "<p>x</p>", "locale": "en-us"}
        result = connected_connector.create_help_center_article(77, body)

        assert result == {"id": 51}
        assert mock_request.call_args[0][0] == "POST"
        assert mock_request.call_args[0][1].endswith(
            "/help_center/sections/77/articles.json"
        )
        assert mock_request.call_args[1]["json"] == {"article": body}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_article_metadata(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"article": {"id": 51}})
        connected_connector.update_help_center_article(51, {"draft": False})

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith("/help_center/articles/51.json")

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_update_article_translation_carries_the_text(
        self, mock_request, connected_connector
    ):
        """Article text lives in a per-locale translation, not on the article."""
        mock_request.return_value = _make_response(200, {"translation": {"id": 61}})
        connected_connector.update_help_center_article_translation(
            51, "en-us", {"title": "T", "body": "<p>b</p>"}
        )

        assert mock_request.call_args[0][0] == "PUT"
        assert mock_request.call_args[0][1].endswith(
            "/help_center/articles/51/translations/en-us.json"
        )
        assert mock_request.call_args[1]["json"] == {
            "translation": {"title": "T", "body": "<p>b</p>"}
        }

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_get_article_unwraps(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"article": {"id": 51, "title": "x"}}
        )
        assert connected_connector.get_help_center_article(51)["title"] == "x"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_guide_permission_groups_use_the_guide_path(
        self, mock_request, connected_connector
    ):
        """Permissions live under /guide, content under /help_center."""
        mock_request.return_value = _make_response(
            200, {"permission_groups": [{"id": 1, "name": "Admins"}]}
        )
        assert connected_connector.list_guide_permission_groups()[0]["name"] == "Admins"
        assert "/guide/permission_groups.json" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_count_returns_int_only(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"count": 0})
        assert connected_connector.search_count("type:ticket ticket_form_id:5") == 0
        assert mock_request.call_args[1]["params"] == {
            "query": "type:ticket ticket_form_id:5"
        }

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_passes_query_and_splits_sort(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"results": [{"id": 1}], "next_page": None}
        )
        assert connected_connector.search("type:ticket", sort="created_at desc") == [
            {"id": 1}
        ]
        assert mock_request.call_args[1]["params"] == {
            "query": "type:ticket",
            "sort_by": "created_at",
            "sort_order": "desc",
        }

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_defaults_sort_order_when_only_a_field_is_given(
        self, mock_request, connected_connector
    ):
        mock_request.return_value = _make_response(200, {"results": [], "next_page": None})
        connected_connector.search("type:ticket", sort="created_at")
        assert mock_request.call_args[1]["params"]["sort_order"] == "desc"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_omits_sort_keys_entirely_when_unsorted(
        self, mock_request, connected_connector
    ):
        mock_request.return_value = _make_response(200, {"results": [], "next_page": None})
        connected_connector.search("type:ticket")
        assert mock_request.call_args[1]["params"] == {"query": "type:ticket"}

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_stops_paginating_at_max_results(
        self, mock_request, connected_connector
    ):
        """The cap is the point of this method.

        `list_tickets` walks the WHOLE instance, which on CCEF's shared Zendesk
        is thousands of other teams' tickets and a big slice of a rate budget
        IT is already tripping 429s on. A search that ignored max_results would
        be no better.
        """
        page2 = "https://test.zendesk.com/api/v2/search.json?page=2"
        mock_request.side_effect = [
            _make_response(200, {"results": [{"id": 1}, {"id": 2}], "next_page": page2}),
            _make_response(200, {"results": [{"id": 3}], "next_page": None}),
        ]
        assert connected_connector.search("type:ticket", max_results=2) == [
            {"id": 1},
            {"id": 2},
        ]
        assert mock_request.call_count == 1, "should not have fetched page 2"

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_search_truncates_an_overshooting_page(
        self, mock_request, connected_connector
    ):
        """Zendesk's page size is fixed, so the last page routinely overshoots."""
        mock_request.return_value = _make_response(
            200, {"results": [{"id": 1}, {"id": 2}, {"id": 3}], "next_page": None}
        )
        assert connected_connector.search("type:ticket", max_results=2) == [
            {"id": 1},
            {"id": 2},
        ]

    @patch("ccef_connections.connectors.zendesk.requests.request")
    def test_list_recipient_addresses(self, mock_request, connected_connector):
        """Support addresses, with the status fields that decide whether mail to
        an address actually becomes a ticket."""
        mock_request.return_value = _make_response(
            200,
            {
                "recipient_addresses": [
                    {
                        "email": "campaigns@example.zendesk.com",
                        "forwarding_status": "verified",
                    }
                ],
                "next_page": None,
            },
        )
        addresses = connected_connector.list_recipient_addresses()
        assert addresses[0]["forwarding_status"] == "verified"


# ── Scope handling ────────────────────────────────────────────────────


class TestScope:
    def test_read_write_scope_constant_is_read_write(self):
        """'write' alone yields a token that 403s -- see module docstring note 3."""
        from ccef_connections.connectors.zendesk import ZENDESK_READ_WRITE_SCOPE

        assert ZENDESK_READ_WRITE_SCOPE == "read write"

    @patch("ccef_connections.connectors.zendesk.requests.post")
    def test_requested_scope_is_sent_to_token_endpoint(self, mock_post, connector):
        from ccef_connections.connectors.zendesk import ZENDESK_READ_WRITE_SCOPE

        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)
        c = ZendeskConnector(
            subdomain=SUBDOMAIN, scope=ZENDESK_READ_WRITE_SCOPE, max_requests_per_minute=0
        )
        c._credential_manager = connector._credential_manager
        c.connect()

        assert mock_post.call_args[1]["json"]["scope"] == "read write"

    def test_mutation_is_not_available_at_default_scope(self):
        """Writing takes an explicit opt-in; the default connector is read-only."""
        assert ZENDESK_DEFAULT_SCOPE == "read"


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
