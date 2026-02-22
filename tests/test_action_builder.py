"""Tests for the Action Builder connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.action_builder import (
    ACTION_BUILDER_API_BASE,
    ActionBuilderConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# -- helpers ----------------------------------------------------------------

FAKE_API_TOKEN = "test-token-123"
FAKE_SUBDOMAIN = "testorg"
FAKE_CREDS = {"api_token": FAKE_API_TOKEN, "subdomain": FAKE_SUBDOMAIN}
FAKE_BASE_URL = ACTION_BUILDER_API_BASE.format(subdomain=FAKE_SUBDOMAIN)

CAMPAIGN_ID = "campaign-uuid-1"
PERSON_ID = "person-uuid-1"
TAG_ID = "tag-uuid-1"
TAGGING_ID = "tagging-uuid-1"
CONNECTION_ID = "connection-uuid-1"
TYPE_ID = "type-uuid-1"


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    return resp


def _page(resource_key, items, page=1, total_pages=1):
    """Build a page-based HAL body."""
    return {
        "_embedded": {resource_key: items},
        "page": page,
        "per_page": 25,
        "total_pages": total_pages,
    }


# -- fixtures ---------------------------------------------------------------


@pytest.fixture
def connector():
    """Create an ActionBuilderConnector with mocked credentials."""
    with patch.object(
        ActionBuilderConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_action_builder_credentials.return_value = FAKE_CREDS
        c = ActionBuilderConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """Return a connector that is already connected."""
    connector._api_token = FAKE_API_TOKEN
    connector._subdomain = FAKE_SUBDOMAIN
    connector._base_url = FAKE_BASE_URL
    connector._is_connected = True
    return connector


# ==========================================================================
# Initialization
# ==========================================================================


class TestInit:
    def test_initial_state(self):
        c = ActionBuilderConnector()
        assert c._api_token is None
        assert c._subdomain is None
        assert c._base_url is None
        assert not c.is_connected()

    def test_repr_disconnected(self):
        c = ActionBuilderConnector()
        assert repr(c) == "<ActionBuilderConnector status=disconnected>"

    def test_repr_connected(self, connected):
        assert repr(connected) == "<ActionBuilderConnector status=connected>"


# ==========================================================================
# Connect / Disconnect
# ==========================================================================


class TestConnect:
    def test_connect_success(self, connector):
        connector.connect()
        assert connector.is_connected()
        assert connector._api_token == FAKE_API_TOKEN
        assert connector._subdomain == FAKE_SUBDOMAIN
        assert connector._base_url == FAKE_BASE_URL

    def test_connect_missing_credentials(self):
        c = ActionBuilderConnector()
        c._credential_manager.get_action_builder_credentials = MagicMock(
            side_effect=CredentialError("missing")
        )
        with pytest.raises(ConnectionError, match="missing"):
            c.connect()

    def test_connect_credential_error_wraps(self, connector):
        connector._credential_manager.get_action_builder_credentials = MagicMock(
            side_effect=CredentialError("bad json")
        )
        with pytest.raises(ConnectionError):
            connector.connect()


class TestDisconnect:
    def test_disconnect_clears_state(self, connected):
        connected.disconnect()
        assert not connected.is_connected()
        assert connected._api_token is None
        assert connected._subdomain is None
        assert connected._base_url is None


# ==========================================================================
# Health Check
# ==========================================================================


class TestHealthCheck:
    def test_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_success(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:campaigns", [])
        )
        assert connected.health_check() is True

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_failure(self, mock_req, connected):
        mock_req.side_effect = requests.ConnectionError("down")
        assert connected.health_check() is False


# ==========================================================================
# _request – HTTP layer
# ==========================================================================


class TestRequest:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_success(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"ok": True})
        result = connected._request("GET", "/campaigns")
        assert result == {"ok": True}
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns",
            headers={
                "OSDI-Api-Token": FAKE_API_TOKEN,
                "Content-Type": "application/json",
            },
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_put_with_body(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "abc"})
        body = {"inactive": True}
        result = connected._request(
            "PUT",
            f"/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}/connections/{CONNECTION_ID}",
            json_body=body,
        )
        assert result == {"id": "abc"}
        assert mock_req.call_args.kwargs["json"] == body

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_401_raises_auth_error(self, mock_req, connected):
        mock_req.return_value = _make_response(401, text="Unauthorized")
        with pytest.raises(AuthenticationError, match="401"):
            connected._request("GET", "/campaigns")

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_429_raises_rate_limit_error(self, mock_req, connected):
        mock_req.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "2"}
        )
        with pytest.raises(RateLimitError, match="rate limit"):
            connected._request("GET", "/campaigns")

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_429_retry_after_header(self, mock_req, connected):
        mock_req.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "5"}
        )
        with pytest.raises(RateLimitError) as exc_info:
            connected._request("GET", "/campaigns")
        assert exc_info.value.retry_after == 5

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_404_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(404, text="Not Found")
        with pytest.raises(ConnectionError, match="404"):
            connected._request("GET", "/campaigns/nope")

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_500_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(500, text="Internal Server Error")
        with pytest.raises(ConnectionError, match="500"):
            connected._request("GET", "/campaigns")

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_204_returns_none(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        result = connected._request(
            "DELETE", f"/campaigns/{CAMPAIGN_ID}/tags/{TAG_ID}/taggings/{TAGGING_ID}"
        )
        assert result is None

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_network_error(self, mock_req, connected):
        mock_req.side_effect = requests.ConnectionError("DNS failure")
        with pytest.raises(ConnectionError, match="request failed"):
            connected._request("GET", "/campaigns")

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_auto_connect_when_not_connected(self, mock_req, connector):
        mock_req.return_value = _make_response(200, {"ok": True})
        result = connector._request("GET", "/campaigns")
        assert result == {"ok": True}
        assert connector.is_connected()


# ==========================================================================
# _paginate
# ==========================================================================


class TestPaginate:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_single_page(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:campaigns", [{"id": "1"}, {"id": "2"}]),
        )
        result = connected._paginate("/campaigns", "action_builder:campaigns")
        assert len(result) == 2

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_multi_page(self, mock_req, connected):
        page1 = _page("action_builder:campaigns", [{"id": "1"}], page=1, total_pages=2)
        page2 = _page("action_builder:campaigns", [{"id": "2"}], page=2, total_pages=2)
        mock_req.side_effect = [
            _make_response(200, page1),
            _make_response(200, page2),
        ]
        result = connected._paginate("/campaigns", "action_builder:campaigns")
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_empty_page(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            {"_embedded": {}, "page": 1, "per_page": 25, "total_pages": 1},
        )
        result = connected._paginate("/campaigns", "action_builder:campaigns")
        assert result == []

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_none_response(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        result = connected._paginate("/campaigns", "action_builder:campaigns")
        assert result == []

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_passes_extra_params(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:entities", [])
        )
        connected._paginate(
            f"/campaigns/{CAMPAIGN_ID}/people",
            "action_builder:entities",
            params={"filter": "modified_date gt '2026-01-01'"},
        )
        call_params = mock_req.call_args.kwargs["params"]
        assert "filter" in call_params
        assert call_params["page"] == 1


# ==========================================================================
# Campaigns
# ==========================================================================


class TestCampaigns:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_campaigns(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:campaigns", [{"id": CAMPAIGN_ID}]),
        )
        result = connected.list_campaigns()
        assert len(result) == 1
        assert result[0]["id"] == CAMPAIGN_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_campaigns_modified_since(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:campaigns", [])
        )
        connected.list_campaigns(modified_since="2026-01-01T00:00:00")
        call_params = mock_req.call_args.kwargs["params"]
        assert "filter" in call_params
        assert "modified_date gt" in call_params["filter"]

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_campaign(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": CAMPAIGN_ID, "name": "Test"})
        result = connected.get_campaign(CAMPAIGN_ID)
        assert result["id"] == CAMPAIGN_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# Entity Types
# ==========================================================================


class TestEntityTypes:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_entity_types(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:entity_types", [{"id": TYPE_ID}]),
        )
        result = connected.list_entity_types(CAMPAIGN_ID)
        assert len(result) == 1
        assert result[0]["id"] == TYPE_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_entity_type(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": TYPE_ID})
        result = connected.get_entity_type(CAMPAIGN_ID, TYPE_ID)
        assert result["id"] == TYPE_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/entity_types/{TYPE_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# Connection Types
# ==========================================================================


class TestConnectionTypes:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_connection_types(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:connection_types", [{"id": TYPE_ID}]),
        )
        result = connected.list_connection_types(CAMPAIGN_ID)
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_connection_type(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": TYPE_ID})
        result = connected.get_connection_type(CAMPAIGN_ID, TYPE_ID)
        assert result["id"] == TYPE_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/connection_types/{TYPE_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# People / Entities
# ==========================================================================


class TestPeople:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_people(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:entities", [{"id": PERSON_ID}]),
        )
        result = connected.list_people(CAMPAIGN_ID)
        assert len(result) == 1
        assert result[0]["id"] == PERSON_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_people_modified_since(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:entities", [])
        )
        connected.list_people(CAMPAIGN_ID, modified_since="2026-01-01T00:00:00")
        call_params = mock_req.call_args.kwargs["params"]
        assert "filter" in call_params
        assert "modified_date gt" in call_params["filter"]

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_person(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": PERSON_ID})
        result = connected.get_person(CAMPAIGN_ID, PERSON_ID)
        assert result["id"] == PERSON_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_create_person(self, mock_req, connected):
        mock_req.return_value = _make_response(
            201, {"id": PERSON_ID, "given_name": "Jane"}
        )
        result = connected.create_person(
            CAMPAIGN_ID, given_name="Jane", family_name="Doe"
        )
        assert result["id"] == PERSON_ID
        call_json = mock_req.call_args.kwargs["json"]
        assert call_json == {"person": {"given_name": "Jane", "family_name": "Doe"}}

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_update_person(self, mock_req, connected):
        updated = {"id": PERSON_ID, "given_name": "Updated"}
        mock_req.return_value = _make_response(200, updated)
        result = connected.update_person(
            CAMPAIGN_ID, PERSON_ID, {"given_name": "Updated"}
        )
        assert result["given_name"] == "Updated"
        mock_req.assert_called_once_with(
            "PUT",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}",
            headers=connected._get_headers(),
            params=None,
            json={"given_name": "Updated"},
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_delete_person(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        connected.delete_person(CAMPAIGN_ID, PERSON_ID)
        mock_req.assert_called_once_with(
            "DELETE",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# Tags
# ==========================================================================


class TestTags:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_tags(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:tags", [{"id": TAG_ID}]),
        )
        result = connected.list_tags(CAMPAIGN_ID)
        assert len(result) == 1
        assert result[0]["id"] == TAG_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_tag(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": TAG_ID, "name": "Voter"})
        result = connected.get_tag(CAMPAIGN_ID, TAG_ID)
        assert result["id"] == TAG_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/tags/{TAG_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_create_tag(self, mock_req, connected):
        created = {"id": TAG_ID, "name": "Volunteer"}
        mock_req.return_value = _make_response(201, created)
        result = connected.create_tag(
            CAMPAIGN_ID,
            name="Volunteer",
            section="Status",
            field_type="checkbox",
        )
        assert result["id"] == TAG_ID
        call_json = mock_req.call_args.kwargs["json"]
        assert call_json["name"] == "Volunteer"
        assert call_json["section"] == "Status"
        assert call_json["field_type"] == "checkbox"

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_delete_tag(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        connected.delete_tag(CAMPAIGN_ID, TAG_ID)
        mock_req.assert_called_once_with(
            "DELETE",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/tags/{TAG_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# Taggings
# ==========================================================================


class TestTaggings:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_taggings(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:taggings", [{"id": TAGGING_ID}]),
        )
        result = connected.list_taggings(CAMPAIGN_ID, TAG_ID)
        assert len(result) == 1
        assert result[0]["id"] == TAGGING_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_taggings_url(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:taggings", [])
        )
        connected.list_taggings(CAMPAIGN_ID, TAG_ID)
        call_url = mock_req.call_args.args[1]
        assert f"/campaigns/{CAMPAIGN_ID}/tags/{TAG_ID}/taggings" in call_url

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_person_taggings(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:taggings", [{"id": TAGGING_ID}]),
        )
        result = connected.list_person_taggings(CAMPAIGN_ID, PERSON_ID)
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_person_taggings_url(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:taggings", [])
        )
        connected.list_person_taggings(CAMPAIGN_ID, PERSON_ID)
        call_url = mock_req.call_args.args[1]
        assert f"/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}/taggings" in call_url

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_delete_tagging(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        connected.delete_tagging(CAMPAIGN_ID, TAG_ID, TAGGING_ID)
        mock_req.assert_called_once_with(
            "DELETE",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/tags/{TAG_ID}/taggings/{TAGGING_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )


# ==========================================================================
# Connections
# ==========================================================================


class TestConnections:
    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_connections(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_builder:connections", [{"id": CONNECTION_ID}]),
        )
        result = connected.list_connections(CAMPAIGN_ID, PERSON_ID)
        assert len(result) == 1
        assert result[0]["id"] == CONNECTION_ID

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_list_connections_url(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("action_builder:connections", [])
        )
        connected.list_connections(CAMPAIGN_ID, PERSON_ID)
        call_url = mock_req.call_args.args[1]
        assert (
            f"/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}/connections" in call_url
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_get_connection(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": CONNECTION_ID})
        result = connected.get_connection(CAMPAIGN_ID, PERSON_ID, CONNECTION_ID)
        assert result["id"] == CONNECTION_ID
        mock_req.assert_called_once_with(
            "GET",
            f"{FAKE_BASE_URL}/campaigns/{CAMPAIGN_ID}/people/{PERSON_ID}/connections/{CONNECTION_ID}",
            headers=connected._get_headers(),
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_update_connection_inactive_true(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, {"id": CONNECTION_ID, "inactive": True}
        )
        result = connected.update_connection(
            CAMPAIGN_ID, PERSON_ID, CONNECTION_ID, inactive=True
        )
        assert result["inactive"] is True
        call_json = mock_req.call_args.kwargs["json"]
        assert call_json == {"inactive": True}

    @patch("ccef_connections.connectors.action_builder.requests.request")
    def test_update_connection_reactivate(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, {"id": CONNECTION_ID, "inactive": False}
        )
        result = connected.update_connection(
            CAMPAIGN_ID, PERSON_ID, CONNECTION_ID, inactive=False
        )
        assert result["inactive"] is False
        call_json = mock_req.call_args.kwargs["json"]
        assert call_json == {"inactive": False}


# ==========================================================================
# Credentials — get_action_builder_credentials
# ==========================================================================


class TestCredentials:
    def _make_manager(self):
        """Return a fresh CredentialManager instance bypassing singleton."""
        from ccef_connections.core.credentials import CredentialManager

        mgr = object.__new__(CredentialManager)
        mgr._credentials_cache = {}
        mgr._env_loaded = True
        return mgr

    def test_get_credentials_success(self):
        mgr = self._make_manager()
        import json
        import os
        from unittest.mock import patch

        creds_json = json.dumps(FAKE_CREDS)
        with patch.dict(
            os.environ, {"ACTION_BUILDER_CREDENTIALS_PASSWORD": creds_json}
        ):
            result = mgr.get_action_builder_credentials()
        assert result["api_token"] == FAKE_API_TOKEN
        assert result["subdomain"] == FAKE_SUBDOMAIN

    def test_get_credentials_missing_api_token(self):
        mgr = self._make_manager()
        import json
        import os
        from unittest.mock import patch

        from ccef_connections.exceptions import CredentialError

        creds_json = json.dumps({"subdomain": FAKE_SUBDOMAIN})
        with patch.dict(
            os.environ, {"ACTION_BUILDER_CREDENTIALS_PASSWORD": creds_json}
        ):
            with pytest.raises(CredentialError, match="api_token"):
                mgr.get_action_builder_credentials()

    def test_get_credentials_missing_subdomain(self):
        mgr = self._make_manager()
        import json
        import os
        from unittest.mock import patch

        from ccef_connections.exceptions import CredentialError

        creds_json = json.dumps({"api_token": FAKE_API_TOKEN})
        with patch.dict(
            os.environ, {"ACTION_BUILDER_CREDENTIALS_PASSWORD": creds_json}
        ):
            with pytest.raises(CredentialError, match="subdomain"):
                mgr.get_action_builder_credentials()

    def test_get_credentials_bad_json(self):
        mgr = self._make_manager()
        import os
        from unittest.mock import patch

        from ccef_connections.exceptions import CredentialError

        with patch.dict(
            os.environ, {"ACTION_BUILDER_CREDENTIALS_PASSWORD": "not-json"}
        ):
            with pytest.raises(CredentialError):
                mgr.get_action_builder_credentials()

    def test_get_credentials_missing_env_var(self):
        mgr = self._make_manager()
        import os
        from unittest.mock import patch

        from ccef_connections.exceptions import CredentialError

        env = {k: v for k, v in os.environ.items() if "ACTION_BUILDER" not in k}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(CredentialError):
                mgr.get_action_builder_credentials()
