"""Tests for the Action Network connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.action_network import (
    ACTION_NETWORK_API_BASE,
    ActionNetworkConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# -- helpers ----------------------------------------------------------------

FAKE_API_KEY = "fake-an-api-key-12345"


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    return resp


def _page(resource_key, items, next_href=None):
    """Build a HAL+JSON page body."""
    data = {"_embedded": {resource_key: items}, "_links": {}}
    if next_href:
        data["_links"]["next"] = {"href": next_href}
    return data


# -- fixtures ---------------------------------------------------------------


@pytest.fixture
def connector():
    """Create an ActionNetworkConnector with mocked credentials."""
    with patch.object(
        ActionNetworkConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_action_network_key.return_value = FAKE_API_KEY
        c = ActionNetworkConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """Return a connector that is already connected."""
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


# ==========================================================================
# Initialization
# ==========================================================================


class TestInit:
    def test_initial_state(self):
        c = ActionNetworkConnector()
        assert c._api_key is None
        assert not c.is_connected()

    def test_repr_disconnected(self):
        c = ActionNetworkConnector()
        assert repr(c) == "<ActionNetworkConnector status=disconnected>"

    def test_repr_connected(self, connected):
        assert repr(connected) == "<ActionNetworkConnector status=connected>"


# ==========================================================================
# Connect / Disconnect
# ==========================================================================


class TestConnect:
    def test_connect_success(self, connector):
        connector.connect()
        assert connector.is_connected()
        assert connector._api_key == FAKE_API_KEY

    def test_connect_missing_credentials(self):
        c = ActionNetworkConnector()
        c._credential_manager.get_action_network_key = MagicMock(
            side_effect=CredentialError("missing")
        )
        with pytest.raises(ConnectionError, match="missing"):
            c.connect()

    def test_disconnect(self, connected):
        connected.disconnect()
        assert not connected.is_connected()
        assert connected._api_key is None


# ==========================================================================
# Health Check
# ==========================================================================


class TestHealthCheck:
    def test_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_success(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"motd": "Welcome"})
        assert connected.health_check() is True

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_failure(self, mock_req, connected):
        mock_req.side_effect = requests.ConnectionError("down")
        assert connected.health_check() is False


# ==========================================================================
# _request – HTTP layer
# ==========================================================================


class TestRequest:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_success(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"ok": True})
        result = connected._request("GET", "/people")
        assert result == {"ok": True}
        mock_req.assert_called_once_with(
            "GET",
            f"{ACTION_NETWORK_API_BASE}/people",
            headers={
                "OSDI-API-Token": FAKE_API_KEY,
                "Content-Type": "application/hal+json",
            },
            params=None,
            json=None,
            timeout=30,
        )

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_post_with_body(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "abc"})
        body = {"name": "Test"}
        result = connected._request("POST", "/tags", json_body=body)
        assert result == {"id": "abc"}
        call_kwargs = mock_req.call_args
        assert call_kwargs.kwargs["json"] == body

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_401_raises_auth_error(self, mock_req, connected):
        mock_req.return_value = _make_response(401, text="Unauthorized")
        with pytest.raises(AuthenticationError, match="401"):
            connected._request("GET", "/people")

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_429_raises_rate_limit_error(self, mock_req, connected):
        mock_req.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "2"}
        )
        with pytest.raises(RateLimitError, match="rate limit"):
            connected._request("GET", "/people")

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_404_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(404, text="Not Found")
        with pytest.raises(ConnectionError, match="404"):
            connected._request("GET", "/people/nope")

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_500_raises_connection_error(self, mock_req, connected):
        mock_req.return_value = _make_response(500, text="Internal Server Error")
        with pytest.raises(ConnectionError, match="500"):
            connected._request("GET", "/people")

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_204_returns_none(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        result = connected._request("DELETE", "/tags/x/taggings/y")
        assert result is None

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_network_error(self, mock_req, connected):
        mock_req.side_effect = requests.ConnectionError("DNS failure")
        with pytest.raises(ConnectionError, match="request failed"):
            connected._request("GET", "/people")

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_auto_connect_when_not_connected(self, mock_req, connector):
        mock_req.return_value = _make_response(200, {"ok": True})
        result = connector._request("GET", "/people")
        assert result == {"ok": True}
        assert connector.is_connected()


# ==========================================================================
# _paginate
# ==========================================================================


class TestPaginate:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_single_page(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:people", [{"id": "1"}, {"id": "2"}])
        )
        result = connected._paginate("/people", "osdi:people")
        assert len(result) == 2

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_multi_page(self, mock_req, connected):
        page1 = _page(
            "osdi:people",
            [{"id": "1"}],
            next_href=f"{ACTION_NETWORK_API_BASE}/people?page=2",
        )
        page2 = _page("osdi:people", [{"id": "2"}])
        mock_req.side_effect = [
            _make_response(200, page1),
            _make_response(200, page2),
        ]
        result = connected._paginate("/people", "osdi:people")
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_empty_page(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"_embedded": {}, "_links": {}})
        result = connected._paginate("/people", "osdi:people")
        assert result == []

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_none_response(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        result = connected._paginate("/people", "osdi:people")
        assert result == []


# ==========================================================================
# People
# ==========================================================================


class TestPeople:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_people(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:people", [{"given_name": "Jane"}])
        )
        result = connected.list_people()
        assert len(result) == 1
        assert result[0]["given_name"] == "Jane"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_people_with_filters(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:people", [])
        )
        connected.list_people(filter="email_address eq 'a@b.com'")
        call_kwargs = mock_req.call_args
        assert call_kwargs.kwargs["params"] == {
            "filter": "email_address eq 'a@b.com'"
        }

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_person(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, {"given_name": "Jane", "family_name": "Doe"}
        )
        result = connected.get_person("abc-123")
        assert result["given_name"] == "Jane"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_person_basic(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"identifiers": ["an:1"]})
        result = connected.create_person(email="a@b.com")
        assert result["identifiers"] == ["an:1"]
        body = mock_req.call_args.kwargs["json"]
        assert body["person"]["email_addresses"] == [{"address": "a@b.com"}]
        assert "add_tags" not in body

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_person_with_name_and_tags(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"identifiers": ["an:2"]})
        connected.create_person(
            email="jane@example.com",
            given_name="Jane",
            family_name="Doe",
            tags=["volunteer", "2024"],
        )
        body = mock_req.call_args.kwargs["json"]
        assert body["person"]["given_name"] == "Jane"
        assert body["person"]["family_name"] == "Doe"
        assert body["add_tags"] == ["volunteer", "2024"]

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_person_with_kwargs(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {})
        connected.create_person(
            email="a@b.com",
            postal_addresses=[{"postal_code": "20001"}],
        )
        body = mock_req.call_args.kwargs["json"]
        assert body["person"]["postal_addresses"] == [{"postal_code": "20001"}]

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_person(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"given_name": "Janet"})
        result = connected.update_person("abc-123", {"given_name": "Janet"})
        assert result["given_name"] == "Janet"
        assert mock_req.call_args.args[0] == "PUT"


# ==========================================================================
# Tags
# ==========================================================================


class TestTags:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_tags(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:tags", [{"name": "volunteer"}])
        )
        result = connected.list_tags()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_tag(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "volunteer"})
        result = connected.get_tag("tag-1")
        assert result["name"] == "volunteer"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_tag(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "new-tag"})
        result = connected.create_tag("new-tag")
        assert result["name"] == "new-tag"
        body = mock_req.call_args.kwargs["json"]
        assert body == {"name": "new-tag"}


# ==========================================================================
# Taggings
# ==========================================================================


class TestTaggings:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_taggings(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:taggings", [{"id": "t1"}])
        )
        result = connected.list_taggings("tag-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_add_tagging_single(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "tagging-1"})
        result = connected.add_tagging(
            "tag-1",
            ["https://actionnetwork.org/api/v2/people/abc-123"],
        )
        assert result["id"] == "tagging-1"
        body = mock_req.call_args.kwargs["json"]
        assert body["_links"]["osdi:person"]["href"] == (
            "https://actionnetwork.org/api/v2/people/abc-123"
        )

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_add_tagging_multiple(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {})
        connected.add_tagging(
            "tag-1",
            [
                "https://actionnetwork.org/api/v2/people/abc",
                "https://actionnetwork.org/api/v2/people/def",
            ],
        )
        body = mock_req.call_args.kwargs["json"]
        assert isinstance(body["_links"]["osdi:person"], list)
        assert len(body["_links"]["osdi:person"]) == 2

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_delete_tagging(self, mock_req, connected):
        mock_req.return_value = _make_response(204)
        connected.delete_tagging("tag-1", "tagging-1")
        assert mock_req.call_args.args[0] == "DELETE"


# ==========================================================================
# Events
# ==========================================================================


class TestEvents:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_events(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:events", [{"title": "Rally"}])
        )
        result = connected.list_events()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_event(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Rally"})
        result = connected.get_event("ev-1")
        assert result["title"] == "Rally"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_event(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Rally"})
        result = connected.create_event("Rally", start_date="2026-03-01T10:00:00Z")
        body = mock_req.call_args.kwargs["json"]
        assert body["title"] == "Rally"
        assert body["start_date"] == "2026-03-01T10:00:00Z"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_event(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Updated"})
        result = connected.update_event("ev-1", {"title": "Updated"})
        assert result["title"] == "Updated"
        assert mock_req.call_args.args[0] == "PUT"


# ==========================================================================
# Attendances
# ==========================================================================


class TestAttendances:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_attendances(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:attendances", [{"id": "a1"}])
        )
        result = connected.list_attendances("ev-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_attendance(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "a1"})
        result = connected.get_attendance("ev-1", "a1")
        assert result["id"] == "a1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_attendance(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "a2"})
        person = {"person": {"email_addresses": [{"address": "a@b.com"}]}}
        result = connected.create_attendance("ev-1", person)
        assert result["id"] == "a2"


# ==========================================================================
# Petitions
# ==========================================================================


class TestPetitions:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_petitions(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:petitions", [{"title": "Save the Park"}])
        )
        result = connected.list_petitions()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_petition(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Save the Park"})
        result = connected.get_petition("pet-1")
        assert result["title"] == "Save the Park"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_petition(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "New Petition"})
        result = connected.create_petition("New Petition", description="Test")
        body = mock_req.call_args.kwargs["json"]
        assert body["title"] == "New Petition"
        assert body["description"] == "Test"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_petition(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Updated"})
        result = connected.update_petition("pet-1", {"title": "Updated"})
        assert result["title"] == "Updated"


# ==========================================================================
# Signatures
# ==========================================================================


class TestSignatures:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_signatures(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:signatures", [{"id": "s1"}])
        )
        result = connected.list_signatures("pet-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_signature(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "s1"})
        result = connected.get_signature("pet-1", "s1")
        assert result["id"] == "s1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_signature(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "s2"})
        person = {"person": {"email_addresses": [{"address": "a@b.com"}]}}
        result = connected.create_signature("pet-1", person)
        assert result["id"] == "s2"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_signature(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "s1"})
        result = connected.update_signature("pet-1", "s1", {"comments": "updated"})
        assert mock_req.call_args.args[0] == "PUT"


# ==========================================================================
# Forms
# ==========================================================================


class TestForms:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_forms(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:forms", [{"title": "Signup"}])
        )
        result = connected.list_forms()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_form(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Signup"})
        result = connected.get_form("form-1")
        assert result["title"] == "Signup"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_form(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "New Form"})
        result = connected.create_form("New Form")
        assert result["title"] == "New Form"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_form(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Updated"})
        result = connected.update_form("form-1", {"title": "Updated"})
        assert result["title"] == "Updated"


# ==========================================================================
# Submissions
# ==========================================================================


class TestSubmissions:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_submissions(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:submissions", [{"id": "sub1"}])
        )
        result = connected.list_submissions("form-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_submission(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "sub1"})
        result = connected.get_submission("form-1", "sub1")
        assert result["id"] == "sub1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_submission(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "sub2"})
        person = {"person": {"email_addresses": [{"address": "a@b.com"}]}}
        result = connected.create_submission("form-1", person)
        assert result["id"] == "sub2"


# ==========================================================================
# Fundraising Pages
# ==========================================================================


class TestFundraisingPages:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_fundraising_pages(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:fundraising_pages", [{"title": "Donate"}])
        )
        result = connected.list_fundraising_pages()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_fundraising_page(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Donate"})
        result = connected.get_fundraising_page("fp-1")
        assert result["title"] == "Donate"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_fundraising_page(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "New Page"})
        result = connected.create_fundraising_page("New Page")
        assert result["title"] == "New Page"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_fundraising_page(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Updated"})
        result = connected.update_fundraising_page("fp-1", {"title": "Updated"})
        assert result["title"] == "Updated"


# ==========================================================================
# Donations
# ==========================================================================


class TestDonations:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_donations(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:donations", [{"id": "d1"}])
        )
        result = connected.list_donations("fp-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_donation(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "d1"})
        result = connected.get_donation("fp-1", "d1")
        assert result["id"] == "d1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_donation(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "d2"})
        data = {
            "person": {"email_addresses": [{"address": "a@b.com"}]},
            "recipients": [{"amount": "10.00"}],
        }
        result = connected.create_donation("fp-1", data)
        assert result["id"] == "d2"


# ==========================================================================
# Lists
# ==========================================================================


class TestLists:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_lists(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:lists", [{"name": "Active Volunteers"}])
        )
        result = connected.list_lists()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_list(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "Active Volunteers"})
        result = connected.get_list("list-1")
        assert result["name"] == "Active Volunteers"


# ==========================================================================
# Messages
# ==========================================================================


class TestMessages:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_messages(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:messages", [{"subject": "Hello"}])
        )
        result = connected.list_messages()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_message(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"subject": "Hello"})
        result = connected.get_message("msg-1")
        assert result["subject"] == "Hello"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_message(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"subject": "Hello"})
        result = connected.create_message(
            subject="Hello",
            body="<p>World</p>",
            targets=[{"type": "tag", "id": "tag-1"}],
        )
        body = mock_req.call_args.kwargs["json"]
        assert body["subject"] == "Hello"
        assert body["body"] == "<p>World</p>"
        assert body["targets"] == [{"type": "tag", "id": "tag-1"}]


# ==========================================================================
# Wrappers
# ==========================================================================


class TestWrappers:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_wrappers(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:wrappers", [{"id": "w1"}])
        )
        result = connected.list_wrappers()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_wrapper(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "w1"})
        result = connected.get_wrapper("w1")
        assert result["id"] == "w1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_wrapper(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "w2"})
        result = connected.create_wrapper(header="<h1>Hi</h1>", footer="<p>Bye</p>")
        body = mock_req.call_args.kwargs["json"]
        assert body["header"] == "<h1>Hi</h1>"
        assert body["footer"] == "<p>Bye</p>"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_wrapper(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"id": "w1"})
        result = connected.update_wrapper("w1", {"header": "<h1>Updated</h1>"})
        assert mock_req.call_args.args[0] == "PUT"


# ==========================================================================
# Custom Fields (metadata)
# ==========================================================================


class TestCustomFields:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_custom_fields(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:metadata", [{"name": "district"}])
        )
        result = connected.list_custom_fields()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_custom_field(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "district"})
        result = connected.get_custom_field("cf-1")
        assert result["name"] == "district"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_custom_field(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "district"})
        result = connected.create_custom_field("district", "text")
        body = mock_req.call_args.kwargs["json"]
        assert body == {"name": "district", "format": "text"}

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_custom_field(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"name": "district"})
        result = connected.update_custom_field("cf-1", {"name": "district_v2"})
        assert mock_req.call_args.args[0] == "PUT"


# ==========================================================================
# Event Campaigns
# ==========================================================================


class TestEventCampaigns:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_event_campaigns(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200,
            _page("action_network:event_campaigns", [{"title": "Campaign 1"}]),
        )
        result = connected.list_event_campaigns()
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_get_event_campaign(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Campaign 1"})
        result = connected.get_event_campaign("ec-1")
        assert result["title"] == "Campaign 1"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_event_campaign(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "New Campaign"})
        result = connected.create_event_campaign("New Campaign")
        body = mock_req.call_args.kwargs["json"]
        assert body["title"] == "New Campaign"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_update_event_campaign(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "Updated"})
        result = connected.update_event_campaign("ec-1", {"title": "Updated"})
        assert mock_req.call_args.args[0] == "PUT"

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_list_campaign_events(self, mock_req, connected):
        mock_req.return_value = _make_response(
            200, _page("osdi:events", [{"title": "Event in Campaign"}])
        )
        result = connected.list_campaign_events("ec-1")
        assert len(result) == 1

    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_create_campaign_event(self, mock_req, connected):
        mock_req.return_value = _make_response(200, {"title": "New Event"})
        result = connected.create_campaign_event(
            "ec-1", {"title": "New Event", "start_date": "2026-04-01T10:00:00Z"}
        )
        assert result["title"] == "New Event"


# ==========================================================================
# Context Manager
# ==========================================================================


class TestContextManager:
    @patch("ccef_connections.connectors.action_network.requests.request")
    def test_context_manager(self, mock_req, connector):
        mock_req.return_value = _make_response(200, {"motd": "Welcome"})
        with connector as c:
            assert c.is_connected()
        assert not c.is_connected()


# ==========================================================================
# Credential Validation
# ==========================================================================


class TestCredentials:
    def test_credential_manager_singleton_bypass(self):
        """Verify test isolation with object.__new__() bypass."""
        from ccef_connections.core.credentials import CredentialManager

        cm = object.__new__(CredentialManager)
        cm._credentials_cache = {}
        cm._env_loaded = True
        assert isinstance(cm, CredentialManager)
        assert cm._credentials_cache == {}

    def test_get_action_network_key(self):
        """Test that CredentialManager can retrieve the AN key."""
        from ccef_connections.core.credentials import CredentialManager

        cm = object.__new__(CredentialManager)
        cm._credentials_cache = {}
        cm._env_loaded = True
        with patch.dict("os.environ", {"ACTION_NETWORK_API_KEY_PASSWORD": "test-key"}):
            key = cm.get_action_network_key()
            assert key == "test-key"

    def test_get_action_network_key_missing(self):
        """Test error when AN key is missing."""
        from ccef_connections.core.credentials import CredentialManager

        cm = object.__new__(CredentialManager)
        cm._credentials_cache = {}
        cm._env_loaded = True
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(CredentialError, match="ACTION_NETWORK_API_KEY_PASSWORD"):
                cm.get_action_network_key()
