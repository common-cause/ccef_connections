"""Tests for the ROI CRM connector."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.roi_crm import (
    ROI_API_BASE,
    ROI_TOKEN_URL,
    ROICRMConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Fixtures ──────────────────────────────────────────────────────────

FAKE_CREDS = {
    "client_id": "test-client-id",
    "client_secret": "test-client-secret",
    "audience": "https://app.roicrm.net/api/1.0",
    "roi_client_code": "TEST_ORG",
}

TOKEN_RESPONSE = {
    "access_token": "fake-roi-token",
    "token_type": "Bearer",
    "expires_in": 86400,
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
    """Create a ROICRMConnector with mocked credentials."""
    with patch.object(ROICRMConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_roi_crm_credentials.return_value = FAKE_CREDS
        c = ROICRMConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a fake token."""
    connector._access_token = "fake-roi-token"
    connector._token_expires_at = time.time() + 80000
    connector._is_connected = True
    return connector


# ── Initialization ────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = ROICRMConnector()
        assert c._access_token is None
        assert c._token_expires_at == 0.0
        assert not c.is_connected()

    def test_repr_disconnected(self):
        c = ROICRMConnector()
        assert repr(c) == "<ROICRMConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<ROICRMConnector status=connected>"


# ── Connect / Disconnect ─────────────────────────────────────────────


class TestConnect:
    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_connect_success(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        assert connector.is_connected()
        assert connector._access_token == "fake-roi-token"

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_connect_sends_all_required_fields(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        call_kwargs = mock_post.call_args.kwargs
        body = call_kwargs["json"]
        assert body["grant_type"] == "client_credentials"
        assert body["client_id"] == "test-client-id"
        assert body["client_secret"] == "test-client-secret"
        assert body["audience"] == "https://app.roicrm.net/api/1.0"
        assert body["roi_client_code"] == "TEST_ORG"

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_connect_posts_to_auth0_url(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        connector.connect()

        assert mock_post.call_args[0][0] == ROI_TOKEN_URL

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_connect_auth_failure(self, mock_post, connector):
        mock_post.return_value = _make_response(401, text="Unauthorized")

        with pytest.raises(AuthenticationError, match="401"):
            connector.connect()

        assert not connector.is_connected()

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_connect_network_error(self, mock_post, connector):
        mock_post.side_effect = requests.ConnectionError("DNS failure")

        with pytest.raises(ConnectionError, match="Failed to connect to ROI CRM"):
            connector.connect()

    def test_connect_missing_credentials(self):
        c = ROICRMConnector()
        c._credential_manager.get_roi_crm_credentials = MagicMock(
            side_effect=CredentialError("missing")
        )

        with pytest.raises(ConnectionError, match="missing"):
            c.connect()

    def test_disconnect(self, connected_connector):
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._access_token is None
        assert connected_connector._token_expires_at == 0.0

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_token_expiry_set_correctly(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        before = time.time()
        connector.connect()
        after = time.time()

        # Token valid 24h minus 60s buffer
        expected_min = before + 86400 - 60
        expected_max = after + 86400 - 60
        assert expected_min <= connector._token_expires_at <= expected_max


# ── Health Check ──────────────────────────────────────────────────────


class TestHealthCheck:
    def test_health_check_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_health_check_success(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"status": "ok"})

        assert connected_connector.health_check() is True
        assert "/ping/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_health_check_failure(self, mock_request, connected_connector):
        mock_request.side_effect = requests.ConnectionError("timeout")

        assert connected_connector.health_check() is False


# ── Context Manager ───────────────────────────────────────────────────


class TestContextManager:
    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_context_manager(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with connector as c:
            assert c.is_connected()

        assert not c.is_connected()


# ── Token Refresh ─────────────────────────────────────────────────────


class TestTokenRefresh:
    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_refresh_when_expired(self, mock_post, connected_connector):
        mock_post.return_value = _make_response(
            200, {"access_token": "new-token", "expires_in": 86400}
        )
        connected_connector._token_expires_at = time.time() - 1  # expired

        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer new-token"
        mock_post.assert_called_once()

    def test_no_refresh_when_valid(self, connected_connector):
        headers = connected_connector._get_headers()

        assert headers["Authorization"] == "Bearer fake-roi-token"
        assert headers["Content-Type"] == "application/json"


# ── _request internals ────────────────────────────────────────────────


class TestRequest:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_request(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"id": 1})

        result = connected_connector._request("GET", "/donors/1/")

        assert result == {"id": 1}
        assert mock_request.call_args[0][0] == "GET"
        assert mock_request.call_args[0][1] == f"{ROI_API_BASE}/donors/1/"

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_post_request_with_body(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201, {"id": 99})

        result = connected_connector._request(
            "POST", "/donors/", json_body={"first_name": "Jane"}
        )

        assert result == {"id": 99}
        assert mock_request.call_args.kwargs["json"] == {"first_name": "Jane"}

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_204_returns_none(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._request("DELETE", "/donors/1/flags/5/")

        assert result is None

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_401_triggers_refresh_and_retry(
        self, mock_post, mock_request, connected_connector
    ):
        mock_request.side_effect = [
            _make_response(401),
            _make_response(200, {"ok": True}),
        ]
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 86400}
        )

        result = connected_connector._request("GET", "/donors/1/")

        assert result == {"ok": True}
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_401_after_refresh_raises(self, mock_post, mock_request, connected_connector):
        mock_request.return_value = _make_response(401)
        mock_post.return_value = _make_response(
            200, {"access_token": "refreshed", "expires_in": 86400}
        )

        with pytest.raises(AuthenticationError, match="after token refresh"):
            connected_connector._request("GET", "/donors/1/")

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_429_raises_rate_limit(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            429, headers={"Retry-After": "30"}
        )

        with pytest.raises(RateLimitError, match="retry after 30s") as exc_info:
            connected_connector._request("GET", "/donors/")

        assert exc_info.value.retry_after == 30

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_500_raises_connection_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(500, text="Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector._request("GET", "/donors/1/")

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_404_raises_connection_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(404, text="Not Found")

        with pytest.raises(ConnectionError, match="404"):
            connected_connector._request("GET", "/donors/99999/")

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_network_error_raises_connection_error(
        self, mock_request, connected_connector
    ):
        mock_request.side_effect = requests.ConnectionError("timeout")

        with pytest.raises(ConnectionError, match="request failed"):
            connected_connector._request("GET", "/donors/1/")

    @patch("ccef_connections.connectors.roi_crm.requests.post")
    def test_auto_connect_on_request(self, mock_post, connector):
        mock_post.return_value = _make_response(200, TOKEN_RESPONSE)

        with patch("ccef_connections.connectors.roi_crm.requests.request") as mock_req:
            mock_req.return_value = _make_response(200, {"data": "ok"})
            result = connector._request("GET", "/donors/1/")

        assert result == {"data": "ok"}
        assert connector.is_connected()


# ── Pagination ────────────────────────────────────────────────────────


class TestPagination:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_single_page(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {"items": [{"id": 1}, {"id": 2}], "next": None, "prev": None},
        )

        result = connected_connector._paginate("/donors/")

        assert result == [{"id": 1}, {"id": 2}]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_multi_page(self, mock_request, connected_connector):
        page1 = _make_response(
            200,
            {
                "items": [{"id": 1}],
                "next": f"{ROI_API_BASE}/donors/?page=2",
                "prev": None,
            },
        )
        page2 = _make_response(
            200,
            {"items": [{"id": 2}], "next": None, "prev": f"{ROI_API_BASE}/donors/?page=1"},
        )
        mock_request.side_effect = [page1, page2]

        result = connected_connector._paginate("/donors/")

        assert result == [{"id": 1}, {"id": 2}]
        assert mock_request.call_count == 2

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_pagination_includes_per_page(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [], "next": None}
        )

        connected_connector._paginate("/donors/", per_page=50)

        assert mock_request.call_args.kwargs["params"]["per_page"] == 50

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_pagination_empty_items(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [], "next": None}
        )

        result = connected_connector._paginate("/donors/")

        assert result == []

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_pagination_none_response(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._paginate("/donors/")

        assert result == []

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_pagination_increments_page(self, mock_request, connected_connector):
        page1 = _make_response(
            200, {"items": [{"id": 1}], "next": "page2"}
        )
        page2 = _make_response(
            200, {"items": [{"id": 2}], "next": None}
        )
        mock_request.side_effect = [page1, page2]

        connected_connector._paginate("/donors/")

        first_page = mock_request.call_args_list[0].kwargs["params"]["page"]
        second_page = mock_request.call_args_list[1].kwargs["params"]["page"]
        assert second_page == first_page + 1


# ── System ────────────────────────────────────────────────────────────


class TestSystem:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_ping(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"status": "ok"})

        result = connected_connector.ping()

        assert result["status"] == "ok"
        assert "/ping/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_server_time(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"server_time": "2026-02-26T12:00:00Z"}
        )

        result = connected_connector.get_server_time()

        assert result["server_time"] == "2026-02-26T12:00:00Z"
        assert "/server-time/" in mock_request.call_args[0][1]


# ── Donors ────────────────────────────────────────────────────────────


class TestDonors:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_search_donors(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "items": [
                    {"id": 1, "last_name": "Smith"},
                    {"id": 2, "last_name": "Smith"},
                ],
                "next": None,
            },
        )

        result = connected_connector.search_donors(last_name="Smith")

        assert len(result) == 2
        params = mock_request.call_args.kwargs["params"]
        assert params["last_name"] == "Smith"

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_donor(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": 12345, "first_name": "Jane", "last_name": "Doe"}
        )

        result = connected_connector.get_donor(12345)

        assert result["id"] == 12345
        assert "/donors/12345/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_create_donor(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"id": 99999, "first_name": "New", "last_name": "Donor"}
        )

        result = connected_connector.create_donor(
            first_name="New", last_name="Donor", email="new@example.com"
        )

        assert result["id"] == 99999
        body = mock_request.call_args.kwargs["json"]
        assert body["first_name"] == "New"
        assert body["email"] == "new@example.com"

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_update_donor(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"id": 12345, "email": "updated@example.com"}
        )

        result = connected_connector.update_donor(12345, email="updated@example.com")

        assert result["email"] == "updated@example.com"
        assert mock_request.call_args[0][0] == "PATCH"
        assert "/donors/12345/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_donor_flextable(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"table_name": "custom_fields", "fields": []}
        )

        result = connected_connector.get_donor_flextable(12345, "custom_fields")

        assert result["table_name"] == "custom_fields"
        assert "/donors/12345/flextables/custom_fields/" in mock_request.call_args[0][1]


# ── Donations ─────────────────────────────────────────────────────────


class TestDonations:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_donation_summary(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"total_amount": 500.00, "gift_count": 5}
        )

        result = connected_connector.get_donation_summary(12345)

        assert result["total_amount"] == 500.00
        assert "/donors/12345/donations/summary/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_donations(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "items": [{"txn_id": 1, "amount": 100.00}],
                "next": None,
            },
        )

        result = connected_connector.list_donations(12345)

        assert len(result) == 1
        assert "/donors/12345/donations/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_donation(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"txn_id": 67890, "amount": 100.00}
        )

        result = connected_connector.get_donation(12345, 67890)

        assert result["txn_id"] == 67890
        assert "/donors/12345/donations/67890/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_create_donation(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"txn_id": 99001, "amount": 50.00}
        )

        result = connected_connector.create_donation(12345, amount=50.00, fund_code="GEN")

        assert result["txn_id"] == 99001
        assert mock_request.call_args[0][0] == "POST"
        body = mock_request.call_args.kwargs["json"]
        assert body["amount"] == 50.00

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_add_donation_flag(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201, {"flag_code": "MATCH"})

        result = connected_connector.add_donation_flag(12345, 67890, flag_code="MATCH")

        assert result["flag_code"] == "MATCH"
        assert "/donors/12345/donations/67890/flags/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_related_transactions(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"rel_id": 11111}], "next": None}
        )

        result = connected_connector.get_related_transactions(12345, 67890)

        assert len(result) == 1
        assert "/donors/12345/donations/67890/related/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_related_transaction(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"rel_id": 11111})

        result = connected_connector.get_related_transaction(12345, 67890, 11111)

        assert result["rel_id"] == 11111
        assert "/donors/12345/donations/67890/related/11111/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_honoree_transactions(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"txn_id": 22222}], "next": None}
        )

        result = connected_connector.get_honoree_transactions(12345)

        assert len(result) == 1
        assert "/donors/12345/honoree-transactions/" in mock_request.call_args[0][1]


# ── Pledges ───────────────────────────────────────────────────────────


class TestPledges:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_pledges(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"pledge_id": 999}], "next": None}
        )

        result = connected_connector.list_pledges(12345)

        assert len(result) == 1
        assert "/donors/12345/pledges/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_pledge(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"pledge_id": 999, "amount": 50.00}
        )

        result = connected_connector.get_pledge(12345, 999)

        assert result["pledge_id"] == 999
        assert "/donors/12345/pledges/999/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_create_pledge(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"pledge_id": 1001, "amount": 25.00}
        )

        result = connected_connector.create_pledge(
            12345, amount=25.00, frequency="monthly"
        )

        assert result["pledge_id"] == 1001
        body = mock_request.call_args.kwargs["json"]
        assert body["frequency"] == "monthly"

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_update_pledge(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"pledge_id": 999, "amount": 75.00}
        )

        result = connected_connector.update_pledge(12345, 999, amount=75.00)

        assert result["amount"] == 75.00
        assert mock_request.call_args[0][0] == "PATCH"

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_add_pledge_flag(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201, {"flag_code": "PAUSE"})

        result = connected_connector.add_pledge_flag(12345, 999, flag_code="PAUSE")

        assert result["flag_code"] == "PAUSE"
        assert "/donors/12345/pledges/999/flags/" in mock_request.call_args[0][1]


# ── Payment Tokens ────────────────────────────────────────────────────


class TestPaymentTokens:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_payment_tokens(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"token_id": 777}], "next": None}
        )

        result = connected_connector.list_payment_tokens(12345)

        assert len(result) == 1
        assert "/donors/12345/payment-tokens/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_payment_token(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"token_id": 777, "last_four": "4242"}
        )

        result = connected_connector.get_payment_token(12345, 777)

        assert result["last_four"] == "4242"
        assert "/donors/12345/payment-tokens/777/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_create_payment_token(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"token_id": 888, "last_four": "1234"}
        )

        result = connected_connector.create_payment_token(
            12345, token="tok_abc", type="credit_card", last_four="1234"
        )

        assert result["token_id"] == 888

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_update_payment_token(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"token_id": 777, "is_default": True}
        )

        result = connected_connector.update_payment_token(12345, 777, is_default=True)

        assert result["is_default"] is True
        assert mock_request.call_args[0][0] == "PATCH"


# ── Contact Info ──────────────────────────────────────────────────────


class TestContactInfo:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_primary_address(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"street": "123 Main St", "city": "Washington", "state": "DC"}
        )

        result = connected_connector.get_primary_address(12345)

        assert result["city"] == "Washington"
        assert "/donors/12345/primary-address/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_other_addresses(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"street": "456 Oak Ave"}], "next": None}
        )

        result = connected_connector.list_other_addresses(12345)

        assert len(result) == 1
        assert "/donors/12345/addresses/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_emails(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "items": [
                    {"address": "jane@example.com", "is_primary": True},
                ],
                "next": None,
            },
        )

        result = connected_connector.list_emails(12345)

        assert len(result) == 1
        assert result[0]["address"] == "jane@example.com"
        assert "/donors/12345/emails/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_phones(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "items": [{"number": "202-555-1234", "type": "home"}],
                "next": None,
            },
        )

        result = connected_connector.list_phones(12345)

        assert len(result) == 1
        assert "/donors/12345/phones/" in mock_request.call_args[0][1]


# ── Comments & Flags ──────────────────────────────────────────────────


class TestCommentsAndFlags:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_comments(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {"items": [{"comment_id": 555, "text": "Called to update."}], "next": None},
        )

        result = connected_connector.list_comments(12345)

        assert len(result) == 1
        assert "/donors/12345/comments/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_add_comment(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"comment_id": 556, "text": "New note."}
        )

        result = connected_connector.add_comment(12345, text="New note.")

        assert result["comment_id"] == 556
        body = mock_request.call_args.kwargs["json"]
        assert body["text"] == "New note."

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_comment(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"comment_id": 555, "text": "Called to update."}
        )

        result = connected_connector.get_comment(12345, 555)

        assert result["comment_id"] == 555
        assert "/donors/12345/comments/555/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_donor_flags(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"flag_code": "VIP"}], "next": None}
        )

        result = connected_connector.list_donor_flags(12345)

        assert len(result) == 1
        assert "/donors/12345/flags/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_add_donor_flag(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(201, {"flag_code": "VIP"})

        result = connected_connector.add_donor_flag(12345, flag_code="VIP")

        assert result["flag_code"] == "VIP"
        assert mock_request.call_args[0][0] == "POST"


# ── Memberships ───────────────────────────────────────────────────────


class TestMemberships:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_memberships(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"membership_id": 888}], "next": None}
        )

        result = connected_connector.list_memberships(12345)

        assert len(result) == 1
        assert "/donors/12345/memberships/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_membership(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"membership_id": 888, "level": "Gold"}
        )

        result = connected_connector.get_membership(12345, 888)

        assert result["level"] == "Gold"
        assert "/donors/12345/memberships/888/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_submemberships(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"sub_id": 444}], "next": None}
        )

        result = connected_connector.list_submemberships(12345)

        assert len(result) == 1
        assert "/donors/12345/submemberships/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_mvault(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"mvault_id": "MV12345", "status": "active"}
        )

        result = connected_connector.get_mvault(12345)

        assert result["mvault_id"] == "MV12345"
        assert "/donors/12345/mvault/" in mock_request.call_args[0][1]


# ── Orders ────────────────────────────────────────────────────────────


class TestOrders:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_list_orders(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"items": [{"order_id": 333}], "next": None}
        )

        result = connected_connector.list_orders(12345)

        assert len(result) == 1
        assert "/donors/12345/orders/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_order(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"order_id": 333, "product_code": "TSHIRT"}
        )

        result = connected_connector.get_order(12345, 333)

        assert result["order_id"] == 333
        assert "/donors/12345/orders/333/" in mock_request.call_args[0][1]

    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_create_order(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            201, {"order_id": 334, "product_code": "HAT"}
        )

        result = connected_connector.create_order(12345, product_code="HAT", quantity=1)

        assert result["order_id"] == 334
        body = mock_request.call_args.kwargs["json"]
        assert body["product_code"] == "HAT"


# ── Code Tables ───────────────────────────────────────────────────────


class TestCodeTables:
    @patch("ccef_connections.connectors.roi_crm.requests.request")
    def test_get_codes(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200,
            {
                "items": [
                    {"code": "GEN", "description": "General Fund"},
                    {"code": "CAP", "description": "Capital Campaign"},
                ],
                "next": None,
            },
        )

        result = connected_connector.get_codes("donations")

        assert len(result) == 2
        assert result[0]["code"] == "GEN"
        assert "/codes/donations/" in mock_request.call_args[0][1]


# ── Credentials ───────────────────────────────────────────────────────


class TestCredentials:
    def _make_manager(self):
        """Create a fresh CredentialManager instance, bypassing the singleton."""
        from ccef_connections.core.credentials import CredentialManager

        mgr = object.__new__(CredentialManager)
        mgr._credentials_cache = {}
        mgr._env_loaded = True
        return mgr

    def test_get_roi_crm_credentials_success(self):
        creds_json = json.dumps(FAKE_CREDS)
        with patch.dict("os.environ", {"ROI_CRM_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            result = cm.get_roi_crm_credentials()

        assert result["client_id"] == "test-client-id"
        assert result["client_secret"] == "test-client-secret"
        assert result["audience"] == "https://app.roicrm.net/api/1.0"
        assert result["roi_client_code"] == "TEST_ORG"

    def test_get_roi_crm_credentials_missing_key(self):
        creds_json = json.dumps(
            {"client_id": "x", "client_secret": "y", "audience": "z"}
        )
        with patch.dict("os.environ", {"ROI_CRM_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="roi_client_code"):
                cm.get_roi_crm_credentials()

    def test_get_roi_crm_credentials_invalid_json(self):
        with patch.dict("os.environ", {"ROI_CRM_CREDENTIALS_PASSWORD": "not-json"}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="valid JSON"):
                cm.get_roi_crm_credentials()

    def test_get_roi_crm_credentials_missing_env(self):
        with patch.dict("os.environ", {}, clear=True):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="ROI_CRM_CREDENTIALS_PASSWORD"):
                cm.get_roi_crm_credentials()

    def test_get_roi_crm_credentials_multiple_missing_keys(self):
        creds_json = json.dumps({"client_id": "only-one-key"})
        with patch.dict("os.environ", {"ROI_CRM_CREDENTIALS_PASSWORD": creds_json}):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="client_secret"):
                cm.get_roi_crm_credentials()
