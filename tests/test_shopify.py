"""Tests for the Shopify connector."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors import shopify as sh
from ccef_connections.connectors.shopify import ShopifyConnector
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

CREDS = {"shop": "example-store", "client_id": "cid-TEST", "client_secret": "sec-TEST"}
POST = "ccef_connections.connectors.shopify.requests.post"


def _resp(status=200, body=None, text="", headers=None):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.text = text
    r.headers = headers or {}
    r.json.return_value = body if body is not None else {}
    return r


def _token(scope="read_orders,read_customers"):
    return _resp(body={"access_token": "tok", "expires_in": 86399, "scope": scope})


def _order(n, line_items=1, more=False, cursor=None, ref="rREF"):
    return {"id": f"gid://shopify/Order/{n}", "legacyResourceId": str(n), "name": f"#CC{n}",
            "createdAt": "2026-11-05T01:43:27Z",
            "totalPriceSet": {"shopMoney": {"amount": "12.50", "currencyCode": "USD"}},
            "transactions": [{"kind": "AUTHORIZATION", "paymentId": ref},
                             {"kind": "CAPTURE", "paymentId": ref}],
            "lineItems": {"pageInfo": {"hasNextPage": more, "endCursor": cursor},
                          "nodes": [{"name": f"item {i}"} for i in range(line_items)]}}


def _page(nodes, more=False, cursor=None):
    return _resp(body={"data": {"orders": {"pageInfo": {"hasNextPage": more, "endCursor": cursor},
                                           "nodes": nodes}}})


@pytest.fixture(autouse=True)
def _clear_credential_cache():
    CredentialManager().clear_cache()
    yield
    CredentialManager().clear_cache()


def _connector(scopes=("read_orders",)):
    c = ShopifyConnector()
    c._creds = dict(CREDS)
    c._token = "tok"
    c._token_expires = 1e12
    c.scopes = list(scopes)
    c._is_connected = True
    return c


# ── credentials ───────────────────────────────────────────────────────

class TestCredentials:
    def test_canonical(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", json.dumps(CREDS))
        assert CredentialManager().get_shopify_credentials() == CREDS

    def test_spaced_keys_and_full_domain(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", json.dumps(
            {"shop": "https://Example-Store.myshopify.com/", "client id": "a", "client secret": "b"}))
        assert CredentialManager().get_shopify_credentials() == {
            "shop": "example-store", "client_id": "a", "client_secret": "b"}

    def test_same_value_twice_refused(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD",
                           json.dumps({"shop": "s", "client_id": "x", "client_secret": "x"}))
        with pytest.raises(CredentialError, match="same value"):
            CredentialManager().get_shopify_credentials()

    def test_missing_shop(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD",
                           json.dumps({"client_id": "a", "client_secret": "b"}))
        with pytest.raises(CredentialError, match="needs shop"):
            CredentialManager().get_shopify_credentials()

    def test_not_json(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", "not json")
        with pytest.raises(CredentialError, match="not JSON"):
            CredentialManager().get_shopify_credentials()


# ── connect ───────────────────────────────────────────────────────────

class TestConnect:
    def test_client_credentials_exchange(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch(POST, return_value=_token()) as post:
            c = ShopifyConnector()
            c.connect()
        assert c.is_connected()
        assert post.call_args.args[0] == "https://example-store.myshopify.com/admin/oauth/access_token"
        assert post.call_args.kwargs["data"] == {"grant_type": "client_credentials",
                                                 "client_id": "cid-TEST", "client_secret": "sec-TEST"}
        assert c.scopes == ["read_orders", "read_customers"]
        assert c.reads_all_orders is False

    def test_all_orders_scope(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch(POST, return_value=_token("read_orders,read_all_orders")):
            c = ShopifyConnector()
            c.connect()
        assert c.reads_all_orders is True

    def test_other_organization(self, monkeypatch):
        monkeypatch.setenv("SHOPIFY_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch(POST, return_value=_resp(400, {"error": "shop_not_permitted"})):
            with pytest.raises(AuthenticationError, match="different Shopify organizations"):
                ShopifyConnector().connect()

    def test_expired_token_refreshes(self):
        c = _connector()
        c._token_expires = 0
        with patch(POST, side_effect=[_token(), _resp(body={"data": {"shop": {"name": "x"}}})]) as post:
            assert c.graphql("{ shop { name } }") == {"shop": {"name": "x"}}
        assert post.call_args_list[0].args[0].endswith("/admin/oauth/access_token")
        assert post.call_args_list[1].kwargs["headers"]["X-Shopify-Access-Token"] == "tok"


# ── graphql errors ────────────────────────────────────────────────────

class TestGraphql:
    def test_url_and_version(self):
        c = _connector()
        with patch(POST, return_value=_resp(body={"data": {"ok": 1}})) as post:
            c.graphql("{ ok }", {"a": 1})
        assert post.call_args.args[0] == \
            f"https://example-store.myshopify.com/admin/api/{sh.API_VERSION}/graphql.json"
        assert post.call_args.kwargs["json"] == {"query": "{ ok }", "variables": {"a": 1}}

    def test_throttled_is_retried(self):
        c = _connector()
        throttled = _resp(body={"errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}],
                                "extensions": {"cost": {"throttleStatus": {"currentlyAvailable": 0}}}})
        with patch("tenacity.nap.time.sleep"), \
                patch(POST, side_effect=[throttled, _resp(body={"data": {"ok": 1}})]):
            assert c.graphql("{ ok }") == {"ok": 1}

    def test_throttled_gives_up(self):
        c = _connector()
        throttled = _resp(body={"errors": [{"message": "Throttled", "extensions": {"code": "THROTTLED"}}]})
        with patch("tenacity.nap.time.sleep"), patch(POST, return_value=throttled):
            with pytest.raises(RateLimitError):
                c.graphql("{ ok }")

    def test_access_denied(self):
        c = _connector()
        denied = _resp(body={"errors": [{"message": "no", "extensions": {"code": "ACCESS_DENIED"}}]})
        with patch(POST, return_value=denied) as post:
            with pytest.raises(AuthenticationError, match="scope"):
                c.graphql("{ ok }")
        assert post.call_count == 1   # never retried

    def test_other_graphql_error_not_retried(self):
        c = _connector()
        bad = _resp(body={"errors": [{"message": "Field 'x' doesn't exist",
                                      "extensions": {"code": "undefinedField"}}]})
        with patch(POST, return_value=bad) as post:
            with pytest.raises(ConnectionError, match="doesn't exist"):
                c.graphql("{ x }")
        assert post.call_count == 1

    def test_http_401(self):
        c = _connector()
        with patch(POST, return_value=_resp(401, text="Invalid API key")):
            with pytest.raises(AuthenticationError):
                c.graphql("{ ok }")


# ── orders ────────────────────────────────────────────────────────────

class TestOrders:
    def test_pages_and_flattens(self):
        c = _connector()
        with patch(POST, side_effect=[_page([_order(1, 2)], more=True, cursor="c1"),
                                      _page([_order(2)])]) as post:
            got = c.orders(created_since="2026-10-19", page_size=1)
        assert [o["name"] for o in got] == ["#CC1", "#CC2"]
        assert got[0]["lineItems"] == [{"name": "item 0"}, {"name": "item 1"}]
        v1 = post.call_args_list[0].kwargs["json"]["variables"]
        assert v1 == {"first": 1, "after": None, "q": "created_at:>='2026-10-19'"}
        assert post.call_args_list[1].kwargs["json"]["variables"]["after"] == "c1"

    def test_line_item_overflow_fetched(self):
        c = _connector()
        more = _resp(body={"data": {"order": {"lineItems": {
            "pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": [{"name": "late"}]}}}})
        with patch(POST, side_effect=[_page([_order(1, 1, more=True, cursor="li1")]), more]) as post:
            got = c.orders()
        assert [i["name"] for i in got[0]["lineItems"]] == ["item 0", "late"]
        assert post.call_args_list[1].kwargs["json"]["variables"] == {
            "id": "gid://shopify/Order/1", "after": "li1"}

    def test_query_has_no_unfilled_placeholder(self):
        c = _connector()
        with patch(POST, return_value=_page([])) as post:
            c.orders()
        q = post.call_args.kwargs["json"]["query"]
        assert "{LINE_ITEM}" not in q and "vendor" in q and "paymentId" in q

    def test_count(self):
        c = _connector()
        with patch(POST, return_value=_resp(body={"data": {"ordersCount": {"count": 737}}})):
            assert c.orders_count() == 737


# ── helpers ───────────────────────────────────────────────────────────

class TestHelpers:
    def test_money(self):
        assert sh.money({"shopMoney": {"amount": "12.50"}}) == Decimal("12.50")
        assert sh.money({"amount": "3"}) == Decimal("3")
        assert sh.money(None) == Decimal("0")

    def test_payment_references_distinct(self):
        assert sh.payment_references(_order(1, ref="rABC")) == ["rABC"]
        assert sh.payment_references({"transactions": [{"paymentId": None}]}) == []

    def test_order_search(self):
        assert sh.order_search() is None
        assert sh.order_search(created_since="2026-10-19", updated_since="2026-11-01",
                               extra="financial_status:paid") == \
            "created_at:>='2026-10-19' AND updated_at:>='2026-11-01' AND (financial_status:paid)"

    def test_as_datetime_is_utc(self):
        d = sh.as_datetime("2026-11-05T01:43:27Z")
        assert d.utcoffset().total_seconds() == 0 and d.hour == 1
