"""
Shopify connector for CCEF connections library.

Read access to a store's **orders** through the GraphQL Admin API, using direct HTTP via
the requests library -- no Shopify SDK, so this connector needs only the base install.
Built for roi-gift-processing's "Gift Processing - Shopify" (the online store since
October 2026), replacing the Orders CSV export Joe downloaded by hand.

⚠ **Auth is the client credentials grant** (Dev Dashboard apps, since Shopify stopped
new legacy custom apps on 2026-01-01). The app's Client ID and secret are exchanged at
``/admin/oauth/access_token`` for a token that lasts **24 hours**; there is no long-lived
token to copy. It only works when the app and the store belong to the **same Shopify
organization** -- otherwise the exchange fails with ``shop_not_permitted``.

⚠ **Without ``read_all_orders`` only the last 60 days of orders are visible** (by creation
date). CCEF's app could not be given that scope (2026-11-13), so a caller that needs
history must keep its own copy and read daily. :attr:`ShopifyConnector.reads_all_orders`
says which you have.

⚠ **Customer fields need "protected customer data" access** on the app. Without it,
``email``, names and addresses come back null with no error.

⚠ **Rate limits are by query COST**, not request count: a leaky bucket per app and store.
An empty bucket is a ``THROTTLED`` error inside an HTTP 200, raised here as
RateLimitError and retried. Keep ``page_size`` small when a query nests a connection
(line items): a query's cost is roughly page_size x nested first.

⚠ **Timestamps are UTC ISO strings** (``2026-11-05T01:43:27Z``). The store's local date
(what the Orders export shows) needs converting.

Field notes (verified 2026-11-13 against the CCEF store, all 22 own orders):
    legacyResourceId                the export's "Id"
    name                            the export's "Name" (#CC1001)
    transactions[].paymentId        the export's "Payment Reference" (r...)
    paymentGatewayNames             ["shopify_payments"], or ["manual"] on orders copied in
"""

import logging
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_shopify_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

API_VERSION = "2025-10"
ALL_ORDERS_SCOPE = "read_all_orders"
# Nested connections are paged inside the order when they overflow this.
LINE_ITEMS_FIRST = 50

DateLike = Union[date, datetime, str]

_MONEY = "shopMoney { amount currencyCode }"
_ADDRESS = ("name firstName lastName company address1 address2 city province provinceCode "
            "zip country countryCodeV2 phone")

# Everything the Orders export carries that a gift process uses, and a little more.
ORDER_FIELDS = f"""
    id legacyResourceId name number email phone
    createdAt processedAt updatedAt cancelledAt cancelReason closedAt test
    displayFinancialStatus displayFulfillmentStatus currencyCode customerAcceptsMarketing
    subtotalPriceSet {{ {_MONEY} }} totalShippingPriceSet {{ {_MONEY} }} totalTaxSet {{ {_MONEY} }}
    totalPriceSet {{ {_MONEY} }} totalDiscountsSet {{ {_MONEY} }} totalRefundedSet {{ {_MONEY} }}
    totalOutstandingSet {{ {_MONEY} }}
    discountCodes note tags sourceName paymentGatewayNames
    customAttributes {{ key value }}
    billingAddress {{ {_ADDRESS} }}
    shippingAddress {{ {_ADDRESS} }}
    shippingLine {{ title }}
    taxLines {{ title rate priceSet {{ {_MONEY} }} }}
    customer {{ id }}
    transactions {{ gateway kind status paymentId processedAt amountSet {{ {_MONEY} }} }}
    refunds {{ createdAt totalRefundedSet {{ {_MONEY} }} }}
    lineItems(first: {LINE_ITEMS_FIRST}) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {{LINE_ITEM}} }}
    }}
"""
LINE_ITEM_FIELDS = (f"name title variantTitle quantity sku vendor requiresShipping taxable "
                    f"originalUnitPriceSet {{ {_MONEY} }} discountedTotalSet {{ {_MONEY} }} "
                    f"totalDiscountSet {{ {_MONEY} }}")


# ── Order helpers (pure; no connection needed) ───────────────────────────

def money(block: Optional[Dict[str, Any]]) -> Decimal:
    """A money set (``{"shopMoney": {"amount": ...}}``) or bag as a Decimal; absent -> 0."""
    b = (block or {}).get("shopMoney", block) or {}
    v = b.get("amount")
    return Decimal(str(v)) if v not in (None, "") else Decimal("0")


def payment_references(order: Dict[str, Any]) -> List[str]:
    """The order's distinct transaction ``paymentId`` values (the export's Payment
    Reference), in transaction order."""
    out: List[str] = []
    for t in order.get("transactions") or []:
        p = t.get("paymentId")
        if p and p not in out:
            out.append(p)
    return out


def as_datetime(value: str) -> datetime:
    """A Shopify UTC timestamp (``...Z``) as an aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _search_date(value: DateLike) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def order_search(created_since: Optional[DateLike] = None,
                 updated_since: Optional[DateLike] = None,
                 extra: Optional[str] = None) -> Optional[str]:
    """A Shopify search string for :meth:`ShopifyConnector.orders`, e.g.
    ``created_at:>='2026-10-19' AND updated_at:>='2026-11-01'``. None when empty."""
    parts = []
    if created_since is not None:
        parts.append(f"created_at:>='{_search_date(created_since)}'")
    if updated_since is not None:
        parts.append(f"updated_at:>='{_search_date(updated_since)}'")
    if extra:
        parts.append(f"({extra})")
    return " AND ".join(parts) or None


class ShopifyConnector(BaseConnection):
    """
    Shopify connector: read a store's orders through the GraphQL Admin API.

    **Credential**::

        SHOPIFY_API_KEY_PASSWORD={"shop":"<myshopify name>","client_id":"...","client_secret":"..."}

    Examples:
        >>> shop = ShopifyConnector()
        >>> shop.connect()                      # warns when read_all_orders is missing
        >>> recent = shop.orders(updated_since="2026-11-01")
        >>> for o in recent:
        ...     print(o["name"], o["createdAt"], money(o["totalPriceSet"]), payment_references(o))
    """

    def __init__(self, credential_name: str = "SHOPIFY_API_KEY",
                 api_version: str = API_VERSION) -> None:
        """
        Args:
            credential_name: The credential to read ({credential_name}_PASSWORD).
            api_version: The Admin API version (Shopify supports each for a year).
        """
        super().__init__()
        self._credential_name = credential_name
        self.api_version = api_version
        self._creds: Optional[Dict[str, str]] = None
        self._token: Optional[str] = None
        self._token_expires: float = 0.0
        self.scopes: List[str] = []

    # ── Connection lifecycle ──────────────────────────────────────────

    @property
    def shop(self) -> str:
        if not self._creds:
            raise AuthenticationError("Shopify connector is not connected -- call connect() first.")
        return self._creds["shop"]

    @property
    def reads_all_orders(self) -> bool:
        """False: only orders created in the last 60 days are visible (see the module note)."""
        return ALL_ORDERS_SCOPE in self.scopes

    def connect(self) -> None:
        """
        Load the credential and get an access token.

        Raises:
            AuthenticationError: If Shopify refuses the exchange (``shop_not_permitted``:
                the app and store are in different organizations).
            ConnectionError: If Shopify is unreachable
        """
        self._creds = self._credential_manager.get_shopify_credentials(self._credential_name)
        self._refresh_token()
        if not self.reads_all_orders:
            logger.warning("Shopify app lacks %s: only orders created in the last 60 days are "
                           "visible.", ALL_ORDERS_SCOPE)
        self._is_connected = True
        logger.info("Connected to Shopify store %s (API %s; scopes %s)", self.shop,
                    self.api_version, ",".join(self.scopes))

    def disconnect(self) -> None:
        """Drop the token. There is no session to close."""
        self._token = None
        self._token_expires = 0.0
        self._is_connected = False

    def health_check(self) -> bool:
        """True when the shop query answers."""
        try:
            return bool(self.graphql("{ shop { name } }").get("shop"))
        except Exception as exc:  # noqa: BLE001 - health checks report, never raise
            logger.warning("Shopify health check failed: %s", exc)
            return False

    # ── Plumbing ──────────────────────────────────────────────────────

    def _refresh_token(self) -> None:
        if not self._creds:
            raise AuthenticationError("Shopify connector is not connected -- call connect() first.")
        try:
            resp = requests.post(
                f"https://{self._creds['shop']}.myshopify.com/admin/oauth/access_token",
                data={"grant_type": "client_credentials", "client_id": self._creds["client_id"],
                      "client_secret": self._creds["client_secret"]},
                timeout=30)
        except requests.RequestException as exc:
            raise ConnectionError(f"Shopify token request failed: {exc}") from exc
        if resp.status_code != 200:
            try:
                body = resp.json()
            except ValueError:
                body = {}
            err = AuthenticationError(
                f"Shopify token exchange failed ({resp.status_code}): "
                f"{body.get('error_description') or body.get('error') or resp.text[:200]}. "
                f"'shop_not_permitted' means the app and the store are in different "
                f"Shopify organizations."
            )
            err.status_code = resp.status_code
            raise err
        body = resp.json()
        self._token = body["access_token"]
        # Refresh a minute early rather than learn about expiry from a 401 mid-walk.
        self._token_expires = time.time() + int(body.get("expires_in", 0)) - 60
        self.scopes = [s.strip() for s in str(body.get("scope", "")).split(",") if s.strip()]

    def _headers(self) -> Dict[str, str]:
        if not self._token or time.time() >= self._token_expires:
            self._refresh_token()
        return {"X-Shopify-Access-Token": self._token, "Content-Type": "application/json"}

    @retry_shopify_operation
    def graphql(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        One GraphQL Admin API call -> its ``data``.

        Raises:
            AuthenticationError: 401 or 403, or an ACCESS_DENIED error (a missing scope)
            RateLimitError: 429, or a THROTTLED error (retried)
            ConnectionError: any other error status or GraphQL error, or an unreachable API
        """
        url = f"https://{self.shop}.myshopify.com/admin/api/{self.api_version}/graphql.json"
        try:
            resp = requests.post(url, headers=self._headers(),
                                 json={"query": query, "variables": variables or {}}, timeout=120)
        except requests.RequestException as exc:
            raise ConnectionError(f"Shopify API request failed: {exc}") from exc
        if resp.status_code in (401, 403):
            err = AuthenticationError(f"Shopify refused the request ({resp.status_code}): "
                                      f"{resp.text[:300]}")
            err.status_code = resp.status_code
            raise err
        if resp.status_code == 429:
            retry_after = int(float(resp.headers.get("Retry-After", 2)))
            raise RateLimitError(f"Shopify rate limit, retry after {retry_after}s",
                                 retry_after=retry_after)
        if resp.status_code >= 400:
            raise ConnectionError(f"Shopify API error {resp.status_code}: {resp.text[:500]}")
        body = resp.json()
        errors = body.get("errors") or []
        if errors:
            codes = {((e.get("extensions") or {}).get("code") or "") for e in errors}
            if "THROTTLED" in codes:
                raise RateLimitError(f"Shopify query cost throttled: {self._throttle(body)}",
                                     retry_after=2)
            msg = "; ".join(e.get("message", "") for e in errors)[:500]
            if "ACCESS_DENIED" in codes:
                raise AuthenticationError(f"Shopify denied access (a scope missing?): {msg}")
            raise ConnectionError(f"Shopify GraphQL error: {msg}")
        return body.get("data") or {}

    @staticmethod
    def _throttle(body: Dict[str, Any]) -> str:
        st = ((body.get("extensions") or {}).get("cost") or {}).get("throttleStatus") or {}
        return (f"available {st.get('currentlyAvailable')} of {st.get('maximumAvailable')}, "
                f"restoring {st.get('restoreRate')}/s") if st else "no throttle status"

    # ── Reads ─────────────────────────────────────────────────────────

    def orders(self, query: Optional[str] = None, *,
               created_since: Optional[DateLike] = None,
               updated_since: Optional[DateLike] = None,
               page_size: int = 10) -> List[Dict[str, Any]]:
        """
        Every order matching a search, oldest first, with ORDER_FIELDS.

        Args:
            query: A Shopify order search string (``financial_status:paid``...); combined
                with the date arguments by AND. See :func:`order_search`.
            created_since: Orders created on or after this (a date is the store's day
                boundary, as Shopify reads it).
            updated_since: Orders changed on or after this: what a daily sync wants, since a
                refund or cancellation updates an old order.
            page_size: Orders per request. Each carries up to LINE_ITEMS_FIRST line items,
                so keep it small; Shopify refuses a query costing over 1000.

        Each order's ``lineItems`` is flattened to a list of line item dicts, fetched in full
        even past LINE_ITEMS_FIRST. ⚠ Without read_all_orders, nothing older than 60 days.
        """
        search = order_search(created_since, updated_since, query)
        gql = ("query($first: Int!, $after: String, $q: String) {"
               " orders(first: $first, after: $after, query: $q, sortKey: CREATED_AT) {"
               " pageInfo { hasNextPage endCursor } nodes { %s } } }"
               % ORDER_FIELDS.replace("{LINE_ITEM}", LINE_ITEM_FIELDS))
        out: List[Dict[str, Any]] = []
        after = None
        while True:
            data = self.graphql(gql, {"first": page_size, "after": after, "q": search})
            page = data["orders"]
            for o in page["nodes"]:
                li = o.get("lineItems") or {}
                items = list(li.get("nodes") or [])
                info = li.get("pageInfo") or {}
                if info.get("hasNextPage"):
                    items += self._more_line_items(o["id"], info.get("endCursor"))
                o["lineItems"] = items
                out.append(o)
            if not page["pageInfo"]["hasNextPage"]:
                return out
            after = page["pageInfo"]["endCursor"]

    def _more_line_items(self, order_gid: str, after: Optional[str]) -> List[Dict[str, Any]]:
        gql = ("query($id: ID!, $after: String) { order(id: $id) {"
               " lineItems(first: %d, after: $after) { pageInfo { hasNextPage endCursor }"
               " nodes { %s } } } }" % (LINE_ITEMS_FIRST, LINE_ITEM_FIELDS))
        items: List[Dict[str, Any]] = []
        while True:
            li = self.graphql(gql, {"id": order_gid, "after": after})["order"]["lineItems"]
            items += li["nodes"]
            if not li["pageInfo"]["hasNextPage"]:
                return items
            after = li["pageInfo"]["endCursor"]

    def orders_count(self, query: Optional[str] = None) -> int:
        """How many orders match a search (all visible orders when None)."""
        data = self.graphql("query($q: String) { ordersCount(query: $q, limit: null) { count } }",
                            {"q": query})
        return int(data["ordersCount"]["count"])
