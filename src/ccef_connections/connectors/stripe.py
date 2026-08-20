"""
Stripe connector for CCEF connections library.

Read access to Stripe charges, refunds, disputes, payouts and balance
transactions, using direct HTTP via the requests library — no `stripe` SDK, so
this connector needs only the base install.

⚠ **Multi-account by design.** CCEF runs several Stripe accounts side by side
(C3/Action Network, C4 main, and the online stores), and a Stripe API key is
scoped to exactly one of them. One connector instance therefore holds a *map* of
account name -> key, and every method takes an ``account=`` argument. That is
what lets a caller sweep all of them in one pass rather than juggling
connections.

⚠ **`fee` and `net` live on balance transactions, not on charges.** For any
reconciliation against a bank statement, use :meth:`list_balance_transactions`:
a charge tells you the gross, only the balance transaction tells you what Stripe
kept and what actually moved. This is the single most common way to get a Stripe
reconciliation wrong.
"""

import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_stripe_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

STRIPE_API_BASE = "https://api.stripe.com/v1"

# Stripe caps `limit` at 100 for every list endpoint.
STRIPE_MAX_LIMIT = 100

# A single connector holding one key uses this name, so callers that do not care
# about multi-account can omit `account=` entirely.
DEFAULT_ACCOUNT = "default"

TimeLike = Union[int, float, date, datetime]


def _to_unix(value: TimeLike, *, end_of_day: bool = False) -> int:
    """A Stripe `created` filter value from a datetime, date, or unix timestamp.

    ⚠ **A bare `date` is interpreted in UTC**, because that is the clock Stripe
    stamps `created` in. Passing a local-midnight datetime for a day boundary
    will silently shift the window by the UTC offset — pass a `date` and let this
    resolve it, or pass a timezone-aware datetime.

    Args:
        value: unix seconds, a `date`, or a `datetime` (naive is assumed UTC)
        end_of_day: for a `date`, take 23:59:59 instead of 00:00:00, so that
            `created[lte]` includes the whole day rather than only its first
            instant

    Returns:
        Unix timestamp in seconds
    """
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, date):
        t = (23, 59, 59) if end_of_day else (0, 0, 0)
        return int(datetime(value.year, value.month, value.day, *t,
                           tzinfo=timezone.utc).timestamp())
    raise TypeError(f"Cannot convert {type(value).__name__} to a Stripe timestamp")


class StripeConnector(BaseConnection):
    """
    Stripe connector for reconciliation-grade read access.

    **Credentials — one variable per Stripe account** (the preferred form, because
    the meta-project reseeds a fixed set of named keys and this lets each account be
    granted and rotated on its own)::

        STRIPE_C3_CREDENTIALS_PASSWORD={"api_name":"stripeclaudec3","key":"rk_live_..."}
        STRIPE_C4_CREDENTIALS_PASSWORD={"api_name":"stripeclaudec4","key":"rk_live_..."}

    The account name comes from the variable itself: ``STRIPE_C3_...`` -> ``"c3"``,
    ``STRIPE_STORE_TX_...`` -> ``"store_tx"``. A combined map, or a single bare key
    registered as ``"default"``, are also accepted — see
    :meth:`CredentialManager.get_stripe_accounts`.

    ⚠ **Restricted keys (`rk_...`) are recommended, and a least-privilege one will
    not have every permission.** Charges, Refunds, Disputes, Payouts and Balance
    transactions are what this connector reads; grant those. A missing permission
    returns **403**, which this connector reports with the resource named, because
    the Stripe dashboard is the only place to fix it.

    ⚠ **403 and 401 mean opposite things and are not conflated.** A 403 proves the
    key is genuine and merely scope-limited, so :meth:`connect` warns and continues;
    a 401 means the key itself is bad and raises. Refusing to connect over a missing
    permission would make this unusable with exactly the keys it recommends —
    verified against the real C3 key, which cannot read ``/account`` but reads all
    five data resources.

    Examples:
        >>> connector = StripeConnector()
        >>> connector.connect()
        >>> connector.accounts
        ['c3', 'c4']
        >>> for name in connector.accounts:
        ...     info = connector.get_account(name)
        ...     print(name, info["id"], info.get("settings", {}))
        >>> charges = connector.list_charges(
        ...     start=date(2026, 8, 8), end=date(2026, 8, 15), account="c3"
        ... )
    """

    def __init__(self, api_version: Optional[str] = None) -> None:
        """
        Initialize the Stripe connector.

        Args:
            api_version: Optional Stripe API version to pin (e.g. "2024-06-20").
                Left as None the account's own default version is used. ⚠ Pinning
                is worth doing for anything scheduled — an account-level version
                bump in the Stripe dashboard would otherwise change this
                connector's output with no code change — but pin a version you
                have confirmed exists, because an unknown one is rejected.
        """
        super().__init__()
        self._keys: Dict[str, str] = {}
        self._api_names: Dict[str, str] = {}
        self._scope_limited: set = set()
        self._api_version = api_version

    # ── Connection lifecycle ──────────────────────────────────────────

    def connect(self) -> None:
        """
        Load the configured Stripe keys and verify each one against the API.

        ⚠ Every configured key is checked, not just the first. A key that is
        revoked, truncated on paste, or scoped to the wrong account is otherwise
        found later by a caller who has already assumed the sweep was complete.

        Raises:
            CredentialError: If STRIPE_CREDENTIALS_PASSWORD is missing or invalid
            AuthenticationError: If any configured key is rejected
            ConnectionError: If the API is unreachable
        """
        records = self._credential_manager.get_stripe_accounts()
        self._keys = {n: r["key"] for n, r in records.items()}
        self._api_names = {n: r.get("api_name", "") for n, r in records.items()}
        self._scope_limited = set()
        for name in sorted(self._keys):
            try:
                info = self._request("GET", "/account", account=name)
                logger.info(
                    "Stripe account %r verified: stripe_id=%s api_name=%s live_key=%s",
                    name, info.get("id"), self._api_names.get(name) or "(unnamed)",
                    self._is_live(name),
                )
            except AuthenticationError as e:
                # ⚠ A 403 here is NOT a failure. It proves the key authenticated;
                # the probe endpoint simply is not in its permissions. Least-
                # privilege restricted keys routinely lack `/account`, and refusing
                # to connect over that would defeat the point of recommending them.
                # A 401 is a genuinely bad key and still raises.
                if getattr(e, "status_code", None) != 403:
                    raise
                self._scope_limited.add(name)
                logger.warning(
                    "Stripe account %r: key authenticated but cannot read /account "
                    "(no permission). Connected anyway — grant 'Account' read in the "
                    "Stripe dashboard if you want identity checks. api_name=%s",
                    name, self._api_names.get(name) or "(unnamed)",
                )
        self._is_connected = True
        logger.info("Successfully connected to Stripe (%d account(s))", len(self._keys))

    def disconnect(self) -> None:
        """Forget the loaded keys."""
        self._keys = {}
        self._api_names = {}
        self._scope_limited = set()
        self._is_connected = False
        logger.debug("Disconnected from Stripe")

    def health_check(self) -> bool:
        """
        Check that every configured account still answers.

        Returns:
            True if connected and all accounts are reachable, False otherwise
        """
        if not self._is_connected or not self._keys:
            return False
        for name in self._keys:
            try:
                self._request("GET", "/account", account=name)
            except AuthenticationError as e:
                # Same reasoning as connect(): a 403 means reachable-and-authenticated,
                # so it is healthy. Only a 401 or a transport failure is not.
                if getattr(e, "status_code", None) != 403:
                    return False
            except Exception:
                return False
        return True

    # ── Accounts ──────────────────────────────────────────────────────

    @property
    def accounts(self) -> List[str]:
        """The configured account names, sorted."""
        return sorted(self._keys)

    def api_name(self, account: Optional[str] = None) -> str:
        """
        The `api_name` recorded alongside this account's key, or "".

        Useful for reconciling "which key is this?" against the Stripe dashboard,
        where the key is listed under the name you gave it.
        """
        if account is None and len(self._api_names) == 1:
            return next(iter(self._api_names.values()))
        return self._api_names.get(account or "", "")

    def _is_live(self, account: Optional[str] = None) -> bool:
        """Whether this account's key is a live-mode key, read off its prefix."""
        return "_live_" in self._key_for(account)

    def _key_for(self, account: Optional[str]) -> str:
        """
        Resolve an account name to its key.

        ⚠ With more than one account configured, omitting `account=` is an error
        rather than a silent pick of the first. Reconciling the wrong entity's
        Stripe data against a bank statement is exactly the class of mistake this
        library exists to prevent.
        """
        if not self._keys:
            self.connect()
        if account is None:
            if len(self._keys) == 1:
                return next(iter(self._keys.values()))
            raise ValueError(
                f"account= is required: {len(self._keys)} Stripe accounts are "
                f"configured ({', '.join(self.accounts)}). Pass one explicitly."
            )
        try:
            return self._keys[account]
        except KeyError:
            raise ValueError(
                f"Unknown Stripe account {account!r}. Configured: "
                f"{', '.join(self.accounts) or '(none)'}"
            ) from None

    @retry_stripe_operation
    def get_account(self, account: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve the Stripe account a key belongs to.

        Use this to answer "which of our accounts is this key for?" — the `id`
        it returns is the authoritative answer, and is worth recording alongside
        whatever name you gave the key.

        Args:
            account: Configured account name

        Returns:
            The Stripe Account object

        Examples:
            >>> connector.get_account("c3")["id"]
            'acct_1BWFYb...'
        """
        return self._request("GET", "/account", account=account)

    # ── HTTP plumbing ─────────────────────────────────────────────────

    def _headers(self, account: Optional[str]) -> Dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._key_for(account)}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        if self._api_version:
            headers["Stripe-Version"] = self._api_version
        return headers

    def _request(
        self,
        method: str,
        path: str,
        account: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make one Stripe API request.

        Raises:
            AuthenticationError: 401, or 403 for a missing restricted-key scope
            RateLimitError: 429
            ConnectionError: any other error status, or an unreachable API
        """
        url = f"{STRIPE_API_BASE}{path}"
        try:
            resp = requests.request(
                method, url, headers=self._headers(account),
                params=params, timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Stripe API request failed: {e}") from e

        if resp.status_code == 401:
            err = AuthenticationError(
                f"Stripe rejected the key for account "
                f"{account or DEFAULT_ACCOUNT!r}. A key truncated on paste looks "
                f"identical to a revoked one here — check its full length in the "
                f"Stripe dashboard."
            )
            err.status_code = 401
            raise err
        if resp.status_code == 403:
            # ⚠ 401 and 403 mean opposite things here and must not be conflated.
            # 401 = the key is bad. 403 = the key is GENUINE and authenticated, and
            # merely lacks a permission for this resource — fixable only in the
            # Stripe dashboard. `status_code` is attached so connect() can tell the
            # two apart: refusing to connect over a missing scope would make this
            # connector unusable with exactly the least-privilege keys it
            # recommends.
            err = AuthenticationError(
                f"Stripe refused {method} {path} for account "
                f"{account or DEFAULT_ACCOUNT!r} (403). A restricted key needs a "
                f"read permission for this resource; grant it in the Stripe "
                f"dashboard under the key's permissions. Detail: {resp.text[:300]}"
            )
            err.status_code = 403
            raise err
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            raise RateLimitError(
                f"Stripe rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )
        if resp.status_code >= 400:
            raise ConnectionError(
                f"Stripe API error {resp.status_code} on {method} {path}: "
                f"{resp.text[:500]}"
            )
        return resp.json()

    @retry_stripe_operation
    def _page(self, path: str, account: Optional[str],
              params: Dict[str, Any]) -> Dict[str, Any]:
        """One page of a list endpoint, retried on rate limit."""
        return self._request("GET", path, account=account, params=params)

    def _paginate(
        self,
        path: str,
        account: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Walk a Stripe list endpoint to completion.

        Stripe paginates by cursor: ``starting_after`` the last id seen, with
        ``has_more`` telling you whether to continue. ⚠ It is **not** page
        numbers, and it is not safe to stop early on a short page — only
        ``has_more`` is authoritative.

        Args:
            path: API path, e.g. "/charges"
            account: Configured account name
            params: Filters to pass through
            limit: Stop after roughly this many records (None = all)

        Returns:
            Every matching record, oldest-to-newest as Stripe returns them
        """
        out: List[Dict[str, Any]] = []
        page_params = dict(params or {})
        page_params["limit"] = min(STRIPE_MAX_LIMIT, limit or STRIPE_MAX_LIMIT)

        while True:
            data = self._page(path, account, dict(page_params))
            batch = data.get("data", [])
            out.extend(batch)

            if limit is not None and len(out) >= limit:
                return out[:limit]
            if not data.get("has_more") or not batch:
                return out
            page_params["starting_after"] = batch[-1]["id"]

    @staticmethod
    def _window(
        start: Optional[TimeLike], end: Optional[TimeLike],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add Stripe's `created[gte]` / `created[lte]` filters to `params`."""
        if start is not None:
            params["created[gte]"] = _to_unix(start)
        if end is not None:
            params["created[lte]"] = _to_unix(end, end_of_day=True)
        return params

    # ── Reconciliation reads ──────────────────────────────────────────

    def list_balance_transactions(
        self,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        account: Optional[str] = None,
        types: Optional[Iterable[str]] = None,
        payout: Optional[str] = None,
        expand_source: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Balance transactions — **the closest thing to Stripe's reconciliation report.**

        This is the endpoint to reconcile against a bank statement. Each record
        carries ``amount``, ``fee``, ``net`` and ``fee_details``, so it answers
        what Stripe kept as well as what the donor gave. Charges alone cannot:
        they have no fee, and they miss payouts, platform fees and adjustments
        entirely.

        Args:
            start: Earliest `created` (inclusive)
            end: Latest `created` (inclusive; a bare `date` covers the whole day)
            account: Configured account name
            types: Restrict to Stripe types, e.g. ``("charge", "refund",
                "payout", "stripe_fee", "adjustment")``. Stripe filters on one
                type per request, so several are fetched and merged here.
            payout: Only transactions paid out in this payout id — the direct way
                to itemise a single bank deposit
            expand_source: Expand the underlying charge/refund object. Costs
                nothing extra in requests and is how you reach a donor email.
            limit: Stop after roughly this many records

        Returns:
            Balance transaction objects

        Examples:
            >>> # everything that settled in one bank deposit
            >>> connector.list_balance_transactions(payout="po_1Abc", account="c3")
        """
        base: Dict[str, Any] = {}
        if payout:
            base["payout"] = payout
        if expand_source:
            base["expand[]"] = "data.source"
        self._window(start, end, base)

        if not types:
            return self._paginate("/balance_transactions", account, base, limit)

        merged: List[Dict[str, Any]] = []
        seen: set = set()
        for t in types:
            for row in self._paginate(
                "/balance_transactions", account, {**base, "type": t}, limit
            ):
                if row["id"] not in seen:
                    seen.add(row["id"])
                    merged.append(row)
        merged.sort(key=lambda r: r.get("created", 0))
        return merged[:limit] if limit is not None else merged

    def list_charges(
        self,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        account: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Charges in a window — the donation side, for "did this reach the CRM?".

        ⚠ Carries **no fee**. Pair with :meth:`list_balance_transactions` for
        anything that has to tie to a bank figure.

        ⚠ Includes **failed and uncaptured** charges. Filter on
        ``c["paid"] and c["status"] == "succeeded"`` before treating one as a
        donation, or a declined card becomes a missing gift.

        Args:
            start: Earliest `created` (inclusive)
            end: Latest `created` (inclusive)
            account: Configured account name
            limit: Stop after roughly this many records

        Returns:
            Charge objects
        """
        return self._paginate("/charges", account, self._window(start, end, {}), limit)

    def list_refunds(
        self,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        account: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Refunds in a window. Each carries ``charge``, the id being refunded.

        Args:
            start: Earliest `created` (inclusive)
            end: Latest `created` (inclusive)
            account: Configured account name
            limit: Stop after roughly this many records

        Returns:
            Refund objects
        """
        return self._paginate("/refunds", account, self._window(start, end, {}), limit)

    def list_disputes(
        self,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        account: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Disputes (chargebacks) in a window.

        ⚠ **A dispute's `amount` is the donation clawed back; the fee is
        separate.** Stripe's dispute fee is a cost of doing business, not part of
        the gift, and netting the two tells you to reverse the wrong amount in a
        CRM. Read the fee from the matching balance transaction.

        Args:
            start: Earliest `created` (inclusive)
            end: Latest `created` (inclusive)
            account: Configured account name
            limit: Stop after roughly this many records

        Returns:
            Dispute objects
        """
        return self._paginate("/disputes", account, self._window(start, end, {}), limit)

    def list_payouts(
        self,
        start: Optional[TimeLike] = None,
        end: Optional[TimeLike] = None,
        account: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Payouts — money Stripe moved to the bank.

        ⚠ **A payout is the amount that actually reached the bank, and charges do
        not sum to it.** Fees, refunds and any balance carried over from before
        the window land in the payout and not in the charges. When a bank line has
        to be explained, start here, then use
        ``list_balance_transactions(payout=<id>)`` to itemise it.

        ⚠ `created` is when Stripe *initiated* the payout; ``arrival_date`` is
        when the bank gets it. Match a bank statement on `arrival_date`.

        Args:
            start: Earliest `created` (inclusive)
            end: Latest `created` (inclusive)
            account: Configured account name
            limit: Stop after roughly this many records

        Returns:
            Payout objects
        """
        return self._paginate("/payouts", account, self._window(start, end, {}), limit)
