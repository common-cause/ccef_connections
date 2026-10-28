"""
PayPal connector for CCEF connections library.

Read access to the PayPal **Transaction Search** API (``/v1/reporting/transactions``),
using direct HTTP via the requests library — no PayPal SDK, so this connector needs
only the base install. Ported from financial-reconciliation's ``paypal_source.py``
(October 2026), which was always meant to land here.

⚠ **The C3 PayPal money is DAF disbursements, structurally like Chariot** — not like
ActBlue or Stripe. Verified against August 2026:

    payer_info.payer_name            always "Paypal Inc." / "PayPal Giving Fund" — never the donor
    payer_info.account_id            always the aggregator, not a donor
    transaction_info.transaction_subject   "<Sponsor> has sent you money" — the DAF sponsor
    transaction_info.custom_field    a grant reference that appears nowhere in ROI

The individual donor and the designation are NOT in this API; they are only in the
sponsor's grant report.

⚠ **Transaction Search must be enabled on the app**, or the token still issues and the
endpoint returns ``403 NOT_AUTHORIZED``. Enabling it takes a few minutes to propagate;
the scope then appears on the next token request. :meth:`connect` checks the scope.

⚠ **Requests cap at a 31-day window**, so :meth:`transactions` chunks and pages.

⚠ **A chunk that STARTS in the future is a hard error** ("Data for the given start date
is not available"), so the end of a window is clamped to today (UTC). An end slightly in
the future is accepted; only the start is rejected.

⚠ **Timestamps are UTC.** A grant taken on the evening of 23 June Eastern is stamped
24 June. :func:`as_date` is the UTC date; callers reporting in Eastern must convert
:func:`as_datetime` themselves.
"""

import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_paypal_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

PAYPAL_API_BASE = "https://api-m.paypal.com"
PAYPAL_SANDBOX_BASE = "https://api-m.sandbox.paypal.com"

SEARCH_SCOPE = "https://uri.paypal.com/services/reporting/search/read"

# The API refuses a window wider than this.
MAX_WINDOW_DAYS = 31
PAGE_SIZE = 500

# T0001 is a MassPay payment — the shape every PayPal Giving Fund / DAF grant arrives in.
# T0000 is a general PayPal-to-PayPal payment: money into the same balance, not a grant.
# T0400 is PayPal moving the balance to the bank.
EVENT_PAYMENT = "T0001"
EVENT_GENERAL_PAYMENT = "T0000"
EVENT_WITHDRAWAL = "T0400"

# ⚠ Every code that puts money INTO the balance. A bank reconciliation needs this: a
# withdrawal is the sum of everything that arrived before it, whatever its shape. A
# grants match wants EVENT_PAYMENT alone — a general payment is not a grant.
EVENT_MONEY_IN = (EVENT_PAYMENT, EVENT_GENERAL_PAYMENT)

STATUS_WORDS = {"S": "Success", "P": "Pending", "V": "Reversed", "D": "Denied"}

DateLike = Union[date, datetime, str]


# ── Transaction helpers (pure; no connection needed) ─────────────────────

def amount(block: Optional[Dict[str, Any]]) -> float:
    """A PayPal money block (``{"currency_code", "value"}``) as a float; absent -> 0.0."""
    v = (block or {}).get("value")
    return float(v) if v not in (None, "") else 0.0


def info(txn: Dict[str, Any]) -> Dict[str, Any]:
    """The ``transaction_info`` block, or {}."""
    return txn.get("transaction_info") or {}


def event_code(txn: Dict[str, Any]) -> str:
    return info(txn).get("transaction_event_code") or ""


def as_datetime(txn: Dict[str, Any]) -> datetime:
    """The transaction's initiation instant, timezone-aware (UTC). See the module note."""
    s = info(txn).get("transaction_initiation_date") or ""
    return datetime.fromisoformat(s.replace("Z", "+00:00").replace("+0000", "+00:00"))


def as_date(txn: Dict[str, Any]) -> date:
    """The transaction's initiation date in **UTC**. See :func:`as_datetime` first."""
    return date.fromisoformat((info(txn).get("transaction_initiation_date") or "")[:10])


def status_of(txn: Dict[str, Any]) -> str:
    """The transaction status spelled out ('S' -> 'Success'); unknown codes pass through."""
    code = (info(txn).get("transaction_status") or "").strip()
    return STATUS_WORDS.get(code, code)


def sponsor_of(txn: Dict[str, Any]) -> str:
    """The DAF sponsor behind a payment, from ``transaction_subject``.

    The subject reads "<Sponsor> has sent you money". PayPal Giving Fund payouts carry
    no subject; they are named by ``payer_info.payer_name.alternate_full_name``.
    """
    subj = (info(txn).get("transaction_subject") or "").strip()
    marker = " has sent you money"
    if subj.endswith(marker):
        return subj[: -len(marker)].strip()
    if subj:
        return subj
    return (((txn.get("payer_info") or {}).get("payer_name") or {})
            .get("alternate_full_name") or "")


def payments(txns: Iterable[Dict[str, Any]],
             codes: Tuple[str, ...] = (EVENT_PAYMENT,)) -> List[Dict[str, Any]]:
    """The money-in rows. Defaults to grants (T0001) only; pass EVENT_MONEY_IN for all."""
    return [t for t in txns if event_code(t) in codes]


def withdrawals(txns: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The balance-to-bank rows, oldest first — by full timestamp, not by date."""
    return sorted((t for t in txns if event_code(t) == EVENT_WITHDRAWAL), key=as_datetime)


def next_withdrawal(txn: Dict[str, Any],
                    wds: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The withdrawal that pays this payment out: the first one AFTER it.

    ⚠ A derivation, not a field — PayPal has no disbursement column. Verified on July
    2026 C3, where all 11 withdrawals tie to the cent. ⚠ Compared on the timestamp: a
    withdrawal earlier the same day can not pay out a payment taken later that day.
    None means no payout yet — money still in transit.

    ``wds`` must be sorted oldest first (:func:`withdrawals` does that).
    """
    at = as_datetime(txn)
    for w in wds:
        if as_datetime(w) > at:
            return w
    return None


def payout_date(txn: Dict[str, Any], wds: List[Dict[str, Any]]) -> Optional[date]:
    """The **UTC** date of the withdrawal that paid this payment out, or None."""
    w = next_withdrawal(txn, wds)
    return as_date(w) if w else None


def _as_day(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def chunks(lo: date, hi: date) -> List[Tuple[date, date]]:
    """[lo, hi] split into pieces of at most MAX_WINDOW_DAYS, because the API refuses wider."""
    out, start = [], lo
    while start <= hi:
        end = min(start + timedelta(days=MAX_WINDOW_DAYS - 1), hi)
        out.append((start, end))
        start = end + timedelta(days=1)
    return out


class PayPalConnector(BaseConnection):
    """
    PayPal connector for reconciliation-grade read access to Transaction Search.

    **Credential**::

        PAYPAL_API_KEY_PASSWORD={"client_id":"...","secret":"..."}

    The REST app's Client ID and Secret (client-credentials OAuth). One credential is
    one PayPal account; CCEF's is the C3 account.

    Examples:
        >>> pp = PayPalConnector()
        >>> pp.connect()
        >>> txns = pp.transactions("2026-10-01", "2026-10-31")
        >>> wds = withdrawals(txns)
        >>> for t in payments(txns):
        ...     print(as_date(t), sponsor_of(t), amount(info(t)["transaction_amount"]),
        ...           payout_date(t, wds))
    """

    def __init__(self, sandbox: bool = False,
                 credential_name: str = "PAYPAL_API_KEY") -> None:
        """
        Args:
            sandbox: Use PayPal's sandbox host. Credentials are environment-specific,
                so this is not a dry-run switch for production data.
            credential_name: The credential to read ({credential_name}_PASSWORD).
        """
        super().__init__()
        self._base = PAYPAL_SANDBOX_BASE if sandbox else PAYPAL_API_BASE
        self._credential_name = credential_name
        self._creds: Optional[Dict[str, str]] = None
        self._token: Optional[str] = None
        self._token_expires: float = 0.0
        self.scopes: List[str] = []

    # ── Connection lifecycle ──────────────────────────────────────────

    def connect(self) -> None:
        """
        Load the credential and get an access token.

        Raises:
            AuthenticationError: If PayPal rejects the credential, or the app lacks
                the Transaction Search scope (every search would 403).
            ConnectionError: If PayPal is unreachable
        """
        self._creds = self._credential_manager.get_paypal_credentials(self._credential_name)
        self._refresh_token()
        if SEARCH_SCOPE not in self.scopes:
            raise AuthenticationError(
                "The PayPal app has no Transaction Search scope, so every "
                "/v1/reporting/transactions call would 403. Enable 'Transaction "
                "Search' on the app; it takes a few minutes to propagate."
            )
        self._is_connected = True
        logger.info("Connected to PayPal (%s)", self._base)

    def disconnect(self) -> None:
        """Drop the token. There is no session to close."""
        self._token = None
        self._token_expires = 0.0
        self._is_connected = False

    def health_check(self) -> bool:
        """True when a one-day search succeeds."""
        try:
            today = datetime.now(timezone.utc).date()
            self._search(today, today, 1)
            return True
        except Exception as exc:  # noqa: BLE001 - health checks report, never raise
            logger.warning("PayPal health check failed: %s", exc)
            return False

    # ── Plumbing ──────────────────────────────────────────────────────

    def _refresh_token(self) -> None:
        if not self._creds:
            raise AuthenticationError("PayPal connector is not connected — call connect() first.")
        try:
            resp = requests.post(
                f"{self._base}/v1/oauth2/token",
                headers={"Accept": "application/json"},
                auth=(self._creds["client_id"], self._creds["secret"]),
                data={"grant_type": "client_credentials"}, timeout=30)
        except requests.RequestException as exc:
            raise ConnectionError(f"PayPal token request failed: {exc}") from exc
        if resp.status_code != 200:
            try:
                body = resp.json()
            except ValueError:
                body = {}
            err = AuthenticationError(
                f"PayPal auth failed ({resp.status_code}): "
                f"{body.get('error_description') or body.get('error') or resp.text[:200]}. "
                f"'invalid_client' means the credential is for the wrong environment or app."
            )
            err.status_code = resp.status_code
            raise err
        body = resp.json()
        self._token = body["access_token"]
        # Refresh a minute early rather than learn about expiry from a 401 mid-walk.
        self._token_expires = time.time() + int(body.get("expires_in", 0)) - 60
        self.scopes = str(body.get("scope", "")).split()

    def _headers(self) -> Dict[str, str]:
        if not self._token or time.time() >= self._token_expires:
            self._refresh_token()
        return {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        One GET.

        Raises:
            AuthenticationError: 401 or 403
            RateLimitError: 429
            ConnectionError: any other error status, or an unreachable API
        """
        try:
            resp = requests.get(f"{self._base}{path}", headers=self._headers(),
                                params=params, timeout=120)
        except requests.RequestException as exc:
            raise ConnectionError(f"PayPal API request failed: {exc}") from exc
        if resp.status_code in (401, 403):
            err = AuthenticationError(
                f"PayPal refused GET {path} ({resp.status_code}). A 403 NOT_AUTHORIZED "
                f"means the app lacks Transaction Search. Detail: {resp.text[:300]}"
            )
            err.status_code = resp.status_code
            raise err
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            raise RateLimitError(f"PayPal rate limit exceeded, retry after {retry_after}s",
                                 retry_after=retry_after)
        if resp.status_code >= 400:
            raise ConnectionError(
                f"PayPal API error {resp.status_code} on GET {path}: {resp.text[:500]}")
        return resp.json()

    @retry_paypal_operation
    def _search(self, lo: date, hi: date, page: int) -> Dict[str, Any]:
        """One page of one ≤31-day window, retried on rate limit."""
        return self._request("/v1/reporting/transactions", {
            "start_date": f"{lo.isoformat()}T00:00:00-0000",
            "end_date": f"{hi.isoformat()}T23:59:59-0000",
            "fields": "all", "page_size": PAGE_SIZE, "page": page,
        })

    # ── Reads ─────────────────────────────────────────────────────────

    def transactions(self, start: DateLike, end: DateLike) -> List[Dict[str, Any]]:
        """
        Every transaction initiated in [start, end] (UTC dates), all field blocks.

        Chunked to the 31-day cap and paged. ``fields=all`` because the blocks are
        opt-in and ``payer_info`` and ``transaction_subject`` are not in the default
        set. ``end`` is clamped to today (UTC); a window wholly in the future is [].

        ⚠ Transaction Search lags real time by up to a few hours (PayPal documents up
        to three). A payment from this morning may not be here yet.
        """
        lo = _as_day(start)
        hi = min(_as_day(end), datetime.now(timezone.utc).date())
        out: List[Dict[str, Any]] = []
        for c_lo, c_hi in chunks(lo, hi):
            page = 1
            while True:
                body = self._search(c_lo, c_hi, page)
                out.extend(body.get("transaction_details") or [])
                if page >= int(body.get("total_pages") or 1):
                    break
                page += 1
        return out
