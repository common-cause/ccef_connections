"""
Jotform Tables connector for CCEF connections library.

Read-only access to a Jotform **Table that its owner has shared by link** —
the ``https://www.jotform.com/tables/<id>`` page — with no account and no API
key. Built for a partner's form (TN's Election Day Incident Report, 2026-10-30)
where the form belongs to someone else and the only access we have is the
link they handed around.

🚨 These are the endpoints the Tables web app itself calls, not Jotform's
documented API (``api.jotform.com``, which needs the OWNER's key). They are
undocumented and can change without notice. Everything below was verified
live 2026-10-30 against a link-shared table, signed out:

- **A guest session is required.** ``GET /tables/<id>`` 302s to itself once
  to set ``guest`` and ``JOTFORM_SESSION`` cookies, then serves the app. The
  data calls are refused without those cookies.
- **Every data call is scoped through the shared table**:
  ``/API/sheets/<tableID>/<path>``. The same path WITHOUT that prefix
  (``/API/form/<id>/submissions``, ``/API/sheet/<id>/view/<id>/rows``) answers
  ``401 You're not authorized``: the link share authorises reads *through
  the table*, not the form. The app's own request layer adds the prefix.
- **A Referer from jotform.com is required.** Without one every call answers
  ``200 text/plain "Cross-Site Requests not allowed!"`` — not JSON, not an
  error status.
- **Errors arrive twice.** The envelope is ``{"responseCode", "message",
  "content", "resultSet"}`` and ``responseCode`` can disagree with the HTTP
  status, so ``_request`` checks both.
- **The default view of a table has the table's own id** (the form id), so
  rows are ``/sheet/<id>/view/<id>/rows``.
- ``created_at`` is a naive ``YYYY-MM-DD HH:MM:SS`` in the form owner's
  account timezone — **America/New_York** on the table this was built for
  (the newest row was 4 h behind UTC in EDT). It is not in the payload;
  callers must know it.
- ``filter={"created_at:gt": "..."}`` works server-side, as on the official
  API, and rows come back newest first. Paging is ``limit``/``offset``.

Answers are keyed by question id (``"17"``), each ``{"answer": ...}``. A
checkbox answer is a list, or a dict ``{"0": opt, "1": opt, "other": text}``
when the respondent wrote in "Other".
"""

import json
import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_with_backoff
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

JOTFORM_BASE = "https://www.jotform.com"

# What the Tables app sends. ARCHIVED and DELETED rows are the owner's trash.
LIVE_ROWS = {"status:ne": ["ARCHIVED", "DELETED"]}

# The app's own page size, and the most the endpoint returns per call.
PAGE_SIZE = 1000

# Retry rate limiting and nothing else (see core/retry.py): a 401 here means
# the link was revoked, and retrying it only delays saying so.
_retry_429 = retry_with_backoff(max_attempts=4, min_wait=2.0, max_wait=30.0,
                                exceptions=(RateLimitError,))


class JotformTablesConnector(BaseConnection):
    """
    Jotform Tables connector (read-only, link-shared tables, no credential).

    Examples:
        >>> jt = JotformTablesConnector()
        >>> questions = jt.get_questions("242045989689073")
        >>> rows = jt.get_rows("242045989689073",
        ...                    created_after="2026-01-01 00:00:00")
    """

    def __init__(self) -> None:
        """Initialize the Jotform Tables connector."""
        super().__init__()
        self._session: Optional[requests.Session] = None
        self._opened: set = set()

    def connect(self) -> None:
        """
        Start a session. No credential is involved and no call is made; the
        guest session for a table is opened on first use.
        """
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "ccef-connections (JotformTablesConnector)"
        self._opened = set()
        self._is_connected = True
        logger.info("Jotform Tables session started")

    def disconnect(self) -> None:
        """Close the session."""
        if self._session is not None:
            self._session.close()
        self._session = None
        self._opened = set()
        self._is_connected = False
        logger.debug("Disconnected from Jotform Tables")

    def health_check(self) -> bool:
        """True when a session exists."""
        return bool(self._is_connected and self._session is not None)

    # ── Internal helpers ──────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        if not self._is_connected or self._session is None:
            self.connect()

    def _open(self, table_id: str) -> None:
        """Load the table page once, which sets the guest session cookies."""
        self._ensure_connected()
        if table_id in self._opened:
            return
        url = f"{JOTFORM_BASE}/tables/{table_id}"
        try:
            resp = self._session.get(url, timeout=60)
        except requests.RequestException as e:
            raise ConnectionError(f"Jotform Tables page request failed: {e}") from e
        if resp.status_code == 429:
            raise RateLimitError("Jotform rate limit exceeded opening the table",
                                 retry_after=int(resp.headers.get("Retry-After", 30)))
        if resp.status_code >= 400:
            raise ConnectionError(
                f"Jotform Tables page {table_id} answered {resp.status_code}")
        self._opened.add(table_id)

    def _request(self, table_id: str, path: str,
                 params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        GET ``/API/sheets/<table_id><path>`` and return the whole envelope.

        Raises:
            AuthenticationError: HTTP or envelope 401/403 — the table is not
                shared by link, or the link was revoked
            RateLimitError: On HTTP or envelope 429
            ConnectionError: Any other failure, a non-JSON body (including the
                "Cross-Site Requests not allowed!" refusal), or a non-200
                envelope
        """
        self._open(table_id)
        url = f"{JOTFORM_BASE}/API/sheets/{table_id}{path}"
        headers = {"Referer": f"{JOTFORM_BASE}/tables/{table_id}"}
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=60)
        except requests.RequestException as e:
            raise ConnectionError(f"Jotform Tables request failed: {e}") from e

        try:
            body = resp.json()
        except ValueError:
            body = None
        code = body.get("responseCode") if isinstance(body, dict) else None
        status = code if isinstance(code, int) and code >= 400 else resp.status_code

        if status == 429:
            raise RateLimitError(
                f"Jotform rate limit exceeded on {path}",
                retry_after=int(resp.headers.get("Retry-After", 30)),
            )
        if status in (401, 403):
            message = body.get("message") if isinstance(body, dict) else resp.text[:200]
            raise AuthenticationError(
                f"Jotform refused table {table_id} ({status}): {message}. "
                "The table is not shared by link, or the link was revoked."
            )
        if status >= 400:
            raise ConnectionError(
                f"Jotform Tables error {status} on {path}: {resp.text[:200]}")
        if not isinstance(body, dict):
            raise ConnectionError(
                f"Jotform Tables returned a non-JSON body on {path}: {resp.text[:200]}")
        return body

    # ── Reads ─────────────────────────────────────────────────────────

    @_retry_429
    def get_questions(self, table_id: str) -> Dict[str, Dict[str, Any]]:
        """
        The form's questions, keyed by question id.

        Each value carries ``type`` (``control_checkbox``, ...), ``name``,
        ``text`` and, for choice questions, ``options`` (``|``-separated).
        """
        body = self._request(table_id, f"/form/{table_id}/questions")
        return body.get("content") or {}

    @_retry_429
    def _rows_page(self, table_id: str, view_id: str, filt: Dict[str, Any],
                   offset: int, limit: int) -> List[Dict[str, Any]]:
        body = self._request(
            table_id,
            f"/sheet/{table_id}/view/{view_id}/rows",
            params={
                "filter": json.dumps(filt),
                "limit": limit,
                "offset": offset,
                "no-qprops": 1,
                "forceSimpleRows": 1,
            },
        )
        content = body.get("content")
        if not isinstance(content, list):
            raise ConnectionError(
                f"Jotform Tables rows for {table_id} were not a list")
        return content

    def get_rows(self, table_id: str, created_after: Optional[str] = None,
                 view_id: Optional[str] = None,
                 page_size: int = PAGE_SIZE) -> List[Dict[str, Any]]:
        """
        Every live row (submission) of a table's view, newest first.

        Args:
            table_id: The id in the ``/tables/<id>`` URL
            created_after: ``"YYYY-MM-DD HH:MM:SS"`` in the owner's account
                timezone; only rows created after it are returned
            view_id: Defaults to the table's own id, its default view
            page_size: Rows per request

        Returns:
            Submission dicts with ``id``, ``form_id``, ``created_at``,
            ``updated_at``, ``status`` and ``answers``
        """
        filt = dict(LIVE_ROWS)
        if created_after:
            filt["created_at:gt"] = created_after
        view_id = view_id or table_id
        rows: List[Dict[str, Any]] = []
        while True:
            page = self._rows_page(table_id, view_id, filt, len(rows), page_size)
            rows.extend(page)
            if len(page) < page_size:
                return rows
