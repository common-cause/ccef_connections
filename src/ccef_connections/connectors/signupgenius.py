"""
SignUpGenius connector for CCEF connections library.

Read-only access to the SignUpGenius key-based API v2 (Pro accounts only):
the account profile, the account's signups, and per-signup slot reports.

Authentication is a plain API key passed as the ``user_key`` query parameter.
The key is read from ``SIGNUPGENIUS_API_KEY_PASSWORD`` and is found in the
SignUpGenius UI under profile → Settings → Pro Tools → API Management.

Behaviour verified live 2026-09-28 (CC's Platinum account):

- A bad key is a **403 with a text/plain body** ("Authentication failed"),
  not JSON.
- Every JSON response is an envelope ``{"success", "message", "data"}``, and
  a refused request can come back **HTTP 200 with ``success: false``** — e.g.
  a report for a signup the account doesn't own returns
  ``200 {"success": false, "message": ["access denied"], "data": {}}``.
  Reading ``data`` without checking ``success`` turns that into an empty
  report, so ``_request`` raises on it instead.
- No rate-limit headers are sent and no limit is published. 429 is handled
  the same way as every other service in case one ever appears.
- ``group`` / ``groupid`` on a signup is **not** a shared folder: signups
  created by copying each get their own ``groupid`` under the same group name.
  Filter on name if you mean "every signup in this group".
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_signupgenius_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

SIGNUPGENIUS_API_BASE = "https://api.signupgenius.com/v2/k"

SIGNUP_STATUSES = ("active", "expired", "all")
REPORT_TYPES = ("filled", "available", "all")


class SignUpGeniusConnector(BaseConnection):
    """
    SignUpGenius connector (read-only).

    The API key is read from the ``SIGNUPGENIUS_API_KEY_PASSWORD`` environment
    variable (plain string, not JSON).

    Examples:
        >>> sug = SignUpGeniusConnector()
        >>> signups = sug.list_signups("active")
        >>> for s in signups:
        ...     slots = sug.get_report(s["signupid"], "filled")
    """

    def __init__(self) -> None:
        """Initialize the SignUpGenius connector."""
        super().__init__()
        self._api_key: Optional[str] = None

    def connect(self) -> None:
        """
        Load the SignUpGenius API key and mark the connector as connected.

        Does not make a live API call; use :meth:`get_profile` to verify the key.

        Raises:
            CredentialError: If ``SIGNUPGENIUS_API_KEY_PASSWORD`` is not set
            ConnectionError: If credential loading fails for any other reason
        """
        try:
            self._api_key = self._credential_manager.get_signupgenius_key()
            self._is_connected = True
            logger.info("Successfully connected to SignUpGenius")
        except CredentialError:
            logger.error("Failed to connect to SignUpGenius: credentials missing")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to SignUpGenius: {str(e)}")
            raise ConnectionError(f"Failed to connect to SignUpGenius: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the SignUpGenius API key and connection state."""
        self._api_key = None
        self._is_connected = False
        logger.debug("Disconnected from SignUpGenius")

    def health_check(self) -> bool:
        """
        Check whether the connector is connected and has a non-empty API key.

        Returns:
            True if connected with a valid-looking key, False otherwise.
        """
        return bool(self._is_connected and self._api_key)

    # ── Internal helpers ──────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        """Auto-connect if not already connected."""
        if not self._is_connected:
            self.connect()

    def _request(self, endpoint: str) -> Any:
        """
        GET an endpoint and return the envelope's ``data``.

        The key is read *after* connecting, never before: ``requests`` silently
        drops a query parameter whose value is ``None``, so reading it first on
        a fresh connector sends an unauthenticated request (the Geocodio bug).

        Args:
            endpoint: Path relative to the base URL, e.g. ``"/user/profile/"``

        Returns:
            The ``data`` member of the response envelope

        Raises:
            AuthenticationError: On HTTP 401/403, or ``success: false`` with
                an "access denied" message
            RateLimitError: On HTTP 429
            ConnectionError: On any other HTTP error, network failure,
                non-JSON body, or ``success: false``
        """
        self._ensure_connected()
        if not self._api_key:
            raise ConnectionError(
                "SignUpGenius API key is unavailable after connect(); refusing to "
                "send an unauthenticated request"
            )
        url = f"{SIGNUPGENIUS_API_BASE}{endpoint}"

        try:
            resp = requests.get(url, params={"user_key": self._api_key}, timeout=60)
        except requests.RequestException as e:
            raise ConnectionError(f"SignUpGenius request failed: {e}") from e

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            raise RateLimitError(
                f"SignUpGenius rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code in (401, 403):
            raise AuthenticationError(
                f"SignUpGenius authentication failed ({resp.status_code}): {resp.text[:200]}"
            )

        if resp.status_code >= 400:
            raise ConnectionError(
                f"SignUpGenius API error {resp.status_code} on {endpoint}: {resp.text[:200]}"
            )

        try:
            body = resp.json()
        except ValueError as e:
            raise ConnectionError(
                f"SignUpGenius returned a non-JSON body on {endpoint}: {resp.text[:200]}"
            ) from e

        if not isinstance(body, dict) or not body.get("success"):
            messages = body.get("message") if isinstance(body, dict) else None
            text = "; ".join(str(m) for m in messages) if messages else "no message"
            if "access denied" in text.lower():
                raise AuthenticationError(
                    f"SignUpGenius refused {endpoint}: {text}"
                )
            raise ConnectionError(f"SignUpGenius request unsuccessful on {endpoint}: {text}")

        return body.get("data")

    # ── Account ───────────────────────────────────────────────────────

    @retry_signupgenius_operation
    def get_profile(self) -> Dict[str, Any]:
        """
        Get the account profile behind the API key.

        The cheapest live check that the key works. Includes
        ``subscription`` (``ispro``, ``prolevel``, ``expiredate``) and
        ``issubadmin``.

        Returns:
            Profile dict
        """
        return self._request("/user/profile/") or {}

    # ── Signups ───────────────────────────────────────────────────────

    @retry_signupgenius_operation
    def list_signups(self, status: str = "active") -> List[Dict[str, Any]]:
        """
        List signups created by the account.

        Args:
            status: ``"active"``, ``"expired"`` or ``"all"``

        Returns:
            List of signup dicts with ``signupid``, ``title``, ``group``,
            ``groupid``, ``signupurl``, ``startdate``/``enddate`` (epoch
            seconds) and their ``*string`` forms, ``offset``, ``contactname``

        Raises:
            ValueError: If ``status`` is not one of the three above
        """
        if status not in SIGNUP_STATUSES:
            raise ValueError(f"status must be one of {SIGNUP_STATUSES}, got {status!r}")
        return self._request(f"/signups/created/{status}/") or []

    @retry_signupgenius_operation
    def get_report(self, signup_id: int, report: str = "filled") -> List[Dict[str, Any]]:
        """
        Get a signup's slot report.

        Args:
            signup_id: The signup's ``signupid``
            report: ``"filled"`` (slots someone took, with their name, email,
                phone, status and signup date), ``"available"`` (open slots)
                or ``"all"`` (both)

        Returns:
            List of slot-row dicts. Rows carry ``slotitemid`` (the slot) and
            ``itemmemberid`` (the signup in it; empty on an open slot),
            ``item``, ``startdate``/``enddate`` (epoch seconds, UTC) and
            ``offset``, ``myqty``, ``status``, ``waitlist``.

        Raises:
            ValueError: If ``report`` is not one of the three above
            AuthenticationError: If the account can't see this signup
        """
        if report not in REPORT_TYPES:
            raise ValueError(f"report must be one of {REPORT_TYPES}, got {report!r}")
        data = self._request(f"/signups/report/{report}/{int(signup_id)}/") or {}
        return data.get("signup") or []