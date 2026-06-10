"""Transactional email connector for the CCEF connections library.

Sends transactional email (e.g. login magic-links, notifications) through
Resend's HTTP API (https://resend.com). Uses a static API key in a Bearer
``Authorization`` header; the send endpoint takes a simple JSON body.

Only depends on ``requests`` (a base install dependency), so no extra is
required — it's available from the plain ``ccef-connections`` install, like
the Action Network and Geocodio connectors.

Examples:
    >>> from ccef_connections import EmailConnector
    >>> email = EmailConnector()
    >>> email.connect()
    >>> email.send(
    ...     to="director@example.org",
    ...     subject="Your sign-in link",
    ...     html='<a href="https://app/auth/magic?token=...">Sign in</a>',
    ...     from_addr="EP Roving Review <auth@mail.commoncause.org>",
    ... )
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_email_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

RESEND_API_BASE = "https://api.resend.com"


class EmailConnector(BaseConnection):
    """Transactional email connector backed by Resend.

    The from-address can be supplied per-call (``from_addr``) or defaulted
    from the ``RESEND_FROM_EMAIL`` environment variable. Resend requires the
    sending domain to be verified (or the from-address to be on Resend's
    shared sending domain).
    """

    def __init__(self) -> None:
        """Initialize the email connector."""
        super().__init__()
        self._api_key: Optional[str] = None

    def connect(self) -> None:
        """Load the Resend API key and mark the connector connected.

        Raises:
            CredentialError: If the API key is missing
            ConnectionError: If credential loading fails
        """
        try:
            self._api_key = self._credential_manager.get_resend_api_key()
            self._is_connected = True
            logger.info("Successfully connected to Resend email service")
        except Exception as e:
            logger.error(f"Failed to connect to Resend: {str(e)}")
            raise ConnectionError(f"Failed to connect to Resend: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the Resend connection."""
        self._api_key = None
        self._is_connected = False
        logger.debug("Disconnected from Resend")

    def health_check(self) -> bool:
        """Return True if connected with a non-empty API key.

        Mirrors the Geocodio connector: no live API call (Resend has no free
        health endpoint, and we don't want to send a probe email)."""
        return bool(self._is_connected and self._api_key)

    # -- HTTP helpers ---------------------------------------------------------

    def _headers(self) -> Dict[str, str]:
        """Return request headers with the Bearer API key."""
        return {
            "Authorization": f"Bearer {self._api_key or ''}",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Central HTTP method with error handling.

        Args:
            method: HTTP method (GET, POST)
            path: API path relative to the Resend base (e.g. '/emails')
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: On 401/403 responses (bad/again API key)
            RateLimitError: On 429 responses
            ConnectionError: On other HTTP errors or network failures
        """
        if not self._is_connected and not self._api_key:
            self.connect()

        url = f"{RESEND_API_BASE}{path}"

        try:
            resp = requests.request(
                method,
                url,
                headers=self._headers(),
                json=json_body,
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Resend API request failed: {e}") from e

        if resp.status_code in (401, 403):
            raise AuthenticationError(
                f"Resend authentication failed ({resp.status_code}): {resp.text}"
            )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            raise RateLimitError(
                f"Resend rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"Resend API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    # -- Public API -----------------------------------------------------------

    @retry_email_operation
    def send(
        self,
        to: Union[str, List[str]],
        subject: str,
        *,
        html: Optional[str] = None,
        text: Optional[str] = None,
        from_addr: Optional[str] = None,
        reply_to: Optional[Union[str, List[str]]] = None,
    ) -> Dict[str, Any]:
        """Send a transactional email.

        Args:
            to: Recipient address, or a list of addresses.
            subject: Subject line.
            html: HTML body (provide ``html`` and/or ``text``).
            text: Plain-text body.
            from_addr: Sender, e.g. ``"Name <addr@domain>"``. Falls back to
                the ``RESEND_FROM_EMAIL`` env var if not given.
            reply_to: Optional reply-to address(es).

        Returns:
            Resend's response dict (includes the message ``id`` on success).

        Raises:
            ValueError: If no sender resolves, or neither body is given.
            AuthenticationError / RateLimitError / ConnectionError: see ``_request``.
        """
        sender = from_addr or os.getenv("RESEND_FROM_EMAIL")
        if not sender:
            raise ValueError(
                "from_addr is required (or set the RESEND_FROM_EMAIL env var)"
            )
        if html is None and text is None:
            raise ValueError("provide at least one of html= or text=")

        recipients = [to] if isinstance(to, str) else list(to)
        body: Dict[str, Any] = {
            "from": sender,
            "to": recipients,
            "subject": subject,
        }
        if html is not None:
            body["html"] = html
        if text is not None:
            body["text"] = text
        if reply_to is not None:
            body["reply_to"] = reply_to

        result = self._request("POST", "/emails", json_body=body)
        return result or {}
