"""Microsoft Graph mail connector for the CCEF connections library.

Sends mail as a Common Cause mailbox through Microsoft Graph, using the Azure
app "Claude Mail Automation" (client-credentials flow, application
``Mail.Send``). There is no signed-in user, which is what makes unattended
sending from a scheduled job possible.

Proven in cc-notifications (2026-08-20) and since copied into several
projects; this is the shared version those copies should move to.

Things that will otherwise cost someone an afternoon:

* **The sender must be on IT's allowlist.** An Exchange
  ``ApplicationAccessPolicy`` (2026-10-01) scopes the app to ``grants@``,
  ``portfolios@`` and ``dataops@``. Any other mailbox is refused with a 403
  even though the mailbox exists -- that is a policy request to IT, not a
  code or credential bug.
* **202 means accepted, not delivered.** Graph queues the message. A 202
  with no arrival is an Exchange question (transport rules, junk folder).
* **The app holds Mail.Send and nothing else.** ``GET /users/{address}``
  returns 403 and that is correct; don't use it as a pre-flight check.
* **The client secret expires** (Azure secrets run 6-24 months). An
  ``invalid_client`` from the token endpoint usually means it has.

Credential: ``SEND_EMAIL_CREDENTIALS_PASSWORD``, JSON
``{"tenant_id", "client_id", "client_secret", "default_sender"}``. The sender
is resolved per call, then from the ``SEND_EMAIL_SENDER`` env var, then from
``default_sender``.

Only depends on ``requests`` (a base install dependency), so no extra is
required.

Examples:
    >>> from ccef_connections import GraphMailConnector
    >>> mail = GraphMailConnector()
    >>> mail.send(
    ...     to="someone@commoncause.org",
    ...     subject="Nightly job OK",
    ...     html="<p>All 96 runs succeeded.</p>",
    ...     sender="dataops@commoncause.org",
    ... )
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_graph_mail_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = "https://graph.microsoft.com/.default"
TOKEN_URL = "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
SENDER_ENV = "SEND_EMAIL_SENDER"

# Refresh this long before the token's stated expiry, so a send never goes out
# on a token that dies in flight.
TOKEN_SKEW_SECONDS = 300


def _as_list(value: Union[str, List[str], None]) -> List[str]:
    if value is None:
        return []
    return [value] if isinstance(value, str) else list(value)


def _recipients(addresses: List[str]) -> List[Dict[str, Any]]:
    return [{"emailAddress": {"address": a}} for a in addresses]


class GraphMailConnector(BaseConnection):
    """Send mail as a Common Cause mailbox through Microsoft Graph.

    The access token is fetched on first send and reused until shortly before
    it expires (Graph tokens last about an hour).
    """

    def __init__(self) -> None:
        """Initialize the Graph mail connector."""
        super().__init__()
        self._creds: Optional[Dict[str, str]] = None
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def connect(self) -> None:
        """Load the app credentials. Does not fetch a token yet.

        Raises:
            CredentialError: If the credential is missing or incomplete
            ConnectionError: If credential loading fails for another reason
        """
        try:
            self._creds = self._credential_manager.get_graph_mail_credentials()
            self._is_connected = True
            logger.info("Loaded Microsoft Graph mail credentials")
        except CredentialError:
            # Re-raised as-is, as EmailConnector does: a missing credential is
            # not a connection failure.
            logger.error("Failed to connect to Graph mail: credentials missing")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Graph mail: {str(e)}")
            raise ConnectionError(f"Failed to connect to Graph mail: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the credentials and any cached token."""
        self._creds = None
        self._token = None
        self._token_expires_at = 0.0
        self._is_connected = False
        logger.debug("Disconnected from Graph mail")

    def health_check(self) -> bool:
        """Return True if connected with credentials loaded.

        No live call: the app can't read anything (Mail.Send only), and a
        probe send would be a real email."""
        return bool(self._is_connected and self._creds)

    # -- Auth -----------------------------------------------------------------

    def _ensure_connected(self) -> None:
        if not self._is_connected or not self._creds:
            self.connect()

    def _get_token(self) -> str:
        """Return a valid access token, fetching a new one when needed.

        Raises:
            AuthenticationError: If the token endpoint rejects the app (a bad
                or expired client secret, or a wrong tenant/client id)
            ConnectionError: On a network failure or unexpected response
        """
        self._ensure_connected()
        if self._token and time.time() < self._token_expires_at - TOKEN_SKEW_SECONDS:
            return self._token

        creds = self._creds or {}
        try:
            resp = requests.post(
                TOKEN_URL.format(tenant_id=creds["tenant_id"]),
                data={
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                    "scope": GRAPH_SCOPE,
                    "grant_type": "client_credentials",
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Graph token request failed: {e}") from e

        if resp.status_code in (400, 401):
            # invalid_client (expired secret) and unauthorized_client both
            # arrive here; the body says which.
            raise AuthenticationError(
                f"Graph token request rejected ({resp.status_code}): {resp.text[:500]}"
            )
        if resp.status_code >= 400:
            raise ConnectionError(
                f"Graph token endpoint error {resp.status_code}: {resp.text[:500]}"
            )

        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + float(payload.get("expires_in", 3600))
        return self._token

    # -- Public API -----------------------------------------------------------

    def resolve_sender(self, sender: Optional[str] = None) -> str:
        """Return the mailbox a send would go out as.

        Order: ``sender`` argument, the ``SEND_EMAIL_SENDER`` env var, then the
        credential's ``default_sender``.

        Raises:
            ValueError: If none of those is set
        """
        if sender:
            return sender
        env_sender = os.getenv(SENDER_ENV)
        if env_sender and env_sender.strip():
            return env_sender.strip()
        self._ensure_connected()
        default = (self._creds or {}).get("default_sender")
        if default:
            return default
        raise ValueError(
            f"No sender: pass sender=, set {SENDER_ENV}, or add default_sender "
            "to SEND_EMAIL_CREDENTIALS"
        )

    @retry_graph_mail_operation
    def send(
        self,
        to: Union[str, List[str]],
        subject: str,
        *,
        html: Optional[str] = None,
        text: Optional[str] = None,
        sender: Optional[str] = None,
        cc: Union[str, List[str], None] = None,
        bcc: Union[str, List[str], None] = None,
        reply_to: Union[str, List[str], None] = None,
        save_to_sent_items: bool = True,
    ) -> int:
        """Send one message.

        Args:
            to: Recipient address, or a list of addresses.
            subject: Subject line.
            html: HTML body. Graph takes one body, so ``html`` wins if both
                are given.
            text: Plain-text body, used when ``html`` is not given.
            sender: Mailbox to send as (see :meth:`resolve_sender`). It must
                be on IT's ApplicationAccessPolicy allowlist.
            cc / bcc / reply_to: Optional address or list of addresses.
            save_to_sent_items: Keep a copy in the sender's Sent Items
                (default True) -- usually the only audit trail a job has.

        Returns:
            The HTTP status, 202 (accepted -- queued, not yet delivered).

        Raises:
            ValueError: If there are no recipients, no body, or no sender.
            AuthenticationError: On 401/403. A 403 for a real mailbox almost
                always means the sender isn't on the access policy.
            RateLimitError: On 429 (retried by the decorator).
            ConnectionError: On other HTTP errors or network failures.
        """
        to_list = _as_list(to)
        if not to_list:
            raise ValueError("at least one recipient is required")
        if html is None and text is None:
            raise ValueError("provide at least one of html= or text=")
        from_mailbox = self.resolve_sender(sender)

        message: Dict[str, Any] = {
            "subject": subject,
            "body": (
                {"contentType": "HTML", "content": html}
                if html is not None
                else {"contentType": "Text", "content": text}
            ),
            "toRecipients": _recipients(to_list),
        }
        if cc:
            message["ccRecipients"] = _recipients(_as_list(cc))
        if bcc:
            message["bccRecipients"] = _recipients(_as_list(bcc))
        if reply_to:
            message["replyTo"] = _recipients(_as_list(reply_to))

        token = self._get_token()
        try:
            resp = requests.post(
                f"{GRAPH_API_BASE}/users/{from_mailbox}/sendMail",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"message": message, "saveToSentItems": save_to_sent_items},
                timeout=60,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Graph sendMail request failed: {e}") from e

        if resp.status_code == 401:
            # Drop the cached token so the next call fetches a fresh one.
            self._token = None
            raise AuthenticationError(
                f"Graph sendMail unauthorized (401): {resp.text[:500]}"
            )
        if resp.status_code == 403:
            raise AuthenticationError(
                f"Graph refused to send as {from_mailbox} (403). If the mailbox "
                "exists, it is probably not on IT's ApplicationAccessPolicy for "
                f"the mail app: {resp.text[:500]}"
            )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            raise RateLimitError(
                f"Graph rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )
        if resp.status_code != 202:
            raise ConnectionError(
                f"Graph sendMail returned {resp.status_code}: {resp.text[:500]}"
            )
        logger.info(f"Graph accepted a message from {from_mailbox} to {len(to_list)} recipient(s)")
        return resp.status_code
