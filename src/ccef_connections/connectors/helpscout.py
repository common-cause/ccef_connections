"""
HelpScout connector for CCEF connections library.

This module provides automated email processing via the HelpScout API v2:
read conversations from shared inboxes, extract message content, forward emails,
reply, add notes, and close/resolve conversations.

Uses OAuth2 Client Credentials flow with direct HTTP via the requests library.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_helpscout_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

HELPSCOUT_TOKEN_URL = "https://api.helpscout.net/v2/oauth2/token"
HELPSCOUT_API_BASE = "https://api.helpscout.net/v2"


class HelpScoutConnector(BaseConnection):
    """
    HelpScout connector for automated email processing.

    Provides access to HelpScout mailboxes and conversations using
    OAuth2 Client Credentials authentication.

    Examples:
        >>> connector = HelpScoutConnector()
        >>> connector.connect()
        >>> mailboxes = connector.list_mailboxes()
        >>> conversations = connector.list_conversations(mailbox_id=12345)
    """

    def __init__(self) -> None:
        """Initialize the HelpScout connector."""
        super().__init__()
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def connect(self) -> None:
        """
        Establish connection to HelpScout by obtaining an OAuth2 token.

        Raises:
            CredentialError: If HelpScout credentials are missing or invalid
            AuthenticationError: If OAuth2 token request fails
            ConnectionError: If connection setup fails
        """
        try:
            creds = self._credential_manager.get_helpscout_credentials()
            self._fetch_token(creds["app_id"], creds["app_secret"])
            self._is_connected = True
            logger.info("Successfully connected to HelpScout")
        except AuthenticationError:
            logger.error("Failed to connect to HelpScout: authentication failed")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to HelpScout: {str(e)}")
            raise ConnectionError(f"Failed to connect to HelpScout: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the HelpScout connection and token."""
        self._access_token = None
        self._token_expires_at = 0.0
        self._is_connected = False
        logger.debug("Disconnected from HelpScout")

    def health_check(self) -> bool:
        """
        Check if the HelpScout connection is healthy.

        Verifies the token is valid by calling GET /v2/users/me.

        Returns:
            True if connected and token is valid, False otherwise
        """
        if not self._is_connected or not self._access_token:
            return False
        try:
            self._request("GET", "/users/me")
            return True
        except Exception:
            return False

    # ── Token management ──────────────────────────────────────────────

    def _fetch_token(self, app_id: str, app_secret: str) -> None:
        """
        Fetch an OAuth2 access token using client credentials.

        Args:
            app_id: HelpScout OAuth2 application ID
            app_secret: HelpScout OAuth2 application secret

        Raises:
            AuthenticationError: If token request fails
        """
        try:
            resp = requests.post(
                HELPSCOUT_TOKEN_URL,
                json={
                    "grant_type": "client_credentials",
                    "client_id": app_id,
                    "client_secret": app_secret,
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to reach HelpScout token endpoint: {e}") from e

        if resp.status_code != 200:
            raise AuthenticationError(
                f"HelpScout OAuth2 token request failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        # Expire slightly early to avoid edge cases
        self._token_expires_at = time.time() + data.get("expires_in", 172800) - 60
        logger.debug("HelpScout OAuth2 token obtained")

    def _refresh_token_if_needed(self) -> None:
        """Re-fetch the OAuth2 token if it has expired."""
        if time.time() >= self._token_expires_at:
            logger.debug("HelpScout token expired, refreshing")
            creds = self._credential_manager.get_helpscout_credentials()
            self._fetch_token(creds["app_id"], creds["app_secret"])

    def _get_headers(self) -> Dict[str, str]:
        """
        Return authorization headers, refreshing the token if needed.

        Returns:
            Dict with Authorization and Content-Type headers
        """
        self._refresh_token_if_needed()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    # ── HTTP helpers ──────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Central HTTP method with auth headers and auto-refresh on 401.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            path: API path relative to /v2 (e.g., '/mailboxes')
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: If authentication fails after retry
            RateLimitError: If rate limited by the API
            ConnectionError: If the request fails
        """
        if not self._is_connected and not self._access_token:
            self.connect()

        url = f"{HELPSCOUT_API_BASE}{path}"

        try:
            resp = requests.request(
                method,
                url,
                headers=self._get_headers(),
                params=params,
                json=json_body,
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"HelpScout API request failed: {e}") from e

        # Auto-refresh on 401 and retry once
        if resp.status_code == 401:
            logger.debug("Received 401, refreshing token and retrying")
            self._token_expires_at = 0.0  # Force refresh
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._get_headers(),
                    params=params,
                    json=json_body,
                    timeout=30,
                )
            except requests.RequestException as e:
                raise ConnectionError(f"HelpScout API request failed on retry: {e}") from e

            if resp.status_code == 401:
                raise AuthenticationError("HelpScout authentication failed after token refresh")

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            raise RateLimitError(
                f"HelpScout rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"HelpScout API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        resource_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generic pagination helper that follows _links.next across pages.

        Args:
            path: Initial API path
            params: Query parameters for the first request
            resource_key: Key in _embedded that contains the resource list
                (e.g., 'conversations', 'mailboxes', 'threads')

        Returns:
            Combined list of all resources across pages
        """
        results: List[Dict[str, Any]] = []
        current_path = path
        current_params = params

        while True:
            data = self._request("GET", current_path, params=current_params)
            if data is None:
                break

            embedded = data.get("_embedded", {})
            if resource_key and resource_key in embedded:
                results.extend(embedded[resource_key])
            elif embedded:
                # Use the first key in _embedded
                for key in embedded:
                    results.extend(embedded[key])
                    break

            # Follow next page link
            next_link = data.get("_links", {}).get("next", {}).get("href")
            if not next_link:
                break

            # next_link is a full URL; extract the path portion
            if next_link.startswith(HELPSCOUT_API_BASE):
                current_path = next_link[len(HELPSCOUT_API_BASE):]
            else:
                current_path = next_link
            current_params = None  # params are encoded in the next URL

        return results

    # ── Mailboxes ─────────────────────────────────────────────────────

    @retry_helpscout_operation
    def list_mailboxes(self) -> List[Dict[str, Any]]:
        """
        List all available HelpScout mailboxes.

        Returns:
            List of mailbox dicts with id, name, email, etc.

        Examples:
            >>> connector = HelpScoutConnector()
            >>> connector.connect()
            >>> mailboxes = connector.list_mailboxes()
            >>> for mb in mailboxes:
            ...     print(mb['id'], mb['name'])
        """
        return self._paginate("/mailboxes", resource_key="mailboxes")

    # ── Conversations (read) ──────────────────────────────────────────

    @retry_helpscout_operation
    def list_conversations(
        self,
        mailbox_id: int,
        status: Optional[str] = None,
        tag: Optional[str] = None,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        List conversations from a mailbox with optional filters.

        Args:
            mailbox_id: The mailbox ID to query
            status: Filter by status ('active', 'pending', 'closed', 'all')
            tag: Filter by tag name
            **kwargs: Additional query parameters (e.g., page, sortField)

        Returns:
            List of conversation dicts

        Examples:
            >>> conversations = connector.list_conversations(
            ...     mailbox_id=12345, status='active'
            ... )
        """
        params: Dict[str, Any] = {"mailbox": mailbox_id}
        if status:
            params["status"] = status
        if tag:
            params["tag"] = tag
        params.update(kwargs)
        return self._paginate("/conversations", params=params, resource_key="conversations")

    @retry_helpscout_operation
    def get_conversation(self, conversation_id: int) -> Dict[str, Any]:
        """
        Get a single conversation by ID.

        Args:
            conversation_id: The conversation ID

        Returns:
            Conversation dict with full details

        Examples:
            >>> conversation = connector.get_conversation(98765)
        """
        result = self._request("GET", f"/conversations/{conversation_id}")
        return result or {}

    @retry_helpscout_operation
    def list_threads(self, conversation_id: int) -> List[Dict[str, Any]]:
        """
        List all threads (messages) in a conversation.

        Args:
            conversation_id: The conversation ID

        Returns:
            List of thread dicts with message content

        Examples:
            >>> threads = connector.list_threads(98765)
            >>> for thread in threads:
            ...     print(thread.get('body', ''))
        """
        return self._paginate(
            f"/conversations/{conversation_id}/threads",
            resource_key="threads",
        )

    # ── Conversations (write) ─────────────────────────────────────────

    @retry_helpscout_operation
    def reply_to_conversation(
        self,
        conversation_id: int,
        text: str,
        customer: Optional[Dict[str, str]] = None,
        draft: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Reply to a conversation.

        Args:
            conversation_id: The conversation ID
            text: Reply body (HTML supported)
            customer: Customer dict with 'email' key (uses conversation customer
                if not provided)
            draft: If True, save as draft instead of sending
            **kwargs: Additional fields (cc, bcc, attachments, etc.)

        Examples:
            >>> connector.reply_to_conversation(
            ...     98765,
            ...     "Thank you for reaching out. We'll look into this.",
            ... )
        """
        body: Dict[str, Any] = {"text": text, "draft": draft}
        if customer:
            body["customer"] = customer
        body.update(kwargs)
        self._request("POST", f"/conversations/{conversation_id}/reply", json_body=body)

    @retry_helpscout_operation
    def add_note(self, conversation_id: int, text: str) -> None:
        """
        Add an internal note to a conversation.

        Args:
            conversation_id: The conversation ID
            text: Note body (HTML supported)

        Examples:
            >>> connector.add_note(98765, "Escalating to tier 2 support.")
        """
        self._request(
            "POST",
            f"/conversations/{conversation_id}/notes",
            json_body={"text": text},
        )

    @retry_helpscout_operation
    def update_conversation_status(
        self, conversation_id: int, status: str
    ) -> None:
        """
        Update the status of a conversation.

        Args:
            conversation_id: The conversation ID
            status: New status ('active', 'pending', or 'closed')

        Raises:
            ValueError: If status is not a valid value

        Examples:
            >>> connector.update_conversation_status(98765, 'closed')
        """
        valid_statuses = ("active", "pending", "closed")
        if status not in valid_statuses:
            raise ValueError(
                f"Invalid status '{status}'. Must be one of: {', '.join(valid_statuses)}"
            )
        self._request(
            "PUT",
            f"/conversations/{conversation_id}",
            json_body={"op": "replace", "path": "/status", "value": status},
        )

    @retry_helpscout_operation
    def forward_conversation(
        self,
        conversation_id: int,
        to: List[str],
        note: Optional[str] = None,
    ) -> None:
        """
        Forward a conversation to external email addresses.

        Args:
            conversation_id: The conversation ID
            to: List of email addresses to forward to
            note: Optional note to include with the forward

        Examples:
            >>> connector.forward_conversation(
            ...     98765,
            ...     to=["partner@example.com"],
            ...     note="FYI - see thread below.",
            ... )
        """
        body: Dict[str, Any] = {"to": to}
        if note:
            body["text"] = note
        self._request(
            "POST",
            f"/conversations/{conversation_id}/forward",
            json_body=body,
        )
