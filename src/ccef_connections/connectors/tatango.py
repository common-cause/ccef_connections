"""
Tatango (MomoGood) connector for CCEF connections library.

Messaging API v2 client for the Tatango SMS broadcast platform:
subscribers (add / read / update / soft opt-out), per-list custom
fields, and per-list webhook registrations.

Endpoint shapes and behaviors were confirmed by live tests on 2026-06-10
and 2026-08-05 (tatango-sync project, ``docs/tatango_api.md``). The
load-bearing gotchas, baked into this client's docstrings:

- Auth is HTTP Basic where the username is the Tatango **login email**
  and the password is the API key.
- ``POST .../subscribers`` defaults to DOUBLE opt-in (confirmation text
  + YES reply) even on single-opt-in lists; the ``bypass_opt_in_process``
  / ``bypass_opt_in_response`` flags defeat it (confirmed working on the
  Common Cause account).
- **A refused add still returns HTTP 201** — the refusal lives only in
  the response body's ``status`` string (e.g. the ~48 h re-subscribe
  "security timeout" after any opt-out). Callers must inspect ``status``
  or re-GET the subscriber (``optin_in_progress``); the HTTP code alone
  is not the outcome.
- ``DELETE`` is a **soft** opt-out (record retained, ``opted_out_at``
  set) and fires the ``unsubscribe`` webhook with a payload
  indistinguishable from an organic STOP — echo suppression is the
  caller's job (see tatango-sync Flow 2's self-write ledger).
- REST timestamps come back with inconsistent UTC offsets that are
  instant-preserving — parse them *with* the offset, never strip it.
- Writes pass through a WAF that 403-blocks request bodies containing
  threat-listed hostnames (seen with ``webhook.site``); a 403 with an
  HTML block page means the *body content* was refused, not the auth.
- The rate-limit tier is unpublished; 1 request / 3 s ran clean in live
  tests, so the connector paces itself to ``min_request_interval``
  (default 3.0 s) between calls.

There is deliberately no bulk-export surface here: the legacy Data Hub's
HTTP API never shipped and the product is being deprecated (vendor,
2026-08-07) — exports ride the in-app Report Builder, which has no API.
"""

import logging
import time
from typing import Any, Dict, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_tatango_operation
from ..exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

TATANGO_API_BASE = "https://app.tatango.com/api/v2"


class TatangoConnector(BaseConnection):
    """
    Tatango (MomoGood) connector for SMS list operations.

    Provides subscriber, custom-field, and webhook CRUD against the
    Messaging API v2 using HTTP Basic auth (login email : API key).

    All operations are per-list. Pass ``default_list_id`` once at
    construction (Common Cause runs a single production list) or pass
    ``list_id`` per call; the per-call value wins.

    Examples:
        >>> connector = TatangoConnector(default_list_id="1061380")
        >>> connector.connect()
        >>> connector.add_subscriber(
        ...     "3125550123",
        ...     first_name="Jane",
        ...     bypass_opt_in_process=True,
        ...     bypass_opt_in_response=True,
        ... )
    """

    def __init__(
        self,
        default_list_id: Optional[str] = None,
        min_request_interval: float = 3.0,
    ) -> None:
        """
        Initialize the Tatango connector.

        Args:
            default_list_id: List used when a method's ``list_id`` is omitted
            min_request_interval: Client-side pacing floor in seconds between
                requests. Tatango's rate-limit tier is unpublished; 1 req/3 s
                is the live-tested-safe rate. Lower it only once a real tier
                is confirmed with the vendor.
        """
        super().__init__()
        self._login_email: Optional[str] = None
        self._api_key: Optional[str] = None
        self._default_list_id: Optional[str] = (
            str(default_list_id) if default_list_id is not None else None
        )
        self._min_request_interval = min_request_interval
        self._last_request_at: Optional[float] = None

    def connect(self) -> None:
        """
        Establish connection to Tatango by loading Basic-auth credentials.

        Raises:
            CredentialError: If the Tatango credentials are missing or malformed
            ConnectionError: If credential loading fails for any other reason
        """
        try:
            creds = self._credential_manager.get_tatango_credentials()
            self._login_email = creds["email"]
            self._api_key = creds["api_key"]
            self._is_connected = True
            logger.info("Successfully connected to Tatango")
        except CredentialError:
            # Re-raised as-is: a missing credential is not a connection
            # failure, and the two are sibling classes so wrapping meant
            # `except CredentialError` could never fire. Matches the docstring
            # above and SheetsConnector/BigQueryConnector/etc.
            logger.error("Failed to connect to Tatango: credentials missing")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Tatango: {str(e)}")
            raise ConnectionError(f"Failed to connect to Tatango: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the Tatango connection."""
        self._login_email = None
        self._api_key = None
        self._is_connected = False
        logger.debug("Disconnected from Tatango")

    def health_check(self) -> bool:
        """
        Check connection health with a cheap read.

        Reads the default list if one is configured, otherwise the lists
        collection.

        Returns:
            True if connected and the API responds, False otherwise
        """
        if not self._is_connected or not self._api_key:
            return False
        try:
            if self._default_list_id:
                self._request("GET", f"/lists/{self._default_list_id}")
            else:
                self._request("GET", "/lists")
            return True
        except Exception:
            return False

    # -- HTTP helpers ---------------------------------------------------------

    def _resolve_list_id(self, list_id: Optional[str]) -> str:
        """Return the effective list id, preferring the per-call value."""
        effective = str(list_id) if list_id is not None else self._default_list_id
        if not effective:
            raise ConfigurationError(
                "No Tatango list id: pass list_id or construct the connector "
                "with default_list_id"
            )
        return effective

    def _throttle(self) -> None:
        """Sleep as needed to keep requests at least min_request_interval apart."""
        if self._last_request_at is not None and self._min_request_interval > 0:
            elapsed = time.monotonic() - self._last_request_at
            wait = self._min_request_interval - elapsed
            if wait > 0:
                logger.debug(f"Tatango pacing: sleeping {wait:.2f}s")
                time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Central HTTP method with pacing and error handling.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API path relative to /api/v2 (e.g. '/lists/123/subscribers')
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: On 401 responses
            RateLimitError: On 429 responses
            ConnectionError: On other HTTP errors (including WAF 403 body
                blocks), network failures, or non-JSON 2xx responses
        """
        if not self._is_connected and not self._api_key:
            self.connect()

        url = f"{TATANGO_API_BASE}{path}"
        self._throttle()

        try:
            resp = requests.request(
                method,
                url,
                auth=(self._login_email or "", self._api_key or ""),
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                params=params,
                json=json_body,
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Tatango API request failed: {e}") from e

        if resp.status_code == 401:
            raise AuthenticationError(
                f"Tatango authentication failed ({resp.status_code}): {resp.text}"
            )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 3))
            raise RateLimitError(
                f"Tatango rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(f"Tatango API error {resp.status_code}: {resp.text}")

        try:
            return resp.json()
        except ValueError as e:
            raise ConnectionError(
                f"Tatango API returned non-JSON response ({resp.status_code}): "
                f"{resp.text[:500]}"
            ) from e

    # -- Lists ------------------------------------------------------------------

    @retry_tatango_operation
    def list_lists(self, **params: Any) -> Dict[str, Any]:
        """
        List the account's lists.

        Returns the raw response body (pagination shape not live-verified;
        pass paging params through ``**params`` as needed).

        Returns:
            Raw response dict
        """
        result = self._request("GET", "/lists", params=params or None)
        return result or {}

    @retry_tatango_operation
    def get_list(self, list_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a list's configuration.

        Useful fields: ``opt_in_type`` (``single``/``double``) and ``counts``.

        Args:
            list_id: List id (falls back to default_list_id)

        Returns:
            List resource dict
        """
        effective = self._resolve_list_id(list_id)
        result = self._request("GET", f"/lists/{effective}")
        return result or {}

    # -- Subscribers --------------------------------------------------------------

    @retry_tatango_operation
    def list_subscribers(
        self, list_id: Optional[str] = None, **params: Any
    ) -> Dict[str, Any]:
        """
        List a list's subscribers (single page).

        Returns the raw response body — pagination shape is not
        live-verified, so no auto-pagination is attempted; pass paging
        params through ``**params``.

        Args:
            list_id: List id (falls back to default_list_id)
            **params: Query parameters passed through to the API

        Returns:
            Raw response dict
        """
        effective = self._resolve_list_id(list_id)
        result = self._request(
            "GET", f"/lists/{effective}/subscribers", params=params or None
        )
        return result or {}

    @retry_tatango_operation
    def get_subscriber(
        self, phone_number: str, list_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get a subscriber by phone number.

        A soft-opted-out subscriber still returns 200 (record retained
        with ``opted_out_at`` set). Useful fields: ``optin_in_progress``
        (pending double-opt-in flag), ``opt_in_method``, ``api_source``.
        ``subscribed_at`` survives opt-out/re-subscribe cycles — it is a
        first-subscribe timestamp, not a last-opt-in timestamp.

        Args:
            phone_number: Bare 10-digit phone (no country code)
            list_id: List id (falls back to default_list_id)

        Returns:
            Subscriber resource dict
        """
        effective = self._resolve_list_id(list_id)
        result = self._request("GET", f"/lists/{effective}/subscribers/{phone_number}")
        return result or {}

    @retry_tatango_operation
    def add_subscriber(
        self,
        phone_number: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        email: Optional[str] = None,
        zip_code: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
        bypass_opt_in_process: bool = False,
        bypass_opt_in_response: bool = False,
        list_id: Optional[str] = None,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """
        Add (opt in) a subscriber to a list.

        Without the bypass flags this is DOUBLE opt-in even on a
        single-opt-in list: the add lands ``optin_in_progress: true`` and
        Tatango texts a confirmation the person must answer ``YES`` to.
        Flag combos (all live-verified on the CC account):

        - ``bypass_opt_in_process=True, bypass_opt_in_response=True`` —
          silent add, immediately subscribed (the migration/bulk path).
        - ``bypass_opt_in_process=True, bypass_opt_in_response=False`` —
          immediately subscribed AND the list's response message is sent
          (the welcome-text path; wording is list-level config).

        ⚠️ **A refused add still returns HTTP 201.** The refusal is only
        in the returned ``status`` string — notably the ~48 h re-subscribe
        "security timeout" after any opt-out. Check ``status`` (or re-GET
        and check ``optin_in_progress``) before treating the add as done.

        Custom-field values ride as flat keys on the subscriber object and
        are coerced to the field's declared type (the field must already
        exist on the list — see :meth:`create_custom_field`).

        Args:
            phone_number: Bare 10-digit phone (no country code)
            first_name: First name
            last_name: Last name
            email: Email address
            zip_code: ZIP code
            custom_fields: Custom-field values keyed by field key
            bypass_opt_in_process: Skip the double-opt-in confirmation flow
            bypass_opt_in_response: Suppress the opt-in/response message
            list_id: List id (falls back to default_list_id)
            **extra_fields: Additional subscriber fields (e.g. tags)

        Returns:
            Response dict — inspect its ``status`` string
        """
        effective = self._resolve_list_id(list_id)

        subscriber: Dict[str, Any] = {"phone_number": phone_number}
        if first_name:
            subscriber["first_name"] = first_name
        if last_name:
            subscriber["last_name"] = last_name
        if email:
            subscriber["email"] = email
        if zip_code:
            subscriber["zip_code"] = zip_code
        if bypass_opt_in_process:
            subscriber["bypass_opt_in_process"] = True
            subscriber["bypass_opt_in_response"] = bypass_opt_in_response
        if custom_fields:
            subscriber.update(custom_fields)
        subscriber.update(extra_fields)

        result = self._request(
            "POST",
            f"/lists/{effective}/subscribers",
            json_body={"subscriber": subscriber},
        )
        return result or {}

    @retry_tatango_operation
    def update_subscriber(
        self,
        phone_number: str,
        fields: Dict[str, Any],
        list_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a subscriber in place (selective PUT).

        Omitted fields are left untouched; custom-field values update in
        place (this is tatango-sync Flow 5's nightly re-sync mechanism —
        no delete-and-re-add). Per Tatango docs, tags are *additive* on
        update. Datetime custom fields accept a plain ISO date
        (``"2026-07-15"``) and store it as midnight UTC.

        Args:
            phone_number: Bare 10-digit phone (no country code)
            fields: Fields / custom-field values to update
            list_id: List id (falls back to default_list_id)

        Returns:
            Updated subscriber response dict
        """
        effective = self._resolve_list_id(list_id)
        body = {"subscriber": {"phone_number": phone_number, **fields}}
        result = self._request(
            "PUT", f"/lists/{effective}/subscribers/{phone_number}", json_body=body
        )
        return result or {}

    @retry_tatango_operation
    def delete_subscriber(
        self, phone_number: str, list_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Unsubscribe (soft opt-out) a subscriber.

        The record is retained with ``opted_out_at`` set; a later GET
        returns 200, not 404. Two consequences the caller must own:

        - **The** ``unsubscribe`` **webhook fires**, with a payload
          indistinguishable from an organic STOP — log the write to a
          self-write ledger *before* calling if you consume that webhook
          (tatango-sync Flow 2 echo suppression).
        - **A ~48 h re-subscribe cooldown starts**: a re-add inside the
          window is refused (inside an HTTP 201 — see
          :meth:`add_subscriber`).

        Args:
            phone_number: Bare 10-digit phone (no country code)
            list_id: List id (falls back to default_list_id)

        Returns:
            Response dict (observed ``status``: "successfully unsubscribed")
        """
        effective = self._resolve_list_id(list_id)
        result = self._request(
            "DELETE", f"/lists/{effective}/subscribers/{phone_number}"
        )
        return result or {}

    # -- Custom fields ------------------------------------------------------------

    @retry_tatango_operation
    def list_custom_fields(self, list_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get a list's custom-field schema.

        Args:
            list_id: List id (falls back to default_list_id)

        Returns:
            Raw response dict
        """
        effective = self._resolve_list_id(list_id)
        result = self._request("GET", f"/lists/{effective}/custom_fields")
        return result or {}

    @retry_tatango_operation
    def create_custom_field(
        self,
        key: str,
        label: str,
        content_type: str,
        max_length: int = 100,
        list_id: Optional[str] = None,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        """
        Create a custom field on a list.

        Two doc-drift gotchas found live (2026-08-05): the create path is
        **plural** (``/custom_fields`` — the documented singular path
        404s), and ``max_length`` is **required** despite the docs calling
        it optional (422 "Max. characters must be greater than 0").

        Args:
            key: Field key (used for inline value writes on subscribers)
            label: Display label
            content_type: One of 'text', 'datetime', 'number'
            max_length: Max characters — must be > 0
            list_id: List id (falls back to default_list_id)
            **extra_fields: Additional field attributes (merge tag, etc.)

        Returns:
            Created custom-field response dict
        """
        effective = self._resolve_list_id(list_id)
        body = {
            "custom_field": {
                "key": key,
                "label": label,
                "content_type": content_type,
                "max_length": max_length,
                **extra_fields,
            }
        }
        result = self._request(
            "POST", f"/lists/{effective}/custom_fields", json_body=body
        )
        return result or {}

    # -- Webhooks -------------------------------------------------------------

    @retry_tatango_operation
    def list_webhooks(self, list_id: Optional[str] = None) -> Dict[str, Any]:
        """
        List a list's webhook registrations.

        Args:
            list_id: List id (falls back to default_list_id)

        Returns:
            Raw response dict
        """
        effective = self._resolve_list_id(list_id)
        result = self._request("GET", f"/lists/{effective}/webhooks")
        return result or {}

    @retry_tatango_operation
    def create_webhook(
        self,
        callback_url: str,
        subscribe: bool = True,
        unsubscribe: bool = True,
        message_sent: bool = False,
        cleaned: bool = True,
        reply_received: bool = False,
        list_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Register a webhook on a list.

        All five event types are settable via the API (live-verified —
        ``cleaned``/``reply_received`` are not UI-only). Registration is
        per-list. Delivery facts for receiver design: no HMAC/signature
        header (trust = URL secret + IP allowlist), 10 retries on failure,
        and duplicate delivery happens even on success (same ``opt_id``
        20 ms apart) — dedupe on ``opt_id`` at read time.

        Args:
            callback_url: Receiver URL (put a secret in the path/query)
            subscribe: Fire on subscribes (on *confirmation*, not pending add)
            unsubscribe: Fire on unsubscribes (API deletes echo here too)
            message_sent: Fire on mass-message sends
            cleaned: Fire on carrier cleans (opt-out-equivalent downstream)
            reply_received: Fire on replies to mass messages
            list_id: List id (falls back to default_list_id)

        Returns:
            Created webhook response dict (note its id for deletion)
        """
        effective = self._resolve_list_id(list_id)
        body = {
            "webhook": {
                "callback_url": callback_url,
                "subscribe": subscribe,
                "unsubscribe": unsubscribe,
                "message_sent": message_sent,
                "cleaned": cleaned,
                "reply_received": reply_received,
            }
        }
        result = self._request(
            "POST", f"/lists/{effective}/webhooks", json_body=body
        )
        return result or {}

    @retry_tatango_operation
    def delete_webhook(
        self, webhook_id: str, list_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Delete a webhook registration.

        Args:
            webhook_id: Webhook id (from create/list responses)
            list_id: List id (falls back to default_list_id)

        Returns:
            Response dict, or None on 204
        """
        effective = self._resolve_list_id(list_id)
        return self._request("DELETE", f"/lists/{effective}/webhooks/{webhook_id}")
