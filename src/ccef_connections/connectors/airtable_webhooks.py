"""
Airtable **webhooks** connector for CCEF connections library.

Separate from ``AirtableConnector`` on purpose, and the reason is the dependency
rather than tidiness: ``AirtableConnector`` imports pyairtable at module level,
so it lives behind the ``[airtable]`` extra. The webhooks API is plain REST over
``requests``, and the whole point of a webhook backstop is that it reads the
**payload stream**, never records — so a consumer that only wants live change
notification should not have to install pyairtable to get one.

## What a webhook actually delivers

🚨 **An Airtable webhook notification is only a ping.** It carries the base id,
the webhook id and a timestamp — and **no data at all**. The cell values live in
a cursor-ordered payload stream you then pull:

    GET /v0/bases/{baseId}/webhooks/{webhookId}/payloads?cursor=N

This is what makes a backstop cheap. A receiver that was down for twenty minutes
has missed nothing: the cursor is still where it was, and replaying from it
returns the same payloads the pings would have announced, through the same
parsing code. Payloads are retained **7 days**.

## Two properties that bite, both silent

1. 🚨 **Registrations expire.** The ceiling is 7 days, and ``refresh()`` resets
   it. An expired registration stops notifying with no error anywhere — the
   consumer just goes quiet, which is indistinguishable from nothing happening.
   Anything built on this must track ``expiration_time`` and say so out loud.
   ``list_webhooks`` returns it for exactly that reason.

2. 🚨 **Payloads carry field IDs by default**, not field names — ``fldXXXX`` keys
   that match nothing a human or a schema mapping knows. ``create_webhook``
   therefore defaults ``include_field_names=True``, which sets the API's
   ``includes.includeCellValuesInFieldIdsOrNames: "names"``. A webhook created
   elsewhere without it produces payloads that parse to an empty row with no
   error raised.

## MAC verification

Airtable signs each notification with the ``macSecretBase64`` returned **once**,
at creation. It is never retrievable again — store it when you create the hook or
you cannot verify anything it sends. ``verify_mac`` is a module-level function so
a receiver can check a request without constructing a connector or holding a PAT.

PII: an EP incident-report payload carries voter name, address and phone. Nothing
in this module logs a cell value — log lines carry ids, counts and field names.
"""

import base64
import hashlib
import hmac
import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

API_ROOT = "https://api.airtable.com/v0"

# Airtable's own ceiling. Sending more than this is rejected, and sending
# nothing defaults to 7 days anyway; it is named here so a caller can compute a
# refresh cadence against it rather than rediscovering it.
MAX_EXPIRATION_DAYS = 7

# Payloads per page. Airtable's own maximum is 50.
PAYLOAD_PAGE = 50


def verify_mac(mac_secret_base64: str, body: bytes, header_value: str) -> bool:
    """Whether ``header_value`` is Airtable's signature over ``body``.

    ``mac_secret_base64`` is the ``macSecretBase64`` handed back by
    ``create_webhook``. The header is ``X-Airtable-Content-MAC`` and looks like
    ``hmac-sha256=<hex>``.

    The comparison is ``hmac.compare_digest``, not ``==``: a receiver is a public
    endpoint and string equality leaks the signature a byte at a time through
    timing. Any malformed input is False rather than an exception, because a
    receiver must answer a hostile request without a traceback.

    🚨 ``body`` must be the EXACT bytes Airtable sent. Re-serializing the parsed
    JSON changes whitespace and key order and the MAC will not match — in Flask
    that means ``request.get_data()``, never ``json.dumps(request.json)``.
    """
    if not mac_secret_base64 or not header_value:
        return False
    try:
        secret = base64.b64decode(mac_secret_base64)
    except Exception:
        return False
    expected = "hmac-sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header_value)


class AirtableWebhooksConnector(BaseConnection):
    """Create, refresh, list and drain Airtable webhooks over plain REST.

    Examples:
        >>> with AirtableWebhooksConnector() as hooks:
        ...     hook = hooks.create_webhook(
        ...         "appXXX",
        ...         "https://example.org/webhook/airtable",
        ...         table_id="tblXXX",
        ...     )
        ...     # store hook["macSecretBase64"] NOW — it is never returned again
        ...     batch = hooks.get_payloads("appXXX", hook["id"], cursor=1)
    """

    def __init__(self, timeout: int = 30) -> None:
        super().__init__()
        self._session: Optional[requests.Session] = None
        self._timeout = timeout

    # -- lifecycle ------------------------------------------------------------

    def connect(self) -> None:
        """Build an authenticated session.

        No network call: Airtable has no whoami endpoint, so "connected" means
        "a token was found", and the first real request is what proves it works.
        Claiming more than that here would be its own small lie.
        """
        try:
            token = self._credential_manager.get_airtable_key()
        except CredentialError:
            logger.error("Airtable webhooks: missing AIRTABLE_API_KEY credential")
            raise
        session = requests.Session()
        session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )
        self._session = session
        self._is_connected = True
        logger.info("Airtable webhooks connector ready")

    def disconnect(self) -> None:
        if self._session is not None:
            self._session.close()
        self._session = None
        self._is_connected = False

    def health_check(self) -> bool:
        return self._is_connected and self._session is not None

    # -- plumbing -------------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        """One API call, with Airtable's error codes mapped to ours.

        403 is called out separately because it is the error every Airtable
        integration meets first and it is ambiguous by design: Airtable returns
        INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND for a token missing the base, a
        token missing a scope, AND a base id that does not exist. The message
        says which three things to check rather than asserting one of them.
        """
        if not self._is_connected or self._session is None:
            self.connect()
        assert self._session is not None

        response = self._session.request(
            method, f"{API_ROOT}{path}", timeout=self._timeout, **kwargs
        )

        if response.status_code == 401:
            raise AuthenticationError("Airtable rejected the PAT (401)")
        if response.status_code == 403:
            raise AuthenticationError(
                f"Airtable 403 on {method} {path}. A PAT is scoped per base AND "
                "per scope: check the base is on the token's access list, that "
                "the token carries webhook:manage, and that the base id is right "
                "— all three return this same error."
            )
        if response.status_code == 429:
            raise RateLimitError("Airtable rate limit (429)")
        if response.status_code >= 400:
            # Body, not just the code: Airtable puts the useful part in
            # {"error": {"type": ..., "message": ...}} and it names no cell data.
            raise ConnectionError(
                f"Airtable {response.status_code} on {method} {path}: "
                f"{response.text[:400]}"
            )
        if not response.content:
            return {}
        return response.json()

    # -- schema ---------------------------------------------------------------

    def list_tables(self, base_id: str) -> List[Dict[str, Any]]:
        """Every table in a base: ``id``, ``name``, ``fields`` and more.

        Here rather than only on ``AirtableConnector`` because scoping a webhook
        to one table needs that table's ID, and a caller should not have to
        install pyairtable to look one up. Requires ``schema.bases:read`` on the
        PAT — a scope distinct from ``webhook:manage``, so a token can create a
        webhook and still fail here.
        """
        return self._request("GET", f"/meta/bases/{base_id}/tables").get("tables", [])

    # -- registration ---------------------------------------------------------

    def list_webhooks(self, base_id: str) -> List[Dict[str, Any]]:
        """Every webhook registered on a base.

        Each row carries ``id``, ``notificationUrl``, ``expirationTime``,
        ``isHookEnabled`` and ``cursorForNextPayload``. It does NOT carry
        ``macSecretBase64`` — that is returned only at creation.

        ``lastSuccessfulNotificationResult`` is worth reading on a schedule: it
        is how you find a hook Airtable has been failing to deliver to, which
        otherwise looks exactly like a base nobody is filing reports in.
        """
        return self._request("GET", f"/bases/{base_id}/webhooks").get("webhooks", [])

    def create_webhook(
        self,
        base_id: str,
        notification_url: str,
        table_id: Optional[str] = None,
        data_types: Optional[List[str]] = None,
        include_field_names: bool = True,
        include_previous_values: bool = False,
    ) -> Dict[str, Any]:
        """Register a webhook and return Airtable's response.

        🚨 The response carries ``macSecretBase64`` and this is the ONLY time it
        is ever returned. Persist it in the same step that calls this. Without it
        every future notification is unverifiable, and the only remedy is to
        delete the registration and make a new one.

        ``table_id`` scopes notification to one table (``recordChangeScope``).
        Leave it None to watch the whole base — which for an EP field-report base
        means quiz rows and polling-place edits ping too, so pass it when you
        know the table.

        ``include_field_names`` sets cell values to arrive keyed by field NAME.
        See the module docstring: the default the API applies is field IDs, which
        parse to nothing without a schema fetch to translate them.
        """
        options: Dict[str, Any] = {
            "filters": {"dataTypes": data_types or ["tableData"]},
            "includes": {
                "includeCellValuesInFieldIdsOrNames": (
                    "names" if include_field_names else "ids"
                ),
                "includePreviousCellValues": include_previous_values,
            },
        }
        if table_id:
            options["filters"]["recordChangeScope"] = table_id

        payload = {
            "notificationUrl": notification_url,
            "specification": {"options": options},
        }
        result = self._request("POST", f"/bases/{base_id}/webhooks", json=payload)
        logger.info(
            "Created Airtable webhook %s on base %s (table=%s)",
            result.get("id"), base_id, table_id or "<all>",
        )
        return result

    def refresh_webhook(self, base_id: str, webhook_id: str) -> Dict[str, Any]:
        """Reset a registration's expiry clock, returning the new expiry.

        Must be called well inside the 7-day ceiling. There is no "about to
        expire" notification from Airtable — expiry is silent, which is why this
        belongs on a schedule rather than in a runbook step someone remembers.
        """
        return self._request(
            "POST", f"/bases/{base_id}/webhooks/{webhook_id}/refresh"
        )

    def delete_webhook(self, base_id: str, webhook_id: str) -> None:
        self._request("DELETE", f"/bases/{base_id}/webhooks/{webhook_id}")
        logger.info("Deleted Airtable webhook %s on base %s", webhook_id, base_id)

    # -- the payload stream ---------------------------------------------------

    def get_payloads(
        self,
        base_id: str,
        webhook_id: str,
        cursor: Optional[int] = None,
        limit: int = PAYLOAD_PAGE,
    ) -> Dict[str, Any]:
        """One page of the payload stream.

        Returns ``{"payloads": [...], "cursor": <next>, "mightHaveMore": bool}``.

        ``cursor`` is where to read FROM; the returned ``cursor`` is where to
        read from next and is what you persist. Omitting it starts at the oldest
        payload Airtable still holds (7 days), which is a replay of the whole
        retention window — fine for a first run against a quiet base, expensive
        and duplicative against a busy one, so pass the stored cursor once you
        have one.

        🚨 ``mightHaveMore`` is the loop condition, NOT an empty ``payloads``
        list. Airtable can return a page with no payloads and more behind it, so
        stopping on empty silently truncates the catch-up this whole mechanism
        exists to perform. ``iter_payloads`` does this correctly.
        """
        params: Dict[str, Any] = {"limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        return self._request(
            "GET", f"/bases/{base_id}/webhooks/{webhook_id}/payloads", params=params
        )

    def iter_payloads(
        self,
        base_id: str,
        webhook_id: str,
        cursor: Optional[int] = None,
        max_pages: int = 200,
    ):
        """Walk the stream from ``cursor`` to the end, yielding (payload, cursor).

        The yielded cursor is the one to persist **after** that payload is
        handled, so a crash mid-page replays the payload rather than skipping it.
        At-least-once beats at-most-once here: these payloads become rows keyed
        by record id, so a duplicate upserts to the same row, while a skip is a
        field report that silently never existed.

        ``max_pages`` bounds a stream that keeps reporting ``mightHaveMore`` —
        at 50 payloads a page the default covers 10,000 payloads, far past any
        real catch-up, and stopping beats looping forever inside a cron job.
        """
        pages = 0
        while pages < max_pages:
            batch = self.get_payloads(base_id, webhook_id, cursor=cursor)
            next_cursor = batch.get("cursor")
            for payload in batch.get("payloads", []):
                yield payload, next_cursor
            cursor = next_cursor
            pages += 1
            if not batch.get("mightHaveMore"):
                return
        logger.warning(
            "Stopped draining base %s webhook %s at the %d-page bound; "
            "more payloads remain",
            base_id, webhook_id, max_pages,
        )
