"""
Zendesk connector for CCEF connections library.

Provides read access to a Zendesk Suite instance's ticketing and configuration
objects, plus ticket create/update for automated ticket generation.

Uses OAuth2 **client_credentials** with direct HTTP via the requests library.

Why not API tokens: Zendesk is removing API tokens as an authentication method
(creation blocked 2026-10-27, all tokens deactivated 2027-04-30, and tokens idle
30+ days auto-deactivate as of 2026-07-28). An idle-deactivated token fails
silently in an unattended job, so token/basic auth is deliberately not supported
here -- not even as a fallback.
See https://developer.zendesk.com/documentation/authentication/oauth-migration/

Two operational notes specific to CCEF's instance (commoncause.zendesk.com):

1. It is a SHARED instance -- Campaigns lives inside IT's Zendesk. The ~400/min
   rate budget is instance-wide and IT's own provisioning automation already
   trips occasional 429s on its own. This connector therefore self-throttles
   below the ceiling by default (see ``max_requests_per_minute``) and honors
   ``Retry-After`` rather than racing IT for headroom.
2. The issued access token acts as the OAuth client's OWNING USER, so that
   user's role -- not the requested scope -- is the real permission ceiling.
   Scope is an additional, narrower cap on top of it.
3. To write, request the scope string ``"read write"`` -- NOT ``"write"``.
   Zendesk happily issues a token for scope ``"write"`` alone, and that token
   then 403s on every call with "missing the following required scopes:
   users:read, read", because even a write request reads the acting user first.
   The failure looks like a permissions problem on the endpoint; it is actually
   the scope string. Verified live against commoncause 2026-08-20.

Examples:
    >>> connector = ZendeskConnector()
    >>> connector.connect()
    >>> groups = connector.list_groups()
    >>> agents = connector.list_agents()
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_zendesk_operation
from ..exceptions import AuthenticationError, ConfigurationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

ZENDESK_TOKEN_URL_TEMPLATE = "https://{subdomain}.zendesk.com/oauth/tokens"
ZENDESK_API_BASE_TEMPLATE = "https://{subdomain}.zendesk.com/api/v2"

# Read-only by default. Callers that need to mutate anything must opt in
# explicitly by passing ZENDESK_READ_WRITE_SCOPE.
ZENDESK_DEFAULT_SCOPE = "read"

# The correct scope string for mutation. "write" on its own is NOT usable --
# see note 3 in the module docstring.
ZENDESK_READ_WRITE_SCOPE = "read write"

# Self-throttle default, well under the instance-wide ~400/min shared with IT.
ZENDESK_DEFAULT_MAX_RPM = 120

# Refresh this many seconds before the token actually expires.
_TOKEN_EXPIRY_BUFFER = 60


class ZendeskConnector(BaseConnection):
    """
    Zendesk connector using OAuth2 client_credentials.

    Attributes:
        subdomain: The Zendesk subdomain (e.g. 'commoncause')
        scope: OAuth scope requested for issued tokens

    Examples:
        >>> with ZendeskConnector() as zd:
        ...     forms = zd.list_ticket_forms()
    """

    def __init__(
        self,
        subdomain: Optional[str] = None,
        scope: str = ZENDESK_DEFAULT_SCOPE,
        max_requests_per_minute: int = ZENDESK_DEFAULT_MAX_RPM,
    ) -> None:
        """
        Initialize the Zendesk connector.

        Args:
            subdomain: Zendesk subdomain. Defaults to the ZENDESK_SUBDOMAIN
                environment variable.
            scope: OAuth scope to request. Defaults to 'read'. Requesting a
                scope beyond the OAuth client's configured "Allowed scopes"
                ceiling fails with invalid_scope -- that is the intended
                guardrail, not a bug.
            max_requests_per_minute: Client-side throttle. The instance budget
                is shared with IT's automation, so this stays deliberately
                below the account ceiling.
        """
        super().__init__()
        self.subdomain = subdomain
        self.scope = scope
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._min_interval = 60.0 / max_requests_per_minute if max_requests_per_minute else 0.0
        self._last_request_at: float = 0.0
        self._rate_limit_seen: Dict[str, str] = {}

    # ── Connection lifecycle ──────────────────────────────────────────

    def _resolve_subdomain(self) -> str:
        """
        Return the configured subdomain, or raise.

        Returns:
            The Zendesk subdomain

        Raises:
            ConfigurationError: If no subdomain was provided or configured
        """
        subdomain = self.subdomain or os.getenv("ZENDESK_SUBDOMAIN")
        if not subdomain:
            raise ConfigurationError(
                "No Zendesk subdomain configured. Pass subdomain= or set "
                "ZENDESK_SUBDOMAIN in the environment/.env."
            )
        self.subdomain = subdomain
        return subdomain

    @property
    def api_base(self) -> str:
        """The instance's /api/v2 base URL."""
        return ZENDESK_API_BASE_TEMPLATE.format(subdomain=self._resolve_subdomain())

    @property
    def token_url(self) -> str:
        """The instance's OAuth token endpoint (NOT under /api/v2)."""
        return ZENDESK_TOKEN_URL_TEMPLATE.format(subdomain=self._resolve_subdomain())

    def connect(self) -> None:
        """
        Establish connection by obtaining an OAuth2 access token.

        Raises:
            CredentialError: If Zendesk credentials are missing or invalid
            ConfigurationError: If no subdomain is configured
            AuthenticationError: If the token request fails
            ConnectionError: If connection setup fails
        """
        try:
            creds = self._credential_manager.get_zendesk_credentials()
            self._fetch_token(creds["client_id"], creds["client_secret"])
            self._is_connected = True
            logger.info(
                f"Successfully connected to Zendesk ({self.subdomain}, scope={self.scope!r})"
            )
        except (AuthenticationError, ConfigurationError):
            logger.error("Failed to connect to Zendesk: authentication/configuration error")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Zendesk: {str(e)}")
            raise ConnectionError(f"Failed to connect to Zendesk: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the Zendesk connection and token."""
        self._access_token = None
        self._token_expires_at = 0.0
        self._is_connected = False
        logger.debug("Disconnected from Zendesk")

    def health_check(self) -> bool:
        """
        Check if the Zendesk connection is healthy.

        Verifies the token is valid by calling GET /users/me.json.

        Returns:
            True if connected and the token is valid, False otherwise
        """
        if not self._is_connected or not self._access_token:
            return False
        try:
            self._request("GET", "/users/me.json")
            return True
        except Exception:
            return False

    # ── Token management ──────────────────────────────────────────────

    def _fetch_token(self, client_id: str, client_secret: str) -> None:
        """
        Fetch an OAuth2 access token using the client_credentials grant.

        Args:
            client_id: The OAuth client's *Identifier* field (an author-chosen
                slug), NOT its numeric id
            client_secret: The OAuth client secret

        Raises:
            AuthenticationError: If the token request fails
            ConnectionError: If the token endpoint is unreachable
        """
        try:
            resp = requests.post(
                self.token_url,
                json={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": self.scope,
                },
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to reach Zendesk token endpoint: {e}") from e

        if resp.status_code != 200:
            hint = ""
            body = resp.text or ""
            if "invalid_client" in body:
                hint = (
                    " -- check that client_id is the OAuth client's Identifier "
                    "field (not its numeric id) and that the client kind is "
                    "Confidential; client_credentials refuses Public clients."
                )
            elif "invalid_scope" in body:
                hint = (
                    f" -- scope {self.scope!r} exceeds the OAuth client's "
                    "Allowed scopes ceiling."
                )
            raise AuthenticationError(
                f"Zendesk OAuth2 token request failed ({resp.status_code}): {body}{hint}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        # Zendesk issues 1800s by default and does NOT return a refresh token
        # for client_credentials -- re-request instead.
        self._token_expires_at = time.time() + data.get("expires_in", 1800) - _TOKEN_EXPIRY_BUFFER
        logger.debug(f"Zendesk OAuth2 token obtained (scope={data.get('scope')})")

    def _refresh_token_if_needed(self) -> None:
        """Re-fetch the access token if it has expired (no refresh token exists)."""
        if time.time() >= self._token_expires_at:
            logger.debug("Zendesk token expired, re-requesting")
            creds = self._credential_manager.get_zendesk_credentials()
            self._fetch_token(creds["client_id"], creds["client_secret"])

    def _get_headers(self) -> Dict[str, str]:
        """
        Return authorization headers, re-requesting the token if needed.

        Returns:
            Dict with Authorization and Content-Type headers
        """
        self._refresh_token_if_needed()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    # ── HTTP helpers ──────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Space requests out to stay under the self-imposed rate ceiling."""
        if not self._min_interval:
            return
        elapsed = time.time() - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

    def _record_rate_limit(self, resp: requests.Response) -> None:
        """
        Capture rate-limit headers for observability.

        The instance returns both the standard ``ratelimit-*`` headers and the
        legacy ``x-rate-limit*`` pair; prefer the standard ones.
        """
        for header in (
            "ratelimit-limit",
            "ratelimit-remaining",
            "ratelimit-reset",
            "x-rate-limit",
            "x-rate-limit-remaining",
        ):
            if header in resp.headers:
                self._rate_limit_seen[header] = resp.headers[header]

    @property
    def rate_limit_status(self) -> Dict[str, str]:
        """Rate-limit headers seen on the most recent response."""
        return dict(self._rate_limit_seen)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Central HTTP method with auth headers, throttling, and 401 auto-refresh.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API path relative to /api/v2 (e.g. '/groups.json'), or an
                absolute URL (pagination links are absolute)
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: If authentication fails after re-requesting
            RateLimitError: If rate limited by the API
            ConnectionError: If the request fails
        """
        if not self._is_connected and not self._access_token:
            self.connect()

        url = path if path.startswith("http") else f"{self.api_base}{path}"

        def _send() -> requests.Response:
            self._throttle()
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
                raise ConnectionError(f"Zendesk API request failed: {e}") from e
            finally:
                self._last_request_at = time.time()
            self._record_rate_limit(resp)
            return resp

        resp = _send()

        # Re-request the token on 401 and retry once.
        if resp.status_code == 401:
            logger.debug("Received 401, re-requesting token and retrying")
            self._token_expires_at = 0.0  # Force re-request
            resp = _send()
            if resp.status_code == 401:
                raise AuthenticationError("Zendesk authentication failed after token refresh")

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            raise RateLimitError(
                f"Zendesk rate limit exceeded, retry after {retry_after}s "
                f"(budget is instance-wide and shared with IT automation)",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(f"Zendesk API error {resp.status_code}: {resp.text}")

        return resp.json()

    @staticmethod
    def _next_url(data: Dict[str, Any]) -> Optional[str]:
        """
        Return the next page URL, supporting both pagination styles.

        Zendesk endpoints use either cursor-based pagination (``meta.has_more``
        plus ``links.next``) or the older offset pagination (``next_page``);
        which one applies varies by endpoint, so handle both.

        Args:
            data: A parsed list-endpoint response

        Returns:
            Absolute URL of the next page, or None if this is the last page
        """
        meta = data.get("meta") or {}
        if meta.get("has_more"):
            return (data.get("links") or {}).get("next")
        return data.get("next_page")

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        resource_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Follow pagination links and return every record.

        Page size is deliberately left to Zendesk's default: not every endpoint
        accepts ``page[size]``, and sending it where unsupported can 400.

        Args:
            path: Initial API path (e.g. '/groups.json')
            params: Query parameters for the first request only -- subsequent
                pages come from absolute links that already carry them
            resource_key: Response key holding the records (e.g. 'groups').
                Inferred from the first list-valued key when omitted.

        Returns:
            Combined list of records across all pages
        """
        results: List[Dict[str, Any]] = []
        next_target: Optional[str] = path
        first = True

        while next_target:
            data = self._request("GET", next_target, params=params if first else None)
            first = False
            if data is None:
                break

            key = resource_key
            if key is None:
                key = next(
                    (k for k, v in data.items() if isinstance(v, list)),
                    None,
                )
            if key and isinstance(data.get(key), list):
                results.extend(data[key])

            next_target = self._next_url(data)

        return results

    # ── Identity & account ────────────────────────────────────────────

    @retry_zendesk_operation
    def get_me(self) -> Dict[str, Any]:
        """
        Return the user the access token acts as.

        This is the effective permission ceiling: the token inherits the OAuth
        client owner's role.

        Returns:
            The user record
        """
        return self._request("GET", "/users/me.json")["user"]

    @retry_zendesk_operation
    def get_account_settings(self) -> Dict[str, Any]:
        """
        Return instance-wide account settings (plan-gated feature flags).

        Returns:
            The settings record
        """
        return self._request("GET", "/account/settings.json")["settings"]

    # ── Configuration objects (read) ──────────────────────────────────

    @retry_zendesk_operation
    def list_groups(self) -> List[Dict[str, Any]]:
        """Return all groups. Returns: list of group records."""
        return self._paginate("/groups.json", resource_key="groups")

    @retry_zendesk_operation
    def list_ticket_forms(self) -> List[Dict[str, Any]]:
        """Return all ticket forms. Returns: list of ticket form records."""
        return self._paginate("/ticket_forms.json", resource_key="ticket_forms")

    @retry_zendesk_operation
    def list_ticket_fields(self) -> List[Dict[str, Any]]:
        """Return all ticket fields. Returns: list of ticket field records."""
        return self._paginate("/ticket_fields.json", resource_key="ticket_fields")

    @retry_zendesk_operation
    def list_views(self) -> List[Dict[str, Any]]:
        """Return all views. Returns: list of view records."""
        return self._paginate("/views.json", resource_key="views")

    @retry_zendesk_operation
    def list_triggers(self) -> List[Dict[str, Any]]:
        """Return all ticket triggers. Returns: list of trigger records."""
        return self._paginate("/triggers.json", resource_key="triggers")

    @retry_zendesk_operation
    def list_automations(self) -> List[Dict[str, Any]]:
        """Return all automations. Returns: list of automation records."""
        return self._paginate("/automations.json", resource_key="automations")

    @retry_zendesk_operation
    def list_macros(self) -> List[Dict[str, Any]]:
        """Return all macros. Returns: list of macro records."""
        return self._paginate("/macros.json", resource_key="macros")

    @retry_zendesk_operation
    def list_sla_policies(self) -> List[Dict[str, Any]]:
        """Return all SLA policies. Returns: list of SLA policy records."""
        return self._paginate("/slas/policies.json", resource_key="sla_policies")

    @retry_zendesk_operation
    def list_brands(self) -> List[Dict[str, Any]]:
        """Return all brands. Returns: list of brand records."""
        return self._paginate("/brands.json", resource_key="brands")

    @retry_zendesk_operation
    def list_custom_roles(self) -> List[Dict[str, Any]]:
        """
        Return all custom agent roles.

        Suite Growth nominally lacks custom agent roles (Enterprise-only), so
        an empty list here is the expected result rather than an error.

        Returns:
            List of custom role records
        """
        return self._paginate("/custom_roles.json", resource_key="custom_roles")

    # ── People ────────────────────────────────────────────────────────

    @retry_zendesk_operation
    def list_users(self, role: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Return users, optionally filtered by role.

        NOTE: user records contain names and email addresses (row-level PII).
        Do not commit raw output -- see the library's PII policy.

        Args:
            role: Optional role filter ('end-user', 'agent', 'admin')

        Returns:
            List of user records
        """
        params = {"role": role} if role else None
        return self._paginate("/users.json", params=params, resource_key="users")

    def list_agents(self) -> List[Dict[str, Any]]:
        """
        Return agent-role users (seat consumers).

        Admins hold agent seats too but report role 'admin', so query both to
        get a true seat count.

        Returns:
            List of agent and admin user records
        """
        return self.list_users(role="agent") + self.list_users(role="admin")

    @retry_zendesk_operation
    def list_organizations(self) -> List[Dict[str, Any]]:
        """Return all organizations. Returns: list of organization records."""
        return self._paginate("/organizations.json", resource_key="organizations")

    # ── Tickets (read) ────────────────────────────────────────────────

    @retry_zendesk_operation
    def get_ticket(self, ticket_id: int) -> Dict[str, Any]:
        """
        Return a single ticket.

        Args:
            ticket_id: The ticket's numeric id

        Returns:
            The ticket record
        """
        return self._request("GET", f"/tickets/{ticket_id}.json")["ticket"]

    @retry_zendesk_operation
    def list_tickets(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Return tickets.

        Args:
            params: Optional query parameters (e.g. sort/side-load options)

        Returns:
            List of ticket records
        """
        return self._paginate("/tickets.json", params=params, resource_key="tickets")

    @retry_zendesk_operation
    def list_group_tickets(self, group_id: int) -> List[Dict[str, Any]]:
        """
        Return tickets assigned to one group.

        Args:
            group_id: The group's numeric id

        Returns:
            List of ticket records
        """
        return self._paginate(f"/groups/{group_id}/tickets.json", resource_key="tickets")

    @retry_zendesk_operation
    def search_count(self, query: str) -> int:
        """
        Return how many results a search query matches, without fetching them.

        Much cheaper than paginating a result set just to size it, and it
        returns no record bodies -- so it is the safe way to ask "is this
        config object actually in use?" without pulling PII.

        Args:
            query: A Zendesk search query (e.g. 'type:ticket ticket_form_id:123')

        Returns:
            The match count
        """
        return self._request("GET", "/search/count.json", params={"query": query})["count"]

    # ── Tickets (write) ───────────────────────────────────────────────
    #
    # These require the 'read write' scope. The default 'read' scope is
    # intentional: CCEF's instance is shared with IT, so read and write are
    # separated by CREDENTIAL rather than by convention.

    @retry_zendesk_operation
    def create_ticket(self, ticket: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a single ticket.

        Args:
            ticket: The ticket payload (subject, comment, group_id, ...)

        Returns:
            The created ticket record
        """
        return self._request("POST", "/tickets.json", json_body={"ticket": ticket})["ticket"]

    @retry_zendesk_operation
    def update_ticket(self, ticket_id: int, ticket: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a single ticket.

        Args:
            ticket_id: The ticket's numeric id
            ticket: The fields to change

        Returns:
            The updated ticket record
        """
        return self._request(
            "PUT", f"/tickets/{ticket_id}.json", json_body={"ticket": ticket}
        )["ticket"]

    @retry_zendesk_operation
    def create_many_tickets(self, tickets: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create up to 100 tickets in one asynchronous job.

        Far more rate-efficient than N single creates -- the preferred path for
        the proactive engine's election-prep batches.

        Args:
            tickets: Ticket payloads (max 100 per call)

        Returns:
            The job status record; poll it with get_job_status()

        Raises:
            ValueError: If more than 100 tickets are supplied
        """
        if len(tickets) > 100:
            raise ValueError(
                f"create_many_tickets accepts at most 100 tickets per call, got {len(tickets)}"
            )
        return self._request(
            "POST", "/tickets/create_many.json", json_body={"tickets": tickets}
        )["job_status"]

    @retry_zendesk_operation
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Return the status of an asynchronous job.

        Args:
            job_id: The job status id returned by a bulk operation

        Returns:
            The job status record
        """
        return self._request("GET", f"/job_statuses/{job_id}.json")["job_status"]

    # ── Configuration objects (write) ─────────────────────────────────
    #
    # These exist to support reviewed, idempotent config-as-code. They are
    # deliberately NARROW: every mutation targets ONE object, addressed by an
    # explicit id the caller had to look up first.
    #
    # What is deliberately absent, and must stay absent:
    #   - delete/destroy of any config object
    #   - bulk or "reconcile everything" helpers
    #   - anything that enumerates config and writes back what it found
    #
    # CCEF's instance is SHARED with IT, whose own automation owns most of the
    # triggers, macros, views, groups and fields in it. An enumerate-and-write
    # helper here would be one bug away from clobbering IT's production
    # helpdesk config. Namespace policy (which objects a project may touch)
    # belongs in that project's reviewed apply script, not in this library --
    # the library only provides the single-object primitives it enforces on.

    @retry_zendesk_operation
    def get_ticket_field(self, field_id: int) -> Dict[str, Any]:
        """
        Return a single ticket field.

        Args:
            field_id: The field's numeric id

        Returns:
            The ticket field record
        """
        return self._request("GET", f"/ticket_fields/{field_id}.json")["ticket_field"]

    @retry_zendesk_operation
    def create_ticket_field(self, field: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a custom ticket field.

        NOTE: ticket fields are INSTANCE-WIDE objects, and so are the tag values
        of drop-down (``tagger``) options. On a shared instance, namespace both
        the field title and every option value.

        Args:
            field: The field payload (type, title, custom_field_options, ...)

        Returns:
            The created ticket field record
        """
        return self._request(
            "POST", "/ticket_fields.json", json_body={"ticket_field": field}
        )["ticket_field"]

    @retry_zendesk_operation
    def update_ticket_field(self, field_id: int, field: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update one custom ticket field.

        WARNING: a field's ``required``, ``required_in_portal`` and
        ``visible_in_portal`` properties are GLOBAL to the field -- the ticket
        forms API stores only ``ticket_field_ids`` and has no per-form
        overrides. Changing them on a field that appears on someone else's form
        changes their form too. Confirm a field is exclusive to your own form(s)
        before touching its properties.

        Passing ``custom_field_options`` REPLACES the option list: options you
        omit are removed, and any ticket still holding a removed value keeps an
        orphaned tag. Send the full intended list.

        Args:
            field_id: The field's numeric id
            field: The properties to change

        Returns:
            The updated ticket field record
        """
        return self._request(
            "PUT", f"/ticket_fields/{field_id}.json", json_body={"ticket_field": field}
        )["ticket_field"]

    @retry_zendesk_operation
    def get_ticket_form(self, form_id: int) -> Dict[str, Any]:
        """
        Return a single ticket form.

        Args:
            form_id: The form's numeric id

        Returns:
            The ticket form record
        """
        return self._request("GET", f"/ticket_forms/{form_id}.json")["ticket_form"]

    @retry_zendesk_operation
    def update_ticket_form(self, form_id: int, form: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update one ticket form.

        Both ``ticket_field_ids`` and the conditional-field sets
        (``agent_conditions`` / ``end_user_conditions``) are REPLACED wholesale
        by what you send, so read the form first and send the full intended
        state rather than a partial edit.

        A field referenced by ``end_user_conditions`` must be on the form AND
        have ``visible_in_portal`` set, or the update is rejected.

        Args:
            form_id: The form's numeric id
            form: The properties to change

        Returns:
            The updated ticket form record
        """
        return self._request(
            "PUT", f"/ticket_forms/{form_id}.json", json_body={"ticket_form": form}
        )["ticket_form"]

    @retry_zendesk_operation
    def create_trigger(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a ticket trigger.

        Args:
            trigger: The trigger payload (title, conditions, actions, ...)

        Returns:
            The created trigger record
        """
        return self._request(
            "POST", "/triggers.json", json_body={"trigger": trigger}
        )["trigger"]

    @retry_zendesk_operation
    def update_trigger(self, trigger_id: int, trigger: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update one ticket trigger.

        ``conditions`` and ``actions`` are replaced wholesale by what you send.

        Args:
            trigger_id: The trigger's numeric id
            trigger: The properties to change

        Returns:
            The updated trigger record
        """
        return self._request(
            "PUT", f"/triggers/{trigger_id}.json", json_body={"trigger": trigger}
        )["trigger"]
