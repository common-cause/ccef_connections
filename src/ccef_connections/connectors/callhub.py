"""
CallHub connector for CCEF connections library.

REST client for CallHub (P2P texting / call center platform): contact lists
("phonebooks"), CSV bulk import into a phonebook, contact-field catalogue,
custom fields, do-not-contact (DNC) entries, tags, and read-only listings of
P2P campaigns, agents and teams. Campaign start/stop is deliberately absent:
several CallHub status changes text or call real people.

Behaviors confirmed live against CC's account on 2026-10-15 (callhub-setup
project; fabricated rows, cleaned up afterwards). The load-bearing gotchas:

- **The host is per account.** ``api.callhub.io`` is deprecated (403
  "Endpoint is deprecated"); each account is pinned to a regional host shown
  in Settings → API Key → API Domain (CC: ``https://api-na1.callhub.io``).
  It rides in the credential JSON as ``api_domain``.
- Auth is ``Authorization: Token <key>``. A bad/missing key is a **403**,
  not a 401.
- **bulk_create always skips the first CSV row** as a header — a headerless
  file silently loses its first contact. :meth:`bulk_create_contacts`
  writes a header row for you when given ``rows``.
- ``mapping`` must be a JSON *string* of ``{"<field id>": "<column index>"}``
  with string values. The field ids are the ones from
  ``GET /v1/contacts/fields/`` (built-ins 0–18; **16 = Tags**, a
  comma-separated cell that creates tags on the fly; **17 = Agent
  Username**). A custom field has a *different* id in that catalogue (e.g.
  ``19``) than its own ``/v1/custom_fields/`` id (e.g. ``30178``) — map with
  the catalogue id. Contact reads return ``custom_fields`` as a JSON
  *string* keyed by the ``/custom_fields/`` id.
- bulk_create now returns a ``job_id`` but there is still no status
  endpoint; completion is visible only as ``numbers_count`` rising. Tiny
  files landed in ~10 s.
- A phone already in the target phonebook is **replaced**, not added: the
  phonebook ends up pointing at a new contact and the old contact is
  orphaned in the account. Contacts are **not** deduplicated account-wide
  (the same number can exist as several contacts).
- Throttle: an immediate second bulk_create got 429 ``Retry-After: 60``
  with ``{"detail": "Request was throttled. Expected available in 60
  seconds."}``. Published limits: bulk_create 1/min, 5/hour, 100/day;
  GET 2/s, 10,800/day; POST 1/s, 7,200/day; 20/s overall.
- IDs are 64-bit snowflakes; this client returns them as **strings**
  (``pk_str`` or parsed from ``url``) so nothing downstream floats them.
- ``GET /v1/contacts/fields/`` answers JSON labelled ``text/html``;
  ``/v1/custom_fields/`` is a bare list; ``page_size=1000`` is honoured.
- P2P *drafts* do not appear in ``/v1/p2p_campaigns/``.

No documented per-file row cap exists for bulk_create; the UI's import
dialog states a 100 MB file maximum (taken as the presumptive API cap).
"""

import csv
import io
import json
import logging
import re
import time
from collections import deque
from typing import Any, Deque, Dict, Iterable, Iterator, List, Optional, Sequence

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_callhub_operation
from ..exceptions import (
    AuthenticationError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

CALLHUB_DEFAULT_DOMAIN = "https://api-na1.callhub.io"
CALLHUB_DEPRECATED_HOST = "api.callhub.io"

# Built-in ids from GET /v1/contacts/fields/ (stable across accounts).
FIELD_CALLING_NUMBER = "0"
FIELD_TEXTING_NUMBER = "1"
FIELD_LAST_NAME = "2"
FIELD_FIRST_NAME = "3"
FIELD_EMAIL = "4"
FIELD_COUNTRY_CODE = "5"
FIELD_ADDRESS = "6"
FIELD_CITY = "7"
FIELD_STATE = "8"
FIELD_ZIPCODE = "9"
FIELD_TAGS = "16"
FIELD_AGENT_USERNAME = "17"

_THROTTLE_SECONDS = re.compile(r"available in (\d+) second", re.I)


def callhub_id(obj: Dict[str, Any]) -> str:
    """
    Return a CallHub object's id as a string.

    Prefers ``pk_str``; falls back to ``id``, then to the last path segment of
    ``url`` (DNC lists/entries and some list rows carry no ``id``).
    """
    if obj.get("pk_str"):
        return str(obj["pk_str"])
    if obj.get("id") is not None:
        return str(obj["id"])
    if obj.get("url"):
        return str(obj["url"]).rstrip("/").split("/")[-1]
    raise ValueError("CallHub object has no pk_str, id or url")


class CallHubConnector(BaseConnection):
    """
    CallHub connector for contact-list loading and account reads.

    Examples:
        >>> with CallHubConnector() as ch:
        ...     pb = ch.create_phonebook("Wave 1 - GA")
        ...     ch.bulk_create_contacts(
        ...         callhub_id(pb),
        ...         header=["phone", "first_name"],
        ...         rows=[["14045550123", "Ada"]],
        ...         mapping={"0": 0, "1": 0, "3": 1},
        ...     )
        ...     ch.wait_for_import(callhub_id(pb), expected=1)
    """

    def __init__(
        self,
        api_domain: Optional[str] = None,
        read_interval: float = 0.55,
        write_interval: float = 1.1,
        bulk_interval: float = 70.0,
        bulk_per_hour: int = 5,
        timeout: float = 60.0,
    ) -> None:
        """
        Initialize the CallHub connector.

        Args:
            api_domain: Account API host; overrides the credential's
                ``api_domain`` (Settings → API Key → API Domain)
            read_interval: Client-side floor between GETs (limit 2/s)
            write_interval: Floor between POST/PUT/PATCH/DELETE (limit 1/s)
            bulk_interval: Floor between bulk_create calls. The published
                limit is 1/min; exactly 60 s has been seen to trip it.
            bulk_per_hour: bulk_create calls allowed per rolling hour
            timeout: Per-request timeout in seconds
        """
        super().__init__()
        self._api_key: Optional[str] = None
        self._domain_override = api_domain
        self._base: Optional[str] = None
        self._intervals = {"read": read_interval, "write": write_interval, "bulk": bulk_interval}
        self._last: Dict[str, float] = {}
        self._bulk_per_hour = bulk_per_hour
        self._bulk_history: Deque[float] = deque()
        self._timeout = timeout
        self._session: Optional[requests.Session] = None

    # -- connection -------------------------------------------------------------

    def connect(self) -> None:
        """
        Load the API key and resolve the account host.

        Raises:
            CredentialError: If CALLHUB_API_KEY_PASSWORD is missing/malformed
            ConfigurationError: If the host is the deprecated api.callhub.io
        """
        try:
            creds = self._credential_manager.get_callhub_credentials()
        except CredentialError:
            logger.error("Failed to connect to CallHub: credentials missing")
            raise
        domain = self._domain_override or creds.get("api_domain")
        if not domain:
            logger.warning(
                "No CallHub api_domain in credential or constructor; assuming "
                f"{CALLHUB_DEFAULT_DOMAIN}. Copy the real value from Settings → "
                "API Key into CALLHUB_API_KEY_PASSWORD as JSON."
            )
            domain = CALLHUB_DEFAULT_DOMAIN
        domain = domain.strip().rstrip("/")
        if not domain.startswith("http"):
            domain = "https://" + domain
        if re.match(rf"^https?://{re.escape(CALLHUB_DEPRECATED_HOST)}$", domain):
            raise ConfigurationError(
                "api.callhub.io is deprecated; use the account's regional host "
                "from Settings → API Key (e.g. https://api-na1.callhub.io)"
            )
        self._api_key = creds["api_key"]
        self._base = domain
        self._session = requests.Session()
        self._session.headers.update(
            {"Authorization": f"Token {self._api_key}", "Accept": "application/json"}
        )
        self._is_connected = True
        logger.info(f"Connected to CallHub at {self._base}")

    def disconnect(self) -> None:
        """Drop the session and key."""
        if self._session is not None:
            self._session.close()
        self._session = None
        self._api_key = None
        self._is_connected = False

    def health_check(self) -> bool:
        """Cheap authenticated read of ``/v1/users/``."""
        if not self._is_connected:
            return False
        try:
            self._request("GET", "/v1/users/")
            return True
        except Exception:
            return False

    # -- HTTP helpers -------------------------------------------------------------

    def _pace(self, bucket: str) -> None:
        """Sleep to respect the per-bucket floor (and the bulk hourly window)."""
        now = time.monotonic()
        if bucket == "bulk" and self._bulk_per_hour > 0:
            while self._bulk_history and now - self._bulk_history[0] >= 3600:
                self._bulk_history.popleft()
            if len(self._bulk_history) >= self._bulk_per_hour:
                wait = 3600 - (now - self._bulk_history[0]) + 1
                logger.info(f"CallHub bulk_create hourly budget used; sleeping {wait:.0f}s")
                time.sleep(wait)
                now = time.monotonic()
                self._bulk_history.popleft()
        last = self._last.get(bucket)
        floor = self._intervals.get(bucket, 0)
        if last is not None and floor > 0:
            wait = floor - (now - last)
            if wait > 0:
                if wait > 5:
                    logger.info(f"CallHub pacing ({bucket}): sleeping {wait:.0f}s")
                time.sleep(wait)
        self._last[bucket] = time.monotonic()
        if bucket == "bulk":
            self._bulk_history.append(self._last[bucket])

    def _path(self, path_or_url: str) -> str:
        """Re-base an absolute CallHub URL onto the configured host; keep trailing slash."""
        path = re.sub(r"^https?://[^/]+", "", path_or_url)
        if "?" in path:
            p, q = path.split("?", 1)
            return (p if p.endswith("/") else p + "/") + "?" + q
        return path if path.endswith("/") else path + "/"

    @staticmethod
    def _detail(resp: requests.Response) -> str:
        try:
            body = resp.json()
        except ValueError:
            return resp.text[:500]
        if isinstance(body, dict):
            return str(body.get("error_message") or body.get("detail") or body)[:500]
        return str(body)[:500]

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, Any]] = None,
        bucket: Optional[str] = None,
    ) -> Any:
        """
        Central HTTP method with pacing and error mapping.

        Returns:
            Parsed JSON (dict or list), or None for 204

        Raises:
            AuthenticationError: 403 with an auth detail (CallHub never 401s)
            RateLimitError: 429, with retry_after from the header or body
            ConnectionError: Any other HTTP error or network failure
        """
        if not self._is_connected:
            self.connect()
        assert self._session is not None and self._base is not None
        bucket = bucket or ("read" if method == "GET" else "write")
        self._pace(bucket)
        url = self._base + self._path(path)
        try:
            resp = self._session.request(
                method, url, params=params, json=json_body, data=data,
                files=files, timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"CallHub request failed: {e}") from e

        if resp.status_code == 429:
            detail = self._detail(resp)
            retry_after: Optional[int] = None
            if resp.headers.get("Retry-After"):
                try:
                    retry_after = int(float(resp.headers["Retry-After"]))
                except ValueError:
                    retry_after = None
            if retry_after is None:
                m = _THROTTLE_SECONDS.search(detail)
                retry_after = int(m.group(1)) if m else None
            raise RateLimitError(
                f"CallHub throttled {method} {path}: {detail}", retry_after=retry_after
            )
        if resp.status_code == 403:
            detail = self._detail(resp)
            if re.search(r"token|credentials|inactive", detail, re.I):
                raise AuthenticationError(f"CallHub authentication failed: {detail}")
            raise ConnectionError(f"CallHub 403 on {method} {path}: {detail}")
        if resp.status_code == 204:
            return None
        if resp.status_code >= 400:
            raise ConnectionError(
                f"CallHub API error {resp.status_code} on {method} {path}: {self._detail(resp)}"
            )
        if not resp.content:
            return None
        try:
            return resp.json()  # /contacts/fields/ is JSON labelled text/html
        except ValueError as e:
            raise ConnectionError(
                f"CallHub returned non-JSON ({resp.status_code}) for {path}: {resp.text[:300]}"
            ) from e

    @retry_callhub_operation
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params)

    def _paginate(
        self, path: str, params: Optional[Dict[str, Any]] = None,
        page_size: int = 1000, limit: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield results across pages, following ``next`` re-based onto our host.

        Accepts bare-list responses as a single page. ``limit`` caps items.
        """
        q = dict(params or {})
        q.setdefault("page_size", page_size)
        body = self._get(path, q)
        n = 0
        while True:
            items = body if isinstance(body, list) else (body or {}).get("results", [])
            for item in items:
                yield item
                n += 1
                if limit is not None and n >= limit:
                    return
            nxt = None if isinstance(body, list) else (body or {}).get("next")
            if not nxt:
                return
            body = self._get(self._path(nxt))

    # -- account ----------------------------------------------------------------------

    def get_users(self) -> List[Dict[str, Any]]:
        """Return the account user rows (cheap auth check)."""
        return list(self._paginate("/v1/users/"))

    # -- phonebooks (UI: "Contact Lists") ---------------------------------------------

    def list_phonebooks(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List contact lists. ``count`` on each row is a URL, not a number."""
        return list(self._paginate("/v1/phonebooks/", limit=limit))

    def find_phonebook(self, name: str) -> Optional[Dict[str, Any]]:
        """Return the phonebook with exactly this name, or None (error if several)."""
        hits = [p for p in self.list_phonebooks() if p.get("name") == name]
        if len(hits) > 1:
            raise ConfigurationError(f"{len(hits)} CallHub phonebooks are named {name!r}")
        return hits[0] if hits else None

    def get_phonebook(self, phonebook_id: str) -> Dict[str, Any]:
        """Retrieve one phonebook."""
        return self._get(f"/v1/phonebooks/{phonebook_id}/")

    @retry_callhub_operation
    def create_phonebook(self, name: str, description: str = "") -> Dict[str, Any]:
        """Create a phonebook. Use :func:`callhub_id` on the result."""
        return self._request(
            "POST", "/v1/phonebooks/", json_body={"name": name, "description": description}
        )

    @retry_callhub_operation
    def delete_phonebook(self, phonebook_id: str) -> None:
        """Delete a phonebook. Its contacts remain in the account."""
        self._request("DELETE", f"/v1/phonebooks/{phonebook_id}/")

    def phonebook_count(self, phonebook_id: str) -> Dict[str, int]:
        """
        Return ``{"phonenumber_count", "mobilenumber_count"}`` — the only
        signal that a bulk import has landed.
        """
        body = self._get(f"/v1/phonebooks/{phonebook_id}/numbers_count/")
        return {k: int(v or 0) for k, v in (body or {}).items()}

    def iter_phonebook_contacts(
        self, phonebook_id: str, limit: Optional[int] = None
    ) -> Iterator[Dict[str, Any]]:
        """Yield the contacts in a phonebook (row-level PII — don't log)."""
        return self._paginate(f"/v1/phonebooks/{phonebook_id}/contacts/", limit=limit)

    # -- contact fields / custom fields --------------------------------------------------

    def list_contact_fields(self) -> List[Dict[str, Any]]:
        """
        The import-mapping catalogue: ``[{"id": 0, "name": "Calling Number*"}, ...]``.

        Custom fields appear here with their *mapping* id, which differs from
        their ``/v1/custom_fields/`` id.
        """
        return list(self._paginate("/v1/contacts/fields/"))

    def field_id_map(self) -> Dict[str, str]:
        """Lower-cased field name → mapping id (string), built-ins and custom."""
        return {str(f["name"]).strip().lower(): str(f["id"]) for f in self.list_contact_fields()}

    def list_custom_fields(self) -> List[Dict[str, Any]]:
        """Custom field definitions (``id``, ``name``, ``field_type``, ``cfmultichoice``)."""
        return list(self._paginate("/v1/custom_fields/"))

    @retry_callhub_operation
    def create_custom_field(
        self, name: str, field_type: str = "Text", choices: Optional[Sequence[str]] = None
    ) -> Dict[str, Any]:
        """
        Create an account-wide custom field.

        Args:
            name: Field name (case-insensitively unique in the account)
            field_type: "Text", "Number", "Boolean" or "Multi-choice"
            choices: Options for a Multi-choice field
        """
        body: Dict[str, Any] = {"name": name, "field_type": field_type}
        if choices:
            body["choice"] = list(choices)
        return self._request("POST", "/v1/custom_fields/", json_body=body)

    # -- bulk import ---------------------------------------------------------------------

    @staticmethod
    def rows_to_csv(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> bytes:
        """Serialise rows to CSV bytes in memory (header first, since row 1 is skipped)."""
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(list(header))
        for r in rows:
            w.writerow(["" if v is None else v for v in r])
        return buf.getvalue().encode("utf-8")

    @retry_callhub_operation
    def bulk_create_contacts(
        self,
        phonebook_id: str,
        mapping: Dict[str, Any],
        rows: Optional[Iterable[Sequence[Any]]] = None,
        header: Optional[Sequence[str]] = None,
        csv_bytes: Optional[bytes] = None,
        country_iso: str = "US",
        filename: str = "contacts.csv",
    ) -> Dict[str, Any]:
        """
        Queue a CSV import into an existing phonebook.

        Paced to ``bulk_interval`` and ``bulk_per_hour``; a 429 is retried
        (the request is rejected before processing, so a replay is safe). Any
        other failure is not replayed — the import may already be queued.

        Args:
            phonebook_id: Target phonebook id
            mapping: ``{field id: column index}`` using ``/v1/contacts/fields/``
                ids. Map the phone column to both "0" and "1" or the contacts
                can't be texted. Values are stringified for you.
            rows: Data rows (a header row is written from ``header``)
            header: Column names for ``rows`` (required with ``rows``)
            csv_bytes: Pre-built CSV **including a header row** (alternative
                to ``rows``) — CallHub always drops the first row
            country_iso: Country for every number (``country_choice=custom``)
            filename: Multipart filename (no spaces)

        Returns:
            Ack dict, e.g. ``{"message": "...", "job_id": "..."}``
        """
        if (rows is None) == (csv_bytes is None):
            raise ConfigurationError("Pass exactly one of rows or csv_bytes")
        if rows is not None:
            if not header:
                raise ConfigurationError("header is required with rows (CallHub skips row 1)")
            csv_bytes = self.rows_to_csv(header, rows)
        if not any(str(k) in (FIELD_CALLING_NUMBER, FIELD_TEXTING_NUMBER) for k in mapping):
            raise ConfigurationError("mapping needs a phone column (field 0 and/or 1)")
        data = {
            "phonebook_id": str(phonebook_id),
            "mapping": json.dumps({str(k): str(v) for k, v in mapping.items()}),
            "country_choice": "custom",
            "country_iso": country_iso,
        }
        files = {"contacts_csv": (filename.replace(" ", "_"), csv_bytes, "text/csv")}
        return self._request(
            "POST", "/v1/contacts/bulk_create/", data=data, files=files, bucket="bulk"
        ) or {}

    def wait_for_import(
        self,
        phonebook_id: str,
        expected: int,
        timeout: float = 1800.0,
        poll_interval: float = 15.0,
        stall_after: float = 300.0,
    ) -> int:
        """
        Poll ``numbers_count`` until it reaches ``expected``.

        Invalid or replaced (already-in-phonebook) numbers mean the count may
        stop short, so this also returns once the count has not moved for
        ``stall_after`` seconds after it last changed.

        Args:
            phonebook_id: Phonebook being loaded
            expected: Total phone count expected when the import finishes
            timeout: Hard ceiling in seconds
            poll_interval: Seconds between polls
            stall_after: Return early if the count is flat this long

        Returns:
            The final ``phonenumber_count``

        Raises:
            ConnectionError: On timeout
        """
        start = time.monotonic()
        last_count = -1
        last_change = start
        while True:
            count = self.phonebook_count(phonebook_id).get("phonenumber_count", 0)
            now = time.monotonic()
            if count != last_count:
                last_count, last_change = count, now
            if count >= expected:
                return count
            if now - last_change >= stall_after and count > 0:
                logger.warning(
                    f"CallHub import into {phonebook_id} stalled at {count}/{expected}"
                )
                return count
            if now - start >= timeout:
                raise ConnectionError(
                    f"Timed out waiting for CallHub import into {phonebook_id} "
                    f"({count}/{expected})"
                )
            time.sleep(poll_interval)

    # -- DNC / tags / campaigns / agents (read-only) ---------------------------------------

    def list_dnc_lists(self) -> List[Dict[str, Any]]:
        """DNC lists (no ``id`` field; use :func:`callhub_id`)."""
        return list(self._paginate("/v1/dnc_lists/"))

    def iter_dnc_contacts(self) -> Iterator[Dict[str, Any]]:
        """Every DNC entry across lists (``phone_number`` digits, no '+')."""
        return self._paginate("/v1/dnc_contacts/")

    def dnc_phone_set(self) -> set:
        """All DNC phone numbers as 10-digit strings (US), for suppression."""
        out = set()
        for e in self.iter_dnc_contacts():
            digits = re.sub(r"\D", "", str(e.get("phone_number") or ""))
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) == 10:
                out.add(digits)
        return out

    def list_tags(self) -> List[Dict[str, Any]]:
        """Account tags."""
        return list(self._paginate("/v1/tags/"))

    def list_p2p_campaigns(self) -> List[Dict[str, Any]]:
        """P2P texting campaigns (drafts are not listed)."""
        return list(self._paginate("/v1/p2p_campaigns/"))

    def list_agents(self, include_pending: bool = False) -> List[Dict[str, Any]]:
        """Agents (verified only unless ``include_pending``)."""
        params = {"include_pending": "true"} if include_pending else None
        return list(self._paginate("/v1/agents/", params=params))

    def list_teams(self) -> List[Dict[str, Any]]:
        """Agent teams."""
        return list(self._paginate("/v1/teams/"))
