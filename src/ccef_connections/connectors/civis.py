"""
Civis Platform connector for CCEF connections library.

Thin REST client for the Civis Platform API (https://api.civisanalytics.com),
covering the object model CCEF actually operates: jobs, container scripts,
workflows, runs, logs, credentials metadata, and the platform's own
self-describing spec. Verified live against the TMC tenant on 2026-08-20.

This connector is transport only. Policy — which jobs belong to which project,
what a schedule *means*, whether a manifest matches reality — lives in the
`civis-ops` project, which consumes this connector. Keep policy out of here.

Auth is an API key via the {NAME}_PASSWORD env var convention (default
CIVIS_API_KEY_PASSWORD), sent as HTTP Basic with the key as the username and
an empty password. A bearer header works identically; basic is what the
official client uses, so it is what we use.

Things worth knowing before you call this (all verified 2026-08-20):

- ⚠ **Civis API keys expire — 30 days is the longest TMC will grant.** There is
  no service-account path. Call :meth:`api_key_status` to get the live
  ``expiresAt`` straight from the platform (``GET /users/{id}/api_keys``, which
  reports every key's expiry, use count and revocation state) rather than
  tracking the date by hand. Everything built on this connector should surface
  days-remaining somewhere a human will see it.
- **Rate limit is 1000 requests/hour**, reported on every response as
  ``x-ratelimit-limit`` / ``x-ratelimit-remaining``. :meth:`rate_limit` returns
  the last observed pair. That budget is small enough to matter: a naive
  "GET every script, then GET each one's detail" sweep over CC's 666 scripts
  would blow it. Prefer ``GET /jobs`` with filters (below).
- **``GET /jobs?scheduled=true`` is the money query.** It returns every
  scheduled job — script or workflow-triggered — in ONE call, each with its
  schedule, state and last-run outcome inline. The ``/scripts`` list endpoint
  does *not* include ``schedule``, so building an inventory from it costs one
  request per script. Use :meth:`list_scheduled_jobs`.
- **This is a shared platform.** CC is one member org of The Movement
  Cooperative; the tenant holds 84 users and the org admins are TMC staff, not
  ours. Listing endpoints return other members' objects. Filter by author
  before doing anything write-shaped — :meth:`is_mine` is here for that.
- **Container script bodies are not ours to write.** Every CCEF job is
  GitHub-backed: Civis clones the project repo into ``app/`` and the job body
  is just ``bash app/civis/<script>.sh``. The versioned ``.sh`` is the real
  job. Change behaviour by pushing to the repo, not by PATCHing
  ``dockerCommand``.
- **Run log levels lie.** Civis tags anything a container wrote to stderr as
  ``level: "error"``, and Python's ``logging`` writes INFO to stderr — so a
  perfectly healthy run's INFO lines all come back as ``error``. Judge a run
  by its ``state``, never by log levels. :meth:`run_logs` documents this too.
- **Schedules fire in the timezone of the account that owns them**, exposed as
  ``timeZone`` on the container object (CC's jobs: ``America/New_York``). It is
  not a platform default and not UTC.
- ``GET /files`` and ``GET /api_keys`` are 404 — files are per-id only, and key
  metadata lives under ``/users/{id}/api_keys``.
- Stray-package trap on dev machines: an empty ``civis/resources/`` directory
  in a Python install root makes ``import civis`` succeed as an empty namespace
  package. This connector does not use the official ``civis`` client at all, so
  it is immune — but if you are debugging someone else's script, that is why
  their ``civis.APIClient`` raised ``AttributeError``.
"""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_civis_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

CIVIS_API_BASE = "https://api.civisanalytics.com"

#: Civis' documented hourly request budget for a user API key.
CIVIS_RATE_LIMIT_PER_HOUR = 1000

#: Max page size the list endpoints accept.
CIVIS_MAX_PAGE_SIZE = 50

_DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


class CivisConnector(BaseConnection):
    """
    Civis Platform REST API connector.

    Read calls retry on 429 with backoff; write calls run single-shot, because
    a retried POST to ``/runs`` starts a second job run.

        >>> with CivisConnector() as civis:
        ...     print(civis.api_key_status()["days_remaining"])
        ...     for job in civis.list_scheduled_jobs():
        ...         print(job["name"], describe_schedule(job["schedule"]))

    The full API is 674 paths across 41 resources — far more than is worth
    hand-wrapping. Typed methods below cover the objects CCEF operates; for
    anything else use :meth:`request` (any method, any path) or :meth:`spec`
    to discover what exists.

    Credentials: an API key in CIVIS_API_KEY_PASSWORD (or pass a custom
    ``credential_name``).
    """

    def __init__(
        self,
        credential_name: str = "CIVIS_API_KEY",
        base_url: str = CIVIS_API_BASE,
        timeout: int = 60,
    ) -> None:
        """
        Initialize the Civis connector.

        Args:
            credential_name: Credential name to read the key from. The env var
                read is {credential_name}_PASSWORD. Default: CIVIS_API_KEY
                (reads CIVIS_API_KEY_PASSWORD).
            base_url: API base URL. Override only for a non-default tenant.
            timeout: Per-request timeout in seconds.
        """
        super().__init__()
        self._credential_name = credential_name
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session: Optional[requests.Session] = None
        self._rate_limit: Dict[str, Optional[int]] = {"limit": None, "remaining": None}
        self._spec_cache: Optional[Dict[str, Any]] = None
        self._me_cache: Optional[Dict[str, Any]] = None

    # -- Lifecycle -----------------------------------------------------

    def connect(self) -> None:
        """
        Build an authenticated session.

        Raises:
            CredentialError: If the API key is missing
            ConnectionError: If credential lookup fails for any other reason
        """
        try:
            key = self._credential_manager.get_civis_api_key(self._credential_name)
            session = requests.Session()
            # Civis takes the key as the Basic-auth *username*, password empty.
            session.auth = (key, "")
            session.headers.update({"Accept": "application/json"})
            self._session = session
            self._client = session
            self._is_connected = True
            logger.info(
                f"Successfully connected to Civis (credential: {self._credential_name})"
            )
        except CredentialError:
            logger.error(
                f"Failed to connect to Civis: credential {self._credential_name} missing"
            )
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Civis: {e}")
            raise ConnectionError(f"Failed to connect to Civis: {e}") from e

    def disconnect(self) -> None:
        """
        Close the session and drop the cached identity.

        The API spec cache is deliberately kept: it describes the tenant, not
        the session, and re-fetching 674 paths would spend requests from a
        1000/hour budget for no new information.
        """
        if self._session is not None:
            self._session.close()
        self._session = None
        self._client = None
        self._is_connected = False
        self._me_cache = None
        logger.debug("Disconnected from Civis")

    def health_check(self) -> bool:
        """
        Check the connection by resolving the authenticated user.

        Returns:
            True if the key is valid and the API is reachable, False otherwise
        """
        if not self._is_connected:
            return False
        try:
            self._request("GET", "/users/me")
            return True
        except Exception:
            return False

    # -- HTTP core -----------------------------------------------------

    def _raw(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> requests.Response:
        """
        Issue one request and return the raw response, recording rate-limit state.

        A 404 on a **GET** is returned rather than raised, so ``get_*`` methods
        can answer "no such object" with None. On any other verb a 404 still
        raises: there, it means the write went nowhere, and swallowing it would
        report success for something that never happened.

        Raises:
            AuthenticationError: For 401 (commonly an EXPIRED KEY) and 403
            RateLimitError: For 429
            ConnectionError: For other 4xx/5xx or a transport failure
        """
        if self._session is None:
            self.connect()
        assert self._session is not None  # for type checkers

        url = f"{self._base_url}{path}"
        try:
            resp = self._session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Civis API request failed: {e}") from e

        for header, key in (
            ("x-ratelimit-limit", "limit"),
            ("x-ratelimit-remaining", "remaining"),
        ):
            value = resp.headers.get(header)
            if value is not None:
                try:
                    self._rate_limit[key] = int(value)
                except ValueError:
                    pass

        if resp.status_code == 429:
            retry_after = _parse_retry_after(resp.headers)
            raise RateLimitError(
                f"Civis rate limit exceeded ({CIVIS_RATE_LIMIT_PER_HOUR} req/hour), "
                f"retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 401:
            raise AuthenticationError(
                "Civis authentication failed — the most likely cause is an EXPIRED "
                "API KEY (Civis caps them at 30 days). Check api_key_status() with a "
                f"working key, or mint a new one in the Civis UI. Response: {resp.text}"
            )

        if resp.status_code == 403:
            raise AuthenticationError(
                f"Civis authorization failed (object owned by another org?): {resp.text}"
            )

        if resp.status_code == 404 and method.upper() == "GET":
            return resp

        if resp.status_code >= 400:
            raise ConnectionError(
                f"Civis API error {resp.status_code} on {method} {path}: {resp.text}"
            )

        return resp

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Optional[Any]:
        """
        Issue one request and return parsed JSON (None for an empty body).

        See :meth:`_raw` for the error mapping.
        """
        resp = self._raw(method, path, params=params, json_body=json_body)
        if resp.status_code in (204, 404) or not resp.content:
            return None
        try:
            return resp.json()
        except ValueError:
            return resp.text

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        max_pages: int = 200,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield items from a page-numbered list endpoint.

        Civis paginates with ``limit`` + ``page_num`` and reports
        ``x-pagination-total-pages`` on the response, which is what this walks.
        Falls back to "stop on a short page" when the header is absent.

        ``limit`` in ``params`` is treated as the **total number of items the
        caller wants**, not the page size — which is what ``limit=3`` obviously
        means and, more usefully, stops a request for three runs from walking
        thirty pages of history against a 1000/hour budget. Page size is derived
        from it, capped at the API's 50. Omit ``limit`` for everything.

        Args:
            path: List endpoint path, e.g. ``/jobs``.
            params: Query params; ``limit`` caps total items returned.
            max_pages: Hard stop, so a pagination bug cannot burn the hourly
                request budget.
        """
        merged = dict(params or {})
        raw_limit = merged.pop("limit", None)
        max_items = int(raw_limit) if raw_limit else None
        page_size = min(max_items or CIVIS_MAX_PAGE_SIZE, CIVIS_MAX_PAGE_SIZE)

        yielded = 0
        page = 1
        while page <= max_pages:
            resp = self._raw("GET", path, params={**merged, "limit": page_size,
                                                  "page_num": page})
            if resp.status_code == 404:
                # _raw hands GET-404s back rather than raising; here that means
                # the collection does not exist, which is an empty result, not an
                # error dict to iterate over.
                return
            batch = resp.json() if resp.content else []
            if not isinstance(batch, list):
                yield batch
                return
            for item in batch:
                yield item
                yielded += 1
                if max_items is not None and yielded >= max_items:
                    return
            total_pages = resp.headers.get("x-pagination-total-pages")
            if total_pages is not None:
                try:
                    if page >= int(total_pages):
                        return
                except ValueError:
                    pass
            elif len(batch) < page_size:
                return
            if not batch:
                return
            page += 1
        logger.warning(f"Civis pagination hit max_pages={max_pages} on {path}")

    def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Any] = None,
    ) -> Optional[Any]:
        """
        Call any Civis endpoint directly — the escape hatch for the ~600 paths
        with no typed method here.

        No retry and no guardrails: you get exactly the request you asked for.
        Use :meth:`spec` or :meth:`find_endpoints` to discover paths.

        Args:
            method: HTTP verb.
            path: Path beginning with ``/``, e.g. ``/media/spend``.
            params: Query params.
            json_body: Request body, serialized as JSON.

        Returns:
            Parsed JSON, or None for an empty body.

        Examples:
            >>> civis.request("GET", "/announcements", params={"limit": 1})
        """
        return self._request(method, path, params=params, json_body=json_body)

    def rate_limit(self) -> Dict[str, Optional[int]]:
        """
        Return the rate-limit budget observed on the most recent response.

        Returns:
            ``{"limit": 1000, "remaining": 973}`` — both None before the first
            request.
        """
        return dict(self._rate_limit)

    # -- Identity and key hygiene --------------------------------------

    @retry_civis_operation
    def whoami(self, refresh: bool = False) -> Dict[str, Any]:
        """
        Get the authenticated user, including roles and group memberships.

        Cached per connection; pass ``refresh=True`` to re-fetch.

        Returns:
            The ``/users/me`` payload (id, name, username, email, roles,
            groups, ...).
        """
        if self._me_cache is None or refresh:
            self._me_cache = self._request("GET", "/users/me") or {}
        return self._me_cache

    @retry_civis_operation
    def api_key_status(self, include_inactive: bool = False) -> Dict[str, Any]:
        """
        Report the live expiry of the key this connector is authenticating with.

        ⚠ This is the single most important call in the connector for anything
        scheduled. Civis caps API keys at 30 days with no service-account
        alternative, so every consumer eventually dies of expiry; the platform
        will tell you when, if you ask.

        The API does not identify *which* key made the request, so this returns
        the user's active, unexpired key when there is exactly one (the normal
        case) and otherwise the one expiring soonest — with ``ambiguous`` set so
        a caller can tell the difference. ``key_count`` is the number of active
        keys considered.

        Args:
            include_inactive: Also return expired/revoked keys under ``all``.

        Returns:
            Dict with ``name``, ``id``, ``expires_at``, ``created_at``,
            ``days_remaining`` (float, negative once expired), ``expired``,
            ``active``, ``use_count``, ``scopes``, ``constraint_count``,
            ``ambiguous``, ``key_count``, and ``all`` when requested.
            ``days_remaining`` is None if Civis reported no expiry.
        """
        from datetime import datetime, timezone

        me = self.whoami()
        keys = self._request("GET", f"/users/{me['id']}/api_keys") or []
        active = [k for k in keys if k.get("active") and not k.get("expired")]
        pool = active or keys

        def expiry_key(rec: Dict[str, Any]) -> str:
            return rec.get("expiresAt") or "9999-12-31T00:00:00.000Z"

        chosen = min(pool, key=expiry_key) if pool else {}

        days_remaining: Optional[float] = None
        expires_at = chosen.get("expiresAt")
        if expires_at:
            try:
                expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
                delta = expiry - datetime.now(timezone.utc)
                days_remaining = round(delta.total_seconds() / 86400, 2)
            except ValueError:
                logger.warning(f"Could not parse Civis key expiry {expires_at!r}")

        status: Dict[str, Any] = {
            "id": chosen.get("id"),
            "name": chosen.get("name"),
            "expires_at": expires_at,
            "created_at": chosen.get("createdAt"),
            "last_used_at": chosen.get("lastUsedAt"),
            "days_remaining": days_remaining,
            "expired": bool(chosen.get("expired")),
            "active": bool(chosen.get("active")),
            "use_count": chosen.get("useCount"),
            "scopes": chosen.get("scopes") or [],
            "constraint_count": chosen.get("constraintCount"),
            "key_count": len(active),
            "ambiguous": len(active) > 1,
        }
        if include_inactive:
            status["all"] = keys
        if days_remaining is not None and days_remaining < 7:
            logger.warning(
                f"Civis API key {chosen.get('name')!r} expires in "
                f"{days_remaining} days — mint a replacement before it dies"
            )
        return status

    def is_mine(self, obj: Dict[str, Any]) -> bool:
        """
        True if the authenticated user authored this object.

        Cheap ownership check for write paths on a shared tenant: listing
        endpoints return other TMC member orgs' jobs, and the key carries full
        ``manage`` permission, so "can I write this" and "should I" differ.

        Args:
            obj: Any Civis object carrying an ``author`` block (job, script,
                workflow, credential, ...).

        Returns:
            True if ``obj["author"]["id"]`` is the authenticated user.
        """
        author_id = (obj.get("author") or {}).get("id")
        return author_id is not None and author_id == self.whoami().get("id")

    @retry_civis_operation
    def spec(self, refresh: bool = False) -> Dict[str, Any]:
        """
        Fetch the platform's own Swagger 2.0 spec (``GET /endpoints``).

        The authoritative catalogue of what the API can do — 674 paths at time
        of writing. It is ~3 MB, so it is cached per connection.

        Args:
            refresh: Re-fetch even if cached.

        Returns:
            The parsed spec (``paths``, ``definitions``, ...).
        """
        if self._spec_cache is None or refresh:
            spec = self._request("GET", "/endpoints")
            if isinstance(spec, list):
                spec = spec[0] if spec else {}
            self._spec_cache = spec or {}
        return self._spec_cache

    def find_endpoints(self, needle: str) -> List[Tuple[str, str, str]]:
        """
        Search the spec for endpoints whose path or summary matches ``needle``.

        Args:
            needle: Case-insensitive substring, e.g. ``"workflow"``.

        Returns:
            Sorted list of ``(METHOD, path, summary)`` tuples.

        Examples:
            >>> civis.find_endpoints("execution")[:2]
        """
        needle = needle.lower()
        found: List[Tuple[str, str, str]] = []
        for path, ops in (self.spec().get("paths") or {}).items():
            for method, op in (ops or {}).items():
                if not isinstance(op, dict):
                    continue
                summary = op.get("summary") or ""
                if needle in path.lower() or needle in summary.lower():
                    found.append((method.upper(), path, summary))
        return sorted(found, key=lambda t: (t[1], t[0]))

    # -- Jobs: the unified view over every runnable thing ---------------

    @retry_civis_operation
    def list_jobs(
        self,
        scheduled: Optional[bool] = None,
        state: Optional[str] = None,
        type: Optional[str] = None,
        author: Optional[Any] = None,
        archived: Optional[Any] = None,
        q: Optional[str] = None,
        permission: Optional[str] = None,
        hidden: Optional[bool] = None,
        limit: Optional[int] = None,
        order: Optional[str] = None,
        order_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List jobs, following pagination.

        ``/jobs`` is the one listing that carries ``schedule``, ``state`` and
        ``lastRun`` inline for every job type — scripts, imports, workflows —
        which makes it the cheap way to see what runs and how it went. The
        ``/scripts`` listing omits ``schedule`` entirely.

        Args:
            scheduled: True for only scheduled jobs, False for only unscheduled.
            state: Filter by state, e.g. ``"failed"``, ``"succeeded"``,
                ``"running"``, ``"idle"``.
            type: Filter by job type, e.g. ``"JobTypes::ContainerDocker"``.
            author: User id (or comma-joined ids) to filter by.
            archived: ``False``, ``True`` or ``"all"``.
            q: Free-text name search.
            permission: Minimum permission level, e.g. ``"manage"``.
            hidden: Include hidden jobs.
            limit: Max jobs to return; omit to page through all of them.
            order: Sort field. order_dir: ``"asc"``/``"desc"``.

        Returns:
            List of job dicts.

        Examples:
            >>> failing = civis.list_jobs(scheduled=True, state="failed")
        """
        params = _clean(
            scheduled=scheduled, state=state, type=type, author=author,
            archived=archived, q=q, permission=permission, hidden=hidden,
            limit=limit, order=order, order_dir=order_dir,
        )
        return list(self._paginate("/jobs", params=params))

    def list_scheduled_jobs(self, mine_only: bool = False) -> List[Dict[str, Any]]:
        """
        List every scheduled job, newest schedule detail inline.

        The cheapest complete answer to "what runs on Civis, and did it work
        last time" — one request for the whole inventory.

        Args:
            mine_only: Restrict to jobs the authenticated user authored. On the
                shared TMC tenant this is usually what you want.

        Returns:
            List of job dicts, each with ``schedule``, ``state`` and ``lastRun``.
        """
        jobs = self.list_jobs(scheduled=True)
        if mine_only:
            jobs = [j for j in jobs if self.is_mine(j)]
        return jobs

    @retry_civis_operation
    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get basic info for any job by id (type-agnostic).

        For a container script's full configuration — repo, ref, docker image,
        resources, credential bindings — use :meth:`get_container`.
        """
        return self._request("GET", f"/jobs/{job_id}")

    @retry_civis_operation
    def job_children(self, job_id: int) -> List[Dict[str, Any]]:
        """Show the tree of jobs this job triggers on success."""
        return self._request("GET", f"/jobs/{job_id}/children") or []

    @retry_civis_operation
    def job_parents(self, job_id: int) -> List[Dict[str, Any]]:
        """Show the chain of jobs that trigger this one."""
        return self._request("GET", f"/jobs/{job_id}/parents") or []

    @retry_civis_operation
    def job_workflows(self, job_id: int, archived: Optional[Any] = None
                      ) -> List[Dict[str, Any]]:
        """
        List the workflows this job is a step in.

        Empty for a standalone scheduled job. A job that belongs to a workflow
        is typically NOT itself scheduled — the workflow carries the schedule.
        """
        return self._request(
            "GET", f"/jobs/{job_id}/workflows", params=_clean(archived=archived)
        ) or []

    def run_job(self, job_id: int) -> Dict[str, Any]:
        """
        Start a run of any job, by id and regardless of type.

        Single-shot, never retried: a replayed POST starts a second run.

        Args:
            job_id: The job id.

        Returns:
            The new run dict (includes ``id`` and ``state``).
        """
        result = self._request("POST", f"/jobs/{job_id}/runs")
        if not result:
            raise ConnectionError(f"Unexpected empty response starting job {job_id}")
        logger.info(f"Started Civis job {job_id} (run {result.get('id')})")
        return result

    @retry_civis_operation
    def list_runs(
        self, job_id: int, limit: Optional[int] = None,
        order: Optional[str] = None, order_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List runs for any job, newest first — id, state, timings, error.

        Works for every job type, and returns a *minimal* run object. If you
        want the resource peaks (``maxMemoryUsage`` / ``maxCpuUsage``), use
        :meth:`container_runs` — the type-agnostic ``/jobs`` endpoint omits
        them, verified 2026-08-20.

        Args:
            job_id: The job id.
            limit: Max runs to return; omit to page through all history.
            order / order_dir: Sort field and direction.
        """
        params = _clean(limit=limit, order=order, order_dir=order_dir)
        return list(self._paginate(f"/jobs/{job_id}/runs", params=params))

    @retry_civis_operation
    def container_runs(
        self, script_id: int, limit: Optional[int] = None,
        order: Optional[str] = None, order_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List a container script's runs, including per-run resource peaks.

        Same runs as :meth:`list_runs`, but the container-specific endpoint adds
        ``maxMemoryUsage`` (MB), ``maxCpuUsage`` (millicores) and
        ``isCancelRequested``. Those two peaks are the whole basis for sizing a
        job: Civis' defaults (1 GB memory, 256m CPU) are too low for anything
        dbt-shaped, a run that is OOM-killed partway through is worse than one
        that fails fast, and this is the only place the platform tells you how
        close to the ceiling you are running.

        Args:
            script_id: The container script id.
            limit: Max runs to return; omit to page through all history.
            order / order_dir: Sort field and direction.

        Examples:
            >>> peaks = [(r["maxMemoryUsage"], r["maxCpuUsage"])
            ...          for r in civis.container_runs(362699252, limit=5)]
        """
        params = _clean(limit=limit, order=order, order_dir=order_dir)
        return list(
            self._paginate(f"/scripts/containers/{script_id}/runs", params=params)
        )

    @retry_civis_operation
    def get_run(self, job_id: int, run_id: int) -> Optional[Dict[str, Any]]:
        """Check the status of one run."""
        return self._request("GET", f"/jobs/{job_id}/runs/{run_id}")

    def cancel_run(self, job_id: int, run_id: int) -> None:
        """Cancel a run that is queued or in flight."""
        self._request("DELETE", f"/jobs/{job_id}/runs/{run_id}")
        logger.info(f"Cancelled Civis run {run_id} of job {job_id}")

    @retry_civis_operation
    def run_logs(
        self,
        job_id: int,
        run_id: int,
        last_id: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get a run's log lines, newest first.

        ⚠ **``level`` is not a severity.** Civis labels everything the container
        wrote to stderr as ``"error"``, and Python's ``logging`` defaults to
        stderr — so a clean run's INFO lines all arrive as ``level: "error"``.
        Judge success by the run's ``state``; treat ``level`` as a stream tag
        (stdout vs stderr), not a verdict.

        Args:
            job_id: The job id.
            run_id: The run id.
            last_id: Return only lines after this log id — the tailing cursor.
            limit: Max lines.

        Returns:
            List of ``{"id", "createdAt", "message", "level"}`` dicts.
        """
        params = _clean(last_id=last_id, limit=limit)
        return self._request(
            "GET", f"/jobs/{job_id}/runs/{run_id}/logs", params=params
        ) or []

    @retry_civis_operation
    def run_outputs(self, job_id: int, run_id: int, limit: Optional[int] = None
                    ) -> List[Dict[str, Any]]:
        """List a run's registered outputs (files, tables, reports it produced)."""
        return list(self._paginate(f"/jobs/{job_id}/runs/{run_id}/outputs",
                                   params=_clean(limit=limit)))

    def set_job_archived(self, job_id: int, archived: bool = True) -> Dict[str, Any]:
        """
        Archive or unarchive a job — the reversible way to retire one.

        Prefer this over :meth:`delete_container`: archiving stops the schedule
        and hides the job while keeping its run history.
        """
        result = self._request("PUT", f"/jobs/{job_id}/archive",
                               json_body={"status": archived}) or {}
        logger.info(f"Set Civis job {job_id} archived={archived}")
        return result

    # -- Container scripts: full CRUD -----------------------------------

    @retry_civis_operation
    def list_scripts(
        self,
        type: Optional[str] = None,
        category: Optional[str] = None,
        author: Optional[Any] = None,
        status: Optional[str] = None,
        archived: Optional[Any] = None,
        hidden: Optional[bool] = None,
        limit: Optional[int] = None,
        order: Optional[str] = None,
        order_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List scripts of every type (SQL, Python, R, containers, dbt, custom).

        ⚠ This payload has NO ``schedule`` field — use :meth:`list_jobs` if you
        care when things run. Note also the scale on a long-lived account: CC's
        own user has 666 scripts, mostly historical one-off SQL, so always pass
        ``type`` and/or ``author``.

        Args:
            type: ``"containers"``, ``"sql"``, ``"python3"``, ``"r"``,
                ``"javascript"``, ``"dbt"`` (see :meth:`script_types`).
            category: Script category filter.
            author: User id or comma-joined ids.
            status: State filter.
            archived: ``False``, ``True`` or ``"all"``.
            hidden: Include hidden scripts.
            limit / order / order_dir: Paging and sort.

        Returns:
            List of script dicts.
        """
        params = _clean(
            type=type, category=category, author=author, status=status,
            archived=archived, hidden=hidden, limit=limit, order=order,
            order_dir=order_dir,
        )
        return list(self._paginate("/scripts", params=params))

    @retry_civis_operation
    def script_types(self) -> List[Dict[str, Any]]:
        """List the script types this tenant supports."""
        return self._request("GET", "/scripts/types") or []

    @retry_civis_operation
    def get_container(self, script_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a container script's full configuration.

        This is the object that holds everything an audit wants: ``repoHttpUri``
        and ``repoRef`` (the GitHub backing), ``dockerImageName``/
        ``dockerImageTag``, ``dockerCommand``, ``requiredResources``
        (cpu/memory/diskSpace), ``schedule`` + ``timeZone``, ``notifications``,
        ``arguments`` (credential-name → credential-id bindings), ``lastRun``,
        ``archived`` and ``myPermissionLevel``.

        Args:
            script_id: The container script id.

        Returns:
            The container dict, or None if not found.
        """
        return self._request("GET", f"/scripts/containers/{script_id}")

    def create_container(self, name: str, **fields: Any) -> Dict[str, Any]:
        """
        Create a container script.

        Follow the house convention: back the job with a GitHub repo and point
        ``docker_command`` at a committed shell script, rather than pasting a
        body in. A minimal CCEF-shaped job looks like::

            civis.create_container(
                name="My Sync",
                required_resources={"cpu": 1024, "memory": 4096, "diskSpace": 2},
                docker_image_name="civisanalytics/datascience-python",
                docker_image_tag="8.5.0",
                repo_http_uri="https://github.com/common-cause/my-project.git",
                repo_ref="main",
                docker_command="bash app/civis/my_sync.sh",
                time_zone="America/New_York",
            )

        Pin a current 8.x image tag: new jobs default to 6.4.0, whose frozen
        2023 pip index silently caps package versions.

        Args:
            name: Job name.
            **fields: Any container attribute, in snake_case (converted to the
                API's camelCase) or camelCase directly. Common ones:
                ``required_resources``, ``docker_image_name``,
                ``docker_image_tag``, ``docker_command``, ``repo_http_uri``,
                ``repo_ref``, ``schedule``, ``time_zone``, ``notifications``,
                ``params``, ``arguments``, ``git_credential_id``,
                ``cancel_timeout``, ``hidden``.

        Returns:
            The created container dict (includes ``id``).
        """
        body = {"name": name, **_camelize(fields)}
        result = self._request("POST", "/scripts/containers", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(
                f"Unexpected response creating container {name!r}: {result}"
            )
        logger.info(f"Created Civis container script {result['id']} ({name!r})")
        return result

    def update_container(self, script_id: int, **fields: Any) -> Dict[str, Any]:
        """
        PATCH a container script — change only the attributes you pass.

        The routine reasons to reach for this:

        * **Resources.** ``required_resources={"cpu": 1024, "memory": 4096,
          "diskSpace": 2}``. Every Civis default is too low for a dbt job;
          check :meth:`list_runs` for ``maxMemoryUsage`` before and after.
        * **Schedule.** See :func:`build_schedule` for the dict shape.
        * **Image tag.** ``docker_image_tag="8.5.0"``.
        * **Repo ref.** ``repo_ref="main"`` — normally left alone, since the
          edit→push→live flow depends on it tracking the default branch.

        What NOT to reach for it for: ``docker_command``. Job behaviour lives in
        the repo's committed ``.sh``; change it there and push.

        Args:
            script_id: The container script id.
            **fields: Attributes to change, snake_case or camelCase.

        Returns:
            The updated container dict.
        """
        if not fields:
            raise ConnectionError("update_container needs at least one field to change")
        result = self._request(
            "PATCH", f"/scripts/containers/{script_id}", json_body=_camelize(fields)
        )
        if not result:
            raise ConnectionError(f"Unexpected response updating container {script_id}")
        logger.info(
            f"Updated Civis container {script_id}: {', '.join(sorted(fields))}"
        )
        return result

    def replace_container(self, script_id: int, name: str, **fields: Any
                          ) -> Dict[str, Any]:
        """
        PUT a container script — replace ALL attributes.

        Anything you omit is reset to its default, which is rarely what you
        want. Prefer :meth:`update_container` unless you are deliberately
        rewriting a job wholesale.
        """
        body = {"name": name, **_camelize(fields)}
        result = self._request("PUT", f"/scripts/containers/{script_id}",
                               json_body=body)
        if not result:
            raise ConnectionError(f"Unexpected response replacing container {script_id}")
        logger.info(f"Replaced Civis container {script_id} ({name!r})")
        return result

    def clone_container(
        self,
        script_id: int,
        clone_schedule: bool = False,
        clone_triggers: bool = False,
        clone_notifications: bool = False,
    ) -> Dict[str, Any]:
        """
        Clone a container script.

        The three flags default to False so a clone does not silently start
        running on the original's schedule — pass them deliberately.

        Returns:
            The new container dict.
        """
        body = {
            "cloneSchedule": clone_schedule,
            "cloneTriggers": clone_triggers,
            "cloneNotifications": clone_notifications,
        }
        result = self._request("POST", f"/scripts/containers/{script_id}/clone",
                               json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response cloning container {script_id}")
        logger.info(f"Cloned Civis container {script_id} -> {result['id']}")
        return result

    def delete_container(self, script_id: int) -> None:
        """
        Delete (archive, in Civis' deprecated spelling) a container script.

        ⚠ Civis documents this endpoint as deprecated in favour of the archive
        endpoints — :meth:`set_job_archived` is the reversible, supported way to
        retire a job and keeps its run history. Reach for this only when you
        actually mean "remove it".
        """
        self._request("DELETE", f"/scripts/containers/{script_id}")
        logger.warning(f"Deleted Civis container script {script_id}")

    @retry_civis_operation
    def container_dependencies(self, script_id: int) -> List[Dict[str, Any]]:
        """
        List objects that depend on this container.

        Worth checking before deleting or retiming anything: it surfaces the
        workflows and downstream jobs that would break.
        """
        return self._request(
            "GET", f"/scripts/containers/{script_id}/dependencies"
        ) or []

    @retry_civis_operation
    def container_shares(self, script_id: int) -> Optional[Dict[str, Any]]:
        """List the users and groups permissioned on a container script."""
        return self._request("GET", f"/scripts/containers/{script_id}/shares")

    # -- Workflows ------------------------------------------------------

    @retry_civis_operation
    def list_workflows(
        self,
        scheduled: Optional[bool] = None,
        state: Optional[str] = None,
        author: Optional[Any] = None,
        archived: Optional[Any] = None,
        hidden: Optional[bool] = None,
        limit: Optional[int] = None,
        order: Optional[str] = None,
        order_dir: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        List workflows, with ``schedule`` and ``state`` inline.

        A workflow is an ordered chain of script jobs; the workflow carries the
        schedule and its member jobs usually do not. So a complete picture of
        "what runs" is :meth:`list_scheduled_jobs` **plus** this with
        ``scheduled=True``.
        """
        params = _clean(scheduled=scheduled, state=state, author=author,
                        archived=archived, hidden=hidden, limit=limit,
                        order=order, order_dir=order_dir)
        return list(self._paginate("/workflows", params=params))

    @retry_civis_operation
    def get_workflow(self, workflow_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a workflow, including its ``definition`` (the YAML task graph).

        The definition names each task and the job it runs, which is how you map
        a workflow's steps back to container script ids.
        """
        return self._request("GET", f"/workflows/{workflow_id}")

    def create_workflow(self, name: str, definition: str, **fields: Any
                        ) -> Dict[str, Any]:
        """
        Create a workflow.

        Args:
            name: Workflow name.
            definition: The workflow YAML.
            **fields: Other attributes (``schedule``, ``notifications``,
                ``time_zone``, ``from_job_chain``, ``hidden``, ...).

        Returns:
            The created workflow dict.
        """
        body = {"name": name, "definition": definition, **_camelize(fields)}
        result = self._request("POST", "/workflows", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response creating workflow {name!r}")
        logger.info(f"Created Civis workflow {result['id']} ({name!r})")
        return result

    def update_workflow(self, workflow_id: int, **fields: Any) -> Dict[str, Any]:
        """PATCH a workflow — change only the attributes you pass."""
        if not fields:
            raise ConnectionError("update_workflow needs at least one field to change")
        result = self._request("PATCH", f"/workflows/{workflow_id}",
                               json_body=_camelize(fields))
        if not result:
            raise ConnectionError(f"Unexpected response updating workflow {workflow_id}")
        logger.info(f"Updated Civis workflow {workflow_id}: {', '.join(sorted(fields))}")
        return result

    def replace_workflow(self, workflow_id: int, name: str, definition: str,
                         **fields: Any) -> Dict[str, Any]:
        """PUT a workflow — replace all attributes; omissions reset to default."""
        body = {"name": name, "definition": definition, **_camelize(fields)}
        result = self._request("PUT", f"/workflows/{workflow_id}", json_body=body)
        if not result:
            raise ConnectionError(f"Unexpected response replacing workflow {workflow_id}")
        logger.info(f"Replaced Civis workflow {workflow_id} ({name!r})")
        return result

    def clone_workflow(self, workflow_id: int, clone_schedule: bool = False,
                       clone_notifications: bool = False) -> Dict[str, Any]:
        """Clone a workflow. Schedule/notifications are opt-in, as for containers."""
        body = {"cloneSchedule": clone_schedule,
                "cloneNotifications": clone_notifications}
        result = self._request("POST", f"/workflows/{workflow_id}/clone",
                               json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response cloning workflow {workflow_id}")
        logger.info(f"Cloned Civis workflow {workflow_id} -> {result['id']}")
        return result

    def set_workflow_archived(self, workflow_id: int, archived: bool = True
                              ) -> Dict[str, Any]:
        """Archive or unarchive a workflow — reversible retirement."""
        result = self._request("PUT", f"/workflows/{workflow_id}/archive",
                               json_body={"status": archived}) or {}
        logger.info(f"Set Civis workflow {workflow_id} archived={archived}")
        return result

    def execute_workflow(self, workflow_id: int, target_task: Optional[str] = None
                         ) -> Dict[str, Any]:
        """
        Start a workflow execution.

        Args:
            workflow_id: The workflow id.
            target_task: Run only up to this task, instead of the whole chain.

        Returns:
            The execution dict (includes ``id`` and ``state``).
        """
        body = _clean(target_task=target_task)
        result = self._request("POST", f"/workflows/{workflow_id}/executions",
                               json_body=_camelize(body) or None)
        if not result:
            raise ConnectionError(f"Unexpected empty response executing {workflow_id}")
        logger.info(
            f"Started Civis workflow {workflow_id} (execution {result.get('id')})"
        )
        return result

    @retry_civis_operation
    def list_executions(self, workflow_id: int, limit: Optional[int] = None
                        ) -> List[Dict[str, Any]]:
        """List a workflow's executions, newest first."""
        return list(self._paginate(f"/workflows/{workflow_id}/executions",
                                   params=_clean(limit=limit)))

    @retry_civis_operation
    def get_execution(self, workflow_id: int, execution_id: int
                      ) -> Optional[Dict[str, Any]]:
        """
        Get one workflow execution, including per-task state.

        This is where a failed nightly chain tells you *which step* failed.
        """
        return self._request(
            "GET", f"/workflows/{workflow_id}/executions/{execution_id}"
        )

    @retry_civis_operation
    def get_execution_task(self, workflow_id: int, execution_id: int, task_name: str
                           ) -> Optional[Dict[str, Any]]:
        """Get one task of a workflow execution."""
        return self._request(
            "GET",
            f"/workflows/{workflow_id}/executions/{execution_id}/tasks/{task_name}",
        )

    def cancel_execution(self, workflow_id: int, execution_id: int) -> None:
        """Cancel a running workflow execution."""
        self._request(
            "POST", f"/workflows/{workflow_id}/executions/{execution_id}/cancel"
        )
        logger.info(f"Cancelled Civis execution {execution_id} of {workflow_id}")

    def retry_execution(self, workflow_id: int, execution_id: int,
                        task_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retry a failed workflow execution.

        Args:
            workflow_id: The workflow id.
            execution_id: The execution id.
            task_name: Retry just this task; omit to retry all failed tasks.
        """
        result = self._request(
            "POST", f"/workflows/{workflow_id}/executions/{execution_id}/retry",
            json_body=_camelize(_clean(task_name=task_name)) or None,
        )
        logger.info(
            f"Retried Civis execution {execution_id} of workflow {workflow_id}"
            + (f" (task {task_name})" if task_name else "")
        )
        return result

    def resume_execution(self, workflow_id: int, execution_id: int
                         ) -> Optional[Dict[str, Any]]:
        """Resume a paused workflow execution."""
        return self._request(
            "POST", f"/workflows/{workflow_id}/executions/{execution_id}/resume"
        )

    # -- Credentials (metadata; the API never returns secret values) -----

    @retry_civis_operation
    def list_credentials(
        self,
        type: Optional[str] = None,
        name: Optional[str] = None,
        remote_host_id: Optional[int] = None,
        default: Optional[bool] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        List credentials visible to this key — **metadata only**.

        Civis never returns a credential's secret over the API, so this is safe
        to log and safe to diff against ``credential_catalog.yaml``: you get
        ``id``, ``name``, ``type``, ``username``, owner and timestamps. The
        ``id`` is what a container's ``arguments`` map binds to, which is how
        you check that a job is wired to the credential you think it is.
        """
        params = _clean(type=type, name=name, remote_host_id=remote_host_id,
                        default=default, limit=limit)
        return list(self._paginate("/credentials", params=params))

    @retry_civis_operation
    def get_credential(self, credential_id: int) -> Optional[Dict[str, Any]]:
        """Get one credential's metadata (never its value)."""
        return self._request("GET", f"/credentials/{credential_id}")

    @retry_civis_operation
    def credential_types(self) -> List[Dict[str, Any]]:
        """List the credential types this tenant supports."""
        return self._request("GET", "/credentials/types") or []

    def create_credential(self, type: str, name: str, password: str,
                          username: Optional[str] = None, **fields: Any
                          ) -> Dict[str, Any]:
        """
        Create a credential.

        The value goes in ``password`` — that is the field a container reads as
        ``{NAME}_PASSWORD``. Never log the argument you pass here, and never
        commit it: this is the one method on the connector that takes a secret.

        Args:
            type: Credential type, e.g. ``"Custom"`` (see
                :meth:`credential_types`).
            name: Credential name — becomes the ``{NAME}_PASSWORD`` env var, so
                match ``credential_catalog.yaml``.
            password: The secret value.
            username: Optional username/identifier half.
            **fields: Other attributes (``description``, ``remote_host_id``, ...).

        Returns:
            The created credential's metadata.
        """
        body = {"type": type, "name": name, "password": password,
                **_camelize(_clean(username=username)), **_camelize(fields)}
        result = self._request("POST", "/credentials", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response creating credential {name!r}")
        logger.info(f"Created Civis credential {result['id']} ({name!r})")
        return result

    def update_credential(self, credential_id: int, **fields: Any) -> Dict[str, Any]:
        """
        PATCH a credential — the rotation path.

        Pass ``password="..."`` to rotate a secret in place, which keeps the
        credential id and therefore every job binding that points at it.
        """
        if not fields:
            raise ConnectionError("update_credential needs at least one field")
        result = self._request("PATCH", f"/credentials/{credential_id}",
                               json_body=_camelize(fields))
        logger.info(
            f"Updated Civis credential {credential_id}: "
            f"{', '.join(sorted(k for k in fields if k != 'password'))}"
            + (" (+password)" if "password" in fields else "")
        )
        return result or {}

    def delete_credential(self, credential_id: int) -> None:
        """
        Delete a credential.

        ⚠ Check :meth:`credential_dependencies` first — deleting a credential a
        scheduled job binds to breaks that job at its next run, silently until
        then.
        """
        self._request("DELETE", f"/credentials/{credential_id}")
        logger.warning(f"Deleted Civis credential {credential_id}")

    @retry_civis_operation
    def credential_dependencies(self, credential_id: int) -> List[Dict[str, Any]]:
        """List the objects that depend on a credential."""
        return self._request(
            "GET", f"/credentials/{credential_id}/dependencies"
        ) or []

    # -- Platform context ------------------------------------------------

    @retry_civis_operation
    def list_projects(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List Civis projects (its folder/grouping concept, unrelated to ours)."""
        return list(self._paginate("/projects", params=_clean(limit=limit)))

    @retry_civis_operation
    def list_databases(self) -> List[Dict[str, Any]]:
        """List database remote hosts. CC sees exactly one: ``TMC`` (id 815)."""
        return self._request("GET", "/databases") or []

    @retry_civis_operation
    def list_remote_hosts(self) -> List[Dict[str, Any]]:
        """
        List remote hosts — the platform's outbound connections.

        Includes the BigQuery JDBC host for our own warehouse
        (``[PROD] Member Project - COM`` → ``proj-tmc-mem-com``), the TMC
        Redshift, and the GitHub/Google connections jobs authenticate through.
        """
        return self._request("GET", "/remote_hosts") or []

    @retry_civis_operation
    def list_users(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List platform users. Shared tenant: this is all of TMC, not just CC."""
        return list(self._paginate("/users", params=_clean(limit=limit)))

    @retry_civis_operation
    def organization_admins(self) -> List[Dict[str, Any]]:
        """
        List the tenant's org admins — i.e. who to escalate to.

        On CC's tenant these are TMC staff, not Common Cause staff.
        """
        return self._request("GET", "/users/me/organization_admins") or []

    @retry_civis_operation
    def list_groups(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List permission groups."""
        return list(self._paginate("/groups", params=_clean(limit=limit)))

    @retry_civis_operation
    def search(self, query: str, limit: Optional[int] = None, **filters: Any
               ) -> Optional[Any]:
        """
        Full-text search across platform objects.

        Args:
            query: Search text.
            limit: Max results.
            **filters: Extra query params the endpoint accepts (e.g. ``type``).
        """
        params = _clean(query=query, limit=limit, **filters)
        return self._request("GET", "/search", params=params)

    @retry_civis_operation
    def list_templates(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """List script templates available on the tenant (many are TMC-shared)."""
        return list(self._paginate("/templates/scripts", params=_clean(limit=limit)))

    @retry_civis_operation
    def usage(self) -> Optional[Any]:
        """Get platform usage figures for the account."""
        return self._request("GET", "/usage")


# -- Schedule helpers (pure functions; no API calls) ---------------------


def describe_schedule(
    schedule: Optional[Dict[str, Any]], time_zone: Optional[str] = None
) -> str:
    """
    Render a Civis ``schedule`` dict as a short human string.

    Civis schedules are not cron — they are a set-product of days, hours and
    minutes, plus two special forms this handles:
    ``scheduledRunsPerHour`` (interval-style) and ``scheduledDaysOfMonth``
    (monthly, which is easy to render as "unknown days" if you only look at
    ``scheduledDays``).

    Args:
        schedule: The ``schedule`` block from a job, script or workflow.
        time_zone: The owning object's ``timeZone``, appended when given. The
            schedule fires in the *account's* zone, so passing this is what
            makes the output unambiguous.

    Returns:
        Strings like ``"daily 06:45 America/New_York"``, ``"Su 02:00"``,
        ``"MoWeFr 04:30, 16:30"``, ``"monthly day 1 03:00"``, ``"4x/hour"``, or
        ``"not scheduled"``. Weekdays render as two letters so a multi-day
        schedule stays compact.

    Examples:
        >>> describe_schedule({"scheduled": True, "scheduledDays": [0, 1, 2, 3,
        ...                    4, 5, 6], "scheduledHours": [6],
        ...                    "scheduledMinutes": [45]})
        'daily 06:45'
        >>> describe_schedule({"scheduled": False})
        'not scheduled'
    """
    if not schedule or not schedule.get("scheduled"):
        return "not scheduled"

    suffix = f" {time_zone}" if time_zone else ""
    runs_per_hour = schedule.get("scheduledRunsPerHour")
    if runs_per_hour:
        return f"{runs_per_hour}x/hour{suffix}"

    hours = schedule.get("scheduledHours") or []
    minutes = schedule.get("scheduledMinutes") or [0]
    minute = minutes[0] if minutes else 0
    times = ", ".join(f"{h:02d}:{minute:02d}" for h in sorted(hours)) or "??:??"

    days_of_month = schedule.get("scheduledDaysOfMonth") or []
    if days_of_month:
        which = ", ".join(str(d) for d in sorted(days_of_month))
        return f"monthly day {which} {times}{suffix}"

    days = schedule.get("scheduledDays")
    if days is None:
        return f"{times}{suffix}"
    if len(days) == 7:
        when = "daily"
    elif not days:
        # Scheduled with no day selected: it will not fire. Say so rather than
        # rendering an empty string that reads like "daily".
        return f"scheduled but no day selected ({times}){suffix}"
    else:
        when = "".join(_DAY_NAMES[d][:2] for d in sorted(days) if 0 <= d <= 6)
    return f"{when} {times}{suffix}"


def build_schedule(
    hours: Any,
    minute: int = 0,
    days: Optional[Any] = None,
    days_of_month: Optional[Any] = None,
    runs_per_hour: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build a Civis ``schedule`` dict for :meth:`CivisConnector.update_container`.

    Remember that the times are interpreted in the owning account's timezone —
    set ``time_zone`` on the object itself if you need it to be explicit.

    Args:
        hours: Hour or list of hours (0–23) to fire at.
        minute: Minute past the hour. Civis takes a list; a single value is the
            normal case, so this is scalar for convenience.
        days: Weekday or list of weekdays, 0=Sunday … 6=Saturday. Defaults to
            all seven (daily).
        days_of_month: Day-of-month or list, for a monthly schedule. Mutually
            exclusive with a weekday schedule in practice.
        runs_per_hour: Interval form — fire N times an hour instead of at set
            times.

    Returns:
        A schedule dict ready to send.

    Examples:
        >>> build_schedule(hours=6, minute=45)["scheduledHours"]
        [6]
        >>> build_schedule(hours=2, days=0)["scheduledDays"]
        [0]
    """
    def as_list(value: Any) -> List[int]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return sorted(int(v) for v in value)
        return [int(value)]

    day_list = as_list(days) if days is not None else list(range(7))
    return {
        "scheduled": True,
        "scheduledDays": day_list,
        "scheduledHours": as_list(hours),
        "scheduledMinutes": [int(minute)],
        "scheduledRunsPerHour": runs_per_hour,
        "scheduledDaysOfMonth": as_list(days_of_month),
    }


UNSCHEDULED: Dict[str, Any] = {"scheduled": False}
"""Pass as ``schedule=`` to turn a job's schedule off without deleting it."""


# -- Internal helpers ---------------------------------------------------


def _clean(**kwargs: Any) -> Dict[str, Any]:
    """Drop None values, so optional args don't become literal query params."""
    return {k: v for k, v in kwargs.items() if v is not None}


def _camelize(fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert snake_case keys to the camelCase the Civis API expects.

    Keys that are already camelCase (no underscore) pass through untouched, so
    callers can use either spelling — or reach for an attribute this connector
    has never heard of.
    """
    out: Dict[str, Any] = {}
    for key, value in fields.items():
        if "_" in key:
            head, *rest = key.split("_")
            key = head + "".join(part.title() for part in rest)
        out[key] = value
    return out


def _parse_retry_after(headers: Any) -> int:
    """
    Extract a retry-after duration in seconds from a rate-limit response.

    Civis' budget is hourly, so an exhausted quota can mean a long wait; this
    reports what the header says and falls back to 60s when it says nothing,
    leaving the decorator to decide how long to actually sleep.
    """
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass
    return 60
