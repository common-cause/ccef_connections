"""
Asana connector for CCEF connections library.

Provides read-only access to the Asana REST API v1.0: tasks (including
custom fields), projects, sections, and workspaces. Built for snapshot-sync
jobs (e.g. nightly Asana -> BigQuery pulls) that list every task in a
project and flatten it downstream.

Uses a Personal Access Token as a Bearer header. PATs work on all Asana
plan tiers and inherit the project access of the user they belong to.
Every response is wrapped in a ``{"data": ...}`` envelope; list endpoints
paginate with opaque offset tokens. Default task responses are compact
stubs, so a useful pull requires ``opt_fields`` — see
``DEFAULT_TASK_FIELDS``. Custom fields requested via ``opt_fields``
include ``display_value``, a universal string rendering that is the
recommended consumption path for syncs.

Asana returns 402 Payment Required for premium-only endpoints or
parameters used on a free workspace; the connector surfaces that as a
non-retryable error with an explicit message.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_asana_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

ASANA_API_BASE = "https://app.asana.com/api/1.0"

# Fields requested on task pulls unless the caller overrides them.
# Comma-separated, dot notation for nested fields; gid is always returned.
DEFAULT_TASK_FIELDS = (
    "name,notes,completed,completed_at,created_at,modified_at,due_on,"
    "due_at,start_on,assignee.name,assignee.email,memberships.section.name,"
    "memberships.project.name,tags.name,custom_fields,num_subtasks,"
    "parent.gid,parent.name,permalink_url"
)


def _iso(value: Union[str, datetime]) -> str:
    """Render a datetime filter value as an ISO-8601 string."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class AsanaConnector(BaseConnection):
    """
    Asana connector for read-only task, project, and workspace access.

    Authenticates with a Personal Access Token from the
    ASANA_API_KEY_PASSWORD env var and validates it against
    ``GET /users/me`` on connect. All GIDs are opaque strings.

    Examples:
        >>> with AsanaConnector() as asana:
        ...     projects = asana.get_projects(workspace_gid="12345")
        ...     tasks = asana.get_project_tasks(projects[0]["gid"])
    """

    def __init__(self) -> None:
        """Initialize the Asana connector."""
        super().__init__()
        self._api_key: Optional[str] = None
        self._session: Optional[requests.Session] = None
        self._user_gid: Optional[str] = None
        self._user_name: Optional[str] = None

    def connect(self) -> None:
        """
        Load the PAT, build an HTTP session, and validate via GET /users/me.

        Stores the authenticated user's gid and name for logging.

        Raises:
            CredentialError: If the PAT is missing
            AuthenticationError: If Asana rejects the PAT
            ConnectionError: If validation fails for any other reason
        """
        try:
            self._api_key = self._credential_manager.get_asana_api_key()
        except CredentialError:
            logger.error("Failed to connect to Asana: ASANA_API_KEY credential missing")
            raise

        self._session = requests.Session()
        self._session.headers.update(self._get_headers())

        try:
            body = self._request("GET", "/users/me")
            user = body.get("data") or {}
            self._user_gid = user.get("gid")
            self._user_name = user.get("name")
            self._is_connected = True
            logger.info(
                f"Successfully connected to Asana as {self._user_name} "
                f"(gid {self._user_gid})"
            )
        except AuthenticationError:
            self._teardown()
            logger.error("Failed to connect to Asana: PAT rejected")
            raise
        except Exception as e:
            self._teardown()
            logger.error(f"Failed to connect to Asana: {e}")
            raise ConnectionError(f"Failed to connect to Asana: {e}") from e

    def disconnect(self) -> None:
        """Close the HTTP session and clear the PAT from memory."""
        self._teardown()
        logger.debug("Disconnected from Asana")

    def _teardown(self) -> None:
        """Close the session and reset connection state."""
        if self._session is not None:
            self._session.close()
        self._session = None
        self._api_key = None
        self._user_gid = None
        self._user_name = None
        self._is_connected = False

    def health_check(self) -> bool:
        """
        Check the connection by calling GET /users/me.

        Returns:
            True if the PAT is valid and the API is reachable, False otherwise
        """
        if not self._is_connected or not self._api_key:
            return False
        try:
            self._request("GET", "/users/me")
            return True
        except Exception:
            return False

    # -- HTTP helpers ---------------------------------------------------------

    def _get_headers(self) -> Dict[str, str]:
        """Return request headers with the Bearer token."""
        return {
            "Authorization": f"Bearer {self._api_key or ''}",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Central HTTP method with auth session and standard error mapping.

        Returns the full parsed response body (the ``data`` envelope plus
        ``next_page`` on paginated endpoints) — callers unwrap ``data``.

        Args:
            method: HTTP method (GET for this read-only connector)
            path: API path relative to /api/1.0 (e.g. '/workspaces')
            params: Query parameters

        Returns:
            Parsed JSON response body

        Raises:
            AuthenticationError: On 401 responses
            RateLimitError: On 429 responses (retry_after from Retry-After)
            ConnectionError: On 402 (paid-tier feature on a free workspace),
                other HTTP errors, or network failures
        """
        if not self._is_connected and self._session is None:
            self.connect()

        session = self._session
        if session is None:
            raise ConnectionError("Asana connector is not connected")

        url = f"{ASANA_API_BASE}{path}"

        try:
            resp = session.request(method, url, params=params, timeout=30)
        except requests.RequestException as e:
            raise ConnectionError(f"Asana API request failed: {e}") from e

        if resp.status_code == 401:
            raise AuthenticationError(
                f"Asana authentication failed ({resp.status_code}): "
                f"{self._extract_error(resp)}"
            )

        if resp.status_code == 402:
            raise ConnectionError(
                "Asana returned 402 Payment Required: this request uses a "
                "paid-tier Asana feature on a free workspace "
                f"({self._extract_error(resp)})"
            )

        if resp.status_code == 429:
            retry_after = _parse_retry_after(resp.headers)
            raise RateLimitError(
                f"Asana rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code >= 400:
            raise ConnectionError(
                f"Asana API error {resp.status_code}: {self._extract_error(resp)}"
            )

        return resp.json()

    @staticmethod
    def _extract_error(resp: requests.Response) -> str:
        """Pull the first message from Asana's errors envelope, else raw text."""
        try:
            return str(resp.json()["errors"][0]["message"])
        except Exception:
            return resp.text

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Follow offset tokens across pages and collect ``data`` items.

        Always requests with an explicit ``limit`` — without one, some Asana
        endpoints truncate around ~1,000 rows instead of returning
        ``next_page``. Offset tokens expire as data changes and are never
        persisted; a mid-pagination expiry surfaces as a failure of the
        whole listing call.

        Args:
            path: API path to list
            params: Query parameters for the first request

        Returns:
            Combined list of all resources across pages
        """
        results: List[Dict[str, Any]] = []
        current_params: Dict[str, Any] = dict(params) if params else {}
        current_params.setdefault("limit", 100)

        while True:
            body = self._request("GET", path, params=current_params)
            results.extend(body.get("data") or [])

            next_page = body.get("next_page")
            offset = next_page.get("offset") if next_page else None
            if not offset:
                break
            current_params = dict(current_params)
            current_params["offset"] = offset

        return results

    # -- Workspaces -------------------------------------------------------------

    @retry_asana_operation
    def get_workspaces(self) -> List[Dict[str, Any]]:
        """
        List all workspaces visible to the PAT, paginated.

        Returns:
            List of workspace resources
        """
        return self._paginate("/workspaces")

    # -- Projects ---------------------------------------------------------------

    @retry_asana_operation
    def get_projects(
        self,
        workspace_gid: str,
        archived: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        List projects in a workspace, paginated.

        Args:
            workspace_gid: Workspace GID
            archived: If set, only return archived (True) or active (False)
                projects; None returns both

        Returns:
            List of project resources
        """
        params: Dict[str, Any] = {"workspace": workspace_gid}
        if archived is not None:
            params["archived"] = "true" if archived else "false"
        return self._paginate("/projects", params=params)

    @retry_asana_operation
    def get_project(
        self,
        project_gid: str,
        opt_fields: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get a single project's metadata.

        Args:
            project_gid: Project GID
            opt_fields: Comma-separated fields to include (dot notation for
                nested fields, e.g. 'custom_field_settings.custom_field.name')

        Returns:
            Project resource dict
        """
        params = {"opt_fields": opt_fields} if opt_fields else None
        body = self._request("GET", f"/projects/{project_gid}", params=params)
        return body.get("data") or {}

    # -- Sections ---------------------------------------------------------------

    @retry_asana_operation
    def get_sections(self, project_gid: str) -> List[Dict[str, Any]]:
        """
        List sections in a project, paginated.

        Args:
            project_gid: Project GID

        Returns:
            List of section resources
        """
        return self._paginate(f"/projects/{project_gid}/sections")

    # -- Tasks ------------------------------------------------------------------

    @retry_asana_operation
    def get_project_tasks(
        self,
        project_gid: str,
        opt_fields: str = DEFAULT_TASK_FIELDS,
        modified_since: Optional[Union[str, datetime]] = None,
        completed_since: Optional[Union[str, datetime]] = None,
    ) -> List[Dict[str, Any]]:
        """
        List all tasks in a project with full fields, paginated.

        Args:
            project_gid: Project GID
            opt_fields: Comma-separated fields to request per task
                (default: DEFAULT_TASK_FIELDS)
            modified_since: Only tasks modified at/after this time
                (ISO-8601 string or datetime)
            completed_since: Only incomplete tasks or ones completed
                at/after this time (ISO-8601 string or datetime)

        Returns:
            List of task resources
        """
        params: Dict[str, Any] = {
            "project": project_gid,
            "opt_fields": opt_fields,
        }
        if modified_since is not None:
            params["modified_since"] = _iso(modified_since)
        if completed_since is not None:
            params["completed_since"] = _iso(completed_since)
        return self._paginate("/tasks", params=params)

    @retry_asana_operation
    def get_task(
        self,
        task_gid: str,
        opt_fields: str = DEFAULT_TASK_FIELDS,
    ) -> Dict[str, Any]:
        """
        Get a single task with full fields.

        Args:
            task_gid: Task GID
            opt_fields: Comma-separated fields to request
                (default: DEFAULT_TASK_FIELDS)

        Returns:
            Task resource dict
        """
        body = self._request(
            "GET", f"/tasks/{task_gid}", params={"opt_fields": opt_fields}
        )
        return body.get("data") or {}

    @retry_asana_operation
    def get_subtasks(
        self,
        task_gid: str,
        opt_fields: str = DEFAULT_TASK_FIELDS,
    ) -> List[Dict[str, Any]]:
        """
        List a task's subtasks with full fields, paginated.

        Args:
            task_gid: Parent task GID
            opt_fields: Comma-separated fields to request per subtask
                (default: DEFAULT_TASK_FIELDS)

        Returns:
            List of subtask resources
        """
        return self._paginate(
            f"/tasks/{task_gid}/subtasks", params={"opt_fields": opt_fields}
        )


def _parse_retry_after(headers: Any) -> int:
    """
    Extract a retry-after duration in seconds from a 429 response.

    Asana sends a Retry-After header (seconds). Defaults to 60s if the
    header is missing or unparseable.
    """
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass
    return 60
