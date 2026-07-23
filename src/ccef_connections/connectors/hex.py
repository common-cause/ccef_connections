"""
Hex connector for CCEF connections library.

Thin REST client for the Hex public API (https://app.hex.tech/api/v1),
covering the endpoints verified live against the mvmtcoop workspace on
2026-07-22: projects, cells (full CRUD), and project runs.

This connector is transport only. Workflow logic — round-trip editing,
guardrails like "never write to a project we don't own", YAML export via
the Hex CLI — lives in the `hex-toolkit` library, which consumes this
connector. Keep policy out of here.

Uses a Hex personal access token via the {NAME}_PASSWORD env var
convention (default HEX_API_KEY_PASSWORD). The workspace admin must have
API access enabled; tokens are created under user settings -> API keys.

Known API quirks (verified 2026-07-22, worth re-testing as Hex ships):
- `dataConnectionId` passed on cell CREATE does not attach the connection
  (comes back null) — but a follow-up PATCH with top-level
  `dataConnectionId` attaches it fine (verified live). update_cell exposes
  this; hex-toolkit's create_cell verb does the two-step automatically.
- `label` in a PATCH body is silently ignored; only `contents` updates.
- Cell writes bump the cell's `id` but keep `staticId` stable. The YAML
  export's `cellId` equals the API `staticId` — treat `staticId` as the
  cell's durable identity.
- Rate limits: 60 requests/min per user; 25 concurrent kernels.
"""

import logging
from typing import Any, Dict, Iterator, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_with_backoff
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

logger = logging.getLogger(__name__)

HEX_API_BASE = "https://app.hex.tech/api/v1"


class HexConnector(BaseConnection):
    """
    Hex REST API connector: projects, cells, and runs.

    Read calls retry with backoff; write calls (create/update/delete) run
    single-shot because a retried POST can duplicate a cell. Callers that
    want retry-on-write must make the operation idempotent themselves.

        >>> with HexConnector() as hex_api:
        ...     projects = hex_api.list_projects()
        ...     cells = hex_api.list_cells(projects[0]["id"])

    Credentials: a personal access token in HEX_API_KEY_PASSWORD (or pass
    a custom `credential_name`). For a self-hosted / non-default instance,
    pass `base_url`.
    """

    def __init__(
        self,
        credential_name: str = "HEX_API_KEY",
        base_url: str = HEX_API_BASE,
    ) -> None:
        """
        Initialize the Hex connector.

        Args:
            credential_name: Credential name to read the token from. The
                env var read is {credential_name}_PASSWORD.
                Default: HEX_API_KEY (reads HEX_API_KEY_PASSWORD).
            base_url: Hex API base URL. Default is the multi-tenant cloud
                instance; override for single-tenant hosts.
        """
        super().__init__()
        self._credential_name = credential_name
        self._base_url = base_url.rstrip("/")
        self._token: Optional[str] = None

    def connect(self) -> None:
        """
        Load the token into memory.

        Raises:
            CredentialError: If the token is missing
            ConnectionError: If credential lookup fails for any other reason
        """
        try:
            self._token = self._credential_manager.get_hex_api_key(self._credential_name)
            self._is_connected = True
            logger.info(f"Successfully connected to Hex (credential: {self._credential_name})")
        except CredentialError:
            logger.error(f"Failed to connect to Hex: credential {self._credential_name} missing")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Hex: {e}")
            raise ConnectionError(f"Failed to connect to Hex: {e}") from e

    def disconnect(self) -> None:
        """Clear the token from memory."""
        self._token = None
        self._is_connected = False
        logger.debug("Disconnected from Hex")

    def health_check(self) -> bool:
        """
        Check the connection by listing one project.

        Returns:
            True if the token is valid and the API is reachable, False otherwise
        """
        if not self._is_connected or not self._token:
            return False
        try:
            self._request("GET", "/projects", params={"limit": 1})
            return True
        except Exception:
            return False

    # -- HTTP helpers --------------------------------------------------

    def _get_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        """
        Central HTTP method with auth headers and standard error mapping.

        Returns parsed JSON, or None for 204/404.

        Raises:
            AuthenticationError: For 401/403
            RateLimitError: For 429 (Hex limit: 60 requests/min per user)
            ConnectionError: For other 4xx/5xx or network failures
        """
        if not self._is_connected and not self._token:
            self.connect()

        url = f"{self._base_url}{path}"

        try:
            resp = requests.request(
                method,
                url,
                headers=self._get_headers(),
                params=params,
                json=json_body,
                timeout=60,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Hex API request failed: {e}") from e

        if resp.status_code == 429:
            retry_after = _parse_retry_after(resp.headers)
            raise RateLimitError(
                f"Hex rate limit exceeded (60 req/min), retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 401:
            raise AuthenticationError(f"Hex authentication failed: {resp.text}")

        if resp.status_code == 403:
            raise AuthenticationError(
                f"Hex authorization failed (token scope / workspace API access?): {resp.text}"
            )

        if resp.status_code in (204, 404):
            return None

        if resp.status_code >= 400:
            raise ConnectionError(f"Hex API error {resp.status_code}: {resp.text}")

        return resp.json()

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Yield items from a cursor-paginated list endpoint.

        Hex list responses look like {"values": [...], "pagination":
        {"before": ..., "after": ...}}; passing `after` back as a query
        param fetches the next page.
        """
        params = dict(params or {})
        while True:
            page = self._request("GET", path, params=params)
            if not page:
                return
            for item in page.get("values", []):
                yield item
            after = (page.get("pagination") or {}).get("after")
            if not after:
                return
            params["after"] = after

    # -- Projects ------------------------------------------------------

    @retry_with_backoff()
    def list_projects(self, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        List all projects the token can view, following pagination.

        NOTE: on a shared workspace (CC lives in TMC's `mvmtcoop`) this
        includes other member orgs' projects. Filter by owner/creator
        before doing anything write-shaped.

        Args:
            params: Optional query params passed through to GET /projects
                (e.g. {"limit": 100}).

        Returns:
            List of project metadata dicts.
        """
        return list(self._paginate("/projects", params=params))

    @retry_with_backoff()
    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a single project.

        Args:
            project_id: The project UUID.

        Returns:
            Project metadata dict, or None if not found.
        """
        return self._request("GET", f"/projects/{project_id}")

    def create_project(self, title: str, description: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new (draft, unpublished) project owned by the token's user.

        Args:
            title: Project title.
            description: Optional project description.

        Returns:
            The created project's metadata dict (includes 'id').
        """
        body: Dict[str, Any] = {"title": title}
        if description is not None:
            body["description"] = description
        result = self._request("POST", "/projects", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response creating project '{title}': {result}")
        logger.info(f"Created Hex project '{title}' ({result['id']})")
        return result

    # -- Cells ---------------------------------------------------------

    @retry_with_backoff()
    def list_cells(
        self, project_id: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        List all cells in a project, following pagination.

        Each cell dict carries id, staticId, cellType, label, and a
        `contents` object with exactly one of sqlCell / codeCell /
        markdownCell populated (INPUT/CHART/MAP cells appear with all
        three null — their config is not exposed by the API).

        Args:
            project_id: The project UUID.
            params: Optional extra query params.

        Returns:
            List of cell dicts.
        """
        merged = {"projectId": project_id, **(params or {})}
        return list(self._paginate("/cells", params=merged))

    @retry_with_backoff()
    def get_cell(self, cell_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a single cell by its (current, non-static) ID.

        Args:
            cell_id: The cell's `id` (NOT `staticId`).

        Returns:
            Cell dict, or None if not found.
        """
        return self._request("GET", f"/cells/{cell_id}")

    @retry_with_backoff()
    def get_cell_output(self, cell_id: str) -> Optional[Any]:
        """
        Get a cell's output from its latest run.

        Args:
            cell_id: The cell's `id`.

        Returns:
            The output payload, or None if not found / no output.
        """
        return self._request("GET", f"/cells/{cell_id}/output")

    def create_cell(
        self,
        project_id: str,
        cell_type: str,
        contents: Dict[str, Any],
        label: Optional[str] = None,
        data_connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a cell in a project's draft.

        Args:
            project_id: The project UUID.
            cell_type: "SQL", "CODE", or "MARKDOWN".
            contents: The contents object, keyed by cell kind, e.g.
                {"sqlCell": {"source": "SELECT 1", "outputDataframe": "df"}}
                or {"markdownCell": {"source": "# hi"}}.
            label: Optional display label (becomes the extractor filename —
                name it like the intended file stem).
            data_connection_id: Data connection UUID for SQL cells. QUIRK:
                create drops this (comes back null) — verify it attached,
                and if not, PATCH it via update_cell(cell_id,
                data_connection_id=...), which works.

        Returns:
            The created cell dict.
        """
        body: Dict[str, Any] = {
            "projectId": project_id,
            "cellType": cell_type,
            "contents": contents,
        }
        if label is not None:
            body["label"] = label
        if data_connection_id is not None:
            body["dataConnectionId"] = data_connection_id
        result = self._request("POST", "/cells", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response creating cell in {project_id}: {result}")
        logger.info(f"Created {cell_type} cell {result['id']} in project {project_id}")
        return result

    def update_cell(
        self,
        cell_id: str,
        contents: Optional[Dict[str, Any]] = None,
        data_connection_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a cell's contents and/or data connection in the project draft.

        This is also the working path for attaching a data connection —
        create drops `dataConnectionId`, but PATCHing it top-level here
        attaches it (verified live 2026-07-22).

        QUIRK: `label` changes via PATCH are silently ignored by the API.
        The returned cell keeps its `staticId`; treat that as the durable
        identity across edits.

        Args:
            cell_id: The cell's `id` (NOT `staticId`).
            contents: New contents object, same shape as create_cell's.
            data_connection_id: Data connection UUID to attach (SQL cells).

        Returns:
            The updated cell dict.
        """
        body: Dict[str, Any] = {}
        if contents is not None:
            body["contents"] = contents
        if data_connection_id is not None:
            body["dataConnectionId"] = data_connection_id
        if not body:
            raise ConnectionError("update_cell needs contents and/or data_connection_id")
        result = self._request("PATCH", f"/cells/{cell_id}", json_body=body)
        if not result or "id" not in result:
            raise ConnectionError(f"Unexpected response updating cell {cell_id}: {result}")
        logger.info(f"Updated cell {cell_id}")
        return result

    def delete_cell(self, cell_id: str) -> bool:
        """
        Delete a cell from the project draft.

        Args:
            cell_id: The cell's `id` (NOT `staticId`).

        Returns:
            True if the API confirmed the deletion, False if the cell was
            not found.
        """
        result = self._request("DELETE", f"/cells/{cell_id}")
        deleted = bool(result and result.get("cellId"))
        if deleted:
            logger.info(f"Deleted cell {cell_id}")
        return deleted

    # -- Data connections ------------------------------------------------

    @retry_with_backoff()
    def list_data_connections(
        self, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        List workspace data connections (id, name, type), pagination followed.

        Use this to find the UUID to attach to SQL cells — e.g. CC's
        BigQuery connection is named "COM Service Account".
        """
        return list(self._paginate("/data-connections", params=params))

    # -- Runs ----------------------------------------------------------

    def run_project(
        self, project_id: str, options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Trigger a run of the latest PUBLISHED version of a project.

        (Draft runs are not exposed by the REST API — the Hex CLI's
        `hex project run` covers that.)

        Args:
            project_id: The project UUID.
            options: Optional run options passed through as the request
                body (e.g. {"updatePublishedResults": true}, input params).

        Returns:
            Run metadata dict (includes 'runId').
        """
        result = self._request("POST", f"/projects/{project_id}/runs", json_body=options or {})
        if not result:
            raise ConnectionError(f"Unexpected empty response running project {project_id}")
        logger.info(f"Triggered run of project {project_id}")
        return result

    @retry_with_backoff()
    def list_runs(
        self, project_id: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get run history/status for a project.

        Args:
            project_id: The project UUID.
            params: Optional query params (limit, statusFilter, ...).

        Returns:
            The runs response dict, or None if not found.
        """
        return self._request("GET", f"/projects/{project_id}/runs", params=params)

    @retry_with_backoff()
    def get_run(self, project_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a specific run.

        Args:
            project_id: The project UUID.
            run_id: The run UUID.

        Returns:
            Run status dict, or None if not found.
        """
        return self._request("GET", f"/projects/{project_id}/runs/{run_id}")

    def cancel_run(self, project_id: str, run_id: str) -> None:
        """
        Cancel a running project run.

        Args:
            project_id: The project UUID.
            run_id: The run UUID.
        """
        self._request("DELETE", f"/projects/{project_id}/runs/{run_id}")
        logger.info(f"Cancelled run {run_id} of project {project_id}")


def _parse_retry_after(headers: Any) -> int:
    """
    Extract a retry-after duration in seconds from a rate-limit response.

    Defaults to 60s (one full rate-limit window) if the header is absent
    or unparseable.
    """
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass
    return 60
