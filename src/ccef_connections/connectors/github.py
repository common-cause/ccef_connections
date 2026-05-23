"""
GitHub connector for CCEF connections library.

Provides file-write access to a GitHub repository via the REST contents API,
suitable for "data sync -> JSON file -> GitHub Pages" patterns where a
scheduled job needs to commit a single file to a repo (e.g. the daily
dynamic-action-map Sheet sync).

Uses Personal Access Token authentication via the {NAME}_PASSWORD env var
convention. The token should be a fine-grained PAT scoped to a single repo
with Contents: Read & Write — that way a leaked token has the smallest
possible blast radius.
"""

import base64
import logging
from typing import Any, Dict, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_github_operation
from ..exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
    WriteError,
)

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"


class GitHubConnector(BaseConnection):
    """
    GitHub connector for committing files to a repository.

    Designed for the common "data -> JSON file -> GitHub Pages" pattern:
    a scheduled job reads from some source of truth, builds a small file,
    and PUTs it into a repo where Pages picks it up automatically. The
    `put_file_if_changed` method is the headline call site — it fetches
    the existing file, compares bytes, and only commits if there's a real
    change, making the job idempotent and safe to run on every tick.

    Credentials are stored as a PAT in the {credential_name}_PASSWORD env
    var. The default credential name is GITHUB_PAT, but per-repo PATs are
    strongly recommended (one credential per script). Pass a custom
    `credential_name` to the constructor to point at a different one:

        # Default credential (GITHUB_PAT_PASSWORD)
        >>> with GitHubConnector() as gh:
        ...     gh.put_file_if_changed(
        ...         repo="myorg/myrepo",
        ...         path="data/states.json",
        ...         content_bytes=b"...",
        ...         message="Daily sync",
        ...     )

        # Per-repo PAT (recommended for multi-repo Civis environments).
        # The env var is DYNAMIC_ACTION_MAP_GITHUB_PAT_PASSWORD.
        >>> with GitHubConnector(credential_name="DYNAMIC_ACTION_MAP_GITHUB_PAT") as gh:
        ...     gh.put_file_if_changed(...)
    """

    def __init__(self, credential_name: str = "GITHUB_PAT") -> None:
        """
        Initialize the GitHub connector.

        Args:
            credential_name: Credential name to read the PAT from. The env
                var read is {credential_name}_PASSWORD. Default: GITHUB_PAT
                (reads GITHUB_PAT_PASSWORD). Use a project-specific name
                when running multiple GitHub-writing scripts in the same
                environment so each script holds a scope-minimized token.
        """
        super().__init__()
        self._credential_name = credential_name
        self._token: Optional[str] = None

    def connect(self) -> None:
        """
        Load the PAT into memory.

        Raises:
            CredentialError: If the PAT is missing
            ConnectionError: If credential lookup fails for any other reason
        """
        try:
            self._token = self._credential_manager.get_github_pat(self._credential_name)
            self._is_connected = True
            logger.info(
                f"Successfully connected to GitHub (credential: {self._credential_name})"
            )
        except CredentialError:
            logger.error(
                f"Failed to connect to GitHub: credential {self._credential_name} missing"
            )
            raise
        except Exception as e:
            logger.error(f"Failed to connect to GitHub: {e}")
            raise ConnectionError(f"Failed to connect to GitHub: {e}") from e

    def disconnect(self) -> None:
        """Clear the PAT from memory."""
        self._token = None
        self._is_connected = False
        logger.debug("Disconnected from GitHub")

    def health_check(self) -> bool:
        """
        Check the connection by calling GET /user.

        Returns:
            True if the PAT is valid and the API is reachable, False otherwise
        """
        if not self._is_connected or not self._token:
            return False
        try:
            self._request("GET", "/user")
            return True
        except Exception:
            return False

    # -- HTTP helpers --------------------------------------------------

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

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
            AuthenticationError: For 401, or 403 not caused by rate limiting
            RateLimitError: For 429 or 403 with x-ratelimit-remaining: 0
            ConnectionError: For other 4xx/5xx or network failures
        """
        if not self._is_connected and not self._token:
            self.connect()

        url = f"{GITHUB_API_BASE}{path}"

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
            raise ConnectionError(f"GitHub API request failed: {e}") from e

        # Rate limiting: 429 always, plus 403 when the remaining quota is 0.
        # GitHub returns 403 for both secondary rate limits and scope failures,
        # so the header check is what disambiguates.
        if resp.status_code == 429 or (
            resp.status_code == 403
            and resp.headers.get("x-ratelimit-remaining") == "0"
        ):
            retry_after = _parse_retry_after(resp.headers)
            raise RateLimitError(
                f"GitHub rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 401:
            raise AuthenticationError(
                f"GitHub authentication failed: {resp.text}"
            )

        if resp.status_code == 403:
            raise AuthenticationError(
                f"GitHub authorization failed (token lacks required scope?): {resp.text}"
            )

        if resp.status_code == 404:
            return None

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"GitHub API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    # -- Contents API --------------------------------------------------

    @retry_github_operation
    def get_file(
        self, repo: str, path: str, ref: str = "main"
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a file from a repository.

        Args:
            repo: Repository in 'owner/name' form, e.g.
                'common-cause/dynamic-action-map'.
            path: Path to the file from the repo root (no leading slash).
            ref: Branch, tag, or commit SHA. Default 'main'.

        Returns:
            Dict with 'content_bytes' (bytes) and 'sha' (str) for the file,
            or None if the file does not exist on `ref`.

        Raises:
            WriteError: If `path` resolves to a directory rather than a file
            AuthenticationError: If the PAT lacks read access
            ConnectionError: For other API failures
        """
        result = self._request(
            "GET", f"/repos/{repo}/contents/{path}", params={"ref": ref}
        )
        if result is None:
            return None
        if isinstance(result, list):
            raise WriteError(
                f"Path '{path}' in {repo} is a directory, not a file"
            )
        try:
            content_bytes = base64.b64decode(result["content"])
        except (KeyError, ValueError) as e:
            raise ConnectionError(
                f"GitHub response missing or invalid 'content' field: {e}"
            ) from e
        return {"content_bytes": content_bytes, "sha": result["sha"]}

    @retry_github_operation
    def put_file(
        self,
        repo: str,
        path: str,
        content_bytes: bytes,
        message: str,
        branch: str = "main",
        sha: Optional[str] = None,
    ) -> str:
        """
        Create or update a file in a repository.

        For updates, `sha` must be the file's current SHA on `branch`. For
        new files, omit it. The simpler `put_file_if_changed` handles this
        SHA lookup for you and is preferred for idempotent sync jobs.

        Args:
            repo: Repository in 'owner/name' form.
            path: Path to the file from the repo root.
            content_bytes: File contents as bytes.
            message: Commit message.
            branch: Branch to commit to. Default 'main'.
            sha: Current SHA of the file (required for updates).

        Returns:
            The new commit SHA.

        Raises:
            WriteError: If the API rejects the write (e.g. SHA mismatch,
                branch protection)
            AuthenticationError: If the PAT lacks write access
            ConnectionError: For other API failures
        """
        body: Dict[str, Any] = {
            "message": message,
            "content": base64.b64encode(content_bytes).decode("ascii"),
            "branch": branch,
        }
        if sha is not None:
            body["sha"] = sha

        try:
            result = self._request(
                "PUT", f"/repos/{repo}/contents/{path}", json_body=body
            )
        except ConnectionError as e:
            # 409 (SHA conflict) and 422 (validation) are write-specific
            # failures — surface them as WriteError for clearer call sites.
            msg = str(e)
            if "409" in msg or "422" in msg:
                raise WriteError(
                    f"Write to {repo}:{path} rejected: {e}"
                ) from e
            raise

        if not result or "commit" not in result:
            raise WriteError(
                f"Unexpected response writing {repo}:{path}: {result}"
            )
        commit_sha: str = result["commit"]["sha"]
        logger.info(f"Wrote {repo}:{path} on {branch} ({commit_sha[:7]})")
        return commit_sha

    def put_file_if_changed(
        self,
        repo: str,
        path: str,
        content_bytes: bytes,
        message: str,
        branch: str = "main",
    ) -> Optional[str]:
        """
        Create or update a file, but skip if its contents are already identical.

        Fetches the file first, compares bytes, and only PUTs if there's a
        real change. Idempotent — safe to call on every scheduled run; no-op
        days produce no commits.

        Args:
            repo: Repository in 'owner/name' form.
            path: Path to the file from the repo root.
            content_bytes: New file contents as bytes.
            message: Commit message (used only if a write happens).
            branch: Branch to commit to. Default 'main'.

        Returns:
            The new commit SHA if a write happened, or None if the file was
            already up-to-date.
        """
        existing = self.get_file(repo, path, ref=branch)
        if existing is not None and existing["content_bytes"] == content_bytes:
            logger.info(f"{repo}:{path} on {branch} unchanged — skipping write")
            return None
        sha = existing["sha"] if existing else None
        return self.put_file(
            repo, path, content_bytes, message, branch=branch, sha=sha
        )


def _parse_retry_after(headers: Dict[str, str]) -> int:
    """
    Extract a retry-after duration in seconds from a GitHub rate-limit response.

    Prefers the Retry-After header (seconds), falls back to computing the
    delta to x-ratelimit-reset (a Unix timestamp). Defaults to 60s if
    neither is parseable.
    """
    retry_after = headers.get("Retry-After")
    if retry_after:
        try:
            return int(retry_after)
        except ValueError:
            pass

    reset = headers.get("x-ratelimit-reset")
    if reset:
        try:
            import time

            delta = int(reset) - int(time.time())
            return max(delta, 1)
        except ValueError:
            pass

    return 60
