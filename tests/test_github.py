"""Tests for the GitHub connector."""

import base64
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.github import (
    GITHUB_API_BASE,
    GitHubConnector,
    _parse_retry_after,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
    WriteError,
)


# -- Fixtures ----------------------------------------------------------------


FAKE_PAT = "github_pat_fake_token_value"
SAMPLE_REPO = "common-cause/dynamic-action-map"
SAMPLE_PATH = "data/states.json"
SAMPLE_CONTENT = b'{"key": "value"}\n'
SAMPLE_SHA = "abc123def456"


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data or {}
    return resp


def _file_response(content_bytes=SAMPLE_CONTENT, sha=SAMPLE_SHA):
    """Build a GitHub contents-API response for a single file."""
    return _make_response(
        200,
        {
            "type": "file",
            "name": "states.json",
            "path": "data/states.json",
            "sha": sha,
            "content": base64.b64encode(content_bytes).decode("ascii"),
            "encoding": "base64",
        },
    )


def _commit_response(commit_sha="newcommit123"):
    """Build a GitHub contents PUT response."""
    return _make_response(
        200,
        {
            "content": {"sha": "newfilesha456", "path": "data/states.json"},
            "commit": {"sha": commit_sha, "message": "Daily sync"},
        },
    )


@pytest.fixture
def connector():
    """Create a GitHubConnector with mocked credentials (default name)."""
    with patch.object(GitHubConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_github_pat.return_value = FAKE_PAT
        c = GitHubConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def custom_name_connector():
    """Create a GitHubConnector that uses a project-specific credential name."""
    with patch.object(GitHubConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_github_pat.return_value = FAKE_PAT
        c = GitHubConnector(credential_name="DYNAMIC_ACTION_MAP_GITHUB_PAT")
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already 'connected' with a fake token."""
    connector._token = FAKE_PAT
    connector._is_connected = True
    return connector


# -- Initialization -----------------------------------------------------------


class TestInit:
    def test_initial_state(self):
        c = GitHubConnector()
        assert c._token is None
        assert c._credential_name == "GITHUB_PAT"
        assert not c.is_connected()

    def test_custom_credential_name(self):
        c = GitHubConnector(credential_name="MY_REPO_PAT")
        assert c._credential_name == "MY_REPO_PAT"

    def test_repr_disconnected(self):
        c = GitHubConnector()
        assert repr(c) == "<GitHubConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<GitHubConnector status=connected>"


# -- Connect / Disconnect ----------------------------------------------------


class TestConnect:
    def test_connect_success(self, connector):
        connector.connect()

        assert connector.is_connected()
        assert connector._token == FAKE_PAT
        connector._credential_manager.get_github_pat.assert_called_once_with("GITHUB_PAT")

    def test_connect_uses_custom_credential_name(self, custom_name_connector):
        custom_name_connector.connect()

        custom_name_connector._credential_manager.get_github_pat.assert_called_once_with(
            "DYNAMIC_ACTION_MAP_GITHUB_PAT"
        )

    def test_connect_credential_error_reraises(self, connector):
        connector._credential_manager.get_github_pat.side_effect = CredentialError(
            "missing credential"
        )

        with pytest.raises(CredentialError, match="missing credential"):
            connector.connect()

        assert not connector.is_connected()

    def test_connect_unexpected_error_wrapped(self, connector):
        connector._credential_manager.get_github_pat.side_effect = RuntimeError("boom")

        with pytest.raises(ConnectionError, match="Failed to connect to GitHub"):
            connector.connect()

        assert not connector.is_connected()


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._token is None

    def test_disconnect_when_already_disconnected(self, connector):
        connector.disconnect()

        assert not connector.is_connected()
        assert connector._token is None


# -- Health Check -------------------------------------------------------------


class TestHealthCheck:
    def test_health_check_not_connected(self, connector):
        assert connector.health_check() is False

    @patch("ccef_connections.connectors.github.requests.request")
    def test_health_check_success(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"login": "octocat"})

        assert connected_connector.health_check() is True

    @patch("ccef_connections.connectors.github.requests.request")
    def test_health_check_failure(self, mock_request, connected_connector):
        mock_request.side_effect = requests.ConnectionError("DNS failure")

        assert connected_connector.health_check() is False

    @patch("ccef_connections.connectors.github.requests.request")
    def test_health_check_auth_failure(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(401, text="Bad credentials")

        assert connected_connector.health_check() is False


# -- Context Manager ----------------------------------------------------------


class TestContextManager:
    def test_context_manager_connects_and_disconnects(self, connector):
        with connector as c:
            assert c.is_connected()
            assert c is connector

        assert not connector.is_connected()
        assert connector._token is None

    def test_context_manager_disconnects_on_exception(self, connector):
        with pytest.raises(RuntimeError):
            with connector as c:
                assert c.is_connected()
                raise RuntimeError("something broke")

        assert not connector.is_connected()
        assert connector._token is None


# -- _request internals ------------------------------------------------------


class TestRequest:
    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_request_sends_auth_headers(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ok": True})

        result = connected_connector._request("GET", "/user")

        assert result == {"ok": True}
        call_kwargs = mock_request.call_args.kwargs
        assert call_kwargs["headers"]["Authorization"] == f"Bearer {FAKE_PAT}"
        assert call_kwargs["headers"]["Accept"] == "application/vnd.github+json"
        assert call_kwargs["headers"]["X-GitHub-Api-Version"] == "2022-11-28"

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_request_uses_full_url(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {})

        connected_connector._request("GET", "/user")

        args = mock_request.call_args.args
        assert args[0] == "GET"
        assert args[1] == f"{GITHUB_API_BASE}/user"

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_request_with_body(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"ok": True})

        connected_connector._request(
            "PUT",
            "/repos/x/y/contents/file",
            json_body={"message": "hi"},
        )

        call_kwargs = mock_request.call_args.kwargs
        assert call_kwargs["json"] == {"message": "hi"}

    @patch("ccef_connections.connectors.github.requests.request")
    def test_request_passes_params(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {})

        connected_connector._request("GET", "/repos/x/y/contents/file", params={"ref": "dev"})

        call_kwargs = mock_request.call_args.kwargs
        assert call_kwargs["params"] == {"ref": "dev"}

    @patch("ccef_connections.connectors.github.requests.request")
    def test_204_returns_none(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(204)

        result = connected_connector._request("DELETE", "/repos/x/y/contents/file")

        assert result is None

    @patch("ccef_connections.connectors.github.requests.request")
    def test_404_returns_none(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(404, text="Not Found")

        result = connected_connector._request("GET", "/repos/x/y/contents/missing")

        assert result is None

    @patch("ccef_connections.connectors.github.requests.request")
    def test_401_raises_auth_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(401, text="Bad credentials")

        with pytest.raises(AuthenticationError, match="authentication failed"):
            connected_connector._request("GET", "/user")

    @patch("ccef_connections.connectors.github.requests.request")
    def test_403_without_rate_limit_raises_auth_error(
        self, mock_request, connected_connector
    ):
        """403 with remaining > 0 is a scope/permission failure, not a rate limit."""
        mock_request.return_value = _make_response(
            403,
            text="Resource not accessible by personal access token",
            headers={"x-ratelimit-remaining": "4999"},
        )

        with pytest.raises(AuthenticationError, match="authorization failed"):
            connected_connector._request("PUT", "/repos/x/y/contents/file")

    @patch("ccef_connections.connectors.github.requests.request")
    def test_403_with_remaining_zero_raises_rate_limit(
        self, mock_request, connected_connector
    ):
        """403 with remaining=0 is GitHub's secondary rate limit signal."""
        mock_request.return_value = _make_response(
            403,
            text="API rate limit exceeded",
            headers={"x-ratelimit-remaining": "0", "Retry-After": "30"},
        )

        with pytest.raises(RateLimitError, match="retry after 30s") as exc_info:
            connected_connector._request("GET", "/user")

        assert exc_info.value.retry_after == 30

    @patch("ccef_connections.connectors.github.requests.request")
    def test_429_raises_rate_limit(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            429, headers={"Retry-After": "15"}
        )

        with pytest.raises(RateLimitError, match="retry after 15s") as exc_info:
            connected_connector._request("GET", "/user")

        assert exc_info.value.retry_after == 15

    @patch("ccef_connections.connectors.github.requests.request")
    def test_429_default_retry_after_when_header_missing(
        self, mock_request, connected_connector
    ):
        mock_request.return_value = _make_response(429)

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector._request("GET", "/user")

        assert exc_info.value.retry_after == 60

    @patch("ccef_connections.connectors.github.requests.request")
    def test_500_raises_connection_error(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(500, text="Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector._request("GET", "/user")

    @patch("ccef_connections.connectors.github.requests.request")
    def test_422_raises_connection_error(self, mock_request, connected_connector):
        """422 is an unprocessable entity — surfaces as ConnectionError;
        put_file is responsible for promoting that to WriteError."""
        mock_request.return_value = _make_response(422, text="Validation Failed")

        with pytest.raises(ConnectionError, match="422"):
            connected_connector._request("PUT", "/repos/x/y/contents/file")

    @patch("ccef_connections.connectors.github.requests.request")
    def test_network_error_raises_connection_error(
        self, mock_request, connected_connector
    ):
        mock_request.side_effect = requests.ConnectionError("timeout")

        with pytest.raises(ConnectionError, match="request failed"):
            connected_connector._request("GET", "/user")

    def test_auto_connect_on_request(self, connector):
        """Calling _request before connect() triggers auto-connect."""
        with patch(
            "ccef_connections.connectors.github.requests.request"
        ) as mock_request:
            mock_request.return_value = _make_response(200, {"ok": True})

            assert not connector.is_connected()
            connector._request("GET", "/user")

        assert connector.is_connected()


# -- get_file ----------------------------------------------------------------


class TestGetFile:
    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_success(self, mock_request, connected_connector):
        mock_request.return_value = _file_response()

        result = connected_connector.get_file(SAMPLE_REPO, SAMPLE_PATH)

        assert result == {"content_bytes": SAMPLE_CONTENT, "sha": SAMPLE_SHA}
        call_args = mock_request.call_args
        assert call_args.args[1] == (
            f"{GITHUB_API_BASE}/repos/{SAMPLE_REPO}/contents/{SAMPLE_PATH}"
        )
        assert call_args.kwargs["params"] == {"ref": "main"}

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_custom_ref(self, mock_request, connected_connector):
        mock_request.return_value = _file_response()

        connected_connector.get_file(SAMPLE_REPO, SAMPLE_PATH, ref="dev")

        assert mock_request.call_args.kwargs["params"] == {"ref": "dev"}

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_not_found(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(404, text="Not Found")

        result = connected_connector.get_file(SAMPLE_REPO, "missing.json")

        assert result is None

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_path_is_directory(self, mock_request, connected_connector):
        """Contents API returns a list when path is a directory."""
        mock_request.return_value = _make_response(
            200, [{"name": "file1"}, {"name": "file2"}]
        )
        # _request returns parsed JSON which is a list; need to bypass the
        # default {} return — set the json mock directly
        mock_request.return_value.json.return_value = [
            {"name": "file1"},
            {"name": "file2"},
        ]

        with pytest.raises(WriteError, match="is a directory"):
            connected_connector.get_file(SAMPLE_REPO, "data")

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_missing_content_field(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(200, {"sha": "abc"})

        with pytest.raises(ConnectionError, match="missing or invalid 'content'"):
            connected_connector.get_file(SAMPLE_REPO, SAMPLE_PATH)

    @patch("ccef_connections.connectors.github.requests.request")
    def test_get_file_invalid_base64(self, mock_request, connected_connector):
        mock_request.return_value = _make_response(
            200, {"sha": "abc", "content": "not-valid-base64!@#"}
        )

        with pytest.raises(ConnectionError, match="missing or invalid 'content'"):
            connected_connector.get_file(SAMPLE_REPO, SAMPLE_PATH)


# -- put_file ----------------------------------------------------------------


class TestPutFile:
    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_create(self, mock_request, connected_connector):
        """No sha argument means we're creating a new file."""
        mock_request.return_value = _commit_response(commit_sha="newsha789")

        result = connected_connector.put_file(
            SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "Initial commit"
        )

        assert result == "newsha789"
        body = mock_request.call_args.kwargs["json"]
        assert body["message"] == "Initial commit"
        assert body["branch"] == "main"
        assert body["content"] == base64.b64encode(SAMPLE_CONTENT).decode("ascii")
        assert "sha" not in body

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_update(self, mock_request, connected_connector):
        """Passing sha means we're updating an existing file."""
        mock_request.return_value = _commit_response(commit_sha="updatesha")

        result = connected_connector.put_file(
            SAMPLE_REPO,
            SAMPLE_PATH,
            SAMPLE_CONTENT,
            "Update",
            sha="oldsha123",
        )

        assert result == "updatesha"
        body = mock_request.call_args.kwargs["json"]
        assert body["sha"] == "oldsha123"

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_custom_branch(self, mock_request, connected_connector):
        mock_request.return_value = _commit_response()

        connected_connector.put_file(
            SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg", branch="release"
        )

        assert mock_request.call_args.kwargs["json"]["branch"] == "release"

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_409_becomes_write_error(self, mock_request, connected_connector):
        """SHA conflict surfaces as WriteError for a clearer call site."""
        mock_request.return_value = _make_response(409, text="Conflict")

        with pytest.raises(WriteError, match="rejected"):
            connected_connector.put_file(
                SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg", sha="stale"
            )

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_422_becomes_write_error(self, mock_request, connected_connector):
        """Validation failure (e.g. missing sha on existing file) is a write error."""
        mock_request.return_value = _make_response(422, text="Validation Failed")

        with pytest.raises(WriteError, match="rejected"):
            connected_connector.put_file(
                SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg"
            )

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_500_passes_through(self, mock_request, connected_connector):
        """5xx is a server error, not a write error — surfaces as ConnectionError."""
        mock_request.return_value = _make_response(500, text="Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector.put_file(
                SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg"
            )

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_403_passes_through(self, mock_request, connected_connector):
        """Token scope failure stays an AuthenticationError, not a WriteError."""
        mock_request.return_value = _make_response(
            403,
            text="Resource not accessible by personal access token",
            headers={"x-ratelimit-remaining": "4999"},
        )

        with pytest.raises(AuthenticationError):
            connected_connector.put_file(
                SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg"
            )

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_unexpected_response_shape(self, mock_request, connected_connector):
        """Response without a 'commit' field is unexpected and surfaces as WriteError."""
        mock_request.return_value = _make_response(200, {"content": {"sha": "x"}})

        with pytest.raises(WriteError, match="Unexpected response"):
            connected_connector.put_file(
                SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg"
            )

    @patch("ccef_connections.connectors.github.requests.request")
    def test_put_file_encodes_binary_content(self, mock_request, connected_connector):
        """Non-UTF-8 binary content is base64-encoded correctly."""
        mock_request.return_value = _commit_response()
        binary = bytes(range(256))

        connected_connector.put_file(SAMPLE_REPO, "blob.bin", binary, "binary")

        body = mock_request.call_args.kwargs["json"]
        assert base64.b64decode(body["content"]) == binary


# -- put_file_if_changed -----------------------------------------------------


class TestPutFileIfChanged:
    @patch("ccef_connections.connectors.github.requests.request")
    def test_unchanged_skips_write(self, mock_request, connected_connector):
        """Identical bytes -> get_file returns the file -> no PUT happens."""
        mock_request.return_value = _file_response(content_bytes=SAMPLE_CONTENT)

        result = connected_connector.put_file_if_changed(
            SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "should not commit"
        )

        assert result is None
        # Only the GET should have happened.
        assert mock_request.call_count == 1
        assert mock_request.call_args.args[0] == "GET"

    @patch("ccef_connections.connectors.github.requests.request")
    def test_changed_writes_with_existing_sha(self, mock_request, connected_connector):
        """Different bytes -> PUT happens with the previous file's SHA."""
        new_content = b'{"key": "new"}\n'
        mock_request.side_effect = [
            _file_response(content_bytes=SAMPLE_CONTENT, sha=SAMPLE_SHA),
            _commit_response(commit_sha="committed"),
        ]

        result = connected_connector.put_file_if_changed(
            SAMPLE_REPO, SAMPLE_PATH, new_content, "Sync"
        )

        assert result == "committed"
        assert mock_request.call_count == 2
        put_call = mock_request.call_args_list[1]
        assert put_call.args[0] == "PUT"
        assert put_call.kwargs["json"]["sha"] == SAMPLE_SHA
        assert base64.b64decode(put_call.kwargs["json"]["content"]) == new_content

    @patch("ccef_connections.connectors.github.requests.request")
    def test_missing_file_creates(self, mock_request, connected_connector):
        """File doesn't exist (404) -> PUT happens without a sha."""
        mock_request.side_effect = [
            _make_response(404, text="Not Found"),
            _commit_response(commit_sha="firstcommit"),
        ]

        result = connected_connector.put_file_if_changed(
            SAMPLE_REPO, "data/new.json", SAMPLE_CONTENT, "Create"
        )

        assert result == "firstcommit"
        assert mock_request.call_count == 2
        put_body = mock_request.call_args_list[1].kwargs["json"]
        assert "sha" not in put_body

    @patch("ccef_connections.connectors.github.requests.request")
    def test_passes_custom_branch(self, mock_request, connected_connector):
        """Custom branch is forwarded to both get_file and put_file."""
        mock_request.side_effect = [
            _make_response(404),
            _commit_response(),
        ]

        connected_connector.put_file_if_changed(
            SAMPLE_REPO, SAMPLE_PATH, SAMPLE_CONTENT, "msg", branch="staging"
        )

        get_call = mock_request.call_args_list[0]
        put_call = mock_request.call_args_list[1]
        assert get_call.kwargs["params"]["ref"] == "staging"
        assert put_call.kwargs["json"]["branch"] == "staging"


# -- _parse_retry_after ------------------------------------------------------


class TestParseRetryAfter:
    def test_uses_retry_after_header(self):
        assert _parse_retry_after({"Retry-After": "42"}) == 42

    def test_falls_back_to_ratelimit_reset(self):
        import time

        reset = int(time.time()) + 90
        delta = _parse_retry_after({"x-ratelimit-reset": str(reset)})
        # Allow 1s of clock drift inside the function
        assert 88 <= delta <= 90

    def test_ratelimit_reset_in_past_returns_minimum(self):
        """Past reset time returns 1s (clamped from negative)."""
        assert _parse_retry_after({"x-ratelimit-reset": "0"}) == 1

    def test_invalid_retry_after_falls_through_to_reset(self):
        import time

        reset = int(time.time()) + 30
        delta = _parse_retry_after(
            {"Retry-After": "nonsense", "x-ratelimit-reset": str(reset)}
        )
        assert 28 <= delta <= 30

    def test_no_headers_returns_default(self):
        assert _parse_retry_after({}) == 60

    def test_invalid_reset_returns_default(self):
        assert _parse_retry_after({"x-ratelimit-reset": "garbage"}) == 60


# -- Credentials -------------------------------------------------------------


class TestCredentials:
    def _make_manager(self):
        """Create a fresh CredentialManager bypassing the singleton."""
        from ccef_connections.core.credentials import CredentialManager

        mgr = object.__new__(CredentialManager)
        mgr._credentials_cache = {}
        mgr._env_loaded = True
        return mgr

    def test_get_github_pat_default_name(self):
        with patch.dict("os.environ", {"GITHUB_PAT_PASSWORD": FAKE_PAT}):
            cm = self._make_manager()
            assert cm.get_github_pat() == FAKE_PAT

    def test_get_github_pat_custom_name(self):
        """A custom credential name reads from {NAME}_PASSWORD."""
        with patch.dict(
            "os.environ", {"DYNAMIC_ACTION_MAP_GITHUB_PAT_PASSWORD": FAKE_PAT}
        ):
            cm = self._make_manager()
            result = cm.get_github_pat("DYNAMIC_ACTION_MAP_GITHUB_PAT")
            assert result == FAKE_PAT

    def test_get_github_pat_missing_env(self):
        with patch.dict("os.environ", {}, clear=True):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="GITHUB_PAT_PASSWORD"):
                cm.get_github_pat()

    def test_get_github_pat_missing_custom_name(self):
        with patch.dict("os.environ", {}, clear=True):
            cm = self._make_manager()
            with pytest.raises(CredentialError, match="MY_REPO_PAT_PASSWORD"):
                cm.get_github_pat("MY_REPO_PAT")
