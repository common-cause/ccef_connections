"""Tests for the Civis Platform connector."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.civis import (
    CIVIS_API_BASE,
    CIVIS_MAX_PAGE_SIZE,
    UNSCHEDULED,
    CivisConnector,
    build_schedule,
    describe_schedule,
    _camelize,
    _clean,
    _parse_retry_after,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# -- Fixtures ----------------------------------------------------------------


FAKE_KEY = "F-fake-civis-api-key-value-0123456789ab"
ME_ID = 10401
JOB_ID = 362699252
RUN_ID = 854855091


def _make_response(status_code=200, json_data=None, text="", headers=None,
                   content=b"{}"):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.content = content
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


def _list_page(items, total_pages=1):
    """Build a Civis page-numbered list response."""
    return _make_response(
        200,
        items,
        headers={
            "x-pagination-total-pages": str(total_pages),
            "x-ratelimit-limit": "1000",
            "x-ratelimit-remaining": "997",
        },
        content=b"[]",
    )


def _me():
    return {"id": ME_ID, "name": "Rob Kerth", "username": "rkerth",
            "roles": ["cusscr", "mdl", "sdm"]}


def _key_record(name="Claude Access Token", days=30, expired=False, active=True,
                key_id=103481669):
    expires = datetime.now(timezone.utc) + timedelta(days=days)
    return {
        "id": key_id,
        "name": name,
        "expiresAt": expires.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "createdAt": "2026-08-20T16:55:30.000Z",
        "revokedAt": None,
        "lastUsedAt": "2026-08-20T17:06:20.000Z",
        "scopes": ["public"],
        "useCount": 76,
        "expired": expired,
        "active": active,
        "constraintCount": 0,
    }


def _container(script_id=JOB_ID, **overrides):
    base = {
        "id": script_id,
        "name": "Sync Airtable Bases",
        "type": "Container",
        "state": "succeeded",
        "author": {"id": ME_ID, "username": "rkerth"},
        "repoHttpUri": "https://github.com/common-cause/ep-syncs.git",
        "repoRef": "main",
        "dockerCommand": "bash app/civis/sync_airtable_bases.sh",
        "dockerImageName": "civisanalytics/datascience-python",
        "dockerImageTag": "8.5.0",
        "requiredResources": {"cpu": 250, "memory": 1000, "diskSpace": 1.0},
        "timeZone": "America/New_York",
        "myPermissionLevel": "manage",
        "archived": False,
        "arguments": {"BIGQUERY_CREDENTIALS": 39428},
        "schedule": build_schedule(hours=6, minute=45),
    }
    base.update(overrides)
    return base


@pytest.fixture
def connector():
    """A CivisConnector with mocked credentials."""
    with patch.object(CivisConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_civis_api_key.return_value = FAKE_KEY
        c = CivisConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected(connector):
    """A connector with a fake authenticated session installed."""
    session = MagicMock(spec=requests.Session)
    session.auth = (FAKE_KEY, "")
    session.headers = {}
    connector._session = session
    connector._client = session
    connector._is_connected = True
    return connector


@pytest.fixture
def no_sleep():
    """Neutralize tenacity's backoff so retry tests don't actually wait.

    Required by the house rule in CLAUDE.md: any test that exercises a
    decorated retry path must patch this, or the suite spends minutes asleep.
    """
    with patch("tenacity.nap.time.sleep") as slept:
        yield slept


# -- Connection lifecycle -----------------------------------------------------


class TestConnection:
    def test_connect_uses_basic_auth_with_empty_password(self, connector):
        """Civis takes the API key as the Basic-auth USERNAME, password empty.

        Getting this backwards -- ("", key) -- returns a clean 401, which is
        indistinguishable from an expired key. Pin the order.
        """
        connector.connect()
        assert connector._session.auth == (FAKE_KEY, "")
        assert connector.is_connected()

    def test_connect_missing_credential_raises_credential_error(self, connector):
        connector._credential_manager.get_civis_api_key.side_effect = CredentialError(
            "missing"
        )
        with pytest.raises(CredentialError):
            connector.connect()
        assert not connector.is_connected()

    def test_disconnect_closes_session_and_clears_identity_cache(self, connected):
        connected._me_cache = _me()
        session = connected._session
        connected.disconnect()
        session.close.assert_called_once()
        assert connected._session is None
        assert connected._me_cache is None
        assert not connected.is_connected()

    def test_default_base_url(self, connector):
        assert connector._base_url == CIVIS_API_BASE

    def test_health_check_false_when_not_connected(self, connector):
        assert connector.health_check() is False

    def test_health_check_true_on_successful_users_me(self, connected):
        connected._session.request.return_value = _make_response(200, _me())
        assert connected.health_check() is True

    def test_health_check_false_when_request_raises(self, connected):
        connected._session.request.side_effect = requests.RequestException("boom")
        assert connected.health_check() is False


# -- HTTP error mapping -------------------------------------------------------


class TestErrorMapping:
    def test_401_names_key_expiry_as_the_likely_cause(self, connected):
        """A 401 from Civis is usually an EXPIRED key, not a wrong one.

        Keys are capped at 30 days with no service-account alternative, so
        expiry is the routine end-state. The error text has to say so, or every
        expiry gets debugged as an auth misconfiguration.
        """
        connected._session.request.return_value = _make_response(
            401, text='{"error":"unauthenticated"}'
        )
        with pytest.raises(AuthenticationError) as exc:
            connected._request("GET", "/users/me")
        assert "EXPIRED" in str(exc.value)

    def test_403_raises_authentication_error(self, connected):
        connected._session.request.return_value = _make_response(403, text="nope")
        with pytest.raises(AuthenticationError):
            connected._request("GET", "/scripts/containers/1")

    def test_429_raises_rate_limit_error_with_retry_after(self, connected):
        connected._session.request.return_value = _make_response(
            429, headers={"Retry-After": "45"}
        )
        with pytest.raises(RateLimitError) as exc:
            connected._request("GET", "/jobs")
        assert exc.value.retry_after == 45

    def test_other_4xx_raises_connection_error_naming_method_and_path(self, connected):
        connected._session.request.return_value = _make_response(
            422, text='{"error":"invalid"}'
        )
        with pytest.raises(ConnectionError) as exc:
            connected._request("PATCH", "/scripts/containers/5")
        assert "PATCH" in str(exc.value)
        assert "/scripts/containers/5" in str(exc.value)

    def test_transport_failure_wrapped_in_connection_error(self, connected):
        connected._session.request.side_effect = requests.RequestException("dns")
        with pytest.raises(ConnectionError):
            connected._request("GET", "/jobs")

    def test_204_and_empty_body_return_none(self, connected):
        connected._session.request.return_value = _make_response(204, content=b"")
        assert connected._request("DELETE", "/jobs/1/runs/2") is None

    def test_non_json_body_returned_as_text(self, connected):
        resp = _make_response(200, text="plain", content=b"plain")
        resp.json.side_effect = ValueError("not json")
        connected._session.request.return_value = resp
        assert connected._request("GET", "/whatever") == "plain"


# -- Rate limiting ------------------------------------------------------------


class TestRateLimit:
    def test_rate_limit_headers_are_recorded(self, connected):
        connected._session.request.return_value = _make_response(
            200, _me(),
            headers={"x-ratelimit-limit": "1000", "x-ratelimit-remaining": "973"},
        )
        connected._request("GET", "/users/me")
        assert connected.rate_limit() == {"limit": 1000, "remaining": 973}

    def test_rate_limit_empty_before_first_request(self, connector):
        assert connector.rate_limit() == {"limit": None, "remaining": None}

    def test_unparseable_rate_limit_header_is_ignored(self, connected):
        connected._session.request.return_value = _make_response(
            200, _me(), headers={"x-ratelimit-remaining": "lots"}
        )
        connected._request("GET", "/users/me")
        assert connected.rate_limit()["remaining"] is None

    def test_decorated_read_retries_429_then_succeeds(self, connected, no_sleep):
        connected._session.request.side_effect = [
            _make_response(429, headers={"Retry-After": "1"}),
            _list_page([{"id": 1, "name": "job"}]),
        ]
        assert len(connected.list_jobs()) == 1
        assert no_sleep.called

    def test_parse_retry_after_defaults_to_60(self):
        assert _parse_retry_after({}) == 60
        assert _parse_retry_after({"Retry-After": "not-a-number"}) == 60
        assert _parse_retry_after({"Retry-After": "12"}) == 12


# -- Pagination ---------------------------------------------------------------


class TestPagination:
    def test_follows_total_pages_header(self, connected):
        connected._session.request.side_effect = [
            _list_page([{"id": 1}, {"id": 2}], total_pages=2),
            _list_page([{"id": 3}], total_pages=2),
        ]
        assert [i["id"] for i in connected._paginate("/jobs")] == [1, 2, 3]

    def test_stops_on_short_page_when_header_absent(self, connected):
        connected._session.request.return_value = _make_response(
            200, [{"id": 1}], content=b"[]"
        )
        assert len(list(connected._paginate("/jobs"))) == 1

    def test_page_size_is_capped_at_api_maximum(self, connected):
        connected._session.request.return_value = _list_page([{"id": 1}])
        list(connected._paginate("/jobs", params={"limit": 5000}))
        sent = connected._session.request.call_args.kwargs["params"]
        assert sent["limit"] == CIVIS_MAX_PAGE_SIZE

    def test_limit_caps_total_items_not_page_size(self, connected):
        """`limit=3` means three items, and must not walk the whole history.

        Treating it as page size made `list_runs(job, limit=3)` return all 30
        runs of a daily job -- surprising, and it spends the hourly request
        budget paging through history the caller did not ask for.
        """
        connected._session.request.return_value = _list_page(
            [{"id": i} for i in range(50)], total_pages=10
        )
        items = list(connected._paginate("/jobs/1/runs", params={"limit": 3}))
        assert [i["id"] for i in items] == [0, 1, 2]
        assert connected._session.request.call_count == 1
        assert connected._session.request.call_args.kwargs["params"]["limit"] == 3

    def test_limit_larger_than_a_page_spans_pages_then_stops(self, connected):
        connected._session.request.side_effect = [
            _list_page([{"id": i} for i in range(50)], total_pages=10),
            _list_page([{"id": 50 + i} for i in range(50)], total_pages=10),
        ]
        items = list(connected._paginate("/jobs", params={"limit": 60}))
        assert len(items) == 60
        assert connected._session.request.call_count == 2

    def test_no_limit_pages_through_everything(self, connected):
        connected._session.request.side_effect = [
            _list_page([{"id": 1}], total_pages=2),
            _list_page([{"id": 2}], total_pages=2),
        ]
        assert len(list(connected._paginate("/jobs"))) == 2

    def test_max_pages_guard_stops_runaway(self, connected):
        """A pagination bug must not be able to burn the hourly request budget."""
        connected._session.request.return_value = _list_page(
            [{"id": 1}], total_pages=9999
        )
        items = list(connected._paginate("/jobs", max_pages=3))
        assert len(items) == 3
        assert connected._session.request.call_count == 3

    def test_non_list_payload_yielded_once(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"id": 7}, content=b"{}"
        )
        assert list(connected._paginate("/usage")) == [{"id": 7}]


# -- Identity and key hygiene -------------------------------------------------


class TestIdentityAndKeyStatus:
    def test_whoami_is_cached_until_refresh(self, connected):
        connected._session.request.return_value = _make_response(200, _me())
        connected.whoami()
        connected.whoami()
        assert connected._session.request.call_count == 1
        connected.whoami(refresh=True)
        assert connected._session.request.call_count == 2

    def test_api_key_status_computes_days_remaining(self, connected):
        connected._me_cache = _me()
        connected._session.request.return_value = _make_response(
            200, [_key_record(days=12)]
        )
        status = connected.api_key_status()
        assert 11.5 < status["days_remaining"] < 12.5
        assert status["expired"] is False
        assert status["name"] == "Claude Access Token"
        assert status["use_count"] == 76
        assert status["ambiguous"] is False
        assert status["key_count"] == 1

    def test_api_key_status_ignores_expired_keys_when_an_active_one_exists(
        self, connected
    ):
        """A long-lived account accumulates dead keys.

        The real account had a 2020 'Test API Key' sitting next to the live one;
        picking the soonest expiry across *all* keys would have reported the
        connection as years expired while it worked fine.
        """
        connected._me_cache = _me()
        stale = _key_record(name="Test API Key", days=-2000, expired=True,
                            active=False, key_id=27859680)
        connected._session.request.return_value = _make_response(
            200, [stale, _key_record(days=20)]
        )
        status = connected.api_key_status()
        assert status["name"] == "Claude Access Token"
        assert status["days_remaining"] > 0
        assert status["ambiguous"] is False

    def test_api_key_status_flags_ambiguity_and_picks_soonest_expiry(self, connected):
        """With two live keys we cannot know which one authenticated us.

        Reporting the soonest expiry is the safe direction (warns early); the
        ambiguous flag is what lets a caller say so out loud.
        """
        connected._me_cache = _me()
        connected._session.request.return_value = _make_response(
            200, [_key_record(name="A", days=25, key_id=1),
                  _key_record(name="B", days=3, key_id=2)]
        )
        status = connected.api_key_status()
        assert status["name"] == "B"
        assert status["ambiguous"] is True
        assert status["key_count"] == 2

    def test_api_key_status_reports_negative_days_when_expired(self, connected):
        connected._me_cache = _me()
        connected._session.request.return_value = _make_response(
            200, [_key_record(days=-3, expired=True, active=False)]
        )
        status = connected.api_key_status()
        assert status["days_remaining"] < 0
        assert status["expired"] is True

    def test_api_key_status_survives_missing_expiry(self, connected):
        connected._me_cache = _me()
        record = _key_record()
        record["expiresAt"] = None
        connected._session.request.return_value = _make_response(200, [record])
        assert connected.api_key_status()["days_remaining"] is None

    def test_api_key_status_handles_no_keys(self, connected):
        connected._me_cache = _me()
        connected._session.request.return_value = _make_response(
            200, [], content=b"[]"
        )
        status = connected.api_key_status()
        assert status["id"] is None
        assert status["key_count"] == 0

    def test_is_mine_true_for_own_object_false_for_neighbours(self, connected):
        """Listing endpoints return other TMC member orgs' jobs.

        The key carries full 'manage' permission, so "can I write this" and
        "should I" are different questions; this is the cheap answer to the
        second one.
        """
        connected._me_cache = _me()
        assert connected.is_mine(_container()) is True
        assert connected.is_mine(
            {"author": {"id": 21501, "username": "amiller"}}
        ) is False
        assert connected.is_mine({}) is False


# -- Jobs ---------------------------------------------------------------------


class TestJobs:
    def test_list_jobs_drops_none_filters(self, connected):
        connected._session.request.return_value = _list_page([])
        connected.list_jobs(scheduled=True)
        params = connected._session.request.call_args.kwargs["params"]
        assert params["scheduled"] is True
        assert "state" not in params
        assert "author" not in params

    def test_list_scheduled_jobs_passes_scheduled_filter(self, connected):
        connected._session.request.return_value = _list_page(
            [{"id": 1, "author": {"id": ME_ID}}]
        )
        connected.list_scheduled_jobs()
        assert connected._session.request.call_args.kwargs["params"]["scheduled"] is True

    def test_list_scheduled_jobs_mine_only_filters_by_author(self, connected):
        connected._me_cache = _me()
        connected._session.request.return_value = _list_page([
            {"id": 1, "name": "mine", "author": {"id": ME_ID}},
            {"id": 2, "name": "theirs", "author": {"id": 999}},
        ])
        jobs = connected.list_scheduled_jobs(mine_only=True)
        assert [j["name"] for j in jobs] == ["mine"]

    def test_run_job_posts_and_returns_run(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"id": RUN_ID, "state": "queued"}
        )
        run = connected.run_job(JOB_ID)
        assert run["id"] == RUN_ID
        args = connected._session.request.call_args
        assert args.args[0] == "POST"
        assert args.args[1].endswith(f"/jobs/{JOB_ID}/runs")

    def test_run_job_is_not_retried(self, connected, no_sleep):
        """A replayed POST /runs starts a SECOND job run.

        Every other read here retries 429; this must not, and a bare 429 has to
        surface on the first attempt.
        """
        connected._session.request.return_value = _make_response(
            429, headers={"Retry-After": "1"}
        )
        with pytest.raises(RateLimitError):
            connected.run_job(JOB_ID)
        assert connected._session.request.call_count == 1
        assert not no_sleep.called

    def test_run_job_raises_on_empty_response(self, connected):
        connected._session.request.return_value = _make_response(200, None,
                                                                content=b"")
        with pytest.raises(ConnectionError):
            connected.run_job(JOB_ID)

    def test_cancel_run_issues_delete(self, connected):
        connected._session.request.return_value = _make_response(204, content=b"")
        connected.cancel_run(JOB_ID, RUN_ID)
        args = connected._session.request.call_args
        assert args.args[0] == "DELETE"
        assert args.args[1].endswith(f"/jobs/{JOB_ID}/runs/{RUN_ID}")

    def test_container_runs_uses_the_endpoint_that_reports_resource_peaks(
        self, connected
    ):
        """maxMemoryUsage/maxCpuUsage exist ONLY on the container run endpoint.

        Verified live 2026-08-20: /jobs/{id}/runs returns a minimal run object.
        Those two peaks are the whole basis for sizing a dbt job, so the method
        that promises them has to hit the right path.
        """
        connected._session.request.return_value = _list_page(
            [{"id": RUN_ID, "state": "succeeded", "maxMemoryUsage": 460.74,
              "maxCpuUsage": 132.02}]
        )
        runs = connected.container_runs(JOB_ID, limit=1)
        assert runs[0]["maxMemoryUsage"] == 460.74
        url = connected._session.request.call_args.args[1]
        assert url.endswith(f"/scripts/containers/{JOB_ID}/runs")

    def test_list_runs_uses_the_type_agnostic_endpoint(self, connected):
        connected._session.request.return_value = _list_page([{"id": RUN_ID}])
        connected.list_runs(JOB_ID, limit=1)
        assert connected._session.request.call_args.args[1].endswith(
            f"/jobs/{JOB_ID}/runs"
        )

    def test_run_logs_passes_tail_cursor(self, connected):
        connected._session.request.return_value = _make_response(
            200, [{"id": 1, "message": "Finished", "level": "info"}]
        )
        connected.run_logs(JOB_ID, RUN_ID, last_id=42, limit=10)
        params = connected._session.request.call_args.kwargs["params"]
        assert params == {"last_id": 42, "limit": 10}

    def test_set_job_archived_sends_status_body(self, connected):
        connected._session.request.return_value = _make_response(200, {})
        connected.set_job_archived(JOB_ID, archived=True)
        assert connected._session.request.call_args.kwargs["json"] == {"status": True}


# -- Container CRUD -----------------------------------------------------------


class TestContainers:
    def test_get_container_returns_full_config(self, connected):
        connected._session.request.return_value = _make_response(200, _container())
        c = connected.get_container(JOB_ID)
        assert c["repoHttpUri"].endswith("ep-syncs.git")
        assert c["requiredResources"]["memory"] == 1000

    def test_create_container_camelizes_snake_case_fields(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"id": 999, "name": "New Job"}
        )
        connected.create_container(
            name="New Job",
            docker_image_tag="8.5.0",
            required_resources={"cpu": 1024, "memory": 4096, "diskSpace": 2},
            repo_http_uri="https://github.com/common-cause/x.git",
        )
        body = connected._session.request.call_args.kwargs["json"]
        assert body["dockerImageTag"] == "8.5.0"
        assert body["repoHttpUri"].endswith("x.git")
        assert body["requiredResources"]["memory"] == 4096
        assert body["name"] == "New Job"

    def test_update_container_patches_only_given_fields(self, connected):
        connected._session.request.return_value = _make_response(200, _container())
        connected.update_container(JOB_ID, required_resources={"memory": 4096})
        args = connected._session.request.call_args
        assert args.args[0] == "PATCH"
        assert args.kwargs["json"] == {"requiredResources": {"memory": 4096}}

    def test_update_container_rejects_empty_change(self, connected):
        with pytest.raises(ConnectionError):
            connected.update_container(JOB_ID)

    def test_camel_case_keys_pass_through_untouched(self, connected):
        """Callers must be able to set an attribute this connector never named.

        The API has 674 paths and gains fields faster than this wrapper does, so
        an already-camelCase key is forwarded as-is rather than mangled.
        """
        connected._session.request.return_value = _make_response(200, _container())
        connected.update_container(JOB_ID, someBrandNewField="x")
        assert connected._session.request.call_args.kwargs["json"] == {
            "someBrandNewField": "x"
        }

    def test_clone_defaults_to_not_cloning_the_schedule(self, connected):
        """A clone that inherits the schedule silently doubles a production job."""
        connected._session.request.return_value = _make_response(200, {"id": 1001})
        connected.clone_container(JOB_ID)
        body = connected._session.request.call_args.kwargs["json"]
        assert body == {"cloneSchedule": False, "cloneTriggers": False,
                        "cloneNotifications": False}

    def test_delete_container_issues_delete(self, connected):
        connected._session.request.return_value = _make_response(204, content=b"")
        connected.delete_container(JOB_ID)
        assert connected._session.request.call_args.args[0] == "DELETE"

    def test_list_scripts_requires_no_filters_but_forwards_them(self, connected):
        connected._session.request.return_value = _list_page([])
        connected.list_scripts(type="containers", author=ME_ID)
        params = connected._session.request.call_args.kwargs["params"]
        assert params["type"] == "containers"
        assert params["author"] == ME_ID


# -- Workflows ----------------------------------------------------------------


class TestWorkflows:
    def test_execute_workflow_posts_execution(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"id": 5001, "state": "queued"}
        )
        ex = connected.execute_workflow(119217)
        assert ex["id"] == 5001
        assert connected._session.request.call_args.args[0] == "POST"

    def test_execute_workflow_target_task_is_camelized(self, connected):
        connected._session.request.return_value = _make_response(200, {"id": 1})
        connected.execute_workflow(119217, target_task="AB Inserts")
        assert connected._session.request.call_args.kwargs["json"] == {
            "targetTask": "AB Inserts"
        }

    def test_execute_workflow_sends_no_body_when_no_target(self, connected):
        connected._session.request.return_value = _make_response(200, {"id": 1})
        connected.execute_workflow(119217)
        assert connected._session.request.call_args.kwargs["json"] is None

    def test_retry_execution_can_target_one_task(self, connected):
        connected._session.request.return_value = _make_response(200, {})
        connected.retry_execution(119217, 5001, task_name="AB Notes Append")
        assert connected._session.request.call_args.kwargs["json"] == {
            "taskName": "AB Notes Append"
        }

    def test_update_workflow_rejects_empty_change(self, connected):
        with pytest.raises(ConnectionError):
            connected.update_workflow(119217)


# -- Credentials --------------------------------------------------------------


class TestCredentials:
    def test_create_credential_sends_password_in_body(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"id": 40000, "name": "TEST_KEY"}
        )
        connected.create_credential(type="Custom", name="TEST_KEY",
                                    password="s3cret", username="rkerth")
        body = connected._session.request.call_args.kwargs["json"]
        assert body["password"] == "s3cret"
        assert body["name"] == "TEST_KEY"
        assert body["username"] == "rkerth"

    def test_update_credential_does_not_log_the_secret(self, connected, caplog):
        """Rotation is the main reason to PATCH a credential.

        The log line has to be useful without becoming the place a secret ends
        up on disk, so it names the fields changed and says '+password' rather
        than printing it.
        """
        connected._session.request.return_value = _make_response(200, {})
        with caplog.at_level("INFO"):
            connected.update_credential(39428, password="rotated-value",
                                        description="new note")
        logged = " ".join(r.message for r in caplog.records)
        assert "rotated-value" not in logged
        assert "+password" in logged
        assert "description" in logged


# -- Spec discovery -----------------------------------------------------------


class TestSpec:
    def test_spec_is_cached(self, connected):
        connected._session.request.return_value = _make_response(
            200, {"paths": {"/jobs": {"get": {"summary": "List Jobs"}}}}
        )
        connected.spec()
        connected.spec()
        assert connected._session.request.call_count == 1

    def test_spec_unwraps_a_list_response(self, connected):
        connected._session.request.return_value = _make_response(
            200, [{"paths": {}}], content=b"[]"
        )
        assert connected.spec() == {"paths": {}}

    def test_find_endpoints_matches_path_and_summary(self, connected):
        connected._spec_cache = {
            "paths": {
                "/workflows/{id}/executions": {
                    "post": {"summary": "Execute a workflow"}},
                "/jobs": {"get": {"summary": "List Jobs"}},
                "/scripts": {"get": {"summary": "List Scripts"}, "parameters": []},
            }
        }
        by_path = connected.find_endpoints("workflow")
        assert ("POST", "/workflows/{id}/executions", "Execute a workflow") in by_path
        by_summary = connected.find_endpoints("list jobs")
        assert [t[1] for t in by_summary] == ["/jobs"]

    def test_find_endpoints_skips_non_operation_entries(self, connected):
        connected._spec_cache = {"paths": {"/x": {"parameters": ["not-an-op"]}}}
        assert connected.find_endpoints("x") == []


# -- Schedule helpers ---------------------------------------------------------


class TestDescribeSchedule:
    def test_none_and_unscheduled(self):
        assert describe_schedule(None) == "not scheduled"
        assert describe_schedule({}) == "not scheduled"
        assert describe_schedule(UNSCHEDULED) == "not scheduled"

    def test_daily(self):
        assert describe_schedule(build_schedule(hours=6, minute=45)) == "daily 06:45"

    def test_single_day(self):
        assert describe_schedule(build_schedule(hours=2, days=0)) == "Su 02:00"

    def test_multiple_days_and_hours(self):
        s = build_schedule(hours=[4, 16], minute=30, days=[1, 3, 5])
        assert describe_schedule(s) == "MoWeFr 04:30, 16:30"

    def test_timezone_suffix_is_appended(self):
        """Civis fires a schedule in the OWNING ACCOUNT's timezone, not UTC.

        Rendering a time without the zone is how a 6am ET job gets documented
        as 6am UTC.
        """
        s = build_schedule(hours=6, minute=45)
        assert describe_schedule(s, "America/New_York") == (
            "daily 06:45 America/New_York"
        )

    def test_monthly_day_of_month(self):
        """A real CC job ('AN/AB Tables Monthly Audit') uses this form.

        Its scheduledDays list is empty, so a formatter that only looks at
        weekdays renders it as unknown -- which is how it went missing from the
        schedule rollup.
        """
        s = build_schedule(hours=3, days=[], days_of_month=[1])
        assert describe_schedule(s) == "monthly day 1 03:00"

    def test_runs_per_hour_form(self):
        s = build_schedule(hours=[], runs_per_hour=4)
        assert describe_schedule(s) == "4x/hour"

    def test_scheduled_with_no_day_selected_says_so(self):
        """This configuration never fires; it must not read as 'daily'."""
        s = {"scheduled": True, "scheduledDays": [], "scheduledHours": [3],
             "scheduledMinutes": [0]}
        assert "no day selected" in describe_schedule(s)

    def test_missing_hours_does_not_crash(self):
        s = {"scheduled": True, "scheduledDays": [0, 1, 2, 3, 4, 5, 6]}
        assert describe_schedule(s) == "daily ??:??"

    def test_absent_days_key_renders_time_only(self):
        assert describe_schedule(
            {"scheduled": True, "scheduledHours": [5], "scheduledMinutes": [0]}
        ) == "05:00"


class TestBuildSchedule:
    def test_defaults_to_daily(self):
        assert build_schedule(hours=6)["scheduledDays"] == list(range(7))

    def test_scalars_become_lists(self):
        s = build_schedule(hours=6, minute=45, days=3, days_of_month=1)
        assert s["scheduledHours"] == [6]
        assert s["scheduledMinutes"] == [45]
        assert s["scheduledDays"] == [3]
        assert s["scheduledDaysOfMonth"] == [1]

    def test_lists_are_sorted(self):
        assert build_schedule(hours=[16, 4])["scheduledHours"] == [4, 16]

    def test_round_trips_through_describe(self):
        s = build_schedule(hours=22, minute=0)
        assert describe_schedule(s) == "daily 22:00"

    def test_unscheduled_constant_shape(self):
        assert UNSCHEDULED == {"scheduled": False}


# -- Internal helpers ---------------------------------------------------------


class TestHelpers:
    def test_clean_drops_none_only(self):
        assert _clean(a=1, b=None, c=False, d="") == {"a": 1, "c": False, "d": ""}

    def test_camelize_converts_snake_case(self):
        assert _camelize({"docker_image_tag": "8.5.0"}) == {"dockerImageTag": "8.5.0"}

    def test_camelize_handles_multi_underscore(self):
        assert _camelize({"remote_host_credential_id": 1}) == {
            "remoteHostCredentialId": 1
        }

    def test_camelize_leaves_camel_case_alone(self):
        assert _camelize({"repoHttpUri": "x"}) == {"repoHttpUri": "x"}


# -- Surface pins -------------------------------------------------------------


class TestSurface:
    def test_write_surface_is_pinned(self, connector):
        """The set of mutators is pinned, so adding one is a deliberate edit.

        This connector deliberately carries FULL CRUD -- that was an explicit
        call, made knowing the key holds 'manage' on a platform shared with 84
        users across many TMC member orgs. The pin does not restrict what can be
        added; it just means nobody widens the blast radius by accident, and the
        list stays readable next to `is_mine`.
        """
        allowed = {
            # containers
            "create_container",
            "update_container",
            "replace_container",
            "clone_container",
            "delete_container",
            # workflows
            "create_workflow",
            "update_workflow",
            "replace_workflow",
            "clone_workflow",
            "execute_workflow",
            "cancel_execution",
            "retry_execution",
            "resume_execution",
            "set_workflow_archived",
            # credentials
            "create_credential",
            "update_credential",
            "delete_credential",
            # runs
            "run_job",
            "cancel_run",
            # lifecycle
            "set_job_archived",
        }
        verbs = ("create_", "update_", "replace_", "clone_", "delete_", "set_",
                 "run_job", "cancel_", "retry_execution", "resume_",
                 "execute_")
        actual = {
            name for name in dir(connector)
            if not name.startswith("_") and name.startswith(verbs)
        }
        assert actual == allowed

    def test_archive_is_offered_alongside_every_delete(self, connector):
        """Archiving is reversible and keeps run history; deleting is not.

        Civis itself deprecates DELETE on containers in favour of the archive
        endpoints, so both must stay reachable -- a caller who only finds
        `delete_container` will use it.
        """
        assert hasattr(connector, "set_job_archived")
        assert hasattr(connector, "set_workflow_archived")

    def test_generic_request_escape_hatch_exists(self, connected):
        """674 paths; the typed methods cover maybe 40 of them."""
        connected._session.request.return_value = _make_response(200, {"ok": True})
        assert connected.request("GET", "/announcements") == {"ok": True}
