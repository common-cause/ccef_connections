"""Tests for the core modules: exceptions, base connection, credentials, and retry."""

import json
from unittest.mock import MagicMock, patch

import pytest

from ccef_connections.exceptions import (
    AuthenticationError,
    CCEFConnectionError,
    ConfigurationError,
    ConnectionError,
    CredentialError,
    QueryError,
    RateLimitError,
    WriteError,
)
from ccef_connections.core.base import BaseConnection
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.core.retry import (
    _is_google_retryable,
    retry_action_network_operation,
    retry_airtable_operation,
    retry_google_operation,
    retry_helpscout_operation,
    retry_openai_operation,
    retry_snowflake_operation,
    retry_with_backoff,
    retry_zoom_operation,
)


# ── Helpers ──────────────────────────────────────────────────────────


class ConcreteConnector(BaseConnection):
    """Minimal concrete subclass of BaseConnection for testing."""

    def connect(self) -> None:
        self._is_connected = True

    def disconnect(self) -> None:
        self._is_connected = False

    def health_check(self) -> bool:
        return self._is_connected


def _make_manager():
    """Create a fresh CredentialManager instance, bypassing the singleton."""
    mgr = object.__new__(CredentialManager)
    mgr._credentials_cache = {}
    mgr._env_loaded = True  # skip dotenv reload
    return mgr


# ── Exceptions ───────────────────────────────────────────────────────


class TestExceptions:
    """Test the custom exception hierarchy."""

    def test_ccef_connection_error_is_base(self):
        """CCEFConnectionError inherits from Exception."""
        assert issubclass(CCEFConnectionError, Exception)

    @pytest.mark.parametrize(
        "exc_class",
        [
            CredentialError,
            ConnectionError,
            AuthenticationError,
            RateLimitError,
            ConfigurationError,
            QueryError,
            WriteError,
        ],
    )
    def test_all_exceptions_inherit_from_base(self, exc_class):
        """Every custom exception inherits from CCEFConnectionError."""
        assert issubclass(exc_class, CCEFConnectionError)

    def test_rate_limit_error_stores_retry_after(self):
        """RateLimitError stores the retry_after attribute."""
        err = RateLimitError("rate limited", retry_after=30)
        assert str(err) == "rate limited"
        assert err.retry_after == 30

    def test_rate_limit_error_retry_after_defaults_to_none(self):
        """RateLimitError.retry_after defaults to None when not provided."""
        err = RateLimitError("rate limited")
        assert err.retry_after is None

    @pytest.mark.parametrize(
        "exc_class,args",
        [
            (CCEFConnectionError, ("base error",)),
            (CredentialError, ("cred error",)),
            (ConnectionError, ("conn error",)),
            (AuthenticationError, ("auth error",)),
            (RateLimitError, ("rate error",)),
            (ConfigurationError, ("config error",)),
            (QueryError, ("query error",)),
            (WriteError, ("write error",)),
        ],
    )
    def test_exceptions_can_be_raised_and_caught(self, exc_class, args):
        """Every exception type can be raised and caught."""
        with pytest.raises(exc_class):
            raise exc_class(*args)

    def test_catch_subclass_via_base(self):
        """Catching CCEFConnectionError also catches all subclasses."""
        with pytest.raises(CCEFConnectionError):
            raise CredentialError("missing key")

        with pytest.raises(CCEFConnectionError):
            raise RateLimitError("too fast", retry_after=5)

        with pytest.raises(CCEFConnectionError):
            raise QueryError("bad SQL")


# ── BaseConnection ───────────────────────────────────────────────────


class TestBaseConnection:
    """Test the abstract BaseConnection class."""

    def test_cannot_instantiate_directly(self):
        """BaseConnection is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract method"):
            BaseConnection()

    def test_concrete_subclass_works(self):
        """A concrete subclass that implements all abstract methods can be instantiated."""
        connector = ConcreteConnector()
        assert connector is not None
        assert isinstance(connector, BaseConnection)

    def test_is_connected_returns_false_initially(self):
        """A new connector starts disconnected."""
        connector = ConcreteConnector()
        assert connector.is_connected() is False

    def test_is_connected_after_connect(self):
        """is_connected returns True after connect() is called."""
        connector = ConcreteConnector()
        connector.connect()
        assert connector.is_connected() is True

    def test_is_connected_after_disconnect(self):
        """is_connected returns False after disconnect() is called."""
        connector = ConcreteConnector()
        connector.connect()
        connector.disconnect()
        assert connector.is_connected() is False

    def test_context_manager_calls_connect_and_disconnect(self):
        """The context manager calls connect on entry and disconnect on exit."""
        connector = ConcreteConnector()

        with connector as c:
            assert c is connector
            assert c.is_connected() is True

        assert connector.is_connected() is False

    def test_context_manager_calls_disconnect_on_exception(self):
        """disconnect is called even when an exception occurs inside the with block."""
        connector = ConcreteConnector()

        with pytest.raises(ValueError):
            with connector:
                assert connector.is_connected() is True
                raise ValueError("boom")

        assert connector.is_connected() is False

    def test_repr_disconnected(self):
        """__repr__ shows status=disconnected when not connected."""
        connector = ConcreteConnector()
        assert repr(connector) == "<ConcreteConnector status=disconnected>"

    def test_repr_connected(self):
        """__repr__ shows status=connected after connect."""
        connector = ConcreteConnector()
        connector.connect()
        assert repr(connector) == "<ConcreteConnector status=connected>"

    def test_client_is_none_initially(self):
        """The internal _client is None by default."""
        connector = ConcreteConnector()
        assert connector._client is None

    def test_health_check_delegates_to_subclass(self):
        """health_check returns the value from the concrete implementation."""
        connector = ConcreteConnector()
        assert connector.health_check() is False
        connector.connect()
        assert connector.health_check() is True


# ── CredentialManager ────────────────────────────────────────────────


class TestCredentialManager:
    """Test the CredentialManager class."""

    def test_singleton_pattern(self):
        """Two calls to CredentialManager() return the same instance."""
        a = CredentialManager()
        b = CredentialManager()
        assert a is b

    def test_get_credential_reads_env_var(self):
        """get_credential reads from {NAME}_PASSWORD environment variable."""
        with patch.dict("os.environ", {"MY_KEY_PASSWORD": "secret123"}):
            cm = _make_manager()
            result = cm.get_credential("MY_KEY")
        assert result == "secret123"

    def test_get_credential_required_true_raises_when_missing(self):
        """get_credential with required=True raises CredentialError when env var is missing."""
        with patch.dict("os.environ", {}, clear=True):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="MISSING_KEY_PASSWORD"):
                cm.get_credential("MISSING_KEY", required=True)

    def test_get_credential_required_false_returns_none_when_missing(self):
        """get_credential with required=False returns None when env var is missing."""
        with patch.dict("os.environ", {}, clear=True):
            cm = _make_manager()
            result = cm.get_credential("MISSING_KEY", required=False)
        assert result is None

    def test_get_credential_is_json_parses_json(self):
        """get_credential with is_json=True parses the value as JSON."""
        json_data = {"key": "value", "num": 42}
        with patch.dict("os.environ", {"JSON_CRED_PASSWORD": json.dumps(json_data)}):
            cm = _make_manager()
            result = cm.get_credential("JSON_CRED", is_json=True)
        assert result == json_data

    def test_get_credential_is_json_raises_on_invalid_json(self):
        """get_credential with is_json=True raises CredentialError on invalid JSON."""
        with patch.dict("os.environ", {"BAD_JSON_PASSWORD": "not-valid-json{"}):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="Failed to parse JSON"):
                cm.get_credential("BAD_JSON", is_json=True)

    def test_caching_returns_same_value(self):
        """A second call for the same credential returns the cached value."""
        with patch.dict("os.environ", {"CACHED_KEY_PASSWORD": "first_value"}):
            cm = _make_manager()
            first = cm.get_credential("CACHED_KEY")

        # Even though the env var is gone, cached value is returned
        second = cm.get_credential("CACHED_KEY")
        assert first == second == "first_value"

    def test_clear_cache_works(self):
        """clear_cache empties the credential cache."""
        with patch.dict("os.environ", {"CACHE_TEST_PASSWORD": "val"}):
            cm = _make_manager()
            cm.get_credential("CACHE_TEST")
            assert "CACHE_TEST" in cm._credentials_cache

            cm.clear_cache()
            assert "CACHE_TEST" not in cm._credentials_cache

    def test_clear_cache_then_fetch_re_reads_env(self):
        """After clear_cache, the next get_credential re-reads from os.environ."""
        with patch.dict("os.environ", {"REFRESH_PASSWORD": "old"}):
            cm = _make_manager()
            assert cm.get_credential("REFRESH") == "old"

        cm.clear_cache()

        with patch.dict("os.environ", {"REFRESH_PASSWORD": "new"}):
            assert cm.get_credential("REFRESH") == "new"

    def test_has_credential_returns_true_when_present(self):
        """has_credential returns True when the env var exists."""
        with patch.dict("os.environ", {"EXISTS_PASSWORD": "yes"}):
            cm = _make_manager()
            assert cm.has_credential("EXISTS") is True

    def test_has_credential_returns_true_even_when_env_missing(self):
        """has_credential returns True when the env var is absent.

        NOTE: This reflects actual behaviour -- has_credential calls
        get_credential(required=False) which returns None without raising,
        so the try block always succeeds and returns True.  The method
        would only return False if get_credential raised CredentialError
        (which can happen for is_json=True with bad JSON, but not for a
        simple missing env var with required=False).
        """
        with patch.dict("os.environ", {}, clear=True):
            cm = _make_manager()
            # Because get_credential(required=False) returns None (no raise),
            # has_credential returns True -- this matches the current implementation.
            assert cm.has_credential("NOPE") is True

    # ── Shortcut methods ─────────────────────────────────────────────

    def test_get_airtable_key(self):
        """get_airtable_key reads AIRTABLE_API_KEY_PASSWORD."""
        with patch.dict("os.environ", {"AIRTABLE_API_KEY_PASSWORD": "at_key_123"}):
            cm = _make_manager()
            result = cm.get_airtable_key()
        assert result == "at_key_123"

    def test_get_openai_key(self):
        """get_openai_key reads OPENAI_API_KEY_PASSWORD."""
        with patch.dict("os.environ", {"OPENAI_API_KEY_PASSWORD": "sk-test"}):
            cm = _make_manager()
            result = cm.get_openai_key()
        assert result == "sk-test"

    def test_get_google_sheets_credentials(self):
        """get_google_sheets_credentials parses JSON from GOOGLE_SHEETS_CREDENTIALS_PASSWORD."""
        creds = {"type": "service_account", "project_id": "my-project"}
        with patch.dict(
            "os.environ",
            {"GOOGLE_SHEETS_CREDENTIALS_PASSWORD": json.dumps(creds)},
        ):
            cm = _make_manager()
            result = cm.get_google_sheets_credentials()
        assert result == creds

    def test_get_google_sheets_credentials_rejects_non_dict(self):
        """get_google_sheets_credentials raises if the JSON is not a dict."""
        with patch.dict(
            "os.environ",
            {"GOOGLE_SHEETS_CREDENTIALS_PASSWORD": json.dumps(["not", "a", "dict"])},
        ):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="valid JSON object"):
                cm.get_google_sheets_credentials()

    def test_get_bigquery_credentials(self):
        """get_bigquery_credentials parses JSON from BIGQUERY_CREDENTIALS_PASSWORD."""
        creds = {"type": "service_account", "project_id": "bq-project"}
        with patch.dict(
            "os.environ",
            {"BIGQUERY_CREDENTIALS_PASSWORD": json.dumps(creds)},
        ):
            cm = _make_manager()
            result = cm.get_bigquery_credentials()
        assert result == creds

    def test_get_bigquery_credentials_rejects_non_dict(self):
        """get_bigquery_credentials raises if the JSON is not a dict."""
        with patch.dict(
            "os.environ",
            {"BIGQUERY_CREDENTIALS_PASSWORD": json.dumps("just a string")},
        ):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="valid JSON object"):
                cm.get_bigquery_credentials()

    def test_get_helpscout_credentials(self):
        """get_helpscout_credentials parses JSON with app_id and app_secret."""
        creds = {"app_id": "hs-id", "app_secret": "hs-secret"}
        with patch.dict(
            "os.environ",
            {"HELPSCOUT_CREDENTIALS_PASSWORD": json.dumps(creds)},
        ):
            cm = _make_manager()
            result = cm.get_helpscout_credentials()
        assert result["app_id"] == "hs-id"
        assert result["app_secret"] == "hs-secret"

    def test_get_helpscout_credentials_missing_keys(self):
        """get_helpscout_credentials raises if required keys are missing."""
        creds = {"app_id": "only-id"}
        with patch.dict(
            "os.environ",
            {"HELPSCOUT_CREDENTIALS_PASSWORD": json.dumps(creds)},
        ):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="app_secret"):
                cm.get_helpscout_credentials()

    def test_get_helpscout_credentials_rejects_non_dict(self):
        """get_helpscout_credentials raises if the JSON is not a dict."""
        with patch.dict(
            "os.environ",
            {"HELPSCOUT_CREDENTIALS_PASSWORD": json.dumps(42)},
        ):
            cm = _make_manager()
            with pytest.raises(CredentialError, match="valid JSON object"):
                cm.get_helpscout_credentials()


# ── Retry Decorators ─────────────────────────────────────────────────


class TestRetry:
    """Test retry decorators from the retry module."""

    def test_retry_with_backoff_creates_working_decorator(self):
        """retry_with_backoff returns a decorator that can wrap a function."""
        call_count = 0

        @retry_with_backoff(max_attempts=3, min_wait=0.01, max_wait=0.02)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("temporary failure")
            return "success"

        result = flaky()
        assert result == "success"
        assert call_count == 3

    def test_retry_with_backoff_reraises_after_max_attempts(self):
        """retry_with_backoff reraises the exception after exhausting attempts."""

        @retry_with_backoff(max_attempts=2, min_wait=0.01, max_wait=0.02)
        def always_fails():
            raise ConnectionError("persistent failure")

        with pytest.raises(ConnectionError, match="persistent failure"):
            always_fails()

    def test_retry_with_backoff_does_not_retry_unmatched_exceptions(self):
        """retry_with_backoff does not retry exceptions not in the exceptions tuple."""
        call_count = 0

        @retry_with_backoff(
            max_attempts=3,
            min_wait=0.01,
            max_wait=0.02,
            exceptions=(ConnectionError,),
        )
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retried")

        with pytest.raises(ValueError, match="not retried"):
            raises_value_error()

        assert call_count == 1

    def test_retry_with_backoff_succeeds_on_first_try(self):
        """When the function succeeds on the first call, no retries occur."""
        call_count = 0

        @retry_with_backoff(max_attempts=3, min_wait=0.01, max_wait=0.02)
        def works_first_time():
            nonlocal call_count
            call_count += 1
            return "immediate"

        result = works_first_time()
        assert result == "immediate"
        assert call_count == 1


# ── Service-specific decorators ──────────────────────────────────────
#
# These tests must never actually sleep. The decorators carry real
# exponential backoff (up to 60s per wait), so a retry-exhausting path costs
# ~30s of wall clock if the sleep is left live. Patching
# ``tenacity.nap.time.sleep`` works because tenacity's nap module resolves
# ``time.sleep`` dynamically at call time.

# Every decorator below is expected to retry rate limits and nothing else.
SERVICE_DECORATORS = [
    ("airtable", retry_airtable_operation),
    ("openai", retry_openai_operation),
    ("google", retry_google_operation),
    ("helpscout", retry_helpscout_operation),
    ("zoom", retry_zoom_operation),
    ("action_network", retry_action_network_operation),
    ("snowflake", retry_snowflake_operation),
]

SERVICE_IDS = [name for name, _ in SERVICE_DECORATORS]


@pytest.mark.parametrize("name,decorator", SERVICE_DECORATORS, ids=SERVICE_IDS)
class TestServiceRetryDecorators:
    """Each service decorator retries RateLimitError, and only RateLimitError."""

    @patch("tenacity.nap.time.sleep")
    def test_retries_rate_limit_then_succeeds(self, mock_sleep, name, decorator):
        """A RateLimitError is retried, and a later success is returned."""
        call_count = 0

        @decorator
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RateLimitError("rate limited", retry_after=1)
            return f"{name}_ok"

        assert flaky() == f"{name}_ok"
        assert call_count == 3
        assert mock_sleep.called

    @patch("tenacity.nap.time.sleep")
    def test_reraises_rate_limit_after_five_attempts(self, mock_sleep, name, decorator):
        """A persistent RateLimitError is reraised after 5 attempts."""
        call_count = 0

        @decorator
        def always_rate_limited():
            nonlocal call_count
            call_count += 1
            raise RateLimitError(f"{name} rate limited", retry_after=1)

        with pytest.raises(RateLimitError, match=f"{name} rate limited"):
            always_rate_limited()

        assert call_count == 5

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_connection_error(self, mock_sleep, name, decorator):
        """ConnectionError wraps a 4xx/5xx response — it must fail immediately.

        Regression guard: these predicates once included bare ``Exception``,
        which made every deterministic failure cost ~30s of backoff before
        surfacing.
        """
        call_count = 0

        @decorator
        def not_connected():
            nonlocal call_count
            call_count += 1
            raise ConnectionError(f"{name} down")

        with pytest.raises(ConnectionError, match=f"{name} down"):
            not_connected()

        assert call_count == 1
        assert not mock_sleep.called

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_generic_exception(self, mock_sleep, name, decorator):
        """A bug in our own code surfaces immediately, not after 5 attempts."""
        call_count = 0

        @decorator
        def buggy():
            nonlocal call_count
            call_count += 1
            raise KeyError("missing_field")

        with pytest.raises(KeyError, match="missing_field"):
            buggy()

        assert call_count == 1
        assert not mock_sleep.called

    @patch("tenacity.nap.time.sleep")
    def test_does_not_retry_auth_error(self, mock_sleep, name, decorator):
        """A bad credential will never succeed on attempt 2."""
        call_count = 0

        @decorator
        def unauthorized():
            nonlocal call_count
            call_count += 1
            raise AuthenticationError(f"{name} auth failed")

        with pytest.raises(AuthenticationError):
            unauthorized()

        assert call_count == 1
        assert not mock_sleep.called


class TestGoogleRateLimitPredicate:
    """_is_google_retryable matches what is safe to replay for Google APIs.

    The Google connectors do not translate HTTP status into our exception
    hierarchy — gspread and google-cloud-bigquery raise their own types — so
    this predicate is what keeps 429 handling alive for them.

    Since 0.13.0 it also matches transient 5xx from gspread, and the asymmetry
    with api_core is the point of several tests below: BigQuery's DML methods
    share this decorator, so a 5xx there must NOT be replayed.
    """

    @staticmethod
    def _gspread_api_error(status_code):
        """Build a real gspread APIError carrying the given HTTP status."""
        from gspread.exceptions import APIError

        response = MagicMock()
        response.status_code = status_code
        response.json.return_value = {
            "error": {"code": status_code, "message": "boom", "status": "ERROR"}
        }
        return APIError(response)

    def test_matches_our_rate_limit_error(self):
        assert _is_google_retryable(RateLimitError("slow down")) is True

    def test_matches_api_core_too_many_requests(self):
        from google.api_core.exceptions import TooManyRequests

        assert _is_google_retryable(TooManyRequests("429")) is True

    def test_does_not_match_api_core_service_unavailable(self):
        """A 503 through api_core is NOT safe to replay, unlike the gspread one.

        BigQueryConnector.query / execute_dml / insert_rows sit under the same
        decorator and carry MERGE/INSERT, so replaying a 5xx there could
        double-apply a write. google-cloud-bigquery also retries transient
        errors itself, so nothing is lost by declining here. This asymmetry is
        deliberate — see _GSPREAD_TRANSIENT_STATUSES.
        """
        from google.api_core.exceptions import ServiceUnavailable

        assert _is_google_retryable(ServiceUnavailable("503")) is False

    def test_matches_gspread_429(self):
        assert _is_google_retryable(self._gspread_api_error(429)) is True

    @pytest.mark.parametrize("status_code", [500, 502, 503, 504])
    def test_matches_transient_gspread_5xx(self, status_code):
        """gspread has no retry of its own and a Sheets 503 is a routine blip.

        Regression guard for the ep-syncs volunteer-sheets job, which failed on
        5 of 8 consecutive nightly runs with a bare 503 and no 429 in sight.
        """
        assert _is_google_retryable(self._gspread_api_error(status_code)) is True

    @pytest.mark.parametrize("status_code", [400, 403, 404, 501])
    def test_does_not_match_permanent_gspread_statuses(self, status_code):
        """4xx will never succeed on attempt 2; 501 is a permanent answer."""
        assert _is_google_retryable(self._gspread_api_error(status_code)) is False

    def test_does_not_match_gspread_worksheet_not_found(self):
        from gspread.exceptions import WorksheetNotFound

        assert _is_google_retryable(WorksheetNotFound("Missing")) is False

    def test_does_not_match_our_connection_error(self):
        assert _is_google_retryable(ConnectionError("Not connected")) is False

    @pytest.mark.parametrize(
        "exc", [Exception("unexpected"), KeyError("k"), TypeError("t")]
    )
    def test_does_not_match_generic_exceptions(self, exc):
        assert _is_google_retryable(exc) is False

    def test_degrades_gracefully_without_the_google_extras(self, monkeypatch):
        """core.retry is imported by the base install, where neither the sheets
        nor the bigquery extra is present. The vendor types resolve lazily for
        exactly that reason, so a missing extra must contribute nothing rather
        than raise ImportError."""
        import builtins

        import ccef_connections.core.retry as retry_module

        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.startswith(("google", "gspread")):
                raise ImportError(f"No module named {name!r}")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocked_import)
        monkeypatch.setattr(retry_module, "_GOOGLE_VENDOR_TYPES", None)

        assert retry_module._google_vendor_types() == {"api_core": (), "gspread": None}
        # Our own RateLimitError still matches; nothing else does.
        assert retry_module._is_google_retryable(RateLimitError("slow down")) is True
        assert retry_module._is_google_retryable(Exception("boom")) is False


class TestLazyImportRegistration:
    """Every connector must be registered in BOTH _LAZY_IMPORTS maps.

    Connectors lazy-load via PEP 562 ``__getattr__``, and the map is duplicated
    in ``ccef_connections/__init__.py`` and
    ``ccef_connections/connectors/__init__.py``. Registering in only one leaves
    the connector importable from one path and not the other — ZendeskConnector
    shipped that way and was missing from the top-level package entirely.
    """

    @staticmethod
    def _maps():
        """Both maps, filtered to connectors.

        The top-level map also carries ConfigManager, which is not a connector
        and correctly has no entry in the connectors package.
        """
        import ccef_connections
        import ccef_connections.connectors as connectors_pkg

        def connectors_only(mapping):
            return {k: v for k, v in mapping.items() if k.endswith("Connector")}

        return (
            connectors_only(ccef_connections._LAZY_IMPORTS),
            connectors_only(connectors_pkg._LAZY_IMPORTS),
        )

    def test_both_maps_register_the_same_names(self):
        top_level, connectors = self._maps()
        assert set(top_level) == set(connectors)

    def test_both_maps_agree_on_the_required_extra(self):
        """A connector must not claim different extras from the two paths."""
        top_level, connectors = self._maps()
        mismatched = {
            name: (top_level[name][1], connectors[name][1])
            for name in set(top_level) & set(connectors)
            if top_level[name][1] != connectors[name][1]
        }
        assert mismatched == {}

    def test_all_exports_are_resolvable(self):
        """Every name in __all__ actually resolves, not just appears in a list."""
        import ccef_connections

        for name in ccef_connections.__all__:
            assert getattr(ccef_connections, name) is not None, name

    def test_every_connector_module_is_registered(self):
        """A new connector file must not be left out of the maps entirely."""
        import pkgutil

        import ccef_connections.connectors as connectors_pkg

        on_disk = {
            module.name
            for module in pkgutil.iter_modules(connectors_pkg.__path__)
            if not module.name.startswith("_")
        }
        registered = {
            module_name for module_name, _extra in connectors_pkg._LAZY_IMPORTS.values()
        }
        assert on_disk - registered == set()
