"""Tests for the ConfigManager configuration management module."""

import time
from unittest.mock import MagicMock, patch

import pytest

from ccef_connections.config import ConfigManager
from ccef_connections.exceptions import ConfigurationError


# ── Sample Data ──────────────────────────────────────────────────────

SAMPLE_SHEETS_DATA = [
    {"Section": "airtable", "Key": "base_id", "Value": "appXXX123", "Description": "Airtable base"},
    {"Section": "airtable", "Key": "table_name", "Value": "Test Input", "Description": "Table name"},
    {"Section": "airtable", "Key": "enabled", "Value": "true", "Description": "Enabled flag"},
    {"Section": "openai", "Key": "model", "Value": "gpt-4o", "Description": "Model name"},
    {"Section": "openai", "Key": "temperature", "Value": "0.7", "Description": "Temperature"},
    {"Section": "openai", "Key": "max_tokens", "Value": "4096", "Description": "Max tokens"},
]

EXPECTED_PARSED = {
    "airtable": {
        "base_id": "appXXX123",
        "table_name": "Test Input",
        "enabled": True,
    },
    "openai": {
        "model": "gpt-4o",
        "temperature": 0.7,
        "max_tokens": 4096,
    },
}


# ── Fixtures ─────────────────────────────────────────────────────────


@pytest.fixture
def mock_sheets_connector():
    """Create a mock SheetsConnector."""
    mock = MagicMock()
    mock.get_worksheet_as_dicts.return_value = SAMPLE_SHEETS_DATA
    return mock


@pytest.fixture
def manager():
    """Create a ConfigManager with auto_refresh disabled (no Sheets calls on get_config)."""
    return ConfigManager(
        sheets_id="test-spreadsheet-id",
        worksheet_name="Config",
        ttl=300,
        auto_refresh=False,
    )


@pytest.fixture
def auto_manager(mock_sheets_connector):
    """Create a ConfigManager with auto_refresh enabled and a mocked SheetsConnector."""
    mgr = ConfigManager(
        sheets_id="test-spreadsheet-id",
        worksheet_name="Config",
        ttl=300,
        auto_refresh=True,
    )
    mgr._sheets_connector = mock_sheets_connector
    return mgr


@pytest.fixture
def loaded_manager(manager, mock_sheets_connector):
    """Create a ConfigManager that already has cached config loaded."""
    manager._sheets_connector = mock_sheets_connector
    manager._config_cache = EXPECTED_PARSED.copy()
    manager._config_cache["airtable"] = EXPECTED_PARSED["airtable"].copy()
    manager._config_cache["openai"] = EXPECTED_PARSED["openai"].copy()
    manager._cache_timestamp = time.time()
    return manager


# ── Initialization ───────────────────────────────────────────────────


class TestInit:
    def test_stores_sheets_id(self):
        mgr = ConfigManager(sheets_id="my-sheet-id")
        assert mgr._sheets_id == "my-sheet-id"

    def test_stores_worksheet_name_default(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._worksheet_name == "Config"

    def test_stores_worksheet_name_custom(self):
        mgr = ConfigManager(sheets_id="id", worksheet_name="Settings")
        assert mgr._worksheet_name == "Settings"

    def test_stores_ttl_default(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._ttl == 300

    def test_stores_ttl_custom(self):
        mgr = ConfigManager(sheets_id="id", ttl=60)
        assert mgr._ttl == 60

    def test_stores_auto_refresh_default(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._auto_refresh is True

    def test_stores_auto_refresh_false(self):
        mgr = ConfigManager(sheets_id="id", auto_refresh=False)
        assert mgr._auto_refresh is False

    def test_initial_cache_is_none(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._config_cache is None

    def test_initial_cache_timestamp_is_zero(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._cache_timestamp == 0.0

    def test_initial_sheets_connector_is_none(self):
        mgr = ConfigManager(sheets_id="id")
        assert mgr._sheets_connector is None


# ── get_config() ─────────────────────────────────────────────────────


class TestGetConfig:
    def test_no_cache_auto_refresh_disabled_raises(self, manager):
        """get_config() with no cache and auto_refresh=False raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="No configuration available"):
            manager.get_config()

    def test_no_cache_refresh_if_expired_false_raises(self, auto_manager):
        """get_config(refresh_if_expired=False) with no cache and auto_refresh=True raises."""
        auto_manager._config_cache = None
        with pytest.raises(ConfigurationError, match="No configuration available"):
            auto_manager.get_config(refresh_if_expired=False)

    def test_returns_cached_config_when_valid(self, loaded_manager):
        """get_config() returns cached config without refreshing when cache is valid."""
        result = loaded_manager.get_config()
        assert result["airtable"]["base_id"] == "appXXX123"
        assert result["openai"]["model"] == "gpt-4o"

    def test_triggers_refresh_when_cache_expired(self, auto_manager, mock_sheets_connector):
        """get_config() triggers refresh when cache has expired."""
        # Set an old timestamp so the cache appears expired
        auto_manager._config_cache = {"old": {"key": "value"}}
        auto_manager._cache_timestamp = time.time() - 999

        result = auto_manager.get_config()

        mock_sheets_connector.get_worksheet_as_dicts.assert_called_once_with(
            "test-spreadsheet-id", "Config"
        )
        assert "airtable" in result
        assert "openai" in result

    def test_triggers_refresh_when_no_cache(self, auto_manager, mock_sheets_connector):
        """get_config() triggers refresh when cache is None."""
        result = auto_manager.get_config()

        mock_sheets_connector.get_worksheet_as_dicts.assert_called_once()
        assert result is not None

    def test_returns_expired_cache_when_auto_refresh_disabled(self, manager):
        """get_config() returns expired cache when auto_refresh is disabled."""
        manager._config_cache = {"stale": {"key": "old_value"}}
        manager._cache_timestamp = time.time() - 999

        result = manager.get_config()

        assert result == {"stale": {"key": "old_value"}}

    def test_refresh_failure_raises_configuration_error(self, auto_manager, mock_sheets_connector):
        """get_config() raises ConfigurationError when refresh fails."""
        mock_sheets_connector.get_worksheet_as_dicts.side_effect = Exception("API error")

        with pytest.raises(ConfigurationError, match="Failed to refresh configuration"):
            auto_manager.get_config()

    def test_does_not_refresh_when_cache_valid(self, auto_manager, mock_sheets_connector):
        """get_config() does not call refresh when cache is still valid."""
        auto_manager._config_cache = EXPECTED_PARSED
        auto_manager._cache_timestamp = time.time()

        auto_manager.get_config()

        mock_sheets_connector.get_worksheet_as_dicts.assert_not_called()


# ── refresh() ────────────────────────────────────────────────────────


class TestRefresh:
    @patch("ccef_connections.config.SheetsConnector")
    def test_creates_sheets_connector_on_first_call(self, mock_cls):
        """refresh() creates a SheetsConnector if one does not exist."""
        mock_instance = MagicMock()
        mock_instance.get_worksheet_as_dicts.return_value = SAMPLE_SHEETS_DATA
        mock_cls.return_value = mock_instance

        mgr = ConfigManager(sheets_id="test-id")
        mgr.refresh()

        mock_cls.assert_called_once()
        assert mgr._sheets_connector is mock_instance

    def test_reads_from_sheets_connector(self, auto_manager, mock_sheets_connector):
        """refresh() reads from SheetsConnector with correct arguments."""
        auto_manager.refresh()

        mock_sheets_connector.get_worksheet_as_dicts.assert_called_once_with(
            "test-spreadsheet-id", "Config"
        )

    def test_stores_parsed_config_in_cache(self, auto_manager, mock_sheets_connector):
        """refresh() stores the parsed config in the cache."""
        auto_manager.refresh()

        assert auto_manager._config_cache is not None
        assert "airtable" in auto_manager._config_cache
        assert "openai" in auto_manager._config_cache
        assert auto_manager._config_cache["airtable"]["base_id"] == "appXXX123"

    def test_updates_cache_timestamp(self, auto_manager, mock_sheets_connector):
        """refresh() updates the cache timestamp."""
        before = time.time()
        auto_manager.refresh()
        after = time.time()

        assert before <= auto_manager._cache_timestamp <= after

    def test_failure_raises_configuration_error(self, auto_manager, mock_sheets_connector):
        """refresh() raises ConfigurationError when Sheets read fails."""
        mock_sheets_connector.get_worksheet_as_dicts.side_effect = Exception("Network error")

        with pytest.raises(ConfigurationError, match="Failed to refresh configuration"):
            auto_manager.refresh()

    def test_failure_preserves_original_exception(self, auto_manager, mock_sheets_connector):
        """refresh() chains the original exception as __cause__."""
        original = RuntimeError("underlying problem")
        mock_sheets_connector.get_worksheet_as_dicts.side_effect = original

        with pytest.raises(ConfigurationError) as exc_info:
            auto_manager.refresh()

        assert exc_info.value.__cause__ is original

    def test_reuses_existing_connector(self, auto_manager, mock_sheets_connector):
        """refresh() reuses the existing SheetsConnector on subsequent calls."""
        auto_manager.refresh()
        auto_manager.refresh()

        # The connector was set in the fixture; it should still be the same object
        assert auto_manager._sheets_connector is mock_sheets_connector
        assert mock_sheets_connector.get_worksheet_as_dicts.call_count == 2

    @patch("ccef_connections.config.SheetsConnector")
    def test_uses_custom_worksheet_name(self, mock_cls):
        """refresh() uses the worksheet_name specified at init."""
        mock_instance = MagicMock()
        mock_instance.get_worksheet_as_dicts.return_value = SAMPLE_SHEETS_DATA
        mock_cls.return_value = mock_instance

        mgr = ConfigManager(sheets_id="test-id", worksheet_name="Settings")
        mgr.refresh()

        mock_instance.get_worksheet_as_dicts.assert_called_once_with("test-id", "Settings")


# ── get() ────────────────────────────────────────────────────────────


class TestGet:
    def test_returns_specific_value(self, loaded_manager):
        """get() returns a specific config value by section and key."""
        assert loaded_manager.get("airtable", "base_id") == "appXXX123"

    def test_returns_default_for_missing_key(self, loaded_manager):
        """get() returns default when key is not found."""
        result = loaded_manager.get("airtable", "nonexistent", default="fallback")
        assert result == "fallback"

    def test_returns_default_for_missing_section(self, loaded_manager):
        """get() returns default when section is not found."""
        result = loaded_manager.get("nosection", "nokey", default="missing")
        assert result == "missing"

    def test_returns_none_when_no_default_and_missing(self, loaded_manager):
        """get() returns None when key is missing and no default is given."""
        result = loaded_manager.get("airtable", "nonexistent")
        assert result is None

    def test_triggers_refresh_via_get_config(self, auto_manager, mock_sheets_connector):
        """get() triggers config refresh through get_config() when cache is empty."""
        result = auto_manager.get("airtable", "base_id")
        assert result == "appXXX123"
        mock_sheets_connector.get_worksheet_as_dicts.assert_called_once()


# ── clear_cache() ────────────────────────────────────────────────────


class TestClearCache:
    def test_clears_config_cache(self, loaded_manager):
        """clear_cache() sets config cache to None."""
        loaded_manager.clear_cache()
        assert loaded_manager._config_cache is None

    def test_resets_cache_timestamp(self, loaded_manager):
        """clear_cache() resets cache timestamp to 0."""
        loaded_manager.clear_cache()
        assert loaded_manager._cache_timestamp == 0.0

    def test_subsequent_get_config_raises_without_auto_refresh(self, loaded_manager):
        """After clear_cache(), get_config() raises when auto_refresh is disabled."""
        loaded_manager.clear_cache()
        with pytest.raises(ConfigurationError, match="No configuration available"):
            loaded_manager.get_config()


# ── _parse_config() ──────────────────────────────────────────────────


class TestParseConfig:
    def test_normal_parsing(self, manager):
        """_parse_config() converts list of dicts to nested dict structure."""
        result = manager._parse_config(SAMPLE_SHEETS_DATA)

        assert "airtable" in result
        assert "openai" in result
        assert result["airtable"]["base_id"] == "appXXX123"
        assert result["airtable"]["table_name"] == "Test Input"
        assert result["openai"]["model"] == "gpt-4o"

    def test_converts_values(self, manager):
        """_parse_config() converts value types via _convert_value()."""
        result = manager._parse_config(SAMPLE_SHEETS_DATA)

        assert result["airtable"]["enabled"] is True
        assert result["openai"]["temperature"] == 0.7
        assert result["openai"]["max_tokens"] == 4096

    def test_skips_rows_missing_section(self, manager):
        """_parse_config() skips rows where Section is empty."""
        data = [
            {"Section": "", "Key": "orphan_key", "Value": "orphan_val"},
            {"Section": "valid", "Key": "good_key", "Value": "good_val"},
        ]
        result = manager._parse_config(data)

        assert "valid" in result
        assert result["valid"]["good_key"] == "good_val"
        assert "" not in result

    def test_skips_rows_missing_key(self, manager):
        """_parse_config() skips rows where Key is empty."""
        data = [
            {"Section": "valid", "Key": "", "Value": "no_key"},
            {"Section": "valid", "Key": "real_key", "Value": "real_val"},
        ]
        result = manager._parse_config(data)

        assert result["valid"] == {"real_key": "real_val"}

    def test_skips_rows_missing_both(self, manager):
        """_parse_config() skips rows where both Section and Key are empty."""
        data = [
            {"Section": "", "Key": "", "Value": "nothing"},
        ]
        result = manager._parse_config(data)
        assert result == {}

    def test_empty_list(self, manager):
        """_parse_config() returns empty dict for empty input."""
        result = manager._parse_config([])
        assert result == {}

    def test_strips_whitespace(self, manager):
        """_parse_config() strips whitespace from Section and Key."""
        data = [
            {"Section": "  airtable  ", "Key": "  base_id  ", "Value": "appXXX"},
        ]
        result = manager._parse_config(data)

        assert "airtable" in result
        assert "base_id" in result["airtable"]

    def test_multiple_rows_same_section(self, manager):
        """_parse_config() groups multiple keys under the same section."""
        data = [
            {"Section": "db", "Key": "host", "Value": "localhost"},
            {"Section": "db", "Key": "port", "Value": "5432"},
            {"Section": "db", "Key": "name", "Value": "mydb"},
        ]
        result = manager._parse_config(data)

        assert len(result) == 1
        assert len(result["db"]) == 3
        assert result["db"]["host"] == "localhost"
        assert result["db"]["port"] == 5432
        assert result["db"]["name"] == "mydb"

    def test_missing_section_key_in_dict(self, manager):
        """_parse_config() handles dicts that don't have Section or Key keys at all."""
        data = [
            {"Value": "no_section_or_key"},
            {"Section": "valid", "Key": "ok", "Value": "works"},
        ]
        result = manager._parse_config(data)

        assert result == {"valid": {"ok": "works"}}

    def test_duplicate_key_last_wins(self, manager):
        """_parse_config() overwrites a key if it appears again in the same section."""
        data = [
            {"Section": "app", "Key": "mode", "Value": "debug"},
            {"Section": "app", "Key": "mode", "Value": "production"},
        ]
        result = manager._parse_config(data)

        assert result["app"]["mode"] == "production"

    def test_missing_value_key_defaults_empty(self, manager):
        """_parse_config() defaults to empty string if Value key is missing."""
        data = [
            {"Section": "app", "Key": "empty_val"},
        ]
        result = manager._parse_config(data)

        assert result["app"]["empty_val"] == ""


# ── _convert_value() ─────────────────────────────────────────────────


class TestConvertValue:
    # -- Booleans: true variants --
    @pytest.mark.parametrize("input_val", ["true", "True", "TRUE", "tRuE"])
    def test_true_variants(self, manager, input_val):
        assert manager._convert_value(input_val) is True

    @pytest.mark.parametrize("input_val", ["yes", "Yes", "YES"])
    def test_yes_variants(self, manager, input_val):
        assert manager._convert_value(input_val) is True

    def test_one_string_is_true(self, manager):
        assert manager._convert_value("1") is True

    # -- Booleans: false variants --
    @pytest.mark.parametrize("input_val", ["false", "False", "FALSE", "fAlSe"])
    def test_false_variants(self, manager, input_val):
        assert manager._convert_value(input_val) is False

    @pytest.mark.parametrize("input_val", ["no", "No", "NO"])
    def test_no_variants(self, manager, input_val):
        assert manager._convert_value(input_val) is False

    def test_zero_string_is_false(self, manager):
        assert manager._convert_value("0") is False

    # -- Integers --
    def test_positive_integer(self, manager):
        assert manager._convert_value("42") == 42
        assert isinstance(manager._convert_value("42"), int)

    def test_negative_integer(self, manager):
        assert manager._convert_value("-10") == -10
        assert isinstance(manager._convert_value("-10"), int)

    def test_large_integer(self, manager):
        assert manager._convert_value("1000000") == 1000000

    # -- Floats --
    def test_positive_float(self, manager):
        assert manager._convert_value("3.14") == 3.14
        assert isinstance(manager._convert_value("3.14"), float)

    def test_negative_float(self, manager):
        assert manager._convert_value("-0.5") == -0.5

    def test_float_with_no_integer_part(self, manager):
        assert manager._convert_value(".25") == 0.25

    def test_scientific_notation(self, manager):
        result = manager._convert_value("1e5")
        assert result == 100000.0

    # -- Strings (no conversion) --
    def test_plain_string(self, manager):
        assert manager._convert_value("hello world") == "hello world"

    def test_empty_string(self, manager):
        assert manager._convert_value("") == ""

    def test_string_with_spaces(self, manager):
        assert manager._convert_value("  some text  ") == "  some text  "

    def test_url_string(self, manager):
        url = "https://example.com/api"
        assert manager._convert_value(url) == url

    def test_mixed_alpha_numeric(self, manager):
        assert manager._convert_value("abc123") == "abc123"

    # -- Non-string passthrough --
    def test_non_string_int(self, manager):
        assert manager._convert_value(42) == 42

    def test_non_string_float(self, manager):
        assert manager._convert_value(3.14) == 3.14

    def test_non_string_bool(self, manager):
        assert manager._convert_value(True) is True

    def test_non_string_none(self, manager):
        assert manager._convert_value(None) is None

    def test_non_string_list(self, manager):
        assert manager._convert_value([1, 2, 3]) == [1, 2, 3]


# ── _apply_env_overrides() ───────────────────────────────────────────


class TestApplyEnvOverrides:
    def test_overrides_existing_value(self, manager):
        """Environment variable CCEF_SECTION_KEY overrides config value."""
        config = {"airtable": {"base_id": "original"}}
        with patch.dict("os.environ", {"CCEF_AIRTABLE_BASE_ID": "env_override"}):
            result = manager._apply_env_overrides(config)

        assert result["airtable"]["base_id"] == "env_override"

    def test_overrides_with_type_conversion(self, manager):
        """Environment variable values are converted via _convert_value."""
        config = {"openai": {"max_tokens": 100}}
        with patch.dict("os.environ", {"CCEF_OPENAI_MAX_TOKENS": "true"}):
            result = manager._apply_env_overrides(config)

        assert result["openai"]["max_tokens"] is True

    def test_overrides_numeric_string(self, manager):
        """Environment variable numeric strings are converted to numbers."""
        config = {"openai": {"temperature": 0.7}}
        with patch.dict("os.environ", {"CCEF_OPENAI_TEMPERATURE": "0.9"}):
            result = manager._apply_env_overrides(config)

        assert result["openai"]["temperature"] == 0.9

    def test_no_override_when_env_missing(self, manager):
        """Config values are preserved when no matching env var exists."""
        config = {"airtable": {"base_id": "appXXX"}}
        with patch.dict("os.environ", {}, clear=True):
            result = manager._apply_env_overrides(config)

        assert result["airtable"]["base_id"] == "appXXX"

    def test_uppercase_conversion_in_env_var_name(self, manager):
        """Env var name uses uppercased section and key."""
        config = {"mySection": {"myKey": "original"}}
        with patch.dict("os.environ", {"CCEF_MYSECTION_MYKEY": "overridden"}):
            result = manager._apply_env_overrides(config)

        assert result["mySection"]["myKey"] == "overridden"

    def test_multiple_overrides(self, manager):
        """Multiple environment variables can override multiple values."""
        config = {
            "airtable": {"base_id": "old_base", "table_name": "old_table"},
        }
        env = {
            "CCEF_AIRTABLE_BASE_ID": "new_base",
            "CCEF_AIRTABLE_TABLE_NAME": "new_table",
        }
        with patch.dict("os.environ", env):
            result = manager._apply_env_overrides(config)

        assert result["airtable"]["base_id"] == "new_base"
        assert result["airtable"]["table_name"] == "new_table"

    def test_override_across_sections(self, manager):
        """Environment overrides work across different sections."""
        config = {
            "airtable": {"base_id": "old"},
            "openai": {"model": "gpt-3.5"},
        }
        env = {
            "CCEF_AIRTABLE_BASE_ID": "new_base",
            "CCEF_OPENAI_MODEL": "gpt-4o",
        }
        with patch.dict("os.environ", env):
            result = manager._apply_env_overrides(config)

        assert result["airtable"]["base_id"] == "new_base"
        assert result["openai"]["model"] == "gpt-4o"

    def test_empty_config_no_overrides(self, manager):
        """_apply_env_overrides with empty config returns empty dict."""
        config = {}
        with patch.dict("os.environ", {"CCEF_SECTION_KEY": "val"}):
            result = manager._apply_env_overrides(config)

        assert result == {}

    def test_env_override_integrated_in_refresh(self, auto_manager, mock_sheets_connector):
        """Environment overrides are applied during refresh()."""
        with patch.dict("os.environ", {"CCEF_AIRTABLE_BASE_ID": "env_base_override"}):
            auto_manager.refresh()

        assert auto_manager._config_cache["airtable"]["base_id"] == "env_base_override"


# ── cache_age property ───────────────────────────────────────────────


class TestCacheAge:
    def test_cache_age_zero_when_no_cache(self, manager):
        """cache_age returns 0 when no cache has been loaded."""
        assert manager.cache_age == 0.0

    def test_cache_age_increases_over_time(self, loaded_manager):
        """cache_age returns a positive value after cache is populated."""
        # The loaded_manager fixture sets _cache_timestamp to time.time()
        # so cache_age should be very small but >= 0
        assert loaded_manager.cache_age >= 0.0

    def test_cache_age_reflects_elapsed_time(self, manager):
        """cache_age reflects the time elapsed since cache was set."""
        manager._cache_timestamp = time.time() - 60  # 60 seconds ago

        age = manager.cache_age
        assert 59.0 <= age <= 62.0  # small tolerance for test execution

    def test_cache_age_after_clear(self, loaded_manager):
        """cache_age returns 0 after cache is cleared."""
        loaded_manager.clear_cache()
        assert loaded_manager.cache_age == 0.0


# ── is_cache_valid property ──────────────────────────────────────────


class TestIsCacheValid:
    def test_invalid_when_no_cache(self, manager):
        """is_cache_valid returns False when there is no cache."""
        assert manager.is_cache_valid is False

    def test_valid_when_cache_is_fresh(self, loaded_manager):
        """is_cache_valid returns True when cache is within TTL."""
        assert loaded_manager.is_cache_valid is True

    def test_invalid_when_cache_expired(self, loaded_manager):
        """is_cache_valid returns False when cache timestamp is beyond TTL."""
        loaded_manager._cache_timestamp = time.time() - 999
        assert loaded_manager.is_cache_valid is False

    def test_valid_just_before_expiry(self, manager):
        """is_cache_valid returns True when cache is just under TTL."""
        manager._config_cache = {"section": {"key": "value"}}
        manager._cache_timestamp = time.time() - (manager._ttl - 1)
        assert manager.is_cache_valid is True

    def test_invalid_at_exact_expiry(self, manager):
        """is_cache_valid returns False when cache age exactly equals TTL."""
        manager._config_cache = {"section": {"key": "value"}}
        manager._cache_timestamp = time.time() - manager._ttl
        assert manager.is_cache_valid is False

    def test_invalid_after_clear(self, loaded_manager):
        """is_cache_valid returns False after clear_cache()."""
        loaded_manager.clear_cache()
        assert loaded_manager.is_cache_valid is False


# ── _is_cache_expired() ─────────────────────────────────────────────


class TestIsCacheExpired:
    def test_not_expired_within_ttl(self, manager):
        """Cache is not expired when age is less than TTL."""
        manager._cache_timestamp = 1000.0
        assert manager._is_cache_expired(1100.0) is False  # 100s < 300s TTL

    def test_expired_at_ttl_boundary(self, manager):
        """Cache is expired when age exactly equals TTL."""
        manager._cache_timestamp = 1000.0
        assert manager._is_cache_expired(1300.0) is True  # 300s == 300s TTL

    def test_expired_beyond_ttl(self, manager):
        """Cache is expired when age exceeds TTL."""
        manager._cache_timestamp = 1000.0
        assert manager._is_cache_expired(2000.0) is True  # 1000s > 300s TTL

    def test_not_expired_at_zero_age(self, manager):
        """Cache is not expired when current_time equals cache_timestamp."""
        manager._cache_timestamp = 1000.0
        assert manager._is_cache_expired(1000.0) is False

    def test_respects_custom_ttl(self):
        """_is_cache_expired() respects the custom TTL value."""
        mgr = ConfigManager(sheets_id="id", ttl=10)
        mgr._cache_timestamp = 1000.0

        assert mgr._is_cache_expired(1009.0) is False  # 9s < 10s
        assert mgr._is_cache_expired(1010.0) is True   # 10s == 10s
        assert mgr._is_cache_expired(1011.0) is True   # 11s > 10s


# ── TTL Expiration Integration ───────────────────────────────────────


class TestTTLExpiration:
    def test_short_ttl_forces_refresh(self, mock_sheets_connector):
        """A short TTL causes get_config to trigger refresh quickly."""
        mgr = ConfigManager(sheets_id="test-id", ttl=1, auto_refresh=True)
        mgr._sheets_connector = mock_sheets_connector

        # First call populates cache
        mgr.refresh()
        first_result = mgr.get_config()
        assert first_result is not None

        # Simulate time passing beyond TTL
        mgr._cache_timestamp = time.time() - 2

        # Next get_config should trigger another refresh
        mgr.get_config()
        assert mock_sheets_connector.get_worksheet_as_dicts.call_count == 2

    def test_large_ttl_keeps_cache(self, mock_sheets_connector):
        """A large TTL prevents unnecessary refreshes."""
        mgr = ConfigManager(sheets_id="test-id", ttl=86400, auto_refresh=True)
        mgr._sheets_connector = mock_sheets_connector

        mgr.refresh()
        # Multiple get_config calls should not trigger additional refreshes
        for _ in range(10):
            mgr.get_config()

        assert mock_sheets_connector.get_worksheet_as_dicts.call_count == 1

    def test_zero_ttl_always_refreshes(self, mock_sheets_connector):
        """A TTL of 0 causes every get_config call to refresh."""
        mgr = ConfigManager(sheets_id="test-id", ttl=0, auto_refresh=True)
        mgr._sheets_connector = mock_sheets_connector

        mgr.get_config()
        mgr.get_config()
        mgr.get_config()

        assert mock_sheets_connector.get_worksheet_as_dicts.call_count == 3
