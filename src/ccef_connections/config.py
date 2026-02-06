"""
Configuration management for CCEF connections.

This module provides the ConfigManager class for reading configuration
from Google Sheets with caching and environment variable overrides.
"""

import logging
import os
import time
from typing import Any, Dict, Optional

from .connectors.sheets import SheetsConnector
from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Configuration manager that reads from Google Sheets.

    This class provides a convenient way to manage application configuration
    using Google Sheets as the source of truth, with caching and environment
    variable overrides.

    The expected Google Sheets structure is:
    | Section    | Key        | Value        | Description          |
    |------------|------------|--------------|----------------------|
    | airtable   | base_id    | appXXX       | Airtable base ID     |
    | airtable   | table_name | Test Input   | Table name           |
    | ...        | ...        | ...          | ...                  |

    Examples:
        >>> config_mgr = ConfigManager(sheets_id='YOUR_SPREADSHEET_ID')
        >>> config = config_mgr.get_config()
        >>> base_id = config['airtable']['base_id']
        >>>
        >>> # Refresh config from Sheets
        >>> config_mgr.refresh()
        >>> config = config_mgr.get_config()
    """

    def __init__(
        self,
        sheets_id: str,
        worksheet_name: str = "Config",
        ttl: int = 300,
        auto_refresh: bool = True,
    ) -> None:
        """
        Initialize the ConfigManager.

        Args:
            sheets_id: Google Sheets spreadsheet ID
            worksheet_name: Name of the worksheet containing config (default: "Config")
            ttl: Time-to-live for cache in seconds (default: 300 = 5 minutes)
            auto_refresh: Whether to auto-refresh when cache expires (default: True)
        """
        self._sheets_id = sheets_id
        self._worksheet_name = worksheet_name
        self._ttl = ttl
        self._auto_refresh = auto_refresh

        self._config_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self._cache_timestamp: float = 0.0

        self._sheets_connector: Optional[SheetsConnector] = None

        logger.debug(f"Initialized ConfigManager (TTL: {ttl}s)")

    def get_config(self, refresh_if_expired: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Get the configuration.

        Returns a nested dictionary with sections as top-level keys.

        Args:
            refresh_if_expired: Whether to auto-refresh if cache expired (default: True)

        Returns:
            Nested dictionary of configuration values

        Examples:
            >>> config = config_mgr.get_config()
            >>> print(config['airtable']['base_id'])
            >>> print(config['openai']['model'])
        """
        current_time = time.time()

        # Check if cache is valid
        if self._config_cache is not None and not self._is_cache_expired(current_time):
            logger.debug("Returning cached configuration")
            return self._config_cache

        # Cache is invalid or expired
        if self._auto_refresh and refresh_if_expired:
            logger.debug("Cache expired or empty, refreshing configuration")
            self.refresh()
            if self._config_cache is None:
                raise ConfigurationError("Failed to load configuration")
            return self._config_cache

        # Return cached config even if expired (if auto_refresh is disabled)
        if self._config_cache is not None:
            logger.warning("Returning expired cached configuration")
            return self._config_cache

        raise ConfigurationError(
            "No configuration available. Call refresh() to load from Google Sheets."
        )

    def refresh(self) -> None:
        """
        Refresh configuration from Google Sheets.

        This will fetch the latest configuration from the spreadsheet
        and update the cache.

        Raises:
            ConfigurationError: If configuration cannot be loaded
        """
        try:
            logger.info("Refreshing configuration from Google Sheets")

            # Initialize connector if needed
            if self._sheets_connector is None:
                self._sheets_connector = SheetsConnector()

            # Read configuration from Sheets
            config_data = self._sheets_connector.get_worksheet_as_dicts(
                self._sheets_id, self._worksheet_name
            )

            # Convert to nested dictionary structure
            config = self._parse_config(config_data)

            # Apply environment variable overrides
            config = self._apply_env_overrides(config)

            # Update cache
            self._config_cache = config
            self._cache_timestamp = time.time()

            logger.info(f"Configuration refreshed successfully ({len(config)} sections)")

        except Exception as e:
            logger.error(f"Failed to refresh configuration: {str(e)}")
            raise ConfigurationError(f"Failed to refresh configuration: {str(e)}") from e

    def get(
        self, section: str, key: str, default: Any = None
    ) -> Any:
        """
        Get a specific configuration value.

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value or default

        Examples:
            >>> base_id = config_mgr.get('airtable', 'base_id')
            >>> model = config_mgr.get('openai', 'model', default='gpt-4o')
        """
        config = self.get_config()
        return config.get(section, {}).get(key, default)

    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._config_cache = None
        self._cache_timestamp = 0.0
        logger.debug("Configuration cache cleared")

    def _is_cache_expired(self, current_time: float) -> bool:
        """
        Check if the cache has expired.

        Args:
            current_time: Current timestamp

        Returns:
            True if cache is expired, False otherwise
        """
        age = current_time - self._cache_timestamp
        return age >= self._ttl

    def _parse_config(
        self, config_data: list[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse configuration data into nested dictionary.

        Expected format:
        [{'Section': 'airtable', 'Key': 'base_id', 'Value': 'appXXX', ...}, ...]

        Args:
            config_data: List of configuration rows as dictionaries

        Returns:
            Nested dictionary with sections and keys
        """
        config: Dict[str, Dict[str, Any]] = {}

        for row in config_data:
            section = row.get("Section", "").strip()
            key = row.get("Key", "").strip()
            value = row.get("Value", "")

            if not section or not key:
                logger.warning(f"Skipping invalid config row: {row}")
                continue

            # Create section if it doesn't exist
            if section not in config:
                config[section] = {}

            # Store the value
            config[section][key] = self._convert_value(value)

        return config

    def _convert_value(self, value: Any) -> Any:
        """
        Convert string values to appropriate types.

        Attempts to convert to int, float, or boolean.
        Returns original string if conversion fails.

        Args:
            value: Value to convert

        Returns:
            Converted value
        """
        if not isinstance(value, str):
            return value

        # Try boolean
        value_lower = value.lower()
        if value_lower in ("true", "yes", "1"):
            return True
        if value_lower in ("false", "no", "0"):
            return False

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Return as string
        return value

    def _apply_env_overrides(
        self, config: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Apply environment variable overrides.

        Environment variables in format: CCEF_SECTION_KEY override
        corresponding config values.

        Args:
            config: Configuration dictionary

        Returns:
            Configuration with environment overrides applied
        """
        for section in config:
            for key in config[section]:
                env_var = f"CCEF_{section.upper()}_{key.upper()}"
                env_value = os.getenv(env_var)

                if env_value is not None:
                    config[section][key] = self._convert_value(env_value)
                    logger.debug(f"Applied env override: {env_var}")

        return config

    @property
    def cache_age(self) -> float:
        """
        Get the age of the current cache in seconds.

        Returns:
            Cache age in seconds, or 0 if no cache
        """
        if self._cache_timestamp == 0:
            return 0.0
        return time.time() - self._cache_timestamp

    @property
    def is_cache_valid(self) -> bool:
        """
        Check if the cache is valid (not expired).

        Returns:
            True if cache is valid, False otherwise
        """
        if self._config_cache is None:
            return False
        return not self._is_cache_expired(time.time())
