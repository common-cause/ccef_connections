"""
Credential management for CCEF connections.

This module handles loading credentials from environment variables using the
{CREDENTIAL_NAME}_PASSWORD naming convention required for Civis compatibility.
Supports both local development (.env files) and Civis production environments.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from ..exceptions import CredentialError

logger = logging.getLogger(__name__)


class CredentialManager:
    """
    Manages credentials for CCEF connections.

    Supports the {CREDENTIAL_NAME}_PASSWORD naming convention for Civis
    compatibility while also working seamlessly in local development
    environments with .env files.
    """

    _instance: Optional["CredentialManager"] = None
    _credentials_cache: Dict[str, Any] = {}
    _env_loaded: bool = False

    def __new__(cls) -> "CredentialManager":
        """Implement singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialize the credential manager."""
        if not self._env_loaded:
            # Load .env file if it exists (local development)
            load_dotenv()
            self._env_loaded = True
            logger.debug("Environment variables loaded")

    def get_credential(
        self, name: str, required: bool = True, is_json: bool = False
    ) -> Optional[Any]:
        """
        Get a credential by name using the {NAME}_PASSWORD pattern.

        Args:
            name: The credential name (e.g., 'AIRTABLE_API_KEY')
            required: Whether the credential is required (raises error if missing)
            is_json: Whether to parse the credential as JSON

        Returns:
            The credential value (string or parsed JSON dict)

        Raises:
            CredentialError: If a required credential is missing or invalid

        Examples:
            >>> manager = CredentialManager()
            >>> api_key = manager.get_credential('AIRTABLE_API_KEY')
            >>> gcp_creds = manager.get_credential('BIGQUERY_CREDENTIALS', is_json=True)
        """
        # Check cache first
        if name in self._credentials_cache:
            logger.debug(f"Retrieved credential from cache: {name}")
            return self._credentials_cache[name]

        # Construct the environment variable name
        env_var_name = f"{name}_PASSWORD"

        # Get the credential from environment
        credential = os.getenv(env_var_name)

        if credential is None:
            if required:
                raise CredentialError(
                    f"Required credential not found: {env_var_name}\n"
                    f"Please set the environment variable {env_var_name} "
                    f"or add it to your .env file."
                )
            logger.debug(f"Optional credential not found: {name}")
            return None

        # Parse JSON if requested
        if is_json:
            try:
                credential = json.loads(credential)
                logger.debug(f"Parsed JSON credential: {name}")
            except json.JSONDecodeError as e:
                raise CredentialError(
                    f"Failed to parse JSON credential {env_var_name}: {str(e)}\n"
                    f"Ensure the credential is valid JSON."
                )

        # Cache the credential
        self._credentials_cache[name] = credential
        logger.debug(f"Loaded and cached credential: {name}")

        return credential

    def get_airtable_key(self) -> str:
        """
        Get the Airtable API key.

        Returns:
            The Airtable API key

        Raises:
            CredentialError: If the credential is missing
        """
        return str(self.get_credential("AIRTABLE_API_KEY"))

    def get_openai_key(self) -> str:
        """
        Get the OpenAI API key.

        Returns:
            The OpenAI API key

        Raises:
            CredentialError: If the credential is missing
        """
        return str(self.get_credential("OPENAI_API_KEY"))

    def get_google_sheets_credentials(self) -> Dict[str, Any]:
        """
        Get Google Sheets service account credentials.

        Returns:
            The parsed service account JSON credentials

        Raises:
            CredentialError: If the credential is missing or invalid JSON
        """
        creds = self.get_credential("GOOGLE_SHEETS_CREDENTIALS", is_json=True)
        if not isinstance(creds, dict):
            raise CredentialError(
                "GOOGLE_SHEETS_CREDENTIALS_PASSWORD must be a valid JSON object"
            )
        return creds

    def get_bigquery_credentials(self) -> Dict[str, Any]:
        """
        Get BigQuery service account credentials.

        Returns:
            The parsed service account JSON credentials

        Raises:
            CredentialError: If the credential is missing or invalid JSON
        """
        creds = self.get_credential("BIGQUERY_CREDENTIALS", is_json=True)
        if not isinstance(creds, dict):
            raise CredentialError("BIGQUERY_CREDENTIALS_PASSWORD must be a valid JSON object")
        return creds

    def get_helpscout_credentials(self) -> Dict[str, Any]:
        """
        Get HelpScout OAuth2 client credentials.

        Returns:
            Dict with 'app_id' and 'app_secret' keys

        Raises:
            CredentialError: If the credential is missing, invalid JSON,
                or missing required keys
        """
        creds = self.get_credential("HELPSCOUT_CREDENTIALS", is_json=True)
        if not isinstance(creds, dict):
            raise CredentialError(
                "HELPSCOUT_CREDENTIALS_PASSWORD must be a valid JSON object"
            )
        missing = [k for k in ("app_id", "app_secret") if k not in creds]
        if missing:
            raise CredentialError(
                f"HELPSCOUT_CREDENTIALS_PASSWORD missing required keys: {', '.join(missing)}"
            )
        return creds

    def get_action_network_key(self) -> str:
        """
        Get the Action Network API key.

        Returns:
            The Action Network API key

        Raises:
            CredentialError: If the credential is missing
        """
        return str(self.get_credential("ACTION_NETWORK_API_KEY"))

    def get_zoom_credentials(self) -> Dict[str, Any]:
        """
        Get Zoom Server-to-Server OAuth credentials.

        Returns:
            Dict with 'account_id', 'client_id', and 'client_secret' keys

        Raises:
            CredentialError: If the credential is missing, invalid JSON,
                or missing required keys
        """
        creds = self.get_credential("ZOOM_CREDENTIALS", is_json=True)
        if not isinstance(creds, dict):
            raise CredentialError(
                "ZOOM_CREDENTIALS_PASSWORD must be a valid JSON object"
            )
        missing = [
            k for k in ("account_id", "client_id", "client_secret") if k not in creds
        ]
        if missing:
            raise CredentialError(
                f"ZOOM_CREDENTIALS_PASSWORD missing required keys: {', '.join(missing)}"
            )
        return creds

    def clear_cache(self) -> None:
        """Clear the credentials cache. Useful for testing or credential rotation."""
        self._credentials_cache.clear()
        logger.debug("Credentials cache cleared")

    def has_credential(self, name: str) -> bool:
        """
        Check if a credential exists without raising an error.

        Args:
            name: The credential name to check

        Returns:
            True if the credential exists, False otherwise
        """
        try:
            self.get_credential(name, required=False)
            return True
        except CredentialError:
            return False


# Global singleton instance
_credential_manager = CredentialManager()


def get_credential(name: str, required: bool = True, is_json: bool = False) -> Optional[Any]:
    """
    Convenience function to get a credential using the global manager.

    Args:
        name: The credential name (e.g., 'AIRTABLE_API_KEY')
        required: Whether the credential is required
        is_json: Whether to parse as JSON

    Returns:
        The credential value

    Examples:
        >>> from ccef_connections.core.credentials import get_credential
        >>> api_key = get_credential('AIRTABLE_API_KEY')
    """
    return _credential_manager.get_credential(name, required, is_json)
