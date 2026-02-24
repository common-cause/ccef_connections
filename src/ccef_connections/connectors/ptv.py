"""
Protect the Vote (PTV) connector for CCEF connections library.

Provides read access to shift volunteer signup data, volunteer user records,
and shift availability data from the PTV API. All three endpoints return
CSV data per state, accessible via HTTP Basic Auth combined with an API
key query parameter.

Endpoints:
    - shift_volunteers_csv: Volunteer signups per shift
    - users_csv:            All registered volunteers
    - state_shifts_csv:     All shifts (availability, fill rates)
"""

import csv
import io
import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_ptv_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

PTV_API_BASE = "https://app.protectthevote.net/api"
PTV_DEFAULT_USERNAME = "colab"

_ENDPOINT_SHIFT_VOLUNTEERS = f"{PTV_API_BASE}/shift_volunteers_csv"
_ENDPOINT_USERS = f"{PTV_API_BASE}/users_csv"
_ENDPOINT_STATE_SHIFTS = f"{PTV_API_BASE}/state_shifts_csv"

# When a state has no data the API returns this JSON string instead of CSV.
_NOT_FOUND_MARKER = '"errors"'


class PTVConnector(BaseConnection):
    """
    Protect the Vote (PTV) connector for reading shift and volunteer data.

    Provides access to three PTV API endpoints, all returning per-state CSV:

    * ``get_shift_volunteers`` / ``get_all_shift_volunteers`` —
      volunteer signups attached to specific shifts
    * ``get_users`` / ``get_all_users`` —
      all registered volunteers and their attributes
    * ``get_state_shifts`` / ``get_all_state_shifts`` —
      all shifts with volunteer counts and fill rates

    Credentials:
        Reads ``PTV_API_KEY_PASSWORD`` from the environment (or .env file).

    Examples:
        >>> connector = PTVConnector()
        >>> connector.connect()
        >>> signups = connector.get_shift_volunteers("PA")
        >>> volunteers = connector.get_all_users(["PA", "GA", "AZ"])
        >>> shifts = connector.get_all_state_shifts(["PA", "GA"])
    """

    def __init__(self, username: str = PTV_DEFAULT_USERNAME) -> None:
        """
        Initialize the PTV connector.

        Args:
            username: HTTP Basic Auth username. Defaults to 'colab', which
                is the standard username for PTV API access.
        """
        super().__init__()
        self._api_key: Optional[str] = None
        self._username: str = username

    def connect(self) -> None:
        """
        Load the PTV API key from credentials.

        Raises:
            CredentialError: If PTV_API_KEY_PASSWORD is not set
            ConnectionError: If credential loading fails unexpectedly
        """
        try:
            self._api_key = self._credential_manager.get_ptv_api_key()
            self._is_connected = True
            logger.info("Successfully connected to PTV")
        except Exception as e:
            logger.error(f"Failed to connect to PTV: {str(e)}")
            raise ConnectionError(f"Failed to connect to PTV: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the PTV connection state."""
        self._api_key = None
        self._is_connected = False
        logger.debug("Disconnected from PTV")

    def health_check(self) -> bool:
        """
        Check if the connector is ready to make API requests.

        Returns:
            True if connected and API key is loaded, False otherwise.
        """
        return self._is_connected and self._api_key is not None

    # -- HTTP helpers ----------------------------------------------------------

    def _fetch_csv(self, endpoint: str, state_code: str) -> str:
        """
        Fetch raw CSV text from a PTV endpoint for a given state.

        When the API has no data for a state it returns a JSON error body
        instead of CSV. This method detects that and returns an empty string
        so callers always receive parseable (possibly empty) CSV content.

        Args:
            endpoint: Full PTV endpoint URL
            state_code: Two-letter state code (e.g. 'PA')

        Returns:
            Raw CSV text, or empty string when the state has no data

        Raises:
            AuthenticationError: On 401 responses
            RateLimitError: On 429 responses
            ConnectionError: On other HTTP errors or network failures
        """
        if not self._is_connected:
            self.connect()

        params = {"key": self._api_key, "state_code": state_code}

        try:
            resp = requests.get(
                endpoint,
                params=params,
                auth=(self._username, self._api_key),
                timeout=60,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"PTV API request failed: {e}") from e

        if resp.status_code == 401:
            raise AuthenticationError(
                f"PTV authentication failed ({resp.status_code}): {resp.text}"
            )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            raise RateLimitError(
                f"PTV rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code >= 400:
            raise ConnectionError(
                f"PTV API error {resp.status_code}: {resp.text}"
            )

        # When a state has no data the API returns a JSON error body
        # (e.g. {"errors":{"detail":"Not Found"}}) instead of CSV.
        if _NOT_FOUND_MARKER in resp.text:
            logger.debug(f"No data for state {state_code} at {endpoint}")
            return ""

        return resp.text

    def _parse_csv(self, csv_text: str) -> List[Dict[str, Any]]:
        """Parse a CSV string into a list of dicts. Returns [] for empty input."""
        if not csv_text.strip():
            return []
        reader = csv.DictReader(io.StringIO(csv_text))
        return list(reader)

    # -- Shift volunteers (shift_volunteers_csv) --------------------------------

    @retry_ptv_operation
    def get_shift_volunteers(self, state_code: str) -> List[Dict[str, Any]]:
        """
        Fetch volunteer signups for a single state.

        Args:
            state_code: Two-letter state code (e.g. 'PA')

        Returns:
            List of dicts with keys: shift_id, inserted_at, date, start_time,
            end_time, timezone, locations, county, first_name, last_name,
            phone_number, email, role, source.
        """
        csv_text = self._fetch_csv(_ENDPOINT_SHIFT_VOLUNTEERS, state_code)
        rows = self._parse_csv(csv_text)
        logger.info(f"[shift_volunteers] {state_code}: {len(rows)} rows")
        return rows

    def get_all_shift_volunteers(
        self, state_codes: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch volunteer signups for multiple states.

        A 'state' key is added to each row with the two-letter state code.

        Args:
            state_codes: List of two-letter state codes

        Returns:
            Combined list of signup dicts across all states.
        """
        return self._collect_all(
            state_codes, self.get_shift_volunteers, label="shift_volunteers"
        )

    # -- All users (users_csv) -------------------------------------------------

    @retry_ptv_operation
    def get_users(self, state_code: str) -> List[Dict[str, Any]]:
        """
        Fetch all registered volunteers for a single state.

        Args:
            state_code: Two-letter state code (e.g. 'PA')

        Returns:
            List of dicts with keys: id, email, join_date, phone_number,
            first_name, last_name, county, zip_code, source_code,
            regional_admin, shifted, training, role.
        """
        csv_text = self._fetch_csv(_ENDPOINT_USERS, state_code)
        rows = self._parse_csv(csv_text)
        logger.info(f"[users] {state_code}: {len(rows)} rows")
        return rows

    def get_all_users(self, state_codes: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch all registered volunteers for multiple states.

        A 'state' key is added to each row with the two-letter state code.

        Args:
            state_codes: List of two-letter state codes

        Returns:
            Combined list of volunteer dicts across all states.
        """
        return self._collect_all(state_codes, self.get_users, label="users")

    # -- State shifts (state_shifts_csv) ---------------------------------------

    @retry_ptv_operation
    def get_state_shifts(self, state_code: str) -> List[Dict[str, Any]]:
        """
        Fetch all shifts (with fill rates) for a single state.

        Args:
            state_code: Two-letter state code (e.g. 'PA')

        Returns:
            List of dicts with keys: id, date, start_time, end_time,
            locations_string, volunteers, filled.
        """
        csv_text = self._fetch_csv(_ENDPOINT_STATE_SHIFTS, state_code)
        rows = self._parse_csv(csv_text)
        logger.info(f"[state_shifts] {state_code}: {len(rows)} rows")
        return rows

    def get_all_state_shifts(
        self, state_codes: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Fetch all shifts (with fill rates) for multiple states.

        A 'state' key is added to each row with the two-letter state code.

        Args:
            state_codes: List of two-letter state codes

        Returns:
            Combined list of shift dicts across all states.
        """
        return self._collect_all(
            state_codes, self.get_state_shifts, label="state_shifts"
        )

    # -- Internal helpers ------------------------------------------------------

    def _collect_all(
        self,
        state_codes: List[str],
        fetch_fn: Any,
        label: str,
    ) -> List[Dict[str, Any]]:
        """
        Loop over states, call fetch_fn for each, add 'state' key, combine.

        Args:
            state_codes: List of two-letter state codes
            fetch_fn: Single-state fetch method to call
            label: Log label for progress messages

        Returns:
            Combined list of dicts with 'state' key added to each row.
        """
        all_rows: List[Dict[str, Any]] = []
        total = len(state_codes)
        for i, state_code in enumerate(state_codes, start=1):
            rows = fetch_fn(state_code)
            for row in rows:
                row["state"] = state_code
            all_rows.extend(rows)
            logger.info(
                f"[{label}] {i}/{total} states done — "
                f"{len(rows)} rows from {state_code}, "
                f"{len(all_rows)} total"
            )
        return all_rows
