"""
ROI CRM connector for CCEF connections library.

This module provides access to the ROI CRM API v1.0 for managing donors,
donations, pledges, memberships, and other fundraising data.

Uses OAuth2 Client Credentials via Auth0 for authentication, with direct
HTTP via the requests library.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_roi_crm_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

ROI_TOKEN_URL = "https://roisolutions.us.auth0.com/oauth/token"
ROI_API_BASE = "https://app.roicrm.net/api/1.0"


class ROICRMConnector(BaseConnection):
    """
    ROI CRM connector for donor and fundraising data management.

    Provides access to donors, donations, pledges, memberships, payment
    tokens, and other CRM objects using OAuth2 Client Credentials via Auth0.

    Credentials are stored as JSON in ROI_CRM_CREDENTIALS_PASSWORD env var:
    {
        "client_id": "...",
        "client_secret": "...",
        "audience": "...",
        "roi_client_code": "..."
    }

    Examples:
        >>> connector = ROICRMConnector()
        >>> connector.connect()
        >>> donor = connector.get_donor(12345)
        >>> donations = connector.list_donations(12345)
    """

    def __init__(self) -> None:
        """Initialize the ROI CRM connector."""
        super().__init__()
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def connect(self) -> None:
        """
        Establish connection to ROI CRM by obtaining an OAuth2 access token.

        Raises:
            CredentialError: If ROI CRM credentials are missing or invalid
            AuthenticationError: If OAuth token request fails
            ConnectionError: If connection setup fails
        """
        try:
            creds = self._credential_manager.get_roi_crm_credentials()
            self._fetch_token(creds)
            self._is_connected = True
            logger.info("Successfully connected to ROI CRM")
        except AuthenticationError:
            logger.error("Failed to connect to ROI CRM: authentication failed")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to ROI CRM: {str(e)}")
            raise ConnectionError(f"Failed to connect to ROI CRM: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the ROI CRM connection and token."""
        self._access_token = None
        self._token_expires_at = 0.0
        self._is_connected = False
        logger.debug("Disconnected from ROI CRM")

    def health_check(self) -> bool:
        """
        Check if the ROI CRM connection is healthy.

        Verifies the token is valid by calling GET /ping/.

        Returns:
            True if connected and API is reachable, False otherwise
        """
        if not self._is_connected or not self._access_token:
            return False
        try:
            self._request("GET", "/ping/")
            return True
        except Exception:
            return False

    # ── Token management ──────────────────────────────────────────────

    def _fetch_token(self, creds: Dict[str, str]) -> None:
        """
        Fetch an OAuth2 Client Credentials access token from Auth0.

        Args:
            creds: Dict with client_id, client_secret, audience, roi_client_code

        Raises:
            AuthenticationError: If token request fails
            ConnectionError: If the token endpoint is unreachable
        """
        try:
            resp = requests.post(
                ROI_TOKEN_URL,
                json={
                    "grant_type": "client_credentials",
                    "client_id": creds["client_id"],
                    "client_secret": creds["client_secret"],
                    "audience": creds["audience"],
                    "roi_client_code": creds["roi_client_code"],
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(
                f"Failed to reach ROI CRM token endpoint: {e}"
            ) from e

        if resp.status_code != 200:
            raise AuthenticationError(
                f"ROI CRM OAuth token request failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        # Token is valid for 24h; expire slightly early to avoid edge cases
        self._token_expires_at = time.time() + data.get("expires_in", 86400) - 60
        logger.debug("ROI CRM OAuth token obtained")

    def _refresh_token_if_needed(self) -> None:
        """Re-fetch the OAuth token if it has expired."""
        if time.time() >= self._token_expires_at:
            logger.debug("ROI CRM token expired, refreshing")
            creds = self._credential_manager.get_roi_crm_credentials()
            self._fetch_token(creds)

    def _get_headers(self) -> Dict[str, str]:
        """
        Return authorization headers, refreshing the token if needed.

        Returns:
            Dict with Authorization and Content-Type headers
        """
        self._refresh_token_if_needed()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    # ── HTTP helpers ──────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Central HTTP method with auth headers and auto-refresh on 401.

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            path: API path relative to /api/1.0 (e.g., '/donors/12345/')
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: If authentication fails after retry
            RateLimitError: If rate limited (429) by the API
            ConnectionError: If the request fails or returns an error status
        """
        if not self._is_connected and not self._access_token:
            self.connect()

        url = f"{ROI_API_BASE}{path}"

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
            raise ConnectionError(f"ROI CRM API request failed: {e}") from e

        # Auto-refresh on 401 and retry once
        if resp.status_code == 401:
            logger.debug("Received 401, refreshing token and retrying")
            self._token_expires_at = 0.0  # Force refresh
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
                raise ConnectionError(
                    f"ROI CRM API request failed on retry: {e}"
                ) from e

            if resp.status_code == 401:
                raise AuthenticationError(
                    "ROI CRM authentication failed after token refresh"
                )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 30))
            raise RateLimitError(
                f"ROI CRM rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"ROI CRM API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        per_page: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Generic pagination helper using page/per_page parameters.

        ROI CRM returns paginated responses with an 'items' array and
        'next'/'prev' links. Iterates through all pages and returns
        a combined list.

        Args:
            path: API path
            params: Query parameters for the first request
            per_page: Number of records per page

        Returns:
            Combined list of all items across pages
        """
        results: List[Dict[str, Any]] = []
        current_params = dict(params or {})
        current_params["per_page"] = per_page
        current_params.setdefault("page", 1)

        while True:
            data = self._request("GET", path, params=dict(current_params))
            if data is None:
                break

            items = data.get("items", [])
            results.extend(items)

            if not data.get("next"):
                break

            current_params["page"] = current_params["page"] + 1

        return results

    # ── System ────────────────────────────────────────────────────────

    @retry_roi_crm_operation
    def ping(self) -> Dict[str, Any]:
        """
        Ping the ROI CRM API to verify connectivity.

        Returns:
            Response dict from the ping endpoint

        Examples:
            >>> result = connector.ping()
        """
        result = self._request("GET", "/ping/")
        return result or {}

    @retry_roi_crm_operation
    def get_server_time(self) -> Dict[str, Any]:
        """
        Get the current server time from the ROI CRM API.

        Returns:
            Dict containing server time information

        Examples:
            >>> time_info = connector.get_server_time()
            >>> print(time_info["server_time"])
        """
        result = self._request("GET", "/server-time/")
        return result or {}

    # ── Donors ────────────────────────────────────────────────────────

    @retry_roi_crm_operation
    def search_donors(self, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        Search for donors using filter parameters.

        Args:
            **kwargs: Filter parameters such as first_name, last_name,
                email, phone, zip, etc.

        Returns:
            List of donor dicts matching the search criteria

        Examples:
            >>> donors = connector.search_donors(last_name="Smith", zip="20001")
            >>> donors = connector.search_donors(email="donor@example.com")
        """
        return self._paginate("/donors/", params=kwargs)

    @retry_roi_crm_operation
    def get_donor(self, donor_id: int) -> Dict[str, Any]:
        """
        Get a donor record by ID.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            Donor record dict

        Examples:
            >>> donor = connector.get_donor(12345)
            >>> print(donor["first_name"], donor["last_name"])
        """
        result = self._request("GET", f"/donors/{donor_id}/")
        return result or {}

    @retry_roi_crm_operation
    def create_donor(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a new donor record.

        Args:
            **kwargs: Donor field values (first_name, last_name, email, etc.)

        Returns:
            The newly created donor record dict

        Examples:
            >>> donor = connector.create_donor(
            ...     first_name="Jane", last_name="Doe", email="jane@example.com"
            ... )
        """
        result = self._request("POST", "/donors/", json_body=kwargs)
        return result or {}

    @retry_roi_crm_operation
    def update_donor(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Update an existing donor record.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Field values to update

        Returns:
            The updated donor record dict

        Examples:
            >>> donor = connector.update_donor(12345, email="newemail@example.com")
        """
        result = self._request("PATCH", f"/donors/{donor_id}/", json_body=kwargs)
        return result or {}

    @retry_roi_crm_operation
    def get_donor_flextable(self, donor_id: int, table_name: str) -> Dict[str, Any]:
        """
        Get a donor's flextable (custom field table) by name.

        Args:
            donor_id: The ROI CRM donor ID
            table_name: The flextable name

        Returns:
            Flextable data dict

        Examples:
            >>> flex = connector.get_donor_flextable(12345, "custom_fields")
        """
        result = self._request("GET", f"/donors/{donor_id}/flextables/{table_name}/")
        return result or {}

    # ── Donations ─────────────────────────────────────────────────────

    @retry_roi_crm_operation
    def get_donation_summary(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Get a summary of donations for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Optional filter parameters (e.g., start_date, end_date)

        Returns:
            Donation summary dict with totals and statistics

        Examples:
            >>> summary = connector.get_donation_summary(12345)
            >>> summary = connector.get_donation_summary(12345, start_date="2024-01-01")
        """
        result = self._request(
            "GET", f"/donors/{donor_id}/donations/summary/", params=kwargs or None
        )
        return result or {}

    @retry_roi_crm_operation
    def list_donations(self, donor_id: int, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        List all donations for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Optional filter parameters

        Returns:
            List of donation dicts

        Examples:
            >>> donations = connector.list_donations(12345)
        """
        return self._paginate(f"/donors/{donor_id}/donations/", params=kwargs or None)

    @retry_roi_crm_operation
    def get_donation(self, donor_id: int, txn_id: int) -> Dict[str, Any]:
        """
        Get a specific donation transaction.

        Args:
            donor_id: The ROI CRM donor ID
            txn_id: The transaction ID

        Returns:
            Donation transaction dict

        Examples:
            >>> donation = connector.get_donation(12345, 67890)
        """
        result = self._request("GET", f"/donors/{donor_id}/donations/{txn_id}/")
        return result or {}

    @retry_roi_crm_operation
    def create_donation(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a new donation for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Donation field values (amount, date, fund_code, etc.)

        Returns:
            The newly created donation dict

        Examples:
            >>> donation = connector.create_donation(
            ...     12345, amount=100.00, date="2024-01-15", fund_code="GEN"
            ... )
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/donations/", json_body=kwargs
        )
        return result or {}

    @retry_roi_crm_operation
    def add_donation_flag(
        self, donor_id: int, txn_id: int, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Add a flag to a specific donation transaction.

        Args:
            donor_id: The ROI CRM donor ID
            txn_id: The transaction ID
            **kwargs: Flag field values

        Returns:
            The created flag dict

        Examples:
            >>> flag = connector.add_donation_flag(12345, 67890, flag_code="MATCH")
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/donations/{txn_id}/flags/", json_body=kwargs
        )
        return result or {}

    @retry_roi_crm_operation
    def get_related_transactions(
        self, donor_id: int, txn_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all related transactions for a donation.

        Args:
            donor_id: The ROI CRM donor ID
            txn_id: The transaction ID

        Returns:
            List of related transaction dicts

        Examples:
            >>> related = connector.get_related_transactions(12345, 67890)
        """
        return self._paginate(f"/donors/{donor_id}/donations/{txn_id}/related/")

    @retry_roi_crm_operation
    def get_related_transaction(
        self, donor_id: int, txn_id: int, rel_id: int
    ) -> Dict[str, Any]:
        """
        Get a specific related transaction.

        Args:
            donor_id: The ROI CRM donor ID
            txn_id: The transaction ID
            rel_id: The related transaction ID

        Returns:
            Related transaction dict

        Examples:
            >>> rel = connector.get_related_transaction(12345, 67890, 11111)
        """
        result = self._request(
            "GET", f"/donors/{donor_id}/donations/{txn_id}/related/{rel_id}/"
        )
        return result or {}

    @retry_roi_crm_operation
    def get_honoree_transactions(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        Get donations where this donor is listed as an honoree.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of honoree transaction dicts

        Examples:
            >>> honoree_txns = connector.get_honoree_transactions(12345)
        """
        return self._paginate(f"/donors/{donor_id}/honoree-transactions/")

    # ── Pledges ───────────────────────────────────────────────────────

    @retry_roi_crm_operation
    def list_pledges(self, donor_id: int, **kwargs: Any) -> List[Dict[str, Any]]:
        """
        List all pledges for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Optional filter parameters

        Returns:
            List of pledge dicts

        Examples:
            >>> pledges = connector.list_pledges(12345)
        """
        return self._paginate(f"/donors/{donor_id}/pledges/", params=kwargs or None)

    @retry_roi_crm_operation
    def get_pledge(self, donor_id: int, pledge_id: int) -> Dict[str, Any]:
        """
        Get a specific pledge.

        Args:
            donor_id: The ROI CRM donor ID
            pledge_id: The pledge ID

        Returns:
            Pledge dict

        Examples:
            >>> pledge = connector.get_pledge(12345, 999)
        """
        result = self._request("GET", f"/donors/{donor_id}/pledges/{pledge_id}/")
        return result or {}

    @retry_roi_crm_operation
    def create_pledge(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a new pledge for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Pledge field values (amount, frequency, start_date, etc.)

        Returns:
            The newly created pledge dict

        Examples:
            >>> pledge = connector.create_pledge(
            ...     12345, amount=50.00, frequency="monthly", start_date="2024-02-01"
            ... )
        """
        result = self._request("POST", f"/donors/{donor_id}/pledges/", json_body=kwargs)
        return result or {}

    @retry_roi_crm_operation
    def update_pledge(
        self, donor_id: int, pledge_id: int, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Update an existing pledge.

        Args:
            donor_id: The ROI CRM donor ID
            pledge_id: The pledge ID
            **kwargs: Field values to update

        Returns:
            The updated pledge dict

        Examples:
            >>> pledge = connector.update_pledge(12345, 999, amount=75.00)
        """
        result = self._request(
            "PATCH", f"/donors/{donor_id}/pledges/{pledge_id}/", json_body=kwargs
        )
        return result or {}

    @retry_roi_crm_operation
    def add_pledge_flag(
        self, donor_id: int, pledge_id: int, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Add a flag to a specific pledge.

        Args:
            donor_id: The ROI CRM donor ID
            pledge_id: The pledge ID
            **kwargs: Flag field values

        Returns:
            The created flag dict

        Examples:
            >>> flag = connector.add_pledge_flag(12345, 999, flag_code="PAUSE")
        """
        result = self._request(
            "POST",
            f"/donors/{donor_id}/pledges/{pledge_id}/flags/",
            json_body=kwargs,
        )
        return result or {}

    # ── Payment Tokens ────────────────────────────────────────────────

    @retry_roi_crm_operation
    def list_payment_tokens(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all stored payment tokens for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of payment token dicts

        Examples:
            >>> tokens = connector.list_payment_tokens(12345)
        """
        return self._paginate(f"/donors/{donor_id}/payment-tokens/")

    @retry_roi_crm_operation
    def get_payment_token(self, donor_id: int, token_id: int) -> Dict[str, Any]:
        """
        Get a specific payment token.

        Args:
            donor_id: The ROI CRM donor ID
            token_id: The payment token ID

        Returns:
            Payment token dict

        Examples:
            >>> token = connector.get_payment_token(12345, 777)
        """
        result = self._request(
            "GET", f"/donors/{donor_id}/payment-tokens/{token_id}/"
        )
        return result or {}

    @retry_roi_crm_operation
    def create_payment_token(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Store a new payment token for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Payment token field values (token, type, last_four, etc.)

        Returns:
            The newly created payment token dict

        Examples:
            >>> token = connector.create_payment_token(
            ...     12345, token="tok_abc123", type="credit_card", last_four="4242"
            ... )
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/payment-tokens/", json_body=kwargs
        )
        return result or {}

    @retry_roi_crm_operation
    def update_payment_token(
        self, donor_id: int, token_id: int, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Update an existing payment token.

        Args:
            donor_id: The ROI CRM donor ID
            token_id: The payment token ID
            **kwargs: Field values to update

        Returns:
            The updated payment token dict

        Examples:
            >>> token = connector.update_payment_token(12345, 777, is_default=True)
        """
        result = self._request(
            "PATCH",
            f"/donors/{donor_id}/payment-tokens/{token_id}/",
            json_body=kwargs,
        )
        return result or {}

    # ── Contact Info ──────────────────────────────────────────────────

    @retry_roi_crm_operation
    def get_primary_address(self, donor_id: int) -> Dict[str, Any]:
        """
        Get the primary address for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            Address dict with street, city, state, zip, etc.

        Examples:
            >>> address = connector.get_primary_address(12345)
            >>> print(address["city"], address["state"])
        """
        result = self._request("GET", f"/donors/{donor_id}/primary-address/")
        return result or {}

    @retry_roi_crm_operation
    def list_other_addresses(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all non-primary addresses for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of address dicts

        Examples:
            >>> addresses = connector.list_other_addresses(12345)
        """
        return self._paginate(f"/donors/{donor_id}/addresses/")

    @retry_roi_crm_operation
    def list_emails(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all email addresses for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of email dicts

        Examples:
            >>> emails = connector.list_emails(12345)
            >>> for e in emails:
            ...     print(e["address"], e["is_primary"])
        """
        return self._paginate(f"/donors/{donor_id}/emails/")

    @retry_roi_crm_operation
    def list_phones(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all phone numbers for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of phone dicts

        Examples:
            >>> phones = connector.list_phones(12345)
            >>> for p in phones:
            ...     print(p["number"], p["type"])
        """
        return self._paginate(f"/donors/{donor_id}/phones/")

    # ── Comments & Flags ──────────────────────────────────────────────

    @retry_roi_crm_operation
    def list_comments(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all comments for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of comment dicts

        Examples:
            >>> comments = connector.list_comments(12345)
        """
        return self._paginate(f"/donors/{donor_id}/comments/")

    @retry_roi_crm_operation
    def add_comment(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Add a comment to a donor record.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Comment field values (text, date, etc.)

        Returns:
            The created comment dict

        Examples:
            >>> comment = connector.add_comment(12345, text="Donor called to update address.")
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/comments/", json_body=kwargs
        )
        return result or {}

    @retry_roi_crm_operation
    def get_comment(self, donor_id: int, comment_id: int) -> Dict[str, Any]:
        """
        Get a specific comment for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            comment_id: The comment ID

        Returns:
            Comment dict

        Examples:
            >>> comment = connector.get_comment(12345, 555)
        """
        result = self._request(
            "GET", f"/donors/{donor_id}/comments/{comment_id}/"
        )
        return result or {}

    @retry_roi_crm_operation
    def list_donor_flags(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all flags on a donor record.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of flag dicts

        Examples:
            >>> flags = connector.list_donor_flags(12345)
        """
        return self._paginate(f"/donors/{donor_id}/flags/")

    @retry_roi_crm_operation
    def add_donor_flag(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Add a flag to a donor record.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Flag field values (flag_code, date, etc.)

        Returns:
            The created flag dict

        Examples:
            >>> flag = connector.add_donor_flag(12345, flag_code="VIP")
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/flags/", json_body=kwargs
        )
        return result or {}

    # ── Memberships ───────────────────────────────────────────────────

    @retry_roi_crm_operation
    def list_memberships(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all memberships for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of membership dicts

        Examples:
            >>> memberships = connector.list_memberships(12345)
        """
        return self._paginate(f"/donors/{donor_id}/memberships/")

    @retry_roi_crm_operation
    def get_membership(self, donor_id: int, membership_id: int) -> Dict[str, Any]:
        """
        Get a specific membership for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            membership_id: The membership ID

        Returns:
            Membership dict

        Examples:
            >>> membership = connector.get_membership(12345, 888)
        """
        result = self._request(
            "GET", f"/donors/{donor_id}/memberships/{membership_id}/"
        )
        return result or {}

    @retry_roi_crm_operation
    def list_submemberships(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all sub-memberships for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of sub-membership dicts

        Examples:
            >>> subs = connector.list_submemberships(12345)
        """
        return self._paginate(f"/donors/{donor_id}/submemberships/")

    @retry_roi_crm_operation
    def get_mvault(self, donor_id: int) -> Dict[str, Any]:
        """
        Get the MVault membership record for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            MVault record dict

        Examples:
            >>> mvault = connector.get_mvault(12345)
        """
        result = self._request("GET", f"/donors/{donor_id}/mvault/")
        return result or {}

    # ── Orders ────────────────────────────────────────────────────────

    @retry_roi_crm_operation
    def list_orders(self, donor_id: int) -> List[Dict[str, Any]]:
        """
        List all orders for a donor.

        Args:
            donor_id: The ROI CRM donor ID

        Returns:
            List of order dicts

        Examples:
            >>> orders = connector.list_orders(12345)
        """
        return self._paginate(f"/donors/{donor_id}/orders/")

    @retry_roi_crm_operation
    def get_order(self, donor_id: int, order_id: int) -> Dict[str, Any]:
        """
        Get a specific order for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            order_id: The order ID

        Returns:
            Order dict

        Examples:
            >>> order = connector.get_order(12345, 333)
        """
        result = self._request("GET", f"/donors/{donor_id}/orders/{order_id}/")
        return result or {}

    @retry_roi_crm_operation
    def create_order(self, donor_id: int, **kwargs: Any) -> Dict[str, Any]:
        """
        Create a new order for a donor.

        Args:
            donor_id: The ROI CRM donor ID
            **kwargs: Order field values

        Returns:
            The newly created order dict

        Examples:
            >>> order = connector.create_order(12345, product_code="TSHIRT", quantity=2)
        """
        result = self._request(
            "POST", f"/donors/{donor_id}/orders/", json_body=kwargs
        )
        return result or {}

    # ── Code Tables ───────────────────────────────────────────────────

    @retry_roi_crm_operation
    def get_codes(self, entity: str) -> List[Dict[str, Any]]:
        """
        Get the code table for a given entity type.

        Code tables provide valid values for coded fields (e.g., fund codes,
        flag codes, source codes) for a given entity.

        Args:
            entity: The entity type (e.g., 'donors', 'donations', 'pledges')

        Returns:
            List of code dicts with code and description

        Examples:
            >>> fund_codes = connector.get_codes("donations")
            >>> flag_codes = connector.get_codes("donors")
        """
        return self._paginate(f"/codes/{entity}/")
