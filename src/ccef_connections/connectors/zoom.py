"""
Zoom connector for CCEF connections library.

This module provides access to the Zoom API v2 for retrieving meeting and
webinar data, with a focus on pulling attendee/participant lists from large
events hosted for membership.

Uses Server-to-Server OAuth with direct HTTP via the requests library.
"""

import base64
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_zoom_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

ZOOM_TOKEN_URL = "https://zoom.us/oauth/token"
ZOOM_API_BASE = "https://api.zoom.us/v2"


class ZoomConnector(BaseConnection):
    """
    Zoom connector for meeting and webinar attendee retrieval.

    Provides access to Zoom meetings, webinars, and their participant/attendee
    lists using Server-to-Server OAuth authentication.

    Credentials are stored as JSON in ZOOM_CREDENTIALS_PASSWORD env var:
    {"account_id": "...", "client_id": "...", "client_secret": "..."}

    Examples:
        >>> connector = ZoomConnector()
        >>> connector.connect()
        >>> meetings = connector.list_meetings("me")
        >>> participants = connector.get_past_meeting_participants(meeting_id)
    """

    def __init__(self) -> None:
        """Initialize the Zoom connector."""
        super().__init__()
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def connect(self) -> None:
        """
        Establish connection to Zoom by obtaining a Server-to-Server OAuth token.

        Raises:
            CredentialError: If Zoom credentials are missing or invalid
            AuthenticationError: If OAuth token request fails
            ConnectionError: If connection setup fails
        """
        try:
            creds = self._credential_manager.get_zoom_credentials()
            self._fetch_token(
                creds["account_id"], creds["client_id"], creds["client_secret"]
            )
            self._is_connected = True
            logger.info("Successfully connected to Zoom")
        except AuthenticationError:
            logger.error("Failed to connect to Zoom: authentication failed")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to Zoom: {str(e)}")
            raise ConnectionError(f"Failed to connect to Zoom: {str(e)}") from e

    def disconnect(self) -> None:
        """Clear the Zoom connection and token."""
        self._access_token = None
        self._token_expires_at = 0.0
        self._is_connected = False
        logger.debug("Disconnected from Zoom")

    def health_check(self) -> bool:
        """
        Check if the Zoom connection is healthy.

        Verifies the token is valid by calling GET /v2/users/me.

        Returns:
            True if connected and token is valid, False otherwise
        """
        if not self._is_connected or not self._access_token:
            return False
        try:
            self._request("GET", "/users/me")
            return True
        except Exception:
            return False

    # ── Token management ──────────────────────────────────────────────

    def _fetch_token(
        self, account_id: str, client_id: str, client_secret: str
    ) -> None:
        """
        Fetch a Server-to-Server OAuth access token.

        Args:
            account_id: Zoom account ID
            client_id: OAuth application client ID
            client_secret: OAuth application client secret

        Raises:
            AuthenticationError: If token request fails
        """
        basic_auth = base64.b64encode(
            f"{client_id}:{client_secret}".encode()
        ).decode()

        try:
            resp = requests.post(
                ZOOM_TOKEN_URL,
                headers={"Authorization": f"Basic {basic_auth}"},
                data={
                    "grant_type": "account_credentials",
                    "account_id": account_id,
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(
                f"Failed to reach Zoom token endpoint: {e}"
            ) from e

        if resp.status_code != 200:
            raise AuthenticationError(
                f"Zoom OAuth token request failed ({resp.status_code}): {resp.text}"
            )

        data = resp.json()
        self._access_token = data["access_token"]
        # Expire slightly early to avoid edge cases
        self._token_expires_at = time.time() + data.get("expires_in", 3600) - 60
        logger.debug("Zoom OAuth token obtained")

    def _refresh_token_if_needed(self) -> None:
        """Re-fetch the OAuth token if it has expired."""
        if time.time() >= self._token_expires_at:
            logger.debug("Zoom token expired, refreshing")
            creds = self._credential_manager.get_zoom_credentials()
            self._fetch_token(
                creds["account_id"], creds["client_id"], creds["client_secret"]
            )

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
            method: HTTP method (GET, POST, PUT, PATCH, DELETE)
            path: API path relative to /v2 (e.g., '/users/me')
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: If authentication fails after retry
            RateLimitError: If rate limited by the API
            ConnectionError: If the request fails
        """
        if not self._is_connected and not self._access_token:
            self.connect()

        url = f"{ZOOM_API_BASE}{path}"

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
            raise ConnectionError(f"Zoom API request failed: {e}") from e

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
                    f"Zoom API request failed on retry: {e}"
                ) from e

            if resp.status_code == 401:
                raise AuthenticationError(
                    "Zoom authentication failed after token refresh"
                )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 10))
            raise RateLimitError(
                f"Zoom rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"Zoom API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    def _paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        resource_key: Optional[str] = None,
        page_size: int = 300,
    ) -> List[Dict[str, Any]]:
        """
        Generic pagination helper using next_page_token.

        Args:
            path: API path
            params: Query parameters for the first request
            resource_key: Key in the response that contains the resource list
            page_size: Number of records per page (max 300)

        Returns:
            Combined list of all resources across pages
        """
        results: List[Dict[str, Any]] = []
        current_params = dict(params or {})
        current_params["page_size"] = page_size

        while True:
            data = self._request("GET", path, params=current_params)
            if data is None:
                break

            if resource_key and resource_key in data:
                results.extend(data[resource_key])
            elif resource_key:
                break

            next_page_token = data.get("next_page_token", "")
            if not next_page_token:
                break

            current_params["next_page_token"] = next_page_token

        return results

    # ── Users ─────────────────────────────────────────────────────────

    @retry_zoom_operation
    def get_user(self, user_id: str = "me") -> Dict[str, Any]:
        """
        Get a user's profile.

        Args:
            user_id: User ID or email, or 'me' for the authenticated user

        Returns:
            User profile dict

        Examples:
            >>> user = connector.get_user("me")
            >>> print(user["email"])
        """
        result = self._request("GET", f"/users/{user_id}")
        return result or {}

    # ── Meetings ──────────────────────────────────────────────────────

    @retry_zoom_operation
    def list_meetings(
        self,
        user_id: str = "me",
        meeting_type: str = "scheduled",
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        """
        List meetings for a user.

        Args:
            user_id: User ID or email, or 'me'
            meeting_type: Type filter ('scheduled', 'live', 'upcoming',
                'upcoming_meetings', 'previous_meetings')
            **kwargs: Additional query parameters

        Returns:
            List of meeting dicts

        Examples:
            >>> meetings = connector.list_meetings("me", meeting_type="previous_meetings")
        """
        params: Dict[str, Any] = {"type": meeting_type}
        params.update(kwargs)
        return self._paginate(
            f"/users/{user_id}/meetings",
            params=params,
            resource_key="meetings",
        )

    @retry_zoom_operation
    def get_meeting(self, meeting_id: int) -> Dict[str, Any]:
        """
        Get details for a specific meeting.

        Args:
            meeting_id: The meeting ID

        Returns:
            Meeting details dict

        Examples:
            >>> meeting = connector.get_meeting(12345678901)
        """
        result = self._request("GET", f"/meetings/{meeting_id}")
        return result or {}

    @retry_zoom_operation
    def get_past_meeting_participants(
        self, meeting_id: str, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        Get participants from a past meeting via the reports API.

        This is the primary method for pulling attendee lists from completed
        meetings. Returns detailed participant info including name, email,
        join/leave times, and duration.

        Args:
            meeting_id: The meeting ID or UUID. Double-encode UUIDs that
                start with '/' or contain '//'.
            **kwargs: Additional query parameters

        Returns:
            List of participant dicts with keys like 'name', 'user_email',
            'join_time', 'leave_time', 'duration'

        Examples:
            >>> participants = connector.get_past_meeting_participants("12345678901")
            >>> for p in participants:
            ...     print(p["name"], p["user_email"], p["duration"])
        """
        params: Dict[str, Any] = {}
        params.update(kwargs)
        return self._paginate(
            f"/report/meetings/{meeting_id}/participants",
            params=params,
            resource_key="participants",
        )

    # ── Webinars ──────────────────────────────────────────────────────

    @retry_zoom_operation
    def list_webinars(
        self, user_id: str = "me", **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        List webinars for a user.

        Args:
            user_id: User ID or email, or 'me'
            **kwargs: Additional query parameters

        Returns:
            List of webinar dicts

        Examples:
            >>> webinars = connector.list_webinars("me")
        """
        params: Dict[str, Any] = {}
        params.update(kwargs)
        return self._paginate(
            f"/users/{user_id}/webinars",
            params=params,
            resource_key="webinars",
        )

    @retry_zoom_operation
    def get_webinar(self, webinar_id: int) -> Dict[str, Any]:
        """
        Get details for a specific webinar.

        Args:
            webinar_id: The webinar ID

        Returns:
            Webinar details dict

        Examples:
            >>> webinar = connector.get_webinar(99887766554)
        """
        result = self._request("GET", f"/webinars/{webinar_id}")
        return result or {}

    @retry_zoom_operation
    def get_webinar_registrants(
        self, webinar_id: int, status: str = "approved", **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        List registrants for a webinar.

        Args:
            webinar_id: The webinar ID
            status: Filter by status ('pending', 'approved', 'denied')
            **kwargs: Additional query parameters

        Returns:
            List of registrant dicts with name, email, registration time, etc.

        Examples:
            >>> registrants = connector.get_webinar_registrants(99887766554)
            >>> for r in registrants:
            ...     print(r["first_name"], r["email"])
        """
        params: Dict[str, Any] = {"status": status}
        params.update(kwargs)
        return self._paginate(
            f"/webinars/{webinar_id}/registrants",
            params=params,
            resource_key="registrants",
        )

    @retry_zoom_operation
    def get_past_webinar_participants(
        self, webinar_id: str, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        Get participants/attendees from a past webinar via the reports API.

        This is the primary method for pulling attendee lists from completed
        webinars. Returns detailed participant info including name, email,
        join/leave times, and duration.

        Args:
            webinar_id: The webinar ID or UUID
            **kwargs: Additional query parameters

        Returns:
            List of participant dicts with keys like 'name', 'user_email',
            'join_time', 'leave_time', 'duration'

        Examples:
            >>> attendees = connector.get_past_webinar_participants("99887766554")
            >>> for a in attendees:
            ...     print(a["name"], a["user_email"], a["duration"])
        """
        params: Dict[str, Any] = {}
        params.update(kwargs)
        return self._paginate(
            f"/report/webinars/{webinar_id}/participants",
            params=params,
            resource_key="participants",
        )

    @retry_zoom_operation
    def get_webinar_absentees(
        self, webinar_id: str, **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        Get registrants who did not attend a past webinar.

        Args:
            webinar_id: The webinar UUID
            **kwargs: Additional query parameters

        Returns:
            List of absentee registrant dicts

        Examples:
            >>> absentees = connector.get_webinar_absentees("abc123-uuid")
        """
        params: Dict[str, Any] = {}
        params.update(kwargs)
        return self._paginate(
            f"/past_webinars/{webinar_id}/absentees",
            params=params,
            resource_key="registrants",
        )

    # ── Meeting registrants ───────────────────────────────────────────

    @retry_zoom_operation
    def get_meeting_registrants(
        self, meeting_id: int, status: str = "approved", **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        List registrants for a meeting (if registration is enabled).

        Args:
            meeting_id: The meeting ID
            status: Filter by status ('pending', 'approved', 'denied')
            **kwargs: Additional query parameters

        Returns:
            List of registrant dicts

        Examples:
            >>> registrants = connector.get_meeting_registrants(12345678901)
        """
        params: Dict[str, Any] = {"status": status}
        params.update(kwargs)
        return self._paginate(
            f"/meetings/{meeting_id}/registrants",
            params=params,
            resource_key="registrants",
        )
