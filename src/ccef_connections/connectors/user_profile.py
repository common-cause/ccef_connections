"""
User profile connector for CCEF connections library.

Wraps the Power Automate "User Profile Info to API" flow, which answers two
questions against Entra ID: who is this person (manager, job title,
department), and who belongs to a given security group.

The flow is a single endpoint that switches on a ``requestType`` field:

* ``{"requestType": "user", "upn": "someone@commoncause.org"}`` returns
  ``requestedUser``, ``managerDisplayName``, ``managerMail``, ``jobTitle``
  and ``state``.
* ``{"requestType": "groupRoster", "groupEmail": "st-nc@commoncause.org"}``
  returns ``groupEmail`` plus ``members``, a list of
  ``{"displayName": ..., "mail": ...}``.

Two properties of this flow shape the API below, and both are load-bearing:

**A 502 is an answer, not an error.** The flow has no error path. A 502
``NoResponse`` means one of three things -- the address does not resolve in
Entra, the person resolves but has no manager (the top of the org chart does
this), or the request was malformed. It fails fast (~0.8s) and identically
every time, so retrying is pointless. :meth:`get_profile` therefore returns
``None`` rather than raising, and deliberately does not retry.

**``state`` is not always a state.** For non-state staff it carries a
department -- "Direct Marketing", "Foundations", "Major Donor". Callers that
mean a US state must check against a real state list, never against
"is non-blank".

Credentials: ``USER_PROFILE_API_URI`` (the flow's direct-invoke URL, which
embeds a SAS signature) and ``USER_PROFILE_API_CREDENTIALS_PASSWORD`` (the
automation key, sent as the ``x-automation-key`` header). The key must not go
in ``Authorization`` -- that collides with the URL's SAS scheme and the
gateway rejects the call.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..exceptions import ConnectionError

logger = logging.getLogger(__name__)

KEY_HEADER = "x-automation-key"
DEFAULT_TIMEOUT = 120


class UserProfileConnector(BaseConnection):
    """
    Look up staff profiles and security-group rosters via Power Automate.

    Results are memoised per connector instance, since resolving a CC list
    asks about the same managers and group members repeatedly. Pass
    ``cache=False`` for a long-lived instance that must see live changes.

    Examples:
        >>> connector = UserProfileConnector()
        >>> profile = connector.get_profile("sjones@commoncause.org")
        >>> profile["managerMail"]
        'ENunez@commoncause.org'
        >>> profile["jobTitle"]
        'State Director, NC'

        >>> connector.get_group_roster("st-nc@commoncause.org")[:2]
        ['BWarner@commoncause.org', 'RCampbell@commoncause.org']

        >>> # Walk upward until a title matches.
        >>> for step in connector.manager_chain("abarton@commoncause.org"):
        ...     print(step["jobTitle"])
    """

    def __init__(self, cache: bool = True) -> None:
        """
        Initialize the connector.

        Args:
            cache: Memoise profile and roster lookups on this instance
                (default True).
        """
        super().__init__()
        self._uri: Optional[str] = None
        self._key: Optional[str] = None
        self._cache_enabled = cache
        self._profiles: Dict[str, Optional[Dict[str, Any]]] = {}
        self._rosters: Dict[str, List[str]] = {}

    # ── Connection lifecycle ──────────────────────────────────────────

    def connect(self) -> None:
        """
        Load the flow URL and automation key.

        Does not make a live call; credentials are exercised on first use.

        Raises:
            CredentialError: If either credential is missing
            ConnectionError: If credential loading fails for any other reason
        """
        try:
            creds = self._credential_manager.get_user_profile_credentials()
            self._uri = creds["uri"]
            self._key = creds["key"]
            self._is_connected = True
            logger.info("Successfully connected to the user profile flow")
        except Exception as e:
            logger.error(f"Failed to connect to the user profile flow: {str(e)}")
            raise ConnectionError(
                f"Failed to connect to the user profile flow: {str(e)}"
            ) from e

    def disconnect(self) -> None:
        """Clear credentials, cached lookups and connection state."""
        self._uri = None
        self._key = None
        self._profiles.clear()
        self._rosters.clear()
        self._is_connected = False
        logger.debug("Disconnected from the user profile flow")

    def health_check(self) -> bool:
        """
        Check whether the connector holds both credentials.

        Returns:
            True if connected with a URL and key, False otherwise.
        """
        return bool(self._is_connected and self._uri and self._key)

    # ── Internal helpers ──────────────────────────────────────────────

    def _ensure_connected(self) -> None:
        """Auto-connect if not already connected."""
        if not self._is_connected:
            self.connect()

    def _request(self, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        POST one request to the flow.

        Args:
            body: Request body, including its ``requestType``

        Returns:
            Parsed JSON on HTTP 200, or None when the flow gives no answer
            (see the module docstring on 502 semantics).

        Raises:
            ConnectionError: On a network failure, or an HTTP status that is
                neither 200 nor a no-answer 502. A 401 lands here, and means
                the key or the URL's SAS is wrong -- most often a stray quote
                picked up from a .env file.
        """
        self._ensure_connected()
        try:
            resp = requests.post(
                self._uri,  # type: ignore[arg-type]
                headers={"Content-Type": "application/json", KEY_HEADER: self._key},  # type: ignore[dict-item]
                json=body,
                timeout=DEFAULT_TIMEOUT,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"User profile flow request failed: {e}") from e

        if resp.status_code == 200:
            try:
                payload = resp.json()
            except ValueError as e:
                raise ConnectionError(
                    f"User profile flow returned non-JSON: {resp.text[:200]}"
                ) from e
            return payload if isinstance(payload, dict) else None

        if resp.status_code == 502:
            # Not an error: the flow's only way of saying "no answer".
            logger.debug(f"User profile flow gave no answer for {body}")
            return None

        raise ConnectionError(
            f"User profile flow returned {resp.status_code}: {resp.text[:300]}"
        )

    # ── Lookups ───────────────────────────────────────────────────────

    def get_profile(self, upn: str) -> Optional[Dict[str, Any]]:
        """
        Look up one person by user principal name (their email address).

        Matching is case-insensitive. Deliberately does not retry: a genuine
        miss and a transient failure are indistinguishable here, and a miss
        repeats identically, so retrying only multiplies the wait.

        Args:
            upn: The person's email address / UPN

        Returns:
            Dict with ``requestedUser``, ``managerDisplayName``,
            ``managerMail``, ``jobTitle`` and ``state``; or None if the flow
            gave no answer -- meaning the address does not resolve, or the
            person has no manager.

        Raises:
            ConnectionError: On a network or authorization failure

        Examples:
            >>> connector.get_profile("jrupp@commoncause.org")["jobTitle"]
            'Senior Director, Data & Analytics'
        """
        key = (upn or "").strip().lower()
        if not key:
            return None
        if self._cache_enabled and key in self._profiles:
            return self._profiles[key]

        result = self._request({"requestType": "user", "upn": upn.strip()})
        if self._cache_enabled:
            self._profiles[key] = result
        return result

    def get_group_roster(self, group_email: str) -> List[str]:
        """
        List the email addresses belonging to a security group.

        Args:
            group_email: The group's address, e.g. ``"st-nc@commoncause.org"``

        Returns:
            Member email addresses, in the order the flow returned them.
            Empty if the group does not resolve or has no members. The
            group's own address is not included.

        Raises:
            ConnectionError: On a network or authorization failure

        Examples:
            >>> len(connector.get_group_roster("st-nc@commoncause.org"))
            17
        """
        key = (group_email or "").strip().lower()
        if not key:
            return []
        if self._cache_enabled and key in self._rosters:
            return self._rosters[key]

        payload = self._request(
            {"requestType": "groupRoster", "groupEmail": group_email.strip()}
        )
        members: List[str] = []
        if payload:
            # Read members[].mail explicitly. The payload also echoes the
            # group's own address under groupEmail, so scanning the whole
            # response for anything containing "@" picks up the group itself.
            for member in payload.get("members") or []:
                if isinstance(member, dict) and member.get("mail"):
                    members.append(str(member["mail"]).strip())
        if self._cache_enabled:
            self._rosters[key] = members
        return members

    def manager_chain(
        self, upn: str, max_hops: int = 8, include_self: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Walk the management chain upward from one person.

        The flow returns only a direct manager, so the chain is walked by
        feeding each ``managerMail`` back in as the next lookup.

        Args:
            upn: Where to start
            max_hops: Safety bound on the walk (default 8)
            include_self: Include the starting person as the first entry
                (default True)

        Returns:
            List of profile dicts, ordered from the start of the walk upward.
            Each has an extra ``upn`` key naming the address looked up. The
            walk stops at the top of the org chart -- whose own lookup gives
            no answer -- or on a cycle.

        Raises:
            ConnectionError: On a network or authorization failure

        Examples:
            >>> chain = connector.manager_chain("abarton@commoncause.org")
            >>> [c["jobTitle"] for c in chain]
            ['Program Manager, CO', 'State Director, CO', 'Vice President, States', ...]
        """
        chain: List[Dict[str, Any]] = []
        current = (upn or "").strip()
        seen = set()

        for hop in range(max_hops):
            if not current or current.lower() in seen:
                break
            seen.add(current.lower())
            profile = self.get_profile(current)
            if profile is None:
                break
            if hop > 0 or include_self:
                chain.append({**profile, "upn": current})
            current = str(profile.get("managerMail") or "").strip()

        return chain
