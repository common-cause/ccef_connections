"""
Action Builder connector for CCEF connections library.

This module provides access to the Action Builder API (OSDI v1.2.0),
covering Campaigns, Entity Types, Connection Types, People/Entities,
Tags, Taggings, and Connections.

Authentication uses a static API token in the OSDI-API-Token header.
Pagination is page-based (page/per_page/total_pages) rather than
cursor-based like Action Network.

API limitations:
- Connections can only be read or updated (no create via API)
- Taggings can only be read or deleted (no create/update via API)

Tag update pattern (update_records workflow):
  Replacing an existing tag value requires two API calls:
    1. DELETE /campaigns/{id}/tags/{tag_id}/taggings/{tagging_id}
       — removes the existing tagging. 404 is treated as success
         (tagging already absent = desired state achieved).
    2. POST /campaigns/{id}/people  (Person Signup Helper)
       — writes the new value via add_tags + identifiers.
  NOTE: There is no 'remove_tags' parameter in the POST body.
  Passing one causes a 500 Internal Server Error from the AB API.

Retry policy:
  retry_action_builder_operation retries only on RateLimitError (429).
  All other errors (including 4xx/5xx ConnectionError) fail immediately
  so callers see the real error without waiting through backoff.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_action_builder_operation
from ..exceptions import AuthenticationError, ConnectionError, RateLimitError

logger = logging.getLogger(__name__)

ACTION_BUILDER_API_BASE = "https://{subdomain}.actionbuilder.org/api/rest/v1"


class ActionBuilderConnector(BaseConnection):
    """
    Action Builder connector for field organizing and relationship mapping.

    Provides access to Action Builder resources using the OSDI/HAL+JSON
    API v1.2.0 with a static API token.

    Examples:
        >>> connector = ActionBuilderConnector()
        >>> connector.connect()
        >>> campaigns = connector.list_campaigns()
        >>> people = connector.list_people(campaign_id="abc123")
    """

    def __init__(self) -> None:
        """Initialize the Action Builder connector."""
        super().__init__()
        self._api_token: Optional[str] = None
        self._subdomain: Optional[str] = None
        self._base_url: Optional[str] = None

    def connect(self) -> None:
        """
        Establish connection to Action Builder by loading credentials.

        Raises:
            CredentialError: If credentials are missing or malformed
            ConnectionError: If connection setup fails
        """
        try:
            creds = self._credential_manager.get_action_builder_credentials()
            self._api_token = creds["api_token"]
            self._subdomain = creds["subdomain"]
            self._base_url = ACTION_BUILDER_API_BASE.format(
                subdomain=self._subdomain
            )
            self._is_connected = True
            logger.info("Successfully connected to Action Builder")
        except Exception as e:
            logger.error(f"Failed to connect to Action Builder: {str(e)}")
            raise ConnectionError(
                f"Failed to connect to Action Builder: {str(e)}"
            ) from e

    def disconnect(self) -> None:
        """Clear the Action Builder connection."""
        self._api_token = None
        self._subdomain = None
        self._base_url = None
        self._is_connected = False
        logger.debug("Disconnected from Action Builder")

    def health_check(self) -> bool:
        """
        Check connection health by fetching the first page of campaigns.

        Returns:
            True if connected and API responds, False otherwise
        """
        if not self._is_connected or not self._api_token:
            return False
        try:
            self._request("GET", "/campaigns", params={"page": 1, "per_page": 1})
            return True
        except Exception:
            return False

    # -- HTTP helpers ---------------------------------------------------------

    def _get_headers(self) -> Dict[str, str]:
        """Return request headers with the API token."""
        return {
            "OSDI-Api-Token": self._api_token or "",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Central HTTP method with error handling.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API path relative to the base URL (e.g. '/campaigns')
            params: Query parameters
            json_body: JSON request body

        Returns:
            Parsed JSON response, or None for 204 No Content

        Raises:
            AuthenticationError: On 401 responses
            RateLimitError: On 429 responses
            ConnectionError: On other HTTP errors or network failures
        """
        if not self._is_connected and not self._api_token:
            self.connect()

        url = f"{self._base_url}{path}"

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
                f"Action Builder API request failed: {e}"
            ) from e

        if resp.status_code == 401:
            raise AuthenticationError(
                f"Action Builder authentication failed ({resp.status_code}): {resp.text}"
            )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 1))
            raise RateLimitError(
                f"Action Builder rate limit exceeded, retry after {retry_after}s",
                retry_after=retry_after,
            )

        if resp.status_code == 204:
            return None

        if resp.status_code >= 400:
            raise ConnectionError(
                f"Action Builder API error {resp.status_code}: {resp.text}"
            )

        return resp.json()

    def _paginate(
        self,
        path: str,
        resource_key: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Iterate through all pages using page-based pagination.

        Args:
            path: API path
            resource_key: Key inside ``_embedded`` (e.g. 'action_builder:entities')
            params: Additional query parameters

        Returns:
            Combined list of all resources across all pages
        """
        results: List[Dict[str, Any]] = []
        page = 1

        while True:
            page_params: Dict[str, Any] = {"page": page, "per_page": 25}
            if params:
                page_params.update(params)

            data = self._request("GET", path, params=page_params)
            if data is None:
                break

            embedded = data.get("_embedded", {})
            if resource_key in embedded:
                results.extend(embedded[resource_key])

            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break

            page += 1

        return results

    # -- Campaigns ------------------------------------------------------------

    @retry_action_builder_operation
    def list_campaigns(self, modified_since: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all campaigns.

        Args:
            modified_since: Optional ISO-8601 datetime string; filters to
                campaigns modified after this date

        Returns:
            List of campaign resources
        """
        params: Dict[str, Any] = {}
        if modified_since:
            params["filter"] = f"modified_date gt '{modified_since}'"
        return self._paginate("/campaigns", "action_builder:campaigns", params or None)

    @retry_action_builder_operation
    def get_campaign(self, campaign_id: str) -> Dict[str, Any]:
        """
        Get a single campaign by ID.

        Args:
            campaign_id: Campaign UUID

        Returns:
            Campaign resource dict
        """
        result = self._request("GET", f"/campaigns/{campaign_id}")
        return result or {}

    # -- Entity Types ---------------------------------------------------------

    @retry_action_builder_operation
    def list_entity_types(self, campaign_id: str) -> List[Dict[str, Any]]:
        """
        List all entity types for a campaign (read-only).

        Args:
            campaign_id: Campaign UUID

        Returns:
            List of entity type resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/entity_types",
            "action_builder:entity_types",
        )

    @retry_action_builder_operation
    def get_entity_type(self, campaign_id: str, type_id: str) -> Dict[str, Any]:
        """
        Get a single entity type by ID (read-only).

        Args:
            campaign_id: Campaign UUID
            type_id: Entity type UUID

        Returns:
            Entity type resource dict
        """
        result = self._request(
            "GET", f"/campaigns/{campaign_id}/entity_types/{type_id}"
        )
        return result or {}

    # -- Connection Types -----------------------------------------------------

    @retry_action_builder_operation
    def list_connection_types(self, campaign_id: str) -> List[Dict[str, Any]]:
        """
        List all connection types for a campaign (read-only).

        Args:
            campaign_id: Campaign UUID

        Returns:
            List of connection type resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/connection_types",
            "action_builder:connection_types",
        )

    @retry_action_builder_operation
    def get_connection_type(self, campaign_id: str, type_id: str) -> Dict[str, Any]:
        """
        Get a single connection type by ID (read-only).

        Args:
            campaign_id: Campaign UUID
            type_id: Connection type UUID

        Returns:
            Connection type resource dict
        """
        result = self._request(
            "GET", f"/campaigns/{campaign_id}/connection_types/{type_id}"
        )
        return result or {}

    # -- People / Entities ----------------------------------------------------

    @retry_action_builder_operation
    def list_people(
        self, campaign_id: str, modified_since: Optional[str] = None, **filters: Any
    ) -> List[Dict[str, Any]]:
        """
        List all people/entities in a campaign.

        Args:
            campaign_id: Campaign UUID
            modified_since: Optional ISO-8601 datetime; filters by modified_date
            **filters: Additional query parameters

        Returns:
            List of person/entity resources
        """
        params: Dict[str, Any] = dict(filters)
        if modified_since:
            params["filter"] = f"modified_date gt '{modified_since}'"
        return self._paginate(
            f"/campaigns/{campaign_id}/people",
            "osdi:people",
            params or None,
        )

    @retry_action_builder_operation
    def get_person(self, campaign_id: str, person_id: str) -> Dict[str, Any]:
        """
        Get a single person/entity by ID.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID

        Returns:
            Person/entity resource dict
        """
        result = self._request(
            "GET", f"/campaigns/{campaign_id}/people/{person_id}"
        )
        return result or {}

    @retry_action_builder_operation
    def create_person(self, campaign_id: str, **fields: Any) -> Dict[str, Any]:
        """
        Create a new person/entity in a campaign.

        Args:
            campaign_id: Campaign UUID
            **fields: Person/entity fields to set

        Returns:
            Created person/entity resource
        """
        result = self._request(
            "POST",
            f"/campaigns/{campaign_id}/people",
            json_body={"person": fields},
        )
        return result or {}

    @retry_action_builder_operation
    def update_person(
        self, campaign_id: str, person_id: str, fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update an existing person/entity.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID
            fields: Fields to update

        Returns:
            Updated person/entity resource
        """
        result = self._request(
            "PUT",
            f"/campaigns/{campaign_id}/people/{person_id}",
            json_body=fields,
        )
        return result or {}

    @retry_action_builder_operation
    def delete_person(self, campaign_id: str, person_id: str) -> None:
        """
        Delete a person/entity from a campaign.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID
        """
        self._request("DELETE", f"/campaigns/{campaign_id}/people/{person_id}")

    # -- Tags -----------------------------------------------------------------

    @retry_action_builder_operation
    def list_tags(self, campaign_id: str) -> List[Dict[str, Any]]:
        """
        List all tags for a campaign.

        Args:
            campaign_id: Campaign UUID

        Returns:
            List of tag resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/tags",
            "osdi:tags",
        )

    @retry_action_builder_operation
    def get_tag(self, campaign_id: str, tag_id: str) -> Dict[str, Any]:
        """
        Get a single tag by ID.

        Args:
            campaign_id: Campaign UUID
            tag_id: Tag UUID

        Returns:
            Tag resource dict
        """
        result = self._request("GET", f"/campaigns/{campaign_id}/tags/{tag_id}")
        return result or {}

    @retry_action_builder_operation
    def create_tag(
        self,
        campaign_id: str,
        name: str,
        section: str,
        field_type: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Create a new tag (field) in a campaign.

        Args:
            campaign_id: Campaign UUID
            name: Tag/field name
            section: Section the tag belongs to
            field_type: Field type (e.g. 'checkbox', 'text')
            **kwargs: Additional tag fields

        Returns:
            Created tag resource
        """
        body: Dict[str, Any] = {
            "name": name,
            "section": section,
            "field_type": field_type,
        }
        body.update(kwargs)
        result = self._request(
            "POST", f"/campaigns/{campaign_id}/tags", json_body=body
        )
        return result or {}

    @retry_action_builder_operation
    def delete_tag(self, campaign_id: str, tag_id: str) -> None:
        """
        Delete a tag from a campaign.

        Args:
            campaign_id: Campaign UUID
            tag_id: Tag UUID
        """
        self._request("DELETE", f"/campaigns/{campaign_id}/tags/{tag_id}")

    # -- Taggings (read + delete only) ----------------------------------------

    @retry_action_builder_operation
    def list_taggings(self, campaign_id: str, tag_id: str) -> List[Dict[str, Any]]:
        """
        List all taggings for a tag.

        Note: Create/update not supported via API — use the UI.

        Args:
            campaign_id: Campaign UUID
            tag_id: Tag UUID

        Returns:
            List of tagging resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/tags/{tag_id}/taggings",
            "osdi:taggings",
        )

    @retry_action_builder_operation
    def list_person_taggings(
        self, campaign_id: str, person_id: str
    ) -> List[Dict[str, Any]]:
        """
        List all taggings for a person/entity.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID

        Returns:
            List of tagging resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/people/{person_id}/taggings",
            "osdi:taggings",
        )

    @retry_action_builder_operation
    def delete_tagging(
        self, campaign_id: str, tag_id: str, tagging_id: str
    ) -> None:
        """
        Delete a tagging.

        A 404 response is treated as success: if the tagging no longer exists
        (e.g. deleted by a previous sync run), the desired state is already
        achieved and we can proceed.

        Args:
            campaign_id: Campaign UUID
            tag_id: Tag UUID
            tagging_id: Tagging UUID
        """
        try:
            self._request(
                "DELETE",
                f"/campaigns/{campaign_id}/tags/{tag_id}/taggings/{tagging_id}",
            )
        except ConnectionError as e:
            if "404" in str(e):
                logger.debug(
                    f"delete_tagging: tagging {tagging_id} already absent (404) — skipping"
                )
                return
            raise

    # -- Connections (read + update only) -------------------------------------

    @retry_action_builder_operation
    def list_connections(
        self, campaign_id: str, person_id: str
    ) -> List[Dict[str, Any]]:
        """
        List all connections for a person/entity.

        Note: Create not supported via API — use the Connection Helper UI.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID

        Returns:
            List of connection resources
        """
        return self._paginate(
            f"/campaigns/{campaign_id}/people/{person_id}/connections",
            "action_builder:connections",
        )

    @retry_action_builder_operation
    def get_connection(
        self, campaign_id: str, person_id: str, connection_id: str
    ) -> Dict[str, Any]:
        """
        Get a single connection by ID.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID
            connection_id: Connection UUID

        Returns:
            Connection resource dict
        """
        result = self._request(
            "GET",
            f"/campaigns/{campaign_id}/people/{person_id}/connections/{connection_id}",
        )
        return result or {}

    @retry_action_builder_operation
    def update_entity_with_tags(
        self,
        campaign_id: str,
        entity_interact_id: str,
        add_tags: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Update an existing entity's tags via the Person Signup Helper.

        Posts to the Person Signup Helper endpoint with ``identifiers`` set to
        the entity's interact_id, which tells ActionBuilder to update the
        existing entity rather than create a new one.

        To replace an existing tag value, first call delete_tagging() for each
        existing tagging to remove, then call this method to add the new value.

        Args:
            campaign_id: Campaign UUID (interact_id)
            entity_interact_id: Entity interact_id UUID (36 chars)
            add_tags: List of tag dicts, each with keys:
                ``action_builder:section``, ``action_builder:field``, ``name``

        Returns:
            Response dict from the API
        """
        result = self._request(
            "POST",
            f"/campaigns/{campaign_id}/people",
            json_body={
                "person": {
                    "identifiers": [f"action_builder:{entity_interact_id}"]
                },
                "add_tags": add_tags,
            },
        )
        return result or {}

    @retry_action_builder_operation
    def insert_entity(
        self,
        campaign_id: str,
        person_data: Dict[str, Any],
        add_tags: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Insert a new entity via the Person Signup Helper.

        Posts to the Person Signup Helper endpoint without ``identifiers``,
        which creates a new entity in ActionBuilder.

        Args:
            campaign_id: Campaign UUID (interact_id)
            person_data: OSDI person fields dict (given_name, family_name,
                email_addresses, phone_numbers, postal_addresses, etc.)
            add_tags: Optional list of tag dicts, each with keys:
                ``action_builder:section``, ``action_builder:field``, ``name``

        Returns:
            Response dict from the API
        """
        body: Dict[str, Any] = {"person": person_data}
        if add_tags:
            body["add_tags"] = add_tags
        result = self._request(
            "POST",
            f"/campaigns/{campaign_id}/people",
            json_body=body,
        )
        return result or {}

    @retry_action_builder_operation
    def update_connection(
        self,
        campaign_id: str,
        person_id: str,
        connection_id: str,
        inactive: bool,
    ) -> Dict[str, Any]:
        """
        Update a connection's inactive status.

        Args:
            campaign_id: Campaign UUID
            person_id: Person/entity UUID
            connection_id: Connection UUID
            inactive: True to mark the connection inactive, False to reactivate

        Returns:
            Updated connection resource
        """
        result = self._request(
            "PUT",
            f"/campaigns/{campaign_id}/people/{person_id}/connections/{connection_id}",
            json_body={"inactive": inactive},
        )
        return result or {}
