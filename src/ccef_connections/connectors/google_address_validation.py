"""
Google Address Validation connector for CCEF connections library.

Validates and standardizes one postal address per request via
``POST https://addressvalidation.googleapis.com/v1:validateAddress``. It returns
the corrected address, per-component confirmation levels, a verdict
(``possibleNextAction``: ACCEPT / CONFIRM / CONFIRM_ADD_SUBPREMISES / FIX), and,
for US and Puerto Rico addresses, USPS data including ZIP+4 and DPV.

**Every request is billed.** There is no batch endpoint and no free re-ask, so a
caller should send only the addresses it cannot resolve otherwise, and should
keep its own spend cap (see roi-account-contact-information for the pattern).

Authentication: the key goes in the ``X-Goog-Api-Key`` header rather than the
``?key=`` query parameter, so it never appears in a logged URL or an exception
message. It is read from ``GOOGLE_MAPS_API_KEY_PASSWORD``, a BARE ``AIza...``
string (see ``CredentialManager.get_google_maps_key``).
"""

import logging
from typing import Any, Dict, List, Optional, Union

import requests

from ..core.base import BaseConnection
from ..core.retry import retry_google_address_validation_operation
from ..exceptions import ConnectionError, CredentialError, RateLimitError

logger = logging.getLogger(__name__)

ADDRESS_VALIDATION_URL = "https://addressvalidation.googleapis.com/v1:validateAddress"


class GoogleAddressValidationConnector(BaseConnection):
    """
    Google Maps Platform Address Validation.

    Examples:
        >>> av = GoogleAddressValidationConnector()
        >>> resp = av.validate_address(["1600 Amphitheatre Pkwy"],
        ...                            locality="Mountain View",
        ...                            administrative_area="CA")
        >>> summary = GoogleAddressValidationConnector.summarize(resp)
        >>> summary["next_action"]      # e.g. "ACCEPT", "CONFIRM" or "FIX"
    """

    def __init__(self) -> None:
        super().__init__()
        self._api_key: Optional[str] = None
        self.requests_made = 0  # billed calls this instance sent (2xx or 4xx)

    def connect(self) -> None:
        """Load the API key. Makes no live call; the key is checked on first use."""
        try:
            self._api_key = self._credential_manager.get_google_maps_key()
            self._is_connected = True
            logger.info("Successfully connected to Google Address Validation")
        except CredentialError:
            logger.error("Failed to connect to Google Address Validation: credentials missing")
            raise
        except Exception as e:
            raise ConnectionError(
                f"Failed to connect to Google Address Validation: {e}") from e

    def disconnect(self) -> None:
        self._api_key = None
        self._is_connected = False

    def health_check(self) -> bool:
        return bool(self._is_connected and self._api_key)

    def _ensure_connected(self) -> None:
        if not self._is_connected:
            self.connect()
        if not self._api_key:
            # Same trap Geocodio fell into (0.13.1): never send a request
            # without a key, because the 403 then blames the credential.
            raise ConnectionError("Google Maps API key unavailable after connect()")

    def _post(self, body: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_connected()
        try:
            resp = requests.post(
                ADDRESS_VALIDATION_URL,
                json=body,
                headers={"X-Goog-Api-Key": self._api_key or ""},
                timeout=30,
            )
        except requests.RequestException as e:
            raise ConnectionError(f"Google Address Validation request failed: {e}") from e
        self.requests_made += 1

        if resp.status_code == 429:
            raise RateLimitError(
                "Google Address Validation quota or rate limit exceeded (429). "
                "A per-day quota resets at midnight Pacific.",
                retry_after=int(resp.headers.get("Retry-After", 30)),
            )
        if resp.status_code >= 400:
            # Google's error body carries the reason (API_KEY_INVALID,
            # BILLING_DISABLED, the API not enabled, ...). It never contains
            # the key, because the key travels in a header.
            raise ConnectionError(
                f"Google Address Validation error {resp.status_code}: {resp.text[:500]}")
        return resp.json()

    @retry_google_address_validation_operation
    def validate_address(
        self,
        address_lines: Union[str, List[str]],
        *,
        region_code: str = "US",
        locality: Optional[str] = None,
        administrative_area: Optional[str] = None,
        postal_code: Optional[str] = None,
        enable_usps_cass: bool = False,
        previous_response_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Validate one address. Returns Google's response dict (``result``,
        ``responseId``).

        Args:
            address_lines: street line(s); a single string is one line. May also
                be the whole address on one line, leaving the other args None.
            region_code: CLDR region, e.g. ``"US"``, ``"CA"``, ``"PR"``.
            locality / administrative_area / postal_code: city, state, ZIP.
            enable_usps_cass: US/PR only. Asks for CASS-certified processing.
            previous_response_id: the ``responseId`` of an earlier call on the
                SAME address, for Google's re-validation flow.

        Raises:
            RateLimitError: 429 (retried up to 3 times)
            ConnectionError: any other error, including a bad key or a disabled
                API or billing account
        """
        lines = [address_lines] if isinstance(address_lines, str) else list(address_lines)
        lines = [l for l in (s.strip() for s in lines if s) if l]
        if not lines:
            raise ValueError("address_lines is empty")
        address: Dict[str, Any] = {"regionCode": region_code, "addressLines": lines}
        if locality:
            address["locality"] = locality
        if administrative_area:
            address["administrativeArea"] = administrative_area
        if postal_code:
            address["postalCode"] = postal_code
        body: Dict[str, Any] = {"address": address}
        if enable_usps_cass:
            body["enableUspsCass"] = True
        if previous_response_id:
            body["previousResponseId"] = previous_response_id
        return self._post(body)

    @staticmethod
    def summarize(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten a response into the fields a hygiene rule decides on.

        Every key is always present (``None`` when Google omits it), so a caller
        can write a row without guarding each field.
        """
        result = response.get("result") or {}
        verdict = result.get("verdict") or {}
        addr = result.get("address") or {}
        postal = addr.get("postalAddress") or {}
        usps = result.get("uspsData") or {}
        std = usps.get("standardizedAddress") or {}
        comps = addr.get("addressComponents") or []
        return {
            "response_id": response.get("responseId"),
            "next_action": verdict.get("possibleNextAction"),
            "validation_granularity": verdict.get("validationGranularity"),
            "address_complete": bool(verdict.get("addressComplete")),
            "has_unconfirmed_components": bool(verdict.get("hasUnconfirmedComponents")),
            "has_inferred_components": bool(verdict.get("hasInferredComponents")),
            "has_replaced_components": bool(verdict.get("hasReplacedComponents")),
            "formatted_address": addr.get("formattedAddress"),
            "address_lines": postal.get("addressLines"),
            "locality": postal.get("locality"),
            "administrative_area": postal.get("administrativeArea"),
            "postal_code": postal.get("postalCode"),
            "region_code": postal.get("regionCode"),
            "usps_first_line": std.get("firstAddressLine"),
            "usps_city": std.get("city"),
            "usps_state": std.get("state"),
            "zip5": std.get("zipCode"),
            "zip4": std.get("zipCodeExtension"),
            "dpv_confirmation": usps.get("dpvConfirmation"),
            "unconfirmed_components": [
                c.get("componentType") for c in comps
                if c.get("confirmationLevel") not in (None, "CONFIRMED")
            ],
            "replaced_components": [c.get("componentType") for c in comps if c.get("replaced")],
            "missing_components": addr.get("missingComponentTypes") or [],
        }
