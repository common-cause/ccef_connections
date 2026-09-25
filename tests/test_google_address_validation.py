"""Tests for the Google Address Validation connector. No live calls."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.google_address_validation import (
    ADDRESS_VALIDATION_URL,
    GoogleAddressValidationConnector,
)
from ccef_connections.exceptions import ConnectionError, CredentialError, RateLimitError

FAKE_KEY = "AIzaFAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE123"

# Fabricated, shaped like Google's documented response.
RESPONSE = {
    "responseId": "resp-1",
    "result": {
        "verdict": {
            "inputGranularity": "PREMISE",
            "validationGranularity": "PREMISE",
            "possibleNextAction": "ACCEPT",
            "addressComplete": True,
            "hasReplacedComponents": True,
        },
        "address": {
            "formattedAddress": "123 Example St, Springfield, IL 62701-1234, USA",
            "postalAddress": {
                "regionCode": "US",
                "postalCode": "62701-1234",
                "administrativeArea": "IL",
                "locality": "Springfield",
                "addressLines": ["123 Example St"],
            },
            "addressComponents": [
                {"componentType": "street_number", "confirmationLevel": "CONFIRMED"},
                {"componentType": "locality", "confirmationLevel": "CONFIRMED",
                 "replaced": True},
                {"componentType": "subpremise",
                 "confirmationLevel": "UNCONFIRMED_BUT_PLAUSIBLE"},
            ],
        },
        "uspsData": {
            "standardizedAddress": {
                "firstAddressLine": "123 EXAMPLE ST",
                "city": "SPRINGFIELD",
                "state": "IL",
                "zipCode": "62701",
                "zipCodeExtension": "1234",
            },
            "dpvConfirmation": "Y",
        },
    },
}


def _resp(status=200, json_data=None, text="", headers=None):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.text = text
    r.headers = headers or {}
    r.json.return_value = json_data if json_data is not None else {}
    return r


@pytest.fixture
def av(monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY_PASSWORD", FAKE_KEY)
    c = GoogleAddressValidationConnector()
    c._credential_manager._credentials_cache.pop("GOOGLE_MAPS_API_KEY", None)
    return c


class TestCredential:
    def test_connect_loads_key(self, av):
        av.connect()
        assert av.health_check()

    def test_json_wrapped_key_is_refused(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY_PASSWORD", '{"key": "AIza..."}')
        c = GoogleAddressValidationConnector()
        c._credential_manager._credentials_cache.pop("GOOGLE_MAPS_API_KEY", None)
        with pytest.raises(CredentialError):
            c.connect()


class TestRequest:
    @patch("ccef_connections.connectors.google_address_validation.requests.post")
    def test_body_and_header(self, post, av):
        post.return_value = _resp(json_data=RESPONSE)
        av.validate_address(["123 Example St", ""], locality="Springfield",
                            administrative_area="IL", postal_code="62701")
        args, kwargs = post.call_args
        assert args[0] == ADDRESS_VALIDATION_URL
        assert kwargs["headers"]["X-Goog-Api-Key"] == FAKE_KEY
        assert "key" not in (kwargs.get("params") or {})          # never in the URL
        assert kwargs["json"] == {"address": {
            "regionCode": "US", "addressLines": ["123 Example St"],
            "locality": "Springfield", "administrativeArea": "IL",
            "postalCode": "62701"}}
        assert av.requests_made == 1

    @patch("ccef_connections.connectors.google_address_validation.requests.post")
    def test_cass_and_previous_id(self, post, av):
        post.return_value = _resp(json_data=RESPONSE)
        av.validate_address("123 Example St, Springfield IL", enable_usps_cass=True,
                            previous_response_id="resp-0")
        body = post.call_args.kwargs["json"]
        assert body["enableUspsCass"] is True and body["previousResponseId"] == "resp-0"

    def test_empty_address_is_refused_without_a_call(self, av):
        with patch("ccef_connections.connectors.google_address_validation.requests.post") as p:
            with pytest.raises(ValueError):
                av.validate_address(["", "  "])
            p.assert_not_called()

    @patch("tenacity.nap.time.sleep")
    @patch("ccef_connections.connectors.google_address_validation.requests.post")
    def test_429_retried_three_times_then_raised(self, post, _sleep, av):
        post.return_value = _resp(429, text="RESOURCE_EXHAUSTED")
        with pytest.raises(RateLimitError):
            av.validate_address("123 Example St")
        assert post.call_count == 3

    @patch("ccef_connections.connectors.google_address_validation.requests.post")
    def test_403_is_not_retried(self, post, av):
        post.return_value = _resp(403, text='{"error": {"status": "PERMISSION_DENIED"}}')
        with pytest.raises(ConnectionError, match="403"):
            av.validate_address("123 Example St")
        assert post.call_count == 1

    @patch("ccef_connections.connectors.google_address_validation.requests.post")
    def test_transport_failure(self, post, av):
        post.side_effect = requests.ConnectionError("boom")
        with pytest.raises(ConnectionError):
            av.validate_address("123 Example St")


class TestSummarize:
    def test_fields(self):
        s = GoogleAddressValidationConnector.summarize(RESPONSE)
        assert s["next_action"] == "ACCEPT"
        assert (s["zip5"], s["zip4"], s["dpv_confirmation"]) == ("62701", "1234", "Y")
        assert s["locality"] == "Springfield"
        assert s["replaced_components"] == ["locality"]
        assert s["unconfirmed_components"] == ["subpremise"]
        assert s["has_replaced_components"] is True
        assert s["address_complete"] is True

    def test_every_key_present_on_an_empty_response(self):
        s = GoogleAddressValidationConnector.summarize({})
        full = GoogleAddressValidationConnector.summarize(RESPONSE)
        assert set(s) == set(full)
        assert s["next_action"] is None and s["missing_components"] == []
