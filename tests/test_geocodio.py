"""Tests for the Geocodio connector."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.geocodio import (
    GEOCODIO_API_BASE,
    GeocodioConnector,
)
from ccef_connections.exceptions import (
    ConnectionError,
    CredentialError,
    RateLimitError,
)


# ── Helpers ────────────────────────────────────────────────────────────


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


FAKE_API_KEY = "test-geocodio-api-key"

GEOCODE_RESPONSE = {
    "input": {
        "address_components": {
            "number": "1600",
            "street": "Pennsylvania",
            "suffix": "Ave",
            "secondarydesignator": "NW",
            "city": "Washington",
            "state": "DC",
            "zip": "20500",
            "country": "US",
        },
        "formatted_address": "1600 Pennsylvania Ave NW, Washington, DC 20500",
    },
    "results": [
        {
            "address_components": {
                "number": "1600",
                "street": "Pennsylvania",
                "suffix": "Ave",
                "secondarydesignator": "NW",
                "city": "Washington",
                "state": "DC",
                "zip": "20500",
                "country": "US",
            },
            "formatted_address": "1600 Pennsylvania Ave NW, Washington, DC 20500",
            "location": {"lat": 38.897675, "lng": -77.036548},
            "accuracy": 1,
            "accuracy_type": "rooftop",
            "source": "TIGER/Line® dataset from the US Census Bureau",
        }
    ],
}

REVERSE_RESPONSE = {
    "results": [
        {
            "address_components": {
                "number": "1600",
                "street": "Pennsylvania",
                "suffix": "Ave",
                "secondarydesignator": "NW",
                "city": "Washington",
                "state": "DC",
                "zip": "20500",
                "country": "US",
            },
            "formatted_address": "1600 Pennsylvania Ave NW, Washington, DC 20500",
            "location": {"lat": 38.897675, "lng": -77.036548},
            "accuracy": 1,
            "accuracy_type": "rooftop",
            "source": "TIGER/Line® dataset from the US Census Bureau",
        }
    ]
}

BATCH_GEOCODE_RESPONSE = {
    "results": [
        {
            "query": "1600 Pennsylvania Ave NW, Washington, DC 20500",
            "response": GEOCODE_RESPONSE,
        },
        {
            "query": "350 Fifth Ave, New York, NY 10118",
            "response": {
                "input": {"formatted_address": "350 Fifth Ave, New York, NY 10118"},
                "results": [
                    {
                        "formatted_address": "350 5th Ave, New York, NY 10118",
                        "location": {"lat": 40.748441, "lng": -73.996277},
                        "accuracy": 1,
                        "accuracy_type": "rooftop",
                        "source": "NYC Open Data",
                    }
                ],
            },
        },
    ]
}

BATCH_REVERSE_RESPONSE = {
    "results": [
        {
            "query": "38.897675,-77.036548",
            "response": REVERSE_RESPONSE,
        },
        {
            "query": "40.748441,-73.996277",
            "response": {
                "results": [
                    {
                        "formatted_address": "350 5th Ave, New York, NY 10118",
                        "location": {"lat": 40.748441, "lng": -73.996277},
                        "accuracy": 1,
                        "accuracy_type": "rooftop",
                        "source": "NYC Open Data",
                    }
                ]
            },
        },
    ]
}


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture
def connector():
    """Create a GeocodioConnector with a mocked credential manager."""
    with patch.object(GeocodioConnector, "_credential_manager", create=True) as mock_cm:
        mock_cm.get_geocodio_key.return_value = FAKE_API_KEY
        c = GeocodioConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Return a connector that is already connected."""
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


# ── Initialization ─────────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        c = GeocodioConnector()
        assert c._api_key is None
        assert not c.is_connected()

    def test_inherits_base_connection(self):
        from ccef_connections.core.base import BaseConnection

        assert isinstance(GeocodioConnector(), BaseConnection)


# ── Connect / Disconnect ───────────────────────────────────────────────


class TestConnect:
    def test_connect_loads_api_key(self, connector):
        connector.connect()

        assert connector._api_key == FAKE_API_KEY
        assert connector.is_connected()

    def test_connect_calls_credential_manager(self, connector):
        connector.connect()

        connector._credential_manager.get_geocodio_key.assert_called_once()

    def test_connect_raises_connection_error_on_failure(self, connector):
        connector._credential_manager.get_geocodio_key.side_effect = CredentialError(
            "Missing key"
        )

        with pytest.raises(ConnectionError, match="Failed to connect to Geocodio"):
            connector.connect()

    def test_connect_wraps_credential_error(self, connector):
        original = CredentialError("env var not set")
        connector._credential_manager.get_geocodio_key.side_effect = original

        with pytest.raises(ConnectionError) as exc_info:
            connector.connect()

        assert exc_info.value.__cause__ is original


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        connected_connector.disconnect()

        assert connected_connector._api_key is None
        assert not connected_connector.is_connected()

    def test_disconnect_idempotent(self, connector):
        connector.disconnect()
        connector.disconnect()
        assert not connector.is_connected()


# ── Health check ───────────────────────────────────────────────────────


class TestHealthCheck:
    def test_returns_true_when_connected(self, connected_connector):
        assert connected_connector.health_check() is True

    def test_returns_false_when_disconnected(self, connector):
        assert connector.health_check() is False

    def test_returns_false_when_key_is_none(self, connected_connector):
        connected_connector._api_key = None
        assert connected_connector.health_check() is False

    def test_returns_false_when_key_is_empty_string(self, connected_connector):
        connected_connector._api_key = ""
        assert connected_connector.health_check() is False


# ── Context manager ────────────────────────────────────────────────────


class TestContextManager:
    def test_enter_connects(self, connector):
        with connector as c:
            assert c.is_connected()

    def test_exit_disconnects(self, connector):
        with connector as c:
            pass
        assert not c.is_connected()


# ── _base_params ──────────────────────────────────────────────────────


class TestBaseParams:
    def test_includes_api_key(self, connected_connector):
        params = connected_connector._base_params()
        assert params["api_key"] == FAKE_API_KEY

    def test_no_fields_by_default(self, connected_connector):
        params = connected_connector._base_params()
        assert "fields" not in params

    def test_fields_joined_by_comma(self, connected_connector):
        params = connected_connector._base_params(["cd", "stateleg"])
        assert params["fields"] == "cd,stateleg"

    def test_single_field(self, connected_connector):
        params = connected_connector._base_params(["census"])
        assert params["fields"] == "census"


# ── _request ──────────────────────────────────────────────────────────


class TestRequest:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_get_request_to_correct_url(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, {"results": []})

        connected_connector._request("GET", "/geocode", params={"q": "test"})

        mock_req.assert_called_once()
        call_args = mock_req.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == f"{GEOCODIO_API_BASE}/geocode"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_passes_params(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, {"results": []})
        params = {"api_key": FAKE_API_KEY, "q": "some address"}

        connected_connector._request("GET", "/geocode", params=params)

        assert mock_req.call_args.kwargs["params"] == params

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_429_raises_rate_limit_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(
            429, text="Too Many Requests", headers={"Retry-After": "30"}
        )

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector._request("GET", "/geocode")

        assert exc_info.value.retry_after == 30

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_400_raises_connection_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(400, text="Bad Request")

        with pytest.raises(ConnectionError, match="400"):
            connected_connector._request("GET", "/geocode")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_500_raises_connection_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(500, text="Internal Server Error")

        with pytest.raises(ConnectionError, match="500"):
            connected_connector._request("GET", "/geocode")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_network_error_raises_connection_error(self, mock_req, connected_connector):
        mock_req.side_effect = requests.RequestException("timeout")

        with pytest.raises(ConnectionError, match="Geocodio request failed"):
            connected_connector._request("GET", "/geocode")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_auto_connects_if_not_connected(self, mock_req, connector):
        mock_req.return_value = _make_response(200, {"results": []})

        connector._request("GET", "/geocode", params={"api_key": FAKE_API_KEY})

        assert connector.is_connected()

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_returns_parsed_json(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        result = connected_connector._request("GET", "/geocode")

        assert result == GEOCODE_RESPONSE


# ── geocode ───────────────────────────────────────────────────────────


class TestGeocode:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_calls_get_endpoint(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        connected_connector.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        call_args = mock_req.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1].endswith("/geocode")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_passes_address_as_q(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)
        address = "1600 Pennsylvania Ave NW, Washington, DC"

        connected_connector.geocode(address)

        params = mock_req.call_args.kwargs["params"]
        assert params["q"] == address

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_default_limit_is_1(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        connected_connector.geocode("any address")

        params = mock_req.call_args.kwargs["params"]
        assert params["limit"] == "1"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_custom_limit(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        connected_connector.geocode("any address", limit=5)

        params = mock_req.call_args.kwargs["params"]
        assert params["limit"] == "5"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_with_fields(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        connected_connector.geocode("any address", fields=["cd", "stateleg"])

        params = mock_req.call_args.kwargs["params"]
        assert params["fields"] == "cd,stateleg"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_returns_response(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        result = connected_connector.geocode("1600 Pennsylvania Ave NW, Washington, DC")

        assert result == GEOCODE_RESPONSE
        assert result["results"][0]["location"]["lat"] == 38.897675

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_geocode_includes_api_key(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, GEOCODE_RESPONSE)

        connected_connector.geocode("any address")

        params = mock_req.call_args.kwargs["params"]
        assert params["api_key"] == FAKE_API_KEY

    def test_geocode_has_retry_decorator(self):
        assert hasattr(GeocodioConnector.geocode, "retry")


# ── reverse_geocode ───────────────────────────────────────────────────


class TestReverseGeocode:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_reverse_geocode_calls_reverse_endpoint(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, REVERSE_RESPONSE)

        connected_connector.reverse_geocode(38.897675, -77.036548)

        call_args = mock_req.call_args
        assert call_args[0][1].endswith("/reverse")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_reverse_geocode_formats_q_param(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, REVERSE_RESPONSE)

        connected_connector.reverse_geocode(38.897675, -77.036548)

        params = mock_req.call_args.kwargs["params"]
        assert params["q"] == "38.897675,-77.036548"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_reverse_geocode_default_limit(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, REVERSE_RESPONSE)

        connected_connector.reverse_geocode(38.0, -77.0)

        params = mock_req.call_args.kwargs["params"]
        assert params["limit"] == "1"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_reverse_geocode_with_fields(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, REVERSE_RESPONSE)

        connected_connector.reverse_geocode(38.0, -77.0, fields=["census"])

        params = mock_req.call_args.kwargs["params"]
        assert params["fields"] == "census"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_reverse_geocode_returns_response(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, REVERSE_RESPONSE)

        result = connected_connector.reverse_geocode(38.897675, -77.036548)

        assert result == REVERSE_RESPONSE
        assert result["results"][0]["formatted_address"].startswith("1600 Pennsylvania")

    def test_reverse_geocode_has_retry_decorator(self):
        assert hasattr(GeocodioConnector.reverse_geocode, "retry")


# ── batch_geocode ─────────────────────────────────────────────────────


class TestBatchGeocode:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_geocode_uses_post(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_GEOCODE_RESPONSE)
        addresses = ["1600 Pennsylvania Ave NW, DC", "350 Fifth Ave, NY"]

        connected_connector.batch_geocode(addresses)

        assert mock_req.call_args[0][0] == "POST"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_geocode_sends_list_as_body(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_GEOCODE_RESPONSE)
        addresses = ["1600 Pennsylvania Ave NW, DC", "350 Fifth Ave, NY"]

        connected_connector.batch_geocode(addresses)

        assert mock_req.call_args.kwargs["json"] == addresses

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_geocode_sends_dict_as_body(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_GEOCODE_RESPONSE)
        addresses = {"whitehouse": "1600 Pennsylvania Ave NW, DC", "empire": "350 Fifth Ave, NY"}

        connected_connector.batch_geocode(addresses)

        assert mock_req.call_args.kwargs["json"] == addresses

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_geocode_with_fields(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_GEOCODE_RESPONSE)

        connected_connector.batch_geocode(["any address"], fields=["cd"])

        params = mock_req.call_args.kwargs["params"]
        assert params["fields"] == "cd"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_geocode_returns_response(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_GEOCODE_RESPONSE)

        result = connected_connector.batch_geocode(["1600 Pennsylvania Ave NW, DC"])

        assert result == BATCH_GEOCODE_RESPONSE
        assert len(result["results"]) == 2

    def test_batch_geocode_has_retry_decorator(self):
        assert hasattr(GeocodioConnector.batch_geocode, "retry")


# ── batch_reverse_geocode ─────────────────────────────────────────────


class TestBatchReverseGeocode:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_reverse_uses_post(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_REVERSE_RESPONSE)

        connected_connector.batch_reverse_geocode(["38.897675,-77.036548"])

        assert mock_req.call_args[0][0] == "POST"

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_reverse_calls_reverse_endpoint(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_REVERSE_RESPONSE)

        connected_connector.batch_reverse_geocode(["38.897675,-77.036548"])

        assert mock_req.call_args[0][1].endswith("/reverse")

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_reverse_sends_list_as_body(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_REVERSE_RESPONSE)
        coords = ["38.897675,-77.036548", "40.748441,-73.996277"]

        connected_connector.batch_reverse_geocode(coords)

        assert mock_req.call_args.kwargs["json"] == coords

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_reverse_sends_dict_as_body(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_REVERSE_RESPONSE)
        coords = {"loc1": "38.897675,-77.036548", "loc2": "40.748441,-73.996277"}

        connected_connector.batch_reverse_geocode(coords)

        assert mock_req.call_args.kwargs["json"] == coords

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_batch_reverse_returns_response(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(200, BATCH_REVERSE_RESPONSE)

        result = connected_connector.batch_reverse_geocode(["38.897675,-77.036548"])

        assert result == BATCH_REVERSE_RESPONSE

    def test_batch_reverse_has_retry_decorator(self):
        assert hasattr(GeocodioConnector.batch_reverse_geocode, "retry")


# ── Rate limit / error propagation ────────────────────────────────────


class TestErrorHandling:
    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_rate_limit_error_has_retry_after(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(
            429, text="quota exceeded", headers={"Retry-After": "45"}
        )

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector.geocode("any address")

        assert exc_info.value.retry_after == 45

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_rate_limit_defaults_retry_after_60(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(429, text="quota exceeded", headers={})

        with pytest.raises(RateLimitError) as exc_info:
            connected_connector.geocode("any address")

        assert exc_info.value.retry_after == 60

    @patch("ccef_connections.connectors.geocodio.requests.request")
    def test_api_error_raises_connection_error(self, mock_req, connected_connector):
        mock_req.return_value = _make_response(422, text="Unprocessable Entity")

        with pytest.raises(ConnectionError):
            connected_connector.geocode("bad input")
