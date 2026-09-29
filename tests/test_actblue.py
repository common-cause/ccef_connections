"""Tests for the ActBlue CSV API connector."""

import json
from unittest.mock import MagicMock, patch

import pytest

from ccef_connections.connectors.actblue import ACTBLUE_API_BASE, ActBlueConnector
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import AuthenticationError, ConnectionError, CredentialError

CREDS = {"uuid": "u-123", "secret": "s-456"}


def _resp(status=200, body=None, text="", content=b""):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body if body is not None else {}
    r.text = text or json.dumps(body or {})
    r.content = content
    r.headers = {}
    return r


@pytest.fixture
def ab():
    c = ActBlueConnector()
    c._credential_manager = MagicMock()
    c._credential_manager.get_actblue_credentials.return_value = CREDS
    return c


def _fresh_manager():
    """A CredentialManager that bypasses the singleton's cache (as test_core does)."""
    mgr = object.__new__(CredentialManager)
    mgr._credentials_cache = {}
    mgr._env_loaded = True
    return mgr


class TestCredentials:
    def test_reads_json_uuid_secret(self):
        with patch.dict("os.environ", {"ACTBLUE_API_KEY_PASSWORD": json.dumps(CREDS)}):
            assert _fresh_manager().get_actblue_credentials() == CREDS

    def test_rejects_missing_secret(self):
        with patch.dict("os.environ", {"ACTBLUE_API_KEY_PASSWORD": json.dumps({"uuid": "x"})}):
            with pytest.raises(CredentialError, match="uuid"):
                _fresh_manager().get_actblue_credentials()


class TestConnect:
    def test_connect_sets_basic_auth(self, ab):
        ab.connect()
        assert ab._auth == ("u-123", "s-456") and ab.is_connected()


class TestRequestCsv:
    @patch("ccef_connections.connectors.actblue.requests.request")
    def test_posts_type_and_iso_dates(self, mock_req, ab):
        from datetime import date
        mock_req.return_value = _resp(body={"id": "csv-1"})
        assert ab.request_csv("paid_contributions", date(2026, 8, 22), "2026-08-23") == "csv-1"
        method, url = mock_req.call_args.args
        assert method == "POST" and url == f"{ACTBLUE_API_BASE}/csvs"
        assert mock_req.call_args.kwargs["json"] == {
            "csv_type": "paid_contributions", "date_range_start": "2026-08-22",
            "date_range_end": "2026-08-23"}
        assert mock_req.call_args.kwargs["auth"] == ("u-123", "s-456")

    def test_rejects_unknown_type(self, ab):
        with pytest.raises(ValueError):
            ab.request_csv("everything", "2026-01-01", "2026-01-02")

    @patch("ccef_connections.connectors.actblue.requests.request")
    def test_401_is_authentication_error(self, mock_req, ab):
        mock_req.return_value = _resp(401, text="Invalid API credentials")
        with pytest.raises(AuthenticationError):
            ab.request_csv("paid_contributions", "2026-08-22", "2026-08-23")


class TestWaitAndDownload:
    @patch("ccef_connections.connectors.actblue.time.sleep")
    @patch("ccef_connections.connectors.actblue.requests.request")
    def test_polls_until_download_url(self, mock_req, _sleep, ab):
        mock_req.side_effect = [_resp(body={"status": "in_progress"}),
                                _resp(body={"status": "complete", "download_url": "https://dl"})]
        assert ab.wait_for_csv("csv-1") == "https://dl"
        assert mock_req.call_count == 2

    @patch("ccef_connections.connectors.actblue.requests.request")
    def test_failed_status_raises(self, mock_req, ab):
        mock_req.return_value = _resp(body={"status": "failed"})
        with pytest.raises(ConnectionError, match="failed"):
            ab.wait_for_csv("csv-1")

    @patch("ccef_connections.connectors.actblue.requests.get")
    def test_download_parses_csv_with_bom(self, mock_get, ab):
        mock_get.return_value = _resp(content="﻿Receipt ID,Amount\nAB1,5.00\n".encode("utf-8"))
        assert ab.download_csv("https://dl") == [{"Receipt ID": "AB1", "Amount": "5.00"}]


class TestHealthCheck:
    @patch("ccef_connections.connectors.actblue.requests.get")
    def test_404_means_authenticated(self, mock_get, ab):
        ab.connect()
        mock_get.return_value = _resp(404, text="CSV request not found")
        assert ab.health_check() is True

    @patch("ccef_connections.connectors.actblue.requests.get")
    def test_401_means_bad_key(self, mock_get, ab):
        ab.connect()
        mock_get.return_value = _resp(401)
        assert ab.health_check() is False
