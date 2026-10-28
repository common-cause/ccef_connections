"""Tests for the PayPal connector."""

import datetime as dt
import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors import paypal as pp
from ccef_connections.connectors.paypal import PayPalConnector
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

CREDS = {"client_id": "cid-TEST", "secret": "sec-TEST"}


def _resp(status=200, body=None, text="", headers=None):
    r = MagicMock(spec=requests.Response)
    r.status_code = status
    r.text = text
    r.headers = headers or {}
    r.json.return_value = body if body is not None else {}
    return r


def _token_body(scope=pp.SEARCH_SCOPE):
    return {"access_token": "tok", "expires_in": 32400, "scope": "openid " + scope}


def _txn(code, when, value="100.00", subject=None, payer=None, status="S"):
    t = {"transaction_info": {"transaction_event_code": code,
                              "transaction_initiation_date": when,
                              "transaction_status": status,
                              "transaction_amount": {"currency_code": "USD", "value": value}}}
    if subject:
        t["transaction_info"]["transaction_subject"] = subject
    if payer:
        t["payer_info"] = {"payer_name": {"alternate_full_name": payer}}
    return t


@pytest.fixture(autouse=True)
def _clear_credential_cache():
    CredentialManager().clear_cache()
    yield
    CredentialManager().clear_cache()


def _connector():
    c = PayPalConnector()
    c._creds = dict(CREDS)
    c._token = "tok"
    c._token_expires = 1e12
    c._is_connected = True
    return c


# ── credentials ───────────────────────────────────────────────────────

class TestCredentials:
    def test_canonical(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", json.dumps(CREDS))
        assert CredentialManager().get_paypal_credentials() == CREDS

    def test_legacy_spelling(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD",
                           json.dumps({"client ID": "a", "secret": "b"}))
        assert CredentialManager().get_paypal_credentials() == {"client_id": "a", "secret": "b"}

    def test_same_value_twice_refused(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD",
                           json.dumps({"client_id": "x", "secret": "x"}))
        with pytest.raises(CredentialError, match="same value"):
            CredentialManager().get_paypal_credentials()

    def test_missing_field(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", json.dumps({"client_id": "x"}))
        with pytest.raises(CredentialError, match="missing a client id or secret"):
            CredentialManager().get_paypal_credentials()

    def test_not_json(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", "not json")
        with pytest.raises(CredentialError, match="not JSON"):
            CredentialManager().get_paypal_credentials()


# ── connect ───────────────────────────────────────────────────────────

class TestConnect:
    def test_connect_gets_token(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch("ccef_connections.connectors.paypal.requests.post",
                   return_value=_resp(body=_token_body())) as post:
            c = PayPalConnector()
            c.connect()
        assert c.is_connected()
        assert post.call_args.kwargs["auth"] == ("cid-TEST", "sec-TEST")
        assert post.call_args.kwargs["data"] == {"grant_type": "client_credentials"}

    def test_missing_search_scope_refused(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch("ccef_connections.connectors.paypal.requests.post",
                   return_value=_resp(body=_token_body(scope="other"))):
            with pytest.raises(AuthenticationError, match="Transaction Search"):
                PayPalConnector().connect()

    def test_bad_client(self, monkeypatch):
        monkeypatch.setenv("PAYPAL_API_KEY_PASSWORD", json.dumps(CREDS))
        with patch("ccef_connections.connectors.paypal.requests.post",
                   return_value=_resp(401, {"error": "invalid_client"})):
            with pytest.raises(AuthenticationError, match="invalid_client"):
                PayPalConnector().connect()

    def test_expired_token_refreshes(self):
        c = _connector()
        c._token_expires = 0
        with patch("ccef_connections.connectors.paypal.requests.post",
                   return_value=_resp(body=_token_body())) as post:
            c._headers()
        assert post.called


# ── requests ──────────────────────────────────────────────────────────

class TestRequest:
    def test_403_names_transaction_search(self):
        with patch("ccef_connections.connectors.paypal.requests.get",
                   return_value=_resp(403, text="NOT_AUTHORIZED")):
            with pytest.raises(AuthenticationError, match="Transaction Search"):
                _connector()._request("/x", {})

    def test_400_is_connection_error(self):
        with patch("ccef_connections.connectors.paypal.requests.get",
                   return_value=_resp(400, text="INVALID_REQUEST")):
            with pytest.raises(ConnectionError):
                _connector()._request("/x", {})

    def test_429_retried_then_raised(self):
        with patch("ccef_connections.connectors.paypal.requests.get",
                   return_value=_resp(429, headers={"Retry-After": "1"})) as get, \
                patch("tenacity.nap.time.sleep"):
            with pytest.raises(RateLimitError):
                _connector()._search(dt.date(2026, 10, 1), dt.date(2026, 10, 2), 1)
        assert get.call_count == 4


# ── transactions ──────────────────────────────────────────────────────

class TestTransactions:
    def test_chunks_cap_at_31_days(self):
        cs = pp.chunks(dt.date(2026, 7, 1), dt.date(2026, 9, 30))
        assert cs[0] == (dt.date(2026, 7, 1), dt.date(2026, 7, 31))
        assert all((b - a).days <= 30 for a, b in cs)
        assert cs[-1][1] == dt.date(2026, 9, 30)

    def test_pages_and_chunks(self):
        c = _connector()
        pages = {(dt.date(2026, 7, 1), 1): {"transaction_details": [1, 2], "total_pages": 2},
                 (dt.date(2026, 7, 1), 2): {"transaction_details": [3], "total_pages": 2},
                 (dt.date(2026, 8, 1), 1): {"transaction_details": [4], "total_pages": 1}}
        with patch.object(PayPalConnector, "_search",
                          side_effect=lambda lo, hi, page: pages[(lo, page)]):
            assert c.transactions("2026-07-01", "2026-08-10") == [1, 2, 3, 4]

    def test_future_window_is_empty(self):
        c = _connector()
        future = dt.date.today() + dt.timedelta(days=40)
        with patch.object(PayPalConnector, "_search") as s:
            assert c.transactions(future, future + dt.timedelta(days=5)) == []
        assert not s.called


# ── helpers ───────────────────────────────────────────────────────────

class TestHelpers:
    def test_sponsor_from_subject(self):
        t = _txn("T0001", "2026-10-01T15:00:00+0000",
                 subject="Vanguard Charitable has sent you money")
        assert pp.sponsor_of(t) == "Vanguard Charitable"

    def test_sponsor_from_payer_when_no_subject(self):
        t = _txn("T0001", "2026-10-01T15:00:00+0000", payer="PayPal Giving Fund")
        assert pp.sponsor_of(t) == "PayPal Giving Fund"

    def test_dates_are_utc(self):
        t = _txn("T0001", "2026-06-24T01:30:00+0000")
        assert pp.as_date(t) == dt.date(2026, 6, 24)
        assert pp.as_datetime(t).tzinfo is not None

    def test_status_words(self):
        assert pp.status_of(_txn("T0001", "2026-10-01T00:00:00Z", status="V")) == "Reversed"

    def test_payments_default_to_grants(self):
        ts = [_txn("T0001", "2026-10-01T00:00:00Z"), _txn("T0000", "2026-10-01T00:00:00Z"),
              _txn("T0400", "2026-10-02T00:00:00Z")]
        assert len(pp.payments(ts)) == 1
        assert len(pp.payments(ts, pp.EVENT_MONEY_IN)) == 2

    def test_payout_is_first_withdrawal_after_by_timestamp(self):
        pay = _txn("T0001", "2026-07-02T18:00:00+0000")
        same_day_before = _txn("T0400", "2026-07-02T09:00:00+0000", "-50.00")
        after = _txn("T0400", "2026-07-06T09:00:00+0000", "-100.00")
        wds = pp.withdrawals([after, same_day_before])
        assert pp.payout_date(pay, wds) == dt.date(2026, 7, 6)

    def test_no_withdrawal_yet(self):
        pay = _txn("T0001", "2026-07-02T18:00:00+0000")
        assert pp.payout_date(pay, []) is None

    def test_amount(self):
        assert pp.amount({"value": "-1.25"}) == -1.25
        assert pp.amount(None) == 0.0
