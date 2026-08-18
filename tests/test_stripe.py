"""Tests for the Stripe connector."""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.stripe import (
    STRIPE_API_BASE,
    STRIPE_MAX_LIMIT,
    StripeConnector,
    _to_unix,
)
from ccef_connections.core.credentials import CredentialManager
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    CredentialError,
    RateLimitError,
)

# ── Fixtures ──────────────────────────────────────────────────────────

# Long enough to pass the truncation guard (real keys run past 100 chars).
KEY_C3 = "rk_live_" + "a" * 100
KEY_C4 = "rk_live_" + "b" * 100

ACCOUNT_OBJ = {"id": "acct_TEST", "charges_enabled": True}


def _make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    return resp


@pytest.fixture(autouse=True)
def _clear_credential_cache():
    """The CredentialManager is a caching singleton; isolate every test."""
    CredentialManager().clear_cache()
    yield
    CredentialManager().clear_cache()


def _connector(keys=None):
    """A connector with keys already loaded, skipping connect()'s verification."""
    c = StripeConnector()
    c._keys = keys if keys is not None else {"c3": KEY_C3, "c4": KEY_C4}
    c._is_connected = True
    return c


# ── _to_unix ──────────────────────────────────────────────────────────


def test_to_unix_passes_through_numbers():
    assert _to_unix(1765152000) == 1765152000
    assert _to_unix(1765152000.9) == 1765152000


def test_to_unix_treats_a_bare_date_as_utc_midnight():
    assert _to_unix(dt.date(2026, 8, 8)) == int(
        dt.datetime(2026, 8, 8, tzinfo=dt.timezone.utc).timestamp()
    )


def test_to_unix_end_of_day_covers_the_whole_day():
    """A `created[lte]` on a bare date must include the day, not just its first second."""
    start = _to_unix(dt.date(2026, 8, 8))
    end = _to_unix(dt.date(2026, 8, 8), end_of_day=True)
    assert end - start == 86399


def test_to_unix_assumes_utc_for_naive_datetimes():
    assert _to_unix(dt.datetime(2026, 8, 8, 0, 0)) == _to_unix(dt.date(2026, 8, 8))


def test_to_unix_respects_an_aware_datetime_offset():
    aware = dt.datetime(2026, 8, 8, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=-5)))
    assert _to_unix(aware) == _to_unix(dt.date(2026, 8, 8)) + 5 * 3600


def test_to_unix_rejects_junk():
    with pytest.raises(TypeError):
        _to_unix("2026-08-08")


# ── Credential handling ───────────────────────────────────────────────


def test_credentials_accept_a_json_map(monkeypatch):
    monkeypatch.setenv(
        "STRIPE_CREDENTIALS_PASSWORD", f'{{"c3":"{KEY_C3}","c4":"{KEY_C4}"}}'
    )
    keys = CredentialManager().get_stripe_credentials()
    assert keys == {"c3": KEY_C3, "c4": KEY_C4}


def test_credentials_accept_a_single_bare_key(monkeypatch):
    monkeypatch.setenv("STRIPE_CREDENTIALS_PASSWORD", KEY_C3)
    assert CredentialManager().get_stripe_credentials() == {"default": KEY_C3}


def test_credentials_reject_a_truncated_key(monkeypatch):
    """The guard that matters: Stripe answers a truncated key with the same 401
    as a revoked one, so nothing downstream would point at the paste."""
    monkeypatch.setenv("STRIPE_CREDENTIALS_PASSWORD", "rk_live_tooshort")
    with pytest.raises(CredentialError, match="truncated"):
        CredentialManager().get_stripe_credentials()


def test_credentials_reject_a_non_stripe_prefix(monkeypatch):
    monkeypatch.setenv("STRIPE_CREDENTIALS_PASSWORD", "x" * 120)
    with pytest.raises(CredentialError, match="does not look like"):
        CredentialManager().get_stripe_credentials()


def test_credentials_reject_bad_json(monkeypatch):
    monkeypatch.setenv("STRIPE_CREDENTIALS_PASSWORD", '{"c3": ')
    with pytest.raises(CredentialError, match="JSON"):
        CredentialManager().get_stripe_credentials()


def test_credentials_reject_an_empty_json_object(monkeypatch):
    monkeypatch.setenv("STRIPE_CREDENTIALS_PASSWORD", "{}")
    with pytest.raises(CredentialError, match="non-empty"):
        CredentialManager().get_stripe_credentials()


# ── Account resolution ────────────────────────────────────────────────


def test_account_is_required_when_several_are_configured():
    """Silently picking one would reconcile the wrong entity's money."""
    c = _connector()
    with pytest.raises(ValueError, match="account= is required"):
        c._key_for(None)


def test_account_may_be_omitted_when_only_one_is_configured():
    c = _connector({"default": KEY_C3})
    assert c._key_for(None) == KEY_C3


def test_unknown_account_names_the_configured_ones():
    c = _connector()
    with pytest.raises(ValueError, match="Unknown Stripe account"):
        c._key_for("nope")


def test_accounts_property_is_sorted():
    assert _connector().accounts == ["c3", "c4"]


def test_is_live_reads_the_key_prefix():
    c = _connector({"live": KEY_C3, "test": "rk_test_" + "c" * 100})
    assert c._is_live("live") is True
    assert c._is_live("test") is False


# ── HTTP behaviour ────────────────────────────────────────────────────


def test_uses_bearer_auth_for_the_right_account():
    c = _connector()
    with patch("requests.request", return_value=_make_response(json_data=ACCOUNT_OBJ)) as m:
        c.get_account("c4")
    assert m.call_args.kwargs["headers"]["Authorization"] == f"Bearer {KEY_C4}"


def test_pins_the_api_version_only_when_asked():
    with patch("requests.request", return_value=_make_response(json_data=ACCOUNT_OBJ)) as m:
        _connector().get_account("c3")
        assert "Stripe-Version" not in m.call_args.kwargs["headers"]

    c = StripeConnector(api_version="2024-06-20")
    c._keys, c._is_connected = {"c3": KEY_C3}, True
    with patch("requests.request", return_value=_make_response(json_data=ACCOUNT_OBJ)) as m:
        c.get_account("c3")
        assert m.call_args.kwargs["headers"]["Stripe-Version"] == "2024-06-20"


def test_401_says_a_truncated_key_looks_the_same():
    c = _connector()
    with patch("requests.request", return_value=_make_response(401, text="bad key")):
        with pytest.raises(AuthenticationError, match="truncated"):
            c.get_account("c3")


def test_403_names_the_missing_scope_and_does_not_retry():
    """A restricted key missing a permission is fixed in the dashboard, so
    retrying only delays the message that says so."""
    c = _connector()
    resp = _make_response(403, text="permission missing")
    with patch("requests.request", return_value=resp) as m:
        with pytest.raises(AuthenticationError, match="restricted key needs"):
            c.get_account("c3")
    assert m.call_count == 1


def test_429_raises_rate_limit_with_retry_after():
    c = _connector()
    resp = _make_response(429, headers={"Retry-After": "7"})
    with patch("requests.request", return_value=resp):
        with pytest.raises(RateLimitError) as e:
            c._request("GET", "/charges", account="c3")
    assert e.value.retry_after == 7


def test_other_errors_become_connection_errors():
    c = _connector()
    with patch("requests.request", return_value=_make_response(500, text="boom")):
        with pytest.raises(ConnectionError, match="500"):
            c._request("GET", "/charges", account="c3")


def test_network_failure_becomes_a_connection_error():
    c = _connector()
    with patch("requests.request", side_effect=requests.RequestException("no route")):
        with pytest.raises(ConnectionError, match="request failed"):
            c._request("GET", "/charges", account="c3")


# ── Pagination ────────────────────────────────────────────────────────


def test_paginate_follows_has_more_by_cursor():
    c = _connector()
    pages = [
        _make_response(json_data={"data": [{"id": "ch_1"}, {"id": "ch_2"}], "has_more": True}),
        _make_response(json_data={"data": [{"id": "ch_3"}], "has_more": False}),
    ]
    with patch("requests.request", side_effect=pages) as m:
        rows = c.list_charges(account="c3")
    assert [r["id"] for r in rows] == ["ch_1", "ch_2", "ch_3"]
    # the second call must resume after the last id of the first page
    assert m.call_args_list[1].kwargs["params"]["starting_after"] == "ch_2"


def test_paginate_stops_on_has_more_false_even_with_a_full_page():
    """A full page is not a signal to continue; only has_more is."""
    c = _connector()
    full = [{"id": f"ch_{i}"} for i in range(STRIPE_MAX_LIMIT)]
    with patch(
        "requests.request",
        return_value=_make_response(json_data={"data": full, "has_more": False}),
    ) as m:
        rows = c.list_charges(account="c3")
    assert len(rows) == STRIPE_MAX_LIMIT
    assert m.call_count == 1


def test_paginate_honours_limit():
    c = _connector()
    with patch(
        "requests.request",
        return_value=_make_response(
            json_data={"data": [{"id": f"ch_{i}"} for i in range(10)], "has_more": True}
        ),
    ):
        rows = c.list_charges(account="c3", limit=4)
    assert len(rows) == 4


def test_paginate_caps_limit_at_the_stripe_maximum():
    c = _connector()
    with patch(
        "requests.request", return_value=_make_response(json_data={"data": [], "has_more": False})
    ) as m:
        c.list_charges(account="c3", limit=5000)
    assert m.call_args.kwargs["params"]["limit"] == STRIPE_MAX_LIMIT


# ── Window filters ────────────────────────────────────────────────────


def test_window_sends_created_gte_and_lte():
    c = _connector()
    with patch(
        "requests.request", return_value=_make_response(json_data={"data": [], "has_more": False})
    ) as m:
        c.list_charges(start=dt.date(2026, 8, 8), end=dt.date(2026, 8, 15), account="c3")
    params = m.call_args.kwargs["params"]
    assert params["created[gte]"] == _to_unix(dt.date(2026, 8, 8))
    assert params["created[lte]"] == _to_unix(dt.date(2026, 8, 15), end_of_day=True)


def test_endpoints_hit_the_documented_paths():
    c = _connector()
    for method, path in (
        (c.list_charges, "/charges"),
        (c.list_refunds, "/refunds"),
        (c.list_disputes, "/disputes"),
        (c.list_payouts, "/payouts"),
        (c.list_balance_transactions, "/balance_transactions"),
    ):
        with patch(
            "requests.request",
            return_value=_make_response(json_data={"data": [], "has_more": False}),
        ) as m:
            method(account="c3")
        assert m.call_args.args[1] == f"{STRIPE_API_BASE}{path}"


# ── Balance transactions ──────────────────────────────────────────────


def test_balance_transactions_merge_and_dedupe_across_types():
    """Stripe filters one type per request, so several are fetched and merged."""
    c = _connector()
    by_type = {
        "charge": {"data": [{"id": "txn_1", "created": 20}], "has_more": False},
        "refund": {"data": [{"id": "txn_2", "created": 10}], "has_more": False},
    }
    def fake(method, url, **kw):
        return _make_response(json_data=by_type[kw["params"]["type"]])

    with patch("requests.request", side_effect=fake):
        rows = c.list_balance_transactions(account="c3", types=("charge", "refund"))
    # merged, deduped, and sorted oldest-first
    assert [r["id"] for r in rows] == ["txn_2", "txn_1"]


def test_balance_transactions_can_itemise_one_payout():
    c = _connector()
    with patch(
        "requests.request", return_value=_make_response(json_data={"data": [], "has_more": False})
    ) as m:
        c.list_balance_transactions(payout="po_1Abc", account="c3")
    assert m.call_args.kwargs["params"]["payout"] == "po_1Abc"


def test_balance_transactions_can_expand_the_source():
    c = _connector()
    with patch(
        "requests.request", return_value=_make_response(json_data={"data": [], "has_more": False})
    ) as m:
        c.list_balance_transactions(account="c3", expand_source=True)
    assert m.call_args.kwargs["params"]["expand[]"] == "data.source"


# ── Lifecycle ─────────────────────────────────────────────────────────


def test_connect_verifies_every_configured_key(monkeypatch):
    """A key revoked or scoped to the wrong account must fail at connect, not
    later in a sweep the caller already believes is complete."""
    monkeypatch.setenv(
        "STRIPE_CREDENTIALS_PASSWORD", f'{{"c3":"{KEY_C3}","c4":"{KEY_C4}"}}'
    )
    c = StripeConnector()
    with patch("requests.request", return_value=_make_response(json_data=ACCOUNT_OBJ)) as m:
        c.connect()
    assert c.is_connected() is True
    assert m.call_count == 2


def test_connect_fails_if_any_key_is_rejected(monkeypatch):
    monkeypatch.setenv(
        "STRIPE_CREDENTIALS_PASSWORD", f'{{"c3":"{KEY_C3}","c4":"{KEY_C4}"}}'
    )
    c = StripeConnector()
    responses = [_make_response(json_data=ACCOUNT_OBJ), _make_response(401, text="nope")]
    with patch("requests.request", side_effect=responses):
        with pytest.raises(AuthenticationError):
            c.connect()
    assert c.is_connected() is False


def test_disconnect_forgets_the_keys():
    c = _connector()
    c.disconnect()
    assert c.is_connected() is False
    assert c.accounts == []


def test_health_check_is_false_when_disconnected():
    c = StripeConnector()
    assert c.health_check() is False


def test_health_check_checks_all_accounts():
    c = _connector()
    with patch("requests.request", return_value=_make_response(json_data=ACCOUNT_OBJ)) as m:
        assert c.health_check() is True
    assert m.call_count == 2


def test_health_check_is_false_if_one_account_fails():
    c = _connector()
    responses = [_make_response(json_data=ACCOUNT_OBJ), _make_response(500, text="x")]
    with patch("requests.request", side_effect=responses):
        assert c.health_check() is False
