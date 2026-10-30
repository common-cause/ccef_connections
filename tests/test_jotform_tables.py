"""Tests for the Jotform Tables connector. All data is fabricated."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from ccef_connections.connectors.jotform_tables import (
    JOTFORM_BASE,
    JotformTablesConnector,
)
from ccef_connections.exceptions import (
    AuthenticationError,
    ConnectionError,
    RateLimitError,
)

TABLE = "123456789012345"


def _resp(status_code=200, json_data=None, text="", headers=None, bad_json=False):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.text = text
    resp.headers = headers or {}
    if bad_json:
        resp.json.side_effect = ValueError("not json")
    else:
        resp.json.return_value = json_data if json_data is not None else {}
    return resp


def _envelope(content, code=200, message="success"):
    return {"responseCode": code, "message": message, "content": content}


def _row(n):
    return {"id": str(6000000000000000000 + n), "form_id": TABLE,
            "created_at": "2026-11-03 07:41:40", "status": "ACTIVE",
            "answers": {"17": {"answer": "Opening report"}}}


@pytest.fixture
def connector():
    c = JotformTablesConnector()
    c.connect()
    c._session = MagicMock()
    return c


def _calls(connector):
    return [call.args[0] for call in connector._session.get.call_args_list]


class TestSession:
    def test_connect_needs_no_credential(self):
        c = JotformTablesConnector()
        c.connect()
        assert c.health_check()
        c.disconnect()
        assert not c.health_check()

    def test_the_table_page_is_opened_once_before_data_calls(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"), _resp(200, _envelope({})),
            _resp(200, _envelope({})),
        ]
        connector.get_questions(TABLE)
        connector.get_questions(TABLE)
        urls = _calls(connector)
        assert urls[0] == f"{JOTFORM_BASE}/tables/{TABLE}"
        assert urls.count(f"{JOTFORM_BASE}/tables/{TABLE}") == 1

    def test_data_calls_go_through_the_shared_table_with_a_referer(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"), _resp(200, _envelope({"17": {"type": "control_radio"}})),
        ]
        questions = connector.get_questions(TABLE)
        assert questions == {"17": {"type": "control_radio"}}
        call = connector._session.get.call_args_list[1]
        assert call.args[0] == f"{JOTFORM_BASE}/API/sheets/{TABLE}/form/{TABLE}/questions"
        assert call.kwargs["headers"]["Referer"] == f"{JOTFORM_BASE}/tables/{TABLE}"


class TestRows:
    def test_rows_page_until_a_short_page(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(200, _envelope([_row(i) for i in range(2)])),
            _resp(200, _envelope([_row(9)])),
        ]
        rows = connector.get_rows(TABLE, page_size=2)
        assert [r["id"][-1] for r in rows] == ["0", "1", "9"]
        offsets = [c.kwargs["params"]["offset"]
                   for c in connector._session.get.call_args_list[1:]]
        assert offsets == [0, 2]

    def test_created_after_is_a_server_side_filter_on_live_rows(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"), _resp(200, _envelope([])),
        ]
        connector.get_rows(TABLE, created_after="2026-01-01 00:00:00")
        call = connector._session.get.call_args_list[1]
        assert call.args[0] == (f"{JOTFORM_BASE}/API/sheets/{TABLE}/sheet/{TABLE}"
                                f"/view/{TABLE}/rows")
        assert json.loads(call.kwargs["params"]["filter"]) == {
            "status:ne": ["ARCHIVED", "DELETED"],
            "created_at:gt": "2026-01-01 00:00:00",
        }

    def test_rows_that_are_not_a_list_are_refused(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"), _resp(200, _envelope({"oops": 1})),
        ]
        with pytest.raises(ConnectionError):
            connector.get_rows(TABLE)


class TestErrors:
    def test_a_revoked_link_is_an_authentication_error(self, connector):
        """The envelope says 401 even where the status might not."""
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(401, _envelope("", 401, "You're not authorized to use (/sheet-id-view-id-rows)")),
        ]
        with pytest.raises(AuthenticationError, match="shared by link"):
            connector.get_rows(TABLE)

    def test_an_envelope_error_under_http_200_is_raised(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(200, _envelope("", 403, "forbidden")),
        ]
        with pytest.raises(AuthenticationError):
            connector.get_questions(TABLE)

    def test_the_cross_site_refusal_is_not_read_as_data(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(200, text="Cross-Site Requests not allowed!", bad_json=True),
        ]
        with pytest.raises(ConnectionError, match="non-JSON"):
            connector.get_questions(TABLE)

    def test_a_network_failure_is_a_connection_error(self, connector):
        connector._session.get.side_effect = requests.ConnectionError("down")
        with pytest.raises(ConnectionError):
            connector.get_questions(TABLE)

    @patch("tenacity.nap.time.sleep")
    def test_only_429_is_retried(self, _sleep, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(429, _envelope("", 429, "slow down")),
            _resp(200, _envelope({"1": {}})),
        ]
        assert connector.get_questions(TABLE) == {"1": {}}

    @patch("tenacity.nap.time.sleep")
    def test_401_is_not_retried(self, _sleep, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
            _resp(401, _envelope("", 401, "no")),
        ]
        with pytest.raises(AuthenticationError):
            connector.get_questions(TABLE)
        assert connector._session.get.call_count == 2

    def test_rate_limit_error_carries_retry_after(self, connector):
        connector._session.get.side_effect = [
            _resp(200, text="<html>"),
        ] + [_resp(429, _envelope("", 429, "slow"), headers={"Retry-After": "7"})] * 4
        with patch("tenacity.nap.time.sleep"), pytest.raises(RateLimitError) as err:
            connector.get_questions(TABLE)
        assert err.value.retry_after == 7
