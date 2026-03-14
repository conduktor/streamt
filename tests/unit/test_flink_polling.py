"""Tests for Flink polling HTTP status check (flink.py:_poll_statement)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from streamt.deployer.flink import FlinkDeployer


def _make_deployer() -> FlinkDeployer:
    """Create a FlinkDeployer bypassing __init__."""
    d = FlinkDeployer.__new__(FlinkDeployer)
    d.rest_url = "http://flink:8081"
    d.sql_gateway_url = "http://flink:8083"
    d._http_session = MagicMock()
    d._timeout = 30
    d._retries = 3
    d._closed = False
    d._sql_hashes = {}
    d._api_base = ""
    d._state_dir = None
    d._version = None
    d._environment = None
    d._statement_timeout = 60
    return d


class TestPollingHttpStatus:
    """Tests for HTTP status handling in _poll_statement error path."""

    def test_http_500_no_json(self):
        """HTTP 500 with no parseable JSON → error includes status code."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR"})
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.json.side_effect = ValueError("no json")
        d._http_session.get.return_value = resp

        with pytest.raises(RuntimeError, match="HTTP 500"):
            d._poll_statement("sid", "op", "SELECT 1", timeout=5)

    def test_http_404_with_errors(self):
        """HTTP 404 but response has errors array → uses error_list."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR"})
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        resp.json.return_value = {"errors": ["table not found"]}
        d._http_session.get.return_value = resp

        with pytest.raises(RuntimeError, match="table not found"):
            d._poll_statement("sid", "op", "SELECT 1", timeout=5)

    def test_http_200_with_errors(self):
        """HTTP 200 with errors in response body → uses error_list."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR"})
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = {"errors": ["division by zero"]}
        d._http_session.get.return_value = resp

        with pytest.raises(RuntimeError, match="division by zero"):
            d._poll_statement("sid", "op", "SELECT 1", timeout=5)

    def test_http_200_empty_errors(self):
        """HTTP 200, empty errors array → 'Unknown error'."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR"})
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = {"errors": []}
        d._http_session.get.return_value = resp

        with pytest.raises(RuntimeError, match="Unknown error"):
            d._poll_statement("sid", "op", "SELECT 1", timeout=5)

    def test_error_in_status_response(self):
        """Error message directly in status response → used as-is."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR", "error": "syntax error"})

        with pytest.raises(RuntimeError, match="syntax error"):
            d._poll_statement("sid", "op", "BAD SQL", timeout=5)

    def test_fetch_details_exception(self):
        """HTTP get raises exception → fallback error message."""
        d = _make_deployer()
        d._request = MagicMock(return_value={"status": "ERROR"})
        d._http_session.get.side_effect = ConnectionError("refused")

        with pytest.raises(RuntimeError, match="failed to fetch details"):
            d._poll_statement("sid", "op", "SELECT 1", timeout=5)
