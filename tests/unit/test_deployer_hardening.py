"""Tests for deployer production hardening.

Covers: retry logic, URL validation, closed-state guards,
error sanitization, JSON parse resilience.
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from streamt.deployer.flink import FlinkDeployer
from streamt.deployer.gateway import GatewayDeployer
from streamt.deployer.schema_registry import SchemaRegistryDeployer
from streamt.deployer.connect import ConnectDeployer
from streamt.deployer.planner import _sanitize_error


# ===================================================================
# URL Validation
# ===================================================================


class TestUrlValidation:

    def test_flink_rejects_empty_url(self):
        with pytest.raises(ValueError, match="Invalid Flink REST URL"):
            FlinkDeployer(rest_url="")

    def test_flink_rejects_missing_scheme(self):
        with pytest.raises(ValueError, match="must start with http"):
            FlinkDeployer(rest_url="localhost:8082")

    def test_flink_rejects_invalid_gateway_url(self):
        with pytest.raises(ValueError, match="Invalid SQL Gateway URL"):
            FlinkDeployer(rest_url="http://localhost:8082", sql_gateway_url="localhost:8084")

    def test_gateway_rejects_empty_url(self):
        with pytest.raises(ValueError, match="Invalid Gateway admin URL"):
            GatewayDeployer(admin_url="")

    def test_gateway_rejects_missing_scheme(self):
        with pytest.raises(ValueError, match="must start with http"):
            GatewayDeployer(admin_url="gw.example.com:8888")

    def test_sr_rejects_empty_url(self):
        with pytest.raises(ValueError, match="Invalid Schema Registry URL"):
            SchemaRegistryDeployer(url="")

    def test_connect_rejects_empty_url(self):
        with pytest.raises(ValueError, match="Invalid Connect REST URL"):
            ConnectDeployer(rest_url="")


# ===================================================================
# Retry on Timeout
# ===================================================================


class TestRetryOnTimeout:

    def test_flink_retries_on_timeout(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        ok = Mock(status_code=200, content=b'[]', json=Mock(return_value=[]))
        ok.raise_for_status = Mock()
        deployer._http_session.request = Mock(
            side_effect=[requests.Timeout("timed out"), ok]
        )
        with patch("streamt.deployer.flink.time.sleep"):
            result = deployer._request("GET", "/jobs/overview")
        assert result == []
        assert deployer._http_session.request.call_count == 2
        deployer.close()

    def test_sr_retries_on_timeout(self):
        deployer = SchemaRegistryDeployer(url="http://localhost:8081")
        ok = Mock(status_code=200, json=Mock(return_value=["s1"]))
        ok.raise_for_status = Mock()
        deployer._http_session.request = Mock(
            side_effect=[requests.Timeout("timed out"), ok]
        )
        with patch("streamt.deployer.schema_registry.time.sleep"):
            assert deployer.list_subjects() == ["s1"]


# ===================================================================
# Retry on HTTP 5xx
# ===================================================================


class TestRetryOnServerError:

    def test_flink_retries_on_503(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        err_resp = Mock(status_code=503, content=b'Service Unavailable')
        ok_resp = Mock(status_code=200, content=b'[]', json=Mock(return_value=[]))
        ok_resp.raise_for_status = Mock()
        deployer._http_session.request = Mock(side_effect=[err_resp, ok_resp])
        with patch("streamt.deployer.flink.time.sleep"):
            result = deployer._request("GET", "/jobs/overview")
        assert result == []
        assert deployer._http_session.request.call_count == 2
        deployer.close()


# ===================================================================
# Gateway closed-state guard
# ===================================================================


class TestGatewayClosedGuard:

    def test_request_after_close_raises(self):
        deployer = GatewayDeployer(admin_url="http://localhost:8888")
        deployer.close()
        with pytest.raises(RuntimeError, match="closed"):
            deployer._request("GET", "/interceptor")


# ===================================================================
# Error sanitization
# ===================================================================


class TestErrorSanitization:

    def test_strips_password(self):
        assert "***" in _sanitize_error("Connection failed password=s3cret host=db")
        assert "s3cret" not in _sanitize_error("Connection failed password=s3cret host=db")

    def test_strips_bearer_token(self):
        assert "***" in _sanitize_error("401 Authorization: Bearer eyJhbGciOi...")
        assert "eyJhbGciOi" not in _sanitize_error("401 Authorization: Bearer eyJhbGciOi...")

    def test_strips_api_key(self):
        result = _sanitize_error("api_key=AKIAIOSFODNN7EXAMPLE")
        assert "AKIAIOSFODNN7EXAMPLE" not in result

    def test_preserves_safe_messages(self):
        msg = "Topic 'events' not found on cluster"
        assert _sanitize_error(msg) == msg


# ===================================================================
# JSON parse resilience
# ===================================================================


class TestJsonParseResilience:

    def test_sr_handles_malformed_schema_json(self):
        deployer = SchemaRegistryDeployer(url="http://localhost:8081")
        resp = Mock(
            status_code=200,
            json=Mock(return_value={
                "schema": "NOT VALID JSON{{{",
                "version": 1,
                "id": 42,
                "schemaType": "AVRO",
            }),
        )
        resp.raise_for_status = Mock()
        deployer._http_session.request = Mock(return_value=resp)
        state = deployer.get_schema_state("test-value")
        assert state.exists
        assert state.schema == {}  # Fallback on parse error


# ===================================================================
# Configurable timeouts
# ===================================================================


class TestConfigurableTimeouts:

    def test_flink_custom_timeout(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082", timeout=5)
        assert deployer._timeout == 5
        deployer.close()

    def test_flink_custom_retries(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082", retries=5)
        assert deployer._retries == 5
        deployer.close()

    def test_flink_custom_statement_timeout(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082", statement_timeout=120)
        assert deployer._statement_timeout == 120
        deployer.close()

    def test_flink_defaults(self):
        deployer = FlinkDeployer(rest_url="http://localhost:8082")
        assert deployer._timeout == 30
        assert deployer._retries == 3
        assert deployer._statement_timeout == 60
        deployer.close()
