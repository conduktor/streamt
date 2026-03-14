"""Tests for URL format validation in runtime config models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streamt.core.runtime import (
    ConnectClusterConfig,
    FlinkClusterConfig,
    SchemaRegistryConfig,
)


class TestURLValidation:
    def test_sr_rejects_bare_hostname(self):
        with pytest.raises(ValidationError, match="http://"):
            SchemaRegistryConfig(url="my-registry:8081")

    def test_sr_accepts_http(self):
        sr = SchemaRegistryConfig(url="http://localhost:8081")
        assert sr.url == "http://localhost:8081"

    def test_sr_accepts_https(self):
        sr = SchemaRegistryConfig(url="https://registry.example.com")
        assert sr.url.startswith("https://")

    def test_flink_rejects_bare_hostname(self):
        with pytest.raises(ValidationError, match="http://"):
            FlinkClusterConfig(rest_url="flink:8081")

    def test_flink_accepts_http(self):
        fc = FlinkClusterConfig(rest_url="http://flink:8081")
        assert fc.rest_url == "http://flink:8081"

    def test_flink_none_url_ok(self):
        fc = FlinkClusterConfig()
        assert fc.rest_url is None

    def test_connect_rejects_bare_hostname(self):
        with pytest.raises(ValidationError, match="http://"):
            ConnectClusterConfig(rest_url="connect:8083")

    def test_connect_accepts_http(self):
        cc = ConnectClusterConfig(rest_url="http://connect:8083")
        assert cc.rest_url == "http://connect:8083"
