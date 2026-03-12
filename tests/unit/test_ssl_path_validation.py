"""Tests for SSL file path validation at parse time."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from streamt.core.models import ConnectClusterConfig, FlinkClusterConfig, KafkaConfig, SchemaRegistryConfig


class TestSslPathValidationWarning:
    """Non-existent SSL paths log warnings (default behavior)."""

    def test_nonexistent_ca_logs_warning(self, caplog):
        cfg = KafkaConfig(bootstrap_servers="kafka:9092", ssl_ca_location="/no/such/ca.pem")
        assert cfg.ssl_ca_location == "/no/such/ca.pem"
        assert "does not exist" in caplog.text

    def test_nonexistent_cert_logs_warning(self, caplog):
        cfg = SchemaRegistryConfig(url="http://sr:8081", ssl_certificate_location="/no/cert.pem")
        assert cfg.ssl_certificate_location == "/no/cert.pem"
        assert "does not exist" in caplog.text

    def test_nonexistent_key_logs_warning(self, caplog):
        cfg = FlinkClusterConfig(ssl_key_location="/no/key.pem")
        assert cfg.ssl_key_location == "/no/key.pem"
        assert "does not exist" in caplog.text

    def test_connect_nonexistent_ca_logs_warning(self, caplog):
        cfg = ConnectClusterConfig(rest_url="http://c:8083", ssl_ca_location="/no/ca.pem")
        assert cfg.ssl_ca_location == "/no/ca.pem"
        assert "does not exist" in caplog.text


class TestSslPathStrictMode:
    """With STREAMT_STRICT_SSL=1, non-existent paths raise ValidationError."""

    def test_strict_raises_for_nonexistent_ca(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            with pytest.raises(ValidationError, match="does not exist"):
                KafkaConfig(bootstrap_servers="kafka:9092", ssl_ca_location="/no/such/ca.pem")

    def test_strict_raises_for_nonexistent_cert(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            with pytest.raises(ValidationError, match="does not exist"):
                SchemaRegistryConfig(url="http://sr:8081", ssl_certificate_location="/no/cert.pem")

    def test_strict_raises_for_nonexistent_key(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            with pytest.raises(ValidationError, match="does not exist"):
                FlinkClusterConfig(ssl_key_location="/no/key.pem")


class TestSslPathEnvVarPassthrough:
    """Unresolved env var references skip validation entirely."""

    def test_env_var_ca_passes(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            cfg = KafkaConfig(bootstrap_servers="kafka:9092", ssl_ca_location="${SSL_CA_PATH}")
            assert cfg.ssl_ca_location == "${SSL_CA_PATH}"

    def test_env_var_cert_passes(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            cfg = SchemaRegistryConfig(url="http://sr:8081", ssl_certificate_location="${CERT}")
            assert cfg.ssl_certificate_location == "${CERT}"

    def test_env_var_key_passes(self):
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            cfg = ConnectClusterConfig(rest_url="http://c:8083", ssl_key_location="${KEY}")
            assert cfg.ssl_key_location == "${KEY}"


class TestSslPathExistingFile:
    """Existing files pass validation in all modes."""

    def test_existing_file_passes(self, tmp_path):
        ca = tmp_path / "ca.pem"
        ca.write_text("cert data")
        with patch.dict(os.environ, {"STREAMT_STRICT_SSL": "1"}):
            cfg = KafkaConfig(bootstrap_servers="kafka:9092", ssl_ca_location=str(ca))
            assert cfg.ssl_ca_location == str(ca)

    def test_none_passes(self):
        cfg = KafkaConfig(bootstrap_servers="kafka:9092")
        assert cfg.ssl_ca_location is None
