"""Tests for credential field protection.

Production scenario: User logs KafkaConfig or prints a project object.
Passwords, API keys, and other secrets must NOT appear in repr/str output.
"""

import pytest

from streamt.core.models import (
    ConnectClusterConfig,
    FlinkClusterConfig,
    GatewayConfig,
    KafkaConfig,
    SchemaRegistryConfig,
)


class TestCredentialFieldsNotInRepr:
    """Secret fields must not appear in repr() or str() output."""

    def test_kafka_password_hidden(self):
        cfg = KafkaConfig(
            bootstrap_servers="broker:9092",
            sasl_password="super-secret-pw",
            ssl_key_password="key-secret",
        )
        text = repr(cfg)
        assert "super-secret-pw" not in text
        assert "key-secret" not in text

    def test_kafka_password_accessible(self):
        """Hidden from repr but still accessible programmatically."""
        cfg = KafkaConfig(
            bootstrap_servers="broker:9092",
            sasl_password="super-secret-pw",
            ssl_key_password="key-secret",
        )
        # Must still be usable — SecretStr needs .get_secret_value()
        # or the field uses repr=False but remains a plain str
        pw = cfg.sasl_password
        assert pw is not None
        # The actual secret value must be retrievable
        secret_val = pw.get_secret_value() if hasattr(pw, "get_secret_value") else pw
        assert secret_val == "super-secret-pw"

    def test_schema_registry_password_hidden(self):
        cfg = SchemaRegistryConfig(url="http://sr:8081", password="sr-secret")
        text = repr(cfg)
        assert "sr-secret" not in text

    def test_flink_password_hidden(self):
        cfg = FlinkClusterConfig(
            rest_url="http://flink:8082",
            password="flink-pass",
            api_key="flink-api-key-123",
        )
        text = repr(cfg)
        assert "flink-pass" not in text
        assert "flink-api-key-123" not in text

    def test_connect_password_hidden(self):
        cfg = ConnectClusterConfig(
            rest_url="http://connect:8083",
            password="connect-secret",
        )
        text = repr(cfg)
        assert "connect-secret" not in text

    def test_gateway_password_hidden(self):
        cfg = GatewayConfig(
            admin_url="http://gw:8888",
            password="gw-secret",
        )
        text = repr(cfg)
        assert "gw-secret" not in text


class TestToConfluentConfigResolvesSecrets:
    """to_confluent_config() must return plain strings, not SecretStr objects."""

    def test_kafka_to_confluent_returns_plain_strings(self):
        cfg = KafkaConfig(
            bootstrap_servers="broker:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="secret-pw",
            ssl_key_password="key-pw",
        )
        result = cfg.to_confluent_config()
        # confluent_kafka requires plain str values, not SecretStr
        assert result["sasl.password"] == "secret-pw"
        assert result["ssl.key.password"] == "key-pw"
        assert isinstance(result["sasl.password"], str)
        assert isinstance(result["ssl.key.password"], str)


class TestGatewayNoHardcodedDefaults:
    """GatewayConfig must not ship with hardcoded credentials."""

    def test_no_default_username(self):
        cfg = GatewayConfig()
        assert cfg.username is None, "username must not have a hardcoded default"

    def test_no_default_password(self):
        cfg = GatewayConfig()
        assert cfg.password is None, "password must not have a hardcoded default"
