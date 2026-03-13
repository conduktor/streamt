"""Tests for KafkaConfig field validators.

Production scenario: User typos security_protocol as "SSL_SASL" or
sasl_mechanism as "plain" (lowercase). Should fail fast at parse time
with a clear error, not silently pass and fail at Kafka connection.
"""

import pytest

from streamt.core.models import KafkaConfig


class TestSecurityProtocolValidator:
    """security_protocol must be a valid Kafka protocol."""

    def test_valid_sasl_ssl(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", security_protocol="SASL_SSL")
        assert cfg.security_protocol == "SASL_SSL"

    def test_valid_sasl_plaintext(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", security_protocol="SASL_PLAINTEXT")
        assert cfg.security_protocol == "SASL_PLAINTEXT"

    def test_valid_ssl(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", security_protocol="SSL")
        assert cfg.security_protocol == "SSL"

    def test_valid_plaintext(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", security_protocol="PLAINTEXT")
        assert cfg.security_protocol == "PLAINTEXT"

    def test_none_is_valid(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092")
        assert cfg.security_protocol is None

    def test_invalid_protocol_rejected(self):
        with pytest.raises(ValueError, match="security_protocol"):
            KafkaConfig(bootstrap_servers="b:9092", security_protocol="SSL_SASL")

    def test_typo_rejected(self):
        with pytest.raises(ValueError, match="security_protocol"):
            KafkaConfig(bootstrap_servers="b:9092", security_protocol="sasl-ssl")

    def test_lowercase_auto_uppercased(self):
        """Lowercase input should be auto-uppercased, not rejected."""
        cfg = KafkaConfig(bootstrap_servers="b:9092", security_protocol="sasl_ssl")
        assert cfg.security_protocol == "SASL_SSL"


class TestSaslMechanismValidator:
    """sasl_mechanism must be a valid SASL mechanism."""

    def test_valid_plain(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="PLAIN")
        assert cfg.sasl_mechanism == "PLAIN"

    def test_valid_scram_sha_256(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="SCRAM-SHA-256")
        assert cfg.sasl_mechanism == "SCRAM-SHA-256"

    def test_valid_scram_sha_512(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="SCRAM-SHA-512")
        assert cfg.sasl_mechanism == "SCRAM-SHA-512"

    def test_valid_oauthbearer(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="OAUTHBEARER")
        assert cfg.sasl_mechanism == "OAUTHBEARER"

    def test_valid_gssapi(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="GSSAPI")
        assert cfg.sasl_mechanism == "GSSAPI"

    def test_none_is_valid(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092")
        assert cfg.sasl_mechanism is None

    def test_invalid_mechanism_rejected(self):
        with pytest.raises(ValueError, match="sasl_mechanism"):
            KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="DIGEST-MD5")

    def test_lowercase_auto_uppercased(self):
        cfg = KafkaConfig(bootstrap_servers="b:9092", sasl_mechanism="plain")
        assert cfg.sasl_mechanism == "PLAIN"


class TestSaslRequiresProtocol:
    """sasl_mechanism without a SASL protocol should warn or validate."""

    def test_sasl_mechanism_without_sasl_protocol_rejected(self):
        """Setting sasl_mechanism with security_protocol=SSL (no SASL) is a misconfiguration."""
        with pytest.raises(ValueError, match=r"sasl_mechanism.*requires.*SASL"):
            KafkaConfig(
                bootstrap_servers="b:9092",
                security_protocol="SSL",
                sasl_mechanism="PLAIN",
            )

    def test_sasl_mechanism_with_sasl_protocol_ok(self):
        cfg = KafkaConfig(
            bootstrap_servers="b:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
        )
        assert cfg.sasl_mechanism == "PLAIN"
