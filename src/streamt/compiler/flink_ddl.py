"""Flink SQL DDL generation with Kafka auth/SSL support."""

from __future__ import annotations

import re

from pydantic import SecretStr

from streamt.core.models import KafkaConfig

# Patterns for credentials embedded in Flink DDL WITH clauses
_DDL_SASL_JAAS = re.compile(
    r"('properties\.sasl\.jaas\.config'\s*=\s*)'([^']*)'",
    re.IGNORECASE,
)
_DDL_SSL_KEY_PASSWORD = re.compile(
    r"('properties\.ssl\.key\.password'\s*=\s*)'([^']*)'",
    re.IGNORECASE,
)


def redact_ddl_credentials(sql: str) -> str:
    """Redact credential values from Flink DDL SQL for safe storage on disk."""
    sql = _DDL_SASL_JAAS.sub(r"\1'***'", sql)
    sql = _DDL_SSL_KEY_PASSWORD.sub(r"\1'***'", sql)
    return sql


def _secret(val: str | SecretStr | None) -> str | None:
    """Resolve SecretStr to plain string."""
    if val is None:
        return None
    return val.get_secret_value() if isinstance(val, SecretStr) else val


# SASL mechanism → Java login module class
_JAAS_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "OAUTHBEARER": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
}


def kafka_with_properties(
    kafka: KafkaConfig,
    topic: str,
    bootstrap: str,
    extra: dict[str, str] | None = None,
    connector: str = "kafka",
) -> str:
    """Build a complete Flink WITH (...) clause for a Kafka connector.

    Includes bootstrap.servers, auth/SSL properties, format, and any extra kv pairs.
    """
    props: list[tuple[str, str]] = [
        ("connector", connector),
        ("topic", topic),
        ("properties.bootstrap.servers", bootstrap),
    ]

    # Auth / SSL properties
    props.extend(_kafka_auth_properties(kafka))

    # Extra properties (scan.startup.mode, etc.)
    if extra:
        props.extend(extra.items())

    # Format always last
    props.append(("format", "json"))

    lines = ",\n    ".join(f"'{k}' = '{v}'" for k, v in props)
    return f"WITH (\n    {lines}\n)"


def _kafka_auth_properties(kafka: KafkaConfig) -> list[tuple[str, str]]:
    """Return auth/SSL WITH clause entries for Kafka config.

    Returns empty list for PLAINTEXT (no auth) clusters.
    """
    if not kafka.security_protocol:
        return []

    props: list[tuple[str, str]] = []
    props.append(("properties.security.protocol", kafka.security_protocol))

    if kafka.sasl_mechanism:
        props.append(("properties.sasl.mechanism", kafka.sasl_mechanism))

        if kafka.sasl_username and kafka.sasl_password:
            module = _JAAS_MODULES.get(
                kafka.sasl_mechanism,
                "org.apache.kafka.common.security.plain.PlainLoginModule",
            )
            jaas = (
                f"{module} required "
                f'username="{kafka.sasl_username}" '
                f'password="{_secret(kafka.sasl_password)}";'
            )
            props.append(("properties.sasl.jaas.config", jaas))

    if kafka.ssl_ca_location:
        props.append(("properties.ssl.truststore.location", kafka.ssl_ca_location))

    if kafka.ssl_certificate_location:
        props.append(("properties.ssl.keystore.location", kafka.ssl_certificate_location))

    if kafka.ssl_key_location:
        props.append(("properties.ssl.key.location", kafka.ssl_key_location))

    if kafka.ssl_key_password:
        props.append(("properties.ssl.key.password", _secret(kafka.ssl_key_password)))

    return props
