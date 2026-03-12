"""Flink SQL DDL generation with Kafka auth/SSL support."""

from __future__ import annotations

from streamt.core.models import KafkaConfig

# SASL mechanism → Java login module class
_JAAS_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "OAUTHBEARER": "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
}


def kafka_with_properties(kafka: KafkaConfig, topic: str, bootstrap: str, extra: dict[str, str] | None = None) -> str:
    """Build a complete Flink WITH (...) clause for a Kafka connector.

    Includes bootstrap.servers, auth/SSL properties, format, and any extra kv pairs.
    """
    props: list[tuple[str, str]] = [
        ("connector", "kafka"),
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
                f'{module} required '
                f'username="{kafka.sasl_username}" '
                f'password="{kafka.sasl_password}";'
            )
            props.append(("properties.sasl.jaas.config", jaas))

    if kafka.ssl_ca_location:
        props.append(("properties.ssl.truststore.location", kafka.ssl_ca_location))

    if kafka.ssl_certificate_location:
        props.append(("properties.ssl.keystore.location", kafka.ssl_certificate_location))

    if kafka.ssl_key_location:
        props.append(("properties.ssl.key.location", kafka.ssl_key_location))

    if kafka.ssl_key_password:
        props.append(("properties.ssl.key.password", kafka.ssl_key_password))

    return props
