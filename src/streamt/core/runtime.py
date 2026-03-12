"""Runtime configuration models for streamt (Kafka, SR, Flink, Connect, Gateway)."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator

from streamt.core.validators import validate_ssl_path as _validate_ssl_path

_VALID_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
_VALID_SASL_MECHANISMS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"}


class KafkaConfig(BaseModel):
    """Kafka cluster configuration."""

    bootstrap_servers: str
    bootstrap_servers_internal: Optional[str] = None
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    @field_validator("security_protocol", mode="before")
    @classmethod
    def validate_security_protocol(cls, v: str | None) -> str | None:
        if v is None:
            return None
        upper = v.upper()
        if upper not in _VALID_SECURITY_PROTOCOLS:
            raise ValueError(
                f"security_protocol must be one of {sorted(_VALID_SECURITY_PROTOCOLS)}, got '{v}'"
            )
        return upper

    @field_validator("sasl_mechanism", mode="before")
    @classmethod
    def validate_sasl_mechanism(cls, v: str | None) -> str | None:
        if v is None:
            return None
        upper = v.upper()
        if upper not in _VALID_SASL_MECHANISMS:
            raise ValueError(
                f"sasl_mechanism must be one of {sorted(_VALID_SASL_MECHANISMS)}, got '{v}'"
            )
        return upper

    _check_ssl_paths = field_validator(
        "ssl_ca_location", "ssl_certificate_location", "ssl_key_location", mode="before",
    )(_validate_ssl_path)

    @model_validator(mode="after")
    def validate_sasl_requires_protocol(self) -> KafkaConfig:
        if self.sasl_mechanism and self.security_protocol:
            if "SASL" not in self.security_protocol:
                raise ValueError(
                    f"sasl_mechanism='{self.sasl_mechanism}' requires a SASL security protocol "
                    f"(SASL_SSL or SASL_PLAINTEXT), got '{self.security_protocol}'"
                )
        return self

    def to_confluent_config(self) -> dict[str, str]:
        """Return confluent_kafka config dict with only non-None values."""
        mapping = {
            "bootstrap_servers": "bootstrap.servers",
            "security_protocol": "security.protocol",
            "sasl_mechanism": "sasl.mechanism",
            "sasl_username": "sasl.username",
            "sasl_password": "sasl.password",
            "ssl_ca_location": "ssl.ca.location",
            "ssl_certificate_location": "ssl.certificate.location",
            "ssl_key_location": "ssl.key.location",
            "ssl_key_password": "ssl.key.password",
        }
        result = {}
        for field, confluent_key in mapping.items():
            val = getattr(self, field)
            if val is not None:
                result[confluent_key] = val.get_secret_value() if isinstance(val, SecretStr) else val
        return result


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""

    url: str
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location", "ssl_certificate_location", "ssl_key_location", mode="before",
    )(_validate_ssl_path)


class FlinkClusterConfig(BaseModel):
    """Flink cluster configuration."""

    type: str = "rest"
    rest_url: Optional[str] = None
    sql_gateway_url: Optional[str] = None
    version: Optional[str] = None
    environment: Optional[str] = None
    api_key: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location", "ssl_certificate_location", "ssl_key_location", mode="before",
    )(_validate_ssl_path)


class FlinkConfig(BaseModel):
    """Flink runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, FlinkClusterConfig] = Field(default_factory=dict)


class ConnectClusterConfig(BaseModel):
    """Connect cluster configuration."""

    rest_url: str
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location", "ssl_certificate_location", "ssl_key_location", mode="before",
    )(_validate_ssl_path)


class ConnectConfig(BaseModel):
    """Connect runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, ConnectClusterConfig] = Field(default_factory=dict)


class GatewayConfig(BaseModel):
    """Conduktor Gateway configuration."""

    admin_url: Optional[str] = None
    proxy_bootstrap: Optional[str] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    virtual_cluster: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location", "ssl_certificate_location", "ssl_key_location", mode="before",
    )(_validate_ssl_path)


class ConsoleConfig(BaseModel):
    """Conduktor Console configuration."""

    url: str
    api_key: Optional[str] = None


class ConduktorConfig(BaseModel):
    """Conduktor configuration (Gateway + Console)."""

    gateway: Optional[GatewayConfig] = None
    console: Optional[ConsoleConfig] = None


class RuntimeConfig(BaseModel):
    """Runtime configuration for all external systems."""

    kafka: KafkaConfig
    schema_registry: Optional[SchemaRegistryConfig] = None
    flink: Optional[FlinkConfig] = None
    connect: Optional[ConnectConfig] = None
    conduktor: Optional[ConduktorConfig] = None
