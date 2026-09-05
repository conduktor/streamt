"""Runtime configuration models for streamt (Kafka, SR, Flink, Connect, Gateway)."""

from __future__ import annotations

import re
from typing import Literal, Optional

from pydantic import Field, SecretStr, field_validator, model_validator

from streamt.core.base import StreamtBaseModel as BaseModel
from streamt.core.validators import validate_ssl_path as _validate_ssl_path

_VALID_SECURITY_PROTOCOLS = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
_VALID_SASL_MECHANISMS = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER", "GSSAPI"}


def _validate_http_url(v: str, field_name: str = "url") -> str:
    """Validate that a string is a non-empty HTTP(S) URL."""
    if not v or not v.strip():
        raise ValueError(f"{field_name} must not be empty")
    if not v.startswith(("http://", "https://")):
        raise ValueError(f"{field_name} must start with http:// or https://")
    return v


class KafkaConfig(BaseModel):
    """Kafka cluster configuration."""

    bootstrap_servers: str
    bootstrap_servers_internal: Optional[str] = None

    @field_validator("bootstrap_servers", mode="before")
    @classmethod
    def validate_bootstrap_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("bootstrap_servers must not be empty")
        return v

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
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
        mode="before",
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
                result[confluent_key] = (
                    val.get_secret_value() if isinstance(val, SecretStr) else val
                )
        return result


class SchemaRegistryConfig(BaseModel):
    """Schema Registry configuration."""

    url: str

    @field_validator("url", mode="before")
    @classmethod
    def validate_url_format(cls, v: str) -> str:
        return _validate_http_url(v, "Schema Registry url")

    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
        mode="before",
    )(_validate_ssl_path)


class FlinkClusterConfig(BaseModel):
    """Flink cluster configuration."""

    type: str = "rest"
    rest_url: Optional[str] = None
    sql_gateway_url: Optional[str] = None

    @field_validator("rest_url", "sql_gateway_url", mode="before")
    @classmethod
    def validate_urls_format(cls, v: str | None) -> str | None:
        if v is not None:
            _validate_http_url(v, "Flink URL")
        return v

    version: Optional[str] = None
    environment: Optional[str] = None
    api_key: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None
    timeout: Optional[int] = None
    retries: Optional[int] = None
    statement_timeout: Optional[int] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
        mode="before",
    )(_validate_ssl_path)


class FlinkConfig(BaseModel):
    """Flink runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, FlinkClusterConfig] = Field(default_factory=dict)


class ConnectClusterConfig(BaseModel):
    """Connect cluster configuration."""

    rest_url: str

    @field_validator("rest_url", mode="before")
    @classmethod
    def validate_rest_url_format(cls, v: str) -> str:
        return _validate_http_url(v, "Connect rest_url")

    username: Optional[str] = None
    password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
        mode="before",
    )(_validate_ssl_path)


class ConnectConfig(BaseModel):
    """Connect runtime configuration."""

    default: Optional[str] = None
    clusters: dict[str, ConnectClusterConfig] = Field(default_factory=dict)


class GatewayConfig(BaseModel):
    """Conduktor Gateway configuration."""

    admin_url: Optional[str] = None
    proxy_bootstrap: Optional[str] = None

    @field_validator("admin_url", mode="before")
    @classmethod
    def validate_admin_url_format(cls, v: str | None) -> str | None:
        if v is not None:
            _validate_http_url(v, "Gateway admin_url")
        return v

    username: Optional[str] = None
    password: Optional[SecretStr] = None
    virtual_cluster: Optional[str] = None
    ssl_ca_location: Optional[str] = None
    ssl_certificate_location: Optional[str] = None
    ssl_key_location: Optional[str] = None
    ssl_key_password: Optional[SecretStr] = None

    _check_ssl_paths = field_validator(
        "ssl_ca_location",
        "ssl_certificate_location",
        "ssl_key_location",
        mode="before",
    )(_validate_ssl_path)


class ConsoleConfig(BaseModel):
    """Conduktor Console configuration."""

    url: str
    api_key: Optional[str] = None

    @field_validator("url", mode="before")
    @classmethod
    def validate_url_format(cls, v: str) -> str:
        return _validate_http_url(v, "Console url")


class ConduktorConfig(BaseModel):
    """Conduktor configuration (Gateway + Console)."""

    gateway: Optional[GatewayConfig] = None
    console: Optional[ConsoleConfig] = None


class KafkaStreamsConfig(BaseModel):
    """Bounded local Docker runner configuration; never a remote scheduler."""

    backend: Literal["docker"] = "docker"
    image: str
    network: str = "bridge"
    initial_offset: Literal["earliest", "latest"] = "earliest"
    startup_timeout: int = Field(default=60, ge=1, le=300, strict=True)
    stop_timeout: int = Field(default=30, ge=1, le=300, strict=True)

    @field_validator("image")
    @classmethod
    def validate_immutable_image(cls, value: str) -> str:
        """Require a local image ID or an immutable repository digest."""
        if "://" in value or "//" in value or not re.fullmatch(
            r"(?:sha256:[0-9a-f]{64}|[A-Za-z0-9][A-Za-z0-9._:/-]*@sha256:[0-9a-f]{64})",
            value,
        ):
            raise ValueError("Kafka Streams image must be an immutable sha256 ID or repository digest")
        return value

    @field_validator("network")
    @classmethod
    def validate_network(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
            raise ValueError("Kafka Streams network must be one explicit Docker network name")
        return value


class RuntimeConfig(BaseModel):
    """Runtime configuration for all external systems."""

    kafka: KafkaConfig
    schema_registry: Optional[SchemaRegistryConfig] = None
    flink: Optional[FlinkConfig] = None
    connect: Optional[ConnectConfig] = None
    conduktor: Optional[ConduktorConfig] = None
    kafka_streams: Optional[KafkaStreamsConfig] = None
