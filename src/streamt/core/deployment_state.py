"""Strict configuration for deployment ownership-state providers."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, StrictInt, StrictStr, TypeAdapter, model_validator

from streamt.core.base import StreamtBaseModel

_ENVIRONMENT_VARIABLE_NAME = r"^[A-Za-z_][A-Za-z0-9_]*$"
_POSTGRES_SCHEMA_NAME = r"^[A-Za-z_][A-Za-z0-9_]*$"


class LocalDeploymentStateConfig(StreamtBaseModel):
    """Configuration for the existing local version 1 JSON provider."""

    backend: Literal["local"]


class PostgresConnectionConfig(StreamtBaseModel):
    """Non-secret PostgreSQL provider configuration."""

    dsn_env: Annotated[StrictStr, Field(pattern=_ENVIRONMENT_VARIABLE_NAME)]
    writer_dsn_env: Annotated[
        StrictStr,
        Field(pattern=_ENVIRONMENT_VARIABLE_NAME),
    ] | None = None
    writer_role_env: Annotated[
        StrictStr,
        Field(pattern=_ENVIRONMENT_VARIABLE_NAME),
    ] | None = None
    schema_name: Annotated[
        StrictStr,
        Field(alias="schema", pattern=_POSTGRES_SCHEMA_NAME),
    ] = "streamt"

    @model_validator(mode="after")
    def validate_writer_dsn_is_distinct(self) -> PostgresConnectionConfig:
        """Keep runtime writer credentials distinct from administration credentials."""
        if self.writer_dsn_env is not None and self.writer_dsn_env == self.dsn_env:
            raise ValueError("writer_dsn_env must differ from dsn_env")
        return self


class PostgresDeploymentStateConfig(StreamtBaseModel):
    """Configuration boundary for the version-two-writer PostgreSQL provider."""

    backend: Literal["postgres"]
    namespace: Annotated[
        StrictStr,
        Field(min_length=1, pattern=r"^[^/]+$"),
    ]
    lock_timeout_seconds: Annotated[StrictInt, Field(ge=1, le=300)] = 30
    postgres: PostgresConnectionConfig


DeploymentStateConfig = Annotated[
    LocalDeploymentStateConfig | PostgresDeploymentStateConfig,
    Field(discriminator="backend"),
]

_DEPLOYMENT_STATE_ADAPTER: TypeAdapter[DeploymentStateConfig] = TypeAdapter(
    DeploymentStateConfig
)


def local_deployment_state_config() -> LocalDeploymentStateConfig:
    """Return the compatibility-default local provider configuration."""
    return LocalDeploymentStateConfig(backend="local")


def validate_deployment_state_config(value: object) -> DeploymentStateConfig:
    """Strictly validate one complete tagged provider block."""
    return _DEPLOYMENT_STATE_ADAPTER.validate_python(value)


class RemoteStateRequiredError(ValueError):
    """The effective environment policy forbids local ownership state."""


def enforce_remote_state_policy(
    config: DeploymentStateConfig,
    *,
    required: bool,
) -> None:
    """Reject local state when an environment explicitly requires remote state."""
    if required and config.backend == "local":
        raise RemoteStateRequiredError(
            "This environment requires remote deployment state, but the local "
            "deployment state backend is configured"
        )
