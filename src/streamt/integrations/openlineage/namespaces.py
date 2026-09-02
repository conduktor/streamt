"""Pure OpenLineage namespace resolution without environment or project access."""

from __future__ import annotations

from dataclasses import dataclass

from streamt.integrations.openlineage.events import (
    OpenLineageConstructionError,
    kafka_namespace_from_bootstrap,
    validate_kafka_namespace,
)


class OpenLineageNamespaceError(ValueError):
    """A safe namespace failure with a stable command-facing location."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True)
class OpenLineageNamespaces:
    """Exact namespaces selected for one OpenLineage export or command."""

    job: str
    kafka: str | None
    gateway: str | None


def resolve_openlineage_namespaces(
    *,
    job_namespace: str | None,
    kafka_namespace: str | None,
    gateway_namespace: str | None,
    kafka_bootstrap: str | None,
    gateway_bootstrap: str | None,
    require_kafka: bool,
    require_gateway: bool,
) -> OpenLineageNamespaces:
    """Validate explicit values and narrowly derive only required dataset namespaces.

    Option-versus-environment precedence belongs to the caller. Passing a non-``None``
    dataset namespace means the caller selected it explicitly; invalid selected values
    never fall through to bootstrap derivation.
    """
    job = _require_job_namespace(job_namespace)
    kafka = _resolve_dataset_namespace(
        selected=kafka_namespace,
        bootstrap=kafka_bootstrap,
        required=require_kafka,
        label="Kafka",
        selected_location="kafka_namespace",
        bootstrap_location="runtime.kafka.bootstrap_servers",
    )
    gateway = _resolve_dataset_namespace(
        selected=gateway_namespace,
        bootstrap=gateway_bootstrap,
        required=require_gateway,
        label="Gateway",
        selected_location="gateway_namespace",
        bootstrap_location="runtime.conduktor.gateway.proxy_bootstrap",
    )
    return OpenLineageNamespaces(job=job, kafka=kafka, gateway=gateway)


def _require_job_namespace(value: str | None) -> str:
    if value is None or not value.strip():
        raise OpenLineageNamespaceError(
            "OpenLineage job namespace must contain a non-whitespace character",
            location="job_namespace",
        )
    return value


def _resolve_dataset_namespace(
    *,
    selected: str | None,
    bootstrap: str | None,
    required: bool,
    label: str,
    selected_location: str,
    bootstrap_location: str,
) -> str | None:
    if selected is not None:
        try:
            return validate_kafka_namespace(selected)
        except OpenLineageConstructionError as error:
            raise OpenLineageNamespaceError(
                f"Invalid OpenLineage {label} namespace: {error}",
                location=selected_location,
            ) from error

    if not required:
        return None
    if bootstrap is None:
        raise OpenLineageNamespaceError(
            f"OpenLineage {label} namespace is required and cannot be derived",
            location=bootstrap_location,
        )
    try:
        return kafka_namespace_from_bootstrap(bootstrap)
    except OpenLineageConstructionError as error:
        raise OpenLineageNamespaceError(
            f"Cannot derive OpenLineage {label} namespace: {error}",
            location=bootstrap_location,
        ) from error
