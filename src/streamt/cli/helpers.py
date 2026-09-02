"""Shared CLI helpers."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Optional, TypeVar
from uuid import UUID

import click
from pydantic import SecretStr

from streamt.core.redaction import redact_sensitive_text as _redact_sensitive_text
from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context

if TYPE_CHECKING:
    from streamt.core.models import StreamtProject
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.gateway import GatewayDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

_DeployerT = TypeVar("_DeployerT")

# Preserve the existing helper import surface while keeping the implementation
# in a lower-level module that deployers and observers can use safely.
redact_sensitive_text = _redact_sensitive_text


def state_operation_error_details(error: object) -> tuple[str, str | None]:
    """Return a redacted message plus durable operation recovery evidence."""
    message = redact_sensitive_text(error)
    raw_operation_id = getattr(error, "operation_id", None)
    if not isinstance(raw_operation_id, str) or not raw_operation_id:
        return message, None
    try:
        if str(UUID(raw_operation_id)) != raw_operation_id:
            return message, None
    except ValueError:
        return message, None
    operation_id = redact_sensitive_text(raw_operation_id)
    return f"{message} Operation ID: {operation_id}.", operation_id


def get_project_path(project_dir: Optional[str]) -> Path:
    """Get the project path."""
    if project_dir:
        return Path(project_dir).resolve()
    return Path.cwd()


def make_formatter(ctx: click.Context, command: str) -> OutputFormatter:
    """Create an OutputFormatter from Click context."""
    root = ctx
    while root.parent is not None:
        root = root.parent
    quiet = (root.obj or {}).get("quiet", False)
    fmt = OutputFormatter(get_output_format_from_context(ctx), quiet=quiet)
    fmt.set_command(command)
    return fmt


def handle_parse_error(fmt: OutputFormatter, e: Exception, code: str) -> None:
    """Handle a parse/env/environment error uniformly."""
    fmt.add_error(StructuredError(code=code, message=str(e)))
    fmt.print_error(str(e))
    fmt.flush()
    sys.exit(1)


def close_deployers(*deployers: object) -> None:
    """Close all non-None deployers, logging errors."""
    import logging

    logger = logging.getLogger(__name__)
    for d in deployers:
        if d is not None and hasattr(d, "close"):
            try:
                d.close()
            except Exception as e:
                logger.debug(
                    "Failed to close deployer %s: %s",
                    type(d).__name__,
                    redact_sensitive_text(e),
                )


def _classify_connection_error(e: Exception, service: str) -> tuple[str, str]:
    """Classify a deployer exception into (error_code, actionable_message)."""
    from streamt.core.errors import ErrorCode

    msg = str(e).lower()
    safe_error = redact_sensitive_text(e)

    # SSL/TLS errors
    if any(kw in msg for kw in ("ssl", "certificate", "tls", "handshake")):
        return (
            ErrorCode.SSL_ERROR,
            f"SSL/TLS error connecting to {service}: {safe_error}. "
            f"Check ssl_ca_location, ssl_certificate_location, and ssl_key_location in your runtime config.",
        )

    # Auth errors (HTTP 401/403 or SASL failures)
    if any(
        kw in msg for kw in ("401", "403", "unauthorized", "forbidden", "authentication", "sasl")
    ):
        return (
            ErrorCode.AUTH_FAILED,
            f"Authentication failed for {service}: {safe_error}. "
            f"Check your username/password or API key in your runtime config.",
        )

    # Connection refused / unreachable
    if any(
        kw in msg
        for kw in (
            "connection refused",
            "connect timeout",
            "name or service not known",
            "no route",
            "unreachable",
        )
    ):
        return (
            ErrorCode.CONNECTION_REFUSED,
            f"Cannot reach {service}: {safe_error}. Check that the URL/bootstrap_servers is correct and the service is running.",
        )

    # Fallback
    return ("", f"Cannot connect to {service}: {safe_error}")


def _warn_deployer_error(fmt: OutputFormatter, e: Exception, service: str) -> None:
    """Emit a structured warning for a deployer connection error."""
    code, message = _classify_connection_error(e, service)
    if code:
        fmt.add_error(StructuredError(code=code, message=message))
    fmt.print_warning(message)


def _resolve_secret(val: SecretStr | str | None) -> str | None:
    """Resolve SecretStr to plain string for deployer constructors."""
    return val.get_secret_value() if isinstance(val, SecretStr) else val


def check_required_deployers(
    project: StreamtProject,
    kafka_deployer: Optional[KafkaDeployer],
    sr_deployer: Optional[SchemaRegistryDeployer],
    flink_deployer: Optional[FlinkDeployer],
    connect_deployer: Optional[ConnectDeployer],
    gateway_deployer: Optional[GatewayDeployer],
    fmt: OutputFormatter,
) -> bool:
    """Return False (with errors in fmt) if any configured deployer failed to connect."""
    from streamt.core.errors import ErrorCode

    rt = project.runtime
    checks: list[tuple[bool, Optional[object], str]] = [
        (bool(rt.kafka), kafka_deployer, "Kafka"),
        (bool(rt.schema_registry), sr_deployer, "Schema Registry"),
        (bool(rt.flink and getattr(rt.flink, "clusters", None)), flink_deployer, "Flink"),
        (
            bool(rt.connect and getattr(rt.connect, "clusters", None)),
            connect_deployer,
            "Kafka Connect",
        ),
        (
            bool(rt.conduktor and getattr(rt.conduktor, "gateway", None)),
            gateway_deployer,
            "Conduktor Gateway",
        ),
    ]

    ok = True
    for is_configured, deployer, name in checks:
        if is_configured and deployer is None:
            fmt.add_error(
                StructuredError(
                    code=ErrorCode.CONNECTION_REFUSED,
                    message=f"{name} is configured but unreachable. Cannot proceed.",
                )
            )
            fmt.print_error(
                f"{name} is configured but unreachable. Cannot proceed with plan/apply."
            )
            ok = False
    return ok


def _try_create_deployer(
    create_fn: Callable[[], _DeployerT], fmt: OutputFormatter, service: str
) -> Optional[_DeployerT]:
    """Run create_fn(), warn on failure, return None on exception."""
    try:
        return create_fn()
    except Exception as e:
        _warn_deployer_error(fmt, e, service)
    return None


def make_kafka_deployer(project: StreamtProject, fmt: OutputFormatter) -> Optional[KafkaDeployer]:
    """Create KafkaDeployer from project config. Returns None on failure."""
    from streamt.deployer.kafka import KafkaDeployer

    def _create() -> KafkaDeployer:
        cfg = project.runtime.kafka.to_confluent_config()
        bootstrap = cfg.pop("bootstrap.servers")
        return KafkaDeployer(bootstrap, **cfg)

    return _try_create_deployer(_create, fmt, "Kafka")


def make_sr_deployer(
    project: StreamtProject,
    fmt: OutputFormatter,
    *,
    required: bool = True,
) -> Optional[SchemaRegistryDeployer]:
    """Create SchemaRegistryDeployer from project config.

    Optional callers can request warning-only failure handling when Schema
    Registry enriches a result but is not required for the primary operation.
    """
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    if not project.runtime.schema_registry:
        return None
    sr = project.runtime.schema_registry

    def _create() -> SchemaRegistryDeployer:
        return SchemaRegistryDeployer(
            sr.url,
            username=sr.username,
            password=_resolve_secret(sr.password),
            ssl_ca_location=sr.ssl_ca_location,
            ssl_certificate_location=sr.ssl_certificate_location,
            ssl_key_location=sr.ssl_key_location,
            ssl_key_password=_resolve_secret(sr.ssl_key_password),
        )

    if required:
        return _try_create_deployer(_create, fmt, "Schema Registry")
    try:
        return _create()
    except Exception as e:
        from streamt.core.errors import ErrorCode

        _code, message = _classify_connection_error(e, "Schema Registry")
        fmt.print_warning(message, code=ErrorCode.SCHEMA_ENRICHMENT_SKIPPED)
        return None


def make_flink_deployer(
    project: StreamtProject,
    fmt: OutputFormatter,
    state_dir: Optional[Path] = None,
) -> Optional[FlinkDeployer]:
    """Create FlinkDeployer from project config. Returns None on failure."""
    from streamt.deployer.flink import FlinkDeployer

    if not (project.runtime.flink and project.runtime.flink.clusters):
        return None
    default = project.runtime.flink.default
    if not (default and default in project.runtime.flink.clusters):
        return None
    cfg = project.runtime.flink.clusters[default]
    rest_url = cfg.rest_url
    if not rest_url:
        return None

    def _create() -> FlinkDeployer:
        return FlinkDeployer(
            rest_url=rest_url,
            sql_gateway_url=cfg.sql_gateway_url,
            username=cfg.username,
            password=_resolve_secret(cfg.password),
            api_key=_resolve_secret(cfg.api_key),
            ssl_ca_location=cfg.ssl_ca_location,
            ssl_certificate_location=cfg.ssl_certificate_location,
            ssl_key_location=cfg.ssl_key_location,
            ssl_key_password=_resolve_secret(cfg.ssl_key_password),
            version=cfg.version,
            environment=cfg.environment,
            state_dir=state_dir,
            timeout=cfg.timeout,
            retries=cfg.retries,
            statement_timeout=cfg.statement_timeout,
        )

    return _try_create_deployer(_create, fmt, "Flink")


def make_gateway_deployer(
    project: StreamtProject, fmt: OutputFormatter
) -> Optional[GatewayDeployer]:
    """Create GatewayDeployer from project config. Returns None on failure."""
    from streamt.deployer.gateway import GatewayDeployer

    if not (project.runtime.conduktor and project.runtime.conduktor.gateway):
        return None
    gw = project.runtime.conduktor.gateway
    admin_url = gw.admin_url
    if not admin_url:
        return None

    def _create() -> GatewayDeployer:
        return GatewayDeployer(
            admin_url,
            username=gw.username,
            password=_resolve_secret(gw.password),
            virtual_cluster=gw.virtual_cluster,
            ssl_ca_location=gw.ssl_ca_location,
            ssl_certificate_location=gw.ssl_certificate_location,
            ssl_key_location=gw.ssl_key_location,
            ssl_key_password=_resolve_secret(gw.ssl_key_password),
        )

    return _try_create_deployer(_create, fmt, "Conduktor Gateway")


def make_connect_deployer(
    project: StreamtProject, fmt: OutputFormatter
) -> Optional[ConnectDeployer]:
    """Create ConnectDeployer from project config. Returns None on failure."""
    from streamt.deployer.connect import ConnectDeployer

    if not (project.runtime.connect and project.runtime.connect.clusters):
        return None
    default = project.runtime.connect.default
    if not (default and default in project.runtime.connect.clusters):
        return None
    cfg = project.runtime.connect.clusters[default]

    def _create() -> ConnectDeployer:
        return ConnectDeployer(
            cfg.rest_url,
            cluster_alias=default,
            username=cfg.username,
            password=_resolve_secret(cfg.password),
            ssl_ca_location=cfg.ssl_ca_location,
            ssl_certificate_location=cfg.ssl_certificate_location,
            ssl_key_location=cfg.ssl_key_location,
            ssl_key_password=_resolve_secret(cfg.ssl_key_password),
        )

    return _try_create_deployer(_create, fmt, "Kafka Connect")
