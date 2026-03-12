"""Shared CLI helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

import click

from streamt.output import OutputFormatter, StructuredError, get_output_format_from_context


def get_project_path(project_dir: Optional[str]) -> Path:
    """Get the project path."""
    if project_dir:
        return Path(project_dir).resolve()
    return Path.cwd()


def make_formatter(ctx: click.Context, command: str) -> OutputFormatter:
    """Create an OutputFormatter from Click context."""
    fmt = OutputFormatter(get_output_format_from_context(ctx))
    fmt.set_command(command)
    return fmt


def handle_parse_error(fmt: OutputFormatter, e: Exception, code: str) -> None:
    """Handle a parse/env/environment error uniformly."""
    fmt.add_error(StructuredError(code=code, message=str(e)))
    fmt.print_error(str(e))
    fmt.flush()
    sys.exit(1)


def _resolve_secret(val: Any) -> Any:
    """Resolve SecretStr to plain string for deployer constructors."""
    if val is None:
        return None
    return val.get_secret_value() if hasattr(val, "get_secret_value") else val


def make_kafka_deployer(project: Any, fmt: OutputFormatter) -> Any:
    """Create KafkaDeployer from project config. Returns None on failure."""
    from streamt.deployer.kafka import KafkaDeployer
    try:
        confluent_config = project.runtime.kafka.to_confluent_config()
        bootstrap = confluent_config.pop("bootstrap.servers")
        return KafkaDeployer(bootstrap, **confluent_config)
    except Exception as e:
        fmt.print_warning(f"Cannot connect to Kafka: {e}")
    return None


def make_sr_deployer(project: Any, fmt: OutputFormatter) -> Any:
    """Create SchemaRegistryDeployer from project config. Returns None on failure."""
    from streamt.deployer.schema_registry import SchemaRegistryDeployer
    if project.runtime.schema_registry:
        try:
            sr = project.runtime.schema_registry
            return SchemaRegistryDeployer(
                sr.url,
                username=sr.username,
                password=_resolve_secret(sr.password),
                ssl_ca_location=sr.ssl_ca_location,
                ssl_certificate_location=sr.ssl_certificate_location,
                ssl_key_location=sr.ssl_key_location,
            )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Schema Registry: {e}")
    return None


def make_flink_deployer(project: Any, fmt: OutputFormatter) -> Any:
    """Create FlinkDeployer from project config. Returns None on failure."""
    from streamt.deployer.flink import FlinkDeployer
    if project.runtime.flink and project.runtime.flink.clusters:
        try:
            default = project.runtime.flink.default
            if default and default in project.runtime.flink.clusters:
                cfg = project.runtime.flink.clusters[default]
                if cfg.rest_url:
                    return FlinkDeployer(
                        rest_url=cfg.rest_url,
                        sql_gateway_url=cfg.sql_gateway_url,
                        username=cfg.username,
                        password=_resolve_secret(cfg.password),
                        api_key=_resolve_secret(cfg.api_key),
                        ssl_ca_location=cfg.ssl_ca_location,
                        ssl_certificate_location=cfg.ssl_certificate_location,
                        ssl_key_location=cfg.ssl_key_location,
                    )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Flink: {e}")
    return None


def make_connect_deployer(project: Any, fmt: OutputFormatter) -> Any:
    """Create ConnectDeployer from project config. Returns None on failure."""
    from streamt.deployer.connect import ConnectDeployer
    if project.runtime.connect and project.runtime.connect.clusters:
        try:
            default = project.runtime.connect.default
            if default and default in project.runtime.connect.clusters:
                cfg = project.runtime.connect.clusters[default]
                return ConnectDeployer(
                    cfg.rest_url,
                    username=cfg.username,
                    password=_resolve_secret(cfg.password),
                    ssl_ca_location=cfg.ssl_ca_location,
                    ssl_certificate_location=cfg.ssl_certificate_location,
                    ssl_key_location=cfg.ssl_key_location,
                )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Connect: {e}")
    return None
