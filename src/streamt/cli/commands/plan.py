"""streamt plan command."""

from __future__ import annotations

import sys
from typing import Any, Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.errors import ErrorCode
from streamt.output import StructuredError


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.pass_context
def plan(ctx: click.Context, project_dir: Optional[str], environment: Optional[str]) -> None:
    """Show what would change on apply."""
    from streamt.compiler import Compiler
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.planner import DeploymentPlanner

    fmt = make_formatter(ctx, "plan")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path, environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()

        validator = ProjectValidator(project)
        result = validator.validate()
        if not result.is_valid:
            for error in result.errors:
                fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=error.message))
                fmt.print_error(error.message)
            fmt.flush()
            sys.exit(1)

        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Create deployers
        sr_deployer = _make_sr_deployer(project, fmt)
        kafka_deployer = _make_kafka_deployer(project, fmt)
        flink_deployer = _make_flink_deployer(project, fmt)
        connect_deployer = _make_connect_deployer(project, fmt)

        planner = DeploymentPlanner(
            manifest,
            schema_registry_deployer=sr_deployer,
            kafka_deployer=kafka_deployer,
            flink_deployer=flink_deployer,
            connect_deployer=connect_deployer,
        )
        deployment_plan = planner.plan()

        # Build structured changes list
        changes: list[dict[str, Any]] = []
        for c in deployment_plan.schema_changes:
            if c.action != "none":
                changes.append({"type": "schema", "name": c.subject, "action": c.action, "changes": c.changes})
        for c in deployment_plan.topic_changes:
            if c.action != "none":
                changes.append({"type": "topic", "name": c.topic, "action": c.action, "changes": c.changes})
        for c in deployment_plan.flink_changes:
            if c.action != "none":
                changes.append({"type": "flink_job", "name": c.job_name, "action": c.action})
        for c in deployment_plan.connector_changes:
            if c.action != "none":
                changes.append({"type": "connector", "name": c.connector_name, "action": c.action, "changes": c.changes})

        fmt.set_data({
            "summary": deployment_plan.summary(),
            "creates": deployment_plan.creates,
            "updates": deployment_plan.updates,
            "deletes": deployment_plan.deletes,
            "has_changes": deployment_plan.has_changes,
            "changes": changes,
        })
        fmt.print(deployment_plan.details())
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)


def _make_sr_deployer(project: Any, fmt: Any) -> Any:
    from streamt.deployer.schema_registry import SchemaRegistryDeployer
    if project.runtime.schema_registry:
        try:
            sr = project.runtime.schema_registry
            return SchemaRegistryDeployer(
                sr.url,
                username=sr.username,
                password=sr.password,
                ssl_ca_location=sr.ssl_ca_location,
                ssl_certificate_location=sr.ssl_certificate_location,
                ssl_key_location=sr.ssl_key_location,
            )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Schema Registry: {e}")
    return None


def _make_kafka_deployer(project: Any, fmt: Any) -> Any:
    from streamt.deployer.kafka import KafkaDeployer
    try:
        confluent_config = project.runtime.kafka.to_confluent_config()
        bootstrap = confluent_config.pop("bootstrap.servers")
        return KafkaDeployer(bootstrap, **confluent_config)
    except Exception as e:
        fmt.print_warning(f"Cannot connect to Kafka: {e}")
    return None


def _make_flink_deployer(project: Any, fmt: Any) -> Any:
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
                        password=cfg.password,
                        api_key=cfg.api_key,
                        ssl_ca_location=cfg.ssl_ca_location,
                        ssl_certificate_location=cfg.ssl_certificate_location,
                        ssl_key_location=cfg.ssl_key_location,
                    )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Flink: {e}")
    return None


def _make_connect_deployer(project: Any, fmt: Any) -> Any:
    from streamt.deployer.connect import ConnectDeployer
    if project.runtime.connect and project.runtime.connect.clusters:
        try:
            default = project.runtime.connect.default
            if default and default in project.runtime.connect.clusters:
                cfg = project.runtime.connect.clusters[default]
                return ConnectDeployer(
                    cfg.rest_url,
                    username=cfg.username,
                    password=cfg.password,
                    ssl_ca_location=cfg.ssl_ca_location,
                    ssl_certificate_location=cfg.ssl_certificate_location,
                    ssl_key_location=cfg.ssl_key_location,
                )
        except Exception as e:
            fmt.print_warning(f"Cannot connect to Connect: {e}")
    return None
