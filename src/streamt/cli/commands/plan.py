"""streamt plan command."""

from __future__ import annotations

import sys
from typing import Any, Optional

import click

from streamt.cli.helpers import (
    close_deployers,
    get_project_path,
    handle_parse_error,
    make_connect_deployer,
    make_flink_deployer,
    make_formatter,
    make_kafka_deployer,
    make_sr_deployer,
)
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
        sr_deployer = make_sr_deployer(project, fmt)
        kafka_deployer = make_kafka_deployer(project, fmt)
        flink_deployer = make_flink_deployer(project, fmt)
        connect_deployer = make_connect_deployer(project, fmt)
        try:
            planner = DeploymentPlanner(
                manifest,
                schema_registry_deployer=sr_deployer,
                kafka_deployer=kafka_deployer,
                flink_deployer=flink_deployer,
                connect_deployer=connect_deployer,
            )
            deployment_plan = planner.plan()

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
        finally:
            close_deployers(sr_deployer, kafka_deployer, flink_deployer, connect_deployer)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)


