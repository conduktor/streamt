"""CLI for streamt."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from streamt import __version__
from streamt.core.environment import (
    EmptyEnvironmentsDirectoryError,
    EnvironmentError,
    EnvironmentManager,
    EnvironmentNotFoundError,
    InvalidEnvironmentNameError,
    NoEnvironmentsConfiguredError,
    NoEnvironmentSpecifiedError,
    mask_secrets,
)

console = Console()
error_console = Console(stderr=True)


def get_project_path(project_dir: Optional[str]) -> Path:
    """Get the project path."""
    if project_dir:
        return Path(project_dir).resolve()
    return Path.cwd()


@click.group()
@click.version_option(version=__version__)
def main() -> None:
    """streamt - dbt for streaming.

    Declarative streaming pipelines for Kafka, Flink, and Connect.
    """
    pass


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--all-envs",
    is_flag=True,
    help="Validate all environments",
)
@click.option(
    "--check-schemas",
    is_flag=True,
    help="Validate schemas against Schema Registry",
)
def validate(
    project_dir: Optional[str],
    environment: Optional[str],
    all_envs: bool,
    check_schemas: bool,
) -> None:
    """Validate project syntax and references."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    project_path = get_project_path(project_dir)

    def validate_single_env(env_name: Optional[str]) -> bool:
        """Validate a single environment. Returns True if valid."""
        try:
            # Parse project
            parser = ProjectParser(
                project_path,
                environment=env_name,
                warn_callback=lambda msg: console.print(msg),
            )
            project = parser.parse()

            # Validate project
            validator = ProjectValidator(project)
            result = validator.validate()

            # Print results
            if result.warnings:
                for warning in result.warnings:
                    console.print(f"[yellow]WARNING[/yellow]: {warning.message}")
                    if warning.location:
                        console.print(f"  Location: {warning.location}")

            if result.errors:
                for error in result.errors:
                    error_console.print(f"[red]ERROR[/red]: {error.message}")
                    if error.location:
                        error_console.print(f"  Location: {error.location}")
                return False

            env_label = f" ({env_name})" if env_name else ""
            console.print(f"[green]Project '{project.project.name}'{env_label} is valid[/green]")

            # Print summary
            table = Table(title="Project Summary")
            table.add_column("Type", style="cyan")
            table.add_column("Count", style="green")
            table.add_row("Sources", str(len(project.sources)))
            table.add_row("Models", str(len(project.models)))
            table.add_row("Tests", str(len(project.tests)))
            table.add_row("Exposures", str(len(project.exposures)))
            console.print(table)

            if project.rules:
                console.print("[green]All governance rules passed[/green]")

            return True

        except EnvVarError as e:
            error_console.print(f"[red]ERROR[/red]: {e}")
            return False
        except ParseError as e:
            error_console.print(f"[red]ERROR[/red]: {e}")
            return False
        except EnvironmentError as e:
            error_console.print(f"[red]ERROR[/red]: {e}")
            return False

    try:
        if all_envs:
            # Validate all environments
            env_manager = EnvironmentManager(project_path)
            if env_manager.mode == "single":
                error_console.print(
                    "[red]ERROR[/red]: --all-envs requires multi-environment mode. "
                    "Create an environments/ directory with environment files."
                )
                sys.exit(1)

            environments = env_manager.discover_environments()
            if not environments:
                error_console.print(
                    "[red]ERROR[/red]: No environment files found in environments/ directory."
                )
                sys.exit(1)

            console.print(f"Validating {len(environments)} environments...\n")
            all_valid = True
            for env_name in environments:
                console.print(f"[cyan]--- Environment: {env_name} ---[/cyan]")
                if not validate_single_env(env_name):
                    all_valid = False
                console.print()

            if not all_valid:
                error_console.print("[red]Some environments failed validation[/red]")
                sys.exit(1)

            console.print(f"[green]All {len(environments)} environments are valid[/green]")
        else:
            # Validate single environment
            if not validate_single_env(environment):
                sys.exit(1)

    except EnvironmentError as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)
    except Exception as e:
        error_console.print(f"[red]ERROR[/red]: Unexpected error: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output directory for generated artifacts",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be generated without writing files",
)
def compile(
    project_dir: Optional[str],
    environment: Optional[str],
    output: Optional[str],
    dry_run: bool,
) -> None:
    """Compile project to artifacts."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    project_path = get_project_path(project_dir)

    try:
        # Parse and validate
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        validator = ProjectValidator(project)
        result = validator.validate()

        if not result.is_valid:
            for error in result.errors:
                error_console.print(f"[red]ERROR[/red]: {error.message}")
            sys.exit(1)

        # Compile
        output_path = Path(output) if output else None
        compiler = Compiler(project, output_path)
        manifest = compiler.compile(dry_run=dry_run)

        if dry_run:
            console.print("[yellow]Dry run - no files written[/yellow]")
            console.print("\nArtifacts that would be generated:")

            artifacts = manifest.artifacts
            if artifacts.get("topics"):
                console.print(f"\n[cyan]Topics ({len(artifacts['topics'])}):[/cyan]")
                for topic in artifacts["topics"]:
                    console.print(f"  - {topic['name']}")

            if artifacts.get("flink_jobs"):
                console.print(f"\n[cyan]Flink Jobs ({len(artifacts['flink_jobs'])}):[/cyan]")
                for job in artifacts["flink_jobs"]:
                    console.print(f"  - {job['name']}")

            if artifacts.get("connectors"):
                console.print(f"\n[cyan]Connectors ({len(artifacts['connectors'])}):[/cyan]")
                for conn in artifacts["connectors"]:
                    console.print(f"  - {conn['name']}")

            if artifacts.get("gateway_rules"):
                console.print(f"\n[cyan]Gateway Rules ({len(artifacts['gateway_rules'])}):[/cyan]")
                for rule in artifacts["gateway_rules"]:
                    console.print(f"  - {rule['name']}")

            if artifacts.get("schemas"):
                console.print(f"\n[cyan]Schemas ({len(artifacts['schemas'])}):[/cyan]")
                for schema in artifacts["schemas"]:
                    console.print(f"  - {schema['subject']} ({schema['schema_type']})")
        else:
            console.print(f"[green]Compiled to {compiler.output_dir}[/green]")

            table = Table(title="Generated Artifacts")
            table.add_column("Type", style="cyan")
            table.add_column("Count", style="green")
            table.add_row("Schemas", str(len(manifest.artifacts.get("schemas", []))))
            table.add_row("Topics", str(len(manifest.artifacts.get("topics", []))))
            table.add_row("Flink Jobs", str(len(manifest.artifacts.get("flink_jobs", []))))
            table.add_row("Connectors", str(len(manifest.artifacts.get("connectors", []))))
            table.add_row("Gateway Rules", str(len(manifest.artifacts.get("gateway_rules", []))))
            console.print(table)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)
    except Exception as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        raise


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
def plan(project_dir: Optional[str], environment: Optional[str]) -> None:
    """Show what would change on apply."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.planner import DeploymentPlanner
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    project_path = get_project_path(project_dir)

    try:
        # Parse, validate, compile
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        validator = ProjectValidator(project)
        result = validator.validate()

        if not result.is_valid:
            for error in result.errors:
                error_console.print(f"[red]ERROR[/red]: {error.message}")
            sys.exit(1)

        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Create deployers
        schema_registry_deployer = None
        kafka_deployer = None
        flink_deployer = None
        connect_deployer = None

        if project.runtime.schema_registry:
            try:
                schema_registry_deployer = SchemaRegistryDeployer(
                    project.runtime.schema_registry.url,
                    username=project.runtime.schema_registry.username,
                    password=project.runtime.schema_registry.password,
                )
            except Exception as e:
                console.print(f"[yellow]Warning: Cannot connect to Schema Registry: {e}[/yellow]")

        try:
            kafka_deployer = KafkaDeployer(project.runtime.kafka.bootstrap_servers)
        except Exception as e:
            console.print(f"[yellow]Warning: Cannot connect to Kafka: {e}[/yellow]")

        if project.runtime.flink and project.runtime.flink.clusters:
            try:
                default_cluster = project.runtime.flink.default
                if default_cluster and default_cluster in project.runtime.flink.clusters:
                    cluster_config = project.runtime.flink.clusters[default_cluster]
                    if cluster_config.rest_url:
                        flink_deployer = FlinkDeployer(
                            rest_url=cluster_config.rest_url,
                            sql_gateway_url=cluster_config.sql_gateway_url,
                        )
            except Exception as e:
                console.print(f"[yellow]Warning: Cannot connect to Flink: {e}[/yellow]")

        if project.runtime.connect and project.runtime.connect.clusters:
            try:
                default_cluster = project.runtime.connect.default
                if default_cluster and default_cluster in project.runtime.connect.clusters:
                    cluster_config = project.runtime.connect.clusters[default_cluster]
                    connect_deployer = ConnectDeployer(cluster_config.rest_url)
            except Exception as e:
                console.print(f"[yellow]Warning: Cannot connect to Connect: {e}[/yellow]")

        # Create plan
        planner = DeploymentPlanner(
            manifest,
            schema_registry_deployer=schema_registry_deployer,
            kafka_deployer=kafka_deployer,
            flink_deployer=flink_deployer,
            connect_deployer=connect_deployer,
        )

        deployment_plan = planner.plan()
        console.print(deployment_plan.details())

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--target",
    "-t",
    help="Deploy only this model and its dependencies",
)
@click.option(
    "--select",
    "-s",
    help="Select models by tag (e.g., 'tag:payments')",
)
@click.option(
    "--confirm",
    is_flag=True,
    help="Skip confirmation prompt for protected environments",
)
@click.option(
    "--force",
    is_flag=True,
    help="Override safety checks (allow destructive operations)",
)
def apply(
    project_dir: Optional[str],
    environment: Optional[str],
    target: Optional[str],
    select: Optional[str],
    confirm: bool,
    force: bool,
) -> None:
    """Deploy the project."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.planner import DeploymentPlanner
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    project_path = get_project_path(project_dir)

    try:
        # Parse, validate, compile
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        # Check protected environment confirmation
        if parser.env_config and parser.env_config.environment.protected:
            env_name = parser.env_config.environment.name
            console.print(
                f"[yellow]WARNING[/yellow]: Deploying to protected environment '{env_name}'"
            )
            if not confirm:
                # Check if running interactively
                if sys.stdin.isatty():
                    console.print(
                        f"[yellow]WARNING[/yellow]: '{env_name}' is a protected environment."
                    )
                    user_input = click.prompt(
                        f"Type '{env_name}' to confirm", default="", show_default=False
                    )
                    if user_input != env_name:
                        error_console.print("[red]Aborted[/red]")
                        sys.exit(1)
                else:
                    # Non-interactive (CI) mode requires --confirm flag
                    error_console.print(
                        f"[red]ERROR[/red]: '{env_name}' is a protected environment. "
                        "Use --confirm flag in non-interactive mode."
                    )
                    sys.exit(1)

        # Check destructive safety
        if parser.env_config and not parser.env_config.safety.allow_destructive:
            env_name = parser.env_config.environment.name
            if not force:
                error_console.print(
                    f"[red]ERROR[/red]: Destructive operations blocked for '{env_name}' environment. "
                    "Use --force flag to override."
                )
                sys.exit(1)
            else:
                console.print(
                    f"[yellow]WARNING[/yellow]: --force flag used, "
                    f"allowing destructive operations on '{env_name}'"
                )

        validator = ProjectValidator(project)
        result = validator.validate()

        if not result.is_valid:
            for error in result.errors:
                error_console.print(f"[red]ERROR[/red]: {error.message}")
            sys.exit(1)

        compiler = Compiler(project)
        manifest = compiler.compile()

        # Create deployers
        schema_registry_deployer = None
        if project.runtime.schema_registry:
            schema_registry_deployer = SchemaRegistryDeployer(
                project.runtime.schema_registry.url,
                username=project.runtime.schema_registry.username,
                password=project.runtime.schema_registry.password,
            )

        kafka_deployer = KafkaDeployer(project.runtime.kafka.bootstrap_servers)

        flink_deployer = None
        connect_deployer = None

        if project.runtime.flink and project.runtime.flink.clusters:
            default_cluster = project.runtime.flink.default
            if default_cluster and default_cluster in project.runtime.flink.clusters:
                cluster_config = project.runtime.flink.clusters[default_cluster]
                if cluster_config.rest_url:
                    flink_deployer = FlinkDeployer(
                        rest_url=cluster_config.rest_url,
                        sql_gateway_url=cluster_config.sql_gateway_url,
                    )

        if project.runtime.connect and project.runtime.connect.clusters:
            default_cluster = project.runtime.connect.default
            if default_cluster and default_cluster in project.runtime.connect.clusters:
                cluster_config = project.runtime.connect.clusters[default_cluster]
                connect_deployer = ConnectDeployer(cluster_config.rest_url)

        # Apply
        planner = DeploymentPlanner(
            manifest,
            schema_registry_deployer=schema_registry_deployer,
            kafka_deployer=kafka_deployer,
            flink_deployer=flink_deployer,
            connect_deployer=connect_deployer,
        )

        results = planner.apply()

        # Print results
        if results["created"]:
            console.print("\n[green]Created:[/green]")
            for item in results["created"]:
                console.print(f"  + {item}")

        if results["updated"]:
            console.print("\n[yellow]Updated:[/yellow]")
            for item in results["updated"]:
                console.print(f"  ~ {item}")

        if results["unchanged"]:
            console.print("\n[dim]Unchanged:[/dim]")
            for item in results["unchanged"]:
                console.print(f"  = {item}")

        if results["errors"]:
            console.print("\n[red]Errors:[/red]")
            for item in results["errors"]:
                error_console.print(f"  ! {item}")
            sys.exit(1)

        console.print("\n[green]Apply complete[/green]")

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)
    except Exception as e:
        error_console.print(f"[red]ERROR[/red]: Cannot connect to Kafka: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--model",
    "-m",
    help="Run tests for this model only",
)
@click.option(
    "--type",
    "test_type",
    type=click.Choice(["schema", "sample", "continuous"]),
    help="Run only tests of this type",
)
@click.option(
    "--deploy",
    is_flag=True,
    help="Deploy continuous tests as Flink jobs",
)
def test(
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    test_type: Optional[str],
    deploy: bool,
) -> None:
    """Run tests."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.testing import TestRunner

    project_path = get_project_path(project_dir)

    try:
        # Parse and validate
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        validator = ProjectValidator(project)
        result = validator.validate()

        if not result.is_valid:
            for error in result.errors:
                error_console.print(f"[red]ERROR[/red]: {error.message}")
            sys.exit(1)

        # Filter tests
        tests = project.tests
        if model:
            tests = [t for t in tests if t.model == model]
        if test_type:
            tests = [t for t in tests if t.type.value == test_type]

        if not tests:
            console.print("[yellow]No tests to run[/yellow]")
            return

        # Run tests
        runner = TestRunner(project)
        results = runner.run(tests)

        # Print results
        passed = 0
        failed = 0

        for test_result in results:
            if test_result["status"] == "passed":
                console.print(f"[green]PASS[/green]: {test_result['name']}")
                passed += 1
            else:
                console.print(f"[red]FAIL[/red]: {test_result['name']}")
                for error in test_result.get("errors", []):
                    console.print(f"  - {error}")
                failed += 1

        console.print(f"\n{passed} passed, {failed} failed")

        if failed > 0:
            sys.exit(1)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--model",
    "-m",
    help="Focus on this model",
)
@click.option(
    "--upstream",
    is_flag=True,
    help="Show only upstream dependencies",
)
@click.option(
    "--downstream",
    is_flag=True,
    help="Show only downstream dependents",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["ascii", "json"]),
    default="ascii",
    help="Output format",
)
def lineage(
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    upstream: bool,
    downstream: bool,
    output_format: str,
) -> None:
    """Show the DAG lineage."""
    import json

    from streamt.core.dag import DAGBuilder
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    project_path = get_project_path(project_dir)

    try:
        # Parse project
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        # Build DAG
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        if output_format == "json":
            console.print(json.dumps(dag.to_dict(), indent=2))
        else:
            console.print(dag.render_ascii(focus=model))

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--lag",
    is_flag=True,
    help="Show consumer lag for topics (requires consumer groups)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format (default: text)",
)
@click.option(
    "--filter",
    "filter_pattern",
    type=str,
    help="Filter resources by name pattern (glob-style)",
)
def status(
    project_dir: Optional[str],
    environment: Optional[str],
    lag: bool,
    output_format: str,
    filter_pattern: Optional[str],
) -> None:
    """Show status of deployed resources.

    Examples:

        streamt status              # Basic status

        streamt status --lag        # Include consumer lag

        streamt status --format json  # JSON output

        streamt status --filter "payments*"  # Filter by name
    """
    import fnmatch
    import json

    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    project_path = get_project_path(project_dir)

    def matches_filter(name: str) -> bool:
        """Check if name matches the filter pattern."""
        if not filter_pattern:
            return True
        return fnmatch.fnmatch(name, filter_pattern)

    try:
        # Parse and compile
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Collect status data for JSON output
        status_data = {
            "project": project.project.name,
            "schemas": [],
            "topics": [],
            "flink_jobs": [],
            "connectors": [],
        }

        # Check Schema Registry schemas
        if manifest.artifacts.get("schemas"):
            if output_format == "text":
                console.print("\n[cyan]Schemas:[/cyan]")
            if project.runtime.schema_registry:
                try:
                    schema_deployer = SchemaRegistryDeployer(
                        project.runtime.schema_registry.url,
                        username=project.runtime.schema_registry.username,
                        password=project.runtime.schema_registry.password,
                    )
                    for schema_data in manifest.artifacts["schemas"]:
                        if not matches_filter(schema_data["subject"]):
                            continue
                        state = schema_deployer.get_schema_state(schema_data["subject"])
                        schema_status = {
                            "subject": schema_data["subject"],
                            "exists": state.exists,
                            "version": state.version if state.exists else None,
                            "schema_type": state.schema_type if state.exists else None,
                        }
                        status_data["schemas"].append(schema_status)

                        if output_format == "text":
                            if state.exists:
                                console.print(
                                    f"  [green]OK[/green] {schema_data['subject']} "
                                    f"(version: {state.version}, type: {state.schema_type})"
                                )
                            else:
                                console.print(f"  [red]MISSING[/red] {schema_data['subject']}")
                except Exception as e:
                    if output_format == "text":
                        console.print(f"  [yellow]Cannot connect to Schema Registry: {e}[/yellow]")
            else:
                if output_format == "text":
                    console.print("  [yellow]No Schema Registry configured[/yellow]")

        # Check Kafka topics
        if output_format == "text":
            console.print("\n[cyan]Topics:[/cyan]")
        kafka_deployer = None
        try:
            kafka_deployer = KafkaDeployer(project.runtime.kafka.bootstrap_servers)
            for topic_data in manifest.artifacts.get("topics", []):
                if not matches_filter(topic_data["name"]):
                    continue
                state = kafka_deployer.get_topic_state(topic_data["name"])
                topic_status = {
                    "name": topic_data["name"],
                    "exists": state.exists,
                    "partitions": state.partitions if state.exists else None,
                    "replication_factor": state.replication_factor if state.exists else None,
                }

                # Get message count and lag if requested
                if lag and state.exists:
                    msg_count = kafka_deployer.get_topic_message_count(topic_data["name"])
                    topic_status["message_count"] = msg_count

                status_data["topics"].append(topic_status)

                if output_format == "text":
                    if state.exists:
                        status_line = (
                            f"  [green]OK[/green] {topic_data['name']} "
                            f"(partitions: {state.partitions}, rf: {state.replication_factor})"
                        )
                        if lag and "message_count" in topic_status:
                            status_line += f" [dim]~{topic_status['message_count']} msgs[/dim]"
                        console.print(status_line)
                    else:
                        console.print(f"  [red]MISSING[/red] {topic_data['name']}")
        except Exception as e:
            if output_format == "text":
                console.print(f"  [yellow]Cannot connect to Kafka: {e}[/yellow]")

        # Check Flink jobs
        if manifest.artifacts.get("flink_jobs"):
            if output_format == "text":
                console.print("\n[cyan]Flink Jobs:[/cyan]")
            if project.runtime.flink and project.runtime.flink.clusters:
                try:
                    default_cluster = project.runtime.flink.default
                    if default_cluster and default_cluster in project.runtime.flink.clusters:
                        cluster_config = project.runtime.flink.clusters[default_cluster]
                        # Use rest_url for job status (REST API), not sql_gateway_url
                        if cluster_config.rest_url:
                            flink_deployer = FlinkDeployer(cluster_config.rest_url)
                            for job_data in manifest.artifacts["flink_jobs"]:
                                if not matches_filter(job_data["name"]):
                                    continue
                                state = flink_deployer.get_job_state(job_data["name"])
                                job_status = {
                                    "name": job_data["name"],
                                    "exists": state.exists,
                                    "job_id": state.job_id if state.exists else None,
                                    "status": state.status if state.exists else None,
                                }
                                status_data["flink_jobs"].append(job_status)

                                if output_format == "text":
                                    if state.exists:
                                        # Color code status
                                        status_color = "green" if state.status == "RUNNING" else "yellow"
                                        console.print(
                                            f"  [{status_color}]{state.status}[/{status_color}] {job_data['name']}"
                                        )
                                    else:
                                        console.print(f"  [red]NOT FOUND[/red] {job_data['name']}")
                except Exception as e:
                    if output_format == "text":
                        console.print(f"  [yellow]Cannot connect to Flink: {e}[/yellow]")
            else:
                if output_format == "text":
                    console.print("  [yellow]No Flink configured[/yellow]")

        # Check connectors
        if manifest.artifacts.get("connectors"):
            if output_format == "text":
                console.print("\n[cyan]Connectors:[/cyan]")
            if project.runtime.connect and project.runtime.connect.clusters:
                try:
                    default_cluster = project.runtime.connect.default
                    if default_cluster and default_cluster in project.runtime.connect.clusters:
                        cluster_config = project.runtime.connect.clusters[default_cluster]
                        connect_deployer = ConnectDeployer(cluster_config.rest_url)
                        for conn_data in manifest.artifacts["connectors"]:
                            if not matches_filter(conn_data["name"]):
                                continue
                            state = connect_deployer.get_connector_state(conn_data["name"])
                            conn_status = {
                                "name": conn_data["name"],
                                "exists": state.exists,
                                "status": state.status if state.exists else None,
                            }
                            status_data["connectors"].append(conn_status)

                            if output_format == "text":
                                if state.exists:
                                    # Color code status
                                    status_color = "green" if state.status == "RUNNING" else "yellow"
                                    console.print(
                                        f"  [{status_color}]{state.status}[/{status_color}] {conn_data['name']}"
                                    )
                                else:
                                    console.print(f"  [red]NOT FOUND[/red] {conn_data['name']}")
                except Exception as e:
                    if output_format == "text":
                        console.print(f"  [yellow]Cannot connect to Connect: {e}[/yellow]")
            else:
                if output_format == "text":
                    console.print("  [yellow]No Connect configured[/yellow]")

        # Print summary for text output
        if output_format == "text":
            console.print()  # Empty line
            # Count healthy vs unhealthy
            healthy = sum(1 for t in status_data["topics"] if t["exists"])
            missing = sum(1 for t in status_data["topics"] if not t["exists"])
            running_jobs = sum(1 for j in status_data["flink_jobs"] if j.get("status") == "RUNNING")
            other_jobs = sum(1 for j in status_data["flink_jobs"] if j.get("status") and j["status"] != "RUNNING")

            summary_parts = []
            if status_data["topics"]:
                summary_parts.append(f"Topics: {healthy} OK, {missing} missing")
            if status_data["flink_jobs"]:
                summary_parts.append(f"Jobs: {running_jobs} running, {other_jobs} other")
            if summary_parts:
                console.print(f"[dim]Summary: {' | '.join(summary_parts)}[/dim]")
        else:
            # JSON output
            console.print(json.dumps(status_data, indent=2))

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


@main.group()
def docs() -> None:
    """Documentation commands."""
    pass


@docs.command("generate")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.option(
    "--env",
    "-e",
    "environment",
    help="Target environment (reads from STREAMT_ENV if not set)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default="docs",
    help="Output directory",
)
def docs_generate(project_dir: Optional[str], environment: Optional[str], output: str) -> None:
    """Generate HTML documentation."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.docs import generate_docs

    project_path = get_project_path(project_dir)

    try:
        # Parse project
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: console.print(msg),
        )
        project = parser.parse()

        # Build DAG
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        # Generate docs
        output_path = project_path / output
        generate_docs(project, dag, output_path)

        console.print(f"[green]Documentation generated at {output_path}[/green]")

    except (EnvVarError, ParseError, EnvironmentError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


# =============================================================================
# Environment Commands
# =============================================================================


@main.group()
def envs() -> None:
    """Environment management commands."""
    pass


@envs.command("list")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
def envs_list(project_dir: Optional[str]) -> None:
    """List available environments."""
    project_path = get_project_path(project_dir)
    env_manager = EnvironmentManager(project_path)

    if env_manager.mode == "single":
        console.print("No environments configured (single-env mode)")
        return

    environments = env_manager.discover_environments()
    if not environments:
        console.print("No environment files found in environments/ directory")
        return

    for env_name in environments:
        try:
            env_config = env_manager.load_environment(env_name)
            description = env_config.environment.description or ""
            protected_label = " \\[protected]" if env_config.environment.protected else ""
            if description:
                console.print(f"{env_name:12} {description}{protected_label}")
            else:
                console.print(f"{env_name}{protected_label}")
        except EnvironmentError as e:
            console.print(f"{env_name:12} [red]Error: {e}[/red]")


@envs.command("show")
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
@click.argument("name")
def envs_show(project_dir: Optional[str], name: str) -> None:
    """Show resolved configuration for an environment."""
    import yaml

    project_path = get_project_path(project_dir)
    env_manager = EnvironmentManager(project_path)

    if env_manager.mode == "single":
        error_console.print(
            "[red]ERROR[/red]: No environments configured (single-env mode)"
        )
        sys.exit(1)

    try:
        env_config = env_manager.load_environment(name)

        # Mask secrets before displaying
        masked_runtime = mask_secrets(env_config.runtime)

        console.print(f"[cyan]Environment:[/cyan] {env_config.environment.name}")
        if env_config.environment.description:
            console.print(f"[cyan]Description:[/cyan] {env_config.environment.description}")
        if env_config.environment.protected:
            console.print("[cyan]Protected:[/cyan] [yellow]yes[/yellow]")

        console.print("\n[cyan]Runtime:[/cyan]")
        console.print(yaml.dump(masked_runtime, default_flow_style=False, sort_keys=False))

        console.print("[cyan]Safety:[/cyan]")
        console.print(f"  confirm_apply: {env_config.safety.confirm_apply}")
        console.print(f"  allow_destructive: {env_config.safety.allow_destructive}")

    except EnvironmentError as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
