"""CLI for streamt."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.table import Table

from streamt import __version__

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
    "--check-schemas",
    is_flag=True,
    help="Validate schemas against Schema Registry",
)
def validate(project_dir: Optional[str], check_schemas: bool) -> None:
    """Validate project syntax and references."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    project_path = get_project_path(project_dir)

    try:
        # Parse project
        parser = ProjectParser(project_path)
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
            sys.exit(1)

        console.print(f"[green]Project '{project.project.name}' is valid[/green]")

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

    except EnvVarError as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)
    except ParseError as e:
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
def compile(project_dir: Optional[str], output: Optional[str], dry_run: bool) -> None:
    """Compile project to artifacts."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    project_path = get_project_path(project_dir)

    try:
        # Parse and validate
        parser = ProjectParser(project_path)
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

    except (EnvVarError, ParseError) as e:
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
def plan(project_dir: Optional[str]) -> None:
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
        parser = ProjectParser(project_path)
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

    except (EnvVarError, ParseError) as e:
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
    "--target",
    "-t",
    help="Deploy only this model and its dependencies",
)
@click.option(
    "--select",
    "-s",
    help="Select models by tag (e.g., 'tag:payments')",
)
def apply(project_dir: Optional[str], target: Optional[str], select: Optional[str]) -> None:
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
        parser = ProjectParser(project_path)
        project = parser.parse()

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

    except (EnvVarError, ParseError) as e:
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
        parser = ProjectParser(project_path)
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

    except (EnvVarError, ParseError) as e:
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
        parser = ProjectParser(project_path)
        project = parser.parse()

        # Build DAG
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        if output_format == "json":
            console.print(json.dumps(dag.to_dict(), indent=2))
        else:
            console.print(dag.render_ascii(focus=model))

    except (EnvVarError, ParseError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


@main.command()
@click.option(
    "--project-dir",
    "-p",
    type=click.Path(exists=True),
    help="Path to project directory",
)
def status(project_dir: Optional[str]) -> None:
    """Show status of deployed resources."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.deployer.connect import ConnectDeployer
    from streamt.deployer.flink import FlinkDeployer
    from streamt.deployer.kafka import KafkaDeployer
    from streamt.deployer.schema_registry import SchemaRegistryDeployer

    project_path = get_project_path(project_dir)

    try:
        # Parse and compile
        parser = ProjectParser(project_path)
        project = parser.parse()

        compiler = Compiler(project)
        manifest = compiler.compile(dry_run=True)

        # Check Schema Registry schemas
        if manifest.artifacts.get("schemas"):
            console.print("\n[cyan]Schemas:[/cyan]")
            if project.runtime.schema_registry:
                try:
                    schema_deployer = SchemaRegistryDeployer(
                        project.runtime.schema_registry.url,
                        username=project.runtime.schema_registry.username,
                        password=project.runtime.schema_registry.password,
                    )
                    for schema_data in manifest.artifacts["schemas"]:
                        state = schema_deployer.get_schema_state(schema_data["subject"])
                        if state.exists:
                            console.print(
                                f"  [green]OK[/green] {schema_data['subject']} "
                                f"(version: {state.version}, type: {state.schema_type})"
                            )
                        else:
                            console.print(f"  [red]MISSING[/red] {schema_data['subject']}")
                except Exception as e:
                    console.print(f"  [yellow]Cannot connect to Schema Registry: {e}[/yellow]")
            else:
                console.print("  [yellow]No Schema Registry configured[/yellow]")

        # Check Kafka topics
        console.print("\n[cyan]Topics:[/cyan]")
        try:
            kafka_deployer = KafkaDeployer(project.runtime.kafka.bootstrap_servers)
            for topic_data in manifest.artifacts.get("topics", []):
                state = kafka_deployer.get_topic_state(topic_data["name"])
                if state.exists:
                    console.print(
                        f"  [green]OK[/green] {topic_data['name']} "
                        f"(partitions: {state.partitions}, rf: {state.replication_factor})"
                    )
                else:
                    console.print(f"  [red]MISSING[/red] {topic_data['name']}")
        except Exception as e:
            console.print(f"  [yellow]Cannot connect to Kafka: {e}[/yellow]")

        # Check Flink jobs
        if manifest.artifacts.get("flink_jobs"):
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
                                state = flink_deployer.get_job_state(job_data["name"])
                                if state.exists:
                                    console.print(
                                        f"  [green]{state.status}[/green] {job_data['name']}"
                                    )
                                else:
                                    console.print(f"  [red]NOT FOUND[/red] {job_data['name']}")
                except Exception as e:
                    console.print(f"  [yellow]Cannot connect to Flink: {e}[/yellow]")
            else:
                console.print("  [yellow]No Flink configured[/yellow]")

        # Check connectors
        if manifest.artifacts.get("connectors"):
            console.print("\n[cyan]Connectors:[/cyan]")
            if project.runtime.connect and project.runtime.connect.clusters:
                try:
                    default_cluster = project.runtime.connect.default
                    if default_cluster and default_cluster in project.runtime.connect.clusters:
                        cluster_config = project.runtime.connect.clusters[default_cluster]
                        connect_deployer = ConnectDeployer(cluster_config.rest_url)
                        for conn_data in manifest.artifacts["connectors"]:
                            state = connect_deployer.get_connector_state(conn_data["name"])
                            if state.exists:
                                console.print(
                                    f"  [green]{state.status}[/green] {conn_data['name']}"
                                )
                            else:
                                console.print(f"  [red]NOT FOUND[/red] {conn_data['name']}")
                except Exception as e:
                    console.print(f"  [yellow]Cannot connect to Connect: {e}[/yellow]")
            else:
                console.print("  [yellow]No Connect configured[/yellow]")

    except (EnvVarError, ParseError) as e:
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
    "--output",
    "-o",
    type=click.Path(),
    default="docs",
    help="Output directory",
)
def docs_generate(project_dir: Optional[str], output: str) -> None:
    """Generate HTML documentation."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.docs import generate_docs

    project_path = get_project_path(project_dir)

    try:
        # Parse project
        parser = ProjectParser(project_path)
        project = parser.parse()

        # Build DAG
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        # Generate docs
        output_path = project_path / output
        generate_docs(project, dag, output_path)

        console.print(f"[green]Documentation generated at {output_path}[/green]")

    except (EnvVarError, ParseError) as e:
        error_console.print(f"[red]ERROR[/red]: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
