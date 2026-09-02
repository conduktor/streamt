"""streamt show command."""

from __future__ import annotations

import re
import sys
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.dag import DAG
from streamt.core.errors import ErrorCode
from streamt.core.models import StreamtProject
from streamt.output import OutputFormatter, StructuredError


@click.command("show")
@click.argument("resource_type", type=click.Choice(["source", "model", "test", "exposure"]))
@click.argument("name")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option(
    "--diff", "show_diff", is_flag=True, help="Show diff between declared and deployed state"
)
@click.pass_context
def show_resource(
    ctx: click.Context,
    resource_type: str,
    name: str,
    project_dir: Optional[str],
    environment: Optional[str],
    show_diff: bool = False,
) -> None:
    """Show detailed info about a single resource."""
    from streamt.core.dag import DAGBuilder
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "show")
    project_path = get_project_path(project_dir)

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
            warn_callback=lambda msg: fmt.print(msg),
        )
        project = parser.parse()
        dag_builder = DAGBuilder(project)
        dag = dag_builder.build()

        data: dict[str, object] = {"resource_type": resource_type, "name": name}

        if resource_type == "source":
            _show_source(project, dag, name, data, fmt)
        elif resource_type == "model":
            _show_model(project, dag, name, data, fmt)
        elif resource_type == "test":
            _show_test(project, name, data, fmt)
        elif resource_type == "exposure":
            _show_exposure(project, name, data, fmt)

        if show_diff and resource_type == "model":
            _show_model_diff(project, name, data, fmt)
        elif show_diff and resource_type == "source":
            _show_source_diff(project, name, data, fmt)

        fmt.set_data(data)
        fmt.flush()

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)


def _resolve_jinja_sql(sql: str, project: StreamtProject) -> str:
    """Resolve Jinja source()/ref() calls to actual topic/table names."""
    source_map = {s.name: s.topic for s in project.sources}
    model_map: dict[str, str] = {}
    for model in project.models:
        topic_config = model.get_topic_config()
        model_map[model.name] = (
            topic_config.name if topic_config is not None and topic_config.name else model.name
        )

    def _replace_source(m: re.Match[str]) -> str:
        name = m.group(1)
        return source_map.get(name, name)

    def _replace_ref(m: re.Match[str]) -> str:
        name = m.group(1)
        return model_map.get(name, name)

    result = re.sub(r'\{\{\s*source\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}', _replace_source, sql)
    result = re.sub(r'\{\{\s*ref\s*\(\s*["\']([^"\']+)["\']\s*\)\s*\}\}', _replace_ref, result)
    return result


def _not_found(
    fmt: OutputFormatter, code: str, resource_type: str, name: str, available: list[str]
) -> None:
    """Emit not-found error and exit."""
    fmt.add_error(
        StructuredError(
            code=code,
            message=f"{resource_type.capitalize()} '{name}' not found",
            suggestion=f"Available: {', '.join(available)}" if available else None,
        )
    )
    fmt.print_error(f"{resource_type.capitalize()} '{name}' not found")
    fmt.flush()
    sys.exit(1)


def _show_source(
    project: StreamtProject, dag: DAG, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    source = project.get_source(name)
    if not source:
        _not_found(
            fmt, ErrorCode.SOURCE_NOT_FOUND, "source", name, [s.name for s in project.sources]
        )
        return

    node = dag.get_node(name)
    data.update(
        {
            "topic": source.topic,
            "description": source.description,
            "owner": source.owner,
            "tags": source.tags,
            "has_schema": source.schema_ is not None,
            "schema_format": source.schema_.format if source.schema_ else None,
            "columns": [
                {
                    "name": c.name,
                    "type": c.type,
                    "classification": c.classification.value if c.classification else None,
                }
                for c in source.columns
            ],
            "downstream": sorted(node.downstream) if node else [],
            "event_time": {"column": source.event_time.column} if source.event_time else None,
        }
    )

    fmt.print(f"[cyan]Source:[/cyan] {name}")
    fmt.print(f"  Topic: {source.topic}")
    if source.description:
        fmt.print(f"  Description: {source.description}")
    if source.columns:
        fmt.print(f"  Columns: {', '.join(c.name for c in source.columns)}")
    if node and node.downstream:
        fmt.print(f"  Downstream: {', '.join(sorted(node.downstream))}")


def _show_model(
    project: StreamtProject, dag: DAG, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    model = project.get_model(name)
    if not model:
        _not_found(fmt, ErrorCode.MODEL_NOT_FOUND, "model", name, [m.name for m in project.models])
        return

    node = dag.get_node(name)
    mat = model.get_materialized()

    upstream: list[str] = []
    if model.sql:
        upstream += re.findall(r'{{\s*source\(\s*["\'](\w+)["\']\s*\)', model.sql)
        upstream += re.findall(r'{{\s*ref\(\s*["\'](\w+)["\']\s*\)', model.sql)

    data.update(
        {
            "materialized": mat.value,
            "description": model.description,
            "sql": model.sql,
            "upstream": upstream,
            "downstream": sorted(node.downstream) if node else [],
            "tags": model.tags,
            "access": model.access.value,
            "group": model.group,
            "owner": model.owner,
        }
    )

    flink_cfg = model.get_flink_config()
    if flink_cfg:
        data["flink"] = {
            "parallelism": flink_cfg.parallelism,
            "checkpoint_interval_ms": flink_cfg.checkpoint_interval_ms,
            "state_ttl_ms": flink_cfg.state_ttl_ms,
        }
    topic_cfg = model.get_topic_config()
    if topic_cfg:
        data["topic"] = {
            "name": topic_cfg.name,
            "partitions": topic_cfg.partitions,
            "replication_factor": topic_cfg.replication_factor,
        }

    # SHOW-1: Contract info
    if model.contract:
        contract_data: dict[str, object] = {"enforced": model.contract.enforced}
        contract_cols = []
        for c in model.contract.columns:
            col_info: dict[str, object] = {"name": c.name}
            if c.type:
                col_info["type"] = c.type
            if c.nullable is not None:
                col_info["nullable"] = c.nullable
            contract_cols.append(col_info)
        contract_data["columns"] = contract_cols
        data["contract"] = contract_data

    fmt.print(f"[cyan]Model:[/cyan] {name}")
    fmt.print(f"  Materialized: {mat.value}")
    if model.description:
        fmt.print(f"  Description: {model.description}")
    if upstream:
        fmt.print(f"  Upstream: {', '.join(upstream)}")
    if node and node.downstream:
        fmt.print(f"  Downstream: {', '.join(sorted(node.downstream))}")
    if model.sql:
        snippet = model.sql.strip()[:120]
        fmt.print(f"  SQL: {snippet}{'...' if len(model.sql.strip()) > 120 else ''}")
        # SHOW-2: Compiled SQL (Jinja resolved)
        compiled = _resolve_jinja_sql(model.sql, project)
        if compiled != model.sql:
            data["compiled_sql"] = compiled
            fmt.print(f"  Compiled SQL: {compiled.strip()}")
    if model.contract:
        fmt.print(f"  Contract: {'enforced' if model.contract.enforced else 'advisory'}")
        if model.contract.columns:
            for c in model.contract.columns:
                nullable = " (nullable)" if c.nullable else ""
                fmt.print(f"    - {c.name}: {c.type or '?'}{nullable}")


def _show_test(
    project: StreamtProject, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    test_obj = project.get_test(name)
    if not test_obj:
        _not_found(
            fmt, ErrorCode.TEST_MODEL_NOT_FOUND, "test", name, [t.name for t in project.tests]
        )
        return

    data.update(
        {
            "model": test_obj.model,
            "type": test_obj.type.value,
            "assertions": test_obj.assertions,
            "sample_size": test_obj.sample_size,
        }
    )

    fmt.print(f"[cyan]Test:[/cyan] {name}")
    fmt.print(f"  Model: {test_obj.model}")
    fmt.print(f"  Type: {test_obj.type.value}")
    fmt.print(f"  Assertions ({len(test_obj.assertions)}):")
    for assertion in test_obj.assertions:
        for kind, config in assertion.items():
            if isinstance(config, dict):
                details = ", ".join(f"{k}={v}" for k, v in config.items())
                fmt.print(f"    - {kind}: {details}")
            else:
                fmt.print(f"    - {kind}: {config}")


def _show_exposure(
    project: StreamtProject, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    exposure = project.get_exposure(name)
    if not exposure:
        _not_found(
            fmt,
            ErrorCode.EXPOSURE_MODEL_NOT_FOUND,
            "exposure",
            name,
            [e.name for e in project.exposures],
        )
        return

    data.update(
        {
            "type": exposure.type.value,
            "description": exposure.description,
            "owner": exposure.owner,
            "role": exposure.role.value if exposure.role else None,
            "consumes": [{"source": r.source, "ref": r.ref} for r in exposure.consumes],
            "produces": [{"source": r.source, "ref": r.ref} for r in exposure.produces],
        }
    )

    fmt.print(f"[cyan]Exposure:[/cyan] {name}")
    fmt.print(f"  Type: {exposure.type.value}")
    if exposure.description:
        fmt.print(f"  Description: {exposure.description}")
    if exposure.owner:
        fmt.print(f"  Owner: {exposure.owner}")


def _show_model_diff(
    project: StreamtProject, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    """Show diff between declared model config and deployed state."""
    model = project.get_model(name)
    if not model:
        return
    try:
        from streamt.cli.helpers import make_kafka_deployer

        kd = make_kafka_deployer(project, fmt)
        if not kd:
            return
        tc = model.get_topic_config()
        topic_name = tc.name if tc and tc.name else model.name
        state = kd.get_topic_state(topic_name)
        if not state.exists:
            data["diff"] = {"status": "not_deployed"}
            fmt.print(f"\n[yellow]Topic '{topic_name}' not deployed[/yellow]")
            return
        diffs: list[dict[str, object]] = []
        desired_p = tc.partitions if tc and tc.partitions else None
        if desired_p and state.partitions != desired_p:
            diffs.append({"field": "partitions", "declared": desired_p, "actual": state.partitions})
        desired_rf = tc.replication_factor if tc and tc.replication_factor else None
        if desired_rf and state.replication_factor != desired_rf:
            diffs.append(
                {
                    "field": "replication_factor",
                    "declared": desired_rf,
                    "actual": state.replication_factor,
                }
            )
        data["diff"] = {"status": "drift" if diffs else "in_sync", "diffs": diffs}
        if diffs:
            fmt.print("\n[yellow]Drift detected:[/yellow]")
            for d in diffs:
                fmt.print(f"  {d['field']}: declared={d['declared']} actual={d['actual']}")
        else:
            fmt.print("\n[green]In sync with deployed state[/green]")
        kd.close()
    except Exception:
        pass


def _show_source_diff(
    project: StreamtProject, name: str, data: dict[str, object], fmt: OutputFormatter
) -> None:
    """Show diff for source topic existence."""
    source = project.get_source(name)
    if not source:
        return
    try:
        from streamt.cli.helpers import make_kafka_deployer

        kd = make_kafka_deployer(project, fmt)
        if not kd:
            return
        state = kd.get_topic_state(source.topic)
        data["diff"] = {
            "exists": state.exists,
            "partitions": state.partitions if state.exists else None,
        }
        if state.exists:
            fmt.print(
                f"\n[green]Topic '{source.topic}' exists (partitions: {state.partitions})[/green]"
            )
        else:
            fmt.print(f"\n[red]Topic '{source.topic}' does not exist[/red]")
        kd.close()
    except Exception:
        pass
