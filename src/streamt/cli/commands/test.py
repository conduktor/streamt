"""streamt test command."""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, NoReturn, Optional
from uuid import uuid4

import click

from streamt.cli.helpers import (
    get_project_path,
    handle_parse_error,
    make_formatter,
    redact_sensitive_text,
)
from streamt.core.errors import ErrorCode
from streamt.integrations.openlineage import (
    DatasetIdentity,
    JobIdentity,
    OpenLineageConstructionError,
    OpenLineageNamespaceError,
    OpenLineageTransport,
    OpenLineageTransportConfigurationError,
    OpenLineageValidationError,
    RunIdentity,
    build_run_event,
    command_job_name,
    create_openlineage_transport,
    load_openlineage_transport_config,
    resolve_openlineage_namespaces,
    standard_facet,
    validate_event,
    validate_event_sequence,
)
from streamt.output import OutputFormatter, StructuredError, StructuredWarning

_TerminalEventType = Literal["COMPLETE", "FAIL", "ABORT"]
_FAILURE_MESSAGE = "streamt test command did not complete successfully"


class _OpenLineageTestPreflightError(ValueError):
    """A fixed, secret-neutral OpenLineage test preflight failure."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass
class _TestOpenLineageLifecycle:
    """Best-effort delivery state after a fully validated test preflight."""

    transport: OpenLineageTransport
    formatter: OutputFormatter
    start_event: dict[str, object]
    run: RunIdentity
    job: JobIdentity
    inputs: tuple[DatasetIdentity, ...]
    job_facets: dict[str, dict[str, object]]
    started: bool = False
    terminal_attempted: bool = False

    def start(self) -> None:
        """Attempt START once; delivery failures are warning-only."""
        self.started = True
        try:
            self.transport.emit(self.start_event)
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage START event delivery failed",
                location="openlineage.start",
            )

    def terminal(self, event_type: _TerminalEventType) -> None:
        """Attempt exactly one terminal event after START was attempted."""
        if not self.started or self.terminal_attempted:
            return
        self.terminal_attempted = True
        try:
            run_facets = (
                {
                    "errorMessage": standard_facet(
                        "run",
                        "errorMessage",
                        {
                            "message": _FAILURE_MESSAGE,
                            "programmingLanguage": "PYTHON",
                        },
                    )
                }
                if event_type == "FAIL"
                else None
            )
            event = build_run_event(
                event_time=_openlineage_event_time(),
                event_type=event_type,
                run=self.run,
                job=self.job,
                run_facets=run_facets,
                job_facets=self.job_facets,
                inputs=self.inputs,
            )
            validate_event_sequence((self.start_event, event))
            self.transport.emit(event)
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage terminal event delivery failed",
                location="openlineage.terminal",
            )

    def close(self) -> None:
        """Close once at the command boundary without changing command truth."""
        try:
            self.transport.close()
        except Exception:
            _emit_openlineage_delivery_warning(
                self.formatter,
                "OpenLineage transport close failed",
                location="openlineage.transport",
            )


def _normalize_test_errors(value: object) -> list[str]:
    """Return stable diagnostics for the runner's broad result payload boundary."""
    if not isinstance(value, list):
        return ["Malformed test result: 'errors' must be a list"]
    return [item if isinstance(item, str) else str(item) for item in value]


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option("--model", "-m", help="Run tests for this model only")
@click.option(
    "--type",
    "test_type",
    type=click.Choice(["schema", "sample", "continuous"]),
    help="Run only tests of this type",
)
@click.option("--deploy", is_flag=True, help="Deploy continuous tests as Flink jobs")
@click.option(
    "--coverage", is_flag=True, help="Show test coverage report (which models have tests)"
)
@click.option(
    "--emit-openlineage",
    is_flag=True,
    help="Emit finite OpenLineage events for this test run",
)
@click.option("--openlineage-job-namespace", help="OpenLineage job namespace")
@click.option(
    "--openlineage-kafka-namespace",
    help="Kafka dataset namespace (kafka://host:port)",
)
@click.option(
    "--openlineage-gateway-namespace",
    help="Gateway dataset namespace (kafka://host:port)",
)
@click.pass_context
def test(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    test_type: Optional[str],
    deploy: bool,
    coverage: bool,
    emit_openlineage: bool,
    openlineage_job_namespace: str | None,
    openlineage_kafka_namespace: str | None,
    openlineage_gateway_namespace: str | None,
) -> None:
    """Run tests."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.testing import TestRunner, resolve_sample_test_topic

    fmt = make_formatter(ctx, "test")
    project_path = get_project_path(project_dir)

    if deploy:
        fmt.print_warning("--deploy is not yet implemented; running tests locally only")

    try:
        parser = ProjectParser(
            project_path,
            environment=environment,
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

        if coverage:
            tested_models = {t.model for t in project.tests}
            rows = []
            cov_data = []
            for m in project.models:
                has_test = m.name in tested_models
                rows.append([m.name, "[green]yes[/green]" if has_test else "[red]no[/red]"])
                cov_data.append({"model": m.name, "covered": has_test})
            total = len(project.models)
            covered = sum(1 for d in cov_data if d["covered"])
            pct = int(covered / total * 100) if total else 0
            fmt.print_table(
                "Test Coverage",
                [("Model", "cyan"), ("Has Tests", "")],
                rows,
            )
            fmt.print(f"\n{covered}/{total} models covered ({pct}%)")
            fmt.set_data({"coverage": cov_data, "covered": covered, "total": total, "percent": pct})
            fmt.flush()
            return

        tests = project.tests
        if model:
            tests = [t for t in tests if t.model == model]
        if test_type:
            tests = [t for t in tests if t.type.value == test_type]

        if not tests:
            fmt.set_data({"results": [], "passed": 0, "failed": 0, "total": 0})
            fmt.print("[yellow]No tests to run[/yellow]")
            fmt.flush()
            return

        lineage: _TestOpenLineageLifecycle | None = None
        if emit_openlineage and not deploy:
            try:
                sample_topics = {
                    topic
                    for selected_test in tests
                    if selected_test.type.value == "sample"
                    if (topic := resolve_sample_test_topic(project, selected_test.model))
                    is not None
                }
                lineage = _prepare_test_openlineage(
                    project_name=project.project.name,
                    kafka_bootstrap=project.runtime.kafka.bootstrap_servers,
                    gateway_bootstrap=(
                        project.runtime.conduktor.gateway.proxy_bootstrap
                        if project.runtime.conduktor is not None
                        and project.runtime.conduktor.gateway is not None
                        else None
                    ),
                    require_kafka=any(
                        selected_test.type.value == "sample" for selected_test in tests
                    ),
                    sample_topics=sample_topics,
                    job_namespace=openlineage_job_namespace,
                    kafka_namespace=openlineage_kafka_namespace,
                    gateway_namespace=openlineage_gateway_namespace,
                    formatter=fmt,
                )
            except OpenLineageNamespaceError as error:
                _fail_openlineage_test_preflight(
                    fmt,
                    error,
                    location=error.location,
                )
            except OpenLineageTransportConfigurationError as error:
                _fail_openlineage_test_preflight(
                    fmt,
                    error,
                    location=error.location,
                )
            except _OpenLineageTestPreflightError as error:
                _fail_openlineage_test_preflight(
                    fmt,
                    error,
                    location=error.location,
                )
            except (OpenLineageConstructionError, OpenLineageValidationError):
                _fail_openlineage_test_preflight(
                    fmt,
                    _OpenLineageTestPreflightError(
                        "Could not construct validated OpenLineage test events",
                        location="openlineage.events",
                    ),
                    location="openlineage.events",
                )
            except Exception:
                _fail_openlineage_test_preflight(
                    fmt,
                    _OpenLineageTestPreflightError(
                        "Could not prepare OpenLineage test emission",
                        location="openlineage",
                    ),
                    location="openlineage",
                )

        try:
            runner = TestRunner(project)
            if lineage is not None:
                lineage.start()
            results = runner.run(tests)

            passed = 0
            failed = 0
            test_results: list[dict[str, object]] = []

            for test_result in results:
                tr: dict[str, object] = {
                    "name": test_result["name"],
                    "status": test_result["status"],
                }
                if test_result["status"] == "passed":
                    fmt.print(f"[green]PASS[/green]: {test_result['name']}")
                    passed += 1
                else:
                    fmt.print(f"[red]FAIL[/red]: {test_result['name']}")
                    test_errors = _normalize_test_errors(test_result.get("errors", []))
                    tr["errors"] = test_errors
                    for test_error in test_errors:
                        fmt.print(f"  - {test_error}")
                    failed += 1
                test_results.append(tr)

            fmt.set_data(
                {
                    "results": test_results,
                    "passed": passed,
                    "failed": failed,
                    "total": passed + failed,
                }
            )
            fmt.print(f"\n{passed} passed, {failed} failed")
            all_selected_results_passed = len(results) == len(tests) and failed == 0

        except KeyboardInterrupt:
            if lineage is not None:
                lineage.terminal("ABORT")
            raise
        except Exception:
            if lineage is not None:
                lineage.terminal("FAIL")
            raise
        else:
            if lineage is not None:
                lineage.terminal("COMPLETE" if all_selected_results_passed else "FAIL")
        finally:
            if lineage is not None:
                lineage.close()

        if failed > 0:
            fmt.set_status("error")
        fmt.flush()
        if failed > 0:
            sys.exit(1)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)


def _prepare_test_openlineage(
    *,
    project_name: str,
    kafka_bootstrap: str,
    gateway_bootstrap: str | None,
    require_kafka: bool,
    sample_topics: set[str],
    job_namespace: str | None,
    kafka_namespace: str | None,
    gateway_namespace: str | None,
    formatter: OutputFormatter,
) -> _TestOpenLineageLifecycle:
    """Resolve, build, validate, and open the finite test telemetry boundary."""
    namespaces = resolve_openlineage_namespaces(
        job_namespace=_option_or_environment(job_namespace, "OPENLINEAGE_NAMESPACE"),
        kafka_namespace=_option_or_environment(
            kafka_namespace,
            "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
        ),
        gateway_namespace=_option_or_environment(
            gateway_namespace,
            "STREAMT_OPENLINEAGE_GATEWAY_NAMESPACE",
        ),
        kafka_bootstrap=kafka_bootstrap,
        gateway_bootstrap=gateway_bootstrap,
        require_kafka=require_kafka,
        require_gateway=False,
    )
    if sample_topics and namespaces.kafka is None:
        raise _OpenLineageTestPreflightError(
            "OpenLineage Kafka namespace is required for selected sample tests",
            location="kafka_namespace",
        )

    inputs = tuple(
        sorted(
            DatasetIdentity(namespaces.kafka, topic)
            for topic in sample_topics
            if namespaces.kafka is not None
        )
    )
    job = JobIdentity(namespaces.job, command_job_name(project_name, "test"))
    run = RunIdentity(str(uuid4()))
    job_facets = {
        "jobType": standard_facet(
            "job",
            "jobType",
            {
                "processingType": "BATCH",
                "integration": "STREAMT",
                "jobType": "TEST",
            },
        )
    }
    preflight_time = _openlineage_event_time()
    start_event = build_run_event(
        event_time=preflight_time,
        event_type="START",
        run=run,
        job=job,
        job_facets=job_facets,
        inputs=inputs,
    )
    validate_event(start_event)

    failure_facet = {
        "errorMessage": standard_facet(
            "run",
            "errorMessage",
            {
                "message": _FAILURE_MESSAGE,
                "programmingLanguage": "PYTHON",
            },
        )
    }
    for terminal_type in ("COMPLETE", "FAIL", "ABORT"):
        candidate = build_run_event(
            event_time=preflight_time,
            event_type=terminal_type,
            run=run,
            job=job,
            run_facets=failure_facet if terminal_type == "FAIL" else None,
            job_facets=job_facets,
            inputs=inputs,
        )
        validate_event_sequence((start_event, candidate))

    config = load_openlineage_transport_config(os.environ, emission_requested=True)
    transport = create_openlineage_transport(config)
    return _TestOpenLineageLifecycle(
        transport=transport,
        formatter=formatter,
        start_event=start_event,
        run=run,
        job=job,
        inputs=inputs,
        job_facets=job_facets,
    )


def _option_or_environment(option: str | None, environment_name: str) -> str | None:
    """Apply exact option-over-environment precedence after dotenv loading."""
    return option if option is not None else os.environ.get(environment_name)


def _openlineage_event_time() -> str:
    """Return one OpenLineage-compatible UTC event timestamp."""
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _emit_openlineage_delivery_warning(
    formatter: OutputFormatter,
    message: str,
    *,
    location: str,
) -> None:
    """Capture one fixed delivery warning without exposing transport details."""
    safe_message = redact_sensitive_text(message)[:512]
    formatter.add_warning(
        StructuredWarning(
            code=ErrorCode.OPENLINEAGE_EMIT_FAILED,
            message=safe_message,
            location=location,
        )
    )
    if formatter.format == "text" and not formatter.quiet:
        formatter.stderr.print(f"[yellow]WARNING[/yellow]: {safe_message}")


def _fail_openlineage_test_preflight(
    formatter: OutputFormatter,
    error: Exception,
    *,
    location: str,
) -> NoReturn:
    """Emit one safe OpenLineage preflight error without constructing a runner."""
    safe_message = redact_sensitive_text(error)[:1024].strip()
    if not safe_message:
        safe_message = "Could not prepare OpenLineage test emission"
    formatter.add_error(
        StructuredError(
            code=ErrorCode.OPENLINEAGE_INVALID,
            message=safe_message,
            location=location,
        )
    )
    formatter.print_error(safe_message)
    formatter.flush()
    raise click.exceptions.Exit(1)
