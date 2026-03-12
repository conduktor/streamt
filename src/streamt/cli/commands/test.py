"""streamt test command."""

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
@click.option("--model", "-m", help="Run tests for this model only")
@click.option("--type", "test_type", type=click.Choice(["schema", "sample", "continuous"]), help="Run only tests of this type")
@click.option("--deploy", is_flag=True, help="Deploy continuous tests as Flink jobs")
@click.pass_context
def test(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    model: Optional[str],
    test_type: Optional[str],
    deploy: bool,
) -> None:
    """Run tests."""
    from streamt.core.environment import EnvironmentError
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator
    from streamt.testing import TestRunner

    fmt = make_formatter(ctx, "test")
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

        runner = TestRunner(project)
        results = runner.run(tests)

        passed = 0
        failed = 0
        test_results: list[dict[str, Any]] = []

        for test_result in results:
            tr: dict[str, Any] = {"name": test_result["name"], "status": test_result["status"]}
            if test_result["status"] == "passed":
                fmt.print(f"[green]PASS[/green]: {test_result['name']}")
                passed += 1
            else:
                fmt.print(f"[red]FAIL[/red]: {test_result['name']}")
                tr["errors"] = test_result.get("errors", [])
                for error in test_result.get("errors", []):
                    fmt.print(f"  - {error}")
                failed += 1
            test_results.append(tr)

        fmt.set_data({"results": test_results, "passed": passed, "failed": failed, "total": passed + failed})
        fmt.print(f"\n{passed} passed, {failed} failed")

        if failed > 0:
            fmt.set_status("error")
        fmt.flush()
        if failed > 0:
            sys.exit(1)

    except (EnvVarError, ParseError, EnvironmentError) as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
