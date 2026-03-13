"""streamt validate command."""

from __future__ import annotations

import sys
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter
from streamt.core.environment import EnvironmentError, EnvironmentManager
from streamt.core.errors import ErrorCode
from streamt.output import StructuredError, StructuredWarning


@click.command()
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option("--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)")
@click.option("--all-envs", is_flag=True, help="Validate all environments")
@click.option("--check-schemas", is_flag=True, help="Validate schemas against Schema Registry")
@click.option("--model", "-m", "target_model", help="Validate only this model and its dependencies")
@click.pass_context
def validate(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    all_envs: bool,
    check_schemas: bool,
    target_model: Optional[str],
) -> None:
    """Validate project syntax and references."""
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser
    from streamt.core.validator import ProjectValidator

    fmt = make_formatter(ctx, "validate")
    project_path = get_project_path(project_dir)

    if check_schemas:
        fmt.print_warning("--check-schemas is not yet implemented; skipping registry validation")
    if target_model:
        fmt.print_warning(f"--model '{target_model}' filtering is not yet implemented; validating all models")

    def validate_single_env(env_name: Optional[str]) -> dict[str, object]:
        """Validate a single environment. Returns result dict."""
        result_data: dict[str, object] = {"environment": env_name, "valid": False}

        try:
            parser = ProjectParser(
                project_path,
                environment=env_name,
                warn_callback=lambda msg: fmt.print(msg),
            )
            project = parser.parse()
            validator = ProjectValidator(project)
            result = validator.validate()

            warns: list[dict[str, object]] = []
            for warning in result.warnings:
                w: dict[str, object] = {"message": warning.message}
                if warning.location:
                    w["location"] = warning.location
                warns.append(w)
                fmt.add_warning(StructuredWarning(
                    code="W000_VALIDATION_WARNING", message=warning.message, location=warning.location,
                ))
                fmt.print_warning(warning.message)
                if warning.location:
                    fmt.print(f"  Location: {warning.location}")

            if result.errors:
                errs: list[dict[str, object]] = []
                for error in result.errors:
                    e: dict[str, object] = {"message": error.message}
                    if error.location:
                        e["location"] = error.location
                    errs.append(e)
                    fmt.add_error(StructuredError(
                        code=ErrorCode.PARSE_ERROR, message=error.message, location=error.location,
                    ))
                    fmt.print_error(error.message)
                    if error.location:
                        fmt.print(f"  Location: {error.location}")
                result_data["errors"] = errs
                result_data["warnings"] = warns
                return result_data

            env_label = f" ({env_name})" if env_name else ""
            fmt.print(f"[green]Project '{project.project.name}'{env_label} is valid[/green]")

            summary = {
                "sources": len(project.sources),
                "models": len(project.models),
                "tests": len(project.tests),
                "exposures": len(project.exposures),
            }
            result_data.update(summary)
            result_data["valid"] = True
            result_data["project_name"] = project.project.name
            result_data["warnings"] = warns

            fmt.print_table(
                "Project Summary",
                [("Type", "cyan"), ("Count", "green")],
                [[k.capitalize(), str(v)] for k, v in summary.items()],
            )

            if project.rules:
                result_data["governance_passed"] = True
                fmt.print("[green]All governance rules passed[/green]")

            return result_data

        except EnvVarError as e:
            fmt.add_error(StructuredError(code=ErrorCode.ENV_VAR_ERROR, message=str(e)))
            fmt.print_error(str(e))
            return {"environment": env_name, "valid": False, "errors": [{"message": str(e)}]}
        except ParseError as e:
            fmt.add_error(StructuredError(code=ErrorCode.PARSE_ERROR, message=str(e)))
            fmt.print_error(str(e))
            return {"environment": env_name, "valid": False, "errors": [{"message": str(e)}]}
        except EnvironmentError as e:
            fmt.add_error(StructuredError(code=ErrorCode.ENVIRONMENT_ERROR, message=str(e)))
            fmt.print_error(str(e))
            return {"environment": env_name, "valid": False, "errors": [{"message": str(e)}]}

    try:
        if all_envs:
            env_manager = EnvironmentManager(project_path)
            if env_manager.mode == "single":
                fmt.add_error(StructuredError(
                    code=ErrorCode.ENVIRONMENT_ERROR,
                    message="--all-envs requires multi-environment mode.",
                ))
                fmt.print_error("--all-envs requires multi-environment mode. Create an environments/ directory.")
                fmt.flush()
                sys.exit(1)

            environments = env_manager.discover_environments()
            if not environments:
                fmt.add_error(StructuredError(
                    code=ErrorCode.ENVIRONMENT_ERROR,
                    message="No environment files found in environments/ directory.",
                ))
                fmt.print_error("No environment files found in environments/ directory.")
                fmt.flush()
                sys.exit(1)

            fmt.print(f"Validating {len(environments)} environments...\n")
            all_results = []
            all_valid = True
            for env_name in environments:
                fmt.print(f"[cyan]--- Environment: {env_name} ---[/cyan]")
                r = validate_single_env(env_name)
                all_results.append(r)
                if not r.get("valid"):
                    all_valid = False
                fmt.print("")

            fmt.set_data({"environments": all_results, "all_valid": all_valid})
            if not all_valid:
                fmt.set_status("error")
                fmt.print_error("Some environments failed validation")
                fmt.flush()
                sys.exit(1)
            fmt.print(f"[green]All {len(environments)} environments are valid[/green]")
            fmt.flush()
        else:
            r = validate_single_env(environment)
            fmt.set_data(r)
            if not r.get("valid"):
                fmt.set_status("error")
                fmt.flush()
                sys.exit(1)
            fmt.flush()

    except EnvironmentError as e:
        handle_parse_error(fmt, e, ErrorCode.ENVIRONMENT_ERROR)
    except Exception as e:
        handle_parse_error(fmt, e, ErrorCode.PARSE_ERROR)
