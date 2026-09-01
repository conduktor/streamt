"""GitHub Action adapter for validation and deterministic reviewed plans."""

from __future__ import annotations

import html
import json
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_SENSITIVE_JAAS = re.compile(
    r"(sasl[._-]jaas[._-]config)\s*[=:]\s*[^\r\n]*",
    re.IGNORECASE,
)
_SENSITIVE_KV = re.compile(
    r"(password|passwd|secret|token|api[._-]?key|authorization"
    r"|basic[._-]auth[._-]user[._-]info)\s*[=:]\s*\S+",
    re.IGNORECASE,
)
_SENSITIVE_URL = re.compile(r"://([^:@/\s]+):([^@/\s]+)@")
_CHECKSUM = re.compile(r"^sha256:[0-9a-f]{64}$")


class ActionAdapterError(ValueError):
    """The Action configuration or streamt output is invalid."""


@dataclass(frozen=True)
class ActionConfig:
    """Resolved, validated Action inputs and GitHub file-command paths."""

    workspace: Path
    project_directory: Path
    environment: str | None
    offline: bool
    plan_path: Path
    summary_path: Path | None
    output_path: Path | None

    @classmethod
    def from_environment(cls, environ: Mapping[str, str]) -> ActionConfig:
        """Resolve Action inputs without evaluating them as shell source."""
        workspace_raw = environ.get("GITHUB_WORKSPACE", os.getcwd())
        workspace = Path(workspace_raw).resolve()
        if not workspace.is_dir():
            raise ActionAdapterError(f"GitHub workspace does not exist: {workspace}")

        project_directory = _resolve_workspace_path(
            workspace,
            environ.get("STREAMT_ACTION_PROJECT_DIRECTORY", "."),
            "project-directory",
        )
        if not project_directory.is_dir():
            raise ActionAdapterError(
                f"streamt project directory does not exist: {project_directory}"
            )

        plan_path = _resolve_workspace_path(
            workspace,
            environ.get("STREAMT_ACTION_PLAN_PATH", ".streamt/reviewed-plan.json"),
            "plan-path",
        )
        offline = _parse_boolean(environ.get("STREAMT_ACTION_OFFLINE", "true"), "offline")
        environment = environ.get("STREAMT_ACTION_ENVIRONMENT", "").strip() or None
        summary_path = _optional_command_path(environ.get("GITHUB_STEP_SUMMARY"))
        output_path = _optional_command_path(environ.get("GITHUB_OUTPUT"))
        return cls(
            workspace=workspace,
            project_directory=project_directory,
            environment=environment,
            offline=offline,
            plan_path=plan_path,
            summary_path=summary_path,
            output_path=output_path,
        )


@dataclass(frozen=True)
class CommandExecution:
    """Captured result from one streamt CLI invocation."""

    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    payload: dict[str, object] | None

    @property
    def succeeded(self) -> bool:
        return (
            self.returncode == 0
            and self.payload is not None
            and self.payload.get("status") == "ok"
        )


def _parse_boolean(value: str, label: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ActionAdapterError(f"{label} must be 'true' or 'false', got {value!r}")


def _resolve_workspace_path(workspace: Path, value: str, label: str) -> Path:
    if not value or "\n" in value or "\r" in value or "\x00" in value:
        raise ActionAdapterError(f"{label} must be a non-empty single-line path")
    candidate = Path(value)
    resolved = (workspace / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        resolved.relative_to(workspace)
    except ValueError as exc:
        raise ActionAdapterError(f"{label} must stay inside GITHUB_WORKSPACE") from exc
    return resolved


def _optional_command_path(value: str | None) -> Path | None:
    if not value:
        return None
    if "\n" in value or "\r" in value or "\x00" in value:
        raise ActionAdapterError("GitHub file-command path must be a single-line path")
    return Path(value)


def _parse_payload(stdout: str) -> dict[str, object] | None:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def execute_streamt(argv: Sequence[str]) -> CommandExecution:
    """Execute streamt with an argv sequence and capture its JSON envelope."""
    completed = subprocess.run(  # noqa: S603 - argv is explicit and shell=False
        list(argv),
        check=False,
        capture_output=True,
        text=True,
    )
    return CommandExecution(
        argv=tuple(argv),
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        payload=_parse_payload(completed.stdout),
    )


def _streamt_argv(config: ActionConfig, command: str) -> list[str]:
    argv = [
        sys.executable,
        "-m",
        "streamt",
        "-o",
        "json",
        command,
        "--project-dir",
        str(config.project_directory),
    ]
    if config.environment:
        argv.extend(["--env", config.environment])
    return argv


def validation_argv(config: ActionConfig) -> list[str]:
    """Build the strict validation argv."""
    return [*_streamt_argv(config, "validate"), "--strict"]


def plan_argv(config: ActionConfig) -> list[str]:
    """Build the deterministic reviewed-plan argv."""
    argv = [*_streamt_argv(config, "plan"), "--out", str(config.plan_path)]
    if config.offline:
        argv.append("--offline")
    return argv


def _sanitize(value: object, limit: int = 1000) -> str:
    text = _ANSI_ESCAPE.sub("", str(value))
    text = _SENSITIVE_JAAS.sub(r"\1=***", text)
    text = _SENSITIVE_KV.sub(r"\1=***", text)
    text = _SENSITIVE_URL.sub("://***:***@", text)
    text = " ".join(text.split())
    return text[:limit]


def _github_escape(value: object, *, property_value: bool = False) -> str:
    text = _sanitize(value)
    text = text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    if property_value:
        text = text.replace(":", "%3A").replace(",", "%2C")
    return text


def annotations_for_execution(execution: CommandExecution, stage: str) -> list[str]:
    """Render safe GitHub workflow annotations for errors and warnings."""
    annotations: list[str] = []
    payload = execution.payload or {}
    errors = payload.get("errors", [])
    warnings = payload.get("warnings", [])
    has_error = False

    if isinstance(errors, list):
        for item in errors:
            if not isinstance(item, dict):
                continue
            code = item.get("code", "STREAMT_ERROR")
            message = item.get("message", f"streamt {stage} failed")
            location = item.get("location")
            if location:
                message = f"{message} ({location})"
            annotations.append(
                f"::error title={_github_escape(code, property_value=True)}::"
                f"{_github_escape(message)}"
            )
            has_error = True

    if isinstance(warnings, list):
        for item in warnings:
            if not isinstance(item, dict):
                continue
            code = item.get("code", "STREAMT_WARNING")
            message = item.get("message", "streamt warning")
            annotations.append(
                f"::warning title={_github_escape(code, property_value=True)}::"
                f"{_github_escape(message)}"
            )

    if not execution.succeeded and not has_error:
        fallback = _sanitize(execution.stderr) or f"streamt {stage} failed"
        annotations.append(
            f"::error title=streamt {_github_escape(stage, property_value=True)}::"
            f"{_github_escape(fallback)}"
        )
    return annotations


def _payload_data(execution: CommandExecution | None) -> dict[str, object]:
    if execution is None or execution.payload is None:
        return {}
    data = execution.payload.get("data")
    return data if isinstance(data, dict) else {}


def _markdown_cell(value: object) -> str:
    return html.escape(_sanitize(value, limit=300)).replace("|", "&#124;")


def render_summary(
    config: ActionConfig,
    validation: CommandExecution,
    plan: CommandExecution | None,
) -> str:
    """Render a concise, secret-safe Markdown step summary."""
    validation_ok = validation.succeeded
    plan_ok = plan.succeeded if plan else False
    lines = ["## streamt reviewed plan", ""]
    lines.append(f"- Validation: {'✅ passed' if validation_ok else '❌ failed'}")
    if validation_ok:
        lines.append(f"- Plan: {'✅ created' if plan_ok else '❌ failed'}")
    lines.append(f"- Mode: {'offline' if config.offline else 'online'}")
    lines.append(f"- Project directory: `{_markdown_cell(config.project_directory)}`")
    if config.environment:
        lines.append(f"- Environment: `{_markdown_cell(config.environment)}`")

    if not validation_ok:
        lines.extend(_summary_errors(validation))
        return "\n".join(lines) + "\n"
    if plan is None or not plan_ok:
        if plan is not None:
            lines.extend(_summary_errors(plan))
        return "\n".join(lines) + "\n"

    data = _payload_data(plan)
    lines.extend(
        [
            "",
            "| Creates | Updates | Deletes |",
            "| ---: | ---: | ---: |",
            f"| {_markdown_cell(data.get('creates', 0))} | "
            f"{_markdown_cell(data.get('updates', 0))} | "
            f"{_markdown_cell(data.get('deletes', 0))} |",
        ]
    )

    changes = data.get("changes", [])
    if isinstance(changes, list) and changes:
        lines.extend(["", "### Changes", "", "| Action | Type | Resource |", "| --- | --- | --- |"])
        for change in changes[:50]:
            if not isinstance(change, dict):
                continue
            lines.append(
                f"| {_markdown_cell(change.get('action', ''))} | "
                f"{_markdown_cell(change.get('type', ''))} | "
                f"{_markdown_cell(change.get('name', ''))} |"
            )
        if len(changes) > 50:
            lines.append(f"\n_…and {len(changes) - 50} more change(s)._ ")

    requirements = data.get("ownership_requirements", [])
    if isinstance(requirements, list) and requirements:
        lines.extend(["", "### Ownership requirements", ""])
        for requirement in requirements[:20]:
            if not isinstance(requirement, dict):
                continue
            lines.append(
                f"- `{_markdown_cell(requirement.get('kind', 'resource'))}:"
                f"{_markdown_cell(requirement.get('logical_name', 'unknown'))}` — "
                f"{_markdown_cell(requirement.get('reason', 'ownership required'))}"
            )

    checksum = data.get("plan_checksum")
    plan_file = data.get("plan_file")
    if plan_file:
        lines.extend(["", f"Plan file: `{_markdown_cell(plan_file)}`"])
    if checksum:
        lines.append(f"Plan checksum: `{_markdown_cell(checksum)}`")
    return "\n".join(lines) + "\n"


def _summary_errors(execution: CommandExecution) -> list[str]:
    lines = ["", "### Errors", ""]
    payload = execution.payload or {}
    errors = payload.get("errors", [])
    if isinstance(errors, list):
        for item in errors[:20]:
            if isinstance(item, dict):
                lines.append(f"- {_markdown_cell(item.get('message', 'streamt failed'))}")
    if len(lines) == 3:
        lines.append(f"- {_markdown_cell(execution.stderr or 'streamt failed')}")
    return lines


def _append_summary(path: Path | None, summary: str) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(summary)


def _write_outputs(path: Path | None, values: Mapping[str, str]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for name, value in values.items():
            if "\n" in value or "\r" in value:
                raise ActionAdapterError(f"GitHub output {name!r} must be single-line")
            handle.write(f"{name}={value}\n")


def run_action(
    config: ActionConfig,
    runner: Callable[[Sequence[str]], CommandExecution] = execute_streamt,
) -> int:
    """Run validation and plan, then emit annotations, summary, and outputs."""
    validation = runner(validation_argv(config))
    for annotation in annotations_for_execution(validation, "validation"):
        sys.stdout.write(annotation + "\n")
    if not validation.succeeded:
        _append_summary(config.summary_path, render_summary(config, validation, None))
        return 1

    plan = runner(plan_argv(config))
    for annotation in annotations_for_execution(plan, "plan"):
        sys.stdout.write(annotation + "\n")
    if not plan.succeeded:
        _append_summary(config.summary_path, render_summary(config, validation, plan))
        return 1

    data = _payload_data(plan)
    plan_file = data.get("plan_file")
    checksum = data.get("plan_checksum")
    if not isinstance(plan_file, str) or Path(plan_file).resolve() != config.plan_path:
        raise ActionAdapterError("streamt did not report the requested reviewed plan path")
    if not config.plan_path.is_file():
        raise ActionAdapterError("streamt reported success but did not save the reviewed plan")
    if not isinstance(checksum, str) or not _CHECKSUM.fullmatch(checksum):
        raise ActionAdapterError("streamt did not report a valid reviewed plan checksum")
    _append_summary(config.summary_path, render_summary(config, validation, plan))
    _write_outputs(
        config.output_path,
        {
            "plan-path": str(config.plan_path),
            "plan-checksum": checksum,
        },
    )
    return 0


def main() -> int:
    """Entrypoint used by the composite Action."""
    try:
        config = ActionConfig.from_environment(os.environ)
        return run_action(config)
    except ActionAdapterError as exc:
        message = _github_escape(exc)
        sys.stdout.write(f"::error title=streamt Action::{message}\n")
        summary_path = _optional_command_path(os.environ.get("GITHUB_STEP_SUMMARY"))
        _append_summary(summary_path, f"## streamt reviewed plan\n\n❌ {html.escape(_sanitize(exc))}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
