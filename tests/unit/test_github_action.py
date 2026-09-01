"""Tests for the first-party GitHub composite Action adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest
import yaml

from streamt.integrations.github_action import (
    ActionAdapterError,
    ActionConfig,
    CommandExecution,
    annotations_for_execution,
    plan_argv,
    render_summary,
    run_action,
    validation_argv,
)


def _config(tmp_path: Path, *, offline: bool = True) -> ActionConfig:
    project = tmp_path / "project with spaces"
    project.mkdir()
    return ActionConfig(
        workspace=tmp_path,
        project_directory=project,
        environment="staging env",
        offline=offline,
        plan_path=tmp_path / "artifacts" / "reviewed plan.json",
        summary_path=tmp_path / "summary.md",
        output_path=tmp_path / "github-output.txt",
    )


def _execution(
    command: str,
    *,
    status: str = "ok",
    returncode: int = 0,
    data: Optional[dict[str, object]] = None,
    errors: Optional[list[dict[str, object]]] = None,
    warnings: Optional[list[dict[str, object]]] = None,
    stderr: str = "",
) -> CommandExecution:
    payload: dict[str, object] = {
        "status": status,
        "command": command,
        "data": data or {},
        "errors": errors or [],
        "warnings": warnings or [],
    }
    return CommandExecution(
        argv=("python", "-m", "streamt", command),
        returncode=returncode,
        stdout="",
        stderr=stderr,
        payload=payload,
    )


class TestActionConfiguration:
    def test_resolves_inputs_inside_workspace_and_defaults_offline(self, tmp_path: Path):
        project = tmp_path / "pipelines"
        project.mkdir()

        config = ActionConfig.from_environment(
            {
                "GITHUB_WORKSPACE": str(tmp_path),
                "STREAMT_ACTION_PROJECT_DIRECTORY": "pipelines",
                "STREAMT_ACTION_PLAN_PATH": "artifacts/plan.json",
            }
        )

        assert config.project_directory == project
        assert config.plan_path == tmp_path / "artifacts" / "plan.json"
        assert config.offline is True
        assert config.environment is None

    @pytest.mark.parametrize("input_name", ["STREAMT_ACTION_PROJECT_DIRECTORY", "STREAMT_ACTION_PLAN_PATH"])
    def test_rejects_paths_outside_workspace(self, tmp_path: Path, input_name: str):
        env = {
            "GITHUB_WORKSPACE": str(tmp_path),
            "STREAMT_ACTION_PROJECT_DIRECTORY": ".",
            "STREAMT_ACTION_PLAN_PATH": "plan.json",
            input_name: "../outside",
        }

        with pytest.raises(ActionAdapterError, match="inside GITHUB_WORKSPACE"):
            ActionConfig.from_environment(env)

    def test_rejects_non_boolean_offline_input(self, tmp_path: Path):
        with pytest.raises(ActionAdapterError, match="must be 'true' or 'false'"):
            ActionConfig.from_environment(
                {
                    "GITHUB_WORKSPACE": str(tmp_path),
                    "STREAMT_ACTION_OFFLINE": "$(unsafe)",
                }
            )

    def test_argv_keeps_untrusted_inputs_as_single_arguments(self, tmp_path: Path):
        config = _config(tmp_path, offline=False)

        validate = validation_argv(config)
        plan = plan_argv(config)

        assert validate[-3:] == ["--env", "staging env", "--strict"]
        assert str(config.project_directory) in validate
        assert plan[-4:] == ["--env", "staging env", "--out", str(config.plan_path)]
        assert "--offline" not in plan


class TestAnnotationsAndSummary:
    def test_annotations_escape_commands_and_sanitize_credentials(self):
        execution = _execution(
            "validate",
            status="error",
            returncode=1,
            errors=[
                {
                    "code": "E100,BAD:CODE",
                    "message": "bad%value password=hunter2\nnext line",
                    "location": "models.clean",
                }
            ],
        )

        annotations = annotations_for_execution(execution, "validation")

        assert annotations == [
            "::error title=E100%2CBAD%3ACODE::"
            "bad%25value password=*** next line (models.clean)"
        ]
        assert "hunter2" not in annotations[0]

    def test_fallback_annotation_sanitizes_credential_url(self):
        execution = CommandExecution(
            argv=("streamt", "plan"),
            returncode=1,
            stdout="not-json",
            stderr="cannot connect https://alice:super-secret@example.test",
            payload=None,
        )

        annotation = annotations_for_execution(execution, "plan")[0]

        assert "alice" not in annotation
        assert "super-secret" not in annotation
        assert "https://***:***@example.test" in annotation

    def test_failed_strict_validation_emits_error_even_with_warning(self):
        execution = _execution(
            "validate",
            status="error",
            returncode=1,
            warnings=[{"code": "W001", "message": "strict warning"}],
            stderr="strict validation failed",
        )

        annotations = annotations_for_execution(execution, "validation")

        assert annotations[0] == "::warning title=W001::strict warning"
        assert annotations[1] == (
            "::error title=streamt validation::strict validation failed"
        )

    @pytest.mark.parametrize(
        ("stderr", "secrets"),
        [
            (
                "basic.auth.user.info=alice:kafka-password connection rejected",
                ("alice", "kafka-password"),
            ),
            (
                "sasl.jaas.config=PlainLoginModule required username=alice "
                "password=jaas-password; connection rejected",
                ("alice", "jaas-password", "PlainLoginModule"),
            ),
        ],
    )
    def test_fallback_annotation_redacts_sensitive_kafka_properties(
        self,
        stderr: str,
        secrets: tuple[str, ...],
    ):
        execution = CommandExecution(
            argv=("streamt", "plan"),
            returncode=1,
            stdout="not-json",
            stderr=stderr,
            payload=None,
        )

        annotation = annotations_for_execution(execution, "plan")[0]

        assert "=***" in annotation
        for secret in secrets:
            assert secret not in annotation

    def test_summary_renders_plan_changes_requirements_and_checksum(self, tmp_path: Path):
        config = _config(tmp_path)
        validation = _execution("validate")
        plan = _execution(
            "plan",
            data={
                "creates": 1,
                "updates": 2,
                "deletes": 0,
                "changes": [
                    {"action": "create", "type": "topic", "name": "events|raw"}
                ],
                "ownership_requirements": [
                    {
                        "kind": "schema",
                        "logical_name": "raw_events",
                        "reason": "requires_adoption",
                    }
                ],
                "plan_file": str(config.plan_path),
                "plan_checksum": "sha256:" + "a" * 64,
            },
        )

        summary = render_summary(config, validation, plan)

        assert "Validation: ✅ passed" in summary
        assert "| 1 | 2 | 0 |" in summary
        assert "events&#124;raw" in summary
        assert "requires_adoption" in summary
        assert "sha256:" + "a" * 64 in summary


class TestActionExecution:
    def test_success_writes_summary_outputs_and_reviewed_plan(self, tmp_path: Path):
        config = _config(tmp_path)
        calls: list[tuple[str, ...]] = []

        def runner(argv) -> CommandExecution:
            calls.append(tuple(argv))
            if "validate" in argv:
                return _execution("validate")
            config.plan_path.parent.mkdir(parents=True)
            config.plan_path.write_text('{"kind":"streamt.reviewed-plan"}')
            return _execution(
                "plan",
                data={
                    "creates": 1,
                    "updates": 0,
                    "deletes": 0,
                    "changes": [],
                    "ownership_requirements": [],
                    "plan_file": str(config.plan_path),
                    "plan_checksum": "sha256:" + "b" * 64,
                },
            )

        result = run_action(config, runner=runner)

        assert result == 0
        assert len(calls) == 2
        assert calls[1][-1] == "--offline"
        assert "Plan: ✅ created" in config.summary_path.read_text()
        assert config.output_path.read_text().splitlines() == [
            f"plan-path={config.plan_path}",
            f"plan-checksum=sha256:{'b' * 64}",
        ]

    def test_validation_failure_emits_summary_and_skips_plan(self, tmp_path: Path, capsys):
        config = _config(tmp_path)
        calls = 0

        def runner(argv) -> CommandExecution:
            nonlocal calls
            calls += 1
            return _execution(
                "validate",
                status="error",
                returncode=1,
                errors=[{"code": "E001", "message": "invalid project"}],
            )

        result = run_action(config, runner=runner)

        assert result == 1
        assert calls == 1
        assert "::error title=E001::invalid project" in capsys.readouterr().out
        assert "Validation: ❌ failed" in config.summary_path.read_text()
        assert not config.output_path.exists()

    def test_action_metadata_has_no_apply_step(self):
        metadata = yaml.safe_load(Path("action.yml").read_text())
        run_scripts = "\n".join(
            str(step.get("run", "")) for step in metadata["runs"]["steps"]
        )

        assert metadata["runs"]["using"] == "composite"
        assert metadata["runs"]["steps"][0]["uses"] == "actions/setup-python@v6"
        assert metadata["inputs"]["offline"]["default"] == "true"
        assert " apply" not in run_scripts
        assert metadata["outputs"]["plan-checksum"]["value"].endswith(
            "outputs.plan-checksum }}"
        )
