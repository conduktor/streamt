"""CLI ordering and fail-closed tests for deployment-state configuration."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.deployer.planner import DeploymentPlan


def _runtime() -> dict[str, object]:
    return {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}}


def _write_single_project(path: Path, *, postgres: bool) -> None:
    data: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "remote-config-test"},
        "runtime": _runtime(),
    }
    if postgres:
        data["deployment_state"] = {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "STREAMT_STATE_DSN"},
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(data),
        encoding="utf-8",
    )


def _write_policy_project(path: Path, *, protected: bool = False) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "remote-config-test"},
                "deployment_state": {"backend": "local"},
            }
        ),
        encoding="utf-8",
    )
    environments = path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev", "protected": protected},
                "runtime": _runtime(),
                "safety": {"require_remote_state": True},
            }
        ),
        encoding="utf-8",
    )


def _payload(result: Result) -> dict[str, object]:
    return json.loads(result.stdout)


def _assert_no_runtime_deployer_patches(stack: ExitStack, command: str) -> None:
    for factory in (
        "make_sr_deployer",
        "make_kafka_deployer",
        "make_flink_deployer",
        "make_connect_deployer",
        "make_gateway_deployer",
    ):
        stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.{factory}",
                side_effect=AssertionError("runtime deployer constructed before state"),
            )
        )


def test_online_plan_missing_postgres_dsn_fails_before_runtime_or_local(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=True)
    with ExitStack() as stack:
        _assert_no_runtime_deployer_patches(stack, "plan")
        stack.enter_context(
            patch(
                "streamt.deployer.state_backend.LocalDeploymentStateBackend",
                side_effect=AssertionError("remote config fell back to local"),
            )
        )
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
            env={"STREAMT_STATE_DSN": ""},
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    assert not (tmp_path / ".streamt").exists()


def test_offline_plan_does_not_resolve_postgres_dsn_or_construct_factory(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=True)
    plan_path = tmp_path / "offline.plan.json"
    with patch(
        "streamt.cli.commands.plan.make_deployment_state_service",
        side_effect=AssertionError("offline plan constructed a state provider"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "plan",
                "-p",
                str(tmp_path),
                "--offline",
                "--out",
                str(plan_path),
            ],
            env={"STREAMT_STATE_DSN": ""},
        )

    assert result.exit_code == 0, result.output
    factory.assert_not_called()
    assert json.loads(plan_path.read_text(encoding="utf-8"))["state"] is None


def test_apply_missing_postgres_dsn_fails_before_runtime_or_local(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=True)
    with ExitStack() as stack:
        _assert_no_runtime_deployer_patches(stack, "apply")
        stack.enter_context(
            patch(
                "streamt.deployer.state_backend.LocalDeploymentStateBackend",
                side_effect=AssertionError("remote config fell back to local"),
            )
        )
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
            env={"STREAMT_STATE_DSN": ""},
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"


def test_state_status_present_dsn_is_sanitized_unavailable_without_local_read(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=True)
    secret_dsn = "postgresql://alice:secret@db.internal/state?sslmode=require"
    with patch(
        "streamt.deployer.state_backend.LocalDeploymentStateBackend",
        side_effect=AssertionError("remote config fell back to local"),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "status", "-p", str(tmp_path)],
            env={"STREAMT_STATE_DSN": secret_dsn},
        )

    assert result.exit_code == 1, result.output
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "secret" not in serialized
    assert "db.internal" not in serialized
    assert "sslmode" not in serialized
    assert "STREAMT_STATE_DSN" not in serialized
    assert not (tmp_path / ".streamt").exists()


@pytest.mark.parametrize("extra_args", [[], ["--force"]])
def test_require_remote_state_blocks_apply_before_confirmation_compile_or_state(
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    _write_policy_project(tmp_path)
    with (
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("compiler constructed before state policy"),
        ),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            side_effect=AssertionError("state factory constructed before policy"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "-e",
                "dev",
                *extra_args,
            ],
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E421_REMOTE_STATE_REQUIRED"
    assert "confirmation" not in result.output.lower()


def test_require_remote_state_blocks_adopt_before_compile_state_or_runtime(
    tmp_path: Path,
) -> None:
    _write_policy_project(tmp_path)
    with (
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("compiler constructed before state policy"),
        ),
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            side_effect=AssertionError("state factory constructed before policy"),
        ),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            side_effect=AssertionError("runtime deployer constructed before policy"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "adopt",
                "-p",
                str(tmp_path),
                "-e",
                "dev",
                "--kind",
                "topic",
                "--name",
                "anything",
            ],
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E421_REMOTE_STATE_REQUIRED"


def test_reviewed_plan_required_keeps_precedence_over_remote_state_policy(
    tmp_path: Path,
) -> None:
    _write_policy_project(tmp_path, protected=True)

    result = CliRunner().invoke(
        main,
        ["-o", "json", "apply", "-p", str(tmp_path), "-e", "dev"],
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E418_REVIEWED_PLAN_REQUIRED"


def test_offline_reviewed_plan_keeps_precedence_over_remote_state_policy(
    tmp_path: Path,
) -> None:
    _write_policy_project(tmp_path)
    plan_path = tmp_path / "offline.plan.json"
    created = CliRunner().invoke(
        main,
        [
            "plan",
            "-p",
            str(tmp_path),
            "-e",
            "dev",
            "--offline",
            "--out",
            str(plan_path),
        ],
    )
    assert created.exit_code == 0, created.output

    result = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "-e",
            "dev",
            "--plan",
            str(plan_path),
        ],
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_read_only_plan_and_state_status_are_not_blocked_by_remote_policy(
    tmp_path: Path,
) -> None:
    _write_policy_project(tmp_path)
    with ExitStack() as stack:
        for factory in (
            "make_sr_deployer",
            "make_kafka_deployer",
            "make_flink_deployer",
            "make_connect_deployer",
            "make_gateway_deployer",
        ):
            stack.enter_context(
                patch(f"streamt.cli.commands.plan.{factory}", return_value=None)
            )
        stack.enter_context(
            patch(
                "streamt.cli.commands.plan.check_required_deployers",
                return_value=True,
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=DeploymentPlan(),
            )
        )
        planned = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path), "-e", "dev"],
        )

    assert planned.exit_code == 0, planned.output
    assert _payload(planned)["data"]["state_serial"] == 0
    assert not (tmp_path / ".streamt").exists()

    status = CliRunner().invoke(
        main,
        ["-o", "json", "state", "status", "-p", str(tmp_path), "-e", "dev"],
    )

    assert status.exit_code == 0, status.output
    assert _payload(status)["data"]["backend"] == "local"
    assert _payload(status)["data"]["state_status"] == "absent"
    assert not (tmp_path / ".streamt").exists()


def test_adopt_missing_postgres_dsn_fails_before_runtime_or_local(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=True)
    topic = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project="remote-config-test",
            owner_type="model",
            owner_name="orders",
            mode="adopted",
        ),
    )
    manifest = Manifest(
        version="1.0",
        project_name="remote-config-test",
        artifacts={"topics": [topic.to_dict()]},
    )
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            side_effect=AssertionError("runtime deployer constructed before state"),
        ),
        patch(
            "streamt.deployer.state_backend.LocalDeploymentStateBackend",
            side_effect=AssertionError("remote config fell back to local"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "adopt",
                "-p",
                str(tmp_path),
                "-e",
                "default",
                "--kind",
                "topic",
                "--name",
                "orders",
            ],
            env={"STREAMT_STATE_DSN": ""},
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"


def test_malformed_provider_config_fails_before_compiler_state_or_runtime(
    tmp_path: Path,
) -> None:
    _write_single_project(tmp_path, postgres=False)
    data = yaml.safe_load((tmp_path / "stream_project.yml").read_text())
    data["deployment_state"] = {"backend": "local", "namespace": "forbidden"}
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(data))
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "streamt.compiler.Compiler",
                side_effect=AssertionError("compiler constructed for malformed config"),
            )
        )
        stack.enter_context(
            patch(
                "streamt.cli.commands.plan.make_deployment_state_service",
                side_effect=AssertionError("state constructed for malformed config"),
            )
        )
        _assert_no_runtime_deployer_patches(stack, "plan")
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E501_PARSE_ERROR"


def test_state_status_help_describes_configured_provider_not_local_only() -> None:
    result = CliRunner().invoke(main, ["state", "status", "--help"])

    assert result.exit_code == 0, result.output
    assert "configured ownership" in result.output
    assert "safe local ownership" not in result.output.lower()
