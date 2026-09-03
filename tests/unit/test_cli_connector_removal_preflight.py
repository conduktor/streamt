"""CLI ordering for authorized Connector removal provider-free preflight."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Literal, cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.connector_artifact import (
    CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE,
)
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.deployer.connect import ConnectClusterBinding
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import LocalState, ManagedResourceRecord, resource_id
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationControlState,
    OperationIntent,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

_ENDPOINT = "https://connect.example.test:8443/api/"
_CHECKSUM = "sha256:" + "1" * 64
_STORE_ID = "11111111-1111-4111-8111-111111111111"
_RUNTIME_FACTORIES = (
    "make_sr_deployer",
    "make_kafka_deployer",
    "make_flink_deployer",
    "make_connect_deployer",
    "make_gateway_deployer",
)


def _write_project(
    path: Path,
    *,
    removal_cluster: str = "primary",
    include_connect: bool = True,
    include_removal: bool = True,
    postgres: bool = True,
) -> None:
    runtime: dict[str, object] = {
        "kafka": {"bootstrap_servers": "broker.invalid:9092"},
    }
    if include_connect:
        runtime["connect"] = {
            "default": "primary",
            "clusters": {
                "primary": {"rest_url": _ENDPOINT},
                "secondary": {"rest_url": "https://secondary-connect.example.test"},
            },
        }
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "payments", "version": "1.0.0"},
        "runtime": runtime,
        "deployment_state": (
            {
                "backend": "postgres",
                "namespace": "test",
                "postgres": {
                    "dsn_env": "STREAMT_TEST_ADMIN_DSN",
                    "writer_dsn_env": "STREAMT_TEST_WRITER_DSN",
                },
            }
            if postgres
            else {"backend": "local"}
        ),
    }
    if include_removal:
        config["lifecycle"] = {
            "connector_removals": [
                {
                    "logical_owner": "archive_orders",
                    "name": "archive-orders-sink",
                    "cluster": removal_cluster,
                }
            ]
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _payload(result: Result) -> dict[str, object]:
    return cast(dict[str, object], json.loads(result.stdout))


def _desired_only_manifest() -> Manifest:
    connector = ConnectorArtifact(
        name="current-orders-sink",
        connector_class="com.example.Sink",
        topics=["orders.v1"],
        cluster="primary",
        config={"tasks.max": 1},
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="current_orders",
            mode="managed",
        ),
    )
    return Manifest(
        version="1.0.0",
        project_name="payments",
        artifacts={"connectors": [connector.to_dict()]},
    )


def _state_service(
    state: LocalState,
    *,
    backend: str = "postgres",
) -> tuple[MagicMock, MagicMock]:
    address = StateAddress(namespace="test", project="payments", environment="default")
    state_observation = StateObservation(
        store=StateStoreIdentity(backend=backend, store_id=_STORE_ID),
        address=address,
        state=state,
        revision=StateRevision("1"),
    )
    snapshot = OperationSnapshot(
        state=state_observation,
        control=ControlObservation(
            control=OperationControlState.clear(address),
            revision=StateRevision("1"),
        ),
    )
    operation = MagicMock()
    operation.observe.return_value = snapshot

    @contextmanager
    def locked() -> Iterator[MagicMock]:
        yield operation

    service = MagicMock()
    service.operation.side_effect = locked
    return service, operation


def _invoke_authorized(
    path: Path,
    command: str,
    *,
    service: MagicMock | None = None,
    state_error: Exception | None = None,
) -> tuple[Result, MagicMock, list[MagicMock], MagicMock | None]:
    operation = MagicMock()
    reviewed: MagicMock | None = None
    if service is None and state_error is None:
        service, operation = _state_service(LocalState(project="payments", environment="default"))

    with ExitStack() as stack:
        runtime_factories = [
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.{factory}",
                    side_effect=AssertionError(f"{factory} crossed Connector removal preflight"),
                )
            )
            for factory in _RUNTIME_FACTORIES
        ]
        state_factory = stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.make_deployment_state_service",
                return_value=service,
                side_effect=state_error,
            )
        )
        args = ["-o", "json", command, "-p", str(path)]
        if command == "plan":
            args.extend(["--out", str(path / "reviewed.json")])
        else:
            args.extend(["--plan", str(path / "reviewed.json")])
            reviewed = MagicMock(offline=False)
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.ReviewedPlanFile.load",
                    return_value=reviewed,
                )
            )
        result = CliRunner().invoke(main, args)
    state_factory.assert_called_once()
    return result, operation, runtime_factories, reviewed


def _assert_no_state_writes(operation: MagicMock) -> None:
    for method in (
        "begin_operation",
        "record_progress",
        "clear_operation",
        "clear_before_mutation",
        "mark_recovery_required",
        "commit_operation",
        "compare_and_swap",
        "check_lock",
        "finalize_recovery",
    ):
        getattr(operation, method).assert_not_called()


def _invoke_operation_boundary_failure(
    path: Path,
    command: str,
    *,
    service: MagicMock,
) -> tuple[Result, list[MagicMock], MagicMock, MagicMock | None]:
    reviewed: MagicMock | None = None
    with ExitStack() as stack:
        factories = [
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.{factory}",
                    side_effect=AssertionError(
                        f"{factory} crossed Connector removal state precedence"
                    ),
                )
            )
            for factory in _RUNTIME_FACTORIES
        ]
        stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.make_deployment_state_service",
                return_value=service,
            )
        )
        resolver = stack.enter_context(
            patch("streamt.deployer.planner.resolve_connector_planning_targets")
        )
        args = ["-o", "json", command, "-p", str(path)]
        if command == "plan":
            args.extend(["--out", str(path / "reviewed.json")])
        else:
            args.extend(["--plan", str(path / "reviewed.json")])
            reviewed = MagicMock(offline=False)
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.ReviewedPlanFile.load",
                    return_value=reviewed,
                )
            )
        result = CliRunner().invoke(main, args)
    return result, factories, resolver, reviewed


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_nondefault_alias_is_e209_before_runtime_or_state_write(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path, removal_cluster="secondary")
    result, operation, factories, _reviewed = _invoke_authorized(tmp_path, command)

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E209_INVALID_CLUSTER_REF"
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_missing_connect_runtime_is_e205_before_runtime_or_state_write(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path, include_connect=False)
    result, operation, factories, _reviewed = _invoke_authorized(tmp_path, command)

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E205_CONNECT_REQUIRED"
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_prior_provider_collision_is_e427_before_runtime_or_state_write(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path)
    old_alias = ConnectClusterBinding.from_endpoint(
        "retired-default",
        "https://CONNECT.EXAMPLE.TEST:8443/api",
    )
    state = LocalState(
        project="payments",
        environment="default",
        resources={
            resource_id("payments", "default", "connector", "other_owner"): ManagedResourceRecord(
                physical_name="archive-orders-sink",
                ownership="managed",
                artifact_checksum=_CHECKSUM,
                backend=old_alias.backend_identity,
            )
        },
    )
    service, operation = _state_service(state)
    result, _, factories, _reviewed = _invoke_authorized(
        tmp_path,
        command,
        service=service,
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E427_CONNECTOR_REMOVAL_INVALID"
    assert _ENDPOINT not in result.output
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_invalid_postgres_v2_authority_is_e420_before_runtime(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path)
    result, operation, factories, _reviewed = _invoke_authorized(
        tmp_path,
        command,
        state_error=StateBackendInvalidStateError(
            "PostgreSQL deployment state authority is invalid"
        ),
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
@pytest.mark.parametrize("stage", ["enter", "observe", "ensure_ready"])
def test_invalid_state_from_operation_boundary_is_e420_before_resolver_or_runtime(
    tmp_path: Path,
    command: str,
    stage: str,
) -> None:
    _write_project(tmp_path)
    service, operation = _state_service(LocalState(project="payments", environment="default"))
    error = StateBackendInvalidStateError(
        "PostgreSQL operation authority is invalid password=state-secret"
    )
    if stage == "enter":
        context = MagicMock()
        context.__enter__.side_effect = error
        service.operation.side_effect = None
        service.operation.return_value = context
    elif stage == "observe":
        operation.observe.side_effect = error
    else:
        operation.ensure_ready.side_effect = error

    result, factories, resolver, reviewed = _invoke_operation_boundary_failure(
        tmp_path,
        command,
        service=service,
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    assert "state-secret" not in result.output
    resolver.assert_not_called()
    if reviewed is not None:
        reviewed.verify_context.assert_not_called()
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


def _active_control_service(
    status: Literal["in_progress", "recovery_required"],
) -> tuple[MagicMock, MagicMock]:
    state = LocalState(project="payments", environment="default")
    address = StateAddress(namespace="test", project="payments", environment="default")
    operation_id = "00000000-0000-4000-8000-000000000077"
    intent = OperationIntent(
        operation_id=operation_id,
        kind="apply",
        started_at="2026-09-03T00:00:00Z",
        actor="test",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(),
    )
    recovery = (
        RecoveryRecord(
            operation_id=operation_id,
            failure_code="operation_interrupted",
            failed_at="2026-09-03T00:01:00Z",
            last_completed_action_index=None,
        )
        if status == "recovery_required"
        else None
    )
    snapshot = OperationSnapshot(
        state=StateObservation(
            store=StateStoreIdentity(backend="postgres", store_id=_STORE_ID),
            address=address,
            state=state,
            revision=StateRevision("1"),
        ),
        control=ControlObservation(
            control=OperationControlState(
                address=address,
                status=status,
                intent=intent,
                recovery=recovery,
            ),
            revision=StateRevision("1"),
        ),
    )
    service, operation = _state_service(state)
    operation.observe.return_value = snapshot

    def reject_active_control(observed: OperationSnapshot) -> None:
        assert observed is snapshot
        assert observed.control.control.status == status
        raise StateBackendRecoveryRequiredError(
            "deployment state has an unfinished operation; recovery is required"
        )

    operation.ensure_ready.side_effect = reject_active_control
    return service, operation


@pytest.mark.parametrize("command", ["plan", "apply"])
@pytest.mark.parametrize("status", ["in_progress", "recovery_required"])
def test_pending_control_is_e419_before_resolver_review_or_runtime(
    tmp_path: Path,
    command: str,
    status: Literal["in_progress", "recovery_required"],
) -> None:
    _write_project(tmp_path)
    service, operation = _active_control_service(status)

    result, factories, resolver, reviewed = _invoke_operation_boundary_failure(
        tmp_path,
        command,
        service=service,
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E419_STATE_RECOVERY_REQUIRED"
    resolver.assert_not_called()
    if reviewed is not None:
        reviewed.verify_context.assert_not_called()
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_lock_timeout_is_e422_before_resolver_review_or_runtime(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path)
    service, operation = _state_service(LocalState(project="payments", environment="default"))
    context = MagicMock()
    context.__enter__.side_effect = StateBackendLockTimeoutError(
        "state lock timeout password=lock-secret"
    )
    service.operation.side_effect = None
    service.operation.return_value = context

    result, factories, resolver, reviewed = _invoke_operation_boundary_failure(
        tmp_path,
        command,
        service=service,
    )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E422_STATE_LOCK_TIMEOUT"
    assert "lock-secret" not in result.output
    resolver.assert_not_called()
    if reviewed is not None:
        reviewed.verify_context.assert_not_called()
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_successful_preflight_fails_closed_before_plan_or_provider(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path)
    result, operation, factories, reviewed = _invoke_authorized(tmp_path, command)

    assert result.exit_code == 1, result.output
    error = _payload(result)["errors"][0]
    assert error == {
        "code": "E427_CONNECTOR_REMOVAL_INVALID",
        "message": CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE,
    }
    assert not (tmp_path / "reviewed.json").exists()
    if reviewed is not None:
        reviewed.verify_context.assert_not_called()
    for factory in factories:
        factory.assert_not_called()
    _assert_no_state_writes(operation)


@pytest.mark.parametrize(
    ("command", "extra_args"),
    [("plan", []), ("apply", ["--dry-run"])],
)
def test_desired_only_local_workflow_retains_w106_compatibility(
    tmp_path: Path,
    command: str,
    extra_args: list[str],
) -> None:
    _write_project(tmp_path, include_removal=False, postgres=False)
    service, _operation = _state_service(
        LocalState(project="payments", environment="default"),
        backend="local",
    )

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "streamt.compiler.Compiler.compile",
                return_value=_desired_only_manifest(),
            )
        )
        stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.make_deployment_state_service",
                return_value=service,
            )
        )
        for factory in _RUNTIME_FACTORIES:
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.{factory}",
                    return_value=MagicMock(),
                )
            )
        stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.check_required_deployers",
                return_value=True,
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.plan",
                return_value=DeploymentPlan(),
            )
        )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.DeploymentPlanner.planned_actions",
                return_value=[],
            )
        )
        result = CliRunner().invoke(
            main,
            ["-o", "json", command, "-p", str(tmp_path), *extra_args],
        )

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    warnings = payload["warnings"]
    assert isinstance(warnings, list)
    assert any(
        isinstance(warning, dict) and warning.get("code") == "W106_LOCAL_STATE_ONLY"
        for warning in warnings
    )


def test_late_invalid_reviewed_state_with_desired_only_manifest_remains_e411() -> None:
    runner = CliRunner()
    with runner.isolated_filesystem():
        project_path = Path.cwd()
        _write_project(project_path, include_removal=False)
        service, operation = _state_service(LocalState(project="payments", environment="default"))
        reviewed = MagicMock(offline=False)
        reviewed.verify_context.side_effect = StateBackendInvalidStateError(
            "late reviewed state is invalid"
        )

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "streamt.compiler.Compiler.compile",
                    return_value=_desired_only_manifest(),
                )
            )
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.make_deployment_state_service",
                    return_value=service,
                )
            )
            for factory in _RUNTIME_FACTORIES:
                stack.enter_context(
                    patch(
                        f"streamt.cli.commands.apply.{factory}",
                        side_effect=AssertionError(f"{factory} crossed late state validation"),
                    )
                )
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.ReviewedPlanFile.load",
                    return_value=reviewed,
                )
            )
            result = runner.invoke(
                main,
                [
                    "-o",
                    "json",
                    "apply",
                    "-p",
                    str(project_path),
                    "--plan",
                    str(project_path / "reviewed.json"),
                ],
            )

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0]["code"] == "E411_STATE_INVALID"
    _assert_no_state_writes(operation)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_exact_locked_preflight_trace_precedes_review_and_runtime(
    tmp_path: Path,
    command: str,
) -> None:
    _write_project(tmp_path)
    events: list[str] = []
    lock_active = False
    state = LocalState(project="payments", environment="default")
    address = StateAddress(namespace="test", project="payments", environment="default")
    snapshot = OperationSnapshot(
        state=StateObservation(
            store=StateStoreIdentity(backend="postgres", store_id=_STORE_ID),
            address=address,
            state=state,
            revision=StateRevision("1"),
        ),
        control=ControlObservation(
            control=OperationControlState.clear(address),
            revision=StateRevision("1"),
        ),
    )
    operation = MagicMock()
    operation.observe.side_effect = lambda: (events.append("observe"), snapshot)[1]
    operation.ensure_ready.side_effect = lambda _snapshot: events.append("ensure-ready")

    @contextmanager
    def locked() -> Iterator[MagicMock]:
        nonlocal lock_active
        events.append("enter-operation")
        lock_active = True
        try:
            yield operation
        finally:
            assert lock_active
            lock_active = False
            events.append("exit-operation")

    service = MagicMock()
    service.operation.side_effect = lambda: (events.append("operation"), locked())[1]

    def resolve_successfully(*_args: object, **_kwargs: object) -> MagicMock:
        assert lock_active
        events.append("resolver")
        return MagicMock()

    with ExitStack() as stack:
        stack.enter_context(
            patch(
                f"streamt.cli.commands.{command}.make_deployment_state_service",
                side_effect=lambda *_args, **_kwargs: (
                    events.append("state-factory"),
                    service,
                )[1],
            )
        )
        for factory in _RUNTIME_FACTORIES:
            stack.enter_context(
                patch(
                    f"streamt.cli.commands.{command}.{factory}",
                    side_effect=lambda *args, _factory=factory, **kwargs: events.append(
                        f"runtime:{_factory}"
                    ),
                )
            )
        stack.enter_context(
            patch(
                "streamt.deployer.planner.resolve_connector_planning_targets",
                side_effect=resolve_successfully,
            )
        )
        args = ["-o", "json", command, "-p", str(tmp_path)]
        reviewed = MagicMock(offline=False)
        reviewed.verify_context.side_effect = lambda *_args, **_kwargs: events.append(
            "reviewed-context"
        )
        if command == "plan":
            args.extend(["--out", str(tmp_path / "reviewed.json")])
        else:
            args.extend(["--plan", str(tmp_path / "reviewed.json")])
            stack.enter_context(
                patch(
                    "streamt.cli.commands.apply.ReviewedPlanFile.load",
                    return_value=reviewed,
                )
            )
        result = CliRunner().invoke(main, args)

    assert result.exit_code == 1, result.output
    assert _payload(result)["errors"][0] == {
        "code": "E427_CONNECTOR_REMOVAL_INVALID",
        "message": CONNECTOR_REMOVAL_PLANNING_UNAVAILABLE_MESSAGE,
    }
    assert lock_active is False
    assert events == [
        "state-factory",
        "operation",
        "enter-operation",
        "observe",
        "ensure-ready",
        "resolver",
        "exit-operation",
    ]
    reviewed.verify_context.assert_not_called()
    _assert_no_state_writes(operation)
