"""Focused CLI tests for alias-only Conduktor Gateway adoption."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import (
    ArtifactOwnership,
    GatewayRuleArtifact,
    Manifest,
)
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule,
)
from streamt.deployer.gateway_adoption import gateway_alias_mapping_checksum
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    desired_managed_records,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    OperationAction,
    make_deployment_state_service,
)

_PROJECT = "gateway-adoption-test"
_ENVIRONMENT = "default"
_OWNER = "orders_view"
_RULE = "orders_access_rule"
_ALIAS = "orders.public"
_DESIRED_TOPIC = "orders.desired.private"
_OBSERVED_TOPIC = "orders.observed.private"
_ADMIN_URL = "https://gateway.example.test/admin"
_VCLUSTER = "payments"
_RESOURCE = resource_id(
    _PROJECT,
    _ENVIRONMENT,
    "gateway_rule",
    _OWNER,
)


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                    "conduktor": {
                        "gateway": {
                            "admin_url": _ADMIN_URL,
                            "username": "gateway-runtime-user",
                            "password": "gateway-runtime-password",
                            "virtual_cluster": _VCLUSTER,
                        }
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _binding(
    *,
    endpoint: str = _ADMIN_URL,
    virtual_cluster: str = _VCLUSTER,
) -> GatewayBackendBinding:
    return GatewayBackendBinding.from_endpoint(
        endpoint,
        virtual_cluster=virtual_cluster,
    )


def _artifact(
    *,
    owner: str = _OWNER,
    rule: str = _RULE,
    alias: str = _ALIAS,
    physical_topic: str = _DESIRED_TOPIC,
    project: str = _PROJECT,
    owner_type: str = "model",
    mode: str = "adopted",
    interceptors: list[dict[str, object]] | None = None,
) -> GatewayRuleArtifact:
    return GatewayRuleArtifact(
        name=rule,
        virtual_topic=alias,
        physical_topic=physical_topic,
        interceptors=[] if interceptors is None else interceptors,
        ownership=ArtifactOwnership(
            project=project,
            owner_type=owner_type,
            owner_name=owner,
            mode=mode,
        ),
    )


def _manifest(*artifacts: GatewayRuleArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name=_PROJECT,
        artifacts={
            "gateway_rules": [artifact.to_dict() for artifact in artifacts],
            "gateway_rule_removals": [],
        },
    )


def _observation(
    *,
    physical_topic: str = _OBSERVED_TOPIC,
    binding: GatewayBackendBinding | None = None,
    exists: bool = True,
    with_interceptor: bool = False,
) -> ManagedGatewayRuleObservation:
    effective_binding = binding or _binding()
    if not exists:
        return ManagedGatewayRuleObservation(
            binding=effective_binding,
            logical_name=_RULE,
            alias_name=_ALIAS,
            exists=False,
        )
    if with_interceptor:
        return build_desired_gateway_rule(
            _artifact(
                physical_topic=physical_topic,
                interceptors=[{"type": "filter", "config": {"where": "amount > 0"}}],
            ),
            effective_binding,
        )
    return ManagedGatewayRuleObservation(
        binding=effective_binding,
        logical_name=_RULE,
        alias_name=_ALIAS,
        exists=True,
        physical_name=physical_topic,
        physical_cluster="main",
        interceptors=(),
    )


def _gateway(
    *observations: ManagedGatewayRuleObservation,
    binding: GatewayBackendBinding | None = None,
) -> MagicMock:
    deployer = MagicMock(spec=GatewayDeployer)
    deployer.cluster_binding = binding or _binding()
    values = observations or (_observation(),)
    snapshots: list[MagicMock] = []
    for observation in values:
        snapshot = MagicMock()
        snapshot.binding = observation.binding
        snapshot.rule.return_value = observation
        snapshots.append(snapshot)
    deployer.observe_managed_gateway_snapshot.side_effect = snapshots
    deployer.test_snapshots = snapshots
    return deployer


def _invoke(path: Path, *, confirm: bool = True) -> Result:
    args = [
        "-o",
        "json",
        "adopt",
        "-p",
        str(path),
        "-e",
        _ENVIRONMENT,
        "--kind",
        "gateway_rule",
        "--name",
        _OWNER,
    ]
    if confirm:
        args.extend(
            [
                "--confirm-resource",
                _RESOURCE,
                "--confirm-env",
                _ENVIRONMENT,
            ]
        )
    return CliRunner().invoke(main, args)


def _payload(result: Result) -> dict[str, Any]:
    return json.loads(result.stdout)  # type: ignore[no-any-return]


def _patch_adoption(
    manifest: Manifest,
    gateway: MagicMock,
) -> tuple[Any, Any]:
    return (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.adopt.make_gateway_deployer",
            return_value=gateway,
        ),
    )


def _assert_no_gateway_mutation(gateway: MagicMock) -> None:
    for method in (
        "apply_managed_gateway_rule",
        "delete_managed_gateway_rule",
        "create_interceptor",
        "delete_interceptor",
        "create_alias_topic",
        "delete_alias_topic",
        "apply",
        "delete",
    ):
        getattr(gateway, method).assert_not_called()


def test_success_allows_mapping_difference_and_emits_only_secret_neutral_evidence(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _artifact()
    observed = _observation()
    gateway = _gateway(observed, observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(artifact), gateway)

    with (
        compiler_patch,
        gateway_patch,
        patch(
            "streamt.cli.commands.adopt.OperationAction",
            wraps=OperationAction,
        ) as action_factory,
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    data = _payload(result)["data"]
    assert data == {
        **data,
        "resource_id": _RESOURCE,
        "kind": "gateway_rule",
        "effective_vcluster": _VCLUSTER,
        "endpoint_fingerprint": _binding().endpoint_fingerprint,
        "alias_name": _ALIAS,
        "physical_cluster": "main",
        "observed_mapping_checksum": gateway_alias_mapping_checksum(
            _OBSERVED_TOPIC,
            "main",
        ),
        "desired_mapping_checksum": gateway_alias_mapping_checksum(
            _DESIRED_TOPIC,
            "main",
        ),
        "desired_artifact_checksum": artifact_checksum(artifact.to_dict()),
        "pending_change_categories": ["alias_mapping"],
        "observed_aggregate_fingerprint": observed.fingerprint,
        "observation_fingerprint": observed.fingerprint,
        "has_pending_changes": True,
        "adopted": True,
        "already_owned": False,
        "committed": True,
    }
    assert data["desired_aggregate_fingerprint"] != observed.fingerprint
    evidence = action_factory.call_args.kwargs["gateway_evidence"]
    assert evidence.version == 1
    assert evidence.backend_identity == _binding().backend_identity
    assert evidence.rule_name == _RULE
    assert evidence.alias_name == _ALIAS
    assert evidence.current.exists is True
    assert evidence.current.fingerprint == observed.fingerprint
    assert evidence.current.managed_interceptor_count == 0
    assert evidence.desired.exists is True
    assert evidence.desired.fingerprint == data["desired_aggregate_fingerprint"]
    assert evidence.desired.managed_interceptor_count == 0

    serialized = json.dumps(_payload(result), sort_keys=True)
    for sensitive in (
        _ADMIN_URL,
        "gateway-runtime-user",
        "gateway-runtime-password",
        _OBSERVED_TOPIC,
        _DESIRED_TOPIC,
    ):
        assert sensitive not in serialized

    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    snapshots = gateway.test_snapshots
    assert isinstance(snapshots, list)
    for snapshot in snapshots:
        snapshot.rule.assert_called_once_with(_RULE, _ALIAS)
    _assert_no_gateway_mutation(gateway)
    gateway.close.assert_called_once_with()

    state = LocalState.load(local_state_path(tmp_path, environment=_ENVIRONMENT))
    expected = desired_managed_records(
        DeploymentPlan(
            gateway_changes=[
                plan_managed_gateway_rule(
                    artifact,
                    build_desired_gateway_rule(artifact, _binding()),
                    observed,
                )
            ]
        ),
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )
    assert state.serial == 1
    assert state.resources[_RESOURCE] == expected[_RESOURCE]


def test_equal_mapping_is_allowed_and_has_no_pending_categories(tmp_path: Path) -> None:
    _write_project(tmp_path)
    observed = _observation(physical_topic=_DESIRED_TOPIC)
    gateway = _gateway(observed, observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    data = _payload(result)["data"]
    assert data["pending_change_categories"] == []
    assert data["has_pending_changes"] is False
    assert data["observed_mapping_checksum"] == data["desired_mapping_checksum"]
    assert data["observed_aggregate_fingerprint"] == data["desired_aggregate_fingerprint"]
    _assert_no_gateway_mutation(gateway)


@pytest.mark.parametrize(
    "artifact",
    [
        _artifact(mode="managed"),
        _artifact(project="other-project"),
        _artifact(owner_type="source"),
        _artifact(interceptors=[{"type": "filter", "config": {"where": "amount > 0"}}]),
    ],
    ids=["managed", "wrong-project", "wrong-owner-type", "nonempty"],
)
def test_invalid_target_fails_before_gateway_factory(
    tmp_path: Path,
    artifact: GatewayRuleArtifact,
) -> None:
    _write_project(tmp_path)
    gateway_factory = MagicMock()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(artifact)),
        patch(
            "streamt.cli.commands.adopt.make_gateway_deployer",
            gateway_factory,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E412_ADOPTION_TARGET_INVALID"
    gateway_factory.assert_not_called()
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()


def test_whole_manifest_alias_collision_fails_before_gateway_factory(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    gateway_factory = MagicMock()
    collision = _artifact(
        owner="payments_view",
        rule="payments_access_rule",
        alias=_ALIAS,
    )

    with (
        patch(
            "streamt.compiler.Compiler.compile",
            return_value=_manifest(_artifact(), collision),
        ),
        patch(
            "streamt.cli.commands.adopt.make_gateway_deployer",
            gateway_factory,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E412_ADOPTION_TARGET_INVALID"
    gateway_factory.assert_not_called()


@pytest.mark.parametrize("same_resource", [True, False])
def test_legacy_gateway_state_never_authorizes_adoption(
    tmp_path: Path,
    same_resource: bool,
) -> None:
    _write_project(tmp_path)
    claimed_uri = (
        _RESOURCE
        if same_resource
        else resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "gateway_rule",
            "payments_view",
        )
    )
    LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=4,
        resources={
            claimed_uri: ManagedResourceRecord(
                physical_name=_ALIAS,
                ownership="adopted",
                artifact_checksum=artifact_checksum({"legacy": True}),
                backend="conduktor-gateway",
            )
        },
    ).save(local_state_path(tmp_path, environment=_ENVIRONMENT))
    gateway_factory = MagicMock()

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_artifact())),
        patch(
            "streamt.cli.commands.adopt.make_gateway_deployer",
            gateway_factory,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    gateway_factory.assert_not_called()
    assert LocalState.load(local_state_path(tmp_path, environment=_ENVIRONMENT)).serial == 4


def test_factory_binding_mismatch_fails_before_observation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    gateway = _gateway(
        binding=_binding(
            endpoint="https://other-gateway.example.test/admin",
        )
    )
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    gateway.observe_managed_gateway_snapshot.assert_not_called()
    _assert_no_gateway_mutation(gateway)
    gateway.close.assert_called_once_with()


def test_same_alias_on_another_canonical_gateway_binding_is_not_a_collision(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    other_uri = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        "payments_view",
    )
    other_record = ManagedResourceRecord(
        physical_name=_ALIAS,
        ownership="adopted",
        artifact_checksum=artifact_checksum({"other": True}),
        backend=_binding(
            endpoint="https://other-gateway.example.test/admin"
        ).backend_identity,
    )
    LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=4,
        resources={other_uri: other_record},
    ).save(local_state_path(tmp_path, environment=_ENVIRONMENT))
    observed = _observation()
    gateway = _gateway(observed, observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment=_ENVIRONMENT))
    assert state.serial == 5
    assert state.resources[other_uri] == other_record
    assert _RESOURCE in state.resources
    _assert_no_gateway_mutation(gateway)


@pytest.mark.parametrize(
    ("observed", "expected_code"),
    [
        (_observation(exists=False), "E413_ADOPTION_LIVE_NOT_FOUND"),
        (_observation(with_interceptor=True), "E416_ADOPTION_FAILED"),
    ],
    ids=["absent", "owned-interceptor"],
)
def test_invalid_live_surface_never_writes_or_mutates(
    tmp_path: Path,
    observed: ManagedGatewayRuleObservation,
    expected_code: str,
) -> None:
    _write_project(tmp_path)
    gateway = _gateway(observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == expected_code
    gateway.observe_managed_gateway_snapshot.assert_called_once_with()
    _assert_no_gateway_mutation(gateway)
    gateway.close.assert_called_once_with()
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()


def test_second_snapshot_drift_requires_fresh_confirmation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    first = _observation()
    second = _observation(physical_topic="orders.rotated.private")
    gateway = _gateway(first, second)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ("E414_ADOPTION_CONFIRMATION_REQUIRED")
    assert "orders.rotated.private" not in result.output
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    _assert_no_gateway_mutation(gateway)
    gateway.close.assert_called_once_with()
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()


def test_identical_record_is_idempotent_after_one_complete_snapshot(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _artifact()
    LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=7,
        resources={
            _RESOURCE: ManagedResourceRecord(
                physical_name=_ALIAS,
                ownership="adopted",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=_binding().backend_identity,
            )
        },
    ).save(local_state_path(tmp_path, environment=_ENVIRONMENT))
    snapshot = _observation()
    gateway = _gateway(snapshot)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(artifact), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path, confirm=False)

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["already_owned"] is True
    assert gateway.observe_managed_gateway_snapshot.call_count == 1
    assert LocalState.load(local_state_path(tmp_path, environment=_ENVIRONMENT)).serial == 7
    _assert_no_gateway_mutation(gateway)
    gateway.close.assert_called_once_with()


def test_two_snapshots_use_the_exact_rule_and_alias(tmp_path: Path) -> None:
    _write_project(tmp_path)
    observed = _observation()
    gateway = _gateway(observed, observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with compiler_patch, gateway_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    snapshots = gateway.test_snapshots
    assert isinstance(snapshots, list)
    assert [snapshot.rule.call_args for snapshot in snapshots] == [
        call(_RULE, _ALIAS),
        call(_RULE, _ALIAS),
    ]
    _assert_no_gateway_mutation(gateway)


def test_state_drift_after_confirmation_fails_before_operation_intent(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment=_ENVIRONMENT)
    concurrent = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            resource_id(_PROJECT, _ENVIRONMENT, "topic", "audit"): (
                ManagedResourceRecord(
                    physical_name="audit.v1",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"name": "audit.v1"}),
                    backend="direct-kafka",
                )
            )
        },
    )
    observed = _observation()
    gateway = _gateway(observed, observed)
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    def write_concurrent_state(**_kwargs: object) -> None:
        concurrent.save(state_path)

    with (
        compiler_patch,
        gateway_patch,
        patch(
            "streamt.cli.commands.adopt._require_confirmation",
            side_effect=write_concurrent_state,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(state_path) == concurrent
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    _assert_no_gateway_mutation(gateway)


def test_unknown_state_commit_retains_exact_gateway_adoption_evidence(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    observed = _observation()
    gateway = _gateway(observed, observed)
    real_service = make_deployment_state_service(
        tmp_path,
        project=_PROJECT,
        environment=_ENVIRONMENT,
        config=local_deployment_state_config(),
    )
    operation_spy: MagicMock | None = None

    @contextmanager
    def operation() -> Iterator[MagicMock]:
        nonlocal operation_spy
        with real_service.operation() as delegate:
            operation_spy = MagicMock(wraps=delegate)
            operation_spy.commit_operation.side_effect = OSError(
                "commit failed password=commit-secret"
            )
            yield operation_spy

    state_service = MagicMock()
    state_service.operation.side_effect = operation
    compiler_patch, gateway_patch = _patch_adoption(_manifest(_artifact()), gateway)

    with (
        compiler_patch,
        gateway_patch,
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            return_value=state_service,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E425_STATE_UNKNOWN_OUTCOME"
    assert "commit-secret" not in result.output
    assert operation_spy is not None
    operation_spy.commit_operation.assert_called_once()
    operation_spy.mark_recovery_required.assert_called_once()
    control = real_service.read_control().control
    assert control.status == "recovery_required"
    assert control.intent is not None
    assert len(control.intent.actions) == 1
    action = control.intent.actions[0]
    assert action.action == "adopt"
    assert action.resource_id == _RESOURCE
    assert action.gateway_evidence is not None
    assert action.gateway_evidence.current.fingerprint == observed.fingerprint
    assert action.gateway_evidence.desired.exists is True
    assert action.gateway_evidence.desired.managed_interceptor_count == 0
    _assert_no_gateway_mutation(gateway)
