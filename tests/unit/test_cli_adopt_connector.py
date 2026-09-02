"""Focused tests for fail-closed Kafka Connect ownership adoption."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.cli.commands.adopt import AdoptionError, _resolve_connector_artifact
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectManagedObservationError,
    ConnectorChange,
    ManagedConnectorObservation,
    bind_connector_artifact,
)
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    desired_managed_records,
    local_state_path,
    resource_id,
)

_ALIAS = "production"
_REST_URL = "https://connect.example.test:8443/api"
_RESOURCE = resource_id("adoption-test", "default", "connector", "orders")


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "adoption-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "connect": {
                        "default": _ALIAS,
                        "clusters": {
                            _ALIAS: {
                                "rest_url": _REST_URL,
                                "username": "runtime-user",
                                "password": "runtime-password",
                            }
                        },
                    },
                },
            }
        )
    )


def _binding(
    *,
    alias: str = _ALIAS,
    rest_url: str = _REST_URL,
) -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint(alias, rest_url)


def _artifact(
    *,
    logical_name: str = "orders",
    physical_name: str = "orders-sink",
    project: str = "adoption-test",
    mode: str = "adopted",
    cluster: str | None = None,
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=physical_name,
        connector_class="io.desired.SecretSinkConnector",
        topics=["orders.desired.secret.v1"],
        cluster=cluster,
        config={
            "connection.url": (
                "https://desired-user:desired-url-password@warehouse.example.test/"
                "sink?token=desired-query-secret"
            ),
            "password": "desired-config-password",
            "sasl.jaas.config": (
                "org.example.Login required username=desired-jaas-user "
                'password="desired-jaas-password";'
            ),
            "tasks.max": 2,
        },
        ownership=ArtifactOwnership(
            project=project,
            owner_type="model",
            owner_name=logical_name,
            mode=mode,
        ),
    )


def _manifest(*artifacts: ConnectorArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="adoption-test",
        artifacts={"connectors": [artifact.to_dict() for artifact in artifacts]},
    )


def _observation(
    *,
    binding: ConnectClusterBinding | None = None,
    exists: bool = True,
    name: str = "orders-sink",
    config_overrides: dict[str, str | bool | int | float] | None = None,
) -> ManagedConnectorObservation:
    config: dict[str, str | bool | int | float] = {
        "name": name,
        "connector.class": "io.live.SecretSourceConnector",
        "topics": "orders.live.secret.v1",
        "connection.url": (
            "https://live-user:live-url-password@warehouse.example.test/"
            "source?token=live-query-secret"
        ),
        "password": "live-config-password",
        "sasl.jaas.config": (
            "org.example.Login required username=live-jaas-user "
            'password="live-jaas-password";'
        ),
        "tasks.max": 1,
    }
    if config_overrides:
        config.update(config_overrides)
    return ManagedConnectorObservation(
        binding=binding or _binding(),
        name=name,
        exists=exists,
        config=tuple(sorted(config.items())) if exists else (),
    )


def _connect(*observations: object, binding: ConnectClusterBinding | None = None) -> MagicMock:
    deployer = MagicMock()
    deployer.require_cluster_binding.return_value = binding or _binding()
    if observations:
        deployer.observe_managed_connector.side_effect = list(observations)
    else:
        deployer.observe_managed_connector.return_value = _observation()
    return deployer


def _invoke(path: Path, *, extra: list[str] | None = None) -> Result:
    args = [
        "-o",
        "json",
        "adopt",
        "-p",
        str(path),
        "-e",
        "default",
        "--kind",
        "connector",
        "--name",
        "orders",
    ]
    if extra is None:
        args.extend(
            [
                "--confirm-resource",
                _RESOURCE,
                "--confirm-env",
                "default",
            ]
        )
    else:
        args.extend(extra)
    return CliRunner().invoke(main, args)


def _patch_adoption(manifest: Manifest, connect: MagicMock) -> tuple[Any, Any]:
    return (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch("streamt.cli.commands.adopt.make_connect_deployer", return_value=connect),
    )


def _payload(result: Result) -> dict[str, Any]:
    return json.loads(result.stdout)  # type: ignore[no-any-return]


def _assert_no_runtime_actions(connect: MagicMock) -> None:
    for method in (
        "list_connectors",
        "get_connector_state",
        "get_connector_status",
        "create_connector",
        "update_connector",
        "delete_connector",
        "restart_connector",
        "pause_connector",
        "resume_connector",
        "plan_connector",
        "apply_change",
    ):
        getattr(connect, method).assert_not_called()


def test_success_observes_twice_never_mutates_and_emits_only_secret_neutral_evidence(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _artifact()
    observed = _observation()
    connect = _connect(observed, observed)
    compiler_patch, connect_patch = _patch_adoption(_manifest(artifact), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    data = _payload(result)["data"]
    assert data["resource_id"] == _RESOURCE
    assert data["kind"] == "connector"
    assert data["physical_name"] == "orders-sink"
    assert data["cluster_alias"] == _ALIAS
    assert data["endpoint_fingerprint"] == _binding().endpoint_fingerprint
    assert data["observation_fingerprint"] == observed.fingerprint
    assert data["changed_keys"] == sorted(data["pending_diffs"])
    assert data["has_pending_changes"] is True
    assert set(data["observed"]) == {"name", "config_checksum"}
    assert set(data["desired_managed_attributes"]) == {
        "name",
        "cluster_alias",
        "config_checksum",
        "artifact_checksum",
    }
    assert all(
        set(evidence).issubset(
            {
                "change",
                "from_present",
                "to_present",
            }
        )
        for evidence in data["pending_diffs"].values()
    )

    serialized = json.dumps(_payload(result), sort_keys=True)
    for sensitive in (
        "runtime-user",
        "runtime-password",
        _REST_URL,
        "SecretSinkConnector",
        "SecretSourceConnector",
        "orders.desired.secret.v1",
        "orders.live.secret.v1",
        "desired-user",
        "desired-url-password",
        "desired-query-secret",
        "desired-config-password",
        "desired-jaas-user",
        "desired-jaas-password",
        "live-user",
        "live-url-password",
        "live-query-secret",
        "live-config-password",
        "live-jaas-user",
        "live-jaas-password",
    ):
        assert sensitive not in serialized

    connect.observe_managed_connector.assert_has_calls(
        [
            call("orders-sink"),
            call("orders-sink"),
        ]
    )
    assert connect.observe_managed_connector.call_count == 2
    connect.require_cluster_binding.assert_called_once_with()
    connect.close.assert_called_once_with()
    _assert_no_runtime_actions(connect)

    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    resolved = bind_connector_artifact(artifact, _binding())
    assert state.serial == 1
    assert state.resources[_RESOURCE] == ManagedResourceRecord(
        physical_name="orders-sink",
        ownership="adopted",
        artifact_checksum=artifact_checksum(resolved.to_dict()),
        backend=_binding().backend_identity,
    )


@pytest.mark.parametrize("cluster", [None, _ALIAS])
def test_resolver_binds_implicit_or_explicit_effective_default(cluster: str | None) -> None:
    resolved = _resolve_connector_artifact(
        _manifest(_artifact(cluster=cluster)),
        project="adoption-test",
        logical_name="orders",
        binding=_binding(),
    )

    assert resolved.cluster == _ALIAS
    assert resolved.name == "orders-sink"


@pytest.mark.parametrize(
    "artifact",
    [
        _artifact(cluster="other"),
        _artifact(project="other-project"),
        _artifact(mode="managed"),
    ],
    ids=["nondefault-cluster", "wrong-project", "managed-lifecycle"],
)
def test_resolver_rejects_nondefault_project_and_lifecycle(
    artifact: ConnectorArtifact,
) -> None:
    with pytest.raises(AdoptionError) as exc_info:
        _resolve_connector_artifact(
            _manifest(artifact),
            project="adoption-test",
            logical_name="orders",
            binding=_binding(),
        )

    assert exc_info.value.code == "E412_ADOPTION_TARGET_INVALID"


def test_resolver_rejects_malformed_artifacts_and_global_collisions() -> None:
    malformed = _artifact().to_dict()
    malformed_config = malformed["config"]
    assert isinstance(malformed_config, dict)
    malformed["config"] = {**malformed_config, "name": "different"}
    malformed_manifest = Manifest(
        version="1.0",
        project_name="adoption-test",
        artifacts={"connectors": [malformed]},
    )
    with pytest.raises(AdoptionError):
        _resolve_connector_artifact(
            malformed_manifest,
            project="adoption-test",
            logical_name="orders",
            binding=_binding(),
        )

    duplicate_physical = _manifest(
        _artifact(logical_name="orders"),
        _artifact(logical_name="payments"),
    )
    with pytest.raises(AdoptionError, match="provider identity collision"):
        _resolve_connector_artifact(
            duplicate_physical,
            project="adoption-test",
            logical_name="orders",
            binding=_binding(),
        )

    duplicate_owner = _manifest(
        _artifact(physical_name="orders-a"),
        _artifact(physical_name="orders-b"),
    )
    with pytest.raises(AdoptionError, match="logical ownership collision"):
        _resolve_connector_artifact(
            duplicate_owner,
            project="adoption-test",
            logical_name="orders",
            binding=_binding(),
        )


@pytest.mark.parametrize(
    ("observed", "expected_code"),
    [
        (_observation(exists=False), "E413_ADOPTION_LIVE_NOT_FOUND"),
        (MagicMock(), "E416_ADOPTION_FAILED"),
        (
            _observation(
                binding=_binding(
                    alias="other",
                    rest_url="https://other-connect.example.test",
                )
            ),
            "E416_ADOPTION_FAILED",
        ),
    ],
    ids=["absent", "partial", "wrong-binding"],
)
def test_initial_strict_observation_failures_never_write_or_mutate(
    tmp_path: Path,
    observed: object,
    expected_code: str,
) -> None:
    _write_project(tmp_path)
    connect = _connect(observed)
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == expected_code
    connect.observe_managed_connector.assert_called_once_with("orders-sink")
    _assert_no_runtime_actions(connect)
    assert not local_state_path(tmp_path, environment="default").exists()


def test_observer_authorization_error_is_secret_neutral_and_never_writes(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    connect = _connect(
        ConnectManagedObservationError(
            "authorization rejected password=observer-secret at "
            "https://user:url-secret@connect.example.test"
        )
    )
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "observer-secret" not in result.output
    assert "url-secret" not in result.output
    connect.observe_managed_connector.assert_called_once_with("orders-sink")
    _assert_no_runtime_actions(connect)
    assert not local_state_path(tmp_path, environment="default").exists()


@pytest.mark.parametrize("drift", ["config", "binding"])
def test_reobservation_drift_requires_fresh_confirmation_and_never_writes(
    tmp_path: Path,
    drift: str,
) -> None:
    _write_project(tmp_path)
    initial = _observation()
    if drift == "config":
        changed = _observation(config_overrides={"password": "rotated-live-secret"})
    else:
        changed = _observation(
            binding=_binding(
                alias="other",
                rest_url="https://other-connect.example.test",
            )
        )
    connect = _connect(initial, changed)
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E414_ADOPTION_CONFIRMATION_REQUIRED"
    assert "rotated-live-secret" not in result.output
    assert connect.observe_managed_connector.call_count == 2
    _assert_no_runtime_actions(connect)
    assert not local_state_path(tmp_path, environment="default").exists()


def test_factory_binding_mismatch_fails_before_observation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    connect = _connect(
        binding=_binding(
            alias="other",
            rest_url="https://other-connect.example.test",
        )
    )
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    connect.observe_managed_connector.assert_not_called()
    _assert_no_runtime_actions(connect)


def test_identical_canonical_record_is_idempotent_after_one_strict_observation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    resolved = bind_connector_artifact(_artifact(), _binding())
    LocalState(
        project="adoption-test",
        environment="default",
        serial=7,
        resources={
            _RESOURCE: ManagedResourceRecord(
                physical_name=resolved.name,
                ownership="adopted",
                artifact_checksum=artifact_checksum(resolved.to_dict()),
                backend=_binding().backend_identity,
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    connect = _connect(_observation())
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path, extra=[])

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["already_owned"] is True
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 7
    connect.observe_managed_connector.assert_called_once_with("orders-sink")
    connect.close.assert_called_once_with()
    _assert_no_runtime_actions(connect)


@pytest.mark.parametrize("same_resource", [True, False])
def test_generic_legacy_records_never_authorize_rebinding(
    tmp_path: Path,
    same_resource: bool,
) -> None:
    _write_project(tmp_path)
    claimed_uri = (
        _RESOURCE
        if same_resource
        else resource_id("adoption-test", "default", "connector", "payments")
    )
    LocalState(
        project="adoption-test",
        environment="default",
        serial=4,
        resources={
            claimed_uri: ManagedResourceRecord(
                physical_name="orders-sink",
                ownership="adopted",
                artifact_checksum=artifact_checksum({"legacy": True}),
                backend="kafka-connect",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    connect = _connect(_observation())
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 4
    connect.observe_managed_connector.assert_not_called()


def test_same_name_on_a_different_canonical_cluster_is_not_a_collision(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    other_binding = _binding(
        alias="other",
        rest_url="https://other-connect.example.test",
    )
    other_uri = resource_id("adoption-test", "default", "connector", "payments")
    LocalState(
        project="adoption-test",
        environment="default",
        serial=4,
        resources={
            other_uri: ManagedResourceRecord(
                physical_name="orders-sink",
                ownership="adopted",
                artifact_checksum=artifact_checksum({"other": True}),
                backend=other_binding.backend_identity,
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    observed = _observation()
    connect = _connect(observed, observed)
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    assert state.serial == 5
    assert set(state.resources) == {other_uri, _RESOURCE}


def test_adopted_record_is_identical_to_normal_planner_state_projection(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _artifact()
    resolved = bind_connector_artifact(artifact, _binding())
    observed = _observation()
    connect = _connect(observed, observed)
    compiler_patch, connect_patch = _patch_adoption(_manifest(artifact), connect)

    with compiler_patch, connect_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    expected = desired_managed_records(
        DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=resolved.name,
                    action="none",
                    desired=resolved,
                    backend_identity=_binding().backend_identity,
                )
            ]
        ),
        project="adoption-test",
        environment="default",
    )
    assert state.resources[_RESOURCE] == expected[_RESOURCE]


def test_state_drift_after_confirmation_fails_before_operation_intent(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    concurrent = LocalState(
        project="adoption-test",
        environment="default",
        serial=1,
        resources={
            resource_id("adoption-test", "default", "topic", "payments"): (
                ManagedResourceRecord(
                    physical_name="payments.v1",
                    ownership="managed",
                    artifact_checksum=artifact_checksum({"name": "payments.v1"}),
                    backend="direct-kafka",
                )
            )
        },
    )
    connect = _connect(_observation(), _observation())
    compiler_patch, connect_patch = _patch_adoption(_manifest(_artifact()), connect)

    def write_concurrent_state(**_kwargs: object) -> None:
        concurrent.save(state_path)

    with (
        compiler_patch,
        connect_patch,
        patch(
            "streamt.cli.commands.adopt._require_confirmation",
            side_effect=write_concurrent_state,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert connect.observe_managed_connector.call_count == 2
    assert LocalState.load(state_path) == concurrent
    _assert_no_runtime_actions(connect)
