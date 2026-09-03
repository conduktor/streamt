"""Exact live planning contracts for explicit managed Connector removal."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from typing import cast
from unittest.mock import MagicMock, call

import pytest

from streamt.compiler.connector_artifact import ConnectorRemovalPreflightError
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.core.deployment_state import (
    PostgresConnectionConfig,
    PostgresDeploymentStateConfig,
)
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import ConnectClusterConfig, ConnectConfig, KafkaConfig, RuntimeConfig
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ConnectDeployer,
    ConnectorChange,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.planner import (
    ConnectorRemovalAssessment,
    DeploymentPlan,
    DeploymentPlanner,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_ENDPOINT = "https://connect.example.test:8443/api/"
_BINDING = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)


class _BindingSubclass(ConnectClusterBinding):
    pass


def _project() -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="payments"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker:9092"),
            connect=ConnectConfig(
                default="primary",
                clusters={"primary": ConnectClusterConfig(rest_url=_ENDPOINT)},
            ),
        ),
        deployment_state=PostgresDeploymentStateConfig(
            backend="postgres",
            namespace="test",
            postgres=PostgresConnectionConfig(
                dsn_env="STREAMT_TEST_ADMIN_DSN",
                writer_dsn_env="STREAMT_TEST_WRITER_DSN",
            ),
        ),
    )


def _removal(owner: str, name: str) -> dict[str, str]:
    return {"logicalOwner": owner, "name": name, "cluster": "primary"}


def _manifest(*removals: dict[str, str]) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        artifacts={"connectors": [], "connector_removals": list(removals)},
    )


def _artifact(
    owner: str,
    name: str,
    *,
    config: dict[str, object] | None = None,
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.ArchiveSink",
        topics=["orders.v1", "orders.v2"],
        cluster="primary",
        config=config or {"tasks.max": 2},
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name=owner,
            mode="managed",
        ),
    )


def _present(artifact: ConnectorArtifact) -> ManagedConnectorObservation:
    raw_config = artifact.to_dict()["config"]
    assert isinstance(raw_config, dict)
    config = cast(
        tuple[tuple[str, ConnectorConfigScalar], ...],
        tuple(sorted(raw_config.items())),
    )
    return ManagedConnectorObservation(
        binding=_BINDING,
        name=artifact.name,
        exists=True,
        config=config,
    )


def _absent(name: str) -> ManagedConnectorObservation:
    return ManagedConnectorObservation(
        binding=_BINDING,
        name=name,
        exists=False,
    )


def _record(artifact: ConnectorArtifact) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=artifact.name,
        ownership="managed",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend=_BINDING.backend_identity,
    )


def _state(
    records: dict[str, ManagedResourceRecord] | None = None,
) -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        resources=records or {},
    )


def _planner(
    manifest: Manifest,
    observations: dict[str, ManagedConnectorObservation],
    *,
    state: LocalState,
) -> tuple[DeploymentPlanner, MagicMock]:
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.cluster_binding = _BINDING
    deployer.require_cluster_binding.return_value = _BINDING
    deployer.observe_managed_connector.side_effect = observations.__getitem__
    return (
        DeploymentPlanner(
            manifest,
            connect_deployer=deployer,
            project=_project(),
            prior_state=state,
            project_name="payments",
            environment="prod",
        ),
        deployer,
    )


def test_plans_exact_delete_and_ordered_non_actionable_matrix_once_per_target() -> None:
    exact = _artifact(
        "delete_owner",
        "delete-sink",
        config={"tasks.max": 2, "password": "not-persisted-secret"},
    )
    absent_drift = _artifact("absent_drift_owner", "absent-drift-sink")
    checksum_drift = _artifact("checksum_drift_owner", "checksum-drift-sink")
    unowned = _artifact("unowned_owner", "unowned-sink")
    records = {
        resource_id("payments", "prod", "connector", "delete_owner"): _record(exact),
        resource_id("payments", "prod", "connector", "absent_drift_owner"): _record(absent_drift),
        resource_id("payments", "prod", "connector", "checksum_drift_owner"): ManagedResourceRecord(
            physical_name=checksum_drift.name,
            ownership="managed",
            artifact_checksum="sha256:" + "0" * 64,
            backend=_BINDING.backend_identity,
        ),
    }
    manifest = _manifest(
        _removal("already_absent_owner", "already-absent-sink"),
        _removal("delete_owner", exact.name),
        _removal("absent_drift_owner", absent_drift.name),
        _removal("unowned_owner", unowned.name),
        _removal("checksum_drift_owner", checksum_drift.name),
    )
    planner, deployer = _planner(
        manifest,
        {
            "already-absent-sink": _absent("already-absent-sink"),
            exact.name: _present(exact),
            absent_drift.name: _absent(absent_drift.name),
            unowned.name: _present(unowned),
            checksum_drift.name: _present(checksum_drift),
        },
        state=_state(records),
    )

    plan = planner.plan()

    assert deployer.observe_managed_connector.call_args_list == [
        call("already-absent-sink"),
        call("delete-sink"),
        call("absent-drift-sink"),
        call("unowned-sink"),
        call("checksum-drift-sink"),
    ]
    assert [(change.connector_name, change.action) for change in plan.connector_changes] == [
        ("delete-sink", "delete")
    ]
    assert plan.connector_changes[0].current == _present(exact)
    assert [
        (assessment.logical_owner, assessment.status)
        for assessment in plan.connector_removal_assessments
    ] == [
        ("already_absent_owner", "already_absent"),
        ("absent_drift_owner", "state_provider_drift"),
        ("unowned_owner", "ownership_required"),
        ("checksum_drift_owner", "state_provider_drift"),
    ]
    assert {blocker.code for blocker in plan.ordered_safety_blockers} == {
        "connector_removal_state_provider_drift",
        "connector_removal_ownership_required",
    }
    assert plan.is_apply_blocked is True

    # Blocking outcomes suppress reviewed runtime actions at the CLI boundary;
    # inspect the actionable subset independently to freeze its exact evidence.
    actionable = DeploymentPlan(connector_changes=list(plan.connector_changes))
    planned_action = planner.planned_actions(actionable)[0]
    evidence = planned_action.connector_evidence
    assert planned_action.resource_id == resource_id(
        "payments", "prod", "connector", "delete_owner"
    )
    assert evidence is not None
    assert evidence.backend_identity == _BINDING.backend_identity
    assert evidence.connector_name == exact.name
    assert evidence.prior_artifact_checksum == artifact_checksum(exact.to_dict())
    assert evidence.current.fingerprint == _present(exact).fingerprint
    assert evidence.desired.fingerprint == managed_connector_absence_fingerprint(
        _BINDING.backend_identity,
        exact.name,
    )
    serialized_evidence = json.dumps(evidence.to_dict(), sort_keys=True)
    assert "not-persisted-secret" not in serialized_evidence
    assert _ENDPOINT not in serialized_evidence


def test_reconstruction_is_exact_and_malformed_reserved_config_fails_closed() -> None:
    owner = "archive_owner"
    name = "archive-sink"
    artifact = _artifact(owner, name)
    prior_id = resource_id("payments", "prod", "connector", owner)
    malformed = ManagedConnectorObservation(
        binding=_BINDING,
        name=name,
        exists=True,
        config=(("name", name), ("password", "provider-secret")),
    )
    planner, _deployer = _planner(
        _manifest(_removal(owner, name)),
        {name: malformed},
        state=_state({prior_id: _record(artifact)}),
    )

    with pytest.raises(ConnectorRemovalPreflightError) as error:
        planner.plan()

    assert str(error.value) == (
        "Connector removal prior artifact cannot be reconstructed from the exact observation"
    )
    assert "provider-secret" not in str(error.value)


def test_provider_failure_is_sanitized_and_does_not_create_partial_results() -> None:
    manifest = _manifest(
        _removal("first_owner", "first-sink"),
        _removal("second_owner", "second-sink"),
    )
    planner, deployer = _planner(
        manifest,
        {"first-sink": _absent("first-sink")},
        state=_state(),
    )
    deployer.observe_managed_connector.side_effect = RuntimeError(
        "password=provider-secret https://user:pass@connect.invalid"
    )

    with pytest.raises(ConnectorRemovalPreflightError) as error:
        planner.plan()

    assert str(error.value) == "Connector removal live observation failed"
    assert "provider-secret" not in str(error.value)
    deployer.observe_managed_connector.assert_called_once_with("first-sink")


def test_rejects_binding_subclasses_from_deployer_and_observation() -> None:
    hostile_binding = _BindingSubclass(
        cluster_alias=_BINDING.cluster_alias,
        endpoint_fingerprint=_BINDING.endpoint_fingerprint,
    )
    manifest = _manifest(_removal("archive_owner", "archive-sink"))
    planner, deployer = _planner(
        manifest,
        {"archive-sink": _absent("archive-sink")},
        state=_state(),
    )
    deployer.require_cluster_binding.return_value = hostile_binding

    with pytest.raises(ConnectClusterBindingError, match="exact cluster binding"):
        planner.plan()
    deployer.observe_managed_connector.assert_not_called()

    hostile_observation = ManagedConnectorObservation(
        binding=hostile_binding,
        name="archive-sink",
        exists=False,
    )
    planner, deployer = _planner(
        manifest,
        {"archive-sink": hostile_observation},
        state=_state(),
    )

    with pytest.raises(ConnectorRemovalPreflightError, match="exact target"):
        planner.plan()
    deployer.observe_managed_connector.assert_called_once_with("archive-sink")


def test_manual_exact_surface_without_manifest_tombstone_cannot_create_evidence() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    prior_id = resource_id("payments", "prod", "connector", "archive_owner")
    planner, _deployer = _planner(
        _manifest(),
        {},
        state=_state({prior_id: _record(artifact)}),
    )
    manual_plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="delete",
                current=_present(artifact),
                backend_identity=_BINDING.backend_identity,
            )
        ]
    )

    with pytest.raises(StateIdentityError, match="exact action evidence"):
        planner.planned_actions(manual_plan)


def test_managed_delete_apply_routes_only_through_exact_provider_mutation() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    prior_id = resource_id("payments", "prod", "connector", "archive_owner")
    planner, deployer = _planner(
        _manifest(_removal("archive_owner", artifact.name)),
        {artifact.name: _present(artifact)},
        state=_state({prior_id: _record(artifact)}),
    )
    plan = planner.plan()
    deployer.reset_mock()
    deployer.delete_managed_connector.return_value = "deleted"

    results = planner.apply(plan)

    assert results["deleted"] == ["connector:archive-sink"]
    deployer.delete_managed_connector.assert_called_once_with(_present(artifact))
    deployer.delete_connector.assert_not_called()


def test_assessment_is_frozen_and_deployment_plan_requires_exact_tuple() -> None:
    assessment = ConnectorRemovalAssessment(
        resource_id=resource_id("payments", "prod", "connector", "archive_owner"),
        logical_owner="archive_owner",
        connector_name="archive-sink",
        backend_identity=_BINDING.backend_identity,
        status="already_absent",
    )
    assert assessment.to_dict() == {
        "resource_id": "streamt://payments/prod/connector/archive_owner",
        "logical_owner": "archive_owner",
        "connector_name": "archive-sink",
        "backend_identity": _BINDING.backend_identity,
        "status": "already_absent",
    }
    with pytest.raises(FrozenInstanceError):
        assessment.status = "ownership_required"  # type: ignore[misc]
    with pytest.raises(StateIdentityError, match="assessments are invalid"):
        DeploymentPlan(connector_removal_assessments=[assessment])  # type: ignore[arg-type]
