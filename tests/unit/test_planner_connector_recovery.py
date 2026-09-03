"""Planner-owned observation and collision boundaries for Connector recovery."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock, call

import pytest

from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.core.deployment_state import (
    PostgresConnectionConfig,
    PostgresDeploymentStateConfig,
)
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import ConnectClusterConfig, ConnectConfig, KafkaConfig, RuntimeConfig
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.plan_file import deployment_plan_payload
from streamt.deployer.planner import (
    ConnectorRecoveryObservation,
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
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    OperationAction,
)

_ENDPOINT = "https://connect.example.test:8443/api/"
_BINDING = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)


class _OperationActionSubclass(OperationAction):
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


def _artifact(owner: str, name: str) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.ArchiveSink",
        topics=["orders.v1", "orders.v2"],
        cluster="primary",
        config={"tasks.max": 2},
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
    return ManagedConnectorObservation(
        binding=_BINDING,
        name=artifact.name,
        exists=True,
        config=cast(
            tuple[tuple[str, ConnectorConfigScalar], ...],
            tuple(sorted(raw_config.items())),
        ),
    )


def _record(artifact: ConnectorArtifact) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=artifact.name,
        ownership="managed",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend=_BINDING.backend_identity,
    )


def _action(
    artifact: ConnectorArtifact,
    *,
    index: int = 0,
    owner: str | None = None,
) -> OperationAction:
    observation = _present(artifact)
    ownership = ArtifactOwnership.from_dict(artifact.ownership)
    assert ownership is not None
    logical_owner = owner or ownership.owner_name
    return OperationAction(
        index=index,
        resource_id=resource_id(
            "payments",
            "prod",
            "connector",
            logical_owner,
        ),
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=_BINDING.backend_identity,
            connector_name=artifact.name,
            prior_artifact_checksum=artifact_checksum(artifact.to_dict()),
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=observation.fingerprint,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=managed_connector_absence_fingerprint(
                    _BINDING.backend_identity,
                    artifact.name,
                ),
            ),
        ),
    )


def _state(*artifacts: ConnectorArtifact) -> LocalState:
    records: dict[str, ManagedResourceRecord] = {}
    for artifact in artifacts:
        ownership = ArtifactOwnership.from_dict(artifact.ownership)
        assert ownership is not None
        records[
            resource_id(
                "payments",
                "prod",
                "connector",
                ownership.owner_name,
            )
        ] = _record(artifact)
    return LocalState(
        project="payments",
        environment="prod",
        resources=records,
    )


def _deployer(observations: dict[str, ManagedConnectorObservation]) -> MagicMock:
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.cluster_binding = _BINDING
    deployer.require_cluster_binding.return_value = _BINDING
    deployer.observe_managed_connector.side_effect = observations.__getitem__
    return deployer


def _planner(
    manifest: Manifest,
    deployer: MagicMock,
    state: LocalState,
) -> DeploymentPlanner:
    return DeploymentPlanner(
        manifest,
        connect_deployer=deployer,
        project=_project(),
        prior_state=state,
        project_name="payments",
        environment="prod",
    )


def test_connector_recovery_observes_durable_target_without_tombstone() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    action = _action(artifact)
    current = _present(artifact)
    deployer = _deployer({artifact.name: current})
    planner = _planner(
        Manifest(
            version="1.0",
            project_name="payments",
            artifacts={"connectors": []},
        ),
        deployer,
        _state(artifact),
    )

    plan = planner.plan(connector_recovery_actions=(action,))

    deployer.observe_managed_connector.assert_called_once_with(artifact.name)
    assert plan.connector_changes == []
    assert plan.connector_recovery_observations == (
        ConnectorRecoveryObservation(
            resource_id=action.resource_id,
            observation=current,
        ),
    )


def test_retained_tombstone_and_recovery_share_one_exact_observation() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    action = _action(artifact)
    current = _present(artifact)
    deployer = _deployer({artifact.name: current})
    planner = _planner(
        Manifest(
            version="1.0",
            project_name="payments",
            artifacts={
                "connectors": [],
                "connector_removals": [
                    {
                        "logicalOwner": "archive_owner",
                        "name": artifact.name,
                        "cluster": "primary",
                    }
                ],
            },
        ),
        deployer,
        _state(artifact),
    )

    plan = planner.plan(connector_recovery_actions=(action,))

    assert deployer.observe_managed_connector.call_args_list == [call(artifact.name)]
    assert len(plan.connector_changes) == 1
    assert plan.connector_changes[0].action == "delete"
    assert plan.connector_changes[0].current is current
    assert plan.connector_recovery_observations[0].observation is current


def test_desired_connector_provider_collision_fails_before_any_live_read() -> None:
    removed = _artifact("removed_owner", "archive-sink")
    desired = _artifact("different_owner", "archive-sink")
    action = _action(removed)
    deployer = _deployer({})
    planner = _planner(
        Manifest(
            version="1.0",
            project_name="payments",
            artifacts={"connectors": [desired.to_dict()]},
        ),
        deployer,
        _state(removed),
    )

    with pytest.raises(StateIdentityError, match="collides with a desired Connector"):
        planner.plan(connector_recovery_actions=(action,))

    deployer.require_cluster_binding.assert_not_called()
    deployer.observe_managed_connector.assert_not_called()
    deployer.plan_connector.assert_not_called()


def test_duplicate_recovery_resource_fails_before_any_live_read() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    action = _action(artifact)
    deployer = _deployer({})
    planner = _planner(
        Manifest(version="1.0", project_name="payments"),
        deployer,
        _state(artifact),
    )

    with pytest.raises(StateIdentityError, match="duplicate canonical resource"):
        planner.plan(connector_recovery_actions=(action, action))

    deployer.require_cluster_binding.assert_not_called()
    deployer.observe_managed_connector.assert_not_called()


def test_non_base_recovery_action_fails_before_any_live_read() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    base = _action(artifact)
    action = _OperationActionSubclass(
        index=base.index,
        resource_id=base.resource_id,
        action=base.action,
        connector_evidence=base.connector_evidence,
    )
    deployer = _deployer({})
    planner = _planner(
        Manifest(version="1.0", project_name="payments"),
        deployer,
        _state(artifact),
    )

    with pytest.raises(StateIdentityError, match="exact immutable action tuple"):
        planner.plan(connector_recovery_actions=(action,))

    deployer.require_cluster_binding.assert_not_called()
    deployer.observe_managed_connector.assert_not_called()


def test_mismatched_prior_checksum_fails_before_any_live_read() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    action = _action(artifact)
    state = _state(artifact)
    state.resources[action.resource_id] = ManagedResourceRecord(
        physical_name=artifact.name,
        ownership="managed",
        artifact_checksum="sha256:" + "0" * 64,
        backend=_BINDING.backend_identity,
    )
    deployer = _deployer({})
    planner = _planner(
        Manifest(version="1.0", project_name="payments"),
        deployer,
        state,
    )

    with pytest.raises(StateIdentityError, match="exact managed prior ownership"):
        planner.plan(connector_recovery_actions=(action,))

    deployer.require_cluster_binding.assert_not_called()
    deployer.observe_managed_connector.assert_not_called()


def test_connector_recovery_observations_are_hidden_from_identity_and_payload() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    recovery_observation = ConnectorRecoveryObservation(
        resource_id=resource_id(
            "payments",
            "prod",
            "connector",
            "archive_owner",
        ),
        observation=_present(artifact),
    )
    plan = DeploymentPlan(
        connector_recovery_observations=(recovery_observation,)
    )

    assert plan == DeploymentPlan()
    assert artifact.name not in repr(plan)
    assert deployment_plan_payload(plan) == deployment_plan_payload(DeploymentPlan())


@pytest.mark.parametrize("actions", [[], ("not-an-action",)])
def test_connector_recovery_requires_exact_action_tuple(actions: object) -> None:
    deployer = _deployer({})
    planner = _planner(
        Manifest(version="1.0", project_name="payments"),
        deployer,
        LocalState(project="payments", environment="prod"),
    )

    with pytest.raises(StateIdentityError, match="exact immutable action tuple"):
        planner.plan(connector_recovery_actions=actions)  # type: ignore[arg-type]

    deployer.require_cluster_binding.assert_not_called()
    deployer.observe_managed_connector.assert_not_called()
