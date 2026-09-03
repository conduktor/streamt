"""Exact managed Connector deletion routing at the planner mutation boundary."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    Manifest,
    TopicArtifact,
)
from streamt.core.deployment_state import (
    PostgresConnectionConfig,
    PostgresDeploymentStateConfig,
)
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import ConnectClusterConfig, ConnectConfig, KafkaConfig, RuntimeConfig
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectManagedMutationError,
    ConnectorChange,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
)
from streamt.deployer.kafka import TopicChange
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_ENDPOINT = "https://connect.example.test:8443/api/"
_BINDING = ConnectClusterBinding.from_endpoint("primary", _ENDPOINT)


class _DeletedStr(str):
    pass


class _HostileDelete(str):
    def __eq__(self, other: object) -> bool:
        return other == "delete"

    def __ne__(self, other: object) -> bool:
        return other == "delete"


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


def _artifact(owner: str, name: str, *, tasks: int = 2) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.ArchiveSink",
        topics=["orders.v1", "orders.v2"],
        cluster="primary",
        config={"tasks.max": tasks},
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name=owner,
            mode="managed",
        ),
    )


def _present(artifact: ConnectorArtifact) -> ManagedConnectorObservation:
    config = cast(
        tuple[tuple[str, ConnectorConfigScalar], ...],
        tuple(sorted(artifact.to_dict()["config"].items())),
    )
    return ManagedConnectorObservation(
        binding=_BINDING,
        name=artifact.name,
        exists=True,
        config=config,
    )


def _change(artifact: ConnectorArtifact) -> ConnectorChange:
    return ConnectorChange(
        connector_name=artifact.name,
        action="delete",
        current=_present(artifact),
        backend_identity=_BINDING.backend_identity,
    )


def _planner(
    artifacts: list[ConnectorArtifact],
    deployer: MagicMock,
    *,
    kafka_deployer: MagicMock | None = None,
) -> DeploymentPlanner:
    removals = [
        {
            "logicalOwner": artifact.ownership.owner_name,
            "name": artifact.name,
            "cluster": "primary",
        }
        for artifact in artifacts
    ]
    records = {
        resource_id("payments", "prod", "connector", artifact.ownership.owner_name):
            ManagedResourceRecord(
                physical_name=artifact.name,
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=_BINDING.backend_identity,
            )
        for artifact in artifacts
    }
    return DeploymentPlanner(
        Manifest(
            version="1.0",
            project_name="payments",
            artifacts={"connectors": [], "connector_removals": removals},
        ),
        kafka_deployer=kafka_deployer,
        connect_deployer=deployer,
        project=_project(),
        prior_state=LocalState(
            project="payments",
            environment="prod",
            resources=records,
        ),
        project_name="payments",
        environment="prod",
    )


def test_apply_routes_exact_delete_only_through_managed_surface_with_callbacks() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    change = _change(artifact)
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.delete_managed_connector.return_value = "deleted"
    before = MagicMock()
    after = MagicMock()

    results = _planner([artifact], deployer).apply(
        DeploymentPlan(connector_changes=[change]),
        before_action=before,
        after_action=after,
    )

    assert results["deleted"] == ["connector:archive-sink"]
    assert results["errors"] == []
    deployer.delete_managed_connector.assert_called_once_with(change.current)
    deployer.delete_connector.assert_not_called()
    before.assert_called_once_with("connector:archive-sink", 0)
    after.assert_called_once_with("connector:archive-sink", 0, True)


@pytest.mark.parametrize("invalid_result", ["unchanged", _DeletedStr("deleted")])
def test_apply_rejects_invalid_managed_result_and_stops_later_actions(
    invalid_result: str,
) -> None:
    first = _artifact("first_owner", "first-sink")
    second = _artifact("second_owner", "second-sink")
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.delete_managed_connector.return_value = invalid_result
    after = MagicMock()

    results = _planner([first, second], deployer).apply(
        DeploymentPlan(connector_changes=[_change(first), _change(second)]),
        after_action=after,
        stop_on_error=True,
    )

    assert results["deleted"] == []
    assert len(results["errors"]) == 1
    assert "invalid result" in results["errors"][0]
    assert deployer.delete_managed_connector.call_count == 1
    deployer.delete_connector.assert_not_called()
    after.assert_called_once_with("connector:first-sink", 0, False)


def test_apply_sanitizes_managed_provider_error_and_always_stops() -> None:
    first = _artifact("first_owner", "first-sink")
    second = _artifact("second_owner", "second-sink")
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.delete_managed_connector.side_effect = ConnectManagedMutationError(
        "Kafka Connect managed deletion could not prove exact absence"
    )
    after = MagicMock()

    results = _planner([first, second], deployer).apply(
        DeploymentPlan(connector_changes=[_change(first), _change(second)]),
        after_action=after,
    )

    assert results["deleted"] == []
    assert results["errors"] == [
        "connector:first-sink: "
        "Kafka Connect managed deletion could not prove exact absence"
    ]
    deployer.delete_managed_connector.assert_called_once_with(_change(first).current)
    deployer.delete_connector.assert_not_called()
    after.assert_called_once_with("connector:first-sink", 0, False)


@pytest.mark.parametrize("failure", ["missing_tombstone", "mismatched_observation"])
def test_managed_delete_evidence_is_validated_before_earlier_provider_mutation(
    failure: str,
) -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    connect = MagicMock(spec=ConnectDeployer)
    kafka = MagicMock()
    planner = _planner([artifact], connect, kafka_deployer=kafka)
    change = _change(artifact)
    if failure == "missing_tombstone":
        planner.manifest.artifacts["connector_removals"] = []
    else:
        change.current = _present(_artifact("archive_owner", "archive-sink", tasks=3))
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic="orders",
                action="create",
                desired=TopicArtifact(
                    name="orders",
                    partitions=1,
                    replication_factor=1,
                ),
            )
        ],
        connector_changes=[change],
    )

    with pytest.raises(StateIdentityError):
        planner.apply(plan)

    kafka.apply_topic.assert_not_called()
    connect.delete_managed_connector.assert_not_called()
    connect.delete_connector.assert_not_called()


def test_duplicate_managed_action_identity_fails_before_provider_mutation() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    connect = MagicMock(spec=ConnectDeployer)
    kafka = MagicMock()
    planner = _planner([artifact], connect, kafka_deployer=kafka)
    change = _change(artifact)
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic="orders",
                action="create",
                desired=TopicArtifact(
                    name="orders",
                    partitions=1,
                    replication_factor=1,
                ),
            )
        ],
        connector_changes=[change, _change(artifact)],
    )

    with pytest.raises(StateIdentityError, match="duplicate canonical action identity"):
        planner.apply(plan)

    kafka.apply_topic.assert_not_called()
    connect.delete_managed_connector.assert_not_called()
    connect.delete_connector.assert_not_called()


def test_hostile_action_subclass_cannot_bypass_managed_routing() -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    connect = MagicMock(spec=ConnectDeployer)
    kafka = MagicMock()
    planner = _planner([artifact], connect, kafka_deployer=kafka)
    change = _change(artifact)
    change.action = _HostileDelete("delete")

    with pytest.raises(StateIdentityError, match="invalid action shape"):
        planner.apply(DeploymentPlan(connector_changes=[change]))

    kafka.apply_topic.assert_not_called()
    connect.apply_connector.assert_not_called()
    connect.delete_managed_connector.assert_not_called()
    connect.delete_connector.assert_not_called()


@pytest.mark.parametrize("field", ["connector_name", "backend_identity"])
def test_managed_delete_rejects_non_exact_identity_primitives(field: str) -> None:
    artifact = _artifact("archive_owner", "archive-sink")
    connect = MagicMock(spec=ConnectDeployer)
    planner = _planner([artifact], connect)
    change = _change(artifact)
    setattr(change, field, _DeletedStr(cast(str, getattr(change, field))))

    with pytest.raises(StateIdentityError, match="invalid action shape"):
        planner.apply(DeploymentPlan(connector_changes=[change]))

    connect.apply_connector.assert_not_called()
    connect.delete_managed_connector.assert_not_called()
    connect.delete_connector.assert_not_called()


def test_legacy_delete_without_managed_evidence_keeps_bare_compatibility_route() -> None:
    deployer = MagicMock(spec=ConnectDeployer)
    planner = DeploymentPlanner(
        Manifest(version="1.0", project_name="payments"),
        connect_deployer=deployer,
    )
    legacy = ConnectorChange(connector_name="legacy-sink", action="delete")

    results = planner.apply(DeploymentPlan(connector_changes=[legacy]))

    assert results["deleted"] == ["connector:legacy-sink"]
    deployer.delete_connector.assert_called_once_with("legacy-sink")
    deployer.delete_managed_connector.assert_not_called()
    with pytest.raises(StateIdentityError, match="exact action evidence"):
        planner.planned_actions(DeploymentPlan(connector_changes=[legacy]))
