"""Strict compiled Connector artifact handling at planner boundaries."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from streamt.compiler.connector_artifact import ConnectorArtifactFormatError
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import (
    ConnectClusterConfig,
    ConnectConfig,
    KafkaConfig,
    RuntimeConfig,
)
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ConnectDeployer,
    ConnectorChange,
    ConnectorState,
    ManagedConnectorObservation,
)
from streamt.deployer.planner import DeploymentPlanner
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_ENDPOINT = "https://connect.example.test:8443/api"


def _ownership(owner_name: str = "orders") -> ArtifactOwnership:
    return ArtifactOwnership(
        project="payments",
        owner_type="model",
        owner_name=owner_name,
        mode="managed",
    )


def _artifact(
    *,
    name: str = "orders-sink",
    cluster: str | None = "production",
    owner_name: str | None = None,
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.OrdersSink",
        topics=["orders.v1", "orders.v2"],
        cluster=cluster,
        config={"tasks.max": 2, "enabled": True},
        ownership=_ownership(owner_name) if owner_name is not None else None,
    )


def _manifest(connector: object) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        artifacts={"connectors": [connector]},  # type: ignore[list-item]
    )


def _project(
    *,
    default: str = "production",
    endpoint: str = _ENDPOINT,
    include_secondary: bool = False,
) -> StreamtProject:
    clusters = {
        default: ConnectClusterConfig(rest_url=endpoint),
    }
    if include_secondary:
        clusters["secondary"] = ConnectClusterConfig(
            rest_url="https://secondary-connect.example.test",
        )
    return StreamtProject(
        project=ProjectInfo(name="payments"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker:9092"),
            connect=ConnectConfig(default=default, clusters=clusters),
        ),
    )


def _binding(
    *,
    alias: str = "production",
    endpoint: str = _ENDPOINT,
) -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint(alias, endpoint)


def _live_deployer(
    *,
    binding: ConnectClusterBinding | None = None,
    exists: bool = False,
) -> MagicMock:
    selected_binding = binding or _binding()
    deployer = MagicMock(spec=ConnectDeployer)
    deployer.cluster_binding = selected_binding
    deployer.require_cluster_binding.return_value = selected_binding
    deployer.resolve_connector_artifact.side_effect = (
        lambda artifact: ConnectDeployer.resolve_connector_artifact(deployer, artifact)
    )

    def planned(artifact: ConnectorArtifact) -> ConnectorChange:
        return ConnectorChange(
            connector_name=artifact.name,
            action="update" if exists else "create",
            current=ConnectorState(name=artifact.name, exists=exists),
            desired=artifact,
            backend_identity=selected_binding.backend_identity,
        )

    deployer.plan_connector.side_effect = planned
    return deployer


def test_offline_plan_accepts_to_dict_shape_and_preserves_cluster() -> None:
    expected = _artifact()

    plan = DeploymentPlanner(
        _manifest(expected.to_dict()),
        project=_project(),
    ).offline_plan()

    assert len(plan.connector_changes) == 1
    assert plan.connector_changes[0].desired == expected
    assert plan.connector_changes[0].desired.cluster == "production"


def test_live_plan_accepts_to_dict_shape_and_preserves_cluster() -> None:
    expected = _artifact()
    deployer = _live_deployer()

    plan = DeploymentPlanner(
        _manifest(expected.to_dict()),
        connect_deployer=deployer,
    ).plan()

    deployer.plan_connector.assert_called_once_with(expected)
    assert plan.connector_changes[0].desired == expected
    assert plan.connector_changes[0].desired.cluster == "production"


def test_live_planner_uses_one_strict_observation_with_exact_config_types() -> None:
    artifact = _artifact(cluster=None)
    desired_config = artifact.to_dict()["config"]
    assert isinstance(desired_config, dict)
    current_config = dict(desired_config)
    current_config["enabled"] = 1
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "name": artifact.name,
        "config": current_config,
        "tasks": [{"connector": artifact.name, "task": 0}],
    }
    deployer = ConnectDeployer(_ENDPOINT, cluster_alias="production")
    request = MagicMock(return_value=response)
    deployer._http_session.request = request  # type: ignore[method-assign]
    deployer.get_connector_state = MagicMock()  # type: ignore[method-assign]

    try:
        plan = DeploymentPlanner(
            _manifest(artifact.to_dict()),
            connect_deployer=deployer,
        ).plan()
    finally:
        deployer.close()

    change = plan.connector_changes[0]
    assert isinstance(change.current, ManagedConnectorObservation)
    assert change.desired is not None
    assert change.desired.cluster == "production"
    assert set(change.changes) == {"enabled"}
    assert change.changes["enabled"]["change"] == "changed"
    assert change.changes["enabled"]["from_fingerprint"] != (
        change.changes["enabled"]["to_fingerprint"]
    )
    assert plan.ownership_requirements[0].observed_action == "update"
    request.assert_called_once()
    deployer.get_connector_state.assert_not_called()


@pytest.mark.parametrize("cluster", [None, "production"])
@pytest.mark.parametrize("mode", ["offline", "live"])
def test_planner_resolves_omitted_or_explicit_default_cluster(
    cluster: str | None,
    mode: str,
) -> None:
    artifact = _artifact(cluster=cluster)
    deployer = _live_deployer()
    planner = DeploymentPlanner(
        _manifest(artifact.to_dict()),
        project=_project(),
        connect_deployer=deployer,
    )

    plan = planner.offline_plan() if mode == "offline" else planner.plan()

    change = plan.connector_changes[0]
    assert change.desired is not None
    assert change.desired.cluster == "production"
    assert change.backend_identity == _binding().backend_identity


@pytest.mark.parametrize("mode", ["offline", "live"])
def test_planner_rejects_explicit_nondefault_cluster_before_provider_calls(
    mode: str,
) -> None:
    artifact = _artifact(cluster="secondary")
    deployer = _live_deployer()
    planner = DeploymentPlanner(
        _manifest(artifact.to_dict()),
        project=_project(include_secondary=True),
        connect_deployer=deployer,
    )

    with pytest.raises(ConnectClusterBindingError, match="bound Kafka Connect cluster"):
        planner.offline_plan() if mode == "offline" else planner.plan()

    deployer.plan_connector.assert_not_called()


def test_live_planner_rejects_unbound_deployer_before_provider_calls() -> None:
    deployer = _live_deployer()
    deployer.cluster_binding = None
    deployer.require_cluster_binding.side_effect = ConnectClusterBindingError(
        "Kafka Connect backend identity requires an effective cluster binding"
    )

    with pytest.raises(ConnectClusterBindingError, match="cluster binding"):
        DeploymentPlanner(
            _manifest(_artifact().to_dict()),
            connect_deployer=deployer,
        ).plan()

    deployer.plan_connector.assert_not_called()


def test_offline_connector_plan_requires_project_runtime_binding() -> None:
    with pytest.raises(ConnectClusterBindingError, match="project runtime"):
        DeploymentPlanner(_manifest(_artifact().to_dict())).offline_plan()


@pytest.mark.parametrize(
    "binding",
    [
        _binding(endpoint="https://old-connect.example.test"),
        _binding(alias="old-default"),
    ],
    ids=["endpoint-drift", "default-alias-drift"],
)
def test_live_deployer_must_match_project_runtime_before_provider_calls(
    binding: ConnectClusterBinding,
) -> None:
    deployer = _live_deployer(binding=binding)

    with pytest.raises(ConnectClusterBindingError, match="project runtime"):
        DeploymentPlanner(
            _manifest(_artifact(cluster=None).to_dict()),
            project=_project(),
            connect_deployer=deployer,
        ).plan()

    deployer.resolve_connector_artifact.assert_not_called()
    deployer.plan_connector.assert_not_called()


@pytest.mark.parametrize(
    "prior_backend",
    [
        "kafka-connect",
        _binding(endpoint="https://old-connect.example.test").backend_identity,
        _binding(alias="old-default").backend_identity,
    ],
    ids=["legacy", "endpoint-drift", "default-alias-drift"],
)
def test_backend_mismatch_blocks_even_when_live_target_is_absent(
    prior_backend: str,
) -> None:
    artifact = _artifact(cluster=None, owner_name="orders")
    connector_id = resource_id("payments", "prod", "connector", "orders")
    prior_state = LocalState(
        project="payments",
        environment="prod",
        resources={
            connector_id: ManagedResourceRecord(
                physical_name=artifact.name,
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=prior_backend,
            )
        },
    )

    plan = DeploymentPlanner(
        _manifest(artifact.to_dict()),
        connect_deployer=_live_deployer(exists=False),
        prior_state=prior_state,
    ).plan()

    assert plan.connector_changes[0].action == "none"
    assert plan.ownership_requirements[0].reason == "state_mismatch"
    assert "provider identity" in plan.ownership_requirements[0].message


def test_exact_backend_allows_create_when_live_target_is_absent() -> None:
    artifact = _artifact(cluster=None, owner_name="orders")
    connector_id = resource_id("payments", "prod", "connector", "orders")
    prior_state = LocalState(
        project="payments",
        environment="prod",
        resources={
            connector_id: ManagedResourceRecord(
                physical_name=artifact.name,
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=_binding().backend_identity,
            )
        },
    )

    plan = DeploymentPlanner(
        _manifest(artifact.to_dict()),
        connect_deployer=_live_deployer(exists=False),
        prior_state=prior_state,
    ).plan()

    assert plan.connector_changes[0].action == "create"
    assert plan.ownership_requirements == []


@pytest.mark.parametrize("collision", ["provider", "owner"])
def test_connector_identity_collisions_fail_before_provider_calls(collision: str) -> None:
    first = _artifact(cluster=None, owner_name="first")
    second = (
        _artifact(cluster=None, owner_name="second")
        if collision == "provider"
        else _artifact(name="orders-sink-copy", cluster=None, owner_name="first")
    )
    deployer = _live_deployer()

    with pytest.raises(StateIdentityError, match="Connector"):
        DeploymentPlanner(
            Manifest(
                version="1.0",
                project_name="payments",
                artifacts={"connectors": [first.to_dict(), second.to_dict()]},
            ),
            connect_deployer=deployer,
        ).plan()

    deployer.plan_connector.assert_not_called()


def _missing_field(value: dict[str, object]) -> object:
    value.pop("cluster")
    return value


def _unknown_field(value: dict[str, object]) -> object:
    value["future"] = True
    return value


def _reserved_mismatch(value: dict[str, object]) -> object:
    config = value["config"]
    assert isinstance(config, dict)
    config["connector.class"] = "com.example.WrongSink"
    return value


def _malformed_config(value: dict[str, object]) -> object:
    value["config"] = "not-an-object"
    return value


def _non_object(_value: dict[str, object]) -> object:
    return ["not-an-artifact"]


@pytest.mark.parametrize(
    "mutate",
    [
        _missing_field,
        _unknown_field,
        _reserved_mismatch,
        _malformed_config,
        _non_object,
    ],
    ids=["missing", "unknown", "reserved-mismatch", "malformed", "non-object"],
)
@pytest.mark.parametrize("mode", ["offline", "live"])
def test_planner_propagates_strict_connector_format_errors(
    mutate: Callable[[dict[str, object]], object],
    mode: str,
) -> None:
    connector = mutate(deepcopy(_artifact().to_dict()))
    deployer = _live_deployer()
    planner = DeploymentPlanner(
        _manifest(connector),
        project=_project(),
        connect_deployer=deployer,
    )

    with pytest.raises(ConnectorArtifactFormatError):
        planner.offline_plan() if mode == "offline" else planner.plan()

    deployer.plan_connector.assert_not_called()
