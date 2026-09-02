"""Strict compiled Connector artifact handling at planner boundaries."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from streamt.compiler.connector_artifact import ConnectorArtifactFormatError
from streamt.compiler.manifest import ConnectorArtifact, Manifest
from streamt.deployer.connect import ConnectDeployer, ConnectorChange, ConnectorState
from streamt.deployer.planner import DeploymentPlanner


def _artifact() -> ConnectorArtifact:
    return ConnectorArtifact(
        name="orders-sink",
        connector_class="com.example.OrdersSink",
        topics=["orders.v1", "orders.v2"],
        cluster="production",
        config={"tasks.max": 2, "enabled": True},
    )


def _manifest(connector: object) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        artifacts={"connectors": [connector]},  # type: ignore[list-item]
    )


def _live_deployer() -> MagicMock:
    deployer = MagicMock(spec=ConnectDeployer)

    def planned(artifact: ConnectorArtifact) -> ConnectorChange:
        return ConnectorChange(
            connector_name=artifact.name,
            action="create",
            current=ConnectorState(name=artifact.name, exists=False),
            desired=artifact,
        )

    deployer.plan_connector.side_effect = planned
    return deployer


def test_offline_plan_accepts_to_dict_shape_and_preserves_cluster() -> None:
    expected = _artifact()

    plan = DeploymentPlanner(_manifest(expected.to_dict())).offline_plan()

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
        connect_deployer=deployer,
    )

    with pytest.raises(ConnectorArtifactFormatError):
        planner.offline_plan() if mode == "offline" else planner.plan()

    deployer.plan_connector.assert_not_called()
