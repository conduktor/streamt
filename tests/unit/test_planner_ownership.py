"""Ownership policy tests for DeploymentPlanner."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact, Manifest
from streamt.core.models import ProjectInfo, StreamtProject
from streamt.core.runtime import (
    ConduktorConfig,
    ConnectClusterConfig,
    ConnectConfig,
    GatewayConfig,
    KafkaConfig,
    RuntimeConfig,
)
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectorChange,
    ConnectorState,
)
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    ManagedGatewayRuleObservation,
)
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.planner import DeploymentPlanner
from streamt.deployer.schema_registry import SchemaChange, SchemaState
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_CONNECT_BINDING = ConnectClusterBinding.from_endpoint(
    "production",
    "https://connect.example.test",
)
_GATEWAY_URL = "https://gateway.example.test"
_GATEWAY_BINDING = GatewayBackendBinding.from_endpoint(_GATEWAY_URL)


def _project() -> StreamtProject:
    return StreamtProject(
        project=ProjectInfo(name="payments"),
        runtime=RuntimeConfig(
            kafka=KafkaConfig(bootstrap_servers="broker:9092"),
            connect=ConnectConfig(
                default="production",
                clusters={
                    "production": ConnectClusterConfig(
                        rest_url="https://connect.example.test"
                    )
                },
            ),
            conduktor=ConduktorConfig(
                gateway=GatewayConfig(admin_url=_GATEWAY_URL)
            ),
        ),
    )


def _ownership(owner_type: str, owner_name: str, mode: str = "managed") -> dict[str, str]:
    return ArtifactOwnership(
        project="payments",
        owner_type=owner_type,
        owner_name=owner_name,
        mode=mode,
    ).to_dict()


def _manifest(mode: str = "managed") -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        artifacts={
            "schemas": [
                {
                    "subject": "events-value",
                    "schema": {"type": "record", "name": "Event", "fields": []},
                    "schema_type": "AVRO",
                    "ownership": _ownership("source", "raw_events", mode),
                }
            ],
            "topics": [
                {
                    "name": "payments.events.v1",
                    "partitions": 3,
                    "replication_factor": 1,
                    "config": {},
                    "ownership": _ownership("model", "events_model", mode),
                }
            ],
            "flink_jobs": [
                {
                    "name": "transform",
                    "sql": "SELECT 1",
                    "ownership": _ownership("model", "transform", mode),
                }
            ],
            "connectors": [
                {
                    "name": "sink",
                    "connector_class": "example.Sink",
                    "topics": ["payments.events.v1"],
                    "cluster": None,
                    "config": {
                        "name": "sink",
                        "connector.class": "example.Sink",
                        "topics": "payments.events.v1",
                    },
                    "ownership": _ownership("model", "sink", mode),
                }
            ],
            "gateway_rules": [
                {
                    "name": "alias_rule",
                    "virtualTopic": "virtual-events",
                    "physicalTopic": "payments.events.v1",
                    "interceptors": [],
                    "ownership": _ownership("model", "alias_rule", mode),
                }
            ],
        },
    )


def _deployers(*, exists: bool, no_op_topic: bool = False) -> dict[str, MagicMock]:
    schema = MagicMock()
    schema.plan_schema.return_value = SchemaChange(
        subject="events-value",
        action="update" if exists else "register",
        current=SchemaState(subject="events-value", exists=exists),
    )

    kafka = MagicMock()
    kafka.plan_topic.return_value = TopicChange(
        topic="payments.events.v1",
        action="none" if exists and no_op_topic else ("update" if exists else "create"),
        current=TopicState(name="payments.events.v1", exists=exists),
    )

    flink = MagicMock()
    flink.plan_job.return_value = FlinkJobChange(
        job_name="transform",
        action="update" if exists else "submit",
        current=FlinkJobState(name="transform", exists=exists),
    )

    connect = MagicMock()
    connect.cluster_binding = _CONNECT_BINDING
    connect.require_cluster_binding.return_value = _CONNECT_BINDING
    connect.resolve_connector_artifact.side_effect = (
        lambda artifact: ConnectDeployer.resolve_connector_artifact(connect, artifact)
    )

    def plan_connector(artifact: ConnectorArtifact) -> ConnectorChange:
        return ConnectorChange(
            connector_name="sink",
            action="update" if exists else "create",
            current=ConnectorState(name="sink", exists=exists),
            desired=artifact,
            backend_identity=_CONNECT_BINDING.backend_identity,
        )

    connect.plan_connector.side_effect = plan_connector

    gateway = MagicMock()
    gateway.cluster_binding = _GATEWAY_BINDING
    snapshot = MagicMock()
    snapshot.binding = _GATEWAY_BINDING
    snapshot.rule.return_value = ManagedGatewayRuleObservation(
        binding=_GATEWAY_BINDING,
        logical_name="alias_rule",
        alias_name="virtual-events",
        exists=exists,
        physical_name="payments.events.old" if exists else None,
        physical_cluster="main" if exists else None,
    )
    gateway.observe_managed_gateway_snapshot.return_value = snapshot
    return {
        "schema_registry_deployer": schema,
        "kafka_deployer": kafka,
        "flink_deployer": flink,
        "connect_deployer": connect,
        "gateway_deployer": gateway,
    }


def _record(
    physical_name: str,
    ownership: str = "managed",
    *,
    backend: str = "test",
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership=ownership,  # type: ignore[arg-type]
        artifact_checksum=artifact_checksum({"physical_name": physical_name}),
        backend=backend,
    )


def _prior_state() -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        serial=3,
        resources={
            resource_id("payments", "prod", "schema", "raw_events"): _record(
                "events-value", "adopted"
            ),
            resource_id("payments", "prod", "topic", "events_model"): _record(
                "payments.events.v1"
            ),
            resource_id("payments", "prod", "flink_job", "transform"): _record(
                "transform"
            ),
            resource_id("payments", "prod", "connector", "sink"): _record(
                "sink",
                backend=_CONNECT_BINDING.backend_identity,
            ),
            resource_id("payments", "prod", "gateway_rule", "alias_rule"): _record(
                "virtual-events",
                backend=_GATEWAY_BINDING.backend_identity,
            ),
        },
    )


def _actions(plan) -> list[str]:
    return [
        plan.schema_changes[0].action,
        plan.topic_changes[0].action,
        plan.flink_changes[0].action,
        plan.connector_changes[0].action,
        plan.gateway_changes[0].action,
    ]


class TestOwnershipPlanning:
    def test_existing_resources_without_prior_state_require_adoption(self):
        planner = DeploymentPlanner(
            _manifest(),
            project=_project(),
            environment="prod",
            **_deployers(exists=True),
        )

        plan = planner.plan()

        assert _actions(plan) == ["none"] * 5
        assert {requirement.reason for requirement in plan.ownership_requirements} == {
            "requires_adoption"
        }
        assert len(plan.ownership_requirements) == 5
        assert not plan.has_changes
        assert plan.has_ownership_requirements
        assert plan.is_apply_blocked
        assert len(plan.blocking_ownership_requirements) == 5

    def test_absent_managed_resources_may_be_created_without_prior_state(self):
        plan = DeploymentPlanner(
            _manifest(),
            project=_project(),
            environment="prod",
            **_deployers(exists=False),
        ).plan()

        assert _actions(plan) == ["register", "create", "submit", "create", "create"]
        assert plan.ownership_requirements == []
        assert plan.creates == 5
        assert not plan.is_apply_blocked

    def test_matching_managed_or_adopted_state_allows_updates_and_no_ops(self):
        plan = DeploymentPlanner(
            _manifest(),
            project=_project(),
            prior_state=_prior_state(),
            **_deployers(exists=True, no_op_topic=True),
        ).plan()

        assert _actions(plan) == ["update", "none", "update", "update", "update"]
        assert plan.ownership_requirements == []
        assert plan.updates == 4

    def test_external_resources_are_observe_only_even_when_absent(self):
        plan = DeploymentPlanner(
            _manifest(mode="external"),
            project=_project(),
            environment="prod",
            **_deployers(exists=False),
        ).plan()

        assert _actions(plan) == ["none"] * 5
        assert {requirement.reason for requirement in plan.ownership_requirements} == {
            "external"
        }
        assert not plan.has_changes
        assert not plan.is_apply_blocked

    def test_legacy_artifact_is_safe_for_create_but_requires_adoption_when_live(self):
        manifest = _manifest()
        manifest.artifacts["topics"][0].pop("ownership")
        manifest.artifacts["connectors"] = []
        manifest.artifacts["gateway_rules"] = []
        deployers = _deployers(exists=True)
        plan = DeploymentPlanner(
            manifest,
            environment="prod",
            kafka_deployer=deployers["kafka_deployer"],
        ).plan()

        assert plan.topic_changes[0].action == "none"
        assert plan.ownership_requirements[0].reason == "requires_adoption"
        assert plan.ownership_requirements[0].ownership_mode == "managed"
        assert plan.ownership_requirements[0].resource_id == (
            "streamt://payments/prod/topic/payments.events.v1"
        )

    def test_state_physical_name_mismatch_blocks_mutation(self):
        state = _prior_state()
        topic_id = resource_id("payments", "prod", "topic", "events_model")
        state.resources[topic_id] = _record("some-other-topic")
        deployers = _deployers(exists=True)

        manifest = _manifest()
        manifest.artifacts["connectors"] = []
        manifest.artifacts["gateway_rules"] = []
        plan = DeploymentPlanner(
            manifest,
            prior_state=state,
            kafka_deployer=deployers["kafka_deployer"],
        ).plan()

        assert plan.topic_changes[0].action == "none"
        assert plan.ownership_requirements[0].reason == "state_mismatch"

    def test_neutralized_changes_are_never_applied(self):
        deployers = _deployers(exists=True)
        planner = DeploymentPlanner(
            _manifest(),
            project=_project(),
            environment="prod",
            **deployers,
        )
        plan = planner.plan()

        planner.apply(plan)

        deployers["schema_registry_deployer"].apply_schema.assert_not_called()
        deployers["kafka_deployer"].apply_topic.assert_not_called()
        deployers["flink_deployer"].apply_job.assert_not_called()
        deployers["connect_deployer"].apply_connector.assert_not_called()
        deployers["gateway_deployer"].apply.assert_not_called()

    def test_prior_state_identity_must_match_planner(self):
        with pytest.raises(StateIdentityError, match="expected 'other'"):
            DeploymentPlanner(_manifest(), prior_state=_prior_state(), project_name="other")


class TestOwnershipRequirementOutput:
    def test_details_and_machine_record_explain_required_adoption(self):
        deployers = _deployers(exists=True)
        manifest = _manifest()
        manifest.artifacts["connectors"] = []
        manifest.artifacts["gateway_rules"] = []
        plan = DeploymentPlanner(
            manifest,
            environment="prod",
            kafka_deployer=deployers["kafka_deployer"],
        ).plan()

        details = plan.details(color=False)
        requirement = plan.ownership_requirements[0]
        machine = requirement.to_dict()

        assert "1 ownership requirement(s)" in details
        assert "Ownership Requirements:" in details
        assert "explicit adoption is required" in details
        assert machine["reason"] == "requires_adoption"
        assert machine["resource_id"] == "streamt://payments/prod/topic/events_model"
        assert machine["logical_name"] == "events_model"
        assert machine["physical_name"] == "payments.events.v1"
        assert machine["observed_action"] == "update"
