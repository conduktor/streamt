"""Canonical planned-action identity contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    FlinkJobArtifact,
    GatewayRuleArtifact,
    Manifest,
    SchemaArtifact,
    TopicArtifact,
)
from streamt.deployer.connect import ConnectorChange
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayRuleChange,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule,
    plan_managed_gateway_rule_deletion,
)
from streamt.deployer.kafka import TopicChange
from streamt.deployer.planner import DeploymentPlan, DeploymentPlanner
from streamt.deployer.schema_registry import SchemaChange
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)

_GATEWAY_BINDING = GatewayBackendBinding.from_endpoint(
    "https://gateway.example.test",
    virtual_cluster="production",
)


def _ownership(logical_name: str) -> ArtifactOwnership:
    return ArtifactOwnership(
        project="payments",
        owner_type="model",
        owner_name=logical_name,
    )


def _record(
    physical_name: str,
    *,
    backend: str = "test",
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership="managed",
        artifact_checksum=artifact_checksum({"physical_name": physical_name}),
        backend=backend,
    )


def _planner(prior_state: LocalState) -> DeploymentPlanner:
    deployer = MagicMock()
    return DeploymentPlanner(
        Manifest(version="1.0", project_name="payments"),
        schema_registry_deployer=deployer,
        kafka_deployer=deployer,
        flink_deployer=deployer,
        connect_deployer=deployer,
        gateway_deployer=deployer,
        prior_state=prior_state,
        environment="prod",
    )


def test_planned_actions_are_canonical_ordered_and_compatible() -> None:
    prior_state = LocalState(
        project="payments",
        environment="prod",
        serial=7,
        resources={
            resource_id("payments", "prod", "schema", "schema_delete"): _record(
                "runtime-schema-delete"
            ),
            resource_id("payments", "prod", "topic", "topic_delete"): _record(
                "runtime-topic-delete"
            ),
            resource_id("payments", "prod", "flink_job", "flink_delete"): _record(
                "runtime-flink-delete"
            ),
            resource_id("payments", "prod", "connector", "connector_delete"): _record(
                "runtime-connector-delete"
            ),
            resource_id("payments", "prod", "gateway_rule", "gateway_delete"): _record(
                "runtime-gateway-alias-delete",
                backend=_GATEWAY_BINDING.backend_identity,
            ),
        },
    )
    schema_register = SchemaArtifact(
        subject="runtime-schema-register",
        schema={},
        ownership=_ownership("schema_register"),
    )
    schema_update = SchemaArtifact(
        subject="runtime-schema-update",
        schema={},
        ownership=_ownership("schema_update"),
    )
    topic_create = TopicArtifact(
        name="runtime-topic-create",
        partitions=1,
        replication_factor=1,
        ownership=_ownership("topic_create"),
    )
    topic_update = TopicArtifact(
        name="runtime-topic-update",
        partitions=2,
        replication_factor=1,
        ownership=_ownership("topic_update"),
    )
    flink_submit = FlinkJobArtifact(
        name="runtime-flink-submit",
        sql="SELECT 1",
        ownership=_ownership("flink_submit"),
    )
    flink_update = FlinkJobArtifact(
        name="runtime-flink-update",
        sql="SELECT 2",
        ownership=_ownership("flink_update"),
    )
    connector_create = ConnectorArtifact(
        name="runtime-connector-create",
        connector_class="example.Sink",
        topics=["input"],
        ownership=_ownership("connector_create"),
    )
    connector_update = ConnectorArtifact(
        name="runtime-connector-update",
        connector_class="example.Sink",
        topics=["input"],
        ownership=_ownership("connector_update"),
    )
    gateway_create = GatewayRuleArtifact(
        name="runtime-gateway-create",
        virtual_topic="runtime-gateway-alias-create",
        physical_topic="physical-input",
        ownership=_ownership("gateway_create"),
    )
    gateway_update = GatewayRuleArtifact(
        name="runtime-gateway-update",
        virtual_topic="runtime-gateway-alias-update",
        physical_topic="physical-input",
        ownership=_ownership("gateway_update"),
    )
    gateway_binding = _GATEWAY_BINDING
    gateway_create_desired = build_desired_gateway_rule(
        gateway_create,
        gateway_binding,
    )
    gateway_update_desired = build_desired_gateway_rule(
        gateway_update,
        gateway_binding,
    )
    gateway_delete_current = ManagedGatewayRuleObservation(
        binding=gateway_binding,
        logical_name="runtime-gateway-delete",
        alias_name="runtime-gateway-alias-delete",
        exists=True,
        physical_name="physical-input",
        physical_cluster="main",
    )
    gateway_delete = plan_managed_gateway_rule_deletion(gateway_delete_current)
    plan = DeploymentPlan(
        schema_changes=[
            SchemaChange(
                subject=schema_register.subject,
                action="register",
                desired=schema_register,
            ),
            SchemaChange(
                subject=schema_update.subject,
                action="update",
                desired=schema_update,
            ),
            SchemaChange(subject="runtime-schema-delete", action="delete"),
        ],
        topic_changes=[
            TopicChange(
                topic=topic_create.name,
                action="create",
                desired=topic_create,
            ),
            TopicChange(
                topic=topic_update.name,
                action="update",
                desired=topic_update,
            ),
            TopicChange(topic="runtime-topic-delete", action="delete"),
        ],
        flink_changes=[
            FlinkJobChange(
                job_name=flink_submit.name,
                action="submit",
                desired=flink_submit,
            ),
            FlinkJobChange(
                job_name=flink_update.name,
                action="update",
                desired=flink_update,
            ),
            FlinkJobChange(
                job_name="runtime-flink-delete",
                action="cancel",
                current=FlinkJobState(
                    name="runtime-flink-delete",
                    exists=True,
                    job_id="job-123",
                ),
            ),
        ],
        connector_changes=[
            ConnectorChange(
                connector_name=connector_create.name,
                action="create",
                desired=connector_create,
            ),
            ConnectorChange(
                connector_name=connector_update.name,
                action="update",
                desired=connector_update,
            ),
            ConnectorChange(
                connector_name="runtime-connector-delete",
                action="delete",
            ),
        ],
        gateway_changes=[
            plan_managed_gateway_rule(
                gateway_create,
                gateway_create_desired,
                ManagedGatewayRuleObservation(
                    binding=gateway_binding,
                    logical_name=gateway_create.name,
                    alias_name=gateway_create.virtual_topic,
                    exists=False,
                ),
            ),
            plan_managed_gateway_rule(
                gateway_update,
                gateway_update_desired,
                ManagedGatewayRuleObservation(
                    binding=gateway_binding,
                    logical_name=gateway_update.name,
                    alias_name=gateway_update.virtual_topic,
                    exists=True,
                    physical_name="previous-physical-input",
                    physical_cluster="main",
                ),
            ),
            gateway_delete,
        ],
    )
    planner = _planner(prior_state)

    actions = planner.planned_actions(plan)

    expected = [
        ("schema", "schema_register", "schema:runtime-schema-register", "register"),
        ("schema", "schema_update", "schema:runtime-schema-update", "update"),
        ("schema", "schema_delete", "schema:runtime-schema-delete", "delete"),
        ("topic", "topic_create", "topic:runtime-topic-create", "create"),
        ("topic", "topic_update", "topic:runtime-topic-update", "update"),
        ("topic", "topic_delete", "topic:runtime-topic-delete", "delete"),
        ("flink_job", "flink_submit", "flink_job:runtime-flink-submit", "submit"),
        ("flink_job", "flink_update", "flink_job:runtime-flink-update", "update"),
        ("flink_job", "flink_delete", "flink_job:runtime-flink-delete", "cancel"),
        (
            "connector",
            "connector_create",
            "connector:runtime-connector-create",
            "create",
        ),
        (
            "connector",
            "connector_update",
            "connector:runtime-connector-update",
            "update",
        ),
        (
            "connector",
            "connector_delete",
            "connector:runtime-connector-delete",
            "delete",
        ),
        (
            "gateway_rule",
            "gateway_create",
            "gateway_rule:runtime-gateway-create",
            "create",
        ),
        (
            "gateway_rule",
            "gateway_update",
            "gateway_rule:runtime-gateway-update",
            "update",
        ),
        (
            "gateway_rule",
            "gateway_delete",
            "gateway_rule:runtime-gateway-delete",
            "delete",
        ),
    ]
    assert [
        (
            ResourceIdentity.parse(action.resource_id).kind,
            ResourceIdentity.parse(action.resource_id).logical_name,
            action.runtime_label,
            action.action,
        )
        for action in actions
    ] == expected
    assert actions == planner.planned_actions(plan)
    assert planner.operation_actions(plan) == [
        (action.runtime_label, action.action) for action in actions
    ]
    assert all(action.resource_id.startswith("streamt://payments/prod/") for action in actions)
    assert all(action.runtime_label != action.resource_id for action in actions)
    assert all(
        action.runtime_label.partition(":")[2] not in action.resource_id
        for action in actions
    )

    with pytest.raises(FrozenInstanceError):
        actions[0].action = "delete"  # type: ignore[misc]


def test_planned_actions_skip_runtime_actions_apply_would_not_attempt() -> None:
    planner = _planner(LocalState(project="payments", environment="prod"))
    plan = DeploymentPlan(
        schema_changes=[SchemaChange(subject="schema", action="register")],
        topic_changes=[TopicChange(topic="topic", action="create")],
        flink_changes=[
            FlinkJobChange(
                job_name="flink",
                action="cancel",
                current=FlinkJobState(name="flink", exists=True, job_id=None),
            )
        ],
        connector_changes=[ConnectorChange(connector_name="connector", action="none")],
        gateway_changes=[GatewayRuleChange(name="gateway", action="none")],
    )

    assert planner.planned_actions(plan) == []
    assert planner.operation_actions(plan) == []


def test_actionable_legacy_change_without_logical_identity_fails_closed() -> None:
    planner = _planner(LocalState(project="payments", environment="prod"))
    desired = TopicArtifact(
        name="physical-only-topic",
        partitions=1,
        replication_factor=1,
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=desired.name,
                action="create",
                desired=desired,
            )
        ]
    )

    with pytest.raises(
        StateIdentityError,
        match="no canonical ownership identity",
    ):
        planner.planned_actions(plan)


def test_ambiguous_prior_identity_fails_closed() -> None:
    physical_name = "shared-physical-topic"
    prior_state = LocalState(
        project="payments",
        environment="prod",
        serial=4,
        resources={
            resource_id("payments", "prod", "topic", "first_logical"): _record(
                physical_name
            ),
            resource_id("payments", "prod", "topic", "second_logical"): _record(
                physical_name
            ),
        },
    )
    planner = _planner(prior_state)
    plan = DeploymentPlan(
        topic_changes=[TopicChange(topic=physical_name, action="delete")]
    )

    with pytest.raises(
        StateIdentityError,
        match="ambiguous ownership identity",
    ):
        planner.planned_actions(plan)


def test_duplicate_canonical_action_identity_fails_closed() -> None:
    planner = _planner(LocalState(project="payments", environment="prod"))
    first = TopicArtifact(
        name="runtime-topic-first",
        partitions=1,
        replication_factor=1,
        ownership=_ownership("shared_logical_topic"),
    )
    second = TopicArtifact(
        name="runtime-topic-second",
        partitions=1,
        replication_factor=1,
        ownership=_ownership("shared_logical_topic"),
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(topic=first.name, action="create", desired=first),
            TopicChange(topic=second.name, action="create", desired=second),
        ]
    )

    with pytest.raises(
        StateIdentityError,
        match="duplicate canonical action identity",
    ):
        planner.planned_actions(plan)
