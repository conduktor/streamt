"""Tests for fail-closed recovery evidence derived from fresh live plans."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from typing import Protocol, cast

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
from streamt.deployer.connect import ConnectorChange, ConnectorState
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.gateway import (
    AliasTopicState,
    GatewayRuleChange,
    InterceptorState,
)
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.planner import (
    DeploymentPlan,
    DeploymentPlanner,
    OwnershipRequirement,
)
from streamt.deployer.recovery import RecoveryResolution, RecoverySnapshotEvidence
from streamt.deployer.recovery_observer import (
    DeploymentPlanRecoveryObserver,
    RecoveryObservationError,
)
from streamt.deployer.recovery_service import RecoveryLiveObservation
from streamt.deployer.schema_registry import SchemaChange, SchemaState
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    OwnershipMode,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationKind,
    OperationSnapshot,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

PROJECT = "payments"
ENVIRONMENT = "prod"
OPERATION_ID = "00000000-0000-4000-8000-000000000201"
STORE_ID = "00000000-0000-4000-8000-000000000202"


class _Artifact(Protocol):
    def to_dict(self) -> dict[str, object]: ...


def _ownership(
    name: str,
    *,
    mode: str = "managed",
    owner_type: str = "model",
) -> ArtifactOwnership:
    return ArtifactOwnership(
        project=PROJECT,
        owner_type=owner_type,
        owner_name=name,
        mode=mode,
    )


def _record(
    artifact: _Artifact,
    *,
    physical_name: str,
    backend: str,
    ownership: OwnershipMode = "managed",
) -> ManagedResourceRecord:
    raw = artifact.to_dict()
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership=ownership,
        artifact_checksum=artifact_checksum(raw),
        backend=backend,
    )


def _state(
    resources: dict[str, ManagedResourceRecord] | None = None,
    *,
    serial: int = 7,
) -> LocalState:
    return LocalState(
        project=PROJECT,
        environment=ENVIRONMENT,
        serial=serial,
        resources=resources or {},
    )


def _snapshot(
    state: LocalState,
    actions: tuple[OperationAction, ...],
    *,
    kind: OperationKind = "apply",
) -> RecoverySnapshotEvidence:
    address = StateAddress(namespace="platform", project=PROJECT, environment=ENVIRONMENT)
    intent = OperationIntent(
        operation_id=OPERATION_ID,
        kind=kind,
        started_at="2026-09-02T12:00:00Z",
        actor="operator",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=actions,
    )
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
    )
    return RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(backend="postgres", store_id=STORE_ID),
                address=address,
                state=state,
                revision=StateRevision("state-revision"),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("control-revision"),
            ),
        )
    )


def _action(kind: str, name: str, action: str, *, index: int = 0) -> OperationAction:
    return OperationAction(
        index=index,
        resource_id=resource_id(PROJECT, ENVIRONMENT, kind, name),
        action=action,
    )


def _planner(state: LocalState, **deployers: object) -> DeploymentPlanner:
    return DeploymentPlanner(
        Manifest(version="1", project_name=PROJECT),
        prior_state=state,
        environment=ENVIRONMENT,
        **deployers,  # type: ignore[arg-type]
    )


def _observe(
    state: LocalState,
    plan: DeploymentPlan,
    actions: tuple[OperationAction, ...],
    *,
    resolution: RecoveryResolution = "observed",
    intent_kind: OperationKind = "apply",
    planner: DeploymentPlanner | None = None,
) -> RecoveryLiveObservation:
    observer = DeploymentPlanRecoveryObserver(planner or _planner(state), plan)
    return observer.observe_recovery_targets(
        resolution=resolution,
        snapshot=_snapshot(state, actions, kind=intent_kind),
    )


def _candidate_cases() -> Iterator[tuple[OperationAction, DeploymentPlan, str]]:
    schema = SchemaArtifact(
        subject="orders-value",
        schema={"type": "record", "name": "Order", "fields": []},
        schema_type="AVRO",
        compatibility="BACKWARD",
        ownership=_ownership("orders"),
    )
    yield (
        _action("schema", "orders", "register"),
        DeploymentPlan(
            schema_changes=[
                SchemaChange(
                    subject=schema.subject,
                    action="none",
                    current=SchemaState(
                        subject=schema.subject,
                        exists=True,
                        version=1,
                        schema_id=10,
                        schema=schema.schema,
                        schema_type="AVRO",
                        compatibility="BACKWARD",
                    ),
                    desired=schema,
                )
            ]
        ),
        "schema-registry",
    )

    topic = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "compact"},
        ownership=_ownership("orders"),
    )
    yield (
        _action("topic", "orders", "create"),
        DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic=topic.name,
                    action="none",
                    current=TopicState(
                        name=topic.name,
                        exists=True,
                        partitions=3,
                        replication_factor=1,
                        config={"cleanup.policy": "compact"},
                    ),
                    desired=topic,
                )
            ]
        ),
        "direct-kafka",
    )

    connector = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        config={"batch.size": "100"},
        ownership=_ownership("orders_sink"),
    )
    yield (
        _action("connector", "orders_sink", "create"),
        DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=connector.name,
                    action="none",
                    current=ConnectorState(
                        name=connector.name,
                        exists=True,
                        config=connector.to_dict()["config"],  # type: ignore[arg-type]
                        status="RUNNING",
                        tasks=[{"id": 0, "state": "RUNNING"}],
                    ),
                    desired=connector,
                )
            ]
        ),
        "kafka-connect",
    )

    gateway = GatewayRuleArtifact(
        name="orders_alias",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        ownership=_ownership("orders_alias"),
    )
    yield (
        _action("gateway_rule", "orders_alias", "create"),
        DeploymentPlan(
            gateway_changes=[
                GatewayRuleChange(
                    name=gateway.name,
                    action="none",
                    current_alias=AliasTopicState(
                        name=gateway.virtual_topic,
                        exists=True,
                        physical_topic=gateway.physical_topic,
                    ),
                    current_interceptors=[],
                    desired=gateway,
                )
            ]
        ),
        "conduktor-gateway",
    )


@pytest.mark.parametrize(("action", "plan", "backend"), list(_candidate_cases()))
def test_observed_candidate_supported_kinds(
    action: OperationAction,
    plan: DeploymentPlan,
    backend: str,
) -> None:
    state = _state()

    result = _observe(state, plan, (action,))

    assert result.targets[0].action == action
    assert result.targets[0].presence == "present"
    assert result.targets[0].accepted_as == "candidate"
    assert result.targets[0].fingerprint.startswith("sha256:")
    assert result.candidate_state is not None
    assert result.candidate_state.serial == state.serial + 1
    assert result.candidate_state.resources[action.resource_id].backend == backend


@pytest.mark.parametrize(("kind", "change", "verb"), [
    (
        "schema",
        SchemaChange(
            subject="gone-value",
            action="delete",
            current=SchemaState(subject="gone-value", exists=False),
        ),
        "delete",
    ),
    (
        "topic",
        TopicChange(
            topic="gone.v1",
            action="delete",
            current=TopicState(name="gone.v1", exists=False),
        ),
        "delete",
    ),
    (
        "flink_job",
        FlinkJobChange(
            job_name="gone_job",
            action="cancel",
            current=FlinkJobState(name="gone_job", exists=False),
        ),
        "cancel",
    ),
    (
        "connector",
        ConnectorChange(
            connector_name="gone_sink",
            action="delete",
            current=ConnectorState(name="gone_sink", exists=False),
        ),
        "delete",
    ),
    (
        "gateway_rule",
        GatewayRuleChange(
            name="gone_alias",
            action="delete",
            current_alias=AliasTopicState(name="gone_alias", exists=False),
            current_interceptors=[],
        ),
        "delete",
    ),
])
def test_observed_absence_removes_deleted_ownership(
    kind: str,
    change: object,
    verb: str,
) -> None:
    name = {
        "schema": "gone",
        "topic": "gone",
        "flink_job": "gone",
        "connector": "gone",
        "gateway_rule": "gone",
    }[kind]
    target = _action(kind, name, verb)
    backend = {
        "schema": "schema-registry",
        "topic": "direct-kafka",
        "flink_job": "flink",
        "connector": "kafka-connect",
        "gateway_rule": "conduktor-gateway",
    }[kind]
    physical = {
        "schema": "gone-value",
        "topic": "gone.v1",
        "flink_job": "gone_job",
        "connector": "gone_sink",
        "gateway_rule": "gone_alias",
    }[kind]
    prior = ManagedResourceRecord(
        physical_name=physical,
        ownership="managed",
        artifact_checksum=artifact_checksum({"prior": kind}),
        backend=backend,
    )
    state = _state({target.resource_id: prior})
    if kind == "schema":
        plan = DeploymentPlan(schema_changes=[cast(SchemaChange, change)])
    elif kind == "topic":
        plan = DeploymentPlan(topic_changes=[cast(TopicChange, change)])
    elif kind == "flink_job":
        plan = DeploymentPlan(flink_changes=[cast(FlinkJobChange, change)])
    elif kind == "connector":
        plan = DeploymentPlan(connector_changes=[cast(ConnectorChange, change)])
    else:
        plan = DeploymentPlan(gateway_changes=[cast(GatewayRuleChange, change)])

    result = _observe(state, plan, (target,))

    assert result.targets[0].presence == "absent"
    assert result.targets[0].accepted_as == "candidate"
    assert result.candidate_state is not None
    assert target.resource_id not in result.candidate_state.resources
    assert result.candidate_state.serial == state.serial + 1


def test_rolled_back_requires_and_accepts_exact_prior_topic_representation() -> None:
    old = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        config={"cleanup.policy": "delete"},
        ownership=_ownership("orders"),
    )
    desired = replace(old, partitions=6)
    target = _action("topic", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                old,
                physical_name=old.name,
                backend="direct-kafka",
            )
        }
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=old.name,
                action="update",
                current=TopicState(
                    name=old.name,
                    exists=True,
                    partitions=old.partitions,
                    replication_factor=old.replication_factor,
                    config=old.config,
                ),
                desired=desired,
            )
        ]
    )

    result = _observe(state, plan, (target,), resolution="rolled_back")

    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state is None


def test_rolled_back_create_accepts_exact_absence() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    target = _action("topic", "orders", "create")
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=artifact.name,
                action="create",
                current=TopicState(name=artifact.name, exists=False),
                desired=artifact,
            )
        ]
    )

    result = _observe(_state(), plan, (target,), resolution="rolled_back")

    assert result.targets[0].presence == "absent"
    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state is None


def test_rolled_back_rejects_prior_record_with_wrong_backend() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    target = _action("topic", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                artifact,
                physical_name=artifact.name,
                backend="wrong-provider",
            )
        }
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=artifact.name,
                action="update",
                current=TopicState(
                    name=artifact.name,
                    exists=True,
                    partitions=3,
                    replication_factor=1,
                    config={},
                ),
                desired=replace(artifact, partitions=6),
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="does not exactly match prior"):
        _observe(state, plan, (target,), resolution="rolled_back")


def test_observed_mixes_prior_and_candidate_in_intent_order_and_preserves_unrelated() -> None:
    old_topic = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    new_topic = replace(old_topic, partitions=6)
    schema = SchemaArtifact(
        subject="orders-value",
        schema={"type": "record", "name": "Order", "fields": []},
        ownership=_ownership("orders_schema"),
    )
    topic_action = _action("topic", "orders", "update", index=0)
    schema_action = _action("schema", "orders_schema", "register", index=1)
    unrelated_id = resource_id(PROJECT, ENVIRONMENT, "topic", "unrelated")
    unrelated = ManagedResourceRecord(
        physical_name="unrelated.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"unrelated": True}),
        backend="direct-kafka",
    )
    state = _state(
        {
            topic_action.resource_id: _record(
                old_topic,
                physical_name=old_topic.name,
                backend="direct-kafka",
            ),
            unrelated_id: unrelated,
        }
    )
    plan = DeploymentPlan(
        # Deliberately reverse collection order from the durable intent.
        schema_changes=[
            SchemaChange(
                subject=schema.subject,
                action="none",
                current=SchemaState(
                    subject=schema.subject,
                    exists=True,
                    version=1,
                    schema_id=1,
                    schema=schema.schema,
                    schema_type="AVRO",
                ),
                desired=schema,
            )
        ],
        topic_changes=[
            TopicChange(
                topic=old_topic.name,
                action="update",
                current=TopicState(
                    name=old_topic.name,
                    exists=True,
                    partitions=3,
                    replication_factor=1,
                    config={},
                ),
                desired=new_topic,
            )
        ],
    )

    result = _observe(state, plan, (topic_action, schema_action))

    assert [target.action for target in result.targets] == [topic_action, schema_action]
    assert [target.accepted_as for target in result.targets] == ["prior", "candidate"]
    assert result.candidate_state is not None
    assert result.candidate_state.resources[topic_action.resource_id] == state.resources[
        topic_action.resource_id
    ]
    assert result.candidate_state.resources[unrelated_id] == unrelated
    assert result.candidate_state.serial == state.serial + 1


def test_observed_unchanged_ownership_does_not_increment_serial() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    target = _action("topic", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                artifact,
                physical_name=artifact.name,
                backend="direct-kafka",
            )
        }
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=artifact.name,
                action="none",
                current=TopicState(
                    name=artifact.name,
                    exists=True,
                    partitions=3,
                    replication_factor=1,
                    config={},
                ),
                desired=artifact,
            )
        ]
    )

    result = _observe(state, plan, (target,))

    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state == state


@pytest.mark.parametrize("kind", ["topic", "schema"])
def test_observed_supports_exact_adoption_candidate(kind: str) -> None:
    state = _state()
    if kind == "topic":
        topic_artifact = TopicArtifact(
            name="legacy.v1",
            partitions=1,
            replication_factor=1,
            ownership=_ownership("legacy", mode="adopted"),
        )
        topic_change = TopicChange(
            topic=topic_artifact.name,
            action="none",
            current=TopicState(
                name=topic_artifact.name,
                exists=True,
                partitions=1,
                replication_factor=1,
                config={},
            ),
            desired=topic_artifact,
        )
        plan = DeploymentPlan(topic_changes=[topic_change])
    else:
        schema_artifact = SchemaArtifact(
            subject="legacy-value",
            schema={"type": "record", "name": "Legacy", "fields": []},
            ownership=_ownership("legacy", mode="adopted", owner_type="source"),
        )
        schema_change = SchemaChange(
            subject=schema_artifact.subject,
            action="none",
            current=SchemaState(
                subject=schema_artifact.subject,
                exists=True,
                version=1,
                schema_id=1,
                schema=schema_artifact.schema,
                schema_type="AVRO",
            ),
            desired=schema_artifact,
        )
        plan = DeploymentPlan(schema_changes=[schema_change])
    target = _action(kind, "legacy", "adopt")
    plan.ownership_requirements.append(
        OwnershipRequirement(
            resource_id=target.resource_id,
            kind=kind,
            logical_name="legacy",
            physical_name="not-persisted-in-evidence",
            reason="requires_adoption",
            observed_action="none",
            ownership_mode="adopted",
            message="review required",
        )
    )

    result = _observe(state, plan, (target,), intent_kind="adopt")

    assert result.targets[0].accepted_as == "candidate"
    assert result.candidate_state is not None
    assert result.candidate_state.resources[target.resource_id].ownership == "adopted"


def test_observer_performs_no_deployer_calls() -> None:
    class ExplodingDeployer:
        def __getattribute__(self, name: str) -> object:
            if name.startswith("__"):
                return object.__getattribute__(self, name)
            raise AssertionError("observer made a hidden deployer call")

    state = _state()
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=artifact.name,
                action="none",
                current=TopicState(
                    name=artifact.name,
                    exists=True,
                    partitions=3,
                    replication_factor=1,
                    config={},
                ),
                desired=artifact,
            )
        ]
    )
    planner = _planner(state, kafka_deployer=ExplodingDeployer())

    _observe(state, plan, (_action("topic", "orders", "create"),), planner=planner)


@pytest.mark.parametrize(
    ("plan", "target"),
    [
        (
            DeploymentPlan(
                schema_changes=[
                    SchemaChange(
                        subject="orders-value",
                        action="none",
                        current=SchemaState(
                            subject="orders-value",
                            exists=True,
                            version=None,
                            schema_id=1,
                            schema={},
                            schema_type="AVRO",
                        ),
                        desired=SchemaArtifact(
                            subject="orders-value",
                            schema={},
                            ownership=_ownership("orders"),
                        ),
                    )
                ]
            ),
            _action("schema", "orders", "register"),
        ),
        (
            DeploymentPlan(
                topic_changes=[
                    TopicChange(
                        topic="orders.v1",
                        action="none",
                        current=TopicState(
                            name="orders.v1",
                            exists=True,
                            partitions=3,
                            replication_factor=None,
                            config={},
                        ),
                        desired=TopicArtifact(
                            name="orders.v1",
                            partitions=3,
                            replication_factor=1,
                            ownership=_ownership("orders"),
                        ),
                    )
                ]
            ),
            _action("topic", "orders", "create"),
        ),
        (
            DeploymentPlan(
                connector_changes=[
                    ConnectorChange(
                        connector_name="orders-sink",
                        action="none",
                        current=ConnectorState(
                            name="orders-sink",
                            exists=True,
                            config={},
                            status=None,
                        ),
                        desired=ConnectorArtifact(
                            name="orders-sink",
                            connector_class="example.Sink",
                            topics=["orders.v1"],
                            ownership=_ownership("orders"),
                        ),
                    )
                ]
            ),
            _action("connector", "orders", "create"),
        ),
        (
            DeploymentPlan(
                gateway_changes=[
                    GatewayRuleChange(
                        name="orders",
                        action="none",
                        current_alias=AliasTopicState(
                            name="orders.public",
                            exists=True,
                            physical_topic="orders.v1",
                        ),
                        current_interceptors=None,
                        desired=GatewayRuleArtifact(
                            name="orders",
                            virtual_topic="orders.public",
                            physical_topic="orders.v1",
                            ownership=_ownership("orders"),
                        ),
                    )
                ]
            ),
            _action("gateway_rule", "orders", "create"),
        ),
    ],
)
def test_partial_observations_fail_closed(
    plan: DeploymentPlan,
    target: OperationAction,
) -> None:
    with pytest.raises(RecoveryObservationError, match="partial"):
        _observe(_state(), plan, (target,))


def test_present_flink_status_only_observation_fails_closed() -> None:
    artifact = FlinkJobArtifact(
        name="orders_job",
        sql="INSERT INTO sink SELECT * FROM source",
        ownership=_ownership("orders"),
    )
    plan = DeploymentPlan(
        flink_changes=[
            FlinkJobChange(
                job_name=artifact.name,
                action="none",
                current=FlinkJobState(
                    name=artifact.name,
                    exists=True,
                    job_id="opaque-provider-id",
                    status="RUNNING",
                ),
                desired=artifact,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="cannot prove managed artifact"):
        _observe(_state(), plan, (_action("flink_job", "orders", "submit"),))


def test_missing_duplicate_mismatched_and_stale_actions_fail_closed() -> None:
    artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    change = TopicChange(
        topic=artifact.name,
        action="none",
        current=TopicState(
            name=artifact.name,
            exists=True,
            partitions=3,
            replication_factor=1,
            config={},
        ),
        desired=artifact,
    )
    target = _action("topic", "orders", "create")
    state = _state()

    with pytest.raises(RecoveryObservationError, match="no matching fresh observation"):
        _observe(state, DeploymentPlan(), (target,))

    with pytest.raises(RecoveryObservationError, match="duplicate canonical"):
        _observe(state, DeploymentPlan(topic_changes=[change, change]), (target,))

    bad_action = _action("topic", "orders", "register")
    with pytest.raises(RecoveryObservationError, match="incompatible"):
        _observe(state, DeploymentPlan(topic_changes=[change]), (bad_action,))

    stale = _planner(_state(serial=state.serial + 1))
    with pytest.raises(RecoveryObservationError, match="not built against"):
        _observe(state, DeploymentPlan(topic_changes=[change]), (target,), planner=stale)

    assert change.current is not None
    mismatched = replace(change, current=replace(change.current, name="different.v1"))
    with pytest.raises(RecoveryObservationError, match="mismatched identity"):
        _observe(state, DeploymentPlan(topic_changes=[mismatched]), (target,))


def test_unsupported_resource_kind_fails_closed() -> None:
    target = _action("cluster", "primary", "update")

    with pytest.raises(RecoveryObservationError, match="unsupported resource kind"):
        _observe(_state(), DeploymentPlan(), (target,))


def test_duplicate_blocked_action_is_rejected() -> None:
    target = _action("topic", "orders", "create")
    duplicate = replace(target, index=1)

    with pytest.raises(RecoveryObservationError, match="duplicated in the blocked intent"):
        _observe(_state(), DeploymentPlan(), (target, duplicate))


def test_inconsistent_none_action_does_not_override_exact_prior_evidence() -> None:
    prior_artifact = TopicArtifact(
        name="orders.v1",
        partitions=3,
        replication_factor=1,
        ownership=_ownership("orders"),
    )
    desired = replace(prior_artifact, partitions=6)
    target = _action("topic", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                prior_artifact,
                physical_name=prior_artifact.name,
                backend="direct-kafka",
            )
        }
    )
    # This internally-inconsistent fake plan claims no change while retaining old
    # provider state and a different desired record.
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=prior_artifact.name,
                action="none",
                current=TopicState(
                    name=prior_artifact.name,
                    exists=True,
                    partitions=3,
                    replication_factor=1,
                    config={},
                ),
                desired=desired,
            )
        ]
    )

    result = _observe(state, plan, (target,))

    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state == state


def test_errors_and_evidence_do_not_expose_live_secrets_or_physical_names() -> None:
    secret = "super-secret-value"
    physical = "provider-physical-orders"
    artifact = TopicArtifact(
        name=physical,
        partitions=6,
        replication_factor=1,
        config={},
        ownership=_ownership("orders"),
    )
    target = _action("topic", "orders", "update")
    plan = DeploymentPlan(
        topic_changes=[
            TopicChange(
                topic=physical,
                action="update",
                current=TopicState(
                    name=physical,
                    exists=True,
                    partitions=4,
                    replication_factor=1,
                    config={"password": secret, "endpoint": "https://user:pw@example.test"},
                ),
                desired=artifact,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError) as caught:
        _observe(_state(), plan, (target,))

    rendered = str(caught.value)
    assert secret not in rendered
    assert physical not in rendered
    assert "user:pw" not in rendered
    assert target.resource_id in rendered


def test_fingerprints_are_deterministic_for_mapping_and_collection_order() -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        config={"a": "1", "b": "2"},
        ownership=_ownership("orders"),
    )
    target = _action("connector", "orders", "create")

    def plan(config: dict[str, str], tasks: list[dict[str, object]]) -> DeploymentPlan:
        return DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=artifact.name,
                    action="none",
                    current=ConnectorState(
                        name=artifact.name,
                        exists=True,
                        config=config,
                        status="RUNNING",
                        tasks=tasks,
                    ),
                    desired=artifact,
                )
            ]
        )

    first = plan(
        {
            "name": artifact.name,
            "connector.class": artifact.connector_class,
            "topics": "orders.v1",
            "a": "1",
            "b": "2",
        },
        [{"id": 1}, {"id": 0}],
    )
    second = plan(
        {
            "b": "2",
            "a": "1",
            "topics": "orders.v1",
            "connector.class": artifact.connector_class,
            "name": artifact.name,
        },
        [{"id": 0}, {"id": 1}],
    )

    first_result = _observe(_state(), first, (target,))
    second_result = _observe(_state(), second, (target,))

    assert first_result.targets[0].fingerprint == second_result.targets[0].fingerprint


def test_abandoned_resolution_rejects_observer_invocation() -> None:
    with pytest.raises(RecoveryObservationError, match="must not observe"):
        _observe(
            _state(),
            DeploymentPlan(),
            (),
            resolution="abandoned_before_mutation",
        )


def test_gateway_duplicate_interceptor_observation_is_partial() -> None:
    artifact = GatewayRuleArtifact(
        name="orders",
        virtual_topic="orders.public",
        physical_topic="orders.v1",
        ownership=_ownership("orders"),
    )
    interceptor = InterceptorState(
        name="orders_filter_0",
        exists=True,
        plugin_class="example.Filter",
        config={},
        scope={},
    )
    plan = DeploymentPlan(
        gateway_changes=[
            GatewayRuleChange(
                name=artifact.name,
                action="none",
                current_alias=AliasTopicState(
                    name=artifact.virtual_topic,
                    exists=True,
                    physical_topic=artifact.physical_topic,
                ),
                current_interceptors=[interceptor, interceptor],
                desired=artifact,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="partial"):
        _observe(_state(), plan, (_action("gateway_rule", "orders", "create"),))
