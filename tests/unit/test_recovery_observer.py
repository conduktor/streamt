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
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectorChange,
    ConnectorState,
    ManagedConnectorObservation,
)
from streamt.deployer.flink import FlinkJobChange, FlinkJobState
from streamt.deployer.gateway import (
    AliasTopicState,
    GatewayBackendBinding,
    GatewayRuleChange,
    InterceptorState,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    plan_managed_gateway_rule,
    plan_managed_gateway_rule_deletion,
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
CONNECT_BINDING = ConnectClusterBinding.from_endpoint(
    "production",
    "https://connect.example.test/api",
)
CONNECT_BACKEND = CONNECT_BINDING.backend_identity
GATEWAY_BINDING = GatewayBackendBinding.from_endpoint(
    "https://gateway.example.test",
    virtual_cluster="payments-prod",
)
GATEWAY_ENDPOINT = "https://gateway.example.test"
GATEWAY_SECRET = "gateway-recovery-secret-7219"


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


def _connector_observation(
    artifact: ConnectorArtifact,
    *,
    binding: ConnectClusterBinding = CONNECT_BINDING,
    exists: bool = True,
    config: dict[str, object] | None = None,
) -> ManagedConnectorObservation:
    raw_config = artifact.to_dict()["config"] if config is None else config
    assert isinstance(raw_config, dict)
    return ManagedConnectorObservation(
        binding=binding,
        name=artifact.name,
        exists=exists,
        config=tuple(sorted(raw_config.items())) if exists else (),
    )


def _gateway_artifact(
    *,
    physical_topic: str = "orders.v1",
    where: str | None = None,
) -> GatewayRuleArtifact:
    interceptors: list[dict[str, object]] = []
    if where is not None:
        interceptors.append({"type": "filter", "config": {"where": where}})
    return GatewayRuleArtifact(
        name="orders_rule",
        virtual_topic="orders.public",
        physical_topic=physical_topic,
        interceptors=interceptors,
        ownership=_ownership("orders_owner"),
    )


def _gateway_absent(
    desired: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    return ManagedGatewayRuleObservation(
        binding=desired.binding,
        logical_name=desired.logical_name,
        alias_name=desired.alias_name,
        exists=False,
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
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders_sink"),
    )
    yield (
        _action("connector", "orders_sink", "create"),
        DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=connector.name,
                    action="none",
                    current=_connector_observation(connector),
                    desired=connector,
                    backend_identity=CONNECT_BACKEND,
                )
            ]
        ),
        CONNECT_BACKEND,
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


def test_normalized_gateway_applied_create_is_accepted_as_candidate() -> None:
    artifact = _gateway_artifact(
        where=f"customer_token = '{GATEWAY_SECRET}'",
    )
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    target = _action("gateway_rule", "orders_owner", "create")
    plan = DeploymentPlan(
        gateway_changes=[plan_managed_gateway_rule(artifact, desired, desired)]
    )
    state = _state()

    result = _observe(state, plan, (target,))

    assert result.targets[0].presence == "present"
    assert result.targets[0].accepted_as == "candidate"
    assert result.targets[0].fingerprint.startswith("sha256:")
    assert result.candidate_state is not None
    assert result.candidate_state.serial == state.serial + 1
    record = result.candidate_state.resources[target.resource_id]
    assert record.physical_name == artifact.virtual_topic
    assert record.backend == GATEWAY_BINDING.backend_identity
    rendered = repr(plan) + repr(result) + str(result.targets[0].to_dict())
    assert GATEWAY_SECRET not in rendered
    assert GATEWAY_ENDPOINT not in rendered
    assert "config_json" not in rendered


def test_normalized_gateway_applied_update_is_accepted_as_candidate() -> None:
    prior = _gateway_artifact(physical_topic="orders.v1")
    desired_artifact = _gateway_artifact(
        physical_topic="orders.v2",
        where="region = 'US'",
    )
    desired = build_desired_gateway_rule(desired_artifact, GATEWAY_BINDING)
    target = _action("gateway_rule", "orders_owner", "update")
    state = _state(
        {
            target.resource_id: _record(
                prior,
                physical_name=prior.virtual_topic,
                backend=GATEWAY_BINDING.backend_identity,
            )
        }
    )
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(desired_artifact, desired, desired)
        ]
    )

    result = _observe(state, plan, (target,))

    assert result.targets[0].accepted_as == "candidate"
    assert result.candidate_state is not None
    assert result.candidate_state.serial == state.serial + 1
    record = result.candidate_state.resources[target.resource_id]
    assert record.physical_name == desired_artifact.virtual_topic
    assert record.backend == GATEWAY_BINDING.backend_identity
    assert record.artifact_checksum == artifact_checksum(desired_artifact.to_dict())


def test_normalized_gateway_unchanged_create_absence_is_prior() -> None:
    artifact = _gateway_artifact()
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    current = _gateway_absent(desired)
    plan = DeploymentPlan(
        gateway_changes=[plan_managed_gateway_rule(artifact, desired, current)]
    )
    target = _action("gateway_rule", "orders_owner", "create")
    state = _state()

    result = _observe(state, plan, (target,))

    assert result.targets[0].presence == "absent"
    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state == state


def test_normalized_gateway_rolled_back_create_absence_is_prior() -> None:
    artifact = _gateway_artifact()
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(artifact, desired, _gateway_absent(desired))
        ]
    )
    target = _action("gateway_rule", "orders_owner", "create")

    result = _observe(_state(), plan, (target,), resolution="rolled_back")

    assert result.targets[0].presence == "absent"
    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state is None


def test_normalized_gateway_rolled_back_update_fails_without_prior_surface() -> None:
    prior = _gateway_artifact(physical_topic="orders.v1")
    desired_artifact = _gateway_artifact(physical_topic="orders.v2")
    current = build_desired_gateway_rule(prior, GATEWAY_BINDING)
    desired = build_desired_gateway_rule(desired_artifact, GATEWAY_BINDING)
    target = _action("gateway_rule", "orders_owner", "update")
    state = _state(
        {
            target.resource_id: _record(
                prior,
                physical_name=prior.virtual_topic,
                backend=GATEWAY_BINDING.backend_identity,
            )
        }
    )
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(desired_artifact, desired, current)
        ]
    )

    with pytest.raises(
        RecoveryObservationError,
        match="does not exactly match prior state",
    ) as error:
        _observe(state, plan, (target,), resolution="rolled_back")

    assert GATEWAY_SECRET not in str(error.value)
    assert GATEWAY_ENDPOINT not in str(error.value)


@pytest.mark.parametrize(
    ("corruption", "message"),
    [
        ("partial", "partial"),
        ("backend", "mismatched identity"),
        ("alias", "mismatched identity"),
        ("desired", "mismatched desired state"),
        ("current_type", "partial"),
    ],
)
def test_normalized_gateway_partial_or_mismatched_surface_fails_closed(
    corruption: str,
    message: str,
) -> None:
    artifact = _gateway_artifact(where="region = 'US'")
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    change = plan_managed_gateway_rule(artifact, desired, desired)
    if corruption == "partial":
        object.__setattr__(change, "current", None)
    elif corruption == "backend":
        other_binding = GatewayBackendBinding.from_endpoint(
            "https://other-gateway.example.test",
            virtual_cluster=GATEWAY_BINDING.virtual_cluster,
        )
        object.__setattr__(change, "backend_identity", other_binding.backend_identity)
    elif corruption == "alias":
        other_alias = ManagedGatewayRuleObservation(
            binding=GATEWAY_BINDING,
            logical_name=artifact.name,
            alias_name="orders.other",
            exists=True,
            physical_name=artifact.physical_topic,
            physical_cluster="main",
            interceptors=desired.interceptors,
        )
        object.__setattr__(change, "current", other_alias)
    elif corruption == "desired":
        other_artifact = _gateway_artifact(physical_topic="orders.v2")
        object.__setattr__(
            change,
            "desired_managed",
            build_desired_gateway_rule(other_artifact, GATEWAY_BINDING),
        )
    else:
        object.__setattr__(
            change,
            "current",
            AliasTopicState(
                name=artifact.virtual_topic,
                exists=True,
                physical_topic=artifact.physical_topic,
            ),
        )
    plan = DeploymentPlan(gateway_changes=[change])

    with pytest.raises(RecoveryObservationError, match=message):
        _observe(
            _state(),
            plan,
            (_action("gateway_rule", "orders_owner", "create"),),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "current_alias",
            AliasTopicState(
                name="orders.public",
                exists=True,
                physical_topic="orders.v1",
            ),
        ),
        ("current_interceptors", []),
    ],
)
def test_normalized_gateway_rejects_injected_legacy_evidence(
    field: str,
    value: object,
) -> None:
    artifact = _gateway_artifact(
        where=f"customer_token = '{GATEWAY_SECRET}'",
    )
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    change = plan_managed_gateway_rule(artifact, desired, desired)
    object.__setattr__(change, field, value)
    plan = DeploymentPlan(gateway_changes=[change])

    with pytest.raises(
        RecoveryObservationError,
        match="contains legacy evidence",
    ) as error:
        _observe(
            _state(),
            plan,
            (_action("gateway_rule", "orders_owner", "create"),),
        )

    assert GATEWAY_SECRET not in str(error.value)
    assert GATEWAY_ENDPOINT not in str(error.value)


def test_normalized_gateway_drifted_fingerprint_matches_neither_state() -> None:
    desired_artifact = _gateway_artifact(
        where=f"customer_token = '{GATEWAY_SECRET}'",
    )
    observed_artifact = _gateway_artifact(where="region = 'EU'")
    desired = build_desired_gateway_rule(desired_artifact, GATEWAY_BINDING)
    observed = build_desired_gateway_rule(observed_artifact, GATEWAY_BINDING)
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(desired_artifact, desired, observed)
        ]
    )

    with pytest.raises(
        RecoveryObservationError,
        match="matches neither prior nor candidate state",
    ) as error:
        _observe(
            _state(),
            plan,
            (_action("gateway_rule", "orders_owner", "create"),),
        )

    assert GATEWAY_SECRET not in str(error.value)
    assert GATEWAY_ENDPOINT not in str(error.value)


def test_normalized_gateway_rejects_legacy_prior_backend() -> None:
    prior = _gateway_artifact(physical_topic="orders.v1")
    desired_artifact = _gateway_artifact(physical_topic="orders.v2")
    desired = build_desired_gateway_rule(desired_artifact, GATEWAY_BINDING)
    target = _action("gateway_rule", "orders_owner", "update")
    state = _state(
        {
            target.resource_id: _record(
                prior,
                physical_name=prior.virtual_topic,
                backend="conduktor-gateway",
            )
        }
    )
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(desired_artifact, desired, desired)
        ]
    )

    with pytest.raises(RecoveryObservationError, match="legacy or mismatched"):
        _observe(state, plan, (target,))


def test_normalized_gateway_prior_lookup_never_falls_back_to_logical_name() -> None:
    artifact = _gateway_artifact()
    desired = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    target = _action("gateway_rule", "orders_owner", "create")
    state = _state(
        {
            target.resource_id: _record(
                artifact,
                physical_name=artifact.name,
                backend=GATEWAY_BINDING.backend_identity,
            )
        }
    )
    plan = DeploymentPlan(
        gateway_changes=[
            plan_managed_gateway_rule(artifact, desired, _gateway_absent(desired))
        ]
    )

    with pytest.raises(
        RecoveryObservationError,
        match="mismatched prior alias evidence",
    ):
        _observe(state, plan, (target,), resolution="rolled_back")


def test_normalized_gateway_does_not_invent_delete_recovery() -> None:
    artifact = _gateway_artifact()
    current = build_desired_gateway_rule(artifact, GATEWAY_BINDING)
    plan = DeploymentPlan(
        gateway_changes=[plan_managed_gateway_rule_deletion(current)]
    )

    with pytest.raises(RecoveryObservationError, match="partial"):
        _observe(
            _state(),
            plan,
            (_action("gateway_rule", "orders_owner", "delete"),),
        )


def test_connector_observation_can_prove_exact_prior_artifact() -> None:
    prior = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        config={"batch.size": "100"},
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    desired = replace(prior, config={"batch.size": "200"})
    target = _action("connector", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                prior,
                physical_name=prior.name,
                backend=CONNECT_BACKEND,
            )
        }
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=prior.name,
                action="update",
                current=_connector_observation(prior),
                desired=desired,
                backend_identity=CONNECT_BACKEND,
            )
        ]
    )

    result = _observe(state, plan, (target,))

    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state == state


def test_connector_exact_absence_proves_rolled_back_create() -> None:
    desired = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    target = _action("connector", "orders", "create")
    state = _state()
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=desired.name,
                action="create",
                current=_connector_observation(desired, exists=False),
                desired=desired,
                backend_identity=CONNECT_BACKEND,
            )
        ]
    )

    result = _observe(state, plan, (target,), resolution="rolled_back")

    assert result.targets[0].presence == "absent"
    assert result.targets[0].accepted_as == "prior"
    assert result.candidate_state is None


@pytest.mark.parametrize(
    "drifted_binding",
    [
        ConnectClusterBinding.from_endpoint(
            "production",
            "https://other-connect.example.test/api",
        ),
        ConnectClusterBinding.from_endpoint(
            "disaster-recovery",
            "https://connect.example.test/api",
        ),
    ],
    ids=["endpoint", "alias"],
)
def test_connector_backend_binding_drift_fails_closed(
    drifted_binding: ConnectClusterBinding,
) -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="none",
                current=_connector_observation(artifact, binding=drifted_binding),
                desired=artifact,
                backend_identity=CONNECT_BACKEND,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="mismatched backend identity"):
        _observe(_state(), plan, (_action("connector", "orders", "create"),))


def test_legacy_generic_connector_backend_never_upgrades_ownership_implicitly() -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    target = _action("connector", "orders", "update")
    state = _state(
        {
            target.resource_id: _record(
                artifact,
                physical_name=artifact.name,
                backend="kafka-connect",
            )
        }
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="none",
                current=_connector_observation(artifact),
                desired=artifact,
                backend_identity=CONNECT_BACKEND,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="legacy or mismatched"):
        _observe(state, plan, (target,))


def test_legacy_connector_state_is_partial_even_with_bound_change() -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="none",
                current=ConnectorState(
                    name=artifact.name,
                    exists=True,
                    config=artifact.to_dict()["config"],  # type: ignore[arg-type]
                    status="RUNNING",
                    tasks=[],
                ),
                desired=artifact,
                backend_identity=CONNECT_BACKEND,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError, match="partial"):
        _observe(_state(), plan, (_action("connector", "orders", "create"),))


def test_unbound_connector_change_fails_closed() -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="none",
                current=_connector_observation(artifact),
                desired=artifact,
                backend_identity=None,
            )
        ]
    )

    with pytest.raises(RecoveryObservationError):
        _observe(_state(), plan, (_action("connector", "orders", "create"),))


@pytest.mark.parametrize(
    ("kind", "change", "verb"),
    [
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
                current=ManagedConnectorObservation(
                    binding=CONNECT_BINDING,
                    name="gone_sink",
                    exists=False,
                ),
                backend_identity=CONNECT_BACKEND,
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
    ],
)
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
        "connector": CONNECT_BACKEND,
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
    assert (
        result.candidate_state.resources[topic_action.resource_id]
        == state.resources[topic_action.resource_id]
    )
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


def test_observed_supports_exact_connector_adoption_candidate() -> None:
    artifact = ConnectorArtifact(
        name="legacy-sink",
        connector_class="example.Sink",
        topics=["legacy.v1"],
        config={"password": "candidate-secret"},
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("legacy", mode="adopted"),
    )
    target = _action("connector", "legacy", "adopt")
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=artifact.name,
                action="none",
                current=_connector_observation(artifact),
                desired=artifact,
                backend_identity=CONNECT_BACKEND,
            )
        ],
        ownership_requirements=[
            OwnershipRequirement(
                resource_id=target.resource_id,
                kind="connector",
                logical_name="legacy",
                physical_name=artifact.name,
                reason="requires_adoption",
                observed_action="none",
                ownership_mode="adopted",
                message="review required",
            )
        ],
    )

    result = _observe(_state(), plan, (target,), intent_kind="adopt")

    assert result.targets[0].accepted_as == "candidate"
    assert "candidate-secret" not in str(result.targets[0].to_dict())
    assert result.candidate_state is not None
    record = result.candidate_state.resources[target.resource_id]
    assert record.ownership == "adopted"
    assert record.backend == CONNECT_BACKEND


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
                            cluster=CONNECT_BINDING.cluster_alias,
                            ownership=_ownership("orders"),
                        ),
                        backend_identity=CONNECT_BACKEND,
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
    expected = (
        "ambiguous desired ownership evidence"
        if target.resource_id.endswith("/gateway_rule/orders")
        else "partial"
    )
    with pytest.raises(RecoveryObservationError, match=expected):
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


def test_connector_fingerprint_is_deterministic_secret_neutral_and_ignores_volatility() -> None:
    artifact = ConnectorArtifact(
        name="orders-sink",
        connector_class="example.Sink",
        topics=["orders.v1"],
        config={"a": "1", "b": "2", "password": "fingerprint-secret"},
        cluster=CONNECT_BINDING.cluster_alias,
        ownership=_ownership("orders"),
    )
    target = _action("connector", "orders", "create")

    def plan(
        config: dict[str, object],
        *,
        status: str,
        tasks: list[dict[str, object]],
    ) -> DeploymentPlan:
        observation = _connector_observation(artifact, config=config)
        object.__setattr__(observation, "status", status)
        object.__setattr__(observation, "tasks", tasks)
        return DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=artifact.name,
                    action="none",
                    current=observation,
                    desired=artifact,
                    backend_identity=CONNECT_BACKEND,
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
            "password": "fingerprint-secret",
        },
        status="RUNNING",
        tasks=[{"id": 1}, {"id": 0}],
    )
    second = plan(
        {
            "b": "2",
            "a": "1",
            "topics": "orders.v1",
            "connector.class": artifact.connector_class,
            "name": artifact.name,
            "password": "fingerprint-secret",
        },
        status="FAILED",
        tasks=[{"id": 0, "trace": "task-secret"}, {"id": 1}],
    )

    first_result = _observe(_state(), first, (target,))
    second_result = _observe(_state(), second, (target,))

    assert first_result.targets[0].fingerprint == second_result.targets[0].fingerprint
    rendered = str(first_result.targets[0].to_dict()) + str(second_result.targets[0].to_dict())
    assert "fingerprint-secret" not in rendered
    assert "task-secret" not in rendered


def test_abandoned_resolution_rejects_observer_invocation() -> None:
    with pytest.raises(RecoveryObservationError, match="must not observe"):
        _observe(
            _state(),
            DeploymentPlan(),
            (),
            resolution="abandoned_before_mutation",
        )


def test_legacy_gateway_duplicate_interceptor_observation_fails_closed() -> None:
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

    with pytest.raises(
        RecoveryObservationError,
        match="ambiguous desired ownership evidence",
    ):
        _observe(_state(), plan, (_action("gateway_rule", "orders", "create"),))
