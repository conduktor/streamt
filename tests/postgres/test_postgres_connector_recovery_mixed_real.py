"""Real PostgreSQL mixed-action recovery gate for Connector deletions."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest

from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_service import (
    RecoveryLiveObservation,
    RecoveryProjectContext,
    RecoveryService,
)
from streamt.deployer.state import (
    LocalState,
    ManagedConnectorResourceDeletion,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    DeploymentStateService,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    RecoveryRecord,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROJECT = "connector-mixed-recovery"
_ENVIRONMENT = "prod"
_NAMESPACE = "platform"
_CONNECT_ENDPOINT = "https://connect.example.test:8443/api"
_ENVIRONMENT_FINGERPRINT = "sha256:" + "1" * 64
_MANIFEST_CHECKSUM = "sha256:" + "2" * 64


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _binding() -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint("primary", _CONNECT_ENDPOINT)


def _connector(owner: str, name: str) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=name,
        connector_class="com.example.ArchiveSink",
        topics=[f"{owner}.events.v1"],
        cluster="primary",
        config={"tasks.max": 1},
        ownership=ArtifactOwnership(
            project=_PROJECT,
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
        binding=_binding(),
        name=artifact.name,
        exists=True,
        config=config,
    )


def _connector_action(
    index: int,
    owner: str,
    artifact: ConnectorArtifact,
) -> OperationAction:
    current = _present(artifact)
    backend = _binding().backend_identity
    return OperationAction(
        index=index,
        resource_id=resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "connector",
            owner,
        ),
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=backend,
            connector_name=artifact.name,
            prior_artifact_checksum=artifact_checksum(artifact.to_dict()),
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint=current.fingerprint,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=managed_connector_absence_fingerprint(
                    backend,
                    artifact.name,
                ),
            ),
        ),
    )


def _record(artifact: ConnectorArtifact) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=artifact.name,
        ownership="managed",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend=_binding().backend_identity,
    )


def _initialize_service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> tuple[str, DeploymentStateService]:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    migrated = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer.role,
    )
    assert migrated.migrated is True
    return initialized.store_id, DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
            require_v2_writer=True,
        ),
        address=_address(),
    )


@dataclass
class _ExactObserver:
    expected_snapshot: RecoverySnapshotEvidence
    observation: RecoveryLiveObservation
    calls: int = 0

    def observe_recovery_targets(
        self,
        *,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation:
        self.calls += 1
        assert resolution == "observed"
        assert snapshot == self.expected_snapshot
        return self.observation


@dataclass
class _ExactContext:
    calls: int = 0

    def read_recovery_context(self) -> RecoveryProjectContext:
        self.calls += 1
        return RecoveryProjectContext(
            environment_fingerprint=_ENVIRONMENT_FINGERPRINT,
            manifest_checksum=_MANIFEST_CHECKSUM,
        )


def test_mixed_connector_recovery_removes_only_completed_absent_target(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
) -> None:
    """Recover a mixed intent while preserving failed and not-started records."""
    store_id, state_service = _initialize_service(postgres_case, postgres_writer)
    first = _connector("first_archive", "first-archive-sink")
    failed = _connector("failed_archive", "failed-archive-sink")
    not_started = _connector("pending_archive", "pending-archive-sink")
    first_action = _connector_action(0, "first_archive", first)
    topic_action = OperationAction(
        index=1,
        resource_id=resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "topic",
            "audit_log",
        ),
        action="update",
    )
    failed_action = _connector_action(2, "failed_archive", failed)
    not_started_action = _connector_action(3, "pending_archive", not_started)
    actions = (
        first_action,
        topic_action,
        failed_action,
        not_started_action,
    )

    topic_record = ManagedResourceRecord(
        physical_name="audit.events.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
        backend="direct-kafka",
    )
    unrelated_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "schema",
        "payments_value",
    )
    unrelated_record = ManagedResourceRecord(
        physical_name="payments-value",
        ownership="managed",
        artifact_checksum=artifact_checksum({"subject": "payments-value"}),
        backend="schema-registry",
    )
    initial_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            first_action.resource_id: _record(first),
            topic_action.resource_id: topic_record,
            failed_action.resource_id: _record(failed),
            not_started_action.resource_id: _record(not_started),
            unrelated_id: unrelated_record,
        },
    )

    # Seed canonical ownership through the production operation writer.
    with state_service.operation() as operation:
        empty = operation.observe()
        seed_intent = OperationIntent(
            operation_id=str(uuid.uuid4()),
            kind="adopt",
            started_at=operation_timestamp(),
            actor="postgres-mixed-recovery-seed",
            prior_state_serial=empty.state.state_serial,
            prior_state_checksum=state_checksum(empty.state.state),
            reviewed_plan_checksum=None,
            actions=(),
        )
        seeded = operation.begin_operation(empty, seed_intent)
        operation.commit_operation(seeded, initial_state)

    # The first Connector and intervening topic completed. The second Connector
    # failed after its durable start; the final Connector was never started.
    blocked_operation_id = str(uuid.uuid4())
    with state_service.operation() as operation:
        observed = operation.observe()
        intent = OperationIntent(
            operation_id=blocked_operation_id,
            kind="apply",
            started_at=operation_timestamp(),
            actor="postgres-mixed-recovery-test",
            prior_state_serial=observed.state.state_serial,
            prior_state_checksum=state_checksum(observed.state.state),
            reviewed_plan_checksum="sha256:" + "3" * 64,
            actions=actions,
        )
        current = operation.begin_operation(observed, intent)
        for action, succeeded in (
            (first_action, True),
            (topic_action, True),
            (failed_action, False),
        ):
            current = operation.record_progress(
                current,
                OperationProgress(
                    operation_id=blocked_operation_id,
                    action_index=action.index,
                    resource_id=action.resource_id,
                    action=action.action,
                    status="started",
                    succeeded=None,
                    recorded_at=operation_timestamp(),
                ),
            )
            current = operation.record_progress(
                current,
                OperationProgress(
                    operation_id=blocked_operation_id,
                    action_index=action.index,
                    resource_id=action.resource_id,
                    action=action.action,
                    status="completed",
                    succeeded=succeeded,
                    recorded_at=operation_timestamp(),
                ),
            )
        blocked = operation.mark_recovery_required(
            current,
            RecoveryRecord(
                operation_id=blocked_operation_id,
                failure_code="connector_removal_drift",
                failed_at=operation_timestamp(),
                last_completed_action_index=topic_action.index,
            ),
        )

    snapshot = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    candidate_resources = dict(initial_state.resources)
    del candidate_resources[first_action.resource_id]
    candidate_state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=initial_state.serial + 1,
        resources=candidate_resources,
    )
    first_evidence = first_action.connector_evidence
    failed_evidence = failed_action.connector_evidence
    pending_evidence = not_started_action.connector_evidence
    assert first_evidence is not None
    assert failed_evidence is not None
    assert pending_evidence is not None
    targets = (
        RecoveryTargetEvidence(
            action=first_action,
            presence="absent",
            accepted_as="candidate",
            fingerprint=first_evidence.desired.fingerprint,
        ),
        RecoveryTargetEvidence(
            action=topic_action,
            presence="present",
            accepted_as="prior",
            fingerprint=topic_record.artifact_checksum,
        ),
        RecoveryTargetEvidence(
            action=failed_action,
            presence="present",
            accepted_as="prior",
            fingerprint=failed_evidence.current.fingerprint,
        ),
        RecoveryTargetEvidence(
            action=not_started_action,
            presence="present",
            accepted_as="prior",
            fingerprint=pending_evidence.current.fingerprint,
        ),
    )
    live = RecoveryLiveObservation(
        targets=targets,
        candidate_state=candidate_state,
    )
    observer = _ExactObserver(snapshot, live)
    context = _ExactContext()
    recovery = RecoveryService(
        state=state_service,
        resolved_at_factory=lambda: "2026-09-03T12:00:00Z",
    )
    plan_path = tmp_path / "mixed-connector.recovery.json"

    plan = recovery.create_plan(
        resolution="observed",
        destination=plan_path,
        observer=observer,
        context_reader=context,
    )

    assert plan.snapshot.store.backend == "postgres"
    assert plan.snapshot.store.store_id == store_id
    assert plan.targets == targets
    assert plan.candidate_state == candidate_state
    assert plan.managed_connector_deletions() == (
        ManagedConnectorResourceDeletion(
            resource_id=first_action.resource_id,
            backend_identity=_binding().backend_identity,
            connector_name=first.name,
            prior_artifact_checksum=artifact_checksum(first.to_dict()),
        ),
    )
    result = recovery.execute_plan(
        plan_path,
        confirm_operation_id=plan.blocked_operation_id,
        confirm_resolution=plan.resolution,
        confirm_evidence_checksum=plan.evidence_checksum,
        observer=observer,
        context_reader=context,
    )

    assert observer.calls == 2
    assert context.calls == 4
    assert result.state.state == candidate_state
    assert result.control.control == OperationControlState.clear(_address())
    assert state_service.read().state == candidate_state
    assert failed_action.resource_id in result.state.state.resources
    assert not_started_action.resource_id in result.state.state.resources
    assert result.state.state.resources[failed_action.resource_id] == _record(failed)
    assert result.state.state.resources[not_started_action.resource_id] == _record(not_started)
    assert result.state.state.resources[topic_action.resource_id] == topic_record
    assert result.state.state.resources[unrelated_id] == unrelated_record

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        operation_history = list(
            connection.execute(
                postgres_case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind FROM {}.{} "
                    "ORDER BY recorded_at, event_index"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("operation_history"),
                )
            ).fetchall()
        )
        state_history = list(
            connection.execute(
                postgres_case.sql.SQL(
                    "SELECT state_serial, state_checksum, state_json FROM {}.{} ORDER BY revision"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("state_history"),
                )
            ).fetchall()
        )

    blocked_events = [row for row in operation_history if row[0] == blocked_operation_id]
    assert [(row[1], row[2]) for row in blocked_events] == [
        (0, "intent"),
        (1, "progress_started"),
        (2, "progress_completed"),
        (3, "progress_started"),
        (4, "progress_completed"),
        (5, "progress_started"),
        (6, "progress_completed"),
        (7, "recovery_required"),
    ]
    recovery_events = [row for row in operation_history if row[0] == plan.recovery_operation_id]
    assert [(row[1], row[2]) for row in recovery_events] == [
        (0, "recovery_intent"),
        (1, "recovered_observed"),
    ]
    assert [(row[0], row[1]) for row in state_history] == [
        (initial_state.serial, state_checksum(initial_state)),
        (candidate_state.serial, state_checksum(candidate_state)),
    ]
    assert json.loads(state_history[0][2]) == initial_state.to_dict()
    assert json.loads(state_history[1][2]) == candidate_state.to_dict()
