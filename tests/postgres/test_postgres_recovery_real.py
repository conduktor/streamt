"""Real-server PostgreSQL v2 explicit-recovery conformance."""

from __future__ import annotations

import json
import multiprocessing
import uuid
from dataclasses import replace

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend_module
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
    _PsycopgBundle,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.recovery import (
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
)
from streamt.deployer.state import LocalState, ManagedResourceRecord, StateIdentityError
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_EVIDENCE_CHECKSUM = "sha256:" + "e" * 64


class _CommitAckLossConnection:
    def __init__(self, connection: object, *, commit_on_server: bool) -> None:
        self._connection = connection
        self._commit_on_server = commit_on_server

    def cursor(self) -> object:
        return self._connection.cursor()

    def commit(self) -> None:
        if self._commit_on_server:
            self._connection.commit()
        raise RuntimeError("injected recovery commit acknowledgement loss")

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class _FirstConnectionCommitAckLossDriver:
    def __init__(self, driver: object, *, commit_on_server: bool = True) -> None:
        self._driver = driver
        self._commit_on_server = commit_on_server
        self.connections = 0

    def connect(self, conninfo: str, **kwargs: object) -> object:
        connection = self._driver.connect(conninfo, **kwargs)
        self.connections += 1
        if self.connections == 1:
            return _CommitAckLossConnection(
                connection,
                commit_on_server=self._commit_on_server,
            )
        return connection


class _ResolutionInsertFailureCursor:
    def __init__(self, cursor: object) -> None:
        self._cursor = cursor

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        if (
            params is not None
            and len(params) >= 6
            and params[5]
            in {
                "recovered_observed",
                "recovered_rolled_back",
                "recovered_abandoned_before_mutation",
            }
        ):
            raise RuntimeError("injected recovery resolution insert failure")
        return self._cursor.execute(query, params)

    def fetchall(self) -> object:
        return self._cursor.fetchall()

    def close(self) -> None:
        self._cursor.close()


class _ResolutionInsertFailureConnection:
    def __init__(self, connection: object) -> None:
        self._connection = connection

    def cursor(self) -> _ResolutionInsertFailureCursor:
        return _ResolutionInsertFailureCursor(self._connection.cursor())

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


class _FirstConnectionResolutionInsertFailureDriver:
    def __init__(self, driver: object) -> None:
        self._driver = driver
        self.connections = 0

    def connect(self, conninfo: str, **kwargs: object) -> object:
        connection = self._driver.connect(conninfo, **kwargs)
        self.connections += 1
        if self.connections == 1:
            return _ResolutionInsertFailureConnection(connection)
        return connection


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _backend(case: object, writer: WriterIdentity) -> PrivatePostgresStateReadBackend:
    return PrivatePostgresStateReadBackend(
        dsn=writer.dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    )


def _initialize_v2(case: object, writer: WriterIdentity) -> str:
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
    return initialized.store_id


def _intent(snapshot: OperationSnapshot) -> OperationIntent:
    return OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="postgres-recovery-test",
        prior_state_serial=snapshot.state.state_serial,
        prior_state_checksum=state_checksum(snapshot.state.state),
        reviewed_plan_checksum=None,
        actions=(
            OperationAction(
                index=0,
                resource_id="streamt://payments/prod/kafka_topic/orders",
                action="create",
            ),
        ),
    )


def _block_operation(
    operation: object,
    snapshot: OperationSnapshot,
    *,
    started: bool,
) -> OperationSnapshot:
    intent = _intent(snapshot)
    current = operation.begin_operation(snapshot, intent)
    if not started:
        return current
    action = intent.actions[0]
    current = operation.record_progress(
        current,
        OperationProgress(
            operation_id=intent.operation_id,
            action_index=0,
            resource_id=action.resource_id,
            action=action.action,
            status="started",
            succeeded=None,
            recorded_at=operation_timestamp(),
        ),
    )
    return operation.mark_recovery_required(
        current,
        RecoveryRecord(
            operation_id=intent.operation_id,
            failure_code="runtime_outcome_unknown",
            failed_at=operation_timestamp(),
            last_completed_action_index=None,
        ),
    )


def _replacement() -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        serial=1,
        resources={
            "streamt://payments/prod/kafka_topic/orders": ManagedResourceRecord(
                physical_name="orders",
                ownership="managed",
                artifact_checksum="sha256:" + "a" * 64,
                backend="kafka",
            )
        },
    )


def _resolution(
    evidence: RecoverySnapshotEvidence,
    *,
    outcome: str,
    replacement: LocalState | None,
    operation_id: str | None = None,
    resolved_at: str | None = None,
) -> RecoveryResolutionRecord:
    result = replacement or evidence.state
    return RecoveryResolutionRecord(
        address=evidence.address,
        recovery_operation_id=operation_id or str(uuid.uuid4()),
        blocked_operation_id=evidence.blocked_operation_id,
        resolution=outcome,  # type: ignore[arg-type]
        resolved_at=resolved_at or operation_timestamp(),
        evidence_checksum=_EVIDENCE_CHECKSUM,
        prior_state_serial=evidence.state.serial,
        prior_state_checksum=evidence.state_checksum,
        result_state_serial=result.serial,
        result_state_checksum=state_checksum(result),
        state_changed=replacement is not None,
    )


def _rows(case: object, table: str, columns: str) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        return list(
            connection.execute(
                case.sql.SQL("SELECT " + columns + " FROM {}.{} ORDER BY 1, 2").format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier(table),
                )
            ).fetchall()
        )


def _assert_lock_released(case: object) -> None:
    address = _address()
    with case.psycopg.connect(case.owner_dsn, autocommit=True) as connection:
        key = connection.execute(
            case.sql.SQL(
                "SELECT advisory_lock_key FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("state_addresses"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()[0]
        assert connection.execute(
            "SELECT pg_catalog.pg_try_advisory_lock(%s)", (key,)
        ).fetchone() == (True,)
        assert connection.execute(
            "SELECT pg_catalog.pg_advisory_unlock(%s)", (key,)
        ).fetchone() == (True,)


@pytest.mark.parametrize(
    ("outcome", "started", "changed"),
    [
        ("observed", True, True),
        ("observed", True, False),
        ("rolled_back", True, False),
        ("abandoned_before_mutation", False, False),
    ],
)
def test_all_recovery_outcomes_are_atomic_audited_and_exactly_retryable(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    outcome: str,
    started: bool,
    changed: bool,
) -> None:
    store_id = _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=started)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    replacement = _replacement() if changed else None
    resolution = _resolution(evidence, outcome=outcome, replacement=replacement)

    with backend.operation(_address()) as operation:
        current = operation.observe()
        recovered = operation.finalize_recovery(
            current,
            evidence,
            resolution,
            replacement,
        )
    expected_state = replacement or evidence.state
    assert recovered.state.state == expected_state
    assert recovered.control.control.status == "clear"
    control_rows = _rows(
        postgres_case,
        "operation_control",
        "revision, status, control_json",
    )
    assert control_rows == [
        (
            4 if started else 2,
            "clear",
            json.dumps(
                OperationControlState.clear(_address()).to_dict(),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    ]
    current_rows = _rows(
        postgres_case,
        "current_state",
        "revision, state_serial, state_checksum, state_json",
    )
    if changed:
        assert current_rows == [
            (
                1,
                replacement.serial,
                state_checksum(replacement),
                json.dumps(
                    replacement.to_dict(),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        ]
    else:
        assert current_rows == []

    operation_history = _rows(
        postgres_case,
        "operation_history",
        "operation_id::text, event_index, event_kind, control_json",
    )
    recovery_events = [
        row for row in operation_history if row[0] == resolution.recovery_operation_id
    ]
    assert [(row[1], row[2]) for row in recovery_events] == [
        (0, "recovery_intent"),
        (1, f"recovered_{outcome}"),
    ]
    assert json.loads(recovery_events[0][3]) == evidence.to_dict()
    assert json.loads(recovery_events[1][3]) == resolution.to_dict()
    state_history = _rows(
        postgres_case,
        "state_history",
        "revision, state_serial, state_checksum, state_json, operation_id::text",
    )
    assert len(state_history) == (1 if changed else 0)
    if changed:
        assert state_history[0][0:3] == (
            1,
            replacement.serial,
            state_checksum(replacement),
        )
        assert state_history[0][4] == resolution.recovery_operation_id

    before_retry = (operation_history, state_history)
    later_resolution = replace(resolution, resolved_at="2099-01-01T00:00:00Z")
    with backend.operation(_address()) as operation:
        retried = operation.finalize_recovery(
            operation.observe(),
            evidence,
            later_resolution,
            replacement,
        )
    assert retried.state.state == expected_state
    assert before_retry == (
        _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
        _rows(
            postgres_case,
            "state_history",
            "revision, state_serial, state_checksum, state_json, operation_id::text",
        ),
    )
    idempotent = PrivatePostgresStateV2Migrator(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
        writer_role=postgres_writer.role,
    ).migrate(
        confirmed_store_id=store_id,
        confirmed_writer_role=postgres_writer.role,
    )
    assert idempotent.migrated is False
    _assert_lock_released(postgres_case)


def test_completed_recovery_rejects_conflicting_retry_identity_without_dml(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
    with backend.operation(_address()) as operation:
        operation.finalize_recovery(operation.observe(), evidence, resolution, None)

    before = {
        "control": _rows(postgres_case, "operation_control", "revision, status, control_json"),
        "operations": _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
        "state": _rows(
            postgres_case,
            "current_state",
            "revision, state_serial, state_json",
        ),
        "history": _rows(
            postgres_case,
            "state_history",
            "revision, state_serial, operation_id::text",
        ),
    }
    conflicts = (
        replace(
            resolution,
            resolved_at="2099-01-01T00:00:00Z",
            evidence_checksum="sha256:" + "f" * 64,
        ),
        replace(
            resolution,
            recovery_operation_id=str(uuid.uuid4()),
            resolved_at="2099-01-02T00:00:00Z",
        ),
    )
    for conflicting in conflicts:
        with backend.operation(_address()) as operation:
            with pytest.raises(StateBackendConflictError):
                operation.finalize_recovery(
                    operation.observe(),
                    evidence,
                    conflicting,
                    None,
                )
            assert operation.finalized is False

    after = {
        "control": _rows(postgres_case, "operation_control", "revision, status, control_json"),
        "operations": _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
        "state": _rows(
            postgres_case,
            "current_state",
            "revision, state_serial, state_json",
        ),
        "history": _rows(
            postgres_case,
            "state_history",
            "revision, state_serial, operation_id::text",
        ),
    }
    assert after == before
    _assert_lock_released(postgres_case)


def test_wrong_ids_stale_evidence_and_started_abandonment_fail_closed(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)

    with backend.operation(_address()) as operation:
        current = operation.observe()
        wrong = replace(
            _resolution(evidence, outcome="rolled_back", replacement=None),
            blocked_operation_id=str(uuid.uuid4()),
        )
        with pytest.raises(StateIdentityError):
            operation.finalize_recovery(current, evidence, wrong, None)
        abandoned = _resolution(
            evidence,
            outcome="abandoned_before_mutation",
            replacement=None,
        )
        with pytest.raises(StateBackendRecoveryRequiredError):
            operation.finalize_recovery(current, evidence, abandoned, None)

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(
            postgres_case.sql.SQL(
                "UPDATE {}.{} SET revision = revision + 1 WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                postgres_case.sql.Identifier(postgres_case.schema),
                postgres_case.sql.Identifier("operation_control"),
            ),
            (_address().namespace, _address().project, _address().environment),
        )
    with backend.operation(_address()) as operation:
        stale = blocked
        resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
        with pytest.raises(StateBackendConflictError):
            operation.finalize_recovery(stale, evidence, resolution, None)
    assert _rows(postgres_case, "state_history", "revision, state_serial") == []
    _assert_lock_released(postgres_case)


def test_recovery_finalization_rejects_version_one_owner_authority(
    postgres_case: object,
) -> None:
    PostgresStateInitializer(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    backend = PrivatePostgresStateReadBackend(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    )
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    resolution = _resolution(evidence, outcome="rolled_back", replacement=None)

    with (
        backend.operation(_address()) as operation,
        pytest.raises(StateBackendInvalidStateError),
    ):
        operation.finalize_recovery(operation.observe(), evidence, resolution, None)
    assert backend.read_snapshot(_address()).control.control.status == "recovery_required"
    assert not any(
        row[0] == resolution.recovery_operation_id
        for row in _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind",
        )
    )
    _assert_lock_released(postgres_case)


def test_forged_complete_recovery_history_cannot_migrate_from_frozen_v1(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    initialized = PostgresStateInitializer(
        dsn=postgres_case.owner_dsn,
        schema=postgres_case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
    state = LocalState(project="payments", environment="prod")
    intent = OperationIntent(
        operation_id=str(uuid.uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="forged-v1-history",
        prior_state_serial=0,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(),
    )
    control = OperationControlState(
        address=_address(),
        status="in_progress",
        intent=intent,
    )
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(
        OperationSnapshot(
            state=StateObservation(
                store=StateStoreIdentity(
                    backend="postgres",
                    store_id=initialized.store_id,
                ),
                address=_address(),
                state=state,
                revision=StateRevision.absent(),
            ),
            control=ControlObservation(
                control=control,
                revision=StateRevision("postgres-v1:1"),
            ),
        )
    )
    resolution = _resolution(
        evidence,
        outcome="abandoned_before_mutation",
        replacement=None,
    )
    events = (
        (intent.operation_id, 0, "intent", control.to_dict()),
        (
            resolution.recovery_operation_id,
            0,
            "recovery_intent",
            evidence.to_dict(),
        ),
        (
            resolution.recovery_operation_id,
            1,
            "recovered_abandoned_before_mutation",
            resolution.to_dict(),
        ),
    )
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        for operation_id, event_index, event_kind, payload in events:
            connection.execute(
                postgres_case.sql.SQL(
                    "INSERT INTO {}.{} (namespace, project, environment, "
                    "operation_id, event_index, event_kind, control_json, recorded_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, "
                    "pg_catalog.clock_timestamp())"
                ).format(
                    postgres_case.sql.Identifier(postgres_case.schema),
                    postgres_case.sql.Identifier("operation_history"),
                ),
                (
                    _address().namespace,
                    _address().project,
                    _address().environment,
                    operation_id,
                    event_index,
                    event_kind,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                ),
            )

    with pytest.raises(StateBackendInvalidStateError):
        PrivatePostgresStateV2Migrator(
            dsn=postgres_case.owner_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
            writer_role=postgres_writer.role,
        ).migrate(
            confirmed_store_id=initialized.store_id,
            confirmed_writer_role=postgres_writer.role,
        )


def test_recovery_resolution_insert_failure_rolls_back_every_table(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    replacement = _replacement()
    resolution = _resolution(evidence, outcome="observed", replacement=replacement)
    before = {
        "control": _rows(postgres_case, "operation_control", "revision, status, control_json"),
        "operations": _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
        "state": _rows(postgres_case, "current_state", "revision, state_serial, state_json"),
        "history": _rows(postgres_case, "state_history", "revision, state_serial"),
    }
    driver = _FirstConnectionResolutionInsertFailureDriver(postgres_case.psycopg)
    monkeypatch.setattr(
        postgres_backend_module,
        "_load_psycopg",
        lambda: _PsycopgBundle(driver=driver, sql=postgres_case.sql),
    )

    with (
        pytest.raises(
            StateBackendUnavailableError,
            match="state transition is unavailable",
        ),
        backend.operation(_address()) as operation,
    ):
        operation.finalize_recovery(
            operation.observe(),
            evidence,
            resolution,
            replacement,
        )
    after = {
        "control": _rows(postgres_case, "operation_control", "revision, status, control_json"),
        "operations": _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        ),
        "state": _rows(postgres_case, "current_state", "revision, state_serial, state_json"),
        "history": _rows(postgres_case, "state_history", "revision, state_serial"),
    }
    assert after == before
    _assert_lock_released(postgres_case)


def test_recovery_commit_ack_loss_is_verified_without_replay(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
    driver = _FirstConnectionCommitAckLossDriver(postgres_case.psycopg)
    monkeypatch.setattr(
        postgres_backend_module,
        "_load_psycopg",
        lambda: _PsycopgBundle(driver=driver, sql=postgres_case.sql),
    )

    def recover_with_lost_ack() -> None:
        with backend.operation(_address()) as operation:
            operation.finalize_recovery(
                operation.observe(),
                evidence,
                resolution,
                None,
            )

    with pytest.raises(StateBackendReleaseAfterCommitError) as raised:
        recover_with_lost_ack()
    assert raised.value.operation_id == resolution.recovery_operation_id
    assert driver.connections >= 2
    assert (
        len(
            [
                row
                for row in _rows(
                    postgres_case,
                    "operation_history",
                    "operation_id::text, event_index, event_kind",
                )
                if row[0] == resolution.recovery_operation_id
            ]
        )
        == 2
    )


def test_recovery_precommit_ack_loss_preserves_preimage_for_safe_retry(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
    original_loader = postgres_backend_module._load_psycopg
    driver = _FirstConnectionCommitAckLossDriver(
        postgres_case.psycopg,
        commit_on_server=False,
    )
    monkeypatch.setattr(
        postgres_backend_module,
        "_load_psycopg",
        lambda: _PsycopgBundle(driver=driver, sql=postgres_case.sql),
    )

    with (
        pytest.raises(StateBackendLockLostError) as raised,
        backend.operation(_address()) as operation,
    ):
        operation.finalize_recovery(operation.observe(), evidence, resolution, None)
    assert raised.value.operation_id == resolution.recovery_operation_id
    assert backend.read_snapshot(_address()).control.control == evidence.control
    assert not any(
        row[0] == resolution.recovery_operation_id
        for row in _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind",
        )
    )

    monkeypatch.setattr(postgres_backend_module, "_load_psycopg", original_loader)
    retry = replace(resolution, resolved_at="2099-01-01T00:00:00Z")
    with backend.operation(_address()) as operation:
        recovered = operation.finalize_recovery(operation.observe(), evidence, retry, None)
    assert recovered.control.control.status == "clear"
    recovery_events = [
        row
        for row in _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind, control_json",
        )
        if row[0] == resolution.recovery_operation_id
    ]
    assert len(recovery_events) == 2
    assert json.loads(recovery_events[1][3])["resolved_at"] == retry.resolved_at
    _assert_lock_released(postgres_case)


@pytest.mark.parametrize("corruption", ["gap", "cross_store", "secret"])
def test_recovery_semantic_history_corruption_is_rejected(
    postgres_case: object,
    postgres_writer: WriterIdentity,
    corruption: str,
) -> None:
    store_id = _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
    with backend.operation(_address()) as operation:
        operation.finalize_recovery(operation.observe(), evidence, resolution, None)

    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        history = postgres_case.sql.SQL("{}.{}").format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        )
        if corruption == "gap":
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} SET event_index = 2 WHERE operation_id = %s AND event_index = 1"
                ).format(history),
                (resolution.recovery_operation_id,),
            )
        else:
            row = connection.execute(
                postgres_case.sql.SQL(
                    "SELECT control_json FROM {} WHERE operation_id = %s AND event_index = 0"
                ).format(history),
                (resolution.recovery_operation_id,),
            ).fetchone()
            assert row is not None
            payload = json.loads(row[0])
            if corruption == "cross_store":
                payload["store"]["store_id"] = str(uuid.uuid4())
            else:
                payload["control"]["intent"]["actor"] = "token=leaked"
            connection.execute(
                postgres_case.sql.SQL(
                    "UPDATE {} SET control_json = %s WHERE operation_id = %s AND event_index = 0"
                ).format(history),
                (
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    resolution.recovery_operation_id,
                ),
            )

    with pytest.raises(StateBackendInvalidStateError):
        PrivatePostgresStateV2Migrator(
            dsn=postgres_case.owner_dsn,
            schema=postgres_case.schema,
            lock_timeout_seconds=10,
            writer_role=postgres_writer.role,
        ).migrate(
            confirmed_store_id=store_id,
            confirmed_writer_role=postgres_writer.role,
        )


def _recovery_contender(
    dsn: str,
    schema: str,
    evidence_data: dict[str, object],
    resolution_data: dict[str, object],
    sender: object,
) -> None:
    try:
        evidence = RecoverySnapshotEvidence.from_dict(evidence_data)
        resolution = RecoveryResolutionRecord.from_dict(resolution_data)
        backend = PrivatePostgresStateReadBackend(
            dsn=dsn,
            schema=schema,
            lock_timeout_seconds=20,
        )
        with backend.operation(evidence.address) as operation:
            operation.finalize_recovery(operation.observe(), evidence, resolution, None)
        sender.send(("ok", resolution.recovery_operation_id))
    except BaseException as error:
        sender.send(("error", type(error).__name__))
    finally:
        sender.close()


def test_recovery_contenders_produce_one_resolution_and_release_lock(
    postgres_case: object,
    postgres_writer: WriterIdentity,
) -> None:
    _initialize_v2(postgres_case, postgres_writer)
    backend = _backend(postgres_case, postgres_writer)
    with backend.operation(_address()) as operation:
        blocked = _block_operation(operation, operation.observe(), started=True)
    evidence = RecoverySnapshotEvidence.from_operation_snapshot(blocked)
    context = multiprocessing.get_context("spawn")
    processes: list[object] = []
    receivers: list[object] = []
    for _ in range(4):
        resolution = _resolution(evidence, outcome="rolled_back", replacement=None)
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_recovery_contender,
            args=(
                postgres_writer.dsn,
                postgres_case.schema,
                evidence.to_dict(),
                resolution.to_dict(),
                sender,
            ),
        )
        process.start()
        sender.close()
        processes.append(process)
        receivers.append(receiver)
    outcomes: list[tuple[str, str]] = []
    try:
        for receiver in receivers:
            assert receiver.poll(45)
            outcomes.append(receiver.recv())
        for process in processes:
            process.join(timeout=15)
            assert process.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        for receiver in receivers:
            receiver.close()
    assert [outcome[0] for outcome in outcomes].count("ok") == 1
    recovery_rows = [
        row
        for row in _rows(
            postgres_case,
            "operation_history",
            "operation_id::text, event_index, event_kind",
        )
        if row[0] != evidence.blocked_operation_id
    ]
    assert len(recovery_rows) == 2
    assert backend.read_snapshot(_address()).control.control.status == "clear"
    _assert_lock_released(postgres_case)
