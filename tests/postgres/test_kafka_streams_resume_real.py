"""Real PostgreSQL durability of authorized, same-intent runner resumes.

The runner observations below are typed fixture evidence, not live Kafka or
Docker observations. These tests exercise the real least-privilege PostgreSQL
writer, transaction/CAS boundaries, append-only history and restore validation.
"""

from __future__ import annotations

import json
import uuid

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend_module
from streamt.compiler.kafka_streams import application_id, compile_plan
from streamt.compiler.manifest import KafkaStreamsJobArtifact
from streamt.deployer.kafka_streams_evidence import (
    KafkaStreamsActionEvidence,
    KafkaStreamsArtifactSnapshot,
    KafkaStreamsCheckpointEvidence,
    KafkaStreamsPartitionEvidence,
    KafkaStreamsProgressEvidence,
    KafkaStreamsVolumeEvidence,
)
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
    _PsycopgBundle,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import LocalState, ManagedResourceRecord, StateError
from streamt.deployer.state_backend import (
    RESUMABLE_CONTROL_VERSION,
    OperationAction,
    OperationIntent,
    OperationProgress,
    OperationResumeRecord,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ADDRESS = StateAddress("platform", "payments", "prod")
APPLICATION = application_id("payments", "prod", "filtered")
RESOURCE = "streamt://payments/prod/kafka_streams_job/filtered"
BACKEND = "kafka-streams-docker:v1:" + "b" * 64
IMAGE = "sha256:" + "a" * 64
OLD_CONTAINER, NEW_CONTAINER, NETWORK = "c" * 64, "d" * 64, "e" * 64
STAMP = "2026-09-05T12:00:00Z"


class _CommitAckLossConnection:
    def __init__(self, connection: object, *, commit_on_server: bool) -> None:
        self.connection = connection
        self.commit_on_server = commit_on_server
        self.commits = 0

    def cursor(self) -> object:
        return self.connection.cursor()

    def commit(self) -> None:
        self.commits += 1
        if self.commit_on_server:
            self.connection.commit()
        raise RuntimeError("injected resume commit acknowledgement loss")

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()


class _FirstConnectionCommitAckLossDriver:
    def __init__(self, driver: object, *, commit_on_server: bool) -> None:
        self.driver = driver
        self.commit_on_server = commit_on_server
        self.first: _CommitAckLossConnection | None = None
        self.connections = 0

    def connect(self, conninfo: str, **kwargs: object) -> object:
        connection = self.driver.connect(conninfo, **kwargs)
        self.connections += 1
        if self.connections == 1:
            self.first = _CommitAckLossConnection(connection, commit_on_server=self.commit_on_server)
            return self.first
        return connection


def _artifact(threshold: int) -> KafkaStreamsJobArtifact:
    return KafkaStreamsJobArtifact(
        "filtered", APPLICATION, IMAGE, "bridge", "earliest",
        compile_plan(
            f"SELECT id, amount FROM orders WHERE amount >= {threshold}",
            {
                "id": {"type": "STRING", "nullable": False},
                "amount": {"type": "BIGINT", "nullable": False},
            },
            "orders", "orders.input", "orders.output",
        ),
        {"mode": "managed", "project": "payments", "type": "model", "name": "filtered"},
    )


def _progress(*, closed: bool = False) -> KafkaStreamsProgressEvidence:
    return KafkaStreamsProgressEvidence(
        "cluster-a", "AAAAAAAAAAAAAAAAAAAAAQ", "AAAAAAAAAAAAAAAAAAAAAg",
        True, 0 if closed else 1,
        (KafkaStreamsPartitionEvidence(0, 0, 100, 20 if closed else 10),),
    )


def _evidence() -> KafkaStreamsActionEvidence:
    return KafkaStreamsActionEvidence(
        1, BACKEND, OLD_CONTAINER,
        KafkaStreamsArtifactSnapshot.from_artifact(_artifact(50)),
        KafkaStreamsArtifactSnapshot.from_artifact(_artifact(75)), IMAGE, NETWORK,
        KafkaStreamsVolumeEvidence(
            APPLICATION + "-state", "local", STAMP, APPLICATION, BACKEND,
            "00000000-0000-4000-8000-000000000002",
        ),
        _progress(),
    )


def _ownership(*, desired: bool = False) -> LocalState:
    evidence = _evidence()
    artifact = evidence.desired_artifact if desired else evidence.prior_artifact
    return LocalState(
        project=ADDRESS.project, environment=ADDRESS.environment,
        serial=2 if desired else 1,
        resources={RESOURCE: ManagedResourceRecord(APPLICATION, "managed", artifact.checksum, BACKEND)},
    )


def _backend(case: PostgresCase, writer: WriterIdentity) -> PrivatePostgresStateReadBackend:
    return PrivatePostgresStateReadBackend(
        dsn=writer.dsn, schema=case.schema, lock_timeout_seconds=10, require_v2_writer=True,
    )


def _revalidate(case: PostgresCase, writer: WriterIdentity, store_id: str) -> None:
    result = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn, schema=case.schema, lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(confirmed_store_id=store_id, confirmed_writer_role=writer.role)
    assert result.store_id == store_id
    assert not result.migrated


def _initialize(case: PostgresCase, writer: WriterIdentity) -> tuple[PrivatePostgresStateReadBackend, str]:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn, schema=case.schema, lock_timeout_seconds=10,
    ).initialize(ADDRESS)
    migrated = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn, schema=case.schema, lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(confirmed_store_id=initialized.store_id, confirmed_writer_role=writer.role)
    assert migrated.migrated
    backend = _backend(case, writer)
    # Seed the protected preimage through a real state transaction, not SQL.
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        intent = OperationIntent(
            str(uuid.uuid4()), "adopt", operation_timestamp(), "postgres-resume-fixture",
            snapshot.state.state_serial, state_checksum(snapshot.state.state), None, (),
        )
        active = operation.begin_operation(snapshot, intent)
        operation.commit_operation(active, _ownership())
    return backend, initialized.store_id


def _intent(snapshot: OperationSnapshot) -> OperationIntent:
    return OperationIntent(
        str(uuid.uuid4()), "apply", operation_timestamp(), "postgres-resume-test",
        snapshot.state.state_serial, state_checksum(snapshot.state.state), "sha256:" + "f" * 64,
        (OperationAction(0, RESOURCE, "update", kafka_streams_evidence=_evidence()),),
    )


def _boundaries(intent: OperationIntent, *, succeeded: bool = True) -> tuple[OperationProgress, ...]:
    boundaries = [OperationProgress(
        intent.operation_id, 0, RESOURCE, "update", "started", None, operation_timestamp(),
    )]
    for phase in ("old_closed", "old_removed", "replacement_created"):
        closed = phase == "old_closed"
        checkpoint = KafkaStreamsCheckpointEvidence(
            1, phase, intent.operation_id, 0, OLD_CONTAINER,
            NEW_CONTAINER if phase == "replacement_created" else None,
            _evidence().prior_artifact.plan_hash if closed else None,
            143 if closed else None, _progress(closed=True) if closed else None,
        )
        boundaries.append(OperationProgress(
            intent.operation_id, 0, RESOURCE, "update", "checkpoint", None,
            operation_timestamp(), checkpoint,
        ))
    boundaries.append(OperationProgress(
        intent.operation_id, 0, RESOURCE, "update", "completed", succeeded, operation_timestamp(),
    ))
    return tuple(boundaries)


def _interrupt(operation: object, snapshot: OperationSnapshot) -> OperationSnapshot:
    intent = snapshot.control.control.intent
    assert intent is not None
    return operation.mark_recovery_required(snapshot, RecoveryRecord(
        intent.operation_id, "runtime_outcome_unknown", operation_timestamp(),
        0 if snapshot.control.control.actions_completed else None,
    ))


def _resume_record(snapshot: OperationSnapshot) -> OperationResumeRecord:
    return OperationResumeRecord.create(
        snapshot, resume_id=str(uuid.uuid4()), actor="postgres-resume-operator",
        resumed_at=operation_timestamp(),
    )


def _rows(case: PostgresCase, table: str, columns: str) -> list[tuple[object, ...]]:
    with case.psycopg.connect(case.owner_dsn) as connection:
        return connection.execute(case.sql.SQL("SELECT {} FROM {}.{} ORDER BY 1, 2").format(
            case.sql.SQL(columns), case.sql.Identifier(case.schema), case.sql.Identifier(table),
        )).fetchall()


def _durable_rows(case: PostgresCase) -> dict[str, list[tuple[object, ...]]]:
    return {
        table: _rows(case, table, columns)
        for table, columns in (
            ("current_state", "revision, state_serial, state_checksum, state_json"),
            ("operation_control", "revision, status, control_json"),
            ("state_history", "revision, state_serial, state_checksum, state_json, operation_id::text"),
            ("operation_history", "operation_id::text, event_index, event_kind, control_json"),
        )
    }


def _migration_blocked(case: PostgresCase, writer: WriterIdentity, store_id: str) -> None:
    # Administrative migration intentionally requires all controls clear. It
    # must not turn an unfinished deployment into an implicit resume authority.
    before = _durable_rows(case)
    with pytest.raises(StateBackendInvalidStateError):
        _revalidate(case, writer, store_id)
    assert _durable_rows(case) == before


def _history(case: PostgresCase, intent: OperationIntent) -> list[tuple[object, ...]]:
    return [row[1:] for row in _durable_rows(case)["operation_history"] if row[0] == intent.operation_id]


def _blocked_at(backend: PrivatePostgresStateReadBackend, count: int, *, succeeded: bool = True) -> OperationSnapshot:
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot)
        snapshot = operation.begin_operation(snapshot, intent)
        for boundary in _boundaries(intent, succeeded=succeeded)[:count]:
            snapshot = operation.record_progress(snapshot, boundary)
        return _interrupt(operation, snapshot)


def test_real_postgres_repeated_resume_preserves_incidents_and_commits_once(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
) -> None:
    backend, store_id = _initialize(postgres_case, postgres_writer)
    initial_rows = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation:
        initial = operation.observe()
        intent = _intent(initial)
        operation.begin_operation(initial, intent)
    original_intent = intent.to_dict(control_version=4)
    records: list[OperationResumeRecord] = []
    expected_kinds = ["intent"]
    boundaries = _boundaries(intent)

    for count, boundary in enumerate(boundaries[:-1], start=1):
        # Both the interruption and authorization cross independent connections.
        with _backend(postgres_case, postgres_writer).operation(ADDRESS) as operation:
            snapshot = operation.record_progress(operation.observe(), boundary)
            blocked = _interrupt(operation, snapshot)
        blocked_rows = _durable_rows(postgres_case)
        _migration_blocked(postgres_case, postgres_writer, store_id)
        with _backend(postgres_case, postgres_writer).operation(ADDRESS) as operation:
            observed = operation.observe()
            assert observed == blocked
            assert operation.pending_resume_authorization(observed) is None
            with pytest.raises(StateBackendRecoveryRequiredError):
                operation.ensure_ready(observed)
            record = _resume_record(observed)
            resumed = operation.resume_operation(observed, record)
        records.append(record)
        assert resumed.state == initial.state
        assert resumed.control.control.status == "in_progress"
        assert resumed.control.control.control_version == RESUMABLE_CONTROL_VERSION == 5
        assert resumed.control.control.resume_history == tuple(records)
        assert resumed.control.control.intent.to_dict(control_version=4) == original_intent
        assert resumed.control.control.progress == boundaries[:count]
        assert record.recovery == blocked.control.control.recovery
        assert record.progress_count == count
        assert record.store.store_id == store_id
        with _backend(postgres_case, postgres_writer).operation(ADDRESS) as operation:
            assert operation.observe() == resumed
            assert operation.pending_resume_authorization(resumed) is None
        after_rows = _durable_rows(postgres_case)
        assert after_rows["current_state"] == initial_rows["current_state"]
        assert after_rows["state_history"] == initial_rows["state_history"]
        assert after_rows["operation_control"][0][0] == blocked_rows["operation_control"][0][0] + 1
        expected_kinds.extend([f"progress_{boundary.status}", "recovery_required", "operation_resumed"])
        history = _history(postgres_case, intent)
        assert [(row[0], row[1]) for row in history] == list(enumerate(expected_kinds))
        assert [row[3] for row in after_rows["operation_history"] if row[0] == intent.operation_id][:-1] == [
            row[3] for row in blocked_rows["operation_history"] if row[0] == intent.operation_id
        ]
        _migration_blocked(postgres_case, postgres_writer, store_id)

    with backend.operation(ADDRESS) as operation:
        completed = operation.record_progress(operation.observe(), boundaries[-1])
        committed = operation.commit_operation(completed, _ownership(desired=True))
    assert committed.control.control.status == "clear"
    assert committed.state.state == _ownership(desired=True)
    final_rows = _durable_rows(postgres_case)
    assert final_rows["state_history"][:-1] == initial_rows["state_history"]
    assert len(final_rows["state_history"]) == 2
    assert final_rows["state_history"][-1][-1] == intent.operation_id
    expected_kinds.extend(["progress_completed", "succeeded"])
    history = _history(postgres_case, intent)
    assert [(row[0], row[1]) for row in history] == list(enumerate(expected_kinds))
    durable_completed = json.loads(history[-2][2])
    assert durable_completed["resume_history"] == [record.to_dict() for record in records]
    assert durable_completed["progress"][1]["kafka_streams_checkpoint"]["exit_code"] == 143
    assert durable_completed["progress"][1]["kafka_streams_checkpoint"]["progress"]["partitions"][0]["committed"] == 20
    assert [json.loads(row[2])["recovery"] for row in history if row[1] == "recovery_required"] == [
        record.recovery.to_dict() for record in records
    ]
    _revalidate(postgres_case, postgres_writer, store_id)
    assert _durable_rows(postgres_case) == final_rows
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        operation.ensure_ready(observed)
        assert operation.pending_resume_authorization(observed) is None
    assert _durable_rows(postgres_case) == final_rows


@pytest.mark.parametrize("count", [0, 2])
def test_real_postgres_stale_resume_does_not_append_or_change_ownership(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, count: int,
) -> None:
    backend, store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, count)
    first = _resume_record(blocked)
    stale = _resume_record(blocked)
    with backend.operation(ADDRESS) as operation:
        operation.resume_operation(operation.observe(), first)
    before = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation, pytest.raises(StateBackendConflictError):
        operation.pending_resume_authorization(blocked)
    assert _durable_rows(postgres_case) == before
    with backend.operation(ADDRESS) as operation, pytest.raises(StateBackendConflictError):
        operation.resume_operation(blocked, stale)
    assert _durable_rows(postgres_case) == before
    with backend.operation(ADDRESS) as operation:
        interrupted_again = _interrupt(operation, operation.observe())
    with backend.operation(ADDRESS) as operation:
        second = _resume_record(operation.observe())
        active = operation.resume_operation(operation.observe(), second)
    assert active.control.control.resume_history == (first, second)
    assert first.progress_count == second.progress_count == count
    assert second.source_control_checksum != first.source_control_checksum
    assert second.recovery == interrupted_again.control.control.recovery
    _migration_blocked(postgres_case, postgres_writer, store_id)


@pytest.mark.parametrize("succeeded", [True, False])
def test_real_postgres_completed_boundary_cannot_be_resumed(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, succeeded: bool,
) -> None:
    backend, store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 5, succeeded=succeeded)
    before = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation:
        assert operation.observe() == blocked
        with pytest.raises(StateError):
            _resume_record(blocked)
    assert _durable_rows(postgres_case) == before
    _migration_blocked(postgres_case, postgres_writer, store_id)


def test_real_postgres_resume_refuses_changed_protected_ownership(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
) -> None:
    backend, _store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 2)
    authorization = _resume_record(blocked)
    changed = _ownership(desired=True)
    changed.serial = blocked.state.state_serial
    # Simulate out-of-band tampering at the same serial. Resume cannot adopt
    # this changed preimage just because the operation ID is still identical.
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(postgres_case.sql.SQL(
            "UPDATE {}.{} SET state_json = %s, state_checksum = %s",
        ).format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("current_state"),
        ), (json.dumps(changed.to_dict(), sort_keys=True, separators=(",", ":")), state_checksum(changed)))
    before = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        assert observed.state.state == changed
        with pytest.raises(StateBackendConflictError):
            operation.resume_operation(observed, authorization)
    assert _durable_rows(postgres_case) == before


@pytest.mark.parametrize("commit_on_server", [False, True])
def test_real_postgres_resume_commit_ack_loss_never_replays_or_partially_commits(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch, commit_on_server: bool,
) -> None:
    backend, _store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 2)
    authorization = _resume_record(blocked)
    before = _durable_rows(postgres_case)
    driver = _FirstConnectionCommitAckLossDriver(postgres_case.psycopg, commit_on_server=commit_on_server)
    with monkeypatch.context() as patch:
        patch.setattr(postgres_backend_module, "_load_psycopg", lambda: _PsycopgBundle(driver=driver, sql=postgres_case.sql))
        with pytest.raises(StateBackendLockLostError) as raised, backend.operation(ADDRESS) as operation:
            operation.resume_operation(operation.observe(), authorization)
    assert raised.value.operation_id == authorization.operation_id
    assert driver.first is not None
    assert driver.first.commits == 1
    assert driver.connections >= 2
    after = _durable_rows(postgres_case)
    assert after["current_state"] == before["current_state"]
    assert after["state_history"] == before["state_history"]
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        assert operation.pending_resume_authorization(observed) is None
        assert _durable_rows(postgres_case) == after
        if commit_on_server:
            assert observed.control.control.status == "in_progress"
            assert observed.control.control.resume_history == (authorization,)
            with pytest.raises(StateBackendConflictError):
                operation.resume_operation(blocked, authorization)
        else:
            assert after == before
            assert observed == blocked
            operation.resume_operation(observed, authorization)
    final_rows = _durable_rows(postgres_case)
    assert len(final_rows["operation_history"]) == len(before["operation_history"]) + 1
    assert final_rows["operation_control"][0][0] == before["operation_control"][0][0] + 1
    assert final_rows["current_state"] == before["current_state"]
    assert final_rows["state_history"] == before["state_history"]


@pytest.mark.parametrize("corruption", ["missing_resume_event", "edited_history", "missing_history"])
@pytest.mark.parametrize("committed", [False, True])
def test_real_postgres_restore_rejects_missing_or_edited_resume_history(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, corruption: str, committed: bool,
) -> None:
    backend, store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 2)
    intent = blocked.control.control.intent
    assert intent is not None
    with backend.operation(ADDRESS) as operation:
        active = operation.resume_operation(operation.observe(), _resume_record(blocked))
        if committed:
            for boundary in _boundaries(intent)[2:]:
                active = operation.record_progress(active, boundary)
            operation.commit_operation(active, _ownership(desired=True))
    if committed:
        _revalidate(postgres_case, postgres_writer, store_id)
    else:
        _migration_blocked(postgres_case, postgres_writer, store_id)
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        table = postgres_case.sql.SQL("{}.{}").format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        )
        if corruption == "missing_resume_event":
            connection.execute(postgres_case.sql.SQL(
                "DELETE FROM {} WHERE operation_id = %s AND event_kind = 'operation_resumed'",
            ).format(table), (intent.operation_id,))
        else:
            row = connection.execute(postgres_case.sql.SQL(
                "SELECT control_json FROM {} WHERE operation_id = %s AND event_kind = 'operation_resumed'",
            ).format(table), (intent.operation_id,)).fetchone()
            assert row is not None
            payload = json.loads(row[0])
            if corruption == "edited_history":
                payload["resume_history"][0]["actor"] = "different-operator"
            else:
                del payload["resume_history"]
            connection.execute(postgres_case.sql.SQL(
                "UPDATE {} SET control_json = %s WHERE operation_id = %s AND event_kind = 'operation_resumed'",
            ).format(table), (json.dumps(payload, sort_keys=True, separators=(",", ":")), intent.operation_id))
    corrupted = _durable_rows(postgres_case)
    if committed:
        with pytest.raises(StateBackendInvalidStateError):
            _revalidate(postgres_case, postgres_writer, store_id)
    else:
        # Refuse at the snapshot read, before a runtime can use an apparently
        # active control to perform provider IO between journal boundaries.
        with backend.operation(ADDRESS) as operation, pytest.raises(StateBackendInvalidStateError):
            operation.observe()
    assert _durable_rows(postgres_case) == corrupted


@pytest.mark.parametrize("count", [0, 4])
def test_real_postgres_v4_runner_snapshot_refuses_truncated_history_before_resume(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, count: int,
) -> None:
    backend, _store_id = _initialize(postgres_case, postgres_writer)
    with backend.operation(ADDRESS) as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot)
        active = operation.begin_operation(snapshot, intent)
        for boundary in _boundaries(intent)[:count]:
            active = operation.record_progress(active, boundary)
    assert active.control.control.control_version == 4
    assert active.control.control.resume_history == ()
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        connection.execute(postgres_case.sql.SQL(
            "DELETE FROM {}.{} WHERE operation_id = %s AND event_index = %s",
        ).format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        ), (intent.operation_id, count))
    corrupted = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation, pytest.raises(StateBackendInvalidStateError):
        operation.observe()
    assert _durable_rows(postgres_case) == corrupted


@pytest.mark.parametrize("damage", ["missing", "truncated", "changed"])
def test_real_postgres_pending_authorization_rejects_corrupt_blocked_history_read_only(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity, damage: str,
) -> None:
    backend, _store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 4)
    intent = blocked.control.control.intent
    assert intent is not None
    with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
        table = postgres_case.sql.SQL("{}.{}").format(
            postgres_case.sql.Identifier(postgres_case.schema),
            postgres_case.sql.Identifier("operation_history"),
        )
        if damage == "missing":
            connection.execute(postgres_case.sql.SQL(
                "DELETE FROM {} WHERE operation_id = %s",
            ).format(table), (intent.operation_id,))
        elif damage == "truncated":
            connection.execute(postgres_case.sql.SQL(
                "DELETE FROM {} WHERE operation_id = %s AND event_kind = 'recovery_required'",
            ).format(table), (intent.operation_id,))
        else:
            payload = blocked.control.control.to_dict()
            payload["recovery"]["failure_code"] = "changed_incident"
            connection.execute(postgres_case.sql.SQL(
                "UPDATE {} SET control_json = %s WHERE operation_id = %s AND event_kind = 'recovery_required'",
            ).format(table), (json.dumps(payload, sort_keys=True, separators=(",", ":")), intent.operation_id))
    corrupted = _durable_rows(postgres_case)
    with backend.operation(ADDRESS) as operation:
        observed = operation.observe()
        assert observed == blocked
        with pytest.raises(StateBackendInvalidStateError, match="deployment state is invalid"):
            operation.pending_resume_authorization(observed)
    assert _durable_rows(postgres_case) == corrupted


def test_real_postgres_pending_authorization_refuses_a_lost_locked_connection(
    postgres_case: PostgresCase, postgres_writer: WriterIdentity,
) -> None:
    backend, _store_id = _initialize(postgres_case, postgres_writer)
    blocked = _blocked_at(backend, 2)
    before = _durable_rows(postgres_case)
    def read_after_loss() -> None:
        with backend.operation(ADDRESS) as operation:
            observed = operation.observe()
            assert observed == blocked
            with postgres_case.psycopg.connect(postgres_case.admin_dsn, autocommit=True) as connection:
                assert connection.execute(
                    "SELECT pg_catalog.pg_terminate_backend(%s)", (operation.backend_pid,),
                ).fetchone() == (True,)
            operation.pending_resume_authorization(observed)

    with pytest.raises(StateBackendLockLostError):
        read_after_loss()
    assert _durable_rows(postgres_case) == before
