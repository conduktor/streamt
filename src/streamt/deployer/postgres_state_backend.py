"""Private PostgreSQL ownership and session-affine mutation scaffolding.

This module is deliberately unreachable from deployment-state configuration,
the ordinary backend factory, and every CLI command.  It exists so the frozen
version-one catalog can exercise the future remote backend with isolated
schema-owner credentials in focused tests.  Its mutation surface is exposed
only by an explicitly constructed operation; the ordinary backend factory and
CLI remain unable to select it.

Psycopg remains optional: construction imports nothing from the driver, and a
connection loads it lazily through the existing PostgreSQL administration
boundary.
"""

from __future__ import annotations

import json
import time
from contextlib import AbstractContextManager
from types import TracebackType

from streamt.deployer.postgres_state import (
    POSTGRES_STATE_MAX_BYTES,
    PostgresStateAdministration,
    _advisory_lock_key,
    _Connection,
    _Cursor,
    _dsn_tls_options,
    _load_psycopg,
    _one_or_none,
    _PsycopgBundle,
    _query,
    _registered_advisory_lock_key,
    _rows,
    _SqlModule,
    _strict_json,
)
from streamt.deployer.state import LocalState, StateError, StateIdentityError
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendRecoveryRequiredError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

_CONNECT_TIMEOUT_SECONDS = 10
_STATEMENT_TIMEOUT_MILLISECONDS = 30_000
_LOCK_RETRY_INTERVAL_SECONDS = 0.05
_REVISION_PREFIX = "postgres-v1:"
_V1_TABLES = (
    "current_state",
    "operation_control",
    "operation_history",
    "schema_migrations",
    "state_addresses",
    "state_history",
    "store_metadata",
)
_OPERATION_EVENTS = {
    "intent",
    "progress_started",
    "progress_completed",
    "recovery_required",
    "cleared_before_mutation",
    "succeeded",
}


def _canonical_json(value: dict[str, object], *, label: str) -> str:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        byte_length = len(encoded.encode("utf-8"))
    except (TypeError, ValueError, UnicodeError):
        raise StateBackendInvalidStateError(
            f"PostgreSQL deployment state {label} is invalid"
        ) from None
    if byte_length > POSTGRES_STATE_MAX_BYTES:
        raise StateBackendInvalidStateError(
            f"PostgreSQL deployment state {label} exceeds the size limit"
        )
    return encoded


def _revision_number(revision: StateRevision, *, allow_absent: bool) -> int | None:
    if allow_absent and revision.is_absent:
        return None
    if not revision.value.startswith(_REVISION_PREFIX):
        raise StateBackendConflictError(
            "PostgreSQL deployment state observation belongs to another provider"
        )
    raw = revision.value.removeprefix(_REVISION_PREFIX)
    if not raw.isascii() or not raw.isdigit():
        raise StateBackendConflictError(
            "PostgreSQL deployment state observation has an invalid revision"
        )
    return int(raw)


def _revision(value: object, *, allow_zero: bool, label: str) -> StateRevision:
    minimum = 0 if allow_zero else 1
    if type(value) is not int or value < minimum:
        raise StateBackendInvalidStateError(f"PostgreSQL deployment state {label} is invalid")
    return StateRevision(f"{_REVISION_PREFIX}{value}")


def _parse_control_row(
    row: tuple[object, ...] | None,
    address: StateAddress,
) -> ControlObservation:
    if row is None or len(row) != 4:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state operation control is invalid"
        )
    raw_revision, status, control_json, byte_length = row
    revision = _revision(
        raw_revision,
        allow_zero=True,
        label="operation control",
    )
    if (
        type(byte_length) is not int
        or byte_length < 0
        or byte_length > POSTGRES_STATE_MAX_BYTES
        or not isinstance(control_json, str)
    ):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state operation control is invalid"
        )
    try:
        encoded_length = len(control_json.encode("utf-8"))
    except UnicodeError:
        encoded_length = -1
    if encoded_length != byte_length:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state operation control is invalid"
        )
    raw_control = _strict_json(control_json, label="operation control")
    try:
        control = OperationControlState.from_dict(
            raw_control,
            expected_address=address,
        )
    except StateError:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state operation control is invalid"
        ) from None
    if status != control.status:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state operation control is invalid"
        )
    return ControlObservation(control=control, revision=revision)


def _parse_state_row(
    row: tuple[object, ...] | None,
    *,
    address: StateAddress,
    store: StateStoreIdentity,
    control: ControlObservation,
) -> StateObservation:
    if row is None:
        # A strictly parsed, present active control row can legitimately pair
        # with absent ownership after the first operation records intent and
        # before it commits initial state.  Preserving that pair is required
        # for a successor to observe the durable recovery blocker.
        del control
        return StateObservation(
            store=store,
            address=address,
            state=LocalState(
                project=address.project,
                environment=address.environment,
            ),
            revision=StateRevision.absent(),
        )
    if len(row) != 5:
        raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
    raw_revision, serial, checksum, state_json, byte_length = row
    revision = _revision(
        raw_revision,
        allow_zero=False,
        label="ownership state",
    )
    if (
        type(serial) is not int
        or serial < 1
        or not isinstance(checksum, str)
        or not isinstance(state_json, str)
        or type(byte_length) is not int
        or byte_length < 0
        or byte_length > POSTGRES_STATE_MAX_BYTES
    ):
        raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
    try:
        encoded_length = len(state_json.encode("utf-8"))
    except UnicodeError:
        encoded_length = -1
    if encoded_length != byte_length:
        raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
    raw_state = _strict_json(state_json, label="ownership state")
    try:
        state = LocalState.from_dict(
            raw_state,
            expected_project=address.project,
            expected_environment=address.environment,
        )
    except StateError:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment ownership state is invalid"
        ) from None
    if state.serial != serial or state_checksum(state) != checksum:
        raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
    return StateObservation(
        store=store,
        address=address,
        state=state,
        revision=revision,
    )


def _read_snapshot_transaction(
    *,
    cursor: _Cursor,
    bundle: _PsycopgBundle,
    dsn: str,
    schema: str,
    lock_timeout_seconds: int,
    address: StateAddress,
    for_update: bool = False,
) -> OperationSnapshot:
    """Read an exactly validated address from the caller's open transaction."""
    status = PostgresStateAdministration(
        dsn=dsn,
        schema=schema,
        lock_timeout_seconds=lock_timeout_seconds,
    )._read_status(cursor, bundle.sql, address)
    if (
        status.store_status != "ready"
        or status.store_id is None
        or status.address_status != "registered"
    ):
        raise StateBackendInvalidStateError("PostgreSQL deployment state address is not registered")
    try:
        store = StateStoreIdentity(backend="postgres", store_id=status.store_id)
    except StateError:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state metadata is invalid"
        ) from None

    params = (address.namespace, address.project, address.environment)
    row_lock = " FOR UPDATE" if for_update else ""
    control_row = _one_or_none(
        _rows(
            cursor,
            _query(
                bundle.sql,
                (
                    "SELECT revision, status, control_json, octet_length(control_json) "
                    "FROM {} WHERE namespace = %s AND project = %s "
                    f"AND environment = %s LIMIT 2{row_lock}"
                ),
                schema,
                "operation_control",
            ),
            params,
        ),
        label="operation control",
    )
    control = _parse_control_row(control_row, address)
    state_row = _one_or_none(
        _rows(
            cursor,
            _query(
                bundle.sql,
                (
                    "SELECT revision, state_serial, state_checksum, state_json, "
                    "octet_length(state_json) FROM {} WHERE namespace = %s "
                    f"AND project = %s AND environment = %s LIMIT 2{row_lock}"
                ),
                schema,
                "current_state",
            ),
            params,
        ),
        label="ownership",
    )
    state = _parse_state_row(
        state_row,
        address=address,
        store=store,
        control=control,
    )
    return OperationSnapshot(state=state, control=control)


def _begin_snapshot(cursor: _Cursor, lock_timeout_seconds: int) -> None:
    cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    cursor.execute("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)")
    cursor.execute(
        "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
        (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
    )
    cursor.execute(
        "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
        (f"{lock_timeout_seconds * 1000}ms",),
    )


def _begin_mutation(cursor: _Cursor, lock_timeout_seconds: int) -> None:
    cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE")
    cursor.execute("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)")
    cursor.execute(
        "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
        (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
    )
    cursor.execute(
        "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
        (f"{lock_timeout_seconds * 1000}ms",),
    )


def _primary_pid(cursor: _Cursor) -> int:
    rows = _rows(
        cursor,
        "SELECT pg_catalog.pg_is_in_recovery(), pg_catalog.pg_backend_pid()",
    )
    if (
        len(rows) != 1
        or len(rows[0]) != 2
        or rows[0][0] is not False
        or type(rows[0][1]) is not int
        or rows[0][1] <= 0
    ):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state requires a direct primary session"
        )
    return rows[0][1]


def _prove_v1_owner(cursor: _Cursor, schema: str) -> None:
    """Require the private v1 writer to be the exact schema/table owner."""
    rows = _rows(
        cursor,
        (
            "SELECT CURRENT_USER, n.nspowner = r.oid, "
            "pg_catalog.count(c.oid), "
            "pg_catalog.bool_and(c.relowner = r.oid) FROM pg_catalog.pg_namespace AS n "
            "JOIN pg_catalog.pg_roles AS r ON r.rolname = CURRENT_USER "
            "LEFT JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid "
            "AND c.relkind = 'r' AND c.relname = ANY(%s) WHERE n.nspname = %s "
            "GROUP BY n.nspowner, r.oid"
        ),
        (list(_V1_TABLES), schema),
    )
    if (
        len(rows) != 1
        or len(rows[0]) != 4
        or not isinstance(rows[0][0], str)
        or rows[0][1:] != (True, len(_V1_TABLES), True)
    ):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state private mutation requires the version-one owner"
        )


class _PostgresStateReadOperation:
    """One test-only physical session holding the registered advisory lock."""

    __slots__ = (
        "_active_operation_id",
        "_address",
        "_backend_pid",
        "_bundle",
        "_connection",
        "_cursor",
        "_dsn",
        "_finalized",
        "_finalized_operation_id",
        "_last_attempted_operation_id",
        "_lock_key",
        "_lock_timeout_seconds",
        "_lost",
        "_schema",
    )

    def __init__(
        self,
        *,
        connection: _Connection,
        cursor: _Cursor,
        bundle: _PsycopgBundle,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
        address: StateAddress,
        lock_key: int,
        backend_pid: int,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self._bundle = bundle
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds
        self._address = address
        self._active_operation_id: str | None = None
        self._lock_key = lock_key
        self._backend_pid = backend_pid
        self._last_attempted_operation_id: str | None = None
        self._lost = False
        self._finalized = False
        self._finalized_operation_id: str | None = None

    @property
    def backend_pid(self) -> int:
        """Expose only the numeric server PID for focused failure injection."""
        return self._backend_pid

    @property
    def finalized(self) -> bool:
        """Whether a final commit has been independently verified."""
        return self._finalized

    @property
    def finalized_operation_id(self) -> str | None:
        return self._finalized_operation_id

    @property
    def last_operation_id(self) -> str | None:
        return self._active_operation_id or self._last_attempted_operation_id

    def check_lock(self) -> None:
        """Verify primary, physical-session continuity, and exact lock ownership."""
        if self._lost:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None
        try:
            rows = _rows(
                self._cursor,
                (
                    "SELECT pg_catalog.pg_backend_pid(), "
                    "pg_catalog.pg_is_in_recovery(), EXISTS ("
                    "SELECT 1 FROM pg_catalog.pg_locks AS l "
                    "WHERE l.locktype = 'advisory' "
                    "AND l.pid = pg_catalog.pg_backend_pid() AND l.granted "
                    "AND l.classid = (((%s::bigint >> 32) & 4294967295)::oid) "
                    "AND l.objid = ((%s::bigint & 4294967295)::oid) "
                    "AND l.objsubid = 1 AND l.database = (SELECT d.oid "
                    "FROM pg_catalog.pg_database AS d "
                    "WHERE d.datname = pg_catalog.current_database()))"
                ),
                (self._lock_key, self._lock_key),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self._lost = True
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None
        if rows != [(self._backend_pid, False, True)]:
            self._lost = True
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None

    def observe(self) -> OperationSnapshot:
        """Return state and control from one bounded repeatable-read snapshot."""
        self.check_lock()
        transaction_started = False
        invalid = False
        snapshot: OperationSnapshot | None = None
        try:
            _begin_snapshot(self._cursor, self._lock_timeout_seconds)
            transaction_started = True
            snapshot = _read_snapshot_transaction(
                cursor=self._cursor,
                bundle=self._bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=self._address,
            )
        except StateBackendInvalidStateError:
            invalid = True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            self._lost = True
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None
        finally:
            if transaction_started:
                try:
                    self._connection.rollback()
                except Exception:
                    self._lost = True
        if invalid:
            # Catalog incompatibility is deterministic evidence from the
            # completed snapshot and remains the classification even if the
            # subsequent read-only rollback also discovers session loss.
            raise StateBackendInvalidStateError("PostgreSQL deployment state is invalid") from None
        if self._lost:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None
        if snapshot is None:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost",
                operation_id=self.last_operation_id,
            ) from None
        self.check_lock()
        return snapshot

    def read(self) -> StateObservation:
        return self.observe().state

    def read_control(self) -> ControlObservation:
        return self.observe().control

    @staticmethod
    def _operation_id(control: OperationControlState) -> str | None:
        return control.intent.operation_id if control.intent is not None else None

    def _validate_expected_snapshot(
        self,
        expected: OperationSnapshot,
        current: OperationSnapshot,
    ) -> None:
        if expected.address != self._address or current.address != self._address:
            raise StateIdentityError(
                "PostgreSQL deployment state observation belongs to another address"
            )
        if expected.state.store != current.state.store:
            raise StateBackendConflictError(
                "PostgreSQL deployment state store identity changed after observation"
            )
        if (
            expected.state.revision != current.state.revision
            or expected.state.state_serial != current.state.state_serial
            or state_checksum(expected.state.state) != state_checksum(current.state.state)
            or expected.state.state != current.state.state
        ):
            raise StateBackendConflictError(
                "PostgreSQL deployment ownership state changed after observation"
            )
        if (
            expected.control.revision != current.control.revision
            or expected.control.control.status != current.control.control.status
            or self._operation_id(expected.control.control)
            != self._operation_id(current.control.control)
            or expected.control.control != current.control.control
        ):
            raise StateBackendConflictError(
                "PostgreSQL deployment operation control changed after observation"
            )

    @staticmethod
    def _require_active(snapshot: OperationSnapshot) -> OperationIntent:
        control = snapshot.control.control
        if control.status != "in_progress" or control.intent is None:
            raise StateBackendRecoveryRequiredError(
                "deployment state has no active operation that can be advanced"
            )
        return control.intent

    @staticmethod
    def _require_completed(snapshot: OperationSnapshot) -> OperationIntent:
        intent = _PostgresStateReadOperation._require_active(snapshot)
        expected: list[tuple[int, str, bool | None]] = []
        for action in intent.actions:
            expected.extend(
                (
                    (action.index, "started", None),
                    (action.index, "completed", True),
                )
            )
        actual = [
            (item.action_index, item.status, item.succeeded)
            for item in snapshot.control.control.progress
        ]
        if actual != expected:
            raise StateBackendRecoveryRequiredError(
                "deployment operation is incomplete; explicit recovery is required"
            )
        return intent

    def _expected_history_rows(
        self,
        control: OperationControlState,
    ) -> list[tuple[object, ...]]:
        intent = control.intent
        if intent is None:
            return []
        states: list[tuple[str, OperationControlState]] = [
            (
                "intent",
                OperationControlState(
                    address=self._address,
                    status="in_progress",
                    intent=intent,
                ),
            )
        ]
        for index, progress in enumerate(control.progress, start=1):
            states.append(
                (
                    f"progress_{progress.status}",
                    OperationControlState(
                        address=self._address,
                        status="in_progress",
                        intent=intent,
                        progress=control.progress[:index],
                    ),
                )
            )
        if control.status == "recovery_required":
            states.append(("recovery_required", control))
        result: list[tuple[object, ...]] = []
        for index, (kind, state) in enumerate(states):
            payload = _canonical_json(state.to_dict(), label="operation history")
            result.append((index, kind, payload, len(payload.encode("utf-8"))))
        return result

    def _read_operation_history(
        self,
        cursor: _Cursor,
        sql: _SqlModule,
        operation_id: str,
    ) -> list[tuple[object, ...]]:
        return _rows(
            cursor,
            _query(
                sql,
                (
                    "SELECT event_index, event_kind, control_json, "
                    "octet_length(control_json) FROM {} WHERE namespace = %s "
                    "AND project = %s AND environment = %s AND operation_id = %s "
                    "ORDER BY event_index"
                ),
                self._schema,
                "operation_history",
            ),
            (
                self._address.namespace,
                self._address.project,
                self._address.environment,
                operation_id,
            ),
        )

    def ensure_ready(self, observation: OperationSnapshot) -> None:
        """Prove that the supplied state/control pair is still clear."""
        current = self.observe()
        self._validate_expected_snapshot(observation, current)
        if current.control.control.status != "clear":
            raise StateBackendRecoveryRequiredError(
                "deployment state has an unfinished operation; explicit recovery "
                "is required before apply or adopt"
            )

    def _update_control(
        self,
        *,
        expected: OperationSnapshot,
        replacement: OperationControlState,
        operation_id: str,
        event_index: int,
        event_kind: str,
        replacement_state: LocalState | None,
        mutate_state: bool,
    ) -> tuple[OperationSnapshot, int, int | None, bool]:
        expected_control_json = _canonical_json(
            expected.control.control.to_dict(),
            label="operation control",
        )
        replacement_control_json = _canonical_json(
            replacement.to_dict(),
            label="operation control",
        )
        expected_control_revision = _revision_number(
            expected.control.revision,
            allow_absent=False,
        )
        if expected_control_revision is None:
            raise StateBackendConflictError(
                "PostgreSQL deployment operation control revision is invalid"
            )
        expected_state_revision = _revision_number(
            expected.state.revision,
            allow_absent=True,
        )
        expected_state_json = _canonical_json(
            expected.state.state.to_dict(),
            label="ownership state",
        )
        params = (
            self._address.namespace,
            self._address.project,
            self._address.environment,
        )

        transaction_started = False
        commit_attempted = False
        try:
            self.check_lock()
            _begin_mutation(self._cursor, self._lock_timeout_seconds)
            transaction_started = True
            self.check_lock()
            _prove_v1_owner(self._cursor, self._schema)
            current = _read_snapshot_transaction(
                cursor=self._cursor,
                bundle=self._bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=self._address,
                for_update=True,
            )
            self._validate_expected_snapshot(expected, current)
            existing_history = self._read_operation_history(
                self._cursor,
                self._bundle.sql,
                operation_id,
            )
            expected_existing_history = self._expected_history_rows(expected.control.control)
            if existing_history != expected_existing_history:
                raise StateBackendConflictError(
                    "PostgreSQL deployment operation history changed after observation"
                )

            new_state_revision: int | None = expected_state_revision
            committed_state = expected.state.state
            if mutate_state:
                if replacement_state is None:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment replacement state is invalid"
                    )
                if (
                    replacement_state.project != self._address.project
                    or replacement_state.environment != self._address.environment
                ):
                    raise StateIdentityError(
                        "replacement state identity does not match its canonical address"
                    )
                if replacement_state.serial != expected.state.state_serial + 1:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment replacement state serial is invalid"
                    )
                replacement_state_json = _canonical_json(
                    replacement_state.to_dict(),
                    label="ownership state",
                )
                replacement_checksum = state_checksum(replacement_state)
                if expected_state_revision is None:
                    state_rows = _rows(
                        self._cursor,
                        _query(
                            self._bundle.sql,
                            (
                                "INSERT INTO {} (namespace, project, environment, "
                                "revision, state_serial, state_checksum, state_json, "
                                "updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, "
                                "pg_catalog.clock_timestamp()) ON CONFLICT "
                                "(namespace, project, environment) DO NOTHING "
                                "RETURNING revision"
                            ),
                            self._schema,
                            "current_state",
                        ),
                        (
                            *params,
                            1,
                            replacement_state.serial,
                            replacement_checksum,
                            replacement_state_json,
                        ),
                    )
                else:
                    state_rows = _rows(
                        self._cursor,
                        _query(
                            self._bundle.sql,
                            (
                                "UPDATE {} SET revision = revision + 1, state_serial = %s, "
                                "state_checksum = %s, state_json = %s, updated_at = "
                                "pg_catalog.clock_timestamp() WHERE namespace = %s AND "
                                "project = %s AND environment = %s AND revision = %s AND "
                                "state_serial = %s AND state_checksum = %s AND state_json = %s "
                                "RETURNING revision"
                            ),
                            self._schema,
                            "current_state",
                        ),
                        (
                            replacement_state.serial,
                            replacement_checksum,
                            replacement_state_json,
                            *params,
                            expected_state_revision,
                            expected.state.state_serial,
                            state_checksum(expected.state.state),
                            expected_state_json,
                        ),
                    )
                if (
                    len(state_rows) != 1
                    or len(state_rows[0]) != 1
                    or type(state_rows[0][0]) is not int
                ):
                    raise StateBackendConflictError(
                        "PostgreSQL deployment ownership state changed during commit"
                    )
                new_state_revision = state_rows[0][0]
                expected_next = (
                    1 if expected_state_revision is None else expected_state_revision + 1
                )
                if new_state_revision != expected_next:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment ownership revision is invalid"
                    )
                history_rows = _rows(
                    self._cursor,
                    _query(
                        self._bundle.sql,
                        (
                            "INSERT INTO {} (namespace, project, environment, revision, "
                            "state_serial, state_checksum, state_json, operation_id, "
                            "recorded_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                            "pg_catalog.clock_timestamp()) ON CONFLICT "
                            "(namespace, project, environment, revision) DO NOTHING "
                            "RETURNING revision"
                        ),
                        self._schema,
                        "state_history",
                    ),
                    (
                        *params,
                        new_state_revision,
                        replacement_state.serial,
                        replacement_checksum,
                        replacement_state_json,
                        operation_id,
                    ),
                )
                if history_rows != [(new_state_revision,)]:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment ownership history is invalid"
                    )
                committed_state = replacement_state

            control_rows = _rows(
                self._cursor,
                _query(
                    self._bundle.sql,
                    (
                        "UPDATE {} SET revision = revision + 1, status = %s, "
                        "control_json = %s, updated_at = pg_catalog.clock_timestamp() "
                        "WHERE namespace = %s AND project = %s AND environment = %s "
                        "AND revision = %s AND status = %s AND control_json = %s "
                        "RETURNING revision"
                    ),
                    self._schema,
                    "operation_control",
                ),
                (
                    replacement.status,
                    replacement_control_json,
                    *params,
                    expected_control_revision,
                    expected.control.control.status,
                    expected_control_json,
                ),
            )
            if (
                len(control_rows) != 1
                or len(control_rows[0]) != 1
                or type(control_rows[0][0]) is not int
            ):
                raise StateBackendConflictError(
                    "PostgreSQL deployment operation control changed during transition"
                )
            new_control_revision = control_rows[0][0]
            if new_control_revision != expected_control_revision + 1:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation control revision is invalid"
                )

            operation_history_rows = _rows(
                self._cursor,
                _query(
                    self._bundle.sql,
                    (
                        "INSERT INTO {} (namespace, project, environment, operation_id, "
                        "event_index, event_kind, control_json, recorded_at) VALUES "
                        "(%s, %s, %s, %s, %s, %s, %s, "
                        "pg_catalog.clock_timestamp()) ON CONFLICT "
                        "(namespace, project, environment, operation_id, event_index) "
                        "DO NOTHING RETURNING event_index"
                    ),
                    self._schema,
                    "operation_history",
                ),
                (
                    *params,
                    operation_id,
                    event_index,
                    event_kind,
                    replacement_control_json,
                ),
            )
            if operation_history_rows != [(event_index,)]:
                raise StateBackendConflictError(
                    "PostgreSQL deployment operation history changed during transition"
                )
            expected_after = OperationSnapshot(
                state=StateObservation(
                    store=expected.state.store,
                    address=self._address,
                    state=committed_state,
                    revision=(
                        StateRevision.absent()
                        if new_state_revision is None
                        else _revision(
                            new_state_revision,
                            allow_zero=False,
                            label="ownership state",
                        )
                    ),
                ),
                control=ControlObservation(
                    control=replacement,
                    revision=_revision(
                        new_control_revision,
                        allow_zero=True,
                        label="operation control",
                    ),
                ),
            )
            self.check_lock()
            commit_attempted = True
            try:
                self._connection.commit()
            except BaseException:
                # The server may have committed before the client lost its
                # acknowledgement. Never replay DML: resolve only by an
                # independent direct-primary read of the complete postimage.
                self._lost = True
                transaction_started = False
                try:
                    verified = self._verify_transition(
                        expected=expected_after,
                        expected_before=expected,
                        operation_id=operation_id,
                        event_index=event_index,
                        event_kind=event_kind,
                        state_changed=mutate_state,
                    )
                except StateBackendUnknownCommitError as postimage_error:
                    # A preimage is not definitive while the original server
                    # backend could still finish COMMIT. Terminate that lost
                    # session, then require independent proof that it no
                    # longer exists or owns this exact advisory lock.
                    try:
                        self._cursor.close()
                    except BaseException:
                        pass
                    try:
                        self._connection.close()
                    except BaseException:
                        pass
                    try:
                        self._verify_exact_preimage(
                            expected=expected,
                            operation_id=operation_id,
                        )
                    except StateBackendUnknownCommitError:
                        raise postimage_error from None
                    commit_attempted = False
                    raise StateBackendLockLostError(
                        "PostgreSQL deployment state operation lock was lost",
                        operation_id=operation_id,
                    ) from None
                if event_kind != "succeeded":
                    commit_attempted = False
                    raise StateBackendLockLostError(
                        "PostgreSQL deployment state operation lock was lost",
                        operation_id=operation_id,
                    ) from None
                return verified, new_control_revision, new_state_revision, True
            transaction_started = False
        except (KeyboardInterrupt, SystemExit):
            if commit_attempted:
                self._lost = True
                raise StateBackendUnknownCommitError(
                    "PostgreSQL deployment state commit outcome is unknown",
                    operation_id=operation_id,
                ) from None
            if transaction_started:
                try:
                    self._connection.rollback()
                except Exception:
                    self._lost = True
            raise
        except StateBackendUnknownCommitError:
            raise
        except StateError:
            if commit_attempted:
                self._lost = True
                raise StateBackendUnknownCommitError(
                    "PostgreSQL deployment state commit outcome is unknown",
                    operation_id=operation_id,
                ) from None
            if transaction_started:
                try:
                    self._connection.rollback()
                except Exception:
                    self._lost = True
            raise
        except Exception:
            if commit_attempted:
                self._lost = True
                raise StateBackendUnknownCommitError(
                    "PostgreSQL deployment state commit outcome is unknown",
                    operation_id=operation_id,
                ) from None
            if transaction_started:
                try:
                    self._connection.rollback()
                except Exception:
                    self._lost = True
                    raise StateBackendLockLostError(
                        "PostgreSQL deployment state operation lock was lost",
                        operation_id=operation_id,
                    ) from None
            try:
                self.check_lock()
            except StateBackendLockLostError:
                raise
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state transition is unavailable"
            ) from None
        return expected_after, new_control_revision, new_state_revision, False

    def _verify_transition(
        self,
        *,
        expected: OperationSnapshot,
        expected_before: OperationSnapshot,
        operation_id: str,
        event_index: int,
        event_kind: str,
        state_changed: bool,
    ) -> OperationSnapshot:
        """Verify a postimage on a new connection without replaying writes."""
        bundle: _PsycopgBundle | None = None
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        transaction_started = False
        try:
            options = _dsn_tls_options(self._dsn)
            bundle = _load_psycopg()
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
                **options,
            )
            cursor = connection.cursor()
            _primary_pid(cursor)
            _begin_snapshot(cursor, self._lock_timeout_seconds)
            transaction_started = True
            current = _read_snapshot_transaction(
                cursor=cursor,
                bundle=bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=self._address,
            )
            self._validate_expected_snapshot(expected, current)
            history_rows = self._read_operation_history(
                cursor,
                bundle.sql,
                operation_id,
            )
            terminal_payload = _canonical_json(
                expected.control.control.to_dict(),
                label="operation history",
            )
            expected_history_rows = [
                *self._expected_history_rows(expected_before.control.control),
                (
                    event_index,
                    event_kind,
                    terminal_payload,
                    len(terminal_payload.encode("utf-8")),
                ),
            ]
            if history_rows != expected_history_rows:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )
            terminal_control: OperationControlState | None = None
            prior_control: OperationControlState | None = None
            for index, row in enumerate(history_rows):
                if (
                    len(row) != 4
                    or row[0] != index
                    or not isinstance(row[1], str)
                    or row[1] not in _OPERATION_EVENTS
                    or not isinstance(row[2], str)
                    or type(row[3]) is not int
                    or row[3] != len(row[2].encode("utf-8"))
                    or row[3] > POSTGRES_STATE_MAX_BYTES
                ):
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment operation history is invalid"
                    )
                raw_control = _strict_json(row[2], label="operation history")
                terminal_control = OperationControlState.from_dict(
                    raw_control,
                    expected_address=self._address,
                )
                kind = row[1]
                if index == 0:
                    if (
                        kind != "intent"
                        or terminal_control.status != "in_progress"
                        or terminal_control.intent is None
                        or terminal_control.intent.operation_id != operation_id
                        or terminal_control.progress
                    ):
                        raise StateBackendInvalidStateError(
                            "PostgreSQL deployment operation history is invalid"
                        )
                elif kind.startswith("progress_"):
                    expected_progress_kind = (
                        f"progress_{terminal_control.progress[-1].status}"
                        if terminal_control.progress
                        else ""
                    )
                    if (
                        terminal_control.status != "in_progress"
                        or terminal_control.intent is None
                        or terminal_control.intent.operation_id != operation_id
                        or len(terminal_control.progress) != index
                        or kind != expected_progress_kind
                        or prior_control is None
                        or terminal_control.intent != prior_control.intent
                        or terminal_control.progress[:-1] != prior_control.progress
                    ):
                        raise StateBackendInvalidStateError(
                            "PostgreSQL deployment operation history is invalid"
                        )
                elif index != event_index:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment operation history is invalid"
                    )
                prior_control = terminal_control
            if history_rows[-1][1] != event_kind or terminal_control != expected.control.control:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )

            state_history_rows = _rows(
                cursor,
                _query(
                    bundle.sql,
                    (
                        "SELECT revision, state_serial, state_checksum, state_json, "
                        "octet_length(state_json) FROM {} WHERE namespace = %s AND "
                        "project = %s AND environment = %s AND operation_id = %s "
                        "ORDER BY revision"
                    ),
                    self._schema,
                    "state_history",
                ),
                (
                    self._address.namespace,
                    self._address.project,
                    self._address.environment,
                    operation_id,
                ),
            )
            if state_changed:
                state_revision = _revision_number(
                    expected.state.revision,
                    allow_absent=False,
                )
                expected_state_json = _canonical_json(
                    expected.state.state.to_dict(),
                    label="ownership state",
                )
                expected_history = [
                    (
                        state_revision,
                        expected.state.state_serial,
                        state_checksum(expected.state.state),
                        expected_state_json,
                        len(expected_state_json.encode("utf-8")),
                    )
                ]
                if state_history_rows != expected_history:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment ownership history is invalid"
                    )
            elif state_history_rows:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment ownership history is invalid"
                )

            connection.rollback()
            transaction_started = False
            cursor.close()
            cursor = None
            connection.close()
            connection = None
            return current
        except BaseException:
            if transaction_started and connection is not None:
                try:
                    connection.rollback()
                except BaseException:
                    pass
            if cursor is not None:
                try:
                    cursor.close()
                except BaseException:
                    pass
            if connection is not None:
                try:
                    connection.close()
                except BaseException:
                    pass
            raise StateBackendUnknownCommitError(
                "PostgreSQL deployment state commit outcome is unknown",
                operation_id=operation_id,
            ) from None

    def _verify_exact_preimage(
        self,
        *,
        expected: OperationSnapshot,
        operation_id: str,
    ) -> None:
        """Prove an acknowledged-loss write definitely did not commit."""
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        transaction_started = False
        try:
            options = _dsn_tls_options(self._dsn)
            bundle = _load_psycopg()
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
                **options,
            )
            cursor = connection.cursor()
            _primary_pid(cursor)
            deadline = time.monotonic() + self._lock_timeout_seconds
            while True:
                writer_gone = _rows(
                    cursor,
                    (
                        "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity "
                        "WHERE pid = %s), NOT EXISTS (SELECT 1 FROM pg_catalog.pg_locks "
                        "AS l WHERE l.pid = %s AND l.locktype = 'advisory' AND l.granted "
                        "AND l.classid = (((%s::bigint >> 32) & 4294967295)::oid) "
                        "AND l.objid = ((%s::bigint & 4294967295)::oid) "
                        "AND l.objsubid = 1)"
                    ),
                    (
                        self._backend_pid,
                        self._backend_pid,
                        self._lock_key,
                        self._lock_key,
                    ),
                )
                if writer_gone in ([(True, False)], [(False, True)], [(True, True)]):
                    break
                if writer_gone != [(False, False)]:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment state writer status is invalid"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StateBackendUnavailableError(
                        "PostgreSQL deployment state writer termination is unverified"
                    )
                time.sleep(min(_LOCK_RETRY_INTERVAL_SECONDS, remaining))
            _begin_snapshot(cursor, self._lock_timeout_seconds)
            transaction_started = True
            current = _read_snapshot_transaction(
                cursor=cursor,
                bundle=bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=self._address,
            )
            self._validate_expected_snapshot(expected, current)
            if self._read_operation_history(cursor, bundle.sql, operation_id) != (
                self._expected_history_rows(expected.control.control)
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )
            state_history_rows = _rows(
                cursor,
                _query(
                    bundle.sql,
                    (
                        "SELECT revision FROM {} WHERE namespace = %s AND project = %s "
                        "AND environment = %s AND operation_id = %s ORDER BY revision"
                    ),
                    self._schema,
                    "state_history",
                ),
                (
                    self._address.namespace,
                    self._address.project,
                    self._address.environment,
                    operation_id,
                ),
            )
            if state_history_rows:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment ownership history is invalid"
                )
            connection.rollback()
            transaction_started = False
            cursor.close()
            cursor = None
            connection.close()
            connection = None
        except BaseException:
            if transaction_started and connection is not None:
                try:
                    connection.rollback()
                except BaseException:
                    pass
            if cursor is not None:
                try:
                    cursor.close()
                except BaseException:
                    pass
            if connection is not None:
                try:
                    connection.close()
                except BaseException:
                    pass
            raise StateBackendUnknownCommitError(
                "PostgreSQL deployment state commit outcome is unknown",
                operation_id=operation_id,
            ) from None

    def _transition(
        self,
        *,
        expected: OperationSnapshot,
        replacement: OperationControlState,
        operation_id: str,
        event_index: int,
        event_kind: str,
        replacement_state: LocalState | None = None,
        mutate_state: bool = False,
    ) -> OperationSnapshot:
        if event_kind not in _OPERATION_EVENTS:
            raise StateBackendInvalidStateError("PostgreSQL deployment operation event is invalid")
        self._last_attempted_operation_id = operation_id
        expected_after, _control_revision, _state_revision, already_verified = self._update_control(
            expected=expected,
            replacement=replacement,
            operation_id=operation_id,
            event_index=event_index,
            event_kind=event_kind,
            replacement_state=replacement_state,
            mutate_state=mutate_state,
        )
        if already_verified:
            return expected_after
        try:
            return self._verify_transition(
                expected=expected_after,
                expected_before=expected,
                operation_id=operation_id,
                event_index=event_index,
                event_kind=event_kind,
                state_changed=mutate_state,
            )
        except StateBackendUnknownCommitError:
            self._lost = True
            raise

    def begin_operation(
        self,
        observation: OperationSnapshot,
        intent: OperationIntent,
    ) -> OperationSnapshot:
        self._last_attempted_operation_id = intent.operation_id
        self.ensure_ready(observation)
        if (
            intent.prior_state_serial != observation.state.state_serial
            or intent.prior_state_checksum != state_checksum(observation.state.state)
        ):
            raise StateBackendConflictError(
                "operation intent does not match its prior state snapshot"
            )
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
        )
        active = self._transition(
            expected=observation,
            replacement=replacement,
            operation_id=intent.operation_id,
            event_index=0,
            event_kind="intent",
        )
        self._active_operation_id = intent.operation_id
        return active

    def record_progress(
        self,
        observation: OperationSnapshot,
        progress: OperationProgress,
    ) -> OperationSnapshot:
        intent = self._require_active(observation)
        self._last_attempted_operation_id = intent.operation_id
        if progress.operation_id != intent.operation_id:
            raise StateIdentityError("progress belongs to another deployment operation")
        replacement = OperationControlState(
            address=self._address,
            status="in_progress",
            intent=intent,
            progress=(*observation.control.control.progress, progress),
        )
        return self._transition(
            expected=observation,
            replacement=replacement,
            operation_id=intent.operation_id,
            event_index=len(replacement.progress),
            event_kind=f"progress_{progress.status}",
        )

    def mark_recovery_required(
        self,
        observation: OperationSnapshot,
        recovery: RecoveryRecord,
    ) -> OperationSnapshot:
        intent = self._require_active(observation)
        self._last_attempted_operation_id = intent.operation_id
        if recovery.operation_id != intent.operation_id:
            raise StateIdentityError("recovery record belongs to another deployment operation")
        replacement = OperationControlState(
            address=self._address,
            status="recovery_required",
            intent=intent,
            progress=observation.control.control.progress,
            recovery=recovery,
        )
        return self._transition(
            expected=observation,
            replacement=replacement,
            operation_id=intent.operation_id,
            event_index=len(replacement.progress) + 1,
            event_kind="recovery_required",
        )

    def clear_before_mutation(
        self,
        observation: OperationSnapshot,
    ) -> OperationSnapshot:
        intent = self._require_active(observation)
        self._last_attempted_operation_id = intent.operation_id
        if observation.control.control.progress:
            raise StateBackendRecoveryRequiredError(
                "deployment operation may have started mutation; explicit recovery is required"
            )
        cleared = self._transition(
            expected=observation,
            replacement=OperationControlState.clear(self._address),
            operation_id=intent.operation_id,
            event_index=1,
            event_kind="cleared_before_mutation",
        )
        self._active_operation_id = None
        return cleared

    def commit_operation(
        self,
        observation: OperationSnapshot,
        replacement: LocalState | None,
    ) -> OperationSnapshot:
        intent = self._require_completed(observation)
        self._last_attempted_operation_id = intent.operation_id
        committed = self._transition(
            expected=observation,
            replacement=OperationControlState.clear(self._address),
            operation_id=intent.operation_id,
            event_index=len(observation.control.control.progress) + 1,
            event_kind="succeeded",
            replacement_state=replacement,
            mutate_state=replacement is not None,
        )
        self._finalized = True
        self._finalized_operation_id = intent.operation_id
        self._active_operation_id = None
        return committed


class _PostgresStateOperationContext(AbstractContextManager[_PostgresStateReadOperation]):
    __slots__ = (
        "_address",
        "_bundle",
        "_connection",
        "_cursor",
        "_dsn",
        "_entered",
        "_lock_acquired",
        "_lock_key",
        "_lock_timeout_seconds",
        "_operation",
        "_schema",
    )

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
        address: StateAddress,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds
        self._address = address
        self._bundle: _PsycopgBundle | None = None
        self._connection: _Connection | None = None
        self._cursor: _Cursor | None = None
        self._lock_key: int | None = None
        self._lock_acquired = False
        self._entered = False
        self._operation: _PostgresStateReadOperation | None = None

    def __enter__(self) -> _PostgresStateReadOperation:
        if self._entered:
            raise RuntimeError("PostgreSQL state operation context is not reusable")
        self._entered = True
        try:
            options = _dsn_tls_options(self._dsn)
            self._bundle = _load_psycopg()
            self._connection = self._bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
                **options,
            )
            self._cursor = self._connection.cursor()
            self._cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, false)",
                (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
            )
            backend_pid = _primary_pid(self._cursor)

            _begin_snapshot(self._cursor, self._lock_timeout_seconds)
            status = PostgresStateAdministration(
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
            )._read_status(self._cursor, self._bundle.sql, self._address)
            if status.store_status != "ready" or status.address_status != "registered":
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state address is not registered"
                )
            lock_key = _registered_advisory_lock_key(
                self._cursor,
                self._bundle.sql,
                self._schema,
                self._address,
            )
            if lock_key is None or lock_key != _advisory_lock_key(self._address):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state address is invalid"
                )
            self._connection.rollback()
            self._lock_key = lock_key

            deadline = time.monotonic() + self._lock_timeout_seconds
            while True:
                acquired = _rows(
                    self._cursor,
                    (
                        "SELECT pg_catalog.pg_backend_pid(), "
                        "pg_catalog.pg_is_in_recovery(), CASE WHEN "
                        "pg_catalog.pg_backend_pid() = %s AND "
                        "pg_catalog.pg_is_in_recovery() IS FALSE THEN "
                        "pg_catalog.pg_try_advisory_lock(%s) ELSE FALSE END"
                    ),
                    (backend_pid, lock_key),
                )
                if acquired == [(backend_pid, False, True)]:
                    self._lock_acquired = True
                    break
                if acquired != [(backend_pid, False, False)]:
                    raise StateBackendUnavailableError(
                        "PostgreSQL deployment state operation lock is unavailable"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StateBackendLockTimeoutError(
                        "PostgreSQL deployment state operation lock timed out"
                    )
                time.sleep(min(_LOCK_RETRY_INTERVAL_SECONDS, remaining))

            self._operation = _PostgresStateReadOperation(
                connection=self._connection,
                cursor=self._cursor,
                bundle=self._bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=self._address,
                lock_key=lock_key,
                backend_pid=backend_pid,
            )
            self._operation.check_lock()
            return self._operation
        except (StateBackendInvalidStateError, StateBackendLockTimeoutError):
            self._cleanup(suppress_failure=True)
            raise
        except (KeyboardInterrupt, SystemExit):
            self._cleanup(suppress_failure=True)
            raise
        except Exception:
            self._cleanup(suppress_failure=True)
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state operation lock is unavailable"
            ) from None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        del exc_value, traceback
        finalized = self._operation is not None and self._operation.finalized
        cleanup_failed = self._cleanup(suppress_failure=exc_type is not None)
        if cleanup_failed and exc_type is None:
            if finalized:
                raise StateBackendReleaseAfterCommitError(
                    "PostgreSQL deployment state committed, but operation authority "
                    "release was not verified",
                    operation_id=(
                        self._operation.finalized_operation_id
                        if self._operation is not None
                        else None
                    ),
                ) from None
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock release was not verified",
                operation_id=(self._operation.last_operation_id if self._operation else None),
            ) from None
        return None

    def _cleanup(self, *, suppress_failure: bool) -> bool:
        release_failed = False
        cursor = self._cursor
        connection = self._connection
        lock_session_verified = self._operation is None or not self._operation._lost
        if self._lock_acquired and not lock_session_verified:
            release_failed = True
        if (
            cursor is not None
            and self._lock_acquired
            and self._lock_key is not None
            and lock_session_verified
        ):
            try:
                released = _rows(
                    cursor,
                    (
                        "SELECT pg_catalog.pg_backend_pid(), "
                        "pg_catalog.pg_is_in_recovery(), "
                        "CASE WHEN pg_catalog.pg_backend_pid() = %s AND "
                        "pg_catalog.pg_is_in_recovery() IS FALSE THEN "
                        "pg_catalog.pg_advisory_unlock(%s) ELSE FALSE END"
                    ),
                    (
                        self._operation.backend_pid if self._operation is not None else None,
                        self._lock_key,
                    ),
                )
                expected_pid = self._operation.backend_pid if self._operation is not None else None
                if released != [(expected_pid, False, True)]:
                    release_failed = True
            except BaseException:
                release_failed = True
        self._lock_acquired = False
        if cursor is not None:
            try:
                cursor.close()
            except BaseException:
                pass
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                # A successfully verified explicit unlock is authoritative;
                # later client-object cleanup cannot make it ambiguous.
                pass
        self._cursor = None
        self._connection = None
        if release_failed and not suppress_failure:
            return True
        return release_failed


class PrivatePostgresStateReadBackend:
    """Direct-construction-only owner-v1 read/lock/mutation scaffold."""

    __slots__ = ("_dsn", "_lock_timeout_seconds", "_schema")

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
    ) -> None:
        if type(lock_timeout_seconds) is not int or lock_timeout_seconds < 0:
            raise ValueError("lock_timeout_seconds must be a non-negative integer")
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds

    def describe(self) -> StateStoreIdentity:
        """Read the private store identity without selecting it in the factory."""
        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        identity: StateStoreIdentity | None = None
        invalid = False
        unavailable = False
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                **options,
            )
            cursor = connection.cursor()
            _begin_snapshot(cursor, self._lock_timeout_seconds)
            _primary_pid(cursor)
            rows = _rows(
                cursor,
                _query(
                    bundle.sql,
                    (
                        "SELECT store_id::text, schema_version FROM {} "
                        "WHERE singleton IS TRUE LIMIT 2"
                    ),
                    self._schema,
                    "store_metadata",
                ),
            )
            if (
                len(rows) != 1
                or len(rows[0]) != 2
                or not isinstance(rows[0][0], str)
                or rows[0][1] != 1
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state metadata is invalid"
                )
            identity = StateStoreIdentity(backend="postgres", store_id=rows[0][0])
        except (StateBackendInvalidStateError, StateError):
            invalid = True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            unavailable = True
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    unavailable = True
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    unavailable = True
                try:
                    connection.close()
                except Exception:
                    unavailable = True
        if invalid:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state metadata is invalid"
            ) from None
        if unavailable or identity is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state is unavailable"
            ) from None
        return identity

    def read_snapshot(self, address: StateAddress) -> OperationSnapshot:
        """Read ownership and control together without acquiring authority."""
        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        snapshot: OperationSnapshot | None = None
        invalid = False
        unavailable = False
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                **options,
            )
            cursor = connection.cursor()
            _begin_snapshot(cursor, self._lock_timeout_seconds)
            _primary_pid(cursor)
            snapshot = _read_snapshot_transaction(
                cursor=cursor,
                bundle=bundle,
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
                address=address,
            )
        except StateBackendInvalidStateError:
            invalid = True
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            unavailable = True
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    unavailable = True
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    unavailable = True
                try:
                    connection.close()
                except Exception:
                    unavailable = True
        if invalid:
            raise StateBackendInvalidStateError("PostgreSQL deployment state is invalid") from None
        if unavailable or snapshot is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state is unavailable"
            ) from None
        return snapshot

    def read(self, address: StateAddress) -> StateObservation:
        return self.read_snapshot(address).state

    def read_control(self, address: StateAddress) -> ControlObservation:
        return self.read_snapshot(address).control

    def operation(
        self,
        address: StateAddress,
    ) -> AbstractContextManager[_PostgresStateReadOperation]:
        """Acquire the private test-only session lock for one address."""
        return _PostgresStateOperationContext(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
            address=address,
        )
