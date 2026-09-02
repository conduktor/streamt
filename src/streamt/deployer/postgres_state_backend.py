"""Private PostgreSQL ownership reads and session-affine lock scaffolding.

This module is deliberately unreachable from deployment-state configuration,
the ordinary backend factory, and every CLI command.  It exists so the frozen
version-one catalog can exercise the read and lock portions of the future
remote backend with isolated schema-owner credentials in focused tests.  It
does not implement any state or operation-control mutation method.

Psycopg remains optional: construction imports nothing from the driver, and a
connection loads it lazily through the existing PostgreSQL administration
boundary.
"""

from __future__ import annotations

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
    _strict_json,
)
from streamt.deployer.state import LocalState, StateError
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationControlState,
    OperationSnapshot,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockLostError,
    StateBackendLockTimeoutError,
    StateBackendUnavailableError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

_CONNECT_TIMEOUT_SECONDS = 10
_STATEMENT_TIMEOUT_MILLISECONDS = 30_000
_LOCK_RETRY_INTERVAL_SECONDS = 0.05
_REVISION_PREFIX = "postgres-v1:"


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
    control_row = _one_or_none(
        _rows(
            cursor,
            _query(
                bundle.sql,
                (
                    "SELECT revision, status, control_json, octet_length(control_json) "
                    "FROM {} WHERE namespace = %s AND project = %s "
                    "AND environment = %s LIMIT 2"
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
                    "AND project = %s AND environment = %s LIMIT 2"
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


class _PostgresStateReadOperation:
    """One test-only physical session holding the registered advisory lock."""

    __slots__ = (
        "_address",
        "_backend_pid",
        "_bundle",
        "_connection",
        "_cursor",
        "_dsn",
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
        self._lock_key = lock_key
        self._backend_pid = backend_pid
        self._lost = False

    @property
    def backend_pid(self) -> int:
        """Expose only the numeric server PID for focused failure injection."""
        return self._backend_pid

    def check_lock(self) -> None:
        """Verify primary, physical-session continuity, and exact lock ownership."""
        if self._lost:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost"
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
                "PostgreSQL deployment state operation lock was lost"
            ) from None
        if rows != [(self._backend_pid, False, True)]:
            self._lost = True
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost"
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
                "PostgreSQL deployment state operation lock was lost"
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
                "PostgreSQL deployment state operation lock was lost"
            ) from None
        if snapshot is None:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock was lost"
            ) from None
        self.check_lock()
        return snapshot


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
                    "SELECT pg_catalog.pg_try_advisory_lock(%s)",
                    (lock_key,),
                )
                if acquired == [(True,)]:
                    self._lock_acquired = True
                    break
                if acquired != [(False,)]:
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
        cleanup_failed = self._cleanup(suppress_failure=exc_type is not None)
        if cleanup_failed and exc_type is None:
            raise StateBackendLockLostError(
                "PostgreSQL deployment state operation lock release was not verified"
            ) from None
        return None

    def _cleanup(self, *, suppress_failure: bool) -> bool:
        failed = False
        cursor = self._cursor
        connection = self._connection
        if cursor is not None and self._lock_acquired and self._lock_key is not None:
            try:
                released = _rows(
                    cursor,
                    "SELECT pg_catalog.pg_advisory_unlock(%s)",
                    (self._lock_key,),
                )
                if released != [(True,)]:
                    failed = True
            except BaseException:
                failed = True
            self._lock_acquired = False
        if cursor is not None:
            try:
                cursor.close()
            except BaseException:
                failed = True
        if connection is not None:
            try:
                connection.close()
            except BaseException:
                failed = True
        self._cursor = None
        self._connection = None
        if failed and not suppress_failure:
            return True
        return failed


class PrivatePostgresStateReadBackend:
    """Direct-construction-only v1 read/lock scaffold with no mutations."""

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
