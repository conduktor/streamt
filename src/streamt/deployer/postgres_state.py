"""Narrow PostgreSQL deployment-state administration.

This module is deliberately not a ``DeploymentStateBackend``.  It can inspect,
transiently probe, or explicitly initialize a store for administrative state
commands, but it cannot authorize an ordinary plan, apply, adopt, state
mutation, or durable operation lock.

Psycopg is an optional dependency and is imported only when an administrative
operation opens a connection.  Provider exceptions are translated to fixed,
secret-neutral errors and are never chained into user-visible command failures.
"""

from __future__ import annotations

import hashlib
import importlib
import ipaddress
import json
import os
import shlex
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol, cast
from urllib.parse import parse_qs, urlsplit

from streamt.core.deployment_state import (
    DeploymentStateConfig,
    PostgresDeploymentStateConfig,
)
from streamt.deployer.state import LocalState, StateError, StateFormatError
from streamt.deployer.state_backend import (
    OperationControlState,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    state_checksum,
)

POSTGRES_SCHEMA_VERSION = 1
POSTGRES_STATE_MAX_BYTES = 10 * 1024 * 1024
_CONNECT_TIMEOUT_SECONDS = 10
_STATEMENT_TIMEOUT_MILLISECONDS = 30_000
_LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES = (
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGSERVICE",
    "PGSERVICEFILE",
)


class _Composable(Protocol):
    def format(self, *args: object) -> object: ...


class _SqlModule(Protocol):
    SQL: Callable[[str], _Composable]
    Identifier: Callable[..., object]


class _Cursor(Protocol):
    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object: ...

    def fetchall(self) -> object: ...

    def close(self) -> None: ...


class _Connection(Protocol):
    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class _DriverModule(Protocol):
    def connect(self, conninfo: str, **kwargs: object) -> _Connection: ...


@dataclass(frozen=True)
class _PsycopgBundle:
    driver: _DriverModule
    sql: _SqlModule


def _load_psycopg() -> _PsycopgBundle:
    """Load Psycopg lazily so the base package stays operational without it."""
    try:
        driver = importlib.import_module("psycopg")
        sql_module = importlib.import_module("psycopg.sql")
    except (ImportError, ModuleNotFoundError):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state requires the optional postgres package extra"
        ) from None
    return _PsycopgBundle(
        driver=cast(_DriverModule, driver),
        sql=cast(_SqlModule, sql_module),
    )


# The version-one catalog is intentionally frozen before the first writer is
# exposed.  Defaults are absent: initialization and later mutations bind every
# durable value explicitly.
_EXPECTED_COLUMNS: tuple[tuple[str, str, str, str, str, object], ...] = (
    ("store_metadata", "singleton", "boolean", "bool", "NO", None),
    ("store_metadata", "store_id", "uuid", "uuid", "NO", None),
    ("store_metadata", "schema_version", "integer", "int4", "NO", None),
    (
        "store_metadata",
        "initialized_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("schema_migrations", "schema_version", "integer", "int4", "NO", None),
    ("schema_migrations", "migration_name", "text", "text", "NO", None),
    ("schema_migrations", "migration_checksum", "text", "text", "NO", None),
    (
        "schema_migrations",
        "applied_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("state_addresses", "namespace", "text", "text", "NO", None),
    ("state_addresses", "project", "text", "text", "NO", None),
    ("state_addresses", "environment", "text", "text", "NO", None),
    ("state_addresses", "address_uri", "text", "text", "NO", None),
    ("state_addresses", "advisory_lock_key", "bigint", "int8", "NO", None),
    (
        "state_addresses",
        "registered_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("current_state", "namespace", "text", "text", "NO", None),
    ("current_state", "project", "text", "text", "NO", None),
    ("current_state", "environment", "text", "text", "NO", None),
    ("current_state", "revision", "bigint", "int8", "NO", None),
    ("current_state", "state_serial", "bigint", "int8", "NO", None),
    ("current_state", "state_checksum", "text", "text", "NO", None),
    ("current_state", "state_json", "text", "text", "NO", None),
    (
        "current_state",
        "updated_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("operation_control", "namespace", "text", "text", "NO", None),
    ("operation_control", "project", "text", "text", "NO", None),
    ("operation_control", "environment", "text", "text", "NO", None),
    ("operation_control", "revision", "bigint", "int8", "NO", None),
    ("operation_control", "status", "text", "text", "NO", None),
    ("operation_control", "control_json", "text", "text", "NO", None),
    (
        "operation_control",
        "updated_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("state_history", "namespace", "text", "text", "NO", None),
    ("state_history", "project", "text", "text", "NO", None),
    ("state_history", "environment", "text", "text", "NO", None),
    ("state_history", "revision", "bigint", "int8", "NO", None),
    ("state_history", "state_serial", "bigint", "int8", "NO", None),
    ("state_history", "state_checksum", "text", "text", "NO", None),
    ("state_history", "state_json", "text", "text", "NO", None),
    ("state_history", "operation_id", "uuid", "uuid", "YES", None),
    (
        "state_history",
        "recorded_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
    ("operation_history", "namespace", "text", "text", "NO", None),
    ("operation_history", "project", "text", "text", "NO", None),
    ("operation_history", "environment", "text", "text", "NO", None),
    ("operation_history", "operation_id", "uuid", "uuid", "NO", None),
    ("operation_history", "event_index", "integer", "int4", "NO", None),
    ("operation_history", "event_kind", "text", "text", "NO", None),
    ("operation_history", "control_json", "text", "text", "NO", None),
    (
        "operation_history",
        "recorded_at",
        "timestamp with time zone",
        "timestamptz",
        "NO",
        None,
    ),
)
_EXPECTED_COLUMNS = tuple(sorted(_EXPECTED_COLUMNS, key=lambda column: column[0]))
_EXPECTED_TABLES = tuple(sorted({column[0] for column in _EXPECTED_COLUMNS}))

# table, constraint name, kind, local columns, referenced table, referenced
# columns, and normalized check expression.  Names are explicit in schema v1;
# this makes missing PK/unique/FK/check enforcement a compatibility failure.
_EXPECTED_CONSTRAINTS: tuple[tuple[str, str, str, str, str | None, str | None, str | None], ...] = (
    tuple(
        sorted(
            (
                ("store_metadata", "store_metadata_pkey", "p", "singleton", None, None, None),
                (
                    "store_metadata",
                    "store_metadata_store_id_key",
                    "u",
                    "store_id",
                    None,
                    None,
                    None,
                ),
                (
                    "store_metadata",
                    "store_metadata_singleton_check",
                    "c",
                    "",
                    None,
                    None,
                    "singleton",
                ),
                (
                    "store_metadata",
                    "store_metadata_schema_version_check",
                    "c",
                    "",
                    None,
                    None,
                    "schema_version = 1",
                ),
                (
                    "schema_migrations",
                    "schema_migrations_pkey",
                    "p",
                    "schema_version",
                    None,
                    None,
                    None,
                ),
                (
                    "schema_migrations",
                    "schema_migrations_migration_name_key",
                    "u",
                    "migration_name",
                    None,
                    None,
                    None,
                ),
                (
                    "schema_migrations",
                    "schema_migrations_schema_version_check",
                    "c",
                    "",
                    None,
                    None,
                    "schema_version > 0",
                ),
                (
                    "schema_migrations",
                    "schema_migrations_checksum_check",
                    "c",
                    "",
                    None,
                    None,
                    "migration_checksum ~ '^sha256:[0-9a-f]{64}$'",
                ),
                (
                    "state_addresses",
                    "state_addresses_pkey",
                    "p",
                    "namespace,project,environment",
                    None,
                    None,
                    None,
                ),
                (
                    "state_addresses",
                    "state_addresses_address_uri_key",
                    "u",
                    "address_uri",
                    None,
                    None,
                    None,
                ),
                (
                    "state_addresses",
                    "state_addresses_advisory_lock_key_key",
                    "u",
                    "advisory_lock_key",
                    None,
                    None,
                    None,
                ),
                (
                    "state_addresses",
                    "state_addresses_namespace_check",
                    "c",
                    "",
                    None,
                    None,
                    "namespace <> '' AND strpos(namespace, '/') = 0",
                ),
                (
                    "state_addresses",
                    "state_addresses_project_check",
                    "c",
                    "",
                    None,
                    None,
                    "project <> '' AND strpos(project, '/') = 0",
                ),
                (
                    "state_addresses",
                    "state_addresses_environment_check",
                    "c",
                    "",
                    None,
                    None,
                    "environment ~ '^[A-Za-z0-9][A-Za-z0-9-]*$'",
                ),
                (
                    "state_addresses",
                    "state_addresses_uri_check",
                    "c",
                    "",
                    None,
                    None,
                    "address_uri = 'streamt-state://' || namespace || '/' || project || '/' || environment",
                ),
                (
                    "current_state",
                    "current_state_pkey",
                    "p",
                    "namespace,project,environment",
                    None,
                    None,
                    None,
                ),
                (
                    "current_state",
                    "current_state_address_fkey",
                    "f",
                    "namespace,project,environment",
                    "state_addresses",
                    "namespace,project,environment",
                    None,
                ),
                (
                    "current_state",
                    "current_state_revision_check",
                    "c",
                    "",
                    None,
                    None,
                    "revision >= 1",
                ),
                (
                    "current_state",
                    "current_state_serial_check",
                    "c",
                    "",
                    None,
                    None,
                    "state_serial >= 1",
                ),
                (
                    "current_state",
                    "current_state_checksum_check",
                    "c",
                    "",
                    None,
                    None,
                    "state_checksum ~ '^sha256:[0-9a-f]{64}$'",
                ),
                (
                    "current_state",
                    "current_state_size_check",
                    "c",
                    "",
                    None,
                    None,
                    f"octet_length(state_json) <= {POSTGRES_STATE_MAX_BYTES}",
                ),
                (
                    "operation_control",
                    "operation_control_pkey",
                    "p",
                    "namespace,project,environment",
                    None,
                    None,
                    None,
                ),
                (
                    "operation_control",
                    "operation_control_address_fkey",
                    "f",
                    "namespace,project,environment",
                    "state_addresses",
                    "namespace,project,environment",
                    None,
                ),
                (
                    "operation_control",
                    "operation_control_revision_check",
                    "c",
                    "",
                    None,
                    None,
                    "revision >= 0",
                ),
                (
                    "operation_control",
                    "operation_control_status_check",
                    "c",
                    "",
                    None,
                    None,
                    "status = ANY (ARRAY['clear', 'in_progress', 'recovery_required'])",
                ),
                (
                    "operation_control",
                    "operation_control_size_check",
                    "c",
                    "",
                    None,
                    None,
                    f"octet_length(control_json) <= {POSTGRES_STATE_MAX_BYTES}",
                ),
                (
                    "state_history",
                    "state_history_pkey",
                    "p",
                    "namespace,project,environment,revision",
                    None,
                    None,
                    None,
                ),
                (
                    "state_history",
                    "state_history_address_fkey",
                    "f",
                    "namespace,project,environment",
                    "state_addresses",
                    "namespace,project,environment",
                    None,
                ),
                (
                    "state_history",
                    "state_history_revision_check",
                    "c",
                    "",
                    None,
                    None,
                    "revision >= 1",
                ),
                (
                    "state_history",
                    "state_history_serial_check",
                    "c",
                    "",
                    None,
                    None,
                    "state_serial >= 1",
                ),
                (
                    "state_history",
                    "state_history_checksum_check",
                    "c",
                    "",
                    None,
                    None,
                    "state_checksum ~ '^sha256:[0-9a-f]{64}$'",
                ),
                (
                    "state_history",
                    "state_history_size_check",
                    "c",
                    "",
                    None,
                    None,
                    f"octet_length(state_json) <= {POSTGRES_STATE_MAX_BYTES}",
                ),
                (
                    "operation_history",
                    "operation_history_pkey",
                    "p",
                    "namespace,project,environment,operation_id,event_index",
                    None,
                    None,
                    None,
                ),
                (
                    "operation_history",
                    "operation_history_address_fkey",
                    "f",
                    "namespace,project,environment",
                    "state_addresses",
                    "namespace,project,environment",
                    None,
                ),
                (
                    "operation_history",
                    "operation_history_event_index_check",
                    "c",
                    "",
                    None,
                    None,
                    "event_index >= 0",
                ),
                (
                    "operation_history",
                    "operation_history_size_check",
                    "c",
                    "",
                    None,
                    None,
                    f"octet_length(control_json) <= {POSTGRES_STATE_MAX_BYTES}",
                ),
            )
        )
    )
)

# table, index name, unique, primary, ordered key columns.  Constraint-backed
# indexes are still checked explicitly so a hand-built partial schema cannot be
# reported as ready.
_EXPECTED_INDEXES: tuple[tuple[str, str, bool, bool, str], ...] = tuple(
    sorted(
        (
            ("store_metadata", "store_metadata_pkey", True, True, "singleton"),
            (
                "store_metadata",
                "store_metadata_store_id_key",
                True,
                False,
                "store_id",
            ),
            (
                "schema_migrations",
                "schema_migrations_pkey",
                True,
                True,
                "schema_version",
            ),
            (
                "schema_migrations",
                "schema_migrations_migration_name_key",
                True,
                False,
                "migration_name",
            ),
            (
                "state_addresses",
                "state_addresses_pkey",
                True,
                True,
                "namespace,project,environment",
            ),
            (
                "state_addresses",
                "state_addresses_address_uri_key",
                True,
                False,
                "address_uri",
            ),
            (
                "state_addresses",
                "state_addresses_advisory_lock_key_key",
                True,
                False,
                "advisory_lock_key",
            ),
            (
                "current_state",
                "current_state_pkey",
                True,
                True,
                "namespace,project,environment",
            ),
            (
                "operation_control",
                "operation_control_pkey",
                True,
                True,
                "namespace,project,environment",
            ),
            (
                "state_history",
                "state_history_pkey",
                True,
                True,
                "namespace,project,environment,revision",
            ),
            (
                "operation_history",
                "operation_history_pkey",
                True,
                True,
                "namespace,project,environment,operation_id,event_index",
            ),
        )
    )
)
_SCHEMA_CONTRACT_BYTES = json.dumps(
    {
        "columns": _EXPECTED_COLUMNS,
        "constraints": _EXPECTED_CONSTRAINTS,
        "indexes": _EXPECTED_INDEXES,
    },
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")
POSTGRES_SCHEMA_V1_CHECKSUM = "sha256:" + hashlib.sha256(_SCHEMA_CONTRACT_BYTES).hexdigest()
_EXPECTED_MIGRATION = (
    POSTGRES_SCHEMA_VERSION,
    "schema-v1",
    POSTGRES_SCHEMA_V1_CHECKSUM,
)

# Each template's first placeholder is the table being created.  Templates
# with a second placeholder reference the qualified state-address table.  The
# statements intentionally have no IF NOT EXISTS or defaults: initialization
# first proves that the schema is empty, and every durable value is explicit.
_SCHEMA_V1_DDL: tuple[tuple[str, str, str | None], ...] = (
    (
        "store_metadata",
        """CREATE TABLE {} (
            singleton boolean NOT NULL,
            store_id uuid NOT NULL,
            schema_version integer NOT NULL,
            initialized_at timestamp with time zone NOT NULL,
            CONSTRAINT store_metadata_pkey PRIMARY KEY (singleton),
            CONSTRAINT store_metadata_store_id_key UNIQUE (store_id),
            CONSTRAINT store_metadata_singleton_check CHECK (singleton),
            CONSTRAINT store_metadata_schema_version_check CHECK (schema_version = 1)
        )""",
        None,
    ),
    (
        "schema_migrations",
        """CREATE TABLE {} (
            schema_version integer NOT NULL,
            migration_name text NOT NULL,
            migration_checksum text NOT NULL,
            applied_at timestamp with time zone NOT NULL,
            CONSTRAINT schema_migrations_pkey PRIMARY KEY (schema_version),
            CONSTRAINT schema_migrations_migration_name_key UNIQUE (migration_name),
            CONSTRAINT schema_migrations_schema_version_check CHECK (schema_version > 0),
            CONSTRAINT schema_migrations_checksum_check
                CHECK (migration_checksum ~ '^sha256:[0-9a-f]{{64}}$')
        )""",
        None,
    ),
    (
        "state_addresses",
        """CREATE TABLE {} (
            namespace text NOT NULL,
            project text NOT NULL,
            environment text NOT NULL,
            address_uri text NOT NULL,
            advisory_lock_key bigint NOT NULL,
            registered_at timestamp with time zone NOT NULL,
            CONSTRAINT state_addresses_pkey
                PRIMARY KEY (namespace, project, environment),
            CONSTRAINT state_addresses_address_uri_key UNIQUE (address_uri),
            CONSTRAINT state_addresses_advisory_lock_key_key UNIQUE (advisory_lock_key),
            CONSTRAINT state_addresses_namespace_check
                CHECK (namespace <> '' AND strpos(namespace, '/') = 0),
            CONSTRAINT state_addresses_project_check
                CHECK (project <> '' AND strpos(project, '/') = 0),
            CONSTRAINT state_addresses_environment_check
                CHECK (environment ~ '^[A-Za-z0-9][A-Za-z0-9-]*$'),
            CONSTRAINT state_addresses_uri_check
                CHECK (
                    address_uri = 'streamt-state://' || namespace || '/' || project ||
                        '/' || environment
                )
        )""",
        None,
    ),
    (
        "current_state",
        """CREATE TABLE {} (
            namespace text NOT NULL,
            project text NOT NULL,
            environment text NOT NULL,
            revision bigint NOT NULL,
            state_serial bigint NOT NULL,
            state_checksum text NOT NULL,
            state_json text NOT NULL,
            updated_at timestamp with time zone NOT NULL,
            CONSTRAINT current_state_pkey
                PRIMARY KEY (namespace, project, environment),
            CONSTRAINT current_state_address_fkey
                FOREIGN KEY (namespace, project, environment) REFERENCES {}
                    (namespace, project, environment)
                    MATCH SIMPLE ON DELETE RESTRICT ON UPDATE NO ACTION,
            CONSTRAINT current_state_revision_check CHECK (revision >= 1),
            CONSTRAINT current_state_serial_check CHECK (state_serial >= 1),
            CONSTRAINT current_state_checksum_check
                CHECK (state_checksum ~ '^sha256:[0-9a-f]{{64}}$'),
            CONSTRAINT current_state_size_check
                CHECK (octet_length(state_json) <= 10485760)
        )""",
        "state_addresses",
    ),
    (
        "operation_control",
        """CREATE TABLE {} (
            namespace text NOT NULL,
            project text NOT NULL,
            environment text NOT NULL,
            revision bigint NOT NULL,
            status text NOT NULL,
            control_json text NOT NULL,
            updated_at timestamp with time zone NOT NULL,
            CONSTRAINT operation_control_pkey
                PRIMARY KEY (namespace, project, environment),
            CONSTRAINT operation_control_address_fkey
                FOREIGN KEY (namespace, project, environment) REFERENCES {}
                    (namespace, project, environment)
                    MATCH SIMPLE ON DELETE RESTRICT ON UPDATE NO ACTION,
            CONSTRAINT operation_control_revision_check CHECK (revision >= 0),
            CONSTRAINT operation_control_status_check
                CHECK (
                    status = ANY (
                        ARRAY['clear', 'in_progress', 'recovery_required']
                    )
                ),
            CONSTRAINT operation_control_size_check
                CHECK (octet_length(control_json) <= 10485760)
        )""",
        "state_addresses",
    ),
    (
        "state_history",
        """CREATE TABLE {} (
            namespace text NOT NULL,
            project text NOT NULL,
            environment text NOT NULL,
            revision bigint NOT NULL,
            state_serial bigint NOT NULL,
            state_checksum text NOT NULL,
            state_json text NOT NULL,
            operation_id uuid,
            recorded_at timestamp with time zone NOT NULL,
            CONSTRAINT state_history_pkey
                PRIMARY KEY (namespace, project, environment, revision),
            CONSTRAINT state_history_address_fkey
                FOREIGN KEY (namespace, project, environment) REFERENCES {}
                    (namespace, project, environment)
                    MATCH SIMPLE ON DELETE RESTRICT ON UPDATE NO ACTION,
            CONSTRAINT state_history_revision_check CHECK (revision >= 1),
            CONSTRAINT state_history_serial_check CHECK (state_serial >= 1),
            CONSTRAINT state_history_checksum_check
                CHECK (state_checksum ~ '^sha256:[0-9a-f]{{64}}$'),
            CONSTRAINT state_history_size_check
                CHECK (octet_length(state_json) <= 10485760)
        )""",
        "state_addresses",
    ),
    (
        "operation_history",
        """CREATE TABLE {} (
            namespace text NOT NULL,
            project text NOT NULL,
            environment text NOT NULL,
            operation_id uuid NOT NULL,
            event_index integer NOT NULL,
            event_kind text NOT NULL,
            control_json text NOT NULL,
            recorded_at timestamp with time zone NOT NULL,
            CONSTRAINT operation_history_pkey
                PRIMARY KEY (
                    namespace, project, environment, operation_id, event_index
                ),
            CONSTRAINT operation_history_address_fkey
                FOREIGN KEY (namespace, project, environment) REFERENCES {}
                    (namespace, project, environment)
                    MATCH SIMPLE ON DELETE RESTRICT ON UPDATE NO ACTION,
            CONSTRAINT operation_history_event_index_check CHECK (event_index >= 0),
            CONSTRAINT operation_history_size_check
                CHECK (octet_length(control_json) <= 10485760)
        )""",
        "state_addresses",
    ),
)


StoreStatus = Literal["uninitialized", "ready"]
AddressStatus = Literal["unregistered", "registered"]
OwnershipStatus = Literal["unregistered", "absent", "present"]
LockAvailability = Literal["available", "busy", "unregistered"]


@dataclass(frozen=True)
class SafeOperationStatus:
    """Secret-free operation-control fields exposed by status."""

    status: str
    operation_id: str | None
    kind: str | None
    failure_code: str | None
    last_completed_action_index: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "operation_id": self.operation_id,
            "kind": self.kind,
            "failure_code": self.failure_code,
            "last_completed_action_index": self.last_completed_action_index,
        }


@dataclass(frozen=True)
class PostgresStateStatus:
    """Safe, read-only administrative status for one canonical address."""

    store_status: StoreStatus
    store_id: str | None
    schema_version: int | None
    address: StateAddress
    address_status: AddressStatus
    state_status: OwnershipStatus
    state_serial: int | None
    state_checksum: str | None
    operation_status: SafeOperationStatus | None

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": "postgres",
            "store_status": self.store_status,
            "store_id": self.store_id,
            "schema_version": self.schema_version,
            "address": self.address.uri,
            "address_status": self.address_status,
            "state_status": self.state_status,
            "state_serial": self.state_serial,
            "state_checksum": self.state_checksum,
            "operation_status": (
                self.operation_status.to_dict() if self.operation_status is not None else None
            ),
            "mutation_status": "disabled",
        }


@dataclass(frozen=True)
class PostgresStateInitialization:
    """Safe result of one explicitly confirmed initialization request."""

    store_id: str
    address: StateAddress
    created_store: bool
    registered_address: bool

    @property
    def outcome(self) -> str:
        if self.created_store:
            return "initialized"
        if self.registered_address:
            return "address_registered"
        return "already_initialized"

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": "postgres",
            "outcome": self.outcome,
            "store_id": self.store_id,
            "schema_version": POSTGRES_SCHEMA_VERSION,
            "address": self.address.uri,
            "address_status": "registered",
            "state_status": "absent",
            "operation_status": "clear",
            "ordinary_state_authority": "disabled",
        }


@dataclass(frozen=True)
class PostgresStateLockProbeResult:
    """Instantaneous, non-reserving operation-lock availability."""

    store_id: str | None
    address: StateAddress
    lock_status: LockAvailability

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": "postgres",
            "store_id": self.store_id,
            "address": self.address.uri,
            "lock_status": self.lock_status,
            "reservation": "none",
            "ordinary_state_authority": "disabled",
        }


def _is_local_host(host: str) -> bool:
    candidate = host.strip().strip("[]")
    if not candidate or candidate.startswith("/") or candidate.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def _dsn_tls_options(dsn: str) -> dict[str, object]:
    """Validate a DSN-owned endpoint and return a non-empty TLS override."""
    if any(name in os.environ for name in _LIBPQ_ENDPOINT_ENVIRONMENT_VARIABLES):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state connection configuration is invalid"
        ) from None

    hosts: list[str] = []
    sslmodes: list[str] = []
    services: list[str] = []
    try:
        if dsn.lower().startswith(("postgresql://", "postgres://")):
            parsed = urlsplit(dsn)
            authority = parsed.netloc.rsplit("@", 1)[-1]
            if "," in authority:
                # urlsplit exposes only one hostname from libpq's authority
                # multihost syntax.  Reject it rather than overlooking a remote
                # fallback endpoint during TLS classification.
                raise ValueError
            query = parse_qs(parsed.query, keep_blank_values=True)
            query_hosts = query.get("hostaddr", query.get("host", []))
            if query_hosts:
                if len(query_hosts) != 1:
                    raise ValueError
                hosts.extend(query_hosts[0].split(","))
            elif parsed.hostname:
                hosts.extend(parsed.hostname.split(","))
            sslmodes = query.get("sslmode", [])
            services = query.get("service", [])
        else:
            values: dict[str, list[str]] = {}
            for part in shlex.split(dsn, posix=True):
                key, separator, value = part.partition("=")
                if not separator:
                    raise ValueError
                values.setdefault(key.lower(), []).append(value)
            raw_hosts = values.get("hostaddr", values.get("host", []))
            if len(raw_hosts) > 1:
                raise ValueError
            for raw_host in raw_hosts:
                hosts.extend(raw_host.split(","))
            sslmodes = values.get("sslmode", [])
            services = values.get("service", [])
    except (ValueError, UnicodeError):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state connection configuration is invalid"
        ) from None

    if (
        len(sslmodes) > 1
        or len(services) > 1
        or any(not host.strip() for host in hosts)
        or any(not service.strip() for service in services)
        or (not hosts and not services)
        or (hosts and services)
    ):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state connection configuration is invalid"
        ) from None
    sslmode = sslmodes[0].lower() if sslmodes else None
    if sslmode is not None and sslmode not in {
        "disable",
        "allow",
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    }:
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state connection configuration is invalid"
        ) from None
    local = bool(hosts) and all(_is_local_host(host) for host in hosts)

    if not local and sslmode in {"disable", "allow", "prefer"}:
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state requires TLS for non-loopback connections"
        ) from None
    return {"sslmode": sslmode or ("prefer" if local else "require")}


def _qualified(sql_module: _SqlModule, schema: str, table: str) -> object:
    return sql_module.SQL("{}.{}").format(
        sql_module.Identifier(schema),
        sql_module.Identifier(table),
    )


def _query(
    sql_module: _SqlModule,
    template: str,
    schema: str,
    table: str,
) -> object:
    return sql_module.SQL(template).format(_qualified(sql_module, schema, table))


def _rows(
    cursor: _Cursor,
    query: object,
    params: tuple[object, ...] | None = None,
) -> list[tuple[object, ...]]:
    cursor.execute(query, params)
    raw_rows = cursor.fetchall()
    if not isinstance(raw_rows, (list, tuple)):
        raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")
    result: list[tuple[object, ...]] = []
    for row in raw_rows:
        if not isinstance(row, (list, tuple)):
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")
        result.append(tuple(row))
    return result


def _one_or_none(
    rows: list[tuple[object, ...]],
    *,
    label: str,
) -> tuple[object, ...] | None:
    if len(rows) > 1:
        raise StateBackendInvalidStateError(f"PostgreSQL deployment state {label} is invalid")
    return rows[0] if rows else None


def _strict_json(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, str):
        raise StateBackendInvalidStateError(
            f"PostgreSQL deployment state {label} is invalid"
        )
    try:
        encoded_length = len(value.encode("utf-8"))
    except UnicodeError:
        encoded_length = -1
    if encoded_length < 0 or encoded_length > POSTGRES_STATE_MAX_BYTES:
        raise StateBackendInvalidStateError(f"PostgreSQL deployment state {label} is invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise StateFormatError("duplicate field")
            result[key] = item
        return result

    try:
        parsed = json.loads(value, object_pairs_hook=reject_duplicates)
    except (json.JSONDecodeError, RecursionError, StateError, UnicodeError, ValueError):
        raise StateBackendInvalidStateError(
            f"PostgreSQL deployment state {label} is invalid"
        ) from None
    if not isinstance(parsed, dict):
        raise StateBackendInvalidStateError(f"PostgreSQL deployment state {label} is invalid")
    return parsed


def _safe_operation_status(control: OperationControlState) -> SafeOperationStatus:
    # Reuse the existing provider-neutral safe projection rather than traversing
    # intent/progress payloads in the CLI layer.
    intent = control.intent
    recovery = control.recovery
    completed = [
        item.action_index
        for item in control.progress
        if item.status == "completed" and item.succeeded is True
    ]
    return SafeOperationStatus(
        status=control.status,
        operation_id=intent.operation_id if intent is not None else None,
        kind=intent.kind if intent is not None else None,
        failure_code=recovery.failure_code if recovery is not None else None,
        last_completed_action_index=max(completed) if completed else None,
    )


def _normalize_check_expression(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise StateBackendInvalidStateError("PostgreSQL deployment state constraints are invalid")
    normalized = "".join(value.split()).replace("::text[]", "").replace("::text", "")
    return normalized.replace("(", "").replace(")", "")


def _normalized_constraints(
    rows: list[tuple[object, ...]],
) -> list[tuple[object, ...]]:
    result: list[tuple[object, ...]] = []
    for row in rows:
        if len(row) != 15:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state constraints are invalid"
            )
        deferrable, deferred, validated = row[7:10]
        update_action, delete_action, match_type, no_inherit, enforced = row[10:15]
        if (
            deferrable is not False
            or deferred is not False
            or validated is not True
            or enforced is not True
            or (row[2] == "c" and no_inherit is not False)
            or (row[2] != "c" and no_inherit is not True)
            or (row[2] == "f" and (update_action, delete_action, match_type) != ("a", "r", "s"))
            or (row[2] != "f" and (update_action, delete_action, match_type) != (None, None, None))
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state constraints are invalid"
            )
        result.append((*row[:6], _normalize_check_expression(row[6])))
    return result


def _advisory_lock_key(address: StateAddress) -> int:
    digest = hashlib.sha256(f"streamt-postgres-state-address-v1\0{address.uri}".encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _registered_advisory_lock_key(
    cursor: _Cursor,
    sql_module: _SqlModule,
    schema: str,
    address: StateAddress,
) -> int | None:
    address_row = _one_or_none(
        _rows(
            cursor,
            _query(
                sql_module,
                (
                    "SELECT address_uri, advisory_lock_key FROM {} WHERE namespace = %s "
                    "AND project = %s AND environment = %s LIMIT 2"
                ),
                schema,
                "state_addresses",
            ),
            (address.namespace, address.project, address.environment),
        ),
        label="address",
    )
    if address_row is None:
        return None
    if (
        len(address_row) != 2
        or address_row[0] != address.uri
        or type(address_row[1]) is not int
        or address_row[1] < -(2**63)
        or address_row[1] > 2**63 - 1
        or address_row[1] != _advisory_lock_key(address)
    ):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state address is invalid"
        )
    return address_row[1]


def _initialization_lock_key(schema: str) -> tuple[int, int]:
    digest = hashlib.sha256(f"streamt-postgres-state-init-v1\0{schema}".encode()).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


class PostgresStateAdministration:
    """Narrow PostgreSQL status reader; intentionally no mutation methods."""

    __slots__ = ("_dsn", "_lock_timeout_seconds", "_schema")

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds

    def status(self, address: StateAddress) -> PostgresStateStatus:
        """Inspect one store/address in a consistent read-only snapshot."""
        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        result: PostgresStateStatus | None = None
        invalid = False
        unavailable = False
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                **options,
            )
            cursor = connection.cursor()
            cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute(
                "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
                (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
                (f"{self._lock_timeout_seconds * 1000}ms",),
            )
            result = self._read_status(cursor, bundle.sql, address)
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
        if unavailable or result is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state is unavailable"
            ) from None
        return result

    def _read_status(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        address: StateAddress,
    ) -> PostgresStateStatus:
        schema_rows = _rows(
            cursor,
            (
                "SELECT n.nspname, NOT EXISTS (SELECT 1 FROM "
                "pg_catalog.aclexplode(COALESCE(n.nspacl, "
                "pg_catalog.acldefault('n', n.nspowner))) AS acl "
                "WHERE acl.grantee <> n.nspowner AND "
                "(acl.grantee = 0 OR acl.privilege_type <> 'USAGE' "
                "OR acl.is_grantable)) "
                "FROM pg_catalog.pg_namespace AS n WHERE n.nspname = %s "
                "ORDER BY n.nspname"
            ),
            (self._schema,),
        )
        if not schema_rows:
            return self._uninitialized(address)
        if schema_rows != [(self._schema, True)]:
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")

        relation_rows = _rows(
            cursor,
            (
                "SELECT c.relname, c.relkind, c.relpersistence, c.relispartition, "
                "c.relrowsecurity, c.relforcerowsecurity, c.relowner = n.nspowner, "
                "NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(COALESCE(c.relacl, "
                "pg_catalog.acldefault('r', c.relowner))) AS acl "
                "WHERE acl.grantee <> c.relowner AND "
                "(acl.grantee = 0 OR acl.privilege_type <> 'SELECT' "
                "OR acl.is_grantable)) "
                "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) AS acl "
                "WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
                "AND acl.grantee <> c.relowner AND "
                "(acl.grantee = 0 OR acl.privilege_type <> 'SELECT' "
                "OR acl.is_grantable)) "
                "FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind IN ('r','p','v','m','S','f','c') "
                "ORDER BY c.relname"
            ),
            (self._schema,),
        )
        function_rows = _rows(
            cursor,
            (
                "SELECT p.proname FROM pg_catalog.pg_proc AS p "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace "
                "WHERE n.nspname = %s ORDER BY p.proname"
            ),
            (self._schema,),
        )
        type_rows = _rows(
            cursor,
            (
                "SELECT t.typname FROM pg_catalog.pg_type AS t "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace "
                "LEFT JOIN pg_catalog.pg_class AS c ON c.reltype = t.oid "
                "WHERE n.nspname = %s AND c.oid IS NULL "
                "AND t.typtype IN ('c','d','e','m','r') ORDER BY t.typname"
            ),
            (self._schema,),
        )
        schema_object_rows = _rows(
            cursor,
            (
                "SELECT object_kind, object_name FROM ("
                "SELECT 'collation' AS object_kind, c.collname AS object_name "
                "FROM pg_catalog.pg_collation AS c JOIN pg_catalog.pg_namespace AS n "
                "ON n.oid = c.collnamespace WHERE n.nspname = %s UNION ALL "
                "SELECT 'conversion', c.conname FROM pg_catalog.pg_conversion AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'operator', o.oprname FROM pg_catalog.pg_operator AS o "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'operator_class', o.opcname FROM pg_catalog.pg_opclass AS o "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = o.opcnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'operator_family', o.opfname FROM pg_catalog.pg_opfamily AS o "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = o.opfnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'text_search_config', t.cfgname FROM pg_catalog.pg_ts_config AS t "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.cfgnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'text_search_dictionary', t.dictname FROM pg_catalog.pg_ts_dict AS t "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.dictnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'text_search_parser', t.prsname FROM pg_catalog.pg_ts_parser AS t "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.prsnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'text_search_template', t.tmplname "
                "FROM pg_catalog.pg_ts_template AS t "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.tmplnamespace "
                "WHERE n.nspname = %s) AS objects "
                "ORDER BY object_kind, object_name"
            ),
            (self._schema,) * 9,
        )
        if not relation_rows:
            if function_rows or type_rows or schema_object_rows:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state catalog is invalid"
                )
            return self._uninitialized(address)
        if (
            relation_rows
            != [
                (table, "r", "p", False, False, False, True, True)
                for table in _EXPECTED_TABLES
            ]
            or function_rows
            or type_rows
            or schema_object_rows
        ):
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")

        column_rows = _rows(
            cursor,
            (
                "SELECT table_name, column_name, data_type, udt_name, "
                "is_nullable, column_default FROM information_schema.columns "
                "WHERE table_schema = %s ORDER BY table_name, ordinal_position"
            ),
            (self._schema,),
        )
        if column_rows != list(_EXPECTED_COLUMNS):
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")

        constraint_rows = _rows(
            cursor,
            (
                "SELECT r.relname, c.conname, c.contype, "
                "CASE WHEN c.contype IN ('p','u','f') THEN COALESCE(("
                "SELECT string_agg(a.attname, ',' ORDER BY k.ordinality) "
                "FROM unnest(c.conkey) WITH ORDINALITY AS k(attnum, ordinality) "
                "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.conrelid "
                "AND a.attnum = k.attnum), '') ELSE '' END, "
                "CASE WHEN c.contype = 'f' AND rrn.oid = n.oid "
                "THEN rr.relname ELSE NULL END, "
                "CASE WHEN c.contype = 'f' THEN COALESCE(("
                "SELECT string_agg(a.attname, ',' ORDER BY k.ordinality) "
                "FROM unnest(c.confkey) WITH ORDINALITY AS k(attnum, ordinality) "
                "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.confrelid "
                "AND a.attnum = k.attnum), '') ELSE NULL END, "
                "CASE WHEN c.contype = 'c' THEN "
                "pg_catalog.pg_get_expr(c.conbin, c.conrelid, false) ELSE NULL END, "
                "c.condeferrable, c.condeferred, c.convalidated, "
                "CASE WHEN c.contype = 'f' THEN c.confupdtype ELSE NULL END, "
                "CASE WHEN c.contype = 'f' THEN c.confdeltype ELSE NULL END, "
                "CASE WHEN c.contype = 'f' THEN c.confmatchtype ELSE NULL END, "
                "c.connoinherit, "
                # PostgreSQL 18 added enforced NOT NULL constraints and the
                # catalog flag. Columns already enforce nullability; ignore
                # their contype='n' rows while still rejecting other extras.
                "COALESCE((to_jsonb(c)->>'conenforced')::boolean, true) "
                "FROM pg_catalog.pg_constraint AS c "
                "JOIN pg_catalog.pg_class AS r ON r.oid = c.conrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace "
                "LEFT JOIN pg_catalog.pg_class AS rr ON rr.oid = c.confrelid "
                "LEFT JOIN pg_catalog.pg_namespace AS rrn ON rrn.oid = rr.relnamespace "
                "WHERE n.nspname = %s AND c.contype <> 'n' "
                "ORDER BY r.relname, c.conname"
            ),
            (self._schema,),
        )
        expected_constraints = [
            (*constraint[:6], _normalize_check_expression(constraint[6]))
            for constraint in _EXPECTED_CONSTRAINTS
        ]
        if _normalized_constraints(constraint_rows) != expected_constraints:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state constraints are invalid"
            )

        index_rows = _rows(
            cursor,
            (
                "SELECT r.relname, x.relname, i.indisunique, i.indisprimary, "
                "COALESCE((SELECT string_agg(a.attname, ',' ORDER BY k.ordinality) "
                "FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ordinality) "
                "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = i.indrelid "
                "AND a.attnum = k.attnum WHERE k.ordinality <= i.indnkeyatts), ''), "
                "pg_catalog.pg_get_expr(i.indpred, i.indrelid, false), "
                "pg_catalog.pg_get_expr(i.indexprs, i.indrelid, false), "
                "i.indisvalid, i.indisready, (i.indnatts = i.indnkeyatts), "
                "i.indisexclusion, i.indimmediate, "
                # PostgreSQL 15 added the catalog column.  Reading the row as
                # JSON keeps this status query valid on PostgreSQL 14 while
                # still enforcing the version-one default when it is present.
                "COALESCE((to_jsonb(i)->>'indnullsnotdistinct')::boolean, false), "
                "am.amname "
                "FROM pg_catalog.pg_index AS i "
                "JOIN pg_catalog.pg_class AS r ON r.oid = i.indrelid "
                "JOIN pg_catalog.pg_class AS x ON x.oid = i.indexrelid "
                "JOIN pg_catalog.pg_am AS am ON am.oid = x.relam "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace "
                "WHERE n.nspname = %s ORDER BY r.relname, x.relname"
            ),
            (self._schema,),
        )
        if index_rows != [
            (*index, None, None, True, True, True, False, True, False, "btree")
            for index in _EXPECTED_INDEXES
        ]:
            raise StateBackendInvalidStateError("PostgreSQL deployment state indexes are invalid")

        trigger_rows = _rows(
            cursor,
            (
                "SELECT r.relname, t.tgname FROM pg_catalog.pg_trigger AS t "
                "JOIN pg_catalog.pg_class AS r ON r.oid = t.tgrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace "
                "WHERE n.nspname = %s AND NOT t.tgisinternal "
                "ORDER BY r.relname, t.tgname"
            ),
            (self._schema,),
        )
        if trigger_rows:
            raise StateBackendInvalidStateError("PostgreSQL deployment state triggers are invalid")

        policy_rule_rows = _rows(
            cursor,
            (
                "SELECT object_kind, table_name, object_name FROM ("
                "SELECT 'policy' AS object_kind, r.relname AS table_name, "
                "p.polname AS object_name FROM pg_catalog.pg_policy AS p "
                "JOIN pg_catalog.pg_class AS r ON r.oid = p.polrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace "
                "WHERE n.nspname = %s UNION ALL "
                "SELECT 'rule', r.relname, w.rulename FROM pg_catalog.pg_rewrite AS w "
                "JOIN pg_catalog.pg_class AS r ON r.oid = w.ev_class "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = r.relnamespace "
                "WHERE n.nspname = %s AND w.rulename <> '_RETURN') AS controls "
                "ORDER BY object_kind, table_name, object_name"
            ),
            (self._schema, self._schema),
        )
        if policy_rule_rows:
            raise StateBackendInvalidStateError("PostgreSQL deployment state policies are invalid")

        metadata = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    "SELECT singleton, store_id::text, schema_version FROM {} LIMIT 2",
                    self._schema,
                    "store_metadata",
                ),
            ),
            label="metadata",
        )
        if metadata is None or len(metadata) != 3:
            raise StateBackendInvalidStateError("PostgreSQL deployment state metadata is invalid")
        singleton, store_id, schema_version = metadata
        if (
            singleton is not True
            or type(schema_version) is not int
            or schema_version != POSTGRES_SCHEMA_VERSION
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state schema version is invalid"
            )
        try:
            identity = StateStoreIdentity(
                backend="postgres",
                store_id=str(uuid.UUID(str(store_id))),
            )
        except (StateError, ValueError, AttributeError):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state metadata is invalid"
            ) from None

        migrations = _rows(
            cursor,
            _query(
                sql_module,
                (
                    "SELECT schema_version, migration_name, migration_checksum "
                    "FROM {} ORDER BY schema_version"
                ),
                self._schema,
                "schema_migrations",
            ),
        )
        if migrations != [_EXPECTED_MIGRATION]:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration ledger is invalid"
            )

        lock_key = _registered_advisory_lock_key(
            cursor,
            sql_module,
            self._schema,
            address,
        )
        if lock_key is None:
            return PostgresStateStatus(
                store_status="ready",
                store_id=identity.store_id,
                schema_version=POSTGRES_SCHEMA_VERSION,
                address=address,
                address_status="unregistered",
                state_status="unregistered",
                state_serial=None,
                state_checksum=None,
                operation_status=None,
            )
        operation_status = self._read_control(cursor, sql_module, address)
        state_row = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    (
                        "SELECT revision, state_serial, state_checksum, state_json, "
                        "octet_length(state_json) FROM {} WHERE namespace = %s "
                        "AND project = %s AND environment = %s LIMIT 2"
                    ),
                    self._schema,
                    "current_state",
                ),
                (address.namespace, address.project, address.environment),
            ),
            label="ownership",
        )
        if state_row is None:
            empty_state = LocalState(
                project=address.project,
                environment=address.environment,
            )
            return PostgresStateStatus(
                store_status="ready",
                store_id=identity.store_id,
                schema_version=POSTGRES_SCHEMA_VERSION,
                address=address,
                address_status="registered",
                state_status="absent",
                state_serial=0,
                state_checksum=state_checksum(empty_state),
                operation_status=operation_status,
            )
        state, checksum = self._parse_state_row(state_row, address)
        return PostgresStateStatus(
            store_status="ready",
            store_id=identity.store_id,
            schema_version=POSTGRES_SCHEMA_VERSION,
            address=address,
            address_status="registered",
            state_status="present",
            state_serial=state.serial,
            state_checksum=checksum,
            operation_status=operation_status,
        )

    @staticmethod
    def _uninitialized(address: StateAddress) -> PostgresStateStatus:
        return PostgresStateStatus(
            store_status="uninitialized",
            store_id=None,
            schema_version=None,
            address=address,
            address_status="unregistered",
            state_status="unregistered",
            state_serial=None,
            state_checksum=None,
            operation_status=None,
        )

    def _read_control(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        address: StateAddress,
    ) -> SafeOperationStatus:
        row = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    (
                        "SELECT revision, status, control_json, "
                        "octet_length(control_json) FROM {} WHERE namespace = %s "
                        "AND project = %s AND environment = %s LIMIT 2"
                    ),
                    self._schema,
                    "operation_control",
                ),
                (address.namespace, address.project, address.environment),
            ),
            label="operation control",
        )
        if row is None or len(row) != 4:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state operation control is invalid"
            )
        revision, status, control_json, byte_length = row
        if (
            type(revision) is not int
            or revision < 0
            or type(byte_length) is not int
            or byte_length < 0
            or byte_length > POSTGRES_STATE_MAX_BYTES
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state operation control is invalid"
            )
        raw_control = _strict_json(control_json, label="operation control")
        if len(cast(str, control_json).encode("utf-8")) != byte_length:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state operation control is invalid"
            )
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
        return _safe_operation_status(control)

    @staticmethod
    def _parse_state_row(
        row: tuple[object, ...],
        address: StateAddress,
    ) -> tuple[LocalState, str]:
        if len(row) != 5:
            raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
        revision, serial, checksum, state_json, byte_length = row
        if (
            type(revision) is not int
            or revision < 1
            or type(serial) is not int
            or serial < 1
            or not isinstance(checksum, str)
            or type(byte_length) is not int
            or byte_length < 0
            or byte_length > POSTGRES_STATE_MAX_BYTES
        ):
            raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
        raw_state = _strict_json(state_json, label="ownership state")
        if len(cast(str, state_json).encode("utf-8")) != byte_length:
            raise StateBackendInvalidStateError("PostgreSQL deployment ownership state is invalid")
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
        return state, checksum


class PostgresStateLockProbe:
    """Transient lock probe; deliberately not an ordinary state backend."""

    __slots__ = ("_dsn", "_lock_timeout_seconds", "_schema")

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds

    def probe(self, address: StateAddress) -> PostgresStateLockProbeResult:
        """Observe instantaneous availability and release before returning."""
        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        result: PostgresStateLockProbeResult | None = None
        invalid = False
        unavailable = False
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                **options,
            )
            cursor = connection.cursor()
            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
                (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
                (f"{self._lock_timeout_seconds * 1000}ms",),
            )
            status = PostgresStateAdministration(
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
            )._read_status(cursor, bundle.sql, address)

            try:
                recovery_rows = _rows(
                    cursor,
                    "SELECT pg_catalog.pg_is_in_recovery()",
                )
            except StateBackendInvalidStateError:
                raise StateBackendUnavailableError(
                    "PostgreSQL deployment state lock probe is unavailable"
                ) from None
            if recovery_rows != [(False,)]:
                raise StateBackendUnavailableError(
                    "PostgreSQL deployment state lock probe requires a primary server"
                )

            if status.address_status == "unregistered":
                result = PostgresStateLockProbeResult(
                    store_id=status.store_id,
                    address=address,
                    lock_status="unregistered",
                )
            else:
                lock_key = _registered_advisory_lock_key(
                    cursor,
                    bundle.sql,
                    self._schema,
                    address,
                )
                if lock_key is None:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment state address is invalid"
                    )
                try:
                    probe_rows = _rows(
                        cursor,
                        "SELECT pg_catalog.pg_try_advisory_xact_lock(%s)",
                        (lock_key,),
                    )
                except StateBackendInvalidStateError:
                    raise StateBackendUnavailableError(
                        "PostgreSQL deployment state lock probe is unavailable"
                    ) from None
                if probe_rows == [(True,)]:
                    lock_status: LockAvailability = "available"
                elif probe_rows == [(False,)]:
                    lock_status = "busy"
                else:
                    raise StateBackendUnavailableError(
                        "PostgreSQL deployment state lock probe is unavailable"
                    )
                result = PostgresStateLockProbeResult(
                    store_id=status.store_id,
                    address=address,
                    lock_status=lock_status,
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
                    # The probe lock is transaction-scoped. A result is valid
                    # only after this rollback has released it, including when
                    # a transaction-pooling endpoint pins one backend for the
                    # explicit transaction.
                    connection.rollback()
                except Exception:
                    unavailable = True
                try:
                    connection.close()
                except Exception:
                    unavailable = True

        if invalid:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state is invalid"
            ) from None
        if unavailable or result is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state lock probe is unavailable"
            ) from None
        return result


class PostgresStateInitializer:
    """Explicit schema/address initializer; not an ordinary state backend."""

    __slots__ = ("_dsn", "_lock_timeout_seconds", "_schema")

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
    ) -> None:
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds

    def initialize(self, address: StateAddress) -> PostgresStateInitialization:
        """Initialize or exactly repeat initialization for one address."""
        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        result: PostgresStateInitialization | None = None
        invalid = False
        unavailable = False
        unknown_commit = False
        commit_attempted = False
        transaction_started = False
        session_lock_acquired = False
        initialization_lock_key = _initialization_lock_key(self._schema)
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
                **options,
            )
            cursor = connection.cursor()
            cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, false)",
                (f"{self._lock_timeout_seconds * 1000}ms",),
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('lock_timeout', %s, false)",
                (f"{self._lock_timeout_seconds * 1000}ms",),
            )
            cursor.execute(
                "SELECT pg_catalog.pg_advisory_lock(%s, %s)",
                initialization_lock_key,
            )
            session_lock_acquired = True
            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"
            )
            transaction_started = True
            cursor.execute(
                "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
                (f"{self._lock_timeout_seconds * 1000}ms",),
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
                (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
            )
            result = self._initialize_transaction(cursor, bundle.sql, address)
            commit_attempted = True
            connection.commit()
            transaction_started = False
        except StateBackendInvalidStateError:
            if commit_attempted:
                unknown_commit = True
            else:
                invalid = True
        except (KeyboardInterrupt, SystemExit):
            if commit_attempted:
                unknown_commit = True
            else:
                raise
        except Exception:
            if commit_attempted:
                unknown_commit = True
            else:
                unavailable = True
        finally:
            if connection is not None and transaction_started and not commit_attempted:
                try:
                    connection.rollback()
                    transaction_started = False
                except Exception:
                    unavailable = True
            if cursor is not None and session_lock_acquired and not unknown_commit:
                try:
                    unlock_rows = _rows(
                        cursor,
                        "SELECT pg_catalog.pg_advisory_unlock(%s, %s)",
                        initialization_lock_key,
                    )
                    if unlock_rows != [(True,)] and not commit_attempted:
                        unavailable = True
                except Exception:
                    if not commit_attempted:
                        unavailable = True
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    if not unknown_commit and not commit_attempted:
                        unavailable = True
            if connection is not None:
                if transaction_started and not commit_attempted:
                    try:
                        connection.rollback()
                    except Exception:
                        unavailable = True
                try:
                    connection.close()
                except Exception:
                    if not unknown_commit and not commit_attempted:
                        unavailable = True

        if unknown_commit:
            raise StateBackendUnknownCommitError(
                "PostgreSQL deployment state initialization outcome is unknown; "
                "run state status or repeat the same state init confirmation"
            ) from None
        if invalid:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state initialization is incompatible"
            ) from None
        if unavailable or result is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state initialization is unavailable"
            ) from None

        verified = PostgresStateAdministration(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
        ).status(address)
        if not self._verification_matches(verified, result):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state initialization verification failed"
            ) from None
        return result

    def _initialize_transaction(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        address: StateAddress,
    ) -> PostgresStateInitialization:
        schema_rows = _rows(
            cursor,
            (
                "SELECT n.nspname, n.nspowner = (SELECT r.oid "
                "FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user) "
                "FROM pg_catalog.pg_namespace AS n WHERE n.nspname = %s "
                "ORDER BY n.nspname"
            ),
            (self._schema,),
        )
        if schema_rows not in ([], [(self._schema, True)]):
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")
        status = PostgresStateAdministration(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
        )._read_status(cursor, sql_module, address)
        created_store = status.store_status == "uninitialized"
        initialized_at = datetime.now(timezone.utc)

        if created_store:
            self._create_schema_v1(
                cursor,
                sql_module,
                schema_exists=bool(schema_rows),
            )
            store_uuid = uuid.uuid4()
            cursor.execute(
                _query(
                    sql_module,
                    (
                        "INSERT INTO {} "
                        "(singleton, store_id, schema_version, initialized_at) "
                        "VALUES (%s, %s, %s, %s)"
                    ),
                    self._schema,
                    "store_metadata",
                ),
                (True, store_uuid, POSTGRES_SCHEMA_VERSION, initialized_at),
            )
            cursor.execute(
                _query(
                    sql_module,
                    (
                        "INSERT INTO {} (schema_version, migration_name, "
                        "migration_checksum, applied_at) VALUES (%s, %s, %s, %s)"
                    ),
                    self._schema,
                    "schema_migrations",
                ),
                (*_EXPECTED_MIGRATION, initialized_at),
            )
            store_id = str(store_uuid)
        else:
            if status.store_id is None:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state metadata is invalid"
                )
            store_id = status.store_id

        registered_address = self._register_address(
            cursor,
            sql_module,
            address,
            status=status,
            registered_at=initialized_at,
        )
        result = PostgresStateInitialization(
            store_id=store_id,
            address=address,
            created_store=created_store,
            registered_address=registered_address,
        )
        precommit_status = PostgresStateAdministration(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
        )._read_status(cursor, sql_module, address)
        if not self._verification_matches(precommit_status, result):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state initialization verification failed"
            )
        return result

    def _create_schema_v1(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        *,
        schema_exists: bool,
    ) -> None:
        if not schema_exists:
            cursor.execute(
                sql_module.SQL("CREATE SCHEMA {}").format(sql_module.Identifier(self._schema))
            )
            cursor.execute(
                sql_module.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(
                    sql_module.Identifier(self._schema)
                )
            )
        for table, template, reference in _SCHEMA_V1_DDL:
            identifiers = [_qualified(sql_module, self._schema, table)]
            if reference is not None:
                identifiers.append(_qualified(sql_module, self._schema, reference))
            cursor.execute(sql_module.SQL(template).format(*identifiers))
            cursor.execute(
                sql_module.SQL("REVOKE ALL ON TABLE {} FROM PUBLIC").format(
                    _qualified(sql_module, self._schema, table)
                )
            )

    def _register_address(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        address: StateAddress,
        *,
        status: PostgresStateStatus,
        registered_at: datetime,
    ) -> bool:
        lock_key = _advisory_lock_key(address)
        collision = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    (
                        "SELECT namespace, project, environment, address_uri FROM {} "
                        "WHERE advisory_lock_key = %s LIMIT 2"
                    ),
                    self._schema,
                    "state_addresses",
                ),
                (lock_key,),
            ),
            label="address lock mapping",
        )
        expected_collision = (
            address.namespace,
            address.project,
            address.environment,
            address.uri,
        )
        if collision is not None and collision != expected_collision:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state address registration conflicts"
            )

        if status.address_status == "registered":
            operation = status.operation_status
            if (
                collision != expected_collision
                or status.state_status != "absent"
                or status.state_serial != 0
                or operation is None
                or operation.status != "clear"
                or operation.operation_id is not None
                or operation.kind is not None
                or operation.failure_code is not None
                or operation.last_completed_action_index is not None
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state address is not empty"
                )
            return False
        if collision is not None or status.address_status != "unregistered":
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state address registration conflicts"
            )

        cursor.execute(
            _query(
                sql_module,
                (
                    "INSERT INTO {} (namespace, project, environment, address_uri, "
                    "advisory_lock_key, registered_at) VALUES (%s, %s, %s, %s, %s, %s)"
                ),
                self._schema,
                "state_addresses",
            ),
            (
                address.namespace,
                address.project,
                address.environment,
                address.uri,
                lock_key,
                registered_at,
            ),
        )
        clear_control = _canonical_json(OperationControlState.clear(address).to_dict())
        cursor.execute(
            _query(
                sql_module,
                (
                    "INSERT INTO {} (namespace, project, environment, revision, status, "
                    "control_json, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                ),
                self._schema,
                "operation_control",
            ),
            (
                address.namespace,
                address.project,
                address.environment,
                0,
                "clear",
                clear_control,
                registered_at,
            ),
        )
        return True

    @staticmethod
    def _verification_matches(
        status: PostgresStateStatus,
        result: PostgresStateInitialization,
    ) -> bool:
        operation = status.operation_status
        return (
            status.store_status == "ready"
            and status.store_id == result.store_id
            and status.schema_version == POSTGRES_SCHEMA_VERSION
            and status.address == result.address
            and status.address_status == "registered"
            and status.state_status == "absent"
            and status.state_serial == 0
            and operation is not None
            and operation.status == "clear"
            and operation.operation_id is None
            and operation.kind is None
            and operation.failure_code is None
            and operation.last_completed_action_index is None
        )


def make_postgres_state_administration(
    config: DeploymentStateConfig,
) -> PostgresStateAdministration:
    """Construct the separate PostgreSQL administrative reader without fallback."""
    if not isinstance(config, PostgresDeploymentStateConfig):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state administration is not configured"
        )
    dsn = os.environ.get(config.postgres.dsn_env)
    if dsn is None or not dsn.strip():
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state credentials are unavailable"
        )
    return PostgresStateAdministration(
        dsn=dsn,
        schema=config.postgres.schema_name,
        lock_timeout_seconds=config.lock_timeout_seconds,
    )


def make_postgres_state_lock_probe(
    config: DeploymentStateConfig,
) -> PostgresStateLockProbe:
    """Construct the separate transient PostgreSQL lock probe without fallback."""
    if not isinstance(config, PostgresDeploymentStateConfig):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state lock probing is not configured"
        )
    dsn = os.environ.get(config.postgres.dsn_env)
    if dsn is None or not dsn.strip():
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state credentials are unavailable"
        )
    return PostgresStateLockProbe(
        dsn=dsn,
        schema=config.postgres.schema_name,
        lock_timeout_seconds=config.lock_timeout_seconds,
    )


def make_postgres_state_initializer(
    config: DeploymentStateConfig,
) -> PostgresStateInitializer:
    """Construct the separate explicit PostgreSQL initializer without fallback."""
    if not isinstance(config, PostgresDeploymentStateConfig):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state initialization is not configured"
        )
    dsn = os.environ.get(config.postgres.dsn_env)
    if dsn is None or not dsn.strip():
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state credentials are unavailable"
        )
    return PostgresStateInitializer(
        dsn=dsn,
        schema=config.postgres.schema_name,
        lock_timeout_seconds=config.lock_timeout_seconds,
    )
