"""Narrow PostgreSQL deployment-state administration.

This module is deliberately not a ``DeploymentStateBackend``. It implements
administrative inspection, probing, initialization, and migration plus the
canonical version-two catalog/writer proof consumed by the separate ordinary
backend.

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
import time
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
from streamt.deployer.kafka_streams_evidence import KAFKA_STREAMS_CONTROL_VERSION
from streamt.deployer.recovery import (
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
)
from streamt.deployer.state import LocalState, StateError, StateFormatError
from streamt.deployer.state_backend import (
    RESUMABLE_CONTROL_VERSION,
    OperationControlState,
    OperationIntent,
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockTimeoutError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
    StateStoreIdentity,
    state_checksum,
)

POSTGRES_SCHEMA_VERSION = 1
POSTGRES_SCHEMA_V2_VERSION = 2
POSTGRES_ORDINARY_AUTHORITY_SUPPORTED = "supported_for_v2_writer"
POSTGRES_ORDINARY_AUTHORITY_DISABLED = "disabled"
POSTGRES_ORDINARY_AUTHORITY_NOT_VERIFIED = "not_verified"
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


def postgres_ordinary_authority(schema_version: int | None) -> str:
    """Describe the released catalog capability without probing a credential."""
    if schema_version == POSTGRES_SCHEMA_V2_VERSION:
        return POSTGRES_ORDINARY_AUTHORITY_SUPPORTED
    return POSTGRES_ORDINARY_AUTHORITY_DISABLED


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

# Version 2 is an explicit, additive administrative migration.  Version 1's
# constants remain frozen because the shipped initializer and its checksum are
# a durable compatibility contract.
_EXPECTED_COLUMNS_V2: tuple[tuple[str, str, str, str, str, object], ...] = tuple(
    sorted(
        (
            *_EXPECTED_COLUMNS,
            ("store_metadata", "writer_role_name", "text", "text", "NO", None),
        ),
        key=lambda column: column[0],
    )
)
_EXPECTED_CONSTRAINTS_V2: tuple[
    tuple[str, str, str, str, str | None, str | None, str | None], ...
] = tuple(
    sorted(
        (
            *(
                constraint
                for constraint in _EXPECTED_CONSTRAINTS
                if constraint[1] != "store_metadata_schema_version_check"
            ),
            (
                "store_metadata",
                "store_metadata_schema_version_check",
                "c",
                "",
                None,
                None,
                "schema_version = 2",
            ),
            (
                "store_metadata",
                "store_metadata_writer_role_name_check",
                "c",
                "",
                None,
                None,
                "writer_role_name <> ''::text",
            ),
        )
    )
)

_CURRENT_STATE_COLUMNS = (
    "namespace",
    "project",
    "environment",
    "revision",
    "state_serial",
    "state_checksum",
    "state_json",
    "updated_at",
)
_OPERATION_CONTROL_UPDATE_COLUMNS = (
    "revision",
    "status",
    "control_json",
    "updated_at",
)
_STATE_HISTORY_COLUMNS = (
    "namespace",
    "project",
    "environment",
    "revision",
    "state_serial",
    "state_checksum",
    "state_json",
    "operation_id",
    "recorded_at",
)
_OPERATION_HISTORY_COLUMNS = (
    "namespace",
    "project",
    "environment",
    "operation_id",
    "event_index",
    "event_kind",
    "control_json",
    "recorded_at",
)
_SCHEMA_V2_WRITER_COLUMN_PRIVILEGES: tuple[
    tuple[str, str, tuple[str, ...]], ...
] = (
    ("current_state", "INSERT", _CURRENT_STATE_COLUMNS),
    (
        "current_state",
        "UPDATE",
        ("revision", "state_serial", "state_checksum", "state_json", "updated_at"),
    ),
    ("operation_control", "UPDATE", _OPERATION_CONTROL_UPDATE_COLUMNS),
    ("state_history", "INSERT", _STATE_HISTORY_COLUMNS),
    ("operation_history", "INSERT", _OPERATION_HISTORY_COLUMNS),
)
_SCHEMA_V2_CONTRACT_BYTES = json.dumps(
    {
        "columns": _EXPECTED_COLUMNS_V2,
        "constraints": _EXPECTED_CONSTRAINTS_V2,
        "indexes": _EXPECTED_INDEXES,
        "writer_schema_privileges": ("USAGE",),
        "writer_table_privileges": tuple(
            (table, ("SELECT",)) for table in _EXPECTED_TABLES
        ),
        "writer_column_privileges": _SCHEMA_V2_WRITER_COLUMN_PRIVILEGES,
        "writer_role_contract": (
            "LOGIN",
            "NOINHERIT",
            "NOSUPERUSER",
            "NOCREATEDB",
            "NOCREATEROLE",
            "NOREPLICATION",
            "NOBYPASSRLS",
            "NO_MEMBERSHIPS",
            "NOT_OWNER",
        ),
        "writer_grantor": "schema_owner",
        "status_reader_contract": ("USAGE", "SELECT", "NO_GRANT_OPTION"),
        "default_acl": "none",
    },
    sort_keys=True,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")
POSTGRES_SCHEMA_V2_CHECKSUM = (
    "sha256:" + hashlib.sha256(_SCHEMA_V2_CONTRACT_BYTES).hexdigest()
)
_EXPECTED_MIGRATION_V2 = (
    POSTGRES_SCHEMA_V2_VERSION,
    "schema-v2-writer-role",
    POSTGRES_SCHEMA_V2_CHECKSUM,
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
_EXPECTED_MIGRATIONS_V2 = (_EXPECTED_MIGRATION, _EXPECTED_MIGRATION_V2)
_V1_OPERATION_EVENT_KINDS = {
    "intent",
    "progress_started",
    "progress_completed",
    # Operation-control version 4 is independent of the SQL schema version.
    # The strict control parser rejects checkpoints in older control payloads.
    "progress_checkpoint",
    "operation_resumed",
    "recovery_required",
    "cleared_before_mutation",
    "succeeded",
}
_V2_OPERATION_EVENT_KINDS = {
    *_V1_OPERATION_EVENT_KINDS,
    "recovery_intent",
    "recovered_observed",
    "recovered_rolled_back",
    "recovered_abandoned_before_mutation",
}
_V2_RECOVERY_EVENT_KINDS = {
    "recovery_intent",
    "recovered_observed",
    "recovered_rolled_back",
    "recovered_abandoned_before_mutation",
}
_V2_RECOVERY_RESOLUTION_EVENTS = {
    "observed": "recovered_observed",
    "rolled_back": "recovered_rolled_back",
    "abandoned_before_mutation": "recovered_abandoned_before_mutation",
}


def _operation_history_states(
    control: OperationControlState,
) -> list[tuple[str, OperationControlState]]:
    """Reconstruct every durable boundary without rebasing resumed intent.

    Resume records preserve the exact incident at a progress-prefix boundary.
    History indexes therefore count events, not execution-progress entries.
    SQL catalog versions are independent of operation-control wire versions.
    """
    intent = control.intent
    if intent is None:
        return []
    resumes = control.resume_history
    initial_version = (
        KAFKA_STREAMS_CONTROL_VERSION if resumes else control.control_version
    )
    states = [
        (
            "intent",
            OperationControlState(
                address=control.address,
                status="in_progress",
                intent=intent,
                control_version=initial_version,
            ),
        )
    ]
    resume_index = 0
    for progress_count in range(len(control.progress) + 1):
        if progress_count:
            states.append(
                (
                    f"progress_{control.progress[progress_count - 1].status}",
                    OperationControlState(
                        address=control.address,
                        status="in_progress",
                        intent=intent,
                        progress=control.progress[:progress_count],
                        control_version=(
                            RESUMABLE_CONTROL_VERSION if resume_index else initial_version
                        ),
                        resume_history=resumes[:resume_index],
                    ),
                )
            )
        while (
            resume_index < len(resumes)
            and resumes[resume_index].progress_count == progress_count
        ):
            record = resumes[resume_index]
            states.append(
                (
                    "recovery_required",
                    OperationControlState(
                        address=control.address,
                        status="recovery_required",
                        intent=intent,
                        progress=control.progress[:progress_count],
                        recovery=record.recovery,
                        control_version=(
                            RESUMABLE_CONTROL_VERSION if resume_index else initial_version
                        ),
                        resume_history=resumes[:resume_index],
                    ),
                )
            )
            resume_index += 1
            states.append(
                (
                    "operation_resumed",
                    OperationControlState(
                        address=control.address,
                        status="in_progress",
                        intent=intent,
                        progress=control.progress[:progress_count],
                        control_version=RESUMABLE_CONTROL_VERSION,
                        resume_history=resumes[:resume_index],
                    ),
                )
            )
    if control.status == "recovery_required":
        states.append(("recovery_required", control))
    if resume_index != len(resumes) or states[-1][1] != control:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment operation history is invalid"
        )
    return states


def _validate_operation_history_states(
    events: list[tuple[int, str, OperationControlState]],
    *,
    address: StateAddress,
    operation_id: str,
) -> OperationControlState:
    """Validate a complete operation timeline and retain its last active image."""
    if not events or [event[0] for event in events] != list(range(len(events))):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment operation history is invalid"
        )
    terminal_kind = events[-1][1]
    terminal = terminal_kind in {"succeeded", "cleared_before_mutation"}
    active_events = events[:-1] if terminal else events
    if not active_events:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment operation history is invalid"
        )
    latest = active_events[-1][2]
    if (
        latest.address != address
        or latest.intent is None
        or latest.intent.operation_id != operation_id
        or [(kind, state) for _index, kind, state in active_events]
        != _operation_history_states(latest)
    ):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment operation history is invalid"
        )
    if terminal:
        if (
            events[-1][2].status != "clear"
            or events[-1][2].address != address
            or latest.status != "in_progress"
            or (
                terminal_kind == "cleared_before_mutation"
                and (len(events) != 2 or latest.progress or latest.resume_history)
            )
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment operation history is invalid"
            )
        if terminal_kind == "succeeded" and not latest.actions_completed:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment operation history is incomplete"
            )
    return latest

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
            "mutation_status": (
                "catalog_ready"
                if self.schema_version == POSTGRES_SCHEMA_V2_VERSION
                else "disabled"
            ),
            "ordinary_state_authority": postgres_ordinary_authority(
                self.schema_version
            ),
        }


@dataclass(frozen=True)
class PostgresStateInitialization:
    """Safe result of one explicitly confirmed initialization request."""

    store_id: str
    address: StateAddress
    created_store: bool
    registered_address: bool
    schema_version: int = POSTGRES_SCHEMA_VERSION

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
            "schema_version": self.schema_version,
            "address": self.address.uri,
            "address_status": "registered",
            "state_status": "absent",
            "operation_status": "clear",
            "ordinary_state_authority": postgres_ordinary_authority(
                self.schema_version
            ),
        }


@dataclass(frozen=True)
class PostgresStateV2Migration:
    """Secret-neutral result of the direct-only schema-v2 migration."""

    store_id: str
    migrated: bool

    @property
    def outcome(self) -> str:
        return "migrated" if self.migrated else "already_migrated"

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": "postgres",
            "outcome": self.outcome,
            "store_id": self.store_id,
            "schema_version": POSTGRES_SCHEMA_V2_VERSION,
            "ordinary_state_authority": POSTGRES_ORDINARY_AUTHORITY_SUPPORTED,
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
            "ordinary_state_authority": POSTGRES_ORDINARY_AUTHORITY_NOT_VERIFIED,
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


def _validated_v2_role(
    cursor: _Cursor,
    *,
    schema: str,
    writer_name: str,
) -> tuple[int, int]:
    """Resolve one portable role name to its transient writer and owner OIDs."""
    role_rows = _rows(
        cursor,
        (
            "SELECT r.oid::bigint, r.rolname, r.rolsuper, r.rolcreaterole, "
            "r.rolcreatedb, r.rolcanlogin, r.rolreplication, r.rolbypassrls, "
            "r.rolinherit, r.oid <> n.nspowner, NOT EXISTS ("
            "SELECT 1 FROM pg_catalog.pg_auth_members AS m "
            "WHERE m.member = r.oid OR m.roleid = r.oid), n.nspowner::bigint "
            "FROM pg_catalog.pg_roles AS r "
            "JOIN pg_catalog.pg_namespace AS n ON n.nspname = %s "
            "WHERE r.rolname = %s ORDER BY r.oid"
        ),
        (schema, writer_name),
    )
    if len(role_rows) != 1 or len(role_rows[0]) != 12:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state writer role is invalid"
        )
    writer_oid, stored_name, *role_contract, owner_oid = role_rows[0]
    if (
        type(writer_oid) is not int
        or writer_oid <= 0
        or type(owner_oid) is not int
        or owner_oid <= 0
        or stored_name != writer_name
        or role_contract
        != [False, False, False, True, False, False, False, True, True]
    ):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state writer role is invalid"
        )
    return writer_oid, owner_oid


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
        expected_relation_structure = [
            (table, "r", "p", False, False, False, True)
            for table in _EXPECTED_TABLES
        ]
        if [row[:7] for row in relation_rows] != expected_relation_structure:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state catalog is invalid"
            )
        column_rows = _rows(
            cursor,
            (
                "SELECT table_name, column_name, data_type, udt_name, "
                "is_nullable, column_default FROM information_schema.columns "
                "WHERE table_schema = %s ORDER BY table_name, ordinal_position"
            ),
            (self._schema,),
        )
        schema_version = (
            POSTGRES_SCHEMA_V2_VERSION
            if column_rows == list(_EXPECTED_COLUMNS_V2)
            else POSTGRES_SCHEMA_VERSION
        )
        expected_relations = [
            (table, "r", "p", False, False, False, True, True)
            for table in _EXPECTED_TABLES
        ]
        # The v1 query's final flag deliberately rejects every non-reader
        # column grant.  V2's exact writer-column ACL is checked below from
        # pg_catalog rows, so ignore only that aggregate flag for this branch.
        if schema_version == POSTGRES_SCHEMA_V2_VERSION:
            relation_rows = [(*row[:7], True) for row in relation_rows]
        if (
            relation_rows != expected_relations
            or function_rows
            or type_rows
            or schema_object_rows
        ):
            raise StateBackendInvalidStateError("PostgreSQL deployment state catalog is invalid")

        expected_columns = (
            _EXPECTED_COLUMNS_V2
            if schema_version == POSTGRES_SCHEMA_V2_VERSION
            else _EXPECTED_COLUMNS
        )
        if column_rows != list(expected_columns):
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
        constraint_contract = (
            _EXPECTED_CONSTRAINTS_V2
            if schema_version == POSTGRES_SCHEMA_V2_VERSION
            else _EXPECTED_CONSTRAINTS
        )
        expected_constraints = [
            (*constraint[:6], _normalize_check_expression(constraint[6]))
            for constraint in constraint_contract
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

        metadata_columns = (
            "singleton, store_id::text, schema_version, writer_role_name"
            if schema_version == POSTGRES_SCHEMA_V2_VERSION
            else "singleton, store_id::text, schema_version"
        )
        metadata = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    f"SELECT {metadata_columns} FROM {{}} LIMIT 2",
                    self._schema,
                    "store_metadata",
                ),
            ),
            label="metadata",
        )
        expected_metadata_length = (
            4 if schema_version == POSTGRES_SCHEMA_V2_VERSION else 3
        )
        if metadata is None or len(metadata) != expected_metadata_length:
            raise StateBackendInvalidStateError("PostgreSQL deployment state metadata is invalid")
        singleton, store_id, stored_schema_version, *writer_identity = metadata
        if (
            singleton is not True
            or type(stored_schema_version) is not int
            or stored_schema_version != schema_version
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
        expected_migrations = (
            list(_EXPECTED_MIGRATIONS_V2)
            if schema_version == POSTGRES_SCHEMA_V2_VERSION
            else [_EXPECTED_MIGRATION]
        )
        if migrations != expected_migrations:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration ledger is invalid"
            )
        if schema_version == POSTGRES_SCHEMA_V2_VERSION:
            if len(writer_identity) != 1:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state writer identity is invalid"
                )
            (writer_name,) = writer_identity
            if not isinstance(writer_name, str) or not writer_name:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state writer identity is invalid"
                )
            self._validate_v2_access(
                cursor,
                writer_name=writer_name,
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
                schema_version=schema_version,
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
                schema_version=schema_version,
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
            schema_version=schema_version,
            address=address,
            address_status="registered",
            state_status="present",
            state_serial=state.serial,
            state_checksum=checksum,
            operation_status=operation_status,
        )

    def _validate_v2_access(
        self,
        cursor: _Cursor,
        *,
        writer_name: str,
    ) -> None:
        """Validate the stored writer identity and its complete effective ACL.

        Owner ACL entries are PostgreSQL's implicit administration contract.
        Every non-owner entry is constrained to the v1 reader contract, except
        for the one stored writer identity whose additional column privileges
        must be exactly the v2 mutation set.
        """
        writer_oid, owner_oid = _validated_v2_role(
            cursor,
            schema=self._schema,
            writer_name=writer_name,
        )

        default_acl_rows = _rows(
            cursor,
            (
                "SELECT d.defaclobjtype, d.defaclrole::bigint, "
                "COALESCE(n.nspname, '') "
                "FROM pg_catalog.pg_default_acl AS d "
                "LEFT JOIN pg_catalog.pg_namespace AS n ON n.oid = d.defaclnamespace "
                "WHERE d.defaclobjtype IN ('r', 'S') "
                "AND d.defaclrole IN (%s, %s) "
                "AND (d.defaclnamespace = 0 OR n.nspname = %s) "
                "ORDER BY d.defaclobjtype, d.defaclrole, n.nspname"
            ),
            (owner_oid, writer_oid, self._schema),
        )
        if default_acl_rows:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state default privileges are invalid"
            )

        schema_acl_rows = _rows(
            cursor,
            (
                "SELECT acl.grantee::bigint, acl.grantor::bigint, "
                "acl.privilege_type, acl.is_grantable "
                "FROM pg_catalog.pg_namespace AS n CROSS JOIN LATERAL "
                "pg_catalog.aclexplode(COALESCE(n.nspacl, "
                "pg_catalog.acldefault('n', n.nspowner))) AS acl "
                "WHERE n.nspname = %s AND acl.grantee <> n.nspowner "
                "ORDER BY acl.grantee, acl.privilege_type"
            ),
            (self._schema,),
        )
        if any(len(row) != 4 for row in schema_acl_rows):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state schema privileges are invalid"
            )
        if any(
            grantee == 0
            or (grantee == writer_oid and grantor != owner_oid)
            or privilege != "USAGE"
            or grantable is not False
            for grantee, grantor, privilege, grantable in schema_acl_rows
        ) or sum(row[0] == writer_oid for row in schema_acl_rows) != 1:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state schema privileges are invalid"
            )

        table_acl_rows = _rows(
            cursor,
            (
                "SELECT c.relname, acl.grantee::bigint, acl.grantor::bigint, "
                "acl.privilege_type, acl.is_grantable FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl, "
                "pg_catalog.acldefault('r', c.relowner))) AS acl "
                "WHERE n.nspname = %s AND c.relkind = 'r' "
                "AND acl.grantee <> c.relowner "
                "ORDER BY c.relname, acl.grantee, acl.privilege_type"
            ),
            (self._schema,),
        )
        if any(len(row) != 5 for row in table_acl_rows):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state table privileges are invalid"
            )
        if any(
            grantee == 0
            or (grantee == writer_oid and grantor != owner_oid)
            or privilege != "SELECT"
            or grantable is not False
            for _table, grantee, grantor, privilege, grantable in table_acl_rows
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state table privileges are invalid"
            )
        writer_table_privileges = {
            (table, privilege)
            for table, grantee, _grantor, privilege, _grantable in table_acl_rows
            if grantee == writer_oid
        }
        if writer_table_privileges != {
            (table, "SELECT") for table in _EXPECTED_TABLES
        }:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state writer table privileges are invalid"
            )

        column_acl_rows = _rows(
            cursor,
            (
                "SELECT c.relname, a.attname, acl.grantee::bigint, "
                "acl.grantor::bigint, acl.privilege_type, acl.is_grantable "
                "FROM pg_catalog.pg_attribute AS a "
                "JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "CROSS JOIN LATERAL pg_catalog.aclexplode(a.attacl) AS acl "
                "WHERE n.nspname = %s AND c.relkind = 'r' AND a.attnum > 0 "
                "AND NOT a.attisdropped AND acl.grantee <> c.relowner "
                "ORDER BY c.relname, a.attnum, acl.grantee, acl.privilege_type"
            ),
            (self._schema,),
        )
        if any(len(row) != 6 for row in column_acl_rows):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state column privileges are invalid"
            )
        expected_writer_columns = {
            (table, column, privilege)
            for table, privilege, columns in _SCHEMA_V2_WRITER_COLUMN_PRIVILEGES
            for column in columns
        }
        actual_writer_columns: set[tuple[object, object, object]] = set()
        for table, column, grantee, grantor, privilege, grantable in column_acl_rows:
            if (
                grantee == 0
                or (grantee == writer_oid and grantor != owner_oid)
                or grantable is not False
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state column privileges are invalid"
                )
            if grantee == writer_oid:
                actual_writer_columns.add((table, column, privilege))
            elif privilege != "SELECT":
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state column privileges are invalid"
                )
        if actual_writer_columns != expected_writer_columns:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state writer column privileges are invalid"
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


def _prove_private_postgres_v2_writer(
    cursor: _Cursor,
    sql_module: _SqlModule,
    *,
    schema: str,
    address: StateAddress,
    lock_timeout_seconds: int,
) -> PostgresStateStatus:
    """Prove exact v2 catalog and direct writer-session identity in one tx."""
    status = PostgresStateAdministration(
        dsn="",
        schema=schema,
        lock_timeout_seconds=lock_timeout_seconds,
    )._read_status(cursor, sql_module, address)
    if status.store_status != "ready" or status.schema_version != POSTGRES_SCHEMA_V2_VERSION:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state writer catalog is invalid"
        )
    writer_row = _one_or_none(
        _rows(
            cursor,
            _query(
                sql_module,
                "SELECT writer_role_name FROM {} WHERE singleton IS TRUE LIMIT 2",
                schema,
                "store_metadata",
            ),
        ),
        label="writer identity",
    )
    if (
        writer_row is None
        or len(writer_row) != 1
        or not isinstance(writer_row[0], str)
        or not writer_row[0]
    ):
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state writer identity is invalid"
        )
    writer_name = writer_row[0]
    identity_rows = _rows(
        cursor,
        "SELECT session_user, current_user",
    )
    if identity_rows != [(writer_name, writer_name)]:
        raise StateBackendInvalidStateError(
            "PostgreSQL deployment state writer session is invalid"
        )
    return status


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
            schema_version=(
                POSTGRES_SCHEMA_VERSION
                if created_store
                else cast(int, status.schema_version)
            ),
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
            and status.schema_version == result.schema_version
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


class PrivatePostgresStateV2Migrator:
    """Direct-only v1-to-v2 catalog and writer-role administrator.

    This class is reachable only through the explicit administrative command.
    It never creates a role or makes the ordinary backend selectable.
    """

    __slots__ = ("_dsn", "_lock_timeout_seconds", "_schema", "_writer_role")

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        lock_timeout_seconds: int,
        writer_role: str,
    ) -> None:
        try:
            writer_role_length = len(writer_role.encode("utf-8"))
        except (AttributeError, UnicodeError):
            writer_role_length = 64
        if (
            not isinstance(writer_role, str)
            or not writer_role
            or "\x00" in writer_role
            or writer_role_length > 63
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state writer role confirmation is invalid"
            ) from None
        self._dsn = dsn
        self._schema = schema
        self._lock_timeout_seconds = lock_timeout_seconds
        self._writer_role = writer_role

    def migrate(
        self,
        *,
        confirmed_writer_role: str,
        confirmed_store_id: str,
    ) -> PostgresStateV2Migration:
        """Migrate exact v1, or exactly verify an existing same-role v2."""
        # Confirmation is deliberately checked before driver loading or any
        # connection attempt.  Neither role value is reflected in an error.
        try:
            canonical_store_id = str(uuid.UUID(confirmed_store_id))
        except (ValueError, AttributeError, TypeError):
            canonical_store_id = ""
        if (
            confirmed_writer_role != self._writer_role
            or canonical_store_id != confirmed_store_id
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration confirmation does not match"
            ) from None

        options = _dsn_tls_options(self._dsn)
        bundle = _load_psycopg()
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        result: PostgresStateV2Migration | None = None
        acquired_address_locks: list[int] = []
        schema_lock_acquired = False
        backend_pid: int | None = None
        transaction_started = False
        commit_attempted = False
        commit_acknowledged = False
        commit_uncertain = False
        committed = False
        release_unknown_after_commit = False
        invalid = False
        unavailable = False
        release_failed = False
        timeout_error: StateBackendLockTimeoutError | None = None
        interrupted: BaseException | None = None
        deadline = time.monotonic() + self._lock_timeout_seconds
        schema_lock_key = _initialization_lock_key(self._schema)

        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                autocommit=True,
                **options,
            )
            cursor = connection.cursor()
            self._configure_session(cursor)
            identity_rows = _rows(
                cursor,
                "SELECT pg_catalog.pg_backend_pid(), "
                "NOT pg_catalog.pg_is_in_recovery(), NOT EXISTS ("
                "SELECT 1 FROM pg_catalog.pg_locks AS l WHERE l.locktype = 'advisory' "
                "AND l.pid = pg_catalog.pg_backend_pid() AND l.granted)",
            )
            if (
                len(identity_rows) != 1
                or len(identity_rows[0]) != 3
                or type(identity_rows[0][0]) is not int
                or identity_rows[0][1] is not True
                or identity_rows[0][2] is not True
            ):
                raise StateBackendUnavailableError(
                    "PostgreSQL deployment state migration requires a direct primary session"
                )
            backend_pid = identity_rows[0][0]
            self._acquire_schema_lock(
                cursor,
                backend_pid=backend_pid,
                lock_key=schema_lock_key,
                deadline=deadline,
            )
            schema_lock_acquired = True

            cursor.execute(
                "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            transaction_started = True
            self._prove_migration_authority(
                cursor,
                backend_pid=backend_pid,
                schema_lock_key=schema_lock_key,
                address_lock_keys=(),
            )
            self._configure_transaction(cursor)
            source, address_keys = self._read_migration_source(cursor, bundle.sql)
            if source.store_id != confirmed_store_id:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state store confirmation does not match"
                )
            connection.rollback()
            transaction_started = False

            self._acquire_address_locks(
                cursor,
                address_keys,
                backend_pid=backend_pid,
                deadline=deadline,
                acquired=acquired_address_locks,
            )
            cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE")
            transaction_started = True
            self._prove_migration_authority(
                cursor,
                backend_pid=backend_pid,
                schema_lock_key=schema_lock_key,
                address_lock_keys=tuple(acquired_address_locks),
            )
            self._configure_transaction(cursor)
            result = self._migrate_transaction(
                cursor,
                bundle.sql,
                expected_store_id=confirmed_store_id,
                expected_schema_version=source.schema_version,
                acquired_address_locks=acquired_address_locks,
            )
            self._prove_migration_authority(
                cursor,
                backend_pid=backend_pid,
                schema_lock_key=schema_lock_key,
                address_lock_keys=tuple(acquired_address_locks),
            )
            if result.migrated:
                commit_attempted = True
                connection.commit()
                commit_acknowledged = True
                transaction_started = False
                committed = True
            else:
                connection.rollback()
                transaction_started = False

            verification = self._fresh_catalog_state(bundle)
            if not self._is_expected_postimage(verification, result):
                invalid = True
        except StateBackendLockTimeoutError as exc:
            timeout_error = exc
        except StateBackendInvalidStateError:
            if commit_attempted and not commit_acknowledged:
                commit_uncertain = True
                cursor, connection = self._close_uncertain_session(cursor, connection)
                classification = self._classify_commit(bundle, result, backend_pid)
                if classification == "committed":
                    committed = True
                elif classification == "committed_release_unknown":
                    committed = True
                    release_unknown_after_commit = True
                elif classification == "not_committed":
                    unavailable = True
                else:
                    invalid = False
            else:
                invalid = True
        except (KeyboardInterrupt, SystemExit) as exc:
            if commit_attempted and not commit_acknowledged:
                commit_uncertain = True
                cursor, connection = self._close_uncertain_session(cursor, connection)
                classification = self._classify_commit(bundle, result, backend_pid)
                if classification == "committed":
                    committed = True
                elif classification == "committed_release_unknown":
                    committed = True
                    release_unknown_after_commit = True
                elif classification == "not_committed":
                    unavailable = True
                else:
                    invalid = False
            else:
                interrupted = exc
        except Exception:
            if commit_attempted and not commit_acknowledged:
                commit_uncertain = True
                cursor, connection = self._close_uncertain_session(cursor, connection)
                classification = self._classify_commit(bundle, result, backend_pid)
                if classification == "committed":
                    committed = True
                elif classification == "committed_release_unknown":
                    committed = True
                    release_unknown_after_commit = True
                elif classification == "not_committed":
                    unavailable = True
            else:
                unavailable = True
        finally:
            if connection is not None and transaction_started and not commit_attempted:
                try:
                    connection.rollback()
                    transaction_started = False
                except Exception:
                    unavailable = True
            authority_released = False
            if (
                cursor is not None
                and not commit_uncertain
                and backend_pid is not None
            ):
                all_released = True
                for lock_key in reversed(acquired_address_locks):
                    try:
                        if _rows(
                            cursor,
                            "SELECT pg_catalog.pg_backend_pid(), "
                            "NOT pg_catalog.pg_is_in_recovery(), "
                            "CASE WHEN pg_catalog.pg_backend_pid() = %s "
                            "AND NOT pg_catalog.pg_is_in_recovery() "
                            "THEN pg_catalog.pg_advisory_unlock(%s) ELSE FALSE END",
                            (backend_pid, lock_key),
                        ) != [(backend_pid, True, True)]:
                            all_released = False
                    except Exception:
                        all_released = False
                if schema_lock_acquired:
                    try:
                        if _rows(
                            cursor,
                            "SELECT pg_catalog.pg_backend_pid(), "
                            "NOT pg_catalog.pg_is_in_recovery(), "
                            "CASE WHEN pg_catalog.pg_backend_pid() = %s "
                            "AND NOT pg_catalog.pg_is_in_recovery() "
                            "THEN pg_catalog.pg_advisory_unlock(%s, %s) "
                            "ELSE FALSE END",
                            (backend_pid, *schema_lock_key),
                        ) != [(backend_pid, True, True)]:
                            all_released = False
                    except Exception:
                        all_released = False
                if all_released:
                    try:
                        absence_rows = _rows(
                            cursor,
                            "SELECT pg_catalog.pg_backend_pid(), "
                            "NOT pg_catalog.pg_is_in_recovery(), NOT EXISTS ("
                            "SELECT 1 FROM pg_catalog.pg_locks AS l "
                            "WHERE l.locktype = 'advisory' "
                            "AND l.pid = pg_catalog.pg_backend_pid() AND l.granted)",
                        )
                        all_released = absence_rows == [(backend_pid, True, True)]
                    except Exception:
                        all_released = False
                authority_released = all_released
                release_failed = not all_released
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    if not authority_released:
                        release_failed = True
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    if not authority_released:
                        release_failed = True

        if (release_failed or release_unknown_after_commit) and committed:
            raise StateBackendReleaseAfterCommitError(
                "PostgreSQL deployment state migration committed but lock release failed"
            ) from None
        if interrupted is not None:
            raise interrupted
        if release_failed:
            unavailable = True
        if commit_attempted and commit_uncertain and not committed and not unavailable:
            raise StateBackendUnknownCommitError(
                "PostgreSQL deployment state migration outcome is unknown; run state status"
            ) from None
        if timeout_error is not None:
            raise timeout_error
        if invalid:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration is incompatible"
            ) from None
        if unavailable or result is None:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state migration is unavailable"
            ) from None
        return result

    def _configure_session(self, cursor: _Cursor) -> None:
        cursor.execute(
            "SELECT pg_catalog.set_config('statement_timeout', %s, false)",
            (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
        )
        cursor.execute(
            "SELECT pg_catalog.set_config('lock_timeout', %s, false)",
            (f"{self._lock_timeout_seconds * 1000}ms",),
        )

    def _configure_transaction(self, cursor: _Cursor) -> None:
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

    @staticmethod
    def _advisory_catalog_identity(lock_key: int) -> tuple[int, int, int]:
        unsigned = lock_key & ((1 << 64) - 1)
        return ((unsigned >> 32) & 0xFFFFFFFF, unsigned & 0xFFFFFFFF, 1)

    def _prove_migration_authority(
        self,
        cursor: _Cursor,
        *,
        backend_pid: int,
        schema_lock_key: tuple[int, int],
        address_lock_keys: tuple[int, ...],
    ) -> None:
        rows = _rows(
            cursor,
            (
                "SELECT pg_catalog.pg_backend_pid(), "
                "NOT pg_catalog.pg_is_in_recovery(), l.classid::bigint, "
                "l.objid::bigint, l.objsubid FROM pg_catalog.pg_locks AS l "
                "WHERE l.locktype = 'advisory' "
                "AND l.pid = pg_catalog.pg_backend_pid() AND l.granted "
                "ORDER BY l.classid, l.objid, l.objsubid"
            ),
        )
        expected_lock_ids = {
            (
                schema_lock_key[0] & 0xFFFFFFFF,
                schema_lock_key[1] & 0xFFFFFFFF,
                2,
            ),
            *(
                self._advisory_catalog_identity(lock_key)
                for lock_key in address_lock_keys
            ),
        }
        expected = sorted(
            (backend_pid, True, class_id, object_id, object_sub_id)
            for class_id, object_id, object_sub_id in expected_lock_ids
        )
        if rows != expected:
            raise StateBackendUnavailableError(
                "PostgreSQL deployment state migration authority is invalid"
            )

    def _migrate_transaction(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        *,
        expected_store_id: str,
        expected_schema_version: int | None,
        acquired_address_locks: list[int],
    ) -> PostgresStateV2Migration:
        initial, current_keys = self._read_migration_source(cursor, sql_module)
        if (
            initial.store_id != expected_store_id
            or initial.schema_version != expected_schema_version
            or current_keys != sorted(acquired_address_locks)
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration source changed"
            )
        if initial.schema_version is None:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration source is invalid"
            )
        self._validate_all_controls_clear(cursor, sql_module, acquired_address_locks)
        self._validate_all_durable_rows(
            cursor,
            sql_module,
            expected_store_id=initial.store_id,
            source_schema_version=initial.schema_version,
        )

        if initial.schema_version == POSTGRES_SCHEMA_V2_VERSION:
            if self._stored_writer_name(cursor, sql_module) != self._writer_role:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state writer identity is invalid"
                )
            return PostgresStateV2Migration(store_id=expected_store_id, migrated=False)

        self._apply_v2(cursor, sql_module)
        verified = PostgresStateAdministration(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
        )._read_status(cursor, sql_module, self._catalog_probe_address())
        if (
            verified.store_status != "ready"
            or verified.store_id != expected_store_id
            or verified.schema_version != POSTGRES_SCHEMA_V2_VERSION
            or self._stored_writer_name(cursor, sql_module) != self._writer_role
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration verification failed"
            )
        return PostgresStateV2Migration(store_id=expected_store_id, migrated=True)

    def _read_migration_source(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
    ) -> tuple[PostgresStateStatus, list[int]]:
        owner_rows = _rows(
            cursor,
            (
                "SELECT n.nspname, n.nspowner = (SELECT r.oid FROM "
                "pg_catalog.pg_roles AS r WHERE r.rolname = current_user) "
                "FROM pg_catalog.pg_namespace AS n WHERE n.nspname = %s "
                "ORDER BY n.nspname"
            ),
            (self._schema,),
        )
        if owner_rows != [(self._schema, True)]:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration administrator is invalid"
            )
        _validated_v2_role(
            cursor,
            schema=self._schema,
            writer_name=self._writer_role,
        )

        probe = self._catalog_probe_address()
        initial = PostgresStateAdministration(
            dsn=self._dsn,
            schema=self._schema,
            lock_timeout_seconds=self._lock_timeout_seconds,
        )._read_status(cursor, sql_module, probe)
        if initial.store_status != "ready" or initial.store_id is None:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration source is invalid"
            )
        if initial.schema_version not in (
            POSTGRES_SCHEMA_VERSION,
            POSTGRES_SCHEMA_V2_VERSION,
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state migration source is invalid"
            )
        return initial, self._read_address_keys(cursor, sql_module)

    def _read_address_keys(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
    ) -> list[int]:
        key_rows = _rows(
            cursor,
            _query(
                sql_module,
                "SELECT advisory_lock_key FROM {} ORDER BY advisory_lock_key",
                self._schema,
                "state_addresses",
            ),
        )
        keys: list[int] = []
        for row in key_rows:
            if len(row) != 1 or type(row[0]) is not int or row[0] in keys:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state address locks are invalid"
                )
            keys.append(row[0])
        return keys

    def _acquire_schema_lock(
        self,
        cursor: _Cursor,
        *,
        backend_pid: int,
        lock_key: tuple[int, int],
        deadline: float,
    ) -> None:
        while True:
            now = time.monotonic()
            if now >= deadline:
                raise StateBackendLockTimeoutError(
                    "PostgreSQL deployment state migration lock timed out"
                ) from None
            rows = _rows(
                cursor,
                "SELECT pg_catalog.pg_backend_pid(), "
                "NOT pg_catalog.pg_is_in_recovery(), "
                "CASE WHEN pg_catalog.pg_backend_pid() = %s "
                "AND NOT pg_catalog.pg_is_in_recovery() "
                "THEN pg_catalog.pg_try_advisory_lock(%s, %s) ELSE FALSE END",
                (backend_pid, *lock_key),
            )
            if rows == [(backend_pid, True, True)]:
                return
            if rows != [(backend_pid, True, False)]:
                raise StateBackendUnavailableError(
                    "PostgreSQL deployment state migration lock session is invalid"
                )
            now = time.monotonic()
            if now >= deadline:
                raise StateBackendLockTimeoutError(
                    "PostgreSQL deployment state migration lock timed out"
                ) from None
            time.sleep(min(0.05, deadline - now))

    def _acquire_address_locks(
        self,
        cursor: _Cursor,
        keys: list[int],
        *,
        backend_pid: int,
        deadline: float,
        acquired: list[int],
    ) -> None:
        for lock_key in keys:
            while True:
                now = time.monotonic()
                if now >= deadline:
                    raise StateBackendLockTimeoutError(
                        "PostgreSQL deployment state migration lock timed out"
                    ) from None
                rows = _rows(
                    cursor,
                    "SELECT pg_catalog.pg_backend_pid(), "
                    "NOT pg_catalog.pg_is_in_recovery(), "
                    "CASE WHEN pg_catalog.pg_backend_pid() = %s "
                    "AND NOT pg_catalog.pg_is_in_recovery() "
                    "THEN pg_catalog.pg_try_advisory_lock(%s) ELSE FALSE END",
                    (backend_pid, lock_key),
                )
                if rows == [(backend_pid, True, True)]:
                    acquired.append(lock_key)
                    break
                elif rows == [(backend_pid, True, False)]:
                    pass
                else:
                    raise StateBackendUnavailableError(
                        "PostgreSQL deployment state address lock session is invalid"
                    )
                now = time.monotonic()
                if now >= deadline:
                    raise StateBackendLockTimeoutError(
                        "PostgreSQL deployment state migration lock timed out"
                    ) from None
                time.sleep(min(0.05, deadline - now))

    def _validate_all_controls_clear(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        acquired_address_locks: list[int],
    ) -> None:
        rows = _rows(
            cursor,
            sql_module.SQL(
                "SELECT a.namespace, a.project, a.environment, a.address_uri, "
                "a.advisory_lock_key, o.revision, o.status, o.control_json, "
                "octet_length(o.control_json) FROM {} AS a LEFT JOIN {} AS o "
                "ON o.namespace = a.namespace AND o.project = a.project "
                "AND o.environment = a.environment ORDER BY a.advisory_lock_key"
            ).format(
                _qualified(sql_module, self._schema, "state_addresses"),
                _qualified(sql_module, self._schema, "operation_control"),
            ),
        )
        seen_keys: list[int] = []
        for row in rows:
            if len(row) != 9:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state operation control is invalid"
                )
            namespace, project, environment, uri, lock_key, revision, status, raw, size = row
            try:
                address = StateAddress(
                    namespace=cast(str, namespace),
                    project=cast(str, project),
                    environment=cast(str, environment),
                )
            except StateError:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state operation control is invalid"
                ) from None
            if (
                uri != address.uri
                or type(lock_key) is not int
                or lock_key != _advisory_lock_key(address)
                or type(revision) is not int
                or revision < 0
                or status != "clear"
                or type(size) is not int
                or size < 0
                or size > POSTGRES_STATE_MAX_BYTES
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state operation control is invalid"
                )
            try:
                control = OperationControlState.from_dict(
                    _strict_json(raw, label="operation control"),
                    expected_address=address,
                )
            except StateError:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state operation control is invalid"
                ) from None
            safe = _safe_operation_status(control)
            if (
                safe.status != "clear"
                or safe.operation_id is not None
                or safe.kind is not None
                or safe.failure_code is not None
                or safe.last_completed_action_index is not None
                or len(cast(str, raw).encode("utf-8")) != size
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state operation control is invalid"
                )
            seen_keys.append(lock_key)
        if seen_keys != sorted(acquired_address_locks):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state operation controls are incomplete"
            )

    def _validate_all_durable_rows(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
        *,
        expected_store_id: str,
        source_schema_version: int,
    ) -> None:
        current_rows = _rows(
            cursor,
            _query(
                sql_module,
                (
                    "SELECT namespace, project, environment, revision, state_serial, "
                    "state_checksum, state_json, octet_length(state_json) FROM {} "
                    "ORDER BY namespace, project, environment"
                ),
                self._schema,
                "current_state",
            ),
        )
        current: dict[StateAddress, tuple[int, LocalState, str, str]] = {}
        for row in current_rows:
            if len(row) != 8:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment ownership state is invalid"
                )
            namespace, project, environment, revision, serial, checksum, raw, size = row
            address = self._validated_row_address(namespace, project, environment)
            try:
                state, parsed_checksum = PostgresStateAdministration._parse_state_row(
                    (revision, serial, checksum, raw, size),
                    address,
                )
            except StateBackendInvalidStateError:
                raise
            if (
                address in current
                or not isinstance(raw, str)
                or raw != _canonical_json(state.to_dict())
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment ownership state is invalid"
                )
            current[address] = (
                cast(int, revision),
                state,
                parsed_checksum,
                raw,
            )

        state_history_rows = _rows(
            cursor,
            _query(
                sql_module,
                (
                    "SELECT namespace, project, environment, revision, state_serial, "
                    "state_checksum, state_json, operation_id::text, "
                    "octet_length(state_json) FROM {} ORDER BY namespace, project, "
                    "environment, revision"
                ),
                self._schema,
                "state_history",
            ),
        )
        state_history: dict[
            StateAddress,
            list[tuple[int, LocalState, str, str, str | None]],
        ] = {}
        history_operation_ids: set[tuple[StateAddress, str]] = set()
        for row in state_history_rows:
            if len(row) != 9:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state history is invalid"
                )
            namespace, project, environment, revision, serial, checksum, raw, op_id, size = row
            address = self._validated_row_address(namespace, project, environment)
            try:
                state, parsed_checksum = PostgresStateAdministration._parse_state_row(
                    (revision, serial, checksum, raw, size),
                    address,
                )
                if op_id is not None and str(uuid.UUID(cast(str, op_id))) != op_id:
                    raise ValueError
            except (StateBackendInvalidStateError, ValueError, TypeError, AttributeError):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state history is invalid"
                ) from None
            if not isinstance(raw, str) or raw != _canonical_json(state.to_dict()):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state history is invalid"
                )
            typed_operation_id = cast(str | None, op_id)
            if typed_operation_id is not None:
                history_key = (address, typed_operation_id)
                if history_key in history_operation_ids:
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment state history operation is invalid"
                    )
                history_operation_ids.add(history_key)
            state_history.setdefault(address, []).append(
                (
                    cast(int, revision),
                    state,
                    parsed_checksum,
                    raw,
                    typed_operation_id,
                )
            )

        for address, entries in state_history.items():
            revisions = [entry[0] for entry in entries]
            serials = [entry[1].serial for entry in entries]
            expected_sequence = list(range(1, len(entries) + 1))
            if revisions != expected_sequence or serials != expected_sequence:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state history is invalid"
                )
            current_entry = current.get(address)
            last = entries[-1]
            if (
                current_entry is None
                or current_entry[:4] != last[:4]
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment state history is invalid"
                )
        if set(current) != set(state_history):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state history is incomplete"
            )

        operation_rows = _rows(
            cursor,
            _query(
                sql_module,
                (
                    "SELECT namespace, project, environment, operation_id::text, "
                    "event_index, event_kind, control_json, octet_length(control_json) "
                    "FROM {} ORDER BY namespace, project, environment, operation_id, "
                    "event_index"
                ),
                self._schema,
                "operation_history",
            ),
        )
        operations: dict[
            tuple[StateAddress, str],
            list[tuple[int, str, OperationControlState]],
        ] = {}
        recovery_operations: dict[
            tuple[StateAddress, str],
            list[
                tuple[
                    int,
                    str,
                    RecoverySnapshotEvidence | RecoveryResolutionRecord,
                ]
            ],
        ] = {}
        for row in operation_rows:
            if len(row) != 8:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )
            namespace, project, environment, op_id, index, kind, raw, size = row
            address = self._validated_row_address(namespace, project, environment)
            allowed_event_kinds = (
                _V2_OPERATION_EVENT_KINDS
                if source_schema_version == POSTGRES_SCHEMA_V2_VERSION
                else _V1_OPERATION_EVENT_KINDS
            )
            if kind not in allowed_event_kinds:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )
            try:
                operation_id = str(uuid.UUID(cast(str, op_id)))
                if operation_id != op_id:
                    raise ValueError
                parsed = _strict_json(raw, label="operation history")
                if kind == "recovery_intent":
                    recovery_event: RecoverySnapshotEvidence | RecoveryResolutionRecord = (
                        RecoverySnapshotEvidence.from_dict(parsed)
                    )
                    canonical_event = recovery_event.to_dict()
                elif kind in _V2_RECOVERY_EVENT_KINDS:
                    recovery_event = RecoveryResolutionRecord.from_dict(parsed)
                    canonical_event = recovery_event.to_dict()
                else:
                    control = OperationControlState.from_dict(
                        parsed,
                        expected_address=address,
                    )
                    canonical_event = control.to_dict()
            except (StateError, ValueError, TypeError, AttributeError):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                ) from None
            if (
                type(index) is not int
                or index < 0
                or type(size) is not int
                or size < 0
                or size > POSTGRES_STATE_MAX_BYTES
                or not isinstance(raw, str)
                or len(raw.encode("utf-8")) != size
                or raw != _canonical_json(canonical_event)
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment operation history is invalid"
                )
            if kind in _V2_RECOVERY_EVENT_KINDS:
                recovery_operations.setdefault((address, operation_id), []).append(
                    (index, cast(str, kind), recovery_event)
                )
            else:
                operations.setdefault((address, operation_id), []).append(
                    (index, cast(str, kind), control)
                )

        successful_operations: dict[tuple[StateAddress, str], OperationIntent] = {}
        unfinished_operations: dict[
            tuple[StateAddress, str],
            tuple[OperationIntent, OperationControlState],
        ] = {}
        for (operation_address, operation_id), events in operations.items():
            latest_control = _validate_operation_history_states(
                events,
                address=operation_address,
                operation_id=operation_id,
            )
            base_intent = latest_control.intent
            assert base_intent is not None  # Validated by the timeline validator.
            if any(
                record.store.backend != "postgres"
                or record.store.store_id != expected_store_id
                for record in latest_control.resume_history
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment resume history belongs to another store"
                )
            if events[-1][1] not in {"succeeded", "cleared_before_mutation"}:
                unfinished_operations[(operation_address, operation_id)] = (
                    base_intent,
                    events[-1][2],
                )
            if events[-1][1] == "succeeded":
                successful_operations[(operation_address, operation_id)] = base_intent

        recovered_operations: set[tuple[StateAddress, str]] = set()
        changing_recoveries: dict[
            tuple[StateAddress, str],
            tuple[RecoverySnapshotEvidence, RecoveryResolutionRecord],
        ] = {}
        for (
            operation_address,
            operation_id,
        ), recovery_events in recovery_operations.items():
            if [event[0] for event in recovery_events] != [0, 1]:
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment recovery history is invalid"
                )
            first_index, first_kind, first_payload = recovery_events[0]
            final_index, final_kind, final_payload = recovery_events[1]
            if (
                first_index != 0
                or first_kind != "recovery_intent"
                or not isinstance(first_payload, RecoverySnapshotEvidence)
                or final_index != 1
                or not isinstance(final_payload, RecoveryResolutionRecord)
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment recovery history is invalid"
                )
            evidence = first_payload
            resolution = final_payload
            blocked_key = (operation_address, resolution.blocked_operation_id)
            blocked = unfinished_operations.get(blocked_key)
            prior_entries = state_history.get(operation_address, [])
            prior_state_is_known = (
                not evidence.state.resources
                if evidence.state.serial == 0
                else (
                    evidence.state.serial <= len(prior_entries)
                    and prior_entries[evidence.state.serial - 1][1] == evidence.state
                )
            )
            if (
                operation_id != resolution.recovery_operation_id
                or evidence.address != operation_address
                or resolution.address != operation_address
                or evidence.store.backend != "postgres"
                or evidence.store.store_id != expected_store_id
                or evidence.blocked_operation_id != resolution.blocked_operation_id
                or final_kind != _V2_RECOVERY_RESOLUTION_EVENTS.get(resolution.resolution)
                or blocked is None
                or blocked[1] != evidence.control
                or blocked[0].prior_state_serial != evidence.state.serial
                or blocked[0].prior_state_checksum != evidence.state_checksum
                or resolution.prior_state_serial != evidence.state.serial
                or resolution.prior_state_checksum != evidence.state_checksum
                or not prior_state_is_known
                or (
                    resolution.resolution == "abandoned_before_mutation"
                    and bool(evidence.control.progress)
                )
                or blocked_key in recovered_operations
            ):
                raise StateBackendInvalidStateError(
                    "PostgreSQL deployment recovery history is invalid"
                )
            recovered_operations.add(blocked_key)
            if resolution.state_changed:
                changing_recoveries[(operation_address, operation_id)] = (
                    evidence,
                    resolution,
                )
        if set(unfinished_operations) != recovered_operations:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment operation history is incomplete"
            )
        state_history_owners = set(successful_operations) | set(changing_recoveries)
        runner_replacements = {
            key for key, intent in successful_operations.items()
            if any(action.kafka_streams_evidence is not None for action in intent.actions)
        }
        if (
            not history_operation_ids <= state_history_owners
            or not set(changing_recoveries) <= history_operation_ids
            or not runner_replacements <= history_operation_ids
        ):
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state history operation is invalid"
            )
        for address, entries in state_history.items():
            previous_state = LocalState(
                project=address.project,
                environment=address.environment,
            )
            for _revision, state, _checksum, _raw, history_id in entries:
                if history_id is not None:
                    history_key = (address, history_id)
                    intent = successful_operations.get(history_key)
                    recovery = changing_recoveries.get(history_key)
                    valid_owner = False
                    if intent is not None:
                        try:
                            intent.validate_kafka_streams_prior_state(previous_state)
                            intent.validate_kafka_streams_result_state(state)
                        except StateError:
                            raise StateBackendInvalidStateError(
                                "PostgreSQL runner replacement ownership history is invalid"
                            ) from None
                        valid_owner = (
                            intent.prior_state_serial == previous_state.serial
                            and intent.prior_state_checksum == state_checksum(previous_state)
                            and state.serial == intent.prior_state_serial + 1
                        )
                    elif recovery is not None:
                        evidence, resolution = recovery
                        valid_owner = (
                            evidence.state == previous_state
                            and resolution.prior_state_serial == previous_state.serial
                            and resolution.prior_state_checksum == state_checksum(previous_state)
                            and resolution.result_state_serial == state.serial
                            and resolution.result_state_checksum == state_checksum(state)
                        )
                    if not valid_owner:
                        raise StateBackendInvalidStateError(
                            "PostgreSQL deployment state history operation is invalid"
                        )
                previous_state = state

    @staticmethod
    def _validated_row_address(
        namespace: object,
        project: object,
        environment: object,
    ) -> StateAddress:
        try:
            return StateAddress(
                namespace=cast(str, namespace),
                project=cast(str, project),
                environment=cast(str, environment),
            )
        except StateError:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state row address is invalid"
            ) from None

    def _apply_v2(self, cursor: _Cursor, sql_module: _SqlModule) -> None:
        metadata = _qualified(sql_module, self._schema, "store_metadata")
        cursor.execute(
            sql_module.SQL(
                "ALTER TABLE {} DROP CONSTRAINT store_metadata_schema_version_check"
            ).format(metadata)
        )
        cursor.execute(
            sql_module.SQL("ALTER TABLE {} ADD COLUMN writer_role_name text").format(
                metadata
            )
        )
        updated_at = datetime.now(timezone.utc)
        updated_rows = _rows(
            cursor,
            sql_module.SQL(
                "UPDATE {} SET schema_version = %s, writer_role_name = %s "
                "WHERE singleton IS TRUE AND schema_version = %s "
                "AND writer_role_name IS NULL RETURNING singleton"
            ).format(metadata),
            (POSTGRES_SCHEMA_V2_VERSION, self._writer_role, POSTGRES_SCHEMA_VERSION),
        )
        if updated_rows != [(True,)]:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state metadata migration is invalid"
            )
        cursor.execute(
            sql_module.SQL(
                "ALTER TABLE {} ALTER COLUMN writer_role_name SET NOT NULL"
            ).format(metadata)
        )
        cursor.execute(
            sql_module.SQL(
                "ALTER TABLE {} ADD CONSTRAINT store_metadata_schema_version_check "
                "CHECK (schema_version = 2)"
            ).format(metadata)
        )
        cursor.execute(
            sql_module.SQL(
                "ALTER TABLE {} ADD CONSTRAINT store_metadata_writer_role_name_check "
                "CHECK (writer_role_name <> '')"
            ).format(metadata)
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
            (*_EXPECTED_MIGRATION_V2, updated_at),
        )
        self._replace_writer_acl(cursor, sql_module)

    def _replace_writer_acl(self, cursor: _Cursor, sql_module: _SqlModule) -> None:
        role = sql_module.Identifier(self._writer_role)
        schema = sql_module.Identifier(self._schema)
        cursor.execute(
            sql_module.SQL("REVOKE ALL PRIVILEGES ON SCHEMA {} FROM {}").format(
                schema,
                role,
            )
        )
        cursor.execute(
            sql_module.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(schema, role)
        )
        for table in _EXPECTED_TABLES:
            relation = _qualified(sql_module, self._schema, table)
            cursor.execute(
                sql_module.SQL("REVOKE ALL PRIVILEGES ON TABLE {} FROM {}").format(
                    relation,
                    role,
                )
            )
            columns = tuple(
                column[1] for column in _EXPECTED_COLUMNS_V2 if column[0] == table
            )
            rendered_columns = sql_module.SQL(", ".join("{}" for _ in columns)).format(
                *(sql_module.Identifier(column) for column in columns)
            )
            for privilege in ("SELECT", "INSERT", "UPDATE", "REFERENCES"):
                cursor.execute(
                    sql_module.SQL(
                        f"REVOKE {privilege} ({{}}) ON TABLE {{}} FROM {{}}"
                    ).format(rendered_columns, relation, role)
                )
            cursor.execute(
                sql_module.SQL("GRANT SELECT ON TABLE {} TO {}").format(
                    relation,
                    role,
                )
            )
        for table, privilege, columns in _SCHEMA_V2_WRITER_COLUMN_PRIVILEGES:
            rendered_columns = sql_module.SQL(", ".join("{}" for _ in columns)).format(
                *(sql_module.Identifier(column) for column in columns)
            )
            cursor.execute(
                sql_module.SQL(f"GRANT {privilege} ({{}}) ON TABLE {{}} TO {{}}").format(
                    rendered_columns,
                    _qualified(sql_module, self._schema, table),
                    role,
                )
            )

    def _fresh_catalog_state(
        self,
        bundle: _PsycopgBundle,
        *,
        departed_backend_pid: int | None = None,
    ) -> tuple[PostgresStateStatus, str | None, bool | None]:
        options = _dsn_tls_options(self._dsn)
        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = bundle.driver.connect(
                self._dsn,
                connect_timeout=_CONNECT_TIMEOUT_SECONDS,
                **options,
            )
            cursor = connection.cursor()
            cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            verifier_identity = _rows(
                cursor,
                "SELECT pg_catalog.pg_backend_pid(), "
                "NOT pg_catalog.pg_is_in_recovery()",
            )
            if (
                len(verifier_identity) != 1
                or len(verifier_identity[0]) != 2
                or type(verifier_identity[0][0]) is not int
                or verifier_identity[0][1] is not True
            ):
                raise StateBackendUnavailableError(
                    "PostgreSQL deployment state migration verification requires "
                    "a direct primary session"
                )
            cursor.execute(
                "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"
            )
            cursor.execute(
                "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
                (f"{_STATEMENT_TIMEOUT_MILLISECONDS}ms",),
            )
            status = PostgresStateAdministration(
                dsn=self._dsn,
                schema=self._schema,
                lock_timeout_seconds=self._lock_timeout_seconds,
            )._read_status(cursor, bundle.sql, self._catalog_probe_address())
            writer_name = (
                self._stored_writer_name(cursor, bundle.sql)
                if status.schema_version == POSTGRES_SCHEMA_V2_VERSION
                else None
            )
            backend_departed: bool | None = None
            if departed_backend_pid is not None:
                departure_rows = _rows(
                    cursor,
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity "
                    "WHERE pid = %s)",
                    (departed_backend_pid,),
                )
                if departure_rows not in ([(True,)], [(False,)]):
                    raise StateBackendInvalidStateError(
                        "PostgreSQL deployment state migration session is invalid"
                    )
                backend_departed = departure_rows == [(True,)]
            return status, writer_name, backend_departed
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
                try:
                    connection.close()
                except Exception:
                    pass

    def _classify_commit(
        self,
        bundle: _PsycopgBundle,
        result: PostgresStateV2Migration | None,
        backend_pid: int | None,
    ) -> Literal[
        "committed",
        "committed_release_unknown",
        "not_committed",
        "unknown",
    ]:
        if result is None or backend_pid is None:
            return "unknown"
        try:
            observed = self._fresh_catalog_state(
                bundle,
                departed_backend_pid=backend_pid,
            )
        except (KeyboardInterrupt, SystemExit):
            return "unknown"
        except Exception:
            return "unknown"
        status, writer_name, backend_departed = observed
        if self._is_expected_postimage(observed, result):
            return "committed" if backend_departed is True else "committed_release_unknown"
        if (
            backend_departed is True
            and
            status.store_status == "ready"
            and status.store_id == result.store_id
            and status.schema_version == POSTGRES_SCHEMA_VERSION
            and writer_name is None
        ):
            return "not_committed"
        return "unknown"

    def _is_expected_postimage(
        self,
        observed: tuple[PostgresStateStatus, str | None, bool | None],
        result: PostgresStateV2Migration,
    ) -> bool:
        status, writer_name, _backend_departed = observed
        return (
            status.store_status == "ready"
            and status.store_id == result.store_id
            and status.schema_version == POSTGRES_SCHEMA_V2_VERSION
            and writer_name == self._writer_role
        )

    @staticmethod
    def _close_uncertain_session(
        cursor: _Cursor | None,
        connection: _Connection | None,
    ) -> tuple[None, None]:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        return None, None

    def _stored_writer_name(
        self,
        cursor: _Cursor,
        sql_module: _SqlModule,
    ) -> str:
        row = _one_or_none(
            _rows(
                cursor,
                _query(
                    sql_module,
                    "SELECT writer_role_name FROM {} WHERE singleton IS TRUE LIMIT 2",
                    self._schema,
                    "store_metadata",
                ),
            ),
            label="writer identity",
        )
        if row is None or len(row) != 1 or not isinstance(row[0], str) or not row[0]:
            raise StateBackendInvalidStateError(
                "PostgreSQL deployment state writer identity is invalid"
            )
        return row[0]

    @staticmethod
    def _catalog_probe_address() -> StateAddress:
        return StateAddress(
            namespace="streamt-internal",
            project="catalog-migration",
            environment="v2",
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


def make_postgres_state_v2_migrator(
    config: DeploymentStateConfig,
) -> PrivatePostgresStateV2Migrator:
    """Construct the explicit v1-to-v2 administrator without enabling state use."""
    if not isinstance(config, PostgresDeploymentStateConfig):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state migration is not configured"
        )
    writer_role_env = config.postgres.writer_role_env
    if writer_role_env is None:
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state migration credentials are unavailable"
        )
    dsn = os.environ.get(config.postgres.dsn_env)
    writer_role = os.environ.get(writer_role_env)
    if (
        dsn is None
        or not dsn.strip()
        or writer_role is None
        or not writer_role.strip()
    ):
        raise StateBackendUnavailableError(
            "PostgreSQL deployment state migration credentials are unavailable"
        )
    return PrivatePostgresStateV2Migrator(
        dsn=dsn,
        schema=config.postgres.schema_name,
        lock_timeout_seconds=config.lock_timeout_seconds,
        writer_role=writer_role,
    )
