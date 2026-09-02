"""Private PostgreSQL schema-v2 writer routing contracts."""

from __future__ import annotations

import inspect
from typing import cast

import pytest

import streamt.deployer.postgres_state_backend as postgres_backend
from streamt.deployer.postgres_state import (
    POSTGRES_SCHEMA_V2_VERSION,
    POSTGRES_SCHEMA_VERSION,
    PostgresStateAdministration,
    PostgresStateStatus,
    _PsycopgBundle,
)
from streamt.deployer.postgres_state import (
    _Cursor as _CursorProtocol,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    make_deployment_state_service,
)


class _Cursor:
    """No-SQL cursor used to prove routing cannot reach DML."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(
        self,
        query: object,
        params: tuple[object, ...] | None = None,
    ) -> object:
        del params
        rendered = str(query)
        self.calls.append(rendered)
        if rendered.lstrip().upper().startswith(
            ("INSERT ", "UPDATE ", "DELETE ", "MERGE ")
        ):
            raise AssertionError("writer authority proof reached DML")
        return self

    def fetchall(self) -> object:
        return ()

    def close(self) -> None:
        return None


def _address() -> StateAddress:
    return StateAddress(namespace="platform", project="payments", environment="prod")


def _status(*, version: int, registered: bool = True) -> PostgresStateStatus:
    address = _address()
    return PostgresStateStatus(
        store_status="ready",
        store_id="00000000-0000-4000-8000-000000000001",
        schema_version=version,
        address=address,
        address_status="registered" if registered else "unregistered",
        state_status="absent" if registered else "unregistered",
        state_serial=0 if registered else None,
        state_checksum=None,
        operation_status=None,
    )


def _bundle() -> _PsycopgBundle:
    return cast(_PsycopgBundle, type("Bundle", (), {"sql": object()})())


def _install_status(
    monkeypatch: pytest.MonkeyPatch,
    status: PostgresStateStatus,
) -> None:
    monkeypatch.setattr(
        PostgresStateAdministration,
        "_read_status",
        lambda _self, _cursor, _sql, _address: status,
    )


def _prove(cursor: _Cursor) -> None:
    postgres_backend._prove_mutation_authority(
        cast(_CursorProtocol, cursor),
        _bundle(),
        dsn="host=/var/run/postgresql dbname=state",
        schema="streamt",
        lock_timeout_seconds=3,
        address=_address(),
    )


def test_exact_v2_routes_to_the_canonical_writer_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _Cursor()
    _install_status(monkeypatch, _status(version=POSTGRES_SCHEMA_V2_VERSION))
    calls: list[tuple[object, object, str, StateAddress, int]] = []

    def prove_writer(
        received_cursor: object,
        sql_module: object,
        *,
        schema: str,
        address: StateAddress,
        lock_timeout_seconds: int,
    ) -> PostgresStateStatus:
        calls.append(
            (received_cursor, sql_module, schema, address, lock_timeout_seconds)
        )
        return _status(version=POSTGRES_SCHEMA_V2_VERSION)

    monkeypatch.setattr(
        postgres_backend,
        "_prove_private_postgres_v2_writer",
        prove_writer,
    )
    monkeypatch.setattr(
        postgres_backend,
        "_prove_v1_owner",
        lambda *_args: pytest.fail("v2 must not use owner authority"),
    )

    _prove(cursor)

    assert len(calls) == 1
    assert calls[0][0] is cursor
    assert calls[0][2:] == ("streamt", _address(), 3)
    assert cursor.calls == []


def test_exact_v1_keeps_the_isolated_owner_only_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _Cursor()
    _install_status(monkeypatch, _status(version=POSTGRES_SCHEMA_VERSION))
    owner_calls: list[tuple[object, str]] = []
    monkeypatch.setattr(
        postgres_backend,
        "_prove_v1_owner",
        lambda received, schema: owner_calls.append((received, schema)),
    )
    monkeypatch.setattr(
        postgres_backend,
        "_prove_private_postgres_v2_writer",
        lambda *_args, **_kwargs: pytest.fail("v1 must not use v2 writer authority"),
    )

    _prove(cursor)

    assert owner_calls == [(cursor, "streamt")]
    assert "postgres_state_backend" not in inspect.getsource(make_deployment_state_service)
    assert cursor.calls == []


@pytest.mark.parametrize(
    "failure",
    [
        "schema-v2 owner is not the stored writer",
        "session_user differs after SET ROLE",
        "stored writer identity differs",
        "writer role attributes or membership drifted",
        "writer ACL is missing, extra, or grantable",
    ],
)
def test_v2_principal_and_acl_failures_propagate_before_dml(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    cursor = _Cursor()
    _install_status(monkeypatch, _status(version=POSTGRES_SCHEMA_V2_VERSION))

    def reject(*_args: object, **_kwargs: object) -> PostgresStateStatus:
        raise StateBackendInvalidStateError(failure)

    monkeypatch.setattr(
        postgres_backend,
        "_prove_private_postgres_v2_writer",
        reject,
    )

    with pytest.raises(StateBackendInvalidStateError, match=failure):
        _prove(cursor)

    assert cursor.calls == []


@pytest.mark.parametrize(
    "status",
    [
        _status(version=3),
        _status(version=POSTGRES_SCHEMA_V2_VERSION, registered=False),
    ],
    ids=["unknown-version", "unregistered-address"],
)
def test_unknown_version_or_target_fails_before_any_writer_proof_or_dml(
    monkeypatch: pytest.MonkeyPatch,
    status: PostgresStateStatus,
) -> None:
    cursor = _Cursor()
    _install_status(monkeypatch, status)
    monkeypatch.setattr(
        postgres_backend,
        "_prove_v1_owner",
        lambda *_args: pytest.fail("invalid target reached v1 authority"),
    )
    monkeypatch.setattr(
        postgres_backend,
        "_prove_private_postgres_v2_writer",
        lambda *_args, **_kwargs: pytest.fail("invalid target reached v2 authority"),
    )

    with pytest.raises(StateBackendInvalidStateError):
        _prove(cursor)

    assert cursor.calls == []


def test_mutation_authority_is_proved_before_snapshot_cas_and_all_dml() -> None:
    source = inspect.getsource(postgres_backend._PostgresStateReadOperation._update_control)

    authority = source.index("_prove_mutation_authority(")
    snapshot_cas = source.index("_read_snapshot_transaction(")
    first_insert = source.index('"INSERT INTO {}')
    first_update = source.index('"UPDATE {}')

    assert authority < snapshot_cas < min(first_insert, first_update)
