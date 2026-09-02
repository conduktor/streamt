"""CLI and factory boundaries for explicit PostgreSQL schema-v2 migration."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.core.deployment_state import validate_deployment_state_config
from streamt.deployer.postgres_state import (
    PostgresStateStatus,
    PostgresStateV2Migration,
    PrivatePostgresStateV2Migrator,
    SafeOperationStatus,
    make_postgres_state_v2_migrator,
)
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendLockTimeoutError,
    StateBackendReleaseAfterCommitError,
    StateBackendUnavailableError,
    StateBackendUnknownCommitError,
)

_STORE_ID = "00000000-0000-4000-8000-000000000001"
_WRITER_ROLE = "streamt_writer"


def _write_project(
    path: Path,
    *,
    postgres: bool = True,
    writer_role_env: str | None = "STREAMT_TEST_WRITER_ROLE",
    schema: str | None = None,
) -> None:
    deployment_state: dict[str, object]
    if postgres:
        postgres_config: dict[str, object] = {
            "dsn_env": "STREAMT_TEST_OWNER_DSN",
        }
        if writer_role_env is not None:
            postgres_config["writer_role_env"] = writer_role_env
        if schema is not None:
            postgres_config["schema"] = schema
        deployment_state = {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": postgres_config,
        }
    else:
        deployment_state = {"backend": "local"}
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "postgres-migration"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
                "deployment_state": deployment_state,
            }
        ),
        encoding="utf-8",
    )


def _confirmation_args(
    *,
    store_id: str = _STORE_ID,
    writer_role: str = _WRITER_ROLE,
) -> list[str]:
    return [
        "--confirm-store-id",
        store_id,
        "--confirm-writer-role",
        writer_role,
    ]


class _Migrator:
    def __init__(
        self,
        *,
        result: PostgresStateV2Migration | None = None,
        error: Exception | None = None,
    ) -> None:
        self._result = result
        self._error = error
        self.confirmations: list[tuple[str, str]] = []

    def migrate(
        self,
        *,
        confirmed_store_id: str,
        confirmed_writer_role: str,
    ) -> PostgresStateV2Migration:
        self.confirmations.append((confirmed_store_id, confirmed_writer_role))
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


def test_state_help_exposes_only_the_explicit_v2_migration_command() -> None:
    runner = CliRunner()

    state_help = runner.invoke(main, ["state", "--help"])
    command_help = runner.invoke(main, ["state", "migrate-postgres-v2", "--help"])

    assert state_help.exit_code == 0, state_help.output
    assert "migrate-postgres-v2" in state_help.output
    assert "Inspect, initialize, or migrate" in state_help.output
    assert command_help.exit_code == 0, command_help.output
    assert "--confirm-store-id" in command_help.output
    assert "--confirm-writer-role" in command_help.output


def test_migration_uses_only_explicit_admin_factory_and_safe_output(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    migrator = _Migrator(
        result=PostgresStateV2Migration(store_id=_STORE_ID, migrated=True)
    )
    owner_dsn = "postgresql://owner:owner-secret@db.internal/state"
    with (
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_v2_migrator",
            return_value=migrator,
        ) as migrator_factory,
        patch(
            "streamt.cli.commands.state_cmd.make_deployment_state_service",
            side_effect=AssertionError("migration used ordinary state factory"),
        ),
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
            side_effect=AssertionError("migration used initializer factory"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
            env={
                "STREAMT_TEST_OWNER_DSN": owner_dsn,
                "STREAMT_TEST_WRITER_ROLE": _WRITER_ROLE,
            },
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["command"] == "state migrate-postgres-v2"
    assert payload["data"] == {
        "backend": "postgres",
        "outcome": "migrated",
        "store_id": _STORE_ID,
        "schema_version": 2,
        "ordinary_state_authority": "disabled",
        "mutation_status": "catalog_ready",
    }
    serialized = json.dumps(payload)
    assert "owner-secret" not in serialized
    assert "db.internal" not in serialized
    assert _WRITER_ROLE not in serialized
    migrator_factory.assert_called_once()
    assert migrator.confirmations == [(_STORE_ID, _WRITER_ROLE)]
    assert not (tmp_path / ".streamt").exists()


def test_migration_human_output_excludes_connection_role_and_schema(
    tmp_path: Path,
) -> None:
    private_schema = "private_catalog"
    _write_project(tmp_path, schema=private_schema)
    migrator = _Migrator(
        result=PostgresStateV2Migration(store_id=_STORE_ID, migrated=False)
    )
    owner_dsn = "postgresql://owner:owner-secret@db.internal/state"
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_v2_migrator",
        return_value=migrator,
    ):
        result = CliRunner().invoke(
            main,
            [
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
            env={
                "STREAMT_TEST_OWNER_DSN": owner_dsn,
                "STREAMT_TEST_WRITER_ROLE": _WRITER_ROLE,
            },
        )

    assert result.exit_code == 0, result.output
    assert "Outcome: already_migrated" in result.output
    assert "Schema version: 2" in result.output
    assert "Catalog mutation readiness: catalog_ready" in result.output
    assert "Ordinary state authority: disabled" in result.output
    assert "owner-secret" not in result.output
    assert "db.internal" not in result.output
    assert _WRITER_ROLE not in result.output
    assert private_schema not in result.output


@pytest.mark.parametrize(
    "confirmations",
    [
        [],
        ["--confirm-store-id", _STORE_ID],
        ["--confirm-writer-role", _WRITER_ROLE],
        _confirmation_args(store_id="not-a-canonical-uuid"),
        _confirmation_args(store_id="AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        _confirmation_args(writer_role=""),
        _confirmation_args(writer_role="\ud800"),
    ],
)
def test_missing_or_malformed_confirmation_fails_before_parsing_and_factory(
    confirmations: list[str],
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_v2_migrator",
        side_effect=AssertionError("invalid confirmation constructed migrator"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *confirmations,
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E411_STATE_INVALID"
    assert "confirm" in payload["errors"][0]["message"]
    factory.assert_not_called()


def test_local_backend_is_rejected_without_constructing_migrator(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres=False)
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_v2_migrator",
        side_effect=AssertionError("local state constructed PostgreSQL migrator"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    factory.assert_not_called()


def test_missing_writer_role_config_fails_before_migrator_construction(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, writer_role_env=None)
    with patch(
        "streamt.deployer.postgres_state.PrivatePostgresStateV2Migrator",
        side_effect=AssertionError("missing config constructed migrator"),
    ) as migrator_type:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
            env={"STREAMT_TEST_OWNER_DSN": "postgresql://db/state"},
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0] == {
        "code": "E420_STATE_BACKEND_UNAVAILABLE",
        "message": (
            "PostgreSQL deployment state migration credentials are unavailable"
        ),
    }
    migrator_type.assert_not_called()


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (
            StateBackendLockTimeoutError("PostgreSQL migration lock timed out"),
            "E422_STATE_LOCK_TIMEOUT",
        ),
        (
            StateBackendUnknownCommitError("PostgreSQL migration outcome is unknown"),
            "E425_STATE_UNKNOWN_OUTCOME",
        ),
        (
            StateBackendReleaseAfterCommitError(
                "PostgreSQL migration committed but lock release failed"
            ),
            "E426_STATE_RELEASE_FAILED_AFTER_COMMIT",
        ),
        (
            StateBackendInvalidStateError("PostgreSQL migration is incompatible"),
            "E411_STATE_INVALID",
        ),
    ],
)
def test_migration_preserves_stable_provider_error_codes(
    error: Exception,
    code: str,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    migrator = _Migrator(error=error)
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_v2_migrator",
        return_value=migrator,
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "migrate-postgres-v2",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == code
    if isinstance(error, StateBackendReleaseAfterCommitError):
        assert payload["data"] == {
            "committed": True,
            "ordinary_state_authority": "disabled",
        }


@pytest.mark.parametrize(
    ("config_writer_env", "process_environment"),
    [
        (None, {"STREAMT_TEST_OWNER_DSN": "postgresql://db/state"}),
        ("STREAMT_TEST_WRITER_ROLE", {}),
        (
            "STREAMT_TEST_WRITER_ROLE",
            {"STREAMT_TEST_OWNER_DSN": "postgresql://db/state"},
        ),
        (
            "STREAMT_TEST_WRITER_ROLE",
            {
                "STREAMT_TEST_OWNER_DSN": "postgresql://db/state",
                "STREAMT_TEST_WRITER_ROLE": "   ",
            },
        ),
    ],
)
def test_migrator_factory_fails_closed_when_named_credentials_are_unavailable(
    config_writer_env: str | None,
    process_environment: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    postgres: dict[str, object] = {"dsn_env": "STREAMT_TEST_OWNER_DSN"}
    if config_writer_env is not None:
        postgres["writer_role_env"] = config_writer_env
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": postgres,
        }
    )
    monkeypatch.delenv("STREAMT_TEST_OWNER_DSN", raising=False)
    monkeypatch.delenv("STREAMT_TEST_WRITER_ROLE", raising=False)
    for name, value in process_environment.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(StateBackendUnavailableError) as exc_info:
        make_postgres_state_v2_migrator(config)

    assert str(exc_info.value) == (
        "PostgreSQL deployment state migration credentials are unavailable"
    )
    assert _WRITER_ROLE not in str(exc_info.value)


def test_migrator_factory_resolves_only_named_environment_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "lock_timeout_seconds": 17,
            "postgres": {
                "dsn_env": "STREAMT_TEST_OWNER_DSN",
                "writer_role_env": "STREAMT_TEST_WRITER_ROLE",
                "schema": "state_catalog",
            },
        }
    )
    owner_dsn = "postgresql://owner:owner-secret@db.internal/state"
    monkeypatch.setenv("STREAMT_TEST_OWNER_DSN", owner_dsn)
    monkeypatch.setenv("STREAMT_TEST_WRITER_ROLE", _WRITER_ROLE)

    with patch(
        "streamt.deployer.postgres_state.PrivatePostgresStateV2Migrator"
    ) as migrator_type:
        result = make_postgres_state_v2_migrator(config)

    assert result is migrator_type.return_value
    migrator_type.assert_called_once_with(
        dsn=owner_dsn,
        schema="state_catalog",
        lock_timeout_seconds=17,
        writer_role=_WRITER_ROLE,
    )


def test_migrator_constructor_rejects_unencodable_role_without_echoing_it() -> None:
    writer_role = "\ud800"

    with pytest.raises(StateBackendInvalidStateError) as exc_info:
        PrivatePostgresStateV2Migrator(
            dsn="postgresql://db/state",
            schema="streamt",
            lock_timeout_seconds=30,
            writer_role=writer_role,
        )

    assert str(exc_info.value) == (
        "PostgreSQL deployment state writer role confirmation is invalid"
    )
    assert writer_role not in str(exc_info.value)


def test_postgres_v2_human_status_separates_catalog_from_ordinary_authority(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    address = StateAddress(
        namespace="platform",
        project="postgres-migration",
        environment="default",
    )
    status = PostgresStateStatus(
        store_status="ready",
        store_id=_STORE_ID,
        schema_version=2,
        address=address,
        address_status="registered",
        state_status="absent",
        state_serial=0,
        state_checksum="sha256:" + "1" * 64,
        operation_status=SafeOperationStatus(
            status="clear",
            operation_id=None,
            kind=None,
            failure_code=None,
            last_completed_action_index=None,
        ),
    )

    class _Administration:
        def status(self, _address: StateAddress) -> PostgresStateStatus:
            assert _address == address
            return status

    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_administration",
        return_value=_Administration(),
    ):
        result = CliRunner().invoke(
            main,
            ["state", "status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert "Schema version: 2" in result.output
    assert "Catalog mutation readiness: catalog_ready" in result.output
    assert "Ordinary state authority: disabled" in result.output
    assert _WRITER_ROLE not in result.output
