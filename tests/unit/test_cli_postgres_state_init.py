"""CLI safety boundary for explicit PostgreSQL state initialization."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.postgres_state import PostgresStateInitialization
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendUnknownCommitError,
)


def _write_project(path: Path, *, postgres: bool = True) -> None:
    deployment_state: dict[str, object]
    if postgres:
        deployment_state = {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "STREAMT_TEST_INIT_DSN"},
        }
    else:
        deployment_state = {"backend": "local"}
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "postgres-init"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
                "deployment_state": deployment_state,
            }
        ),
        encoding="utf-8",
    )


def _address() -> StateAddress:
    return StateAddress(
        namespace="platform",
        project="postgres-init",
        environment="default",
    )


def _confirmation_args(address: StateAddress | None = None) -> list[str]:
    target = address or _address()
    return [
        "--confirm-project",
        target.project,
        "--confirm-env",
        target.environment,
        "--confirm-address",
        target.uri,
    ]


class _Initializer:
    def __init__(
        self,
        result: PostgresStateInitialization | None = None,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.addresses: list[StateAddress] = []

    def initialize(self, address: StateAddress) -> PostgresStateInitialization:
        self.addresses.append(address)
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result


def test_init_requires_exact_confirmations_then_uses_only_initializer_factory(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    address = _address()
    initializer = _Initializer(
        PostgresStateInitialization(
            store_id="00000000-0000-4000-8000-000000000001",
            address=address,
            created_store=True,
            registered_address=True,
        )
    )
    secret_dsn = "postgresql://alice:provider-secret@db.internal/state"
    with (
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
            return_value=initializer,
        ) as initializer_factory,
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_administration",
            side_effect=AssertionError("init used status factory"),
        ),
        patch(
            "streamt.cli.commands.state_cmd.make_deployment_state_service",
            side_effect=AssertionError("init used ordinary state factory"),
        ),
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("init constructed compiler"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "init",
                "-p",
                str(tmp_path),
                *_confirmation_args(address),
            ],
            env={"STREAMT_TEST_INIT_DSN": secret_dsn},
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["command"] == "state init"
    assert payload["data"] == {
        "backend": "postgres",
        "outcome": "initialized",
        "store_id": "00000000-0000-4000-8000-000000000001",
        "schema_version": 1,
        "address": address.uri,
        "address_status": "registered",
        "state_status": "absent",
        "operation_status": "clear",
        "ordinary_state_authority": "disabled",
    }
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
    initializer_factory.assert_called_once()
    assert initializer.addresses == [address]
    assert not (tmp_path / ".streamt").exists()


@pytest.mark.parametrize(
    "confirmations",
    [
        [],
        [
            "--confirm-project",
            "wrong-project",
            "--confirm-env",
            "default",
            "--confirm-address",
            _address().uri,
        ],
        [
            "--confirm-project",
            "postgres-init",
            "--confirm-env",
            "wrong-environment",
            "--confirm-address",
            _address().uri,
        ],
        [
            "--confirm-project",
            "postgres-init",
            "--confirm-env",
            "default",
            "--confirm-address",
            "streamt-state://platform/postgres-init/wrong",
        ],
    ],
)
def test_confirmation_mismatch_fails_before_initializer_factory_or_connection(
    confirmations: list[str],
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    secret_dsn = "postgresql://alice:provider-secret@db.internal/state"
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
        side_effect=AssertionError("confirmation mismatch constructed initializer"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "init",
                "-p",
                str(tmp_path),
                *confirmations,
            ],
            env={"STREAMT_TEST_INIT_DSN": secret_dsn},
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E411_STATE_INVALID"
    assert "exactly match" in payload["errors"][0]["message"]
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
    factory.assert_not_called()


def test_local_backend_is_rejected_without_constructing_initializer(tmp_path: Path) -> None:
    _write_project(tmp_path, postgres=False)
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
        side_effect=AssertionError("local state constructed PostgreSQL initializer"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "init",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    factory.assert_not_called()


def test_initializer_factory_runs_after_dotenv_and_exact_confirmation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    address = _address()
    secret_dsn = "postgresql://dotenv-user:dotenv-secret@dotenv.internal/state"
    (tmp_path / ".env").write_text(
        f"STREAMT_TEST_INIT_DSN={secret_dsn}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("STREAMT_TEST_INIT_DSN", raising=False)
    initializer = _Initializer(
        PostgresStateInitialization(
            store_id="00000000-0000-4000-8000-000000000001",
            address=address,
            created_store=False,
            registered_address=False,
        )
    )

    def factory(_config):
        assert os.environ["STREAMT_TEST_INIT_DSN"] == secret_dsn
        return initializer

    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
        side_effect=factory,
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "init",
                "-p",
                str(tmp_path),
                *_confirmation_args(address),
            ],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"]["outcome"] == "already_initialized"
    assert "dotenv-user" not in result.output
    assert "dotenv-secret" not in result.output
    assert "dotenv.internal" not in result.output


def test_unknown_commit_is_stable_e420_and_never_exposes_provider_detail(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    initializer = _Initializer(
        error=StateBackendUnknownCommitError(
            "PostgreSQL deployment state initialization outcome is unknown; "
            "run state status or repeat the same state init confirmation"
        )
    )
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
        return_value=initializer,
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "init",
                "-p",
                str(tmp_path),
                *_confirmation_args(),
            ],
            env={
                "STREAMT_TEST_INIT_DSN": (
                    "postgresql://alice:provider-secret@db.internal/state"
                )
            },
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    assert "outcome is unknown" in payload["errors"][0]["message"]
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
