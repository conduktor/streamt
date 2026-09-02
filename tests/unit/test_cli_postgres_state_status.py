"""CLI boundary tests for read-only PostgreSQL state administration."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.postgres_state import (
    PostgresStateStatus,
    SafeOperationStatus,
)
from streamt.deployer.state_backend import StateAddress


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "postgres-status"},
                "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": "platform",
                    "postgres": {"dsn_env": "STREAMT_TEST_STATE_DSN"},
                },
            }
        ),
        encoding="utf-8",
    )


class _Administration:
    def __init__(self, status: PostgresStateStatus) -> None:
        self._status = status
        self.addresses: list[StateAddress] = []

    def status(self, address: StateAddress) -> PostgresStateStatus:
        self.addresses.append(address)
        return self._status


def test_postgres_status_uses_only_separate_admin_factory_and_safe_output(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    address = StateAddress(
        namespace="platform",
        project="postgres-status",
        environment="default",
    )
    administration = _Administration(
        PostgresStateStatus(
            store_status="ready",
            store_id="00000000-0000-4000-8000-000000000001",
            schema_version=1,
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
    )
    with (
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_administration",
            return_value=administration,
        ) as admin_factory,
        patch(
            "streamt.cli.commands.state_cmd.make_deployment_state_service",
            side_effect=AssertionError("postgres status used ordinary state factory"),
        ),
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("postgres status constructed compiler"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "status", "-p", str(tmp_path)],
            env={
                "STREAMT_TEST_STATE_DSN": ("postgresql://alice:provider-secret@db.internal/state")
            },
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["command"] == "state status"
    assert payload["data"] == {
        "backend": "postgres",
        "store_status": "ready",
        "store_id": "00000000-0000-4000-8000-000000000001",
        "schema_version": 1,
        "address": address.uri,
        "address_status": "registered",
        "state_status": "absent",
        "state_serial": 0,
        "state_checksum": "sha256:" + "1" * 64,
        "operation_status": {
            "status": "clear",
            "operation_id": None,
            "kind": None,
            "failure_code": None,
            "last_completed_action_index": None,
        },
        "mutation_status": "disabled",
        "ordinary_state_authority": "disabled",
    }
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
    admin_factory.assert_called_once()
    assert administration.addresses == [address]
    assert not (tmp_path / ".streamt").exists()


def test_postgres_status_factory_runs_after_base_dotenv_is_applied(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_project(tmp_path)
    secret_dsn = "postgresql://dotenv-user:dotenv-secret@dotenv.internal/state"
    (tmp_path / ".env").write_text(
        f"STREAMT_TEST_STATE_DSN={secret_dsn}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("STREAMT_TEST_STATE_DSN", raising=False)
    address = StateAddress(
        namespace="platform",
        project="postgres-status",
        environment="default",
    )
    administration = _Administration(
        PostgresStateStatus(
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
    )

    def factory(_config):
        assert os.environ["STREAMT_TEST_STATE_DSN"] == secret_dsn
        return administration

    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_administration",
        side_effect=factory,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["data"]["store_status"] == "uninitialized"
    assert payload["data"]["mutation_status"] == "disabled"
    assert payload["data"]["ordinary_state_authority"] == "disabled"
    assert "dotenv-user" not in result.output
    assert "dotenv-secret" not in result.output
    assert "dotenv.internal" not in result.output


def test_postgres_optional_dependency_is_not_part_of_base_or_dev_extra() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    configuration = (repository_root / "pyproject.toml").read_text()
    base_dependencies, optional_dependencies = configuration.split(
        "[project.optional-dependencies]", 1
    )
    postgres_extra, dev_extra = optional_dependencies.split("dev = [", 1)

    assert 'postgres = [\n    "psycopg[binary]>=3.2,<4",\n]' in postgres_extra
    assert "psycopg" not in base_dependencies
    assert "psycopg" not in dev_extra.split("]", 1)[0]
