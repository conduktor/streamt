"""CLI boundary tests for the PostgreSQL advisory-lock probe."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.deployer.state_backend import (
    StateAddress,
    StateBackendInvalidStateError,
    StateBackendUnavailableError,
)


def _write_project(path: Path, *, postgres: bool = True) -> None:
    deployment_state: dict[str, object]
    if postgres:
        deployment_state = {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "STREAMT_TEST_LOCK_DSN"},
        }
    else:
        deployment_state = {"backend": "local"}
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "postgres-lock"},
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
        project="postgres-lock",
        environment="default",
    )


@dataclass(frozen=True)
class _ProbeResult:
    address: StateAddress
    lock_status: str

    def to_dict(self) -> dict[str, object]:
        return {
            "backend": "postgres",
            "store_id": "00000000-0000-4000-8000-000000000001",
            "address": self.address.uri,
            "lock_status": self.lock_status,
            "reservation": "none",
            "ordinary_state_authority": "disabled",
        }


class _Probe:
    def __init__(
        self,
        *,
        lock_status: str = "available",
        error: Exception | None = None,
    ) -> None:
        self.lock_status = lock_status
        self.error = error
        self.addresses: list[StateAddress] = []

    def probe(self, address: StateAddress) -> _ProbeResult:
        self.addresses.append(address)
        if self.error is not None:
            raise self.error
        return _ProbeResult(address=address, lock_status=self.lock_status)


@pytest.mark.parametrize("lock_status", ["available", "busy", "unregistered"])
def test_lock_status_returns_structured_success_for_every_probe_outcome(
    lock_status: str,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    address = _address()
    probe = _Probe(lock_status=lock_status)
    secret_dsn = "postgresql://alice:provider-secret@db.internal/state"
    with (
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
            return_value=probe,
        ) as probe_factory,
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_administration",
            side_effect=AssertionError("lock probe used status factory"),
        ),
        patch(
            "streamt.cli.commands.state_cmd.make_postgres_state_initializer",
            side_effect=AssertionError("lock probe used initializer factory"),
        ),
        patch(
            "streamt.cli.commands.state_cmd.make_deployment_state_service",
            side_effect=AssertionError("lock probe used ordinary state factory"),
        ),
        patch(
            "streamt.compiler.Compiler",
            side_effect=AssertionError("lock probe constructed compiler"),
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "lock-status", "-p", str(tmp_path)],
            env={"STREAMT_TEST_LOCK_DSN": secret_dsn},
        )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["command"] == "state lock-status"
    assert payload["data"] == {
        "backend": "postgres",
        "store_id": "00000000-0000-4000-8000-000000000001",
        "address": address.uri,
        "lock_status": lock_status,
        "reservation": "none",
        "ordinary_state_authority": "disabled",
    }
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
    probe_factory.assert_called_once()
    assert probe.addresses == [address]
    assert not (tmp_path / ".streamt").exists()


def test_lock_status_text_is_explicitly_instantaneous_racy_and_non_reserving(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    probe = _Probe(lock_status="busy")
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
        return_value=probe,
    ):
        result = CliRunner().invoke(
            main,
            ["state", "lock-status", "-p", str(tmp_path)],
            env={"STREAMT_TEST_LOCK_DSN": "postgresql://localhost/state"},
        )

    assert result.exit_code == 0, result.output
    text = result.output.lower()
    assert "lock: busy" in text
    assert "instantaneous and racy" in text
    assert "reservation: none" in text
    assert "durable operation status" in text
    assert "state status" in text


def test_lock_status_uses_the_effective_named_environment(tmp_path: Path) -> None:
    _write_project(tmp_path)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "prod.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "unreachable.invalid:9092"}
                },
            }
        ),
        encoding="utf-8",
    )
    probe = _Probe(lock_status="unregistered")
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
        return_value=probe,
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "state",
                "lock-status",
                "-p",
                str(tmp_path),
                "-e",
                "prod",
            ],
            env={"STREAMT_TEST_LOCK_DSN": "postgresql://localhost/state"},
        )

    assert result.exit_code == 0, result.output
    assert probe.addresses == [
        StateAddress(
            namespace="platform",
            project="postgres-lock",
            environment="prod",
        )
    ]


def test_lock_probe_factory_runs_after_base_dotenv_is_applied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path)
    secret_dsn = "postgresql://dotenv-user:dotenv-secret@dotenv.internal/state"
    (tmp_path / ".env").write_text(
        f"STREAMT_TEST_LOCK_DSN={secret_dsn}\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("STREAMT_TEST_LOCK_DSN", raising=False)
    probe = _Probe(lock_status="available")

    def factory(_config: object) -> _Probe:
        assert os.environ["STREAMT_TEST_LOCK_DSN"] == secret_dsn
        return probe

    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
        side_effect=factory,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "lock-status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["lock_status"] == "available"
    assert "dotenv-user" not in result.output
    assert "dotenv-secret" not in result.output
    assert "dotenv.internal" not in result.output


def test_local_backend_is_rejected_as_e420_without_constructing_probe(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, postgres=False)
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
        side_effect=AssertionError("local state constructed PostgreSQL lock probe"),
    ) as factory:
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "lock-status", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    factory.assert_not_called()


@pytest.mark.parametrize(
    ("error_type", "expected_code"),
    [
        (StateBackendUnavailableError, "E420_STATE_BACKEND_UNAVAILABLE"),
        (StateBackendInvalidStateError, "E411_STATE_INVALID"),
    ],
)
def test_lock_probe_errors_use_stable_secret_neutral_mappings(
    error_type: type[Exception],
    expected_code: str,
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    secret_dsn = "postgresql://alice:provider-secret@db.internal/state?sslmode=require"
    probe = _Probe(error=error_type(f"lock probe failed via {secret_dsn}"))
    with patch(
        "streamt.cli.commands.state_cmd.make_postgres_state_lock_probe",
        return_value=probe,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "state", "lock-status", "-p", str(tmp_path)],
            env={"STREAMT_TEST_LOCK_DSN": secret_dsn},
        )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["errors"][0]["code"] == expected_code
    serialized = json.dumps(payload)
    assert "alice" not in serialized
    assert "provider-secret" not in serialized
    assert "db.internal" not in serialized
    assert "sslmode" not in serialized
