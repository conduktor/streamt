"""Real command gates for the production PostgreSQL state factory.

Unlike the earlier provider-neutral command tests, these cases never replace a
command-local state factory.  They configure only the dedicated schema-v2
writer credential and therefore exercise the same factory path shipped in the
installed wheel.
"""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.compiler.manifest import TopicArtifact
from streamt.deployer.kafka import TopicState
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import DeploymentStateService, state_checksum
from tests.postgres.conftest import PostgresCase, WriterIdentity
from tests.postgres.test_postgres_state_commands_real import (
    _ENVIRONMENT,
    _LOGICAL_TOPIC,
    _NAMESPACE,
    _PROJECT,
    _TOPIC,
    _address,
    _assert_finalized,
    _kafka,
    _manifest,
    _operation_rows,
    _topic,
)

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_ADMIN_DSN_ENV = "STREAMT_ORDINARY_FACTORY_ADMIN_DSN"
_WRITER_DSN_ENV = "STREAMT_ORDINARY_FACTORY_WRITER_DSN"
_BASE_WHEEL_GATE_ENV = "STREAMT_EXPECT_POSTGRES_EXTRA_MISSING"


def _write_project(
    path: Path,
    case: PostgresCase,
    *,
    writer_binding: bool = True,
) -> None:
    postgres: dict[str, object] = {
        "dsn_env": _ADMIN_DSN_ENV,
        "schema": case.schema,
    }
    if writer_binding:
        postgres["writer_dsn_env"] = _WRITER_DSN_ENV
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker.invalid:9092"}
                },
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": _NAMESPACE,
                    "lock_timeout_seconds": 10,
                    "postgres": postgres,
                },
            }
        ),
        encoding="utf-8",
    )


def _initialize_v1(case: PostgresCase) -> str:
    return PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address()).store_id


def _initialize_v2(case: PostgresCase, writer: WriterIdentity) -> str:
    store_id = _initialize_v1(case)
    migrated = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=store_id,
        confirmed_writer_role=writer.role,
    )
    assert migrated.migrated is True
    return store_id


def _grant_status_reader(case: PostgresCase) -> None:
    with case.psycopg.connect(case.admin_dsn, autocommit=True) as connection:
        connection.execute(
            case.sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier(case.reader_role),
            )
        )
        connection.execute(
            case.sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier(case.reader_role),
            )
        )


def _verification_service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> DeploymentStateService:
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
        ),
        address=_address(),
    )


def _expected_state(topic: TopicArtifact) -> LocalState:
    return LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            resource_id(
                _PROJECT,
                _ENVIRONMENT,
                "topic",
                _LOGICAL_TOPIC,
            ): ManagedResourceRecord(
                physical_name=_TOPIC,
                ownership=topic.ownership.mode,
                artifact_checksum=artifact_checksum(topic.to_dict()),
                backend="direct-kafka",
            )
        },
    )


def _bind_writer_only(
    monkeypatch: pytest.MonkeyPatch,
    *,
    dsn: str | None,
) -> None:
    monkeypatch.delenv(_ADMIN_DSN_ENV, raising=False)
    if dsn is None:
        monkeypatch.delenv(_WRITER_DSN_ENV, raising=False)
    else:
        monkeypatch.setenv(_WRITER_DSN_ENV, dsn)
    assert _ADMIN_DSN_ENV not in os.environ


def _payload(result: Result) -> dict[str, object]:
    value = json.loads(result.stdout)
    assert isinstance(value, dict)
    return value


def _data(result: Result) -> dict[str, object]:
    value = _payload(result).get("data")
    assert isinstance(value, dict)
    return value


def _first_error(result: Result) -> dict[str, object]:
    errors = _payload(result).get("errors")
    assert isinstance(errors, list)
    assert errors
    error = errors[0]
    assert isinstance(error, dict)
    return error


def _assert_no_local_state(path: Path) -> None:
    assert not local_state_path(path, environment=_ENVIRONMENT).exists()
    assert not (path / ".streamt").exists()


def _assert_safe_factory_failure(
    result: Result,
    *,
    case: PostgresCase,
    writer: WriterIdentity | None = None,
) -> None:
    assert result.exit_code == 1, result.output
    error = _first_error(result)
    assert error["code"] in {
        "E411_STATE_INVALID",
        "E420_STATE_BACKEND_UNAVAILABLE",
    }
    for forbidden in (
        case.schema,
        case.owner_role,
        case.owner_dsn,
        _ADMIN_DSN_ENV,
        _WRITER_DSN_ENV,
        "owner-ci-",
        "writer-ci-",
    ):
        assert forbidden not in result.output
    if writer is not None:
        assert writer.role not in result.output
        assert writer.dsn not in result.output


def test_direct_apply_uses_production_factory_and_v2_writer(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    topic = _topic()
    kafka = _kafka(exists=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(topic)),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert _data(result)["state_serial"] == 1
    _assert_finalized(
        postgres_case,
        _verification_service(postgres_case, postgres_writer),
        _expected_state(topic),
        kind="apply",
        reviewed_plan_checksum=None,
    )
    _assert_no_local_state(tmp_path)


def test_online_plan_and_reviewed_apply_use_production_factory(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    store_id = _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    topic = _topic()
    manifest = _manifest(topic)
    plan_path = tmp_path / "reviewed.plan.json"

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        planned = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "plan",
                "-p",
                str(tmp_path),
                "--out",
                str(plan_path),
            ],
        )

    assert planned.exit_code == 0, planned.output
    plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
    initial = _verification_service(postgres_case, postgres_writer).read()
    assert plan_data["state"] == {
        "backend": "postgres",
        "store_id": store_id,
        "address": _address().uri,
        "serial": 0,
        "checksum": state_checksum(initial.state),
    }
    control, events, current_count = _operation_rows(postgres_case)
    assert control[0:2] == (0, "clear")
    assert events == []
    assert current_count == 0

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
    ):
        applied = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
            ],
        )

    assert applied.exit_code == 0, applied.output
    assert _data(applied)["plan_checksum"] == plan_data["checksum"]
    _assert_finalized(
        postgres_case,
        _verification_service(postgres_case, postgres_writer),
        _expected_state(topic),
        kind="apply",
        reviewed_plan_checksum=plan_data["checksum"],
    )
    serialized_plan = plan_path.read_text(encoding="utf-8")
    for forbidden in (
        postgres_case.schema,
        postgres_case.owner_role,
        postgres_writer.role,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
        _ADMIN_DSN_ENV,
        _WRITER_DSN_ENV,
        "owner-ci-",
        "writer-ci-",
    ):
        assert forbidden not in serialized_plan
    _assert_no_local_state(tmp_path)


def test_adopt_uses_production_factory_without_runtime_mutation(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    topic = _topic(mode="adopted")
    kafka = _kafka(exists=True)
    observed = TopicState(
        name=_TOPIC,
        exists=True,
        partitions=2,
        replication_factor=1,
        config={"cleanup.policy": "delete"},
    )
    kafka.get_topic_state.side_effect = [observed, observed]
    identity = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        _LOGICAL_TOPIC,
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(topic)),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "adopt",
                "-p",
                str(tmp_path),
                "-e",
                _ENVIRONMENT,
                "--kind",
                "topic",
                "--name",
                _LOGICAL_TOPIC,
                "--confirm-resource",
                identity,
                "--confirm-env",
                _ENVIRONMENT,
            ],
        )

    assert result.exit_code == 0, result.output
    assert _data(result)["adopted"] is True
    kafka.apply_topic.assert_not_called()
    _assert_finalized(
        postgres_case,
        _verification_service(postgres_case, postgres_writer),
        _expected_state(topic),
        kind="adopt",
        reviewed_plan_checksum=None,
    )
    _assert_no_local_state(tmp_path)


@pytest.mark.parametrize("writer_binding", [False, True])
def test_online_plan_fails_closed_without_writer_credential(
    tmp_path: Path,
    postgres_case: PostgresCase,
    monkeypatch: pytest.MonkeyPatch,
    writer_binding: bool,
) -> None:
    _write_project(tmp_path, postgres_case, writer_binding=writer_binding)
    _bind_writer_only(monkeypatch, dsn=None)
    runtime_factory = MagicMock(
        side_effect=AssertionError("runtime constructed before writer authority")
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_topic())),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            runtime_factory,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    _assert_safe_factory_failure(result, case=postgres_case)
    runtime_factory.assert_not_called()
    _assert_no_local_state(tmp_path)


def test_online_plan_rejects_v1_owner_bound_as_writer(
    tmp_path: Path,
    postgres_case: PostgresCase,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v1(postgres_case)
    _bind_writer_only(monkeypatch, dsn=postgres_case.owner_dsn)
    runtime_factory = MagicMock(
        side_effect=AssertionError("runtime constructed for version-one owner")
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_topic())),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            runtime_factory,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    _assert_safe_factory_failure(result, case=postgres_case)
    runtime_factory.assert_not_called()
    _assert_no_local_state(tmp_path)


def test_direct_apply_rejects_wrong_v2_session_identity_before_runtime(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_case.owner_dsn)
    kafka = _kafka(exists=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_topic())),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    _assert_safe_factory_failure(
        result,
        case=postgres_case,
        writer=postgres_writer,
    )
    kafka.apply_topic.assert_not_called()
    control, events, current_count = _operation_rows(postgres_case)
    assert control[0:2] == (0, "clear")
    assert events == []
    assert current_count == 0
    _assert_no_local_state(tmp_path)


def test_online_plan_rejects_v2_status_reader_before_runtime(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _grant_status_reader(postgres_case)
    _bind_writer_only(monkeypatch, dsn=postgres_case.reader_dsn)
    runtime_factory = MagicMock(
        side_effect=AssertionError("runtime constructed for status reader")
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_topic())),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            runtime_factory,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    _assert_safe_factory_failure(
        result,
        case=postgres_case,
        writer=postgres_writer,
    )
    runtime_factory.assert_not_called()
    control, events, current_count = _operation_rows(postgres_case)
    assert control[0:2] == (0, "clear")
    assert events == []
    assert current_count == 0
    _assert_no_local_state(tmp_path)


@pytest.mark.skipif(
    os.environ.get(_BASE_WHEEL_GATE_ENV) != "1",
    reason="runs only in CI's isolated base-wheel environment",
)
def test_base_wheel_without_postgres_extra_fails_safely(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert importlib.util.find_spec("psycopg") is None
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker.invalid:9092"}
                },
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": _NAMESPACE,
                    "postgres": {
                        "dsn_env": _ADMIN_DSN_ENV,
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": "ordinary_factory_base_wheel",
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    unavailable_dsn = (
        "postgresql://writer:base-wheel-secret@127.0.0.1:1/unavailable"
    )
    _bind_writer_only(monkeypatch, dsn=unavailable_dsn)
    runtime_factory = MagicMock(
        side_effect=AssertionError("runtime constructed without PostgreSQL extra")
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=_manifest(_topic())),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            runtime_factory,
        ),
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "plan", "-p", str(tmp_path)],
        )

    assert result.exit_code == 1, result.output
    assert _first_error(result)["code"] == "E420_STATE_BACKEND_UNAVAILABLE"
    assert unavailable_dsn not in result.output
    assert "base-wheel-secret" not in result.output
    assert "ordinary_factory_base_wheel" not in result.output
    runtime_factory.assert_not_called()
    _assert_no_local_state(tmp_path)
