"""Real PostgreSQL command E2E through explicitly injected private state.

The production deployment-state factory intentionally remains disabled. These
tests patch each command-local factory only long enough to exercise the
provider-neutral command protocol against an exact schema-v2 writer.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, TopicArtifact
from streamt.deployer.kafka import TopicChange, TopicState
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
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    StateAddress,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

_PROJECT = "command-e2e"
_ENVIRONMENT = "default"
_NAMESPACE = "platform"
_TOPIC = "payments.clean.v1"
_LOGICAL_TOPIC = "payments_clean"


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _write_project(path: Path, case: PostgresCase) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {"kafka": {"bootstrap_servers": "broker.invalid:9092"}},
        "deployment_state": {
            "backend": "postgres",
            "namespace": _NAMESPACE,
            "lock_timeout_seconds": 10,
            "postgres": {
                "dsn_env": "STREAMT_COMMAND_E2E_OWNER_DSN",
                "schema": case.schema,
                "writer_role_env": "STREAMT_COMMAND_E2E_WRITER_ROLE",
            },
        },
    }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(config),
        encoding="utf-8",
    )


def _topic(*, mode: str = "managed") -> TopicArtifact:
    return TopicArtifact(
        name=_TOPIC,
        partitions=3,
        replication_factor=1,
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_LOGICAL_TOPIC,
            mode=mode,
        ),
    )


def _manifest(topic: TopicArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name=_PROJECT,
        artifacts={"topics": [topic.to_dict()]},
    )


def _service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> tuple[DeploymentStateService, str]:
    address = _address()
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(address)
    migrated = PrivatePostgresStateV2Migrator(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
        writer_role=writer.role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer.role,
    )
    assert migrated.migrated is True
    return (
        DeploymentStateService(
            backend=PrivatePostgresStateReadBackend(
                dsn=writer.dsn,
                schema=case.schema,
                lock_timeout_seconds=10,
            ),
            address=address,
        ),
        initialized.store_id,
    )


def _kafka(*, exists: bool) -> MagicMock:
    deployer = MagicMock()

    def plan_topic(artifact: TopicArtifact) -> TopicChange:
        return TopicChange(
            topic=artifact.name,
            action="update" if exists else "create",
            current=TopicState(name=artifact.name, exists=exists),
            desired=artifact,
        )

    deployer.plan_topic.side_effect = plan_topic
    deployer.apply_topic.return_value = "updated" if exists else "created"
    deployer.get_consumer_groups.return_value = []
    return deployer


def _operation_rows(
    case: PostgresCase,
) -> tuple[tuple[object, ...], list[tuple[object, ...]], int]:
    address = _address()
    with case.psycopg.connect(case.owner_dsn) as connection:
        control = connection.execute(
            case.sql.SQL(
                "SELECT revision, status, control_json FROM {}.{} "
                "WHERE namespace = %s AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("operation_control"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()
        events = list(
            connection.execute(
                case.sql.SQL(
                    "SELECT operation_id::text, event_index, event_kind, control_json "
                    "FROM {}.{} WHERE namespace = %s AND project = %s "
                    "AND environment = %s ORDER BY recorded_at, event_index"
                ).format(
                    case.sql.Identifier(case.schema),
                    case.sql.Identifier("operation_history"),
                ),
                (address.namespace, address.project, address.environment),
            ).fetchall()
        )
        current_count = connection.execute(
            case.sql.SQL(
                "SELECT count(*) FROM {}.{} WHERE namespace = %s "
                "AND project = %s AND environment = %s"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("current_state"),
            ),
            (address.namespace, address.project, address.environment),
        ).fetchone()[0]
    assert control is not None
    return control, events, current_count


def _assert_finalized(
    case: PostgresCase,
    service: DeploymentStateService,
    expected: LocalState,
    *,
    kind: str,
    reviewed_plan_checksum: str | None,
) -> None:
    observation = service.read()
    control = service.read_control()
    assert observation.state == expected
    assert observation.state_serial == 1
    assert control.control == OperationControlState.clear(_address())
    assert control.revision.value == "postgres-v1:4"

    control_row, events, current_count = _operation_rows(case)
    assert control_row[0:2] == (4, "clear")
    assert json.loads(control_row[2]) == OperationControlState.clear(
        _address()
    ).to_dict()
    assert current_count == 1
    assert len(events) == 4
    operation_id = events[0][0]
    assert [(row[0], row[1], row[2]) for row in events] == [
        (operation_id, 0, "intent"),
        (operation_id, 1, "progress_started"),
        (operation_id, 2, "progress_completed"),
        (operation_id, 3, "succeeded"),
    ]
    intent = json.loads(events[0][3])["intent"]
    assert intent["kind"] == kind
    assert intent["reviewed_plan_checksum"] == reviewed_plan_checksum

    with case.psycopg.connect(case.owner_dsn) as connection:
        current = connection.execute(
            case.sql.SQL(
                "SELECT revision, state_serial, state_checksum, state_json FROM {}.{}"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("current_state"),
            )
        ).fetchone()
        history = connection.execute(
            case.sql.SQL(
                "SELECT revision, state_serial, state_checksum, state_json, "
                "operation_id::text FROM {}.{}"
            ).format(
                case.sql.Identifier(case.schema),
                case.sql.Identifier("state_history"),
            )
        ).fetchone()
    assert current is not None
    assert history is not None
    assert current[0:3] == (1, 1, state_checksum(expected))
    assert json.loads(current[3]) == expected.to_dict()
    assert history[0:4] == current
    assert history[4] == operation_id


def test_direct_apply_persists_intent_before_runtime_and_finalizes_exactly(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
) -> None:
    _write_project(tmp_path, postgres_case)
    service, _store_id = _service(postgres_case, postgres_writer)
    topic = _topic()
    manifest = _manifest(topic)
    kafka = _kafka(exists=False)
    runtime_evidence: list[dict[str, object]] = []

    def apply_topic(_artifact: TopicArtifact) -> str:
        control, events, current_count = _operation_rows(postgres_case)
        payload = json.loads(control[2])
        runtime_evidence.append(payload)
        assert control[0:2] == (2, "in_progress")
        assert current_count == 0
        assert [(row[1], row[2]) for row in events] == [
            (0, "intent"),
            (1, "progress_started"),
        ]
        assert payload["intent"]["kind"] == "apply"
        assert [item["status"] for item in payload["progress"]] == ["started"]
        return "created"

    kafka.apply_topic.side_effect = apply_topic
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=kafka,
        ),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        ) as state_factory,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path)],
        )

    assert result.exit_code == 0, result.output
    assert runtime_evidence
    assert json.loads(result.stdout)["data"]["state_serial"] == 1
    expected = LocalState(
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
                ownership="managed",
                artifact_checksum=artifact_checksum(topic.to_dict()),
                backend="direct-kafka",
            )
        },
    )
    _assert_finalized(
        postgres_case,
        service,
        expected,
        kind="apply",
        reviewed_plan_checksum=None,
    )
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()
    state_factory.assert_called_once()


def test_online_plan_binds_postgres_state_and_reviewed_apply_succeeds(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case)
    service, store_id = _service(postgres_case, postgres_writer)
    topic = _topic()
    manifest = _manifest(topic)
    plan_path = tmp_path / "reviewed.plan.json"
    monkeypatch.delenv("STREAMT_COMMAND_E2E_OWNER_DSN", raising=False)
    monkeypatch.delenv("STREAMT_COMMAND_E2E_WRITER_ROLE", raising=False)

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.plan.make_kafka_deployer",
            return_value=_kafka(exists=False),
        ),
        patch(
            "streamt.cli.commands.plan.make_deployment_state_service",
            return_value=service,
        ) as plan_state_factory,
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
    plan_state_factory.assert_called_once()
    serialized_plan = plan_path.read_text(encoding="utf-8")
    plan_data = json.loads(serialized_plan)
    initial = service.read()
    assert plan_data["state"] == {
        "backend": "postgres",
        "store_id": store_id,
        "address": _address().uri,
        "serial": 0,
        "checksum": state_checksum(initial.state),
    }
    assert set(plan_data["state"]) == {
        "backend",
        "store_id",
        "address",
        "serial",
        "checksum",
    }
    for forbidden in (
        postgres_case.schema,
        postgres_case.owner_role,
        postgres_writer.role,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
        "STREAMT_COMMAND_E2E_OWNER_DSN",
        "STREAMT_COMMAND_E2E_WRITER_ROLE",
        "owner-ci-",
        "writer-ci-",
        "state_file",
        "state_path",
        "provider_revision",
        "backend_revision",
    ):
        assert forbidden not in serialized_plan

    apply_kafka = _kafka(exists=False)
    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.apply.make_kafka_deployer",
            return_value=apply_kafka,
        ),
        patch(
            "streamt.cli.commands.apply.make_deployment_state_service",
            return_value=service,
        ) as apply_state_factory,
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
    apply_state_factory.assert_called_once()
    applied_data = json.loads(applied.stdout)["data"]
    assert applied_data["plan_checksum"] == plan_data["checksum"]
    expected = LocalState(
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
                ownership="managed",
                artifact_checksum=artifact_checksum(topic.to_dict()),
                backend="direct-kafka",
            )
        },
    )
    _assert_finalized(
        postgres_case,
        service,
        expected,
        kind="apply",
        reviewed_plan_checksum=plan_data["checksum"],
    )
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()


def test_adopt_reobserves_exact_target_and_commits_without_runtime_mutation(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
) -> None:
    _write_project(tmp_path, postgres_case)
    service, _store_id = _service(postgres_case, postgres_writer)
    topic = _topic(mode="adopted")
    manifest = _manifest(topic)
    observed = TopicState(
        name=_TOPIC,
        exists=True,
        partitions=2,
        replication_factor=1,
        config={"cleanup.policy": "delete"},
    )
    confirmed = TopicState(
        name=_TOPIC,
        exists=True,
        partitions=2,
        replication_factor=1,
        config={"cleanup.policy": "delete"},
    )
    kafka = _kafka(exists=True)
    kafka.get_topic_state.side_effect = [observed, confirmed]
    identity = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "topic",
        _LOGICAL_TOPIC,
    )

    with (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.adopt.make_kafka_deployer",
            return_value=kafka,
        ),
        patch(
            "streamt.cli.commands.adopt.make_deployment_state_service",
            return_value=service,
        ) as state_factory,
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
    state_factory.assert_called_once()
    data = json.loads(result.stdout)["data"]
    assert data["adopted"] is True
    assert data["resource_id"] == identity
    assert data["state_serial"] == 1
    assert kafka.get_topic_state.call_count == 2
    assert kafka.get_topic_state.call_args_list == [
        ((_TOPIC,), {"strict_config": True}),
        ((_TOPIC,), {"strict_config": True}),
    ]
    for method in (
        "apply_topic",
        "create_topic",
        "update_topic",
        "delete_topic",
        "apply",
    ):
        getattr(kafka, method).assert_not_called()

    expected = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            identity: ManagedResourceRecord(
                physical_name=_TOPIC,
                ownership="adopted",
                artifact_checksum=artifact_checksum(topic.to_dict()),
                backend="direct-kafka",
            )
        },
    )
    _assert_finalized(
        postgres_case,
        service,
        expected,
        kind="adopt",
        reviewed_plan_checksum=None,
    )
    assert not local_state_path(tmp_path, environment=_ENVIRONMENT).exists()
    assert not (tmp_path / ".streamt").exists()
