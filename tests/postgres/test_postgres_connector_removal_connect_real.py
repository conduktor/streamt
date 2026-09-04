"""Pinned real-Connect gate for the public reviewed Connector removal workflow."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import cast
from urllib.parse import quote

import pytest
import requests
import yaml
from click.testing import CliRunner, Result
from confluent_kafka.admin import AdminClient, NewTopic

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectDeployer,
    ConnectorConfigScalar,
    ManagedConnectorObservation,
)
from streamt.deployer.kafka import KafkaDeployer, TopicState
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.postgres_state_backend import PrivatePostgresStateReadBackend
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationIntent,
    StateAddress,
    operation_timestamp,
    state_checksum,
)
from tests.postgres.conftest import PostgresCase, WriterIdentity

pytestmark = [
    pytest.mark.integration,
    pytest.mark.connect,
    pytest.mark.kafka,
    pytest.mark.schema_registry,
    pytest.mark.postgres,
    pytest.mark.skipif(
        os.environ.get("STREAMT_TEST_REAL_CONNECT_REMOVAL") != "1",
        reason="real Connector removal gate is not enabled",
    ),
]

_PROJECT = "connector-removal-real"
_ENVIRONMENT = "default"
_NAMESPACE = "platform"
_CONNECT_ALIAS = "primary"
_CONNECT_URL = "http://127.0.0.1:8083"
_KAFKA_BOOTSTRAP = "127.0.0.1:9092"
_SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"
_OWNER_DSN_ENV = "STREAMT_REAL_CONNECT_OWNER_DSN"
_WRITER_DSN_ENV = "STREAMT_REAL_CONNECT_WRITER_DSN"
_HTTP_TIMEOUT_SECONDS = 10
_POLL_TIMEOUT_SECONDS = 30
_CLEANUP_HTTP_TIMEOUT_SECONDS = 2
_CLEANUP_STEP_TIMEOUT_SECONDS = 6

logger = logging.getLogger(__name__)


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _binding() -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint(_CONNECT_ALIAS, _CONNECT_URL)


def _artifact(
    *,
    owner: str,
    connector_name: str,
    topic_name: str,
    secret: str,
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=connector_name,
        connector_class="io.confluent.kafka.connect.datagen.DatagenConnector",
        topics=[topic_name],
        cluster=_CONNECT_ALIAS,
        config={
            "iterations": "1",
            "kafka.topic": topic_name,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "password": secret,
            "quickstart": "users",
            "tasks.max": "1",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=owner,
            mode="managed",
        ),
    )


def _request(
    method: str,
    url: str,
    *,
    json_body: object | None = None,
    timeout_seconds: int = _HTTP_TIMEOUT_SECONDS,
) -> requests.Response:
    return requests.request(
        method,
        url,
        json=json_body,
        timeout=timeout_seconds,
        allow_redirects=False,
    )


def _wait_for_status(url: str, expected: int) -> requests.Response:
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    latest: requests.Response | None = None
    while time.monotonic() < deadline:
        try:
            latest = _request("GET", url)
        except requests.RequestException:
            time.sleep(0.25)
            continue
        if latest.status_code == expected:
            return latest
        time.sleep(0.25)
    status = None if latest is None else latest.status_code
    pytest.fail(f"real service did not reach expected HTTP status; last status={status}")


def _wait_for_connector_stable(name: str) -> None:
    """Wait until Connect has completed the rebalance caused by fixture creation."""
    base_url = _connector_url(name)
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    latest: tuple[int | None, int | None] = (None, None)
    consecutive_ready = 0
    while time.monotonic() < deadline:
        try:
            document = _request("GET", base_url)
            status = _request("GET", f"{base_url}/status")
        except requests.RequestException:
            consecutive_ready = 0
            time.sleep(0.25)
            continue
        latest = (document.status_code, status.status_code)
        if latest == (200, 200):
            consecutive_ready += 1
            if consecutive_ready == 2:
                return
        else:
            consecutive_ready = 0
        time.sleep(0.25)
    pytest.fail(
        "real Connect fixture did not stabilize after creation; "
        f"last statuses={latest}"
    )


def _connector_url(name: str) -> str:
    return f"{_CONNECT_URL}/connectors/{quote(name, safe='')}"


def _create_connector(
    artifact: ConnectorArtifact,
) -> None:
    response: requests.Response | None = None
    deadline = time.monotonic() + _POLL_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        response = _request(
            "POST",
            f"{_CONNECT_URL}/connectors",
            json_body={
                "name": artifact.name,
                "config": artifact.to_dict()["config"],
            },
        )
        if response.status_code == 201:
            _wait_for_connector_stable(artifact.name)
            return
        if response.status_code == 409:
            time.sleep(0.25)
            continue
        pytest.fail(
            f"real Connect fixture creation returned unexpected HTTP {response.status_code}"
        )
    status = None if response is None else response.status_code
    pytest.fail(f"real Connect fixture creation timed out; last status={status}")


def _cleanup_connector(name: str) -> str | None:
    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = _request(
                "DELETE",
                _connector_url(name),
                timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            time.sleep(0.25)
            continue
        if response.status_code in (204, 404):
            break
        if response.status_code == 409 or response.status_code >= 500:
            time.sleep(0.25)
            continue
        return "Connector cleanup returned an unexpected HTTP status"
    else:
        return "Connector cleanup DELETE did not complete before its deadline"

    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = _request(
                "GET",
                _connector_url(name),
                timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            time.sleep(0.25)
            continue
        if response.status_code == 404:
            return None
        time.sleep(0.25)
    return "Connector cleanup could not verify exact absence"


def _connector_config(name: str) -> dict[str, object]:
    response = _wait_for_status(_connector_url(name), 200)
    try:
        body = response.json()
    except ValueError:
        pytest.fail("real Connect returned a non-JSON connector fixture")
    if not isinstance(body, dict) or not isinstance(body.get("config"), dict):
        pytest.fail("real Connect returned an invalid connector fixture")
    return cast(dict[str, object], body["config"])


def _connector_document_bytes(name: str) -> bytes:
    return _wait_for_status(_connector_url(name), 200).content


def _connector_names() -> set[str]:
    response = _wait_for_status(f"{_CONNECT_URL}/connectors", 200)
    try:
        names = response.json()
    except ValueError:
        pytest.fail("real Connect returned a non-JSON connector collection")
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        pytest.fail("real Connect returned an invalid connector collection")
    return set(names)


def _create_topic(admin: AdminClient, topic: str) -> None:
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result(timeout=_POLL_TIMEOUT_SECONDS)


def _cleanup_topic(admin: AdminClient, topic: str) -> str | None:
    try:
        admin.delete_topics([topic])[topic].result(timeout=_CLEANUP_STEP_TIMEOUT_SECONDS)
    except Exception:
        # The broker can commit a deletion before the client observes its result.
        pass

    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            metadata = admin.list_topics(timeout=_CLEANUP_HTTP_TIMEOUT_SECONDS)
        except Exception:
            time.sleep(0.25)
            continue
        if topic not in metadata.topics:
            return None
        time.sleep(0.25)
    return "Kafka cleanup could not verify exact topic absence"


def _register_schema(subject: str) -> None:
    response = _request(
        "POST",
        f"{_SCHEMA_REGISTRY_URL}/subjects/{quote(subject, safe='')}/versions",
        json_body={
            "schema": json.dumps(
                {
                    "type": "record",
                    "name": "RemovalSentinel",
                    "fields": [{"name": "id", "type": "string"}],
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
            "schemaType": "AVRO",
        },
    )
    if response.status_code not in (200, 201):
        pytest.fail(
            f"Schema Registry fixture creation returned unexpected HTTP {response.status_code}"
        )


def _schema_snapshot(subject: str) -> dict[str, object]:
    response = _wait_for_status(
        f"{_SCHEMA_REGISTRY_URL}/subjects/{quote(subject, safe='')}/versions/latest",
        200,
    )
    try:
        body = response.json()
    except ValueError:
        pytest.fail("Schema Registry returned a non-JSON fixture")
    if not isinstance(body, dict):
        pytest.fail("Schema Registry returned an invalid fixture")
    return cast(dict[str, object], body)


def _schema_subjects() -> set[str]:
    response = _wait_for_status(f"{_SCHEMA_REGISTRY_URL}/subjects", 200)
    try:
        subjects = response.json()
    except ValueError:
        pytest.fail("Schema Registry returned a non-JSON subject collection")
    if not isinstance(subjects, list) or any(not isinstance(subject, str) for subject in subjects):
        pytest.fail("Schema Registry returned an invalid subject collection")
    return set(subjects)


def _cleanup_schema_delete(url: str) -> bool:
    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = _request(
                "DELETE",
                url,
                timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            time.sleep(0.25)
            continue
        if response.status_code in (200, 404):
            return True
        if response.status_code == 409 or response.status_code >= 500:
            time.sleep(0.25)
            continue
        return False
    return False


def _cleanup_schema(subject: str) -> str | None:
    encoded = quote(subject, safe="")
    for suffix in ("", "?permanent=true"):
        if not _cleanup_schema_delete(f"{_SCHEMA_REGISTRY_URL}/subjects/{encoded}{suffix}"):
            return "Schema Registry cleanup DELETE did not complete"

    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    latest_url = f"{_SCHEMA_REGISTRY_URL}/subjects/{encoded}/versions/latest"
    while time.monotonic() < deadline:
        try:
            response = _request(
                "GET",
                latest_url,
                timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
            )
        except requests.RequestException:
            time.sleep(0.25)
            continue
        if response.status_code == 404:
            return None
        time.sleep(0.25)
    return "Schema Registry cleanup could not verify exact subject absence"


def _connector_names_for_cleanup() -> set[str] | None:
    try:
        response = _request(
            "GET",
            f"{_CONNECT_URL}/connectors",
            timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
        )
        names = response.json()
    except (requests.RequestException, ValueError):
        return None
    if response.status_code != 200:
        return None
    if not isinstance(names, list) or any(not isinstance(name, str) for name in names):
        return None
    return set(names)


def _schema_subjects_for_cleanup() -> set[str] | None:
    try:
        response = _request(
            "GET",
            f"{_SCHEMA_REGISTRY_URL}/subjects",
            timeout_seconds=_CLEANUP_HTTP_TIMEOUT_SECONDS,
        )
        subjects = response.json()
    except (requests.RequestException, ValueError):
        return None
    if response.status_code != 200:
        return None
    if not isinstance(subjects, list) or any(not isinstance(subject, str) for subject in subjects):
        return None
    return set(subjects)


def _wait_for_exact_cleanup_set(
    reader: Callable[[], set[str] | None],
    expected: set[str],
) -> bool:
    deadline = time.monotonic() + _CLEANUP_STEP_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        observed = reader()
        if observed == expected:
            return True
        time.sleep(0.25)
    return False


def _cleanup_fixtures(
    *,
    admin: AdminClient,
    kafka: KafkaDeployer,
    target_name: str,
    unrelated_name: str,
    target_topic: str,
    unrelated_topic: str,
    schema_subject: str,
    connector_baseline: set[str] | None,
    topic_baseline: set[str] | None,
    schema_baseline: set[str] | None,
) -> tuple[str, ...]:
    failures: list[str] = []

    cleanup_operations: list[tuple[str, Callable[[], str | None]]] = []
    if connector_baseline is not None:
        if target_name not in connector_baseline:
            cleanup_operations.append(("target Connector", lambda: _cleanup_connector(target_name)))
        if unrelated_name not in connector_baseline:
            cleanup_operations.append(
                ("unrelated Connector", lambda: _cleanup_connector(unrelated_name))
            )
    if topic_baseline is not None:
        if target_topic not in topic_baseline:
            cleanup_operations.append(("target topic", lambda: _cleanup_topic(admin, target_topic)))
        if unrelated_topic not in topic_baseline:
            cleanup_operations.append(
                ("unrelated topic", lambda: _cleanup_topic(admin, unrelated_topic))
            )
    if schema_baseline is not None and schema_subject not in schema_baseline:
        cleanup_operations.append(
            ("Schema Registry subject", lambda: _cleanup_schema(schema_subject))
        )

    for label, cleanup in cleanup_operations:
        try:
            failure = cleanup()
        except Exception:
            failure = "cleanup raised an unexpected exception"
        if failure is not None:
            failures.append(f"{label}: {failure}")

    restoration_checks: list[tuple[str, Callable[[], bool]]] = []
    if connector_baseline is not None:
        restoration_checks.append(
            (
                "Connector collection",
                lambda: _wait_for_exact_cleanup_set(
                    _connector_names_for_cleanup, connector_baseline
                ),
            )
        )
    if topic_baseline is not None:
        restoration_checks.append(
            (
                "Kafka topic collection",
                lambda: _wait_for_exact_cleanup_set(
                    lambda: set(kafka.list_topic_names()), topic_baseline
                ),
            )
        )
    if schema_baseline is not None:
        restoration_checks.append(
            (
                "Schema Registry subject collection",
                lambda: _wait_for_exact_cleanup_set(_schema_subjects_for_cleanup, schema_baseline),
            )
        )

    for label, restoration_check in restoration_checks:
        try:
            restored = restoration_check()
        except Exception:
            restored = False
        if not restored:
            failures.append(f"{label}: exact baseline restoration was not verified")
    return tuple(failures)


def _write_project(path: Path, case: PostgresCase, artifact: ConnectorArtifact) -> None:
    project = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {
            "kafka": {"bootstrap_servers": _KAFKA_BOOTSTRAP},
            "connect": {
                "default": _CONNECT_ALIAS,
                "clusters": {
                    _CONNECT_ALIAS: {
                        "rest_url": _CONNECT_URL,
                    }
                },
            },
        },
        "lifecycle": {
            "connector_removals": [
                {
                    "logical_owner": cast(ArtifactOwnership, artifact.ownership).owner_name,
                    "name": artifact.name,
                    "cluster": _CONNECT_ALIAS,
                }
            ]
        },
        "deployment_state": {
            "backend": "postgres",
            "namespace": _NAMESPACE,
            "lock_timeout_seconds": 10,
            "postgres": {
                "dsn_env": _OWNER_DSN_ENV,
                "writer_dsn_env": _WRITER_DSN_ENV,
                "schema": case.schema,
            },
        },
    }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )


def _initialize_service(
    case: PostgresCase,
    writer: WriterIdentity,
) -> DeploymentStateService:
    initialized = PostgresStateInitializer(
        dsn=case.owner_dsn,
        schema=case.schema,
        lock_timeout_seconds=10,
    ).initialize(_address())
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
    return DeploymentStateService(
        backend=PrivatePostgresStateReadBackend(
            dsn=writer.dsn,
            schema=case.schema,
            lock_timeout_seconds=10,
            require_v2_writer=True,
        ),
        address=_address(),
    )


def _seed_state(
    service: DeploymentStateService,
    target: ConnectorArtifact,
    topic_record_id: str,
    topic_record: ManagedResourceRecord,
) -> LocalState:
    owner = cast(ArtifactOwnership, target.ownership).owner_name
    state = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            resource_id(
                _PROJECT,
                _ENVIRONMENT,
                "connector",
                owner,
            ): ManagedResourceRecord(
                physical_name=target.name,
                ownership="managed",
                artifact_checksum=artifact_checksum(target.to_dict()),
                backend=_binding().backend_identity,
            ),
            topic_record_id: topic_record,
        },
    )
    with service.operation() as operation:
        observed = operation.observe()
        intent = OperationIntent(
            operation_id=str(uuid.uuid4()),
            kind="adopt",
            started_at=operation_timestamp(),
            actor="real-connect-removal-seed",
            prior_state_serial=observed.state.state_serial,
            prior_state_checksum=state_checksum(observed.state.state),
            reviewed_plan_checksum=None,
            actions=(),
        )
        active = operation.begin_operation(observed, intent)
        operation.commit_operation(active, state)
    return state


def _assert_cli_success_without_secrets(
    result: Result,
    forbidden: tuple[str, ...],
) -> None:
    for value in forbidden:
        assert value not in result.output
    assert result.exit_code == 0, "public Connector removal command failed"


@pytest.mark.parametrize(
    "encoded_name_case",
    [False, True],
    ids=("plain-name", "percent-encoded-name"),
)
def test_public_connector_removal_preserves_every_non_target_surface(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
    encoded_name_case: bool,
) -> None:
    """Delete one real Connector without touching any unrelated provider surface."""
    suffix = postgres_case.schema.removeprefix("streamt_ci_")
    target_name = (
        f"streamt removal+target {suffix}"
        if encoded_name_case
        else f"streamt-removal-target-{suffix}"
    )
    if encoded_name_case:
        assert "%" in quote(target_name, safe="")
    unrelated_name = f"streamt-removal-unrelated-{suffix}"
    target_topic = f"streamt_removal_target_{suffix}"
    unrelated_topic = f"streamt_removal_unrelated_{suffix}"
    schema_subject = f"streamt-removal-sentinel-{suffix}-value"
    target_secret = f"target-config-secret-{suffix}"
    unrelated_secret = f"unrelated-config-secret-{suffix}"
    target = _artifact(
        owner="target_owner",
        connector_name=target_name,
        topic_name=target_topic,
        secret=target_secret,
    )
    unrelated = _artifact(
        owner="unrelated_owner",
        connector_name=unrelated_name,
        topic_name=unrelated_topic,
        secret=unrelated_secret,
    )
    admin = AdminClient({"bootstrap.servers": _KAFKA_BOOTSTRAP})
    kafka = KafkaDeployer(_KAFKA_BOOTSTRAP)
    connector_baseline: set[str] | None = None
    topic_baseline: set[str] | None = None
    schema_baseline: set[str] | None = None
    primary_failure: BaseException | None = None

    try:
        _wait_for_status(f"{_CONNECT_URL}/", 200)
        _wait_for_status(f"{_SCHEMA_REGISTRY_URL}/subjects", 200)
        connector_baseline = _connector_names()
        topic_baseline = set(kafka.list_topic_names())
        schema_baseline = _schema_subjects()
        assert target_name not in connector_baseline
        assert unrelated_name not in connector_baseline
        assert target_topic not in topic_baseline
        assert unrelated_topic not in topic_baseline
        assert schema_subject not in schema_baseline
        _create_topic(admin, target_topic)
        _create_topic(admin, unrelated_topic)
        _register_schema(schema_subject)
        _create_connector(unrelated)
        _create_connector(target)

        target_config = target.to_dict()["config"]
        assert isinstance(target_config, dict)
        with ConnectDeployer(
            _CONNECT_URL,
            cluster_alias=_CONNECT_ALIAS,
        ) as connect:
            target_observation = connect.observe_managed_connector(target.name)
        assert target_observation == ManagedConnectorObservation(
            binding=_binding(),
            name=target.name,
            exists=True,
            config=cast(
                tuple[tuple[str, ConnectorConfigScalar], ...],
                tuple(sorted(target_config.items())),
            ),
        )
        connectors_before = _connector_names()
        topic_names_before = kafka.list_topic_names()
        schema_subjects_before = _schema_subjects()
        target_topic_before: TopicState = kafka.get_topic_state(
            target_topic,
            strict_config=True,
        )
        unrelated_topic_before: TopicState = kafka.get_topic_state(
            unrelated_topic,
            strict_config=True,
        )
        unrelated_config_before = _connector_config(unrelated_name)
        unrelated_document_before = _connector_document_bytes(unrelated_name)
        schema_before = _schema_snapshot(schema_subject)

        _write_project(tmp_path, postgres_case, target)
        state_service = _initialize_service(postgres_case, postgres_writer)
        topic_record_id = resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "topic",
            "unrelated_topic_owner",
        )
        topic_record = ManagedResourceRecord(
            physical_name=unrelated_topic,
            ownership="managed",
            artifact_checksum=artifact_checksum({"name": unrelated_topic}),
            backend="direct-kafka",
        )
        initial_state = _seed_state(
            state_service,
            target,
            topic_record_id,
            topic_record,
        )
        monkeypatch.delenv(_OWNER_DSN_ENV, raising=False)
        monkeypatch.setenv(_WRITER_DSN_ENV, postgres_writer.dsn)
        plan_path = tmp_path / "real-connect-removal.plan.json"
        runner = CliRunner()
        admin_details = postgres_case.conninfo.conninfo_to_dict(postgres_case.admin_dsn)
        owner_details = postgres_case.conninfo.conninfo_to_dict(postgres_case.owner_dsn)
        reader_details = postgres_case.conninfo.conninfo_to_dict(postgres_case.reader_dsn)
        writer_details = postgres_case.conninfo.conninfo_to_dict(postgres_writer.dsn)
        forbidden = (
            target_secret,
            unrelated_secret,
            _CONNECT_URL,
            _KAFKA_BOOTSTRAP,
            _SCHEMA_REGISTRY_URL,
            postgres_case.admin_dsn,
            postgres_case.owner_dsn,
            postgres_case.reader_dsn,
            postgres_writer.dsn,
            cast(str, admin_details["password"]),
            cast(str, owner_details["password"]),
            cast(str, reader_details["password"]),
            cast(str, writer_details["password"]),
            cast(str, admin_details["user"]),
            postgres_case.owner_role,
            postgres_case.reader_role,
            postgres_writer.role,
            postgres_case.schema,
            _OWNER_DSN_ENV,
            _WRITER_DSN_ENV,
        )

        planned = runner.invoke(
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
        _assert_cli_success_without_secrets(planned, forbidden)
        reviewed = ReviewedPlanFile.load(plan_path)
        assert len(reviewed.actions) == 1
        assert reviewed.actions[0].resource_id == resource_id(
            _PROJECT,
            _ENVIRONMENT,
            "connector",
            "target_owner",
        )
        assert reviewed.actions[0].action == "delete"
        plan_wire = plan_path.read_text(encoding="utf-8")
        for value in forbidden:
            assert value not in plan_wire

        applied = runner.invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(tmp_path),
                "--plan",
                str(plan_path),
                "--force",
            ],
        )
        _assert_cli_success_without_secrets(applied, forbidden)

        assert _wait_for_status(_connector_url(target.name), 404).status_code == 404
        assert _connector_names() == connectors_before - {target.name}
        assert _connector_config(unrelated_name) == unrelated_config_before
        assert _connector_document_bytes(unrelated_name) == unrelated_document_before
        assert kafka.list_topic_names() == topic_names_before
        assert kafka.get_topic_state(target_topic, strict_config=True) == target_topic_before
        assert kafka.get_topic_state(unrelated_topic, strict_config=True) == unrelated_topic_before
        assert _schema_subjects() == schema_subjects_before
        assert _schema_snapshot(schema_subject) == schema_before

        final_state = state_service.read().state
        assert final_state == LocalState(
            project=_PROJECT,
            environment=_ENVIRONMENT,
            serial=initial_state.serial + 1,
            resources={topic_record_id: topic_record},
        )
        assert state_service.read_control().control == OperationControlState.clear(_address())

        with postgres_case.psycopg.connect(postgres_case.owner_dsn) as connection:
            persisted = "\n".join(
                row[0]
                for table in (
                    "current_state",
                    "state_history",
                    "operation_control",
                    "operation_history",
                )
                for row in connection.execute(
                    postgres_case.sql.SQL(
                        "SELECT pg_catalog.row_to_json(row_value)::text FROM {}.{} "
                        "AS row_value ORDER BY pg_catalog.row_to_json(row_value)::text"
                    ).format(
                        postgres_case.sql.Identifier(postgres_case.schema),
                        postgres_case.sql.Identifier(table),
                    )
                ).fetchall()
            )
        for value in forbidden:
            assert value not in persisted
    except BaseException as error:
        primary_failure = error
        raise
    finally:
        try:
            cleanup_failures = _cleanup_fixtures(
                admin=admin,
                kafka=kafka,
                target_name=target_name,
                unrelated_name=unrelated_name,
                target_topic=target_topic,
                unrelated_topic=unrelated_topic,
                schema_subject=schema_subject,
                connector_baseline=connector_baseline,
                topic_baseline=topic_baseline,
                schema_baseline=schema_baseline,
            )
        except Exception:
            cleanup_failures = ("fixture cleanup orchestration failed",)
        try:
            kafka.close()
        except Exception:
            cleanup_failures += ("Kafka client cleanup failed",)
        if cleanup_failures:
            message = "real-provider fixture cleanup failures: " + "; ".join(cleanup_failures)
            if primary_failure is None:
                pytest.fail(message)
            add_note = getattr(primary_failure, "add_note", None)
            if callable(add_note):
                add_note(message)
            else:
                logger.error("%s", message)
