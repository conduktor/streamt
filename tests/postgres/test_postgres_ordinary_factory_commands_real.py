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
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    GatewayRuleArtifact,
    Manifest,
    TopicArtifact,
)
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ManagedConnectorObservation,
    bind_connector_artifact,
)
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayDeployer,
    ManagedGatewayRuleObservation,
    ManagedGatewaySnapshot,
    build_desired_gateway_rule,
)
from streamt.deployer.gateway_adoption import gateway_alias_mapping_checksum
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
from streamt.deployer.state_backend import (
    DeploymentStateService,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    state_checksum,
)
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
_CONNECT_ALIAS = "production"
_CONNECT_ENDPOINT = "https://connect.example.test:8443/api"
_CONNECTOR = "payments-sink"
_LOGICAL_CONNECTOR = "payments_sink"
_GATEWAY_ENDPOINT = "https://gateway.example.test:8443/admin"
_GATEWAY_VCLUSTER = "payments-prod"
_GATEWAY_RULE = "orders_view"
_GATEWAY_ALIAS = "orders.public"
_GATEWAY_DESIRED_TOPIC = "orders.desired.private"
_GATEWAY_OBSERVED_TOPIC = "orders.observed.private"
_GATEWAY_RUNTIME_USER = "gateway-runtime-user"
_GATEWAY_RUNTIME_PASSWORD = "gateway-runtime-password"


def _write_project(
    path: Path,
    case: PostgresCase,
    *,
    writer_binding: bool = True,
    connect_runtime: bool = False,
) -> None:
    postgres: dict[str, object] = {
        "dsn_env": _ADMIN_DSN_ENV,
        "schema": case.schema,
    }
    if writer_binding:
        postgres["writer_dsn_env"] = _WRITER_DSN_ENV
    runtime: dict[str, object] = {
        "kafka": {"bootstrap_servers": "broker.invalid:9092"}
    }
    if connect_runtime:
        runtime["connect"] = {
            "default": _CONNECT_ALIAS,
            "clusters": {
                _CONNECT_ALIAS: {
                    "rest_url": _CONNECT_ENDPOINT,
                    "username": "runtime-connect-user",
                    "password": "runtime-connect-password",
                }
            },
        }
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": runtime,
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


def _connector_binding() -> ConnectClusterBinding:
    return ConnectClusterBinding.from_endpoint(_CONNECT_ALIAS, _CONNECT_ENDPOINT)


def _connector() -> ConnectorArtifact:
    return ConnectorArtifact(
        name=_CONNECTOR,
        connector_class="io.desired.SecretSinkConnector",
        topics=["payments.desired.secret.v1"],
        config={
            "connection.url": (
                "https://desired-user:desired-url-password@warehouse.example.test/"
                "sink?token=desired-query-secret"
            ),
            "password": "desired-config-password",
            "sasl.jaas.config": (
                "org.example.Login required username=desired-jaas-user "
                'password="desired-jaas-password";'
            ),
        },
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_LOGICAL_CONNECTOR,
            mode="adopted",
        ),
    )


def _connector_manifest(connector: ConnectorArtifact) -> Manifest:
    return Manifest(
        version="1.0",
        project_name=_PROJECT,
        artifacts={"connectors": [connector.to_dict()]},
    )


def _connector_observation() -> ManagedConnectorObservation:
    config: dict[str, str | int] = {
        "name": _CONNECTOR,
        "connector.class": "io.live.SecretSourceConnector",
        "topics": "payments.live.secret.v1",
        "connection.url": (
            "https://live-user:live-url-password@warehouse.example.test/"
            "source?token=live-query-secret"
        ),
        "password": "live-config-password",
        "sasl.jaas.config": (
            "org.example.Login required username=live-jaas-user "
            'password="live-jaas-password";'
        ),
        "tasks.max": 1,
    }
    return ManagedConnectorObservation(
        binding=_connector_binding(),
        name=_CONNECTOR,
        exists=True,
        config=tuple(sorted(config.items())),
    )


def _write_gateway_adoption_project(path: Path, case: PostgresCase) -> None:
    """Write one real-compiler alias-only Gateway adoption project."""
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": _PROJECT},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                    "conduktor": {
                        "gateway": {
                            "admin_url": _GATEWAY_ENDPOINT,
                            "username": _GATEWAY_RUNTIME_USER,
                            "password": _GATEWAY_RUNTIME_PASSWORD,
                            "virtual_cluster": _GATEWAY_VCLUSTER,
                        }
                    },
                },
                "sources": [
                    {
                        "name": "orders_source",
                        "topic": _GATEWAY_DESIRED_TOPIC,
                    }
                ],
                "models": [
                    {
                        "name": _GATEWAY_RULE,
                        "materialized": "virtual_topic",
                        "gateway": {
                            "virtual_topic": {"name": _GATEWAY_ALIAS}
                        },
                        "ownership": {"mode": "adopted"},
                        "sql": 'SELECT * FROM {{ source("orders_source") }}',
                    }
                ],
                "deployment_state": {
                    "backend": "postgres",
                    "namespace": _NAMESPACE,
                    "lock_timeout_seconds": 10,
                    "postgres": {
                        "dsn_env": _ADMIN_DSN_ENV,
                        "writer_dsn_env": _WRITER_DSN_ENV,
                        "schema": case.schema,
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _gateway_adoption_artifact() -> GatewayRuleArtifact:
    return GatewayRuleArtifact(
        name=_GATEWAY_RULE,
        virtual_topic=_GATEWAY_ALIAS,
        physical_topic=_GATEWAY_DESIRED_TOPIC,
        interceptors=[],
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_GATEWAY_RULE,
            mode="adopted",
        ),
    )


def _gateway_adoption_reader(
    observation: ManagedGatewayRuleObservation,
) -> tuple[MagicMock, list[MagicMock]]:
    gateway = MagicMock(spec=GatewayDeployer)
    gateway.cluster_binding = observation.binding
    snapshots: list[MagicMock] = []
    for _ in range(2):
        snapshot = MagicMock(spec=ManagedGatewaySnapshot)
        snapshot.binding = observation.binding
        snapshot.rule.return_value = observation
        snapshots.append(snapshot)
    gateway.observe_managed_gateway_snapshot.side_effect = snapshots
    return gateway, snapshots


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


def test_connector_adopt_uses_production_v2_writer_and_secret_neutral_evidence(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, postgres_case, connect_runtime=True)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    connector = _connector()
    resolved = bind_connector_artifact(connector, _connector_binding())
    observed = _connector_observation()
    connect = MagicMock()
    connect.require_cluster_binding.return_value = _connector_binding()
    connect.observe_managed_connector.side_effect = [observed, observed]
    identity = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "connector",
        _LOGICAL_CONNECTOR,
    )

    with (
        patch(
            "streamt.compiler.Compiler.compile",
            return_value=_connector_manifest(connector),
        ),
        patch(
            "streamt.cli.commands.adopt.make_connect_deployer",
            return_value=connect,
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
                "connector",
                "--name",
                _LOGICAL_CONNECTOR,
                "--confirm-resource",
                identity,
                "--confirm-env",
                _ENVIRONMENT,
            ],
        )

    assert result.exit_code == 0, result.output
    data = _data(result)
    assert data["adopted"] is True
    assert data["resource_id"] == identity
    assert data["cluster_alias"] == _CONNECT_ALIAS
    assert data["endpoint_fingerprint"] == _connector_binding().endpoint_fingerprint
    assert data["observation_fingerprint"] == observed.fingerprint
    pending_diffs = data["pending_diffs"]
    changed_keys = data["changed_keys"]
    observed_data = data["observed"]
    desired_data = data["desired_managed_attributes"]
    assert isinstance(pending_diffs, dict)
    assert isinstance(changed_keys, list)
    assert isinstance(observed_data, dict)
    assert isinstance(desired_data, dict)
    assert changed_keys == sorted(pending_diffs)
    assert set(observed_data) == {"name", "config_checksum"}
    assert set(desired_data) == {
        "name",
        "cluster_alias",
        "config_checksum",
        "artifact_checksum",
    }
    assert connect.observe_managed_connector.call_args_list == [
        ((_CONNECTOR,), {}),
        ((_CONNECTOR,), {}),
    ]
    connect.require_cluster_binding.assert_called_once_with()
    connect.close.assert_called_once_with()
    for method in (
        "list_connectors",
        "get_connector_state",
        "get_connector_status",
        "create_connector",
        "update_connector",
        "delete_connector",
        "restart_connector",
        "pause_connector",
        "resume_connector",
        "plan_connector",
        "apply_change",
    ):
        getattr(connect, method).assert_not_called()

    serialized = json.dumps(_payload(result), sort_keys=True)
    for forbidden in (
        "runtime-connect-user",
        "runtime-connect-password",
        _CONNECT_ENDPOINT,
        "SecretSinkConnector",
        "SecretSourceConnector",
        "payments.desired.secret.v1",
        "payments.live.secret.v1",
        "desired-user",
        "desired-url-password",
        "desired-query-secret",
        "desired-config-password",
        "desired-jaas-user",
        "desired-jaas-password",
        "live-user",
        "live-url-password",
        "live-query-secret",
        "live-config-password",
        "live-jaas-user",
        "live-jaas-password",
        postgres_case.schema,
        postgres_case.owner_role,
        postgres_writer.role,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
    ):
        assert forbidden not in serialized

    expected = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            identity: ManagedResourceRecord(
                physical_name=_CONNECTOR,
                ownership="adopted",
                artifact_checksum=artifact_checksum(resolved.to_dict()),
                backend=_connector_binding().backend_identity,
            )
        },
    )
    _assert_finalized(
        postgres_case,
        _verification_service(postgres_case, postgres_writer),
        expected,
        kind="adopt",
        reviewed_plan_checksum=None,
    )
    _assert_no_local_state(tmp_path)


def test_gateway_adopt_uses_real_compiler_and_production_v2_writer(
    tmp_path: Path,
    postgres_case: PostgresCase,
    postgres_writer: WriterIdentity,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_gateway_adoption_project(tmp_path, postgres_case)
    _initialize_v2(postgres_case, postgres_writer)
    _bind_writer_only(monkeypatch, dsn=postgres_writer.dsn)
    artifact = _gateway_adoption_artifact()
    binding = GatewayBackendBinding.from_endpoint(
        _GATEWAY_ENDPOINT,
        virtual_cluster=_GATEWAY_VCLUSTER,
    )
    desired = build_desired_gateway_rule(artifact, binding)
    observed = ManagedGatewayRuleObservation(
        binding=binding,
        logical_name=artifact.name,
        alias_name=artifact.virtual_topic,
        exists=True,
        physical_name=_GATEWAY_OBSERVED_TOPIC,
        physical_cluster="main",
        interceptors=(),
    )
    gateway, snapshots = _gateway_adoption_reader(observed)
    identity = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        _GATEWAY_RULE,
    )

    with patch(
        "streamt.cli.commands.adopt.make_gateway_deployer",
        return_value=gateway,
    ) as gateway_factory:
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
                "gateway_rule",
                "--name",
                _GATEWAY_RULE,
                "--confirm-resource",
                identity,
                "--confirm-env",
                _ENVIRONMENT,
            ],
        )

    assert result.exit_code == 0, result.output
    data = _data(result)
    assert data["adopted"] is True
    assert data["already_owned"] is False
    assert data["committed"] is True
    assert data["resource_id"] == identity
    assert data["state_serial"] == 1
    assert data["pending_change_categories"] == ["alias_mapping"]
    assert data["observed_mapping_checksum"] == gateway_alias_mapping_checksum(
        _GATEWAY_OBSERVED_TOPIC,
        "main",
    )
    assert data["desired_mapping_checksum"] == gateway_alias_mapping_checksum(
        _GATEWAY_DESIRED_TOPIC,
        "main",
    )
    assert data["desired_artifact_checksum"] == artifact_checksum(
        artifact.to_dict()
    )
    assert data["observation_fingerprint"] == observed.fingerprint
    assert data["desired_aggregate_fingerprint"] == desired.fingerprint

    gateway_factory.assert_called_once()
    assert gateway.observe_managed_gateway_snapshot.call_count == 2
    for snapshot in snapshots:
        snapshot.rule.assert_called_once_with(_GATEWAY_RULE, _GATEWAY_ALIAS)
    for method in (
        "apply_managed_gateway_rule",
        "delete_managed_gateway_rule",
        "create_alias_topic",
        "delete_alias_topic",
        "create_interceptor",
        "delete_interceptor",
        "apply",
        "delete",
    ):
        getattr(gateway, method).assert_not_called()
    gateway.close.assert_called_once_with()

    expected_evidence = GatewayActionEvidence(
        version=1,
        backend_identity=binding.backend_identity,
        rule_name=artifact.name,
        alias_name=artifact.virtual_topic,
        current=GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint=observed.fingerprint,
            managed_interceptor_count=0,
        ),
        desired=GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint=desired.fingerprint,
            managed_interceptor_count=0,
        ),
    )
    expected = LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=1,
        resources={
            identity: ManagedResourceRecord(
                physical_name=_GATEWAY_ALIAS,
                ownership="adopted",
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend=binding.backend_identity,
            )
        },
    )
    service = _verification_service(postgres_case, postgres_writer)
    _assert_finalized(
        postgres_case,
        service,
        expected,
        kind="adopt",
        reviewed_plan_checksum=None,
    )
    _control, events, _current_count = _operation_rows(postgres_case)
    persisted_intent = json.loads(events[0][3])["intent"]
    assert persisted_intent["actions"] == [
        {
            "index": 0,
            "resource_id": identity,
            "action": "adopt",
            "gateway_evidence": expected_evidence.to_dict(),
            "connector_evidence": None,
        }
    ]
    assert service.read().state == expected

    serialized = json.dumps(_payload(result), sort_keys=True)
    for forbidden in (
        _GATEWAY_ENDPOINT,
        _GATEWAY_RUNTIME_USER,
        _GATEWAY_RUNTIME_PASSWORD,
        _GATEWAY_OBSERVED_TOPIC,
        _GATEWAY_DESIRED_TOPIC,
        postgres_case.schema,
        postgres_case.owner_role,
        postgres_writer.role,
        postgres_case.owner_dsn,
        postgres_writer.dsn,
    ):
        assert forbidden not in serialized
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
