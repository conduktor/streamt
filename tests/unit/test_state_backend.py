"""Characterization and contract tests for deployment state backends."""

from __future__ import annotations

import json
import stat
import tempfile
from contextlib import AbstractContextManager
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.core.deployment_state import (
    local_deployment_state_config,
    validate_deployment_state_config,
)
from streamt.core.parser import ProjectParser
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    StateConflictError,
    StateFormatError,
    StateIdentityError,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    LOCAL_STATE_NAMESPACE,
    DeploymentStateBackend,
    DeploymentStateOperation,
    DeploymentStateService,
    LocalDeploymentStateBackend,
    StateAddress,
    StateBackendUnavailableError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    make_deployment_state_service,
    state_checksum,
)


def _address(*, environment: str = "prod") -> StateAddress:
    return StateAddress(
        namespace=LOCAL_STATE_NAMESPACE,
        project="payments",
        environment=environment,
    )


def _state(*, serial: int, partitions: int = 3) -> LocalState:
    identity = resource_id("payments", "prod", "topic", "payments_clean")
    artifact = {"name": "payments.clean.v1", "partitions": partitions}
    return LocalState(
        project="payments",
        environment="prod",
        serial=serial,
        resources={
            identity: ManagedResourceRecord(
                physical_name="payments.clean.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum(artifact),
                backend="direct-kafka",
            )
        },
    )


def test_state_address_is_canonical_and_strict() -> None:
    address = _address()

    assert address.uri == "streamt-state://local/payments/prod"
    assert StateAddress.parse(address.uri) == address
    with pytest.raises(StateFormatError, match="namespace"):
        StateAddress(namespace="team/platform", project="payments", environment="prod")
    with pytest.raises(StateFormatError, match="environment"):
        StateAddress(namespace="local", project="payments", environment="../prod")
    with pytest.raises(StateFormatError, match="must be"):
        StateAddress.parse("streamt-state://local/payments/prod/extra")


def test_local_store_identity_is_stable_and_path_content_is_not_exposed(
    tmp_path: Path,
) -> None:
    first = LocalDeploymentStateBackend(tmp_path).describe()
    repeated = LocalDeploymentStateBackend(tmp_path).describe()
    other = LocalDeploymentStateBackend(tmp_path / "other").describe()

    assert first == repeated
    assert first.backend == "local"
    assert first.store_id != other.store_id
    assert str(tmp_path) not in first.store_id

    with pytest.raises(StateFormatError, match="lowercase backend identifier"):
        StateStoreIdentity(backend="Postgres URL", store_id=first.store_id)
    with pytest.raises(StateFormatError, match="canonical UUID"):
        StateStoreIdentity(backend="postgres", store_id="password=provider-secret")


def test_absent_address_is_explicit_and_read_only(tmp_path: Path) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)

    observation = backend.read(_address())

    assert observation.revision == StateRevision.absent()
    assert observation.revision.is_absent is True
    assert observation.state == LocalState(project="payments", environment="prod")
    assert not local_state_path(tmp_path, environment="prod").exists()

    with pytest.raises(StateFormatError, match="empty serial-zero"):
        StateObservation(
            store=backend.describe(),
            address=_address(),
            state=_state(serial=1),
            revision=StateRevision.absent(),
        )


def test_factory_constructs_explicit_local_config(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="prod",
        config=local_deployment_state_config(),
    )

    assert isinstance(service.backend, LocalDeploymentStateBackend)
    assert service.store.backend == "local"
    assert service.address == _address()


def test_factory_requires_explicit_provider_config(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="config"):
        make_deployment_state_service(  # type: ignore[call-arg]
            tmp_path,
            project="payments",
            environment="prod",
        )


@pytest.mark.parametrize("dsn", [None, "", "   ", "postgresql://u:p@db.internal/state"])
def test_postgres_factory_is_sanitized_unavailable_and_never_falls_back_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dsn: str | None,
) -> None:
    config = validate_deployment_state_config(
        {
            "backend": "postgres",
            "namespace": "platform",
            "postgres": {"dsn_env": "SECRET_STATE_DSN"},
        }
    )
    if dsn is None:
        monkeypatch.delenv("SECRET_STATE_DSN", raising=False)
    else:
        monkeypatch.setenv("SECRET_STATE_DSN", dsn)

    def fail_local(_project_path: Path) -> object:
        raise AssertionError("PostgreSQL configuration fell back to local state")

    monkeypatch.setattr(
        "streamt.deployer.state_backend.LocalDeploymentStateBackend",
        fail_local,
    )

    with pytest.raises(StateBackendUnavailableError) as raised:
        make_deployment_state_service(
            tmp_path,
            project="payments",
            environment="prod",
            config=config,
        )

    message = str(raised.value)
    assert "SECRET_STATE_DSN" not in message
    assert "db.internal" not in message
    assert "postgresql://" not in message


def test_state_checksum_is_canonical_public_and_covers_complete_content(
    tmp_path: Path,
) -> None:
    original = _state(serial=7, partitions=3)
    same = LocalState.from_dict(original.to_dict())
    content_changed = _state(serial=7, partitions=6)
    serial_changed = _state(serial=8, partitions=3)

    assert state_checksum(original) == state_checksum(same)
    assert state_checksum(original) != state_checksum(content_changed)
    assert state_checksum(original) != state_checksum(serial_changed)

    original.save(local_state_path(tmp_path, environment="prod"))
    observation = LocalDeploymentStateBackend(tmp_path).read(_address())
    assert observation.revision.value == state_checksum(original)


def test_local_backend_preserves_v1_persistence_bytes_and_file_mode(
    tmp_path: Path,
) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    address = _address()
    replacement = _state(serial=1)
    expected_path = tmp_path / "expected.json"
    replacement.save(expected_path)

    with backend.operation(address) as operation:
        prior = operation.read()
        committed = operation.compare_and_swap(prior, replacement)

    state_path = local_state_path(tmp_path, environment="prod")
    assert state_path.read_bytes() == expected_path.read_bytes()
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600
    assert committed.state == replacement
    assert committed.revision.is_absent is False
    assert backend.read(address) == committed


def test_local_backend_rejects_same_serial_content_replacement(tmp_path: Path) -> None:
    backend = LocalDeploymentStateBackend(tmp_path)
    address = _address()
    state_path = local_state_path(tmp_path, environment="prod")
    _state(serial=1, partitions=3).save(state_path)

    with backend.operation(address) as operation:
        observed = operation.read()
        # A non-conforming writer can ignore flock.  The opaque revision still
        # detects same-serial content replacement before the existing CAS save.
        _state(serial=1, partitions=6).save(state_path)
        before = state_path.read_bytes()
        with pytest.raises(StateConflictError, match="revision changed"):
            operation.compare_and_swap(observed, _state(serial=2, partitions=9))

    assert state_path.read_bytes() == before


def test_local_backend_keeps_duplicate_key_and_identity_rejection(
    tmp_path: Path,
) -> None:
    state_path = local_state_path(tmp_path, environment="prod")
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        '{"state_version":1,"project":"payments","project":"other",'
        '"environment":"prod","serial":0,"resources":{}}'
    )
    backend = LocalDeploymentStateBackend(tmp_path)

    with pytest.raises(StateFormatError, match="duplicate field 'project'"):
        backend.read(_address())

    LocalState(project="other", environment="prod").save(state_path)
    with pytest.raises(StateIdentityError, match="expected 'payments'"):
        backend.read(_address())


def test_service_accepts_a_structural_fake_backend_without_local_files(
    tmp_path: Path,
) -> None:
    address = _address()
    local = LocalDeploymentStateBackend(tmp_path)

    class FakeBackend:
        def describe(self) -> StateStoreIdentity:
            return local.describe()

        def read(self, requested: StateAddress) -> StateObservation:
            assert requested == address
            return local.read(requested)

        def operation(
            self,
            requested: StateAddress,
        ) -> AbstractContextManager[DeploymentStateOperation]:
            return local.operation(requested)

    backend: DeploymentStateBackend = FakeBackend()
    service = DeploymentStateService(backend=backend, address=address)

    assert service.read().state.serial == 0
    assert json.loads(json.dumps(service.read().state.to_dict()))["serial"] == 0


def _base_config(flink_config: dict) -> dict:
    return {
        "project": {"name": "test", "version": "1.0.0"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "flink": {
                "default": "local",
                "clusters": {
                    "local": {
                        "type": "rest",
                        "rest_url": "http://localhost:8082",
                    }
                },
            },
        },
        "sources": [{"name": "src", "topic": "src.v1", "columns": [{"name": "id"}]}],
        "models": [
            {
                "name": "out",
                "sql": 'SELECT id FROM {{ source("src") }}',
                "flink": flink_config,
            }
        ],
    }


def _compile_sql(flink_config: dict) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)
        with (path / "stream_project.yml").open("w") as handle:
            yaml.dump(_base_config(flink_config), handle)
        project = ProjectParser(path).parse()
        output_dir = path / "gen"
        Compiler(project, output_dir).compile(dry_run=False)
        return (output_dir / "flink" / "out.sql").read_text()


class TestFlinkStateBackend:
    def test_rocksdb(self) -> None:
        sql = _compile_sql({"state_backend": "rocksdb"})
        assert "SET 'state.backend' = 'rocksdb';" in sql

    def test_hashmap(self) -> None:
        sql = _compile_sql({"state_backend": "hashmap"})
        assert "SET 'state.backend' = 'hashmap';" in sql

    def test_not_set(self) -> None:
        sql = _compile_sql({"parallelism": 4})
        assert "state.backend" not in sql

    def test_combined_with_parallelism_and_ttl(self) -> None:
        sql = _compile_sql(
            {
                "parallelism": 2,
                "state_backend": "rocksdb",
                "state_ttl_ms": 3600000,
            }
        )
        assert "SET 'parallelism.default' = '2';" in sql
        assert "SET 'state.backend' = 'rocksdb';" in sql
        assert "SET 'table.exec.state.ttl' = '1 h';" in sql
