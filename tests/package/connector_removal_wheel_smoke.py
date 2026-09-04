"""Installed-wheel smoke for PostgreSQL-v2 reviewed Connector removal."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import quote, urlsplit

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import streamt
from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.core.parser import ProjectParser
from streamt.deployer.connect import ConnectClusterBinding
from streamt.deployer.plan_file import PLAN_FILE_VERSION, ReviewedPlanFile
from streamt.deployer.postgres_state import (
    PostgresStateInitializer,
    PrivatePostgresStateV2Migrator,
)
from streamt.deployer.recovery_plan import (
    RECOVERY_PLAN_FILE_VERSION,
    RecoveryPlanFile,
)
from streamt.deployer.state import LocalState, ManagedResourceRecord, artifact_checksum, resource_id
from streamt.deployer.state_backend import (
    DeploymentStateService,
    OperationControlState,
    OperationIntent,
    StateAddress,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)

_ADMIN_DSN_ENV = "STREAMT_TEST_POSTGRES_ADMIN_DSN"
_OWNER_DSN_ENV = "STREAMT_CONNECTOR_WHEEL_OWNER_DSN"
_WRITER_DSN_ENV = "STREAMT_CONNECTOR_WHEEL_WRITER_DSN"
_PROJECT = "connector-removal-wheel"
_ENVIRONMENT = "default"
_NAMESPACE = "wheel-gate"
_OWNER = "archive_orders"
_CONNECTOR = "archive/orders sink"
_CONNECT_ALIAS = "primary"
_CONNECT_USERNAME = "connector-wheel-user"
_CONNECT_PASSWORD = "connector-wheel-runtime-password-secret"
_LIVE_CONFIG_SECRET = "connector-wheel-live-config-secret"
_DELETE_404_BODY_SECRET = "connector-wheel-delete-404-body-secret"
_MAX_SERVER_BODY_BYTES = 32 * 1024
_MAX_COMMAND_OUTPUT_BYTES = 128 * 1024
_COMMAND_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class _CommandResult:
    payload: dict[str, object]
    stdout: str
    stderr: str


class _LoopbackConnectState:
    """Thread-safe strict GET/DELETE surface for one encoded Connector."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._config: dict[str, object] = {}
        self._delete_status = 204
        self._present = False
        self._expected_authorization = "Basic " + base64.b64encode(
            f"{_CONNECT_USERNAME}:{_CONNECT_PASSWORD}".encode()
        ).decode("ascii")
        self._requests: list[tuple[str, str]] = []
        self._errors: list[str] = []

    def install(self, config: dict[str, object], *, delete_status: int) -> int:
        assert delete_status in (204, 404)
        with self._lock:
            self._config = json.loads(json.dumps(config))
            self._delete_status = delete_status
            self._present = True
            return len(self._requests)

    def get(
        self,
        path: str,
        *,
        authorization: str | None,
    ) -> tuple[int, object]:
        with self._lock:
            self._requests.append(("GET", path))
            if authorization != self._expected_authorization:
                self._errors.append("GET used invalid authorization")
                return 403, {"error": "forbidden"}
            if path != _connector_path():
                self._errors.append(f"unexpected GET path {path!r}")
                return 404, {"error": "not found"}
            if not self._present:
                return 404, {"error": "not found"}
            return 200, {"name": _CONNECTOR, "config": dict(self._config)}

    def delete(
        self,
        path: str,
        *,
        authorization: str | None,
        content_length: str | None,
        transfer_encoding: str | None,
    ) -> tuple[int, bytes]:
        with self._lock:
            self._requests.append(("DELETE", path))
            if authorization != self._expected_authorization:
                self._errors.append("DELETE used invalid authorization")
                return 403, b'{"error":"forbidden"}'
            if path != _connector_path():
                self._errors.append(f"unexpected DELETE path {path!r}")
                return 404, b'{"error":"not found"}'
            if transfer_encoding is not None or content_length not in (None, "0"):
                self._errors.append("DELETE contained a request body")
                return 400, b'{"error":"invalid body"}'
            if not self._present:
                self._errors.append("DELETE was repeated after exact absence")
                return 404, b'{"error":"not found"}'
            self._present = False
            if self._delete_status == 204:
                return 204, b""
            return 404, _DELETE_404_BODY_SECRET.encode("utf-8")

    def snapshot(self, start: int = 0) -> tuple[bool, list[tuple[str, str]], list[str]]:
        with self._lock:
            return self._present, list(self._requests[start:]), list(self._errors)


class _LoopbackServer(ThreadingHTTPServer):
    """Do not print client connection-reset tracebacks from short-lived CLIs."""

    def handle_error(self, _request: object, _client_address: object) -> None:
        return


def _connector_path() -> str:
    return f"/api/connectors/{quote(_CONNECTOR, safe='')}"


def _handler_for(state: _LoopbackConnectState) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            status, value = state.get(
                parsed.path,
                authorization=self.headers.get("Authorization"),
            )
            self._send_json(status, value)

        def do_DELETE(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            status, body = state.delete(
                parsed.path,
                authorization=self.headers.get("Authorization"),
                content_length=self.headers.get("Content-Length"),
                transfer_encoding=self.headers.get("Transfer-Encoding"),
            )
            self._send_bytes(status, body)

        def _send_json(self, status: int, value: object) -> None:
            body = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
            self._send_bytes(status, body, content_type="application/json")

        def _send_bytes(
            self,
            status: int,
            body: bytes,
            *,
            content_type: str | None = None,
        ) -> None:
            if len(body) > _MAX_SERVER_BODY_BYTES:
                raise AssertionError("loopback response body exceeded its bound")
            self.send_response(status)
            if content_type is not None:
                self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if body:
                self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return _Handler


def _assert_installed_wheel() -> Path:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    checkout_source = (checkout / "src").resolve()
    installed_module = Path(streamt.__file__).resolve()
    import_roots = {Path(entry).resolve() for entry in sys.path if entry}
    assert checkout_source not in import_roots, import_roots
    assert checkout not in installed_module.parents, installed_module
    for module_name, module in tuple(sys.modules.items()):
        if module_name != "streamt" and not module_name.startswith("streamt."):
            continue
        module_file = getattr(module, "__file__", None)
        if module_file is not None:
            assert checkout not in Path(module_file).resolve().parents, module_file
    executable = Path(sys.executable).with_name("streamt")
    assert executable.is_file(), executable
    return executable


def _run_cli(
    executable: Path,
    project_dir: Path,
    writer_dsn: str,
    *arguments: str,
    expected: int,
) -> _CommandResult:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop(_ADMIN_DSN_ENV, None)
    environment.pop(_OWNER_DSN_ENV, None)
    environment[_WRITER_DSN_ENV] = writer_dsn
    environment["PYTHONNOUSERSITE"] = "1"
    result = subprocess.run(
        [str(executable), *arguments],
        cwd=project_dir,
        env=environment,
        capture_output=True,
        text=True,
        timeout=_COMMAND_TIMEOUT_SECONDS,
        check=False,
    )
    stdout_bytes = len(result.stdout.encode("utf-8"))
    stderr_bytes = len(result.stderr.encode("utf-8"))
    assert stdout_bytes <= _MAX_COMMAND_OUTPUT_BYTES
    assert stderr_bytes <= _MAX_COMMAND_OUTPUT_BYTES
    assert result.returncode == expected, (
        f"installed streamt returned {result.returncode}, expected {expected}; "
        f"stdout_bytes={stdout_bytes}; stderr_bytes={stderr_bytes}"
    )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError("installed streamt did not emit one bounded JSON object") from error
    assert isinstance(payload, dict)
    return _CommandResult(payload=payload, stdout=result.stdout, stderr=result.stderr)


def _address() -> StateAddress:
    return StateAddress(
        namespace=_NAMESPACE,
        project=_PROJECT,
        environment=_ENVIRONMENT,
    )


def _prior_artifact() -> ConnectorArtifact:
    return ConnectorArtifact(
        name=_CONNECTOR,
        connector_class="com.example.ArchiveSink",
        topics=["orders.events.v1", "orders.events.v2"],
        cluster=_CONNECT_ALIAS,
        config={"password": _LIVE_CONFIG_SECRET, "tasks.max": 2},
        ownership=ArtifactOwnership(
            project=_PROJECT,
            owner_type="model",
            owner_name=_OWNER,
            mode="managed",
        ),
    )


def _write_project(project_dir: Path, *, connect_url: str, schema: str) -> None:
    project = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": _PROJECT},
        "runtime": {
            "kafka": {"bootstrap_servers": "127.0.0.1:1"},
            "connect": {
                "default": _CONNECT_ALIAS,
                "clusters": {
                    _CONNECT_ALIAS: {
                        "rest_url": connect_url,
                        "username": _CONNECT_USERNAME,
                        "password": _CONNECT_PASSWORD,
                    }
                },
            },
        },
        "lifecycle": {
            "connector_removals": [
                {
                    "logical_owner": _OWNER,
                    "name": _CONNECTOR,
                    "cluster": _CONNECT_ALIAS,
                }
            ]
        },
        "deployment_state": {
            "backend": "postgres",
            "namespace": _NAMESPACE,
            "lock_timeout_seconds": 5,
            "postgres": {
                "dsn_env": _OWNER_DSN_ENV,
                "writer_dsn_env": _WRITER_DSN_ENV,
                "schema": schema,
            },
        },
    }
    (project_dir / "stream_project.yml").write_text(
        json.dumps(project, indent=2),
        encoding="utf-8",
    )


def _remove_tombstone(project_dir: Path) -> None:
    path = project_dir / "stream_project.yml"
    project = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(project, dict)
    lifecycle = project.pop("lifecycle")
    assert lifecycle == {
        "connector_removals": [
            {
                "logical_owner": _OWNER,
                "name": _CONNECTOR,
                "cluster": _CONNECT_ALIAS,
            }
        ]
    }
    path.write_text(json.dumps(project, indent=2), encoding="utf-8")


def _state_service(project_dir: Path) -> DeploymentStateService:
    project = ProjectParser(project_dir).parse()
    return make_deployment_state_service(
        project_dir,
        project=project.project.name,
        environment=_ENVIRONMENT,
        config=project.deployment_state,
    )


def _owned_state(
    *,
    serial: int,
    binding: ConnectClusterBinding,
    unrelated_resource_id: str,
    unrelated_record: ManagedResourceRecord,
) -> LocalState:
    return LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=serial,
        resources={
            resource_id(_PROJECT, _ENVIRONMENT, "connector", _OWNER): ManagedResourceRecord(
                physical_name=_CONNECTOR,
                ownership="managed",
                artifact_checksum=artifact_checksum(_prior_artifact().to_dict()),
                backend=binding.backend_identity,
            ),
            unrelated_resource_id: unrelated_record,
        },
    )


def _seed_state(service: DeploymentStateService, state: LocalState) -> None:
    with service.operation() as operation:
        observed = operation.observe()
        operation.ensure_ready(observed)
        assert state.serial == observed.state.state.serial + 1
        active = operation.begin_operation(
            observed,
            OperationIntent(
                operation_id=str(uuid.uuid4()),
                kind="adopt",
                started_at=operation_timestamp(),
                actor="installed-connector-wheel-smoke",
                prior_state_serial=observed.state.state_serial,
                prior_state_checksum=state_checksum(observed.state.state),
                reviewed_plan_checksum=None,
                actions=(),
            ),
        )
        committed = operation.commit_operation(active, state)
        assert committed.state.state == state
        assert committed.control.control == OperationControlState.clear(_address())


def _durable_wire(admin_dsn: str, schema: str) -> str:
    tables = ("current_state", "operation_control", "operation_history", "state_history")
    values: list[str] = []
    with psycopg.connect(admin_dsn) as connection:
        for table in tables:
            values.extend(
                row[0]
                for row in connection.execute(
                    sql.SQL(
                        "SELECT pg_catalog.row_to_json(row_value)::text FROM {}.{} "
                        "AS row_value ORDER BY pg_catalog.row_to_json(row_value)::text"
                    ).format(sql.Identifier(schema), sql.Identifier(table))
                ).fetchall()
            )
    return "\n".join(values)


def _provision_postgres(
    admin_dsn: str,
    *,
    schema: str,
    owner_role: str,
    owner_password: str,
    writer_role: str,
    writer_password: str,
) -> tuple[str, str, str]:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        database = connection.execute("SELECT current_database()").fetchone()[0]
        for role, password in (
            (owner_role, owner_password),
            (writer_role, writer_password),
        ):
            connection.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
                ).format(sql.Identifier(role), sql.Literal(password))
            )
        connection.execute(
            sql.SQL("GRANT CONNECT, CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(database),
                sql.Identifier(owner_role),
            )
        )
        connection.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database),
                sql.Identifier(writer_role),
            )
        )
    owner_dsn = make_conninfo(admin_dsn, user=owner_role, password=owner_password)
    writer_dsn = make_conninfo(admin_dsn, user=writer_role, password=writer_password)
    initialized = PostgresStateInitializer(
        dsn=owner_dsn,
        schema=schema,
        lock_timeout_seconds=5,
    ).initialize(_address())
    migrated = PrivatePostgresStateV2Migrator(
        dsn=owner_dsn,
        schema=schema,
        lock_timeout_seconds=5,
        writer_role=writer_role,
    ).migrate(
        confirmed_store_id=initialized.store_id,
        confirmed_writer_role=writer_role,
    )
    assert migrated.migrated is True
    return owner_dsn, writer_dsn, initialized.store_id


def _cleanup_postgres(
    admin_dsn: str,
    *,
    schema: str,
    owner_role: str,
    writer_role: str,
) -> None:
    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        database = connection.execute("SELECT current_database()").fetchone()[0]
        connection.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
        for role in (writer_role, owner_role):
            connection.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            connection.execute(
                sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                    sql.Identifier(database),
                    sql.Identifier(role),
                )
            )
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def _exercise_installed_workflow() -> None:
    executable = _assert_installed_wheel()
    admin_dsn = os.environ.get(_ADMIN_DSN_ENV)
    assert admin_dsn is not None, f"{_ADMIN_DSN_ENV} is required"
    assert admin_dsn.strip(), f"{_ADMIN_DSN_ENV} is required"
    # Keep the bootstrap credential only in this local provisioning variable.
    # Installed production libraries and every CLI subprocess receive solely the
    # migrated v2 writer credential configured by the project.
    os.environ.pop(_ADMIN_DSN_ENV, None)
    suffix = uuid.uuid4().hex[:12]
    schema = f"streamt_wheel_{suffix}"
    owner_role = f"streamt_wheel_owner_{suffix}"
    writer_role = f"streamt_wheel_writer_{suffix}"
    owner_password = f"owner-wheel-secret-{suffix}"
    writer_password = f"writer-wheel-secret-{suffix}"

    connect_state = _LoopbackConnectState()
    server = _LoopbackServer(("127.0.0.1", 0), _handler_for(connect_state))
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    provisioned = False
    observed_outputs: list[str] = []
    evidence_paths: list[Path] = []
    owner_dsn = ""
    writer_dsn = ""
    try:
        host, port = server.server_address[:2]
        assert host == "127.0.0.1"
        assert type(port) is int
        assert port > 0
        connect_url = f"http://{host}:{port}/api/"
        owner_dsn, writer_dsn, store_id = _provision_postgres(
            admin_dsn,
            schema=schema,
            owner_role=owner_role,
            owner_password=owner_password,
            writer_role=writer_role,
            writer_password=writer_password,
        )
        provisioned = True
        os.environ.pop(_OWNER_DSN_ENV, None)
        os.environ[_WRITER_DSN_ENV] = writer_dsn

        with tempfile.TemporaryDirectory(prefix="streamt-connector-wheel-") as raw_root:
            root = Path(raw_root)
            project_dir = root / "project"
            project_dir.mkdir()
            _write_project(project_dir, connect_url=connect_url, schema=schema)
            service = _state_service(project_dir)
            assert service.store.backend == "postgres"
            assert service.store.store_id == store_id
            assert service.address == _address()

            binding = ConnectClusterBinding.from_endpoint(_CONNECT_ALIAS, connect_url)
            connector_resource_id = resource_id(
                _PROJECT,
                _ENVIRONMENT,
                "connector",
                _OWNER,
            )
            unrelated_resource_id = resource_id(
                _PROJECT,
                _ENVIRONMENT,
                "topic",
                "audit_log",
            )
            unrelated_record = ManagedResourceRecord(
                physical_name="audit.events.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "audit.events.v1"}),
                backend="direct-kafka",
            )
            initial_state = _owned_state(
                serial=1,
                binding=binding,
                unrelated_resource_id=unrelated_resource_id,
                unrelated_record=unrelated_record,
            )
            _seed_state(service, initial_state)
            raw_config = _prior_artifact().to_dict()["config"]
            assert isinstance(raw_config, dict)

            success_start = connect_state.install(raw_config, delete_status=204)
            plan_path = root / "connector-removal-success.plan.json"
            planned = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "plan",
                "-p",
                str(project_dir),
                "--out",
                str(plan_path),
                expected=0,
            )
            observed_outputs.extend((planned.stdout, planned.stderr))
            evidence_paths.append(plan_path)
            planned_data = planned.payload["data"]
            assert isinstance(planned_data, dict)
            assert planned_data["deletes"] == 1
            reviewed = ReviewedPlanFile.load(plan_path)
            assert reviewed.to_dict()["format_version"] == PLAN_FILE_VERSION == 5
            assert reviewed.state is not None
            assert reviewed.state.backend == "postgres"
            assert reviewed.state.store_id == store_id
            assert reviewed.state.address == _address().uri
            assert reviewed.state.serial == initial_state.serial
            assert reviewed.state.checksum == state_checksum(initial_state)
            assert len(reviewed.actions) == 1
            action = reviewed.actions[0]
            assert action.resource_id == connector_resource_id
            assert action.action == "delete"
            assert action.connector_evidence is not None
            assert action.connector_evidence.connector_name == _CONNECTOR
            assert action.connector_evidence.backend_identity == binding.backend_identity
            assert action.connector_evidence.current.exists is True
            assert action.connector_evidence.desired.exists is False

            refused = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "apply",
                "-p",
                str(project_dir),
                "--plan",
                str(plan_path),
                expected=1,
            )
            observed_outputs.extend((refused.stdout, refused.stderr))
            refused_errors = refused.payload["errors"]
            assert isinstance(refused_errors, list)
            assert refused_errors
            assert refused_errors[0]["code"] == "E503_ENVIRONMENT_ERROR"
            assert "Destructive ops blocked" in refused_errors[0]["message"]
            present, requests_after_refusal, errors = connect_state.snapshot(success_start)
            assert present is True
            assert requests_after_refusal == [
                ("GET", _connector_path()),
                ("GET", _connector_path()),
            ]
            assert errors == []
            assert service.read().state == initial_state

            applied = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "apply",
                "-p",
                str(project_dir),
                "--plan",
                str(plan_path),
                "--force",
                expected=0,
            )
            observed_outputs.extend((applied.stdout, applied.stderr))
            applied_data = applied.payload["data"]
            assert isinstance(applied_data, dict)
            assert applied_data["deleted"] == [f"connector:{_CONNECTOR}"]
            assert applied_data["committed"] is True
            assert applied_data["state_serial"] == initial_state.serial + 1
            present, success_requests, errors = connect_state.snapshot(success_start)
            assert present is False
            assert success_requests == [
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("DELETE", _connector_path()),
                ("GET", _connector_path()),
            ]
            assert errors == []
            success_state = LocalState(
                project=_PROJECT,
                environment=_ENVIRONMENT,
                serial=initial_state.serial + 1,
                resources={unrelated_resource_id: unrelated_record},
            )
            assert service.read().state == success_state
            assert service.read_control().control == OperationControlState.clear(_address())

            recovery_prior_state = _owned_state(
                serial=success_state.serial + 1,
                binding=binding,
                unrelated_resource_id=unrelated_resource_id,
                unrelated_record=unrelated_record,
            )
            _seed_state(service, recovery_prior_state)
            recovery_start = connect_state.install(raw_config, delete_status=404)
            uncertain_plan_path = root / "connector-removal-uncertain.plan.json"
            recovery_path = root / "connector-removal.recovery.json"
            uncertain_planned = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "plan",
                "-p",
                str(project_dir),
                "--out",
                str(uncertain_plan_path),
                expected=0,
            )
            observed_outputs.extend((uncertain_planned.stdout, uncertain_planned.stderr))
            evidence_paths.append(uncertain_plan_path)
            uncertain_reviewed = ReviewedPlanFile.load(uncertain_plan_path)
            assert uncertain_reviewed.state is not None
            assert uncertain_reviewed.state.serial == recovery_prior_state.serial

            uncertain = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "apply",
                "-p",
                str(project_dir),
                "--plan",
                str(uncertain_plan_path),
                "--force",
                expected=1,
            )
            observed_outputs.extend((uncertain.stdout, uncertain.stderr))
            uncertain_errors = uncertain.payload["errors"]
            assert isinstance(uncertain_errors, list)
            assert uncertain_errors
            assert uncertain_errors[0]["code"] == "E428_CONNECTOR_REMOVAL_DRIFT"
            assert uncertain_errors[0]["message"] == (
                "Kafka Connect managed deletion could not prove exact absence"
            )
            assert service.read().state == recovery_prior_state
            blocked = service.read_control().control
            assert blocked.status == "recovery_required"
            assert blocked.intent is not None
            assert blocked.intent.reviewed_plan_checksum == uncertain_reviewed.checksum
            assert blocked.intent.actions == uncertain_reviewed.actions
            assert [
                (entry.action_index, entry.action, entry.status, entry.succeeded)
                for entry in blocked.progress
            ] == [(0, "delete", "started", None), (0, "delete", "completed", False)]
            assert blocked.recovery is not None
            assert blocked.recovery.failure_code == "connector_removal_drift"
            blocked_operation_id = blocked.intent.operation_id
            present, uncertain_requests, errors = connect_state.snapshot(recovery_start)
            assert present is False
            assert uncertain_requests == [
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("DELETE", _connector_path()),
            ]
            assert errors == []

            _remove_tombstone(project_dir)
            recovery_planned = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "state",
                "recovery-plan",
                "-p",
                str(project_dir),
                "--resolution",
                "observed",
                "--out",
                str(recovery_path),
                expected=0,
            )
            observed_outputs.extend((recovery_planned.stdout, recovery_planned.stderr))
            evidence_paths.append(recovery_path)
            recovery = RecoveryPlanFile.load(recovery_path)
            assert recovery.to_dict()["format_version"] == RECOVERY_PLAN_FILE_VERSION == 3
            assert recovery.blocked_operation_id == blocked_operation_id
            assert recovery.resolution == "observed"
            assert recovery.snapshot.store.backend == "postgres"
            assert recovery.snapshot.store.store_id == store_id
            assert recovery.snapshot.address == _address()
            assert len(recovery.targets) == 1
            target = recovery.targets[0]
            assert target.action == uncertain_reviewed.actions[0]
            assert target.presence == "absent"
            assert target.accepted_as == "candidate"
            expected_final_state = LocalState(
                project=_PROJECT,
                environment=_ENVIRONMENT,
                serial=recovery_prior_state.serial + 1,
                resources={unrelated_resource_id: unrelated_record},
            )
            assert recovery.candidate_state == expected_final_state

            recovered = _run_cli(
                executable,
                project_dir,
                writer_dsn,
                "-o",
                "json",
                "state",
                "recover",
                "-p",
                str(project_dir),
                "--plan",
                str(recovery_path),
                "--confirm-operation-id",
                blocked_operation_id,
                "--confirm-resolution",
                "observed",
                "--confirm-evidence-checksum",
                recovery.evidence_checksum,
                expected=0,
            )
            observed_outputs.extend((recovered.stdout, recovered.stderr))
            recovered_data = recovered.payload["data"]
            assert isinstance(recovered_data, dict)
            assert recovered_data["store"] == {
                "backend": "postgres",
                "store_id": store_id,
            }
            assert recovered_data["address"] == _address().uri
            assert recovered_data["state_changed"] is True
            assert recovered_data["state_serial"] == expected_final_state.serial
            assert recovered_data["state_checksum"] == state_checksum(expected_final_state)
            assert recovered_data["control_status"] == "clear"
            assert service.read().state == expected_final_state
            assert service.read_control().control == OperationControlState.clear(_address())
            present, recovery_requests, errors = connect_state.snapshot(recovery_start)
            assert present is False
            assert recovery_requests == [
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("GET", _connector_path()),
                ("DELETE", _connector_path()),
                ("GET", _connector_path()),
                ("GET", _connector_path()),
            ]
            assert errors == []

            evidence_wire = "\n".join(path.read_text(encoding="utf-8") for path in evidence_paths)
            durable_wire = _durable_wire(admin_dsn, schema)
            public_wire = "\n".join(observed_outputs)
            checkout = str(Path(os.environ["STREAMT_CHECKOUT"]).resolve())
            admin_details = conninfo_to_dict(admin_dsn)
            for forbidden in (
                _CONNECT_PASSWORD,
                _LIVE_CONFIG_SECRET,
                _DELETE_404_BODY_SECRET,
                connect_url,
                admin_dsn,
                owner_dsn,
                writer_dsn,
                cast(str, admin_details.get("password", "")),
                owner_password,
                writer_password,
                owner_role,
                writer_role,
                schema,
                _OWNER_DSN_ENV,
                _WRITER_DSN_ENV,
                checkout,
            ):
                if forbidden:
                    assert forbidden not in public_wire
                    assert forbidden not in evidence_wire
                    assert forbidden not in durable_wire
            assert not (project_dir / ".streamt").exists()
    finally:
        os.environ.pop(_WRITER_DSN_ENV, None)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()
        if provisioned:
            _cleanup_postgres(
                admin_dsn,
                schema=schema,
                owner_role=owner_role,
                writer_role=writer_role,
            )


if __name__ == "__main__":
    _exercise_installed_workflow()
    print("installed-wheel PostgreSQL Connector removal workflow passed")
