"""Installed-wheel smoke gate for durable OpenLineage apply telemetry."""

from __future__ import annotations

import base64
import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

import streamt
from streamt.deployer.state import LocalState, local_state_path, resource_id
from streamt.deployer.state_backend import local_control_path
from streamt.integrations.openlineage import validate_event, validate_event_sequence

_PROJECT = "wheel-openlineage-apply"
_ENVIRONMENT = "default"
_OWNER = "orders_view"
_ALIAS = "orders.public"
_PHYSICAL = "orders.private"
_UNRELATED_ALIAS = "audit.public"
_USERNAME = "wheel-gateway-user"
_PASSWORD = "wheel-gateway-password"
_ADMIN_PREFIX = "/admin/private-gateway-token"
_ALIAS_PATH = f"{_ADMIN_PREFIX}/gateway/v2/alias-topic"
_INTERCEPTOR_PATH = f"{_ADMIN_PREFIX}/gateway/v2/interceptor"
_JOB_NAMESPACE = "lineage.wheel"


def _provider_alias(name: str, physical_name: str) -> dict[str, object]:
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": {"name": name, "vCluster": "passthrough"},
        "spec": {"physicalName": physical_name, "physicalCluster": "main"},
    }


class _FakeGatewayState:
    """Strict Gateway v2 surface with one deliberately paused create."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.aliases = [_provider_alias(_UNRELATED_ALIAS, "audit.private")]
        self.requests: list[tuple[str, str]] = []
        self.errors: list[str] = []
        self.mutation_entered = threading.Event()
        self.release_mutation = threading.Event()

    def collection(
        self,
        path: str,
        authorization: str | None,
    ) -> list[dict[str, object]] | None:
        with self._lock:
            self.requests.append(("GET", path))
            if authorization != _expected_authorization():
                self.errors.append("GET used invalid Gateway authentication")
                return None
            if path == _ALIAS_PATH:
                return copy.deepcopy(self.aliases)
            if path == _INTERCEPTOR_PATH:
                return []
            self.errors.append(f"unexpected GET path {path}")
            return None

    def create_alias(
        self,
        path: str,
        authorization: str | None,
        payload: object,
    ) -> tuple[int, dict[str, object]]:
        with self._lock:
            self.requests.append(("PUT", path))
            if authorization != _expected_authorization():
                self.errors.append("PUT used invalid Gateway authentication")
                return 401, {"error": "request rejected"}
            if path != _ALIAS_PATH:
                self.errors.append(f"unexpected PUT path {path}")
                return 404, {"error": "not found"}
            expected = _provider_alias(_ALIAS, _PHYSICAL)
            if payload != expected:
                self.errors.append("PUT used a non-canonical alias payload")
                return 400, {"error": "invalid alias"}

        self.mutation_entered.set()
        if not self.release_mutation.wait(timeout=30):
            with self._lock:
                self.errors.append("timed out waiting to release Gateway mutation")
            return 500, {"error": "test synchronization timeout"}

        with self._lock:
            self.aliases.append(copy.deepcopy(expected))
        return 200, {"resource": expected, "upsertResult": "Created"}

    def snapshot(self) -> tuple[list[dict[str, object]], list[tuple[str, str]], list[str]]:
        with self._lock:
            return copy.deepcopy(self.aliases), list(self.requests), list(self.errors)


def _expected_authorization() -> str:
    encoded = base64.b64encode(f"{_USERNAME}:{_PASSWORD}".encode()).decode()
    return f"Basic {encoded}"


def _handler_for(state: _FakeGatewayState) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            resources = state.collection(parsed.path, self.headers.get("Authorization"))
            if resources is None:
                self._send_json(401, {"error": "request rejected"})
                return
            self._send_json(200, resources)

        def do_PUT(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            raw_length = self.headers.get("Content-Length")
            try:
                length = int(raw_length) if raw_length is not None else -1
            except ValueError:
                length = -1
            if length < 0 or length > 16 * 1024:
                self._send_json(400, {"error": "invalid body length"})
                return
            try:
                payload = json.loads(self.rfile.read(length))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._send_json(400, {"error": "invalid JSON"})
                return
            status, response = state.create_alias(
                parsed.path,
                self.headers.get("Authorization"),
                payload,
            )
            self._send_json(status, response)

        def do_POST(self) -> None:
            self._reject_unexpected_mutation("POST")

        def do_PATCH(self) -> None:
            self._reject_unexpected_mutation("PATCH")

        def do_DELETE(self) -> None:
            self._reject_unexpected_mutation("DELETE")

        def _reject_unexpected_mutation(self, method: str) -> None:
            with state._lock:
                state.requests.append((method, urlsplit(self.path).path))
                state.errors.append(f"unexpected Gateway mutation {method}")
            self._send_json(405, {"error": "mutation is unsupported"})

        def _send_json(self, status: int, value: object) -> None:
            body = json.dumps(value, separators=(",", ":")).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    return _Handler


def _write_project(project_dir: Path, admin_url: str) -> None:
    (project_dir / "stream_project.yml").write_text(
        f"""apiVersion: streamt.dev/v1alpha1
project:
  name: {_PROJECT}
runtime:
  kafka:
    bootstrap_servers: 127.0.0.1:1
  conduktor:
    gateway:
      admin_url: {admin_url}
      username: {_USERNAME}
      password: {_PASSWORD}
sources:
  - name: orders_source
    topic: {_PHYSICAL}
models:
  - name: {_OWNER}
    materialized: virtual_topic
    gateway:
      virtual_topic:
        name: {_ALIAS}
    sql: |
      SELECT * FROM {{{{ source("orders_source") }}}}
""",
        encoding="utf-8",
    )


def _write_openlineage_config(config_path: Path, event_path: Path) -> None:
    config_path.write_text(
        f"transport:\n  type: file\n  log_file_path: {json.dumps(str(event_path))}\n",
        encoding="utf-8",
    )


def _assert_installed_wheel() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    checkout_source = (checkout / "src").resolve()
    installed_module = Path(streamt.__file__).resolve()
    import_roots = {Path(entry).resolve() for entry in sys.path if entry}
    assert checkout_source not in import_roots, import_roots
    assert checkout not in installed_module.parents, installed_module


def _read_events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _assert_secret_neutral(rendered: str, *, extra: tuple[str, ...] = ()) -> None:
    for sensitive in (
        _PASSWORD,
        _USERNAME,
        _ADMIN_PREFIX,
        *extra,
    ):
        assert sensitive not in rendered, sensitive


def _assert_no_runtime_claims(event: dict[str, object]) -> None:
    rendered = json.dumps(event, sort_keys=True)
    for excluded in (_ALIAS, _PHYSICAL, "127.0.0.1:1", "gateway_rule", "create"):
        assert excluded not in rendered, excluded
    assert "inputs" not in event
    assert "outputs" not in event


def _exercise_installed_workflow() -> None:
    _assert_installed_wheel()
    executable = Path(sys.executable).with_name("streamt")
    assert executable.is_file(), executable

    gateway = _FakeGatewayState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(gateway))
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    process: subprocess.Popen[str] | None = None
    try:
        host, port = server.server_address[:2]
        assert isinstance(host, str)
        assert isinstance(port, int)
        admin_url = f"http://{host}:{port}{_ADMIN_PREFIX}"
        with tempfile.TemporaryDirectory(prefix="streamt-openlineage-apply-wheel-") as raw_root:
            root = Path(raw_root)
            project_dir = root / "project"
            project_dir.mkdir()
            private_dir = root / "private-openlineage-token"
            private_dir.mkdir()
            config_path = private_dir / "secret-config.yml"
            event_path = private_dir / "events.jsonl"
            _write_project(project_dir, admin_url)
            _write_openlineage_config(config_path, event_path)

            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            environment["PYTHONNOUSERSITE"] = "1"
            environment["OPENLINEAGE_CONFIG"] = str(config_path)
            process = subprocess.Popen(
                [
                    str(executable),
                    "-o",
                    "json",
                    "apply",
                    "-p",
                    str(project_dir),
                    "--emit-openlineage",
                    "--openlineage-job-namespace",
                    _JOB_NAMESPACE,
                ],
                cwd=root,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            if not gateway.mutation_entered.wait(timeout=30):
                stdout, stderr = process.communicate(timeout=5)
                raise AssertionError(
                    "installed apply never reached the Gateway mutation boundary\n"
                    f"stdout:\n{stdout}\nstderr:\n{stderr}"
                )

            control_path = local_control_path(project_dir, environment=_ENVIRONMENT)
            control = json.loads(control_path.read_text(encoding="utf-8"))
            assert control["status"] == "in_progress", control
            intent = control["intent"]
            assert isinstance(intent, dict)
            assert intent["kind"] == "apply"
            assert len(intent["actions"]) == 1
            assert intent["actions"][0]["action"] == "create"
            assert control["progress"] == [
                {
                    "operation_id": intent["operation_id"],
                    "action_index": 0,
                    "resource_id": resource_id(
                        _PROJECT,
                        _ENVIRONMENT,
                        "gateway_rule",
                        _OWNER,
                    ),
                    "action": "create",
                    "status": "started",
                    "succeeded": None,
                    "recorded_at": control["progress"][0]["recorded_at"],
                }
            ]

            start_events = _read_events(event_path)
            assert len(start_events) == 1
            start = start_events[0]
            validate_event(start)
            assert start["eventType"] == "START"
            assert start["run"] == {"runId": intent["operation_id"]}
            assert start["eventTime"] == intent["started_at"]
            start_job = start["job"]
            assert isinstance(start_job, dict)
            assert start_job["namespace"] == _JOB_NAMESPACE
            assert start_job["name"] == f"streamt/{_PROJECT}/commands/apply"
            _assert_no_runtime_claims(start)
            _assert_secret_neutral(
                json.dumps(start, sort_keys=True),
                extra=(str(config_path), str(event_path), admin_url),
            )

            gateway.release_mutation.set()
            stdout, stderr = process.communicate(timeout=45)
            assert process.returncode == 0, (
                f"installed streamt apply returned {process.returncode}\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
            process = None
            payload = json.loads(stdout)
            assert payload["status"] == "ok", payload
            data = payload["data"]
            assert isinstance(data, dict)
            assert data["created"] == [f"gateway_rule:{_OWNER}"]
            assert data["committed"] is True
            assert data["state_serial"] == 1

            events = _read_events(event_path)
            assert [event["eventType"] for event in events] == ["START", "COMPLETE"]
            for event in events:
                validate_event(event)
                assert event["run"] == {"runId": intent["operation_id"]}
                assert event["job"] == start["job"]
                _assert_no_runtime_claims(event)
            validate_event_sequence(events)
            assert event_path.stat().st_mode & 0o777 == 0o600

            final_control = json.loads(control_path.read_text(encoding="utf-8"))
            assert final_control["status"] == "clear"
            assert final_control["intent"] is None
            assert final_control["progress"] == []
            assert final_control["recovery"] is None

            committed = LocalState.load(
                local_state_path(project_dir, environment=_ENVIRONMENT),
                expected_project=_PROJECT,
                expected_environment=_ENVIRONMENT,
            )
            expected_resource = resource_id(
                _PROJECT,
                _ENVIRONMENT,
                "gateway_rule",
                _OWNER,
            )
            assert committed.serial == 1
            assert set(committed.resources) == {expected_resource}
            record = committed.resources[expected_resource]
            assert record.physical_name == _ALIAS
            assert record.ownership == "managed"

            aliases, requests, errors = gateway.snapshot()
            assert aliases == [
                _provider_alias(_UNRELATED_ALIAS, "audit.private"),
                _provider_alias(_ALIAS, _PHYSICAL),
            ]
            assert requests == [
                ("GET", _ALIAS_PATH),
                ("GET", _INTERCEPTOR_PATH),
                ("PUT", _ALIAS_PATH),
            ]
            assert errors == []

            rendered_events = event_path.read_text(encoding="utf-8")
            rendered_state = local_state_path(
                project_dir,
                environment=_ENVIRONMENT,
            ).read_text(encoding="utf-8")
            _assert_secret_neutral(
                f"{stdout}\n{stderr}\n{rendered_events}\n{rendered_state}",
                extra=(str(config_path), str(event_path), admin_url),
            )
    finally:
        gateway.release_mutation.set()
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()


if __name__ == "__main__":
    _exercise_installed_workflow()
    print("installed-wheel OpenLineage apply workflow passed")
