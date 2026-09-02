"""Installed-wheel smoke gate for alias-only Gateway adoption."""

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
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.gateway import GatewayBackendBinding
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)

_PROJECT = "wheel-gateway-adoption"
_ENVIRONMENT = "default"
_OWNER = "orders_view"
_RULE = "orders_view"
_ALIAS = "orders.public"
_DESIRED_PHYSICAL = "orders.desired.private"
_OBSERVED_PHYSICAL = "orders.observed.private"
_VCLUSTER = "payments"
_USERNAME = "wheel-gateway-user"
_PASSWORD = "wheel-gateway-password"
_ADMIN_PREFIX = "/admin/private-wheel-token"
_ALIAS_PATH = f"{_ADMIN_PREFIX}/gateway/v2/alias-topic"
_INTERCEPTOR_PATH = f"{_ADMIN_PREFIX}/gateway/v2/interceptor"
_RESOURCE_ID = resource_id(
    _PROJECT,
    _ENVIRONMENT,
    "gateway_rule",
    _OWNER,
)


class _FakeGatewayState:
    """Thread-safe, authenticated, GET-only Gateway v2 test surface."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.aliases: list[dict[str, object]] = [
            _provider_alias(_ALIAS, _OBSERVED_PHYSICAL),
            _provider_alias("audit.public", "audit.private"),
        ]
        self.interceptors: list[dict[str, object]] = [
            {
                "kind": "Interceptor",
                "apiVersion": "gateway/v2",
                "metadata": {
                    "name": "audit_rule_filter_0",
                    "scope": {
                        "group": None,
                        "username": None,
                        "vCluster": _VCLUSTER,
                    },
                },
                "spec": {
                    "pluginClass": ("io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin"),
                    "priority": 100,
                    "config": {
                        "virtualTopic": "audit.public",
                        "statement": 'SELECT * FROM "audit.private" WHERE severity > 1',
                    },
                },
            }
        ]
        self.requests: list[tuple[str, str]] = []
        self.mutations: list[tuple[str, str]] = []
        self.errors: list[str] = []

    def get(self, path: str, authorization: str | None) -> list[dict[str, object]] | None:
        with self._lock:
            self.requests.append(("GET", path))
            if authorization != _expected_authorization():
                self.errors.append("GET used invalid Gateway authentication")
                return None
            if path == _ALIAS_PATH:
                return copy.deepcopy(self.aliases)
            if path == _INTERCEPTOR_PATH:
                return copy.deepcopy(self.interceptors)
            self.errors.append(f"unexpected GET path {path}")
            return None

    def reject_mutation(self, method: str, path: str) -> None:
        with self._lock:
            self.requests.append((method, path))
            self.mutations.append((method, path))
            self.errors.append(f"unexpected Gateway mutation {method}")

    def snapshot(
        self,
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]], list[str]]:
        with self._lock:
            return (
                list(self.requests),
                list(self.mutations),
                list(self.errors),
            )


def _expected_authorization() -> str:
    encoded = base64.b64encode(f"{_USERNAME}:{_PASSWORD}".encode()).decode()
    return f"Basic {encoded}"


def _provider_alias(name: str, physical_name: str) -> dict[str, object]:
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": {"name": name, "vCluster": _VCLUSTER},
        "spec": {"physicalName": physical_name, "physicalCluster": "main"},
    }


def _handler_for(state: _FakeGatewayState) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            resources = state.get(parsed.path, self.headers.get("Authorization"))
            if resources is None:
                self._send_json(401, {"error": "request rejected"})
                return
            self._send_json(200, resources)

        def do_POST(self) -> None:
            self._reject_mutation("POST")

        def do_PUT(self) -> None:
            self._reject_mutation("PUT")

        def do_PATCH(self) -> None:
            self._reject_mutation("PATCH")

        def do_DELETE(self) -> None:
            self._reject_mutation("DELETE")

        def _reject_mutation(self, method: str) -> None:
            state.reject_mutation(method, urlsplit(self.path).path)
            self._send_json(405, {"error": "mutations are forbidden"})

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
      virtual_cluster: {_VCLUSTER}
sources:
  - name: orders_source
    topic: {_DESIRED_PHYSICAL}
models:
  - name: {_OWNER}
    ownership:
      mode: adopted
    materialized: virtual_topic
    gateway:
      virtual_topic:
        name: {_ALIAS}
    sql: |
      SELECT * FROM {{{{ source("orders_source") }}}}
""",
        encoding="utf-8",
    )


def _assert_installed_wheel() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    checkout_source = (checkout / "src").resolve()
    installed_module = Path(streamt.__file__).resolve()
    import_roots = {Path(entry).resolve() for entry in sys.path if entry}
    assert checkout_source not in import_roots, import_roots
    assert checkout not in installed_module.parents, installed_module


def _run_cli(
    executable: Path,
    *arguments: str,
    expected: int,
) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [str(executable), *arguments],
        cwd=Path.cwd(),
        env=environment,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == expected, (
        f"streamt {' '.join(arguments)} returned {result.returncode}, expected {expected}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(
            f"streamt did not emit structured JSON\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        ) from error
    assert isinstance(payload, dict), payload
    return result, payload


def _adopt_arguments(project_dir: Path, *, confirm: bool) -> list[str]:
    arguments = [
        "-o",
        "json",
        "adopt",
        "-p",
        str(project_dir),
        "-e",
        _ENVIRONMENT,
        "--kind",
        "gateway_rule",
        "--name",
        _OWNER,
    ]
    if confirm:
        arguments.extend(
            [
                "--confirm-resource",
                _RESOURCE_ID,
                "--confirm-env",
                _ENVIRONMENT,
            ]
        )
    return arguments


def _assert_secret_neutral(result: subprocess.CompletedProcess[str]) -> None:
    rendered = f"{result.stdout}\n{result.stderr}"
    for sensitive in (
        _PASSWORD,
        _USERNAME,
        _ADMIN_PREFIX,
        _OBSERVED_PHYSICAL,
        _DESIRED_PHYSICAL,
    ):
        assert sensitive not in rendered, sensitive


def _exercise_installed_workflow() -> None:
    _assert_installed_wheel()
    executable = Path(sys.executable).with_name("streamt")
    assert executable.is_file(), executable

    gateway_state = _FakeGatewayState()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_for(gateway_state))
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        host, port = server.server_address[:2]
        assert isinstance(host, str)
        assert isinstance(port, int)
        admin_url = f"http://{host}:{port}{_ADMIN_PREFIX}"
        with tempfile.TemporaryDirectory(prefix="streamt-gateway-adopt-wheel-") as raw_root:
            project_dir = Path(raw_root) / "project"
            project_dir.mkdir()
            _write_project(project_dir, admin_url)

            project = ProjectParser(project_dir).parse()
            manifest = Compiler(project).compile(dry_run=True)
            artifacts = manifest.artifacts["gateway_rules"]
            assert len(artifacts) == 1
            artifact = artifacts[0]
            assert artifact == {
                "name": _RULE,
                "virtualTopic": _ALIAS,
                "physicalTopic": _DESIRED_PHYSICAL,
                "interceptors": [],
                "ownership": {
                    "project": _PROJECT,
                    "type": "model",
                    "name": _OWNER,
                    "mode": "adopted",
                },
            }

            first_result, first_payload = _run_cli(
                executable,
                *_adopt_arguments(project_dir, confirm=True),
                expected=0,
            )
            _assert_secret_neutral(first_result)
            assert first_payload["status"] == "ok", first_payload
            first_data = first_payload["data"]
            assert isinstance(first_data, dict)
            assert first_data["resource_id"] == _RESOURCE_ID
            assert first_data["kind"] == "gateway_rule"
            assert first_data["alias_name"] == _ALIAS
            assert first_data["effective_vcluster"] == _VCLUSTER
            assert first_data["physical_cluster"] == "main"
            assert first_data["pending_change_categories"] == ["alias_mapping"]
            assert first_data["has_pending_changes"] is True
            assert first_data["adopted"] is True
            assert first_data["already_owned"] is False
            assert first_data["committed"] is True
            assert first_data["state_serial"] == 1

            first_requests, mutations, errors = gateway_state.snapshot()
            expected_snapshot = [
                ("GET", _ALIAS_PATH),
                ("GET", _INTERCEPTOR_PATH),
            ]
            assert first_requests == [*expected_snapshot, *expected_snapshot]
            assert mutations == []
            assert errors == []

            state_path = local_state_path(project_dir, environment=_ENVIRONMENT)
            committed = LocalState.load(
                state_path,
                expected_project=_PROJECT,
                expected_environment=_ENVIRONMENT,
            )
            expected_record = ManagedResourceRecord(
                physical_name=_ALIAS,
                ownership="adopted",
                artifact_checksum=artifact_checksum(artifact),
                backend=GatewayBackendBinding.from_endpoint(
                    admin_url,
                    virtual_cluster=_VCLUSTER,
                ).backend_identity,
            )
            assert committed.serial == 1
            assert committed.resources == {_RESOURCE_ID: expected_record}
            state_before_retry = committed.to_dict()
            state_text = state_path.read_text(encoding="utf-8")
            assert admin_url not in state_text
            assert _PASSWORD not in state_text
            assert _USERNAME not in state_text
            assert _OBSERVED_PHYSICAL not in state_text
            assert _DESIRED_PHYSICAL not in state_text

            retry_result, retry_payload = _run_cli(
                executable,
                *_adopt_arguments(project_dir, confirm=False),
                expected=0,
            )
            _assert_secret_neutral(retry_result)
            assert retry_payload["status"] == "ok", retry_payload
            retry_data = retry_payload["data"]
            assert isinstance(retry_data, dict)
            assert retry_data["adopted"] is False
            assert retry_data["already_owned"] is True
            assert retry_data["state_serial"] == 1
            assert "committed" not in retry_data

            final_requests, mutations, errors = gateway_state.snapshot()
            assert final_requests == [
                *expected_snapshot,
                *expected_snapshot,
                *expected_snapshot,
            ]
            assert mutations == []
            assert errors == []
            assert LocalState.load(state_path).to_dict() == state_before_retry
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()


if __name__ == "__main__":
    _exercise_installed_workflow()
    print("installed-wheel Gateway adoption workflow passed")
