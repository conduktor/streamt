"""Installed-wheel smoke gate for the reviewed Gateway removal workflow."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

import streamt
from streamt.compiler import Compiler
from streamt.compiler.gateway_artifact import parse_compiled_gateway_rule_artifact
from streamt.core.parser import ProjectParser
from streamt.deployer.gateway import GatewayBackendBinding, build_desired_gateway_rule
from streamt.deployer.plan_file import PLAN_FILE_VERSION, ReviewedPlanFile
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)

_PROJECT = "wheel-gateway-removal"
_ENVIRONMENT = "default"
_OWNER = "orders_view"
_RULE = "orders_access_rule"
_ALIAS = "orders.public"
_PHYSICAL = "orders.raw"
_UNRELATED_ALIAS = "audit.public"
_UNRELATED_RULE = "audit_access_rule"
_INITIAL_SERIAL = 7


class _FakeGatewayState:
    """Thread-safe, strict subset of the Gateway v2 admin surface."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.aliases: list[dict[str, object]] = []
        self.interceptors: list[dict[str, object]] = []
        self.delete_requests: list[tuple[str, dict[str, object]]] = []
        self.errors: list[str] = []

    def install(
        self,
        *,
        aliases: list[dict[str, object]],
        interceptors: list[dict[str, object]],
    ) -> None:
        with self._lock:
            self.aliases = copy.deepcopy(aliases)
            self.interceptors = copy.deepcopy(interceptors)

    def collection(self, path: str) -> list[dict[str, object]] | None:
        with self._lock:
            if path == "/gateway/v2/alias-topic":
                return copy.deepcopy(self.aliases)
            if path == "/gateway/v2/interceptor":
                return copy.deepcopy(self.interceptors)
            self.errors.append(f"unexpected GET {path}")
            return None

    def delete(self, path: str, payload: object) -> int:
        if not isinstance(payload, dict):
            with self._lock:
                self.errors.append(f"non-object DELETE payload for {path}")
            return 400

        with self._lock:
            if path == "/gateway/v2/alias-topic":
                if set(payload) != {"name", "vCluster"}:
                    self.errors.append("non-canonical alias DELETE payload")
                    return 400
                for index, alias in enumerate(self.aliases):
                    metadata = alias.get("metadata")
                    if not isinstance(metadata, dict):
                        continue
                    if (
                        metadata.get("name") == payload["name"]
                        and metadata.get("vCluster", "passthrough") == payload["vCluster"]
                    ):
                        self.delete_requests.append((path, copy.deepcopy(payload)))
                        del self.aliases[index]
                        return 204
                return 404

            interceptor_prefix = "/gateway/v2/interceptor/"
            if path.startswith(interceptor_prefix):
                name = unquote(path.removeprefix(interceptor_prefix))
                if set(payload) != {"group", "username", "vCluster"}:
                    self.errors.append("non-canonical interceptor DELETE payload")
                    return 400
                for index, interceptor in enumerate(self.interceptors):
                    metadata = interceptor.get("metadata")
                    if not isinstance(metadata, dict):
                        continue
                    if metadata.get("name") != name or metadata.get("scope") != payload:
                        continue
                    self.delete_requests.append((path, copy.deepcopy(payload)))
                    del self.interceptors[index]
                    return 204
                return 404

            self.errors.append(f"unexpected DELETE {path}")
            return 404

    def snapshot(
        self,
    ) -> tuple[
        list[dict[str, object]],
        list[dict[str, object]],
        list[tuple[str, dict[str, object]]],
        list[str],
    ]:
        with self._lock:
            return (
                copy.deepcopy(self.aliases),
                copy.deepcopy(self.interceptors),
                copy.deepcopy(self.delete_requests),
                list(self.errors),
            )


def _handler_for(state: _FakeGatewayState) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment:
                self._send_json(400, {"error": "query and fragment are unsupported"})
                return
            resources = state.collection(parsed.path)
            if resources is None:
                self._send_json(404, {"error": "not found"})
                return
            self._send_json(200, resources)

        def do_DELETE(self) -> None:
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
            status = state.delete(parsed.path, payload)
            if status == 204:
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self._send_json(status, {"error": "delete rejected"})

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


def _provider_alias(name: str, physical_name: str) -> dict[str, object]:
    return {
        "kind": "AliasTopic",
        "apiVersion": "gateway/v2",
        "metadata": {"name": name, "vCluster": "passthrough"},
        "spec": {"physicalName": physical_name, "physicalCluster": "main"},
    }


def _provider_interceptor(
    name: str,
    *,
    scope: Mapping[str, object],
    config: dict[str, object],
) -> dict[str, object]:
    return {
        "kind": "Interceptor",
        "apiVersion": "gateway/v2",
        "metadata": {"name": name, "scope": dict(scope)},
        "spec": {
            "pluginClass": "io.conduktor.gateway.interceptor.VirtualSqlTopicPlugin",
            "priority": 100,
            "config": config,
        },
    }


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
lifecycle:
  gateway_rule_removals:
    - logical_owner: {_OWNER}
      prior_artifact:
        name: {_RULE}
        virtualTopic: {_ALIAS}
        physicalTopic: {_PHYSICAL}
        interceptors:
          - type: filter
            config:
              where: amount > 0
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


def _run_cli(executable: Path, *arguments: str, expected: int) -> dict[str, object]:
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
    return payload


def _seed_owned_removal(
    project_dir: Path,
    admin_url: str,
) -> tuple[dict[str, object], str, list[dict[str, object]], list[dict[str, object]]]:
    project = ProjectParser(project_dir).parse()
    manifest = Compiler(project).compile(dry_run=True)
    removals = manifest.artifacts["gateway_rule_removals"]
    assert len(removals) == 1
    removal = removals[0]
    prior_artifact = removal["priorArtifact"]
    assert isinstance(prior_artifact, dict)

    binding = GatewayBackendBinding.from_endpoint(admin_url)
    desired = build_desired_gateway_rule(
        parse_compiled_gateway_rule_artifact(prior_artifact),
        binding,
    )
    assert desired.exists is True
    assert desired.physical_name == _PHYSICAL
    assert len(desired.interceptors) == 1
    target_alias = _provider_alias(desired.alias_name, desired.physical_name)
    target_interceptors = [
        _provider_interceptor(
            interceptor.name,
            scope=dict(interceptor.scope),
            config=json.loads(interceptor.config_json),
        )
        for interceptor in desired.interceptors
    ]

    removed_resource_id = resource_id(
        _PROJECT,
        _ENVIRONMENT,
        "gateway_rule",
        _OWNER,
    )
    LocalState(
        project=_PROJECT,
        environment=_ENVIRONMENT,
        serial=_INITIAL_SERIAL,
        resources={
            removed_resource_id: ManagedResourceRecord(
                physical_name=_ALIAS,
                ownership="managed",
                artifact_checksum=artifact_checksum(prior_artifact),
                backend=binding.backend_identity,
            )
        },
    ).save(local_state_path(project_dir, environment=_ENVIRONMENT))
    return prior_artifact, removed_resource_id, [target_alias], target_interceptors


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
        admin_url = f"http://{host}:{port}"
        with tempfile.TemporaryDirectory(prefix="streamt-gateway-wheel-") as raw_root:
            root = Path(raw_root)
            project_dir = root / "project"
            project_dir.mkdir()
            _write_project(project_dir, admin_url)
            prior_artifact, removed_resource_id, target_aliases, target_interceptors = (
                _seed_owned_removal(project_dir, admin_url)
            )

            unrelated_scope = {
                "group": None,
                "username": None,
                "vCluster": "passthrough",
            }
            unrelated_alias = _provider_alias(_UNRELATED_ALIAS, "audit.raw")
            unrelated_interceptor = _provider_interceptor(
                f"{_UNRELATED_RULE}_filter_0",
                scope=unrelated_scope,
                config={
                    "virtualTopic": _UNRELATED_ALIAS,
                    "statement": 'SELECT * FROM "audit.raw" WHERE severity > 1',
                },
            )
            gateway_state.install(
                aliases=[*target_aliases, unrelated_alias],
                interceptors=[*target_interceptors, unrelated_interceptor],
            )

            compiled_dir = root / "compiled"
            compiled = _run_cli(
                executable,
                "-o",
                "json",
                "compile",
                "-p",
                str(project_dir),
                "--output-dir",
                str(compiled_dir),
                expected=0,
            )
            assert compiled["status"] == "ok", compiled
            compiled_manifest = json.loads(
                (compiled_dir / "manifest.json").read_text(encoding="utf-8")
            )
            assert (
                compiled_manifest["artifacts"]["gateway_rule_removals"][0]["priorArtifact"]
                == prior_artifact
            )

            plan_path = root / "gateway-removal.plan.json"
            planned = _run_cli(
                executable,
                "-o",
                "json",
                "plan",
                "-p",
                str(project_dir),
                "--out",
                str(plan_path),
                expected=0,
            )
            planned_data = planned["data"]
            assert isinstance(planned_data, dict)
            assert planned_data["deletes"] == 1
            assert planned_data["is_apply_blocked"] is False
            assert planned_data["plan_file"] == str(plan_path.resolve())

            reviewed = ReviewedPlanFile.load(plan_path)
            assert reviewed.to_dict()["format_version"] == PLAN_FILE_VERSION == 5
            assert reviewed.offline is False
            assert reviewed.state is not None
            assert reviewed.state.serial == _INITIAL_SERIAL
            assert len(reviewed.actions) == 1
            action = reviewed.actions[0]
            assert action.index == 0
            assert action.resource_id == removed_resource_id
            assert action.action == "delete"
            assert action.gateway_evidence is not None
            assert action.connector_evidence is None
            assert action.gateway_evidence.rule_name == _RULE
            assert action.gateway_evidence.alias_name == _ALIAS
            assert action.gateway_evidence.current.exists is True
            assert action.gateway_evidence.desired.exists is False
            assert admin_url not in plan_path.read_text(encoding="utf-8")

            state_path = local_state_path(project_dir, environment=_ENVIRONMENT)
            before_force = LocalState.load(state_path)
            blocked = _run_cli(
                executable,
                "-o",
                "json",
                "apply",
                "-p",
                str(project_dir),
                "--plan",
                str(plan_path),
                expected=1,
            )
            assert blocked["status"] == "error", blocked
            blocked_errors = blocked["errors"]
            assert isinstance(blocked_errors, list)
            assert blocked_errors
            assert "Destructive ops blocked" in blocked_errors[0]["message"]
            assert gateway_state.snapshot()[2] == []
            assert LocalState.load(state_path).to_dict() == before_force.to_dict()

            applied = _run_cli(
                executable,
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
            applied_data = applied["data"]
            assert isinstance(applied_data, dict)
            assert applied_data["deleted"] == [f"gateway_rule:{_RULE}"]
            assert applied_data["committed"] is True
            assert applied_data["state_serial"] == _INITIAL_SERIAL + 1

            aliases, interceptors, deletes, errors = gateway_state.snapshot()
            assert aliases == [unrelated_alias]
            assert interceptors == [unrelated_interceptor]
            assert deletes == [
                (
                    f"/gateway/v2/interceptor/{_RULE}_filter_0",
                    {
                        "group": None,
                        "username": None,
                        "vCluster": "passthrough",
                    },
                ),
                (
                    "/gateway/v2/alias-topic",
                    {"name": _ALIAS, "vCluster": "passthrough"},
                ),
            ]
            assert errors == []

            committed = LocalState.load(
                state_path,
                expected_project=_PROJECT,
                expected_environment=_ENVIRONMENT,
            )
            assert committed.serial == _INITIAL_SERIAL + 1
            assert removed_resource_id not in committed.resources
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
        assert not server_thread.is_alive()


if __name__ == "__main__":
    _exercise_installed_workflow()
    print("installed-wheel Gateway removal workflow passed")
