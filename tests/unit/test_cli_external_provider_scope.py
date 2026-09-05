"""Provider selection follows lifecycle authority, not configured integrations."""

from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.cli.helpers import required_deployer_services
from streamt.compiler.manifest import ArtifactOwnership, Manifest

FACTORIES = ("kafka", "sr", "flink", "connect", "gateway")
SERVICES = {
    "topics": "Kafka",
    "schemas": "Schema Registry",
    "flink_jobs": "Flink",
    "connectors": "Kafka Connect",
    "gateway_rules": "Conduktor Gateway",
    "connector_removals": "Kafka Connect",
    "gateway_rule_removals": "Conduktor Gateway",
}


def manifest_with(kind: str, ownership: object = None) -> Manifest:
    manifest = Manifest(version="1.0.0", project_name="scope")
    artifact = {"name": "orders"}
    if ownership is not None:
        artifact["ownership"] = ownership
    manifest.artifacts[kind] = [artifact]
    return manifest


@pytest.mark.parametrize(("kind", "service"), SERVICES.items())
@pytest.mark.parametrize("mode", [None, "managed", "adopted", "external"])
def test_required_services_preserve_managed_evidence_and_removals(kind, service, mode):
    ownership = ArtifactOwnership("scope", "model", "orders", mode).to_dict() if mode else None
    services = required_deployer_services(manifest_with(kind, ownership))
    if mode == "external" and kind not in {"connector_removals", "gateway_rule_removals"}:
        assert services == frozenset()
    else:
        assert services == frozenset({service, "Kafka"})


@pytest.mark.parametrize("kind", SERVICES)
def test_foreign_external_is_not_a_provider_exemption(kind):
    ownership = ArtifactOwnership("foreign", "model", "orders", "external").to_dict()
    assert SERVICES[kind] in required_deployer_services(manifest_with(kind, ownership))


@pytest.mark.parametrize("ownership", [
    {}, "external", {"mode": "external"},
    {"project": "scope", "type": "model", "name": "orders", "mode": "unknown"},
    {"project": "scope", "type": "model", "name": "   ", "mode": "external"},
])
def test_malformed_ownership_fails_locally(ownership):
    with pytest.raises(ValueError, match="invalid ownership metadata"):
        required_deployer_services(manifest_with("topics", ownership))


def test_explicit_null_ownership_is_not_legacy():
    manifest = manifest_with("topics")
    manifest.artifacts["topics"][0]["ownership"] = None
    with pytest.raises(ValueError, match="invalid ownership metadata"):
        required_deployer_services(manifest)


def write_project(path: Path, *, managed: bool = False) -> None:
    (path / "stream_project.yml").write_text(yaml.safe_dump({
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "scope"},
        "runtime": {
            "kafka": {"bootstrap_servers": "unreachable.invalid:9092"},
            "schema_registry": {"url": "http://unreachable.invalid:8081"},
        },
        "sources": [{
            "name": "raw", "topic": "raw.orders",
            "schema": {"registry": "confluent", "subject": "orders-value", "version": 1},
            "columns": [{"name": "id", "type": "STRING"}],
        }],
        "models": [{
            "name": "orders", "ownership": {"mode": "managed" if managed else "external"},
            "materialized": "topic", "sql": 'SELECT id FROM {{ source("raw") }}',
        }],
    }))


@pytest.mark.parametrize(("command", "args"), [("plan", []), ("apply", ["--dry-run"]), ("apply", [])])
def test_external_only_online_workflow_never_constructs_runtime_clients(tmp_path, command, args):
    write_project(tmp_path)
    with ExitStack() as stack:
        factories = [stack.enter_context(patch(
            f"streamt.cli.commands.{command}.make_{kind}_deployer",
            side_effect=AssertionError(f"unexpected {kind} runtime access"),
        )) for kind in FACTORIES]
        result = CliRunner().invoke(main, ["-o", "json", command, "-p", str(tmp_path), *args])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert all(factory.call_count == 0 for factory in factories)
    if command == "plan" or args:
        assert payload["data"]["has_changes"] is False
    if command == "plan":
        requirements = payload["data"]["ownership_requirements"]
        assert {row["kind"] for row in requirements} == {"schema", "topic", "flink_job"}
        assert all(row["reason"] == "external" for row in requirements)
        assert all(row["observed_action"] == "none" for row in requirements)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_managed_sql_requires_flink_without_querying_external_registry(tmp_path, command):
    write_project(tmp_path, managed=True)
    with ExitStack() as stack:
        factories = {}
        for kind in FACTORIES:
            factories[kind] = stack.enter_context(patch(
                f"streamt.cli.commands.{command}.make_{kind}_deployer",
                **({"return_value": MagicMock()} if kind == "kafka" else
                   {"return_value": None} if kind == "flink" else
                   {"side_effect": AssertionError(f"unexpected {kind} runtime access")}),
            ))
        result = CliRunner().invoke(main, ["-o", "json", command, "-p", str(tmp_path)])
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert any("Flink" in error["message"] and "not configured" in error["message"]
               for error in payload["errors"])
    factories["kafka"].assert_called_once()
    factories["sr"].assert_not_called()
    factories["connect"].assert_not_called()
    factories["gateway"].assert_not_called()
