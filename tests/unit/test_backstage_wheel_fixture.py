"""Keep the installed-wheel Backstage fixture covered by the normal unit gate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from tests.package import backstage_catalog_wheel_smoke as smoke


def test_backstage_wheel_fixture_compiles_and_exports_offline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Backstage wheel fixture attempted external access")

    for target in (
        "streamt.deployer.kafka.KafkaDeployer",
        "streamt.deployer.schema_registry.SchemaRegistryDeployer",
        "streamt.deployer.flink.FlinkDeployer",
        "streamt.deployer.connect.ConnectDeployer",
        "streamt.deployer.gateway.GatewayDeployer",
        "streamt.deployer.state_backend.DeploymentStateService",
        "socket.getaddrinfo",
        "socket.create_connection",
        "socket.socket.connect",
        "socket.socket.connect_ex",
        "requests.sessions.Session.request",
        "subprocess.Popen",
        "subprocess.run",
    ):
        monkeypatch.setattr(target, forbidden)

    smoke._write_project(tmp_path)
    owner_map = tmp_path / "owners.json"
    smoke._write_owner_map(owner_map)

    project = ProjectParser(tmp_path).parse()
    compiler = Compiler(project)
    compiler.compile(dry_run=True)
    assert {exposure.name for exposure in project.exposures} == {
        "orders_dashboard", "orders_application"
    }
    assert {"orders_dashboard", "orders_application"} <= compiler.dag.nodes.keys()
    assert compiler.dag.nodes["orders_dashboard"].upstream == {"public_orders"}
    assert compiler.dag.nodes["orders_application"].upstream == {"plain_orders"}
    assert not (tmp_path / "generated").exists()

    arguments = smoke._common_arguments(tmp_path, owner_map)
    text_result = smoke._invoke(*arguments)
    structured_result = smoke._invoke("-o", "json", *arguments)
    payload = json.loads(structured_result.stdout)
    entities = payload["data"]["entities"]
    assert payload["data"]["counts"] == {"System": 1, "Resource": 4, "Component": 3}
    assert list(yaml.safe_load_all(text_result.stdout)) == entities
    smoke._assert_entities(entities)
    smoke._assert_warnings(payload, text_result.stderr)
    smoke._assert_secret_neutral(
        text_result.stdout + text_result.stderr + structured_result.stdout,
        checkout=Path(__file__).resolve().parents[2],
    )
