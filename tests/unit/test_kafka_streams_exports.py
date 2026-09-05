"""Existing export consumers retain the new executor's topology without provider access."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.models import StreamtProject
from streamt.integrations.catalog.model import build_catalog_snapshot
from tests.unit.test_cli_backstage import _command as backstage_command
from tests.unit.test_cli_datahub import _command as datahub_command
from tests.unit.test_docs_openlineage import _command as openlineage_command
from tests.unit.test_kafka_streams_compiler import _config


def test_catalog_snapshot_keeps_processor_and_exact_source_output_dependencies() -> None:
    project = StreamtProject.model_validate(_config())
    compiler = Compiler(project)
    compiler.compile(dry_run=True)
    snapshot = build_catalog_snapshot(project, compiler.resolved_models, compiler.compiled_models, "default")
    assert len(snapshot.processes) == 1
    process = snapshot.processes[0]
    assert process.process_kind == "kafka_streams"
    assert process.logical_name == "valuable_orders"
    assert len(process.dependencies) == 1
    assert process.dependencies[0].logical_name == "orders"
    assert {dataset.physical_name for dataset in snapshot.datasets} == {"orders.raw.v1", "valuable_orders"}


@pytest.mark.parametrize("kind", ["backstage", "datahub", "openlineage"])
def test_kafka_streams_exports_remain_offline_and_do_not_expose_runtime_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    config = _config()
    config["runtime"]["kafka"].update({
        "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN",
        "sasl_username": "never-export-runtime-user", "sasl_password": "never-export-runtime-password",
    })
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Offline export attempted provider access")

    for target in (
        "socket.getaddrinfo", "socket.create_connection", "subprocess.run", "subprocess.Popen",
        "streamt.deployer.kafka.KafkaDeployer", "streamt.deployer.kafka_streams.KafkaStreamsDeployer",
    ):
        monkeypatch.setattr(target, forbidden)
    arguments = {
        "backstage": backstage_command(tmp_path),
        "datahub": datahub_command(tmp_path),
        "openlineage": openlineage_command(tmp_path, "--kafka-namespace", "kafka://declared-cluster:9092"),
    }[kind]
    result = CliRunner().invoke(main, ["-o", "json", *arguments])
    assert result.exit_code == 0, (result.stdout, result.stderr, repr(result.exception))
    assert json.loads(result.stdout)["status"] == "ok"
    assert "valuable_orders" in result.stdout
    assert "orders.raw.v1" in result.stdout
    if kind != "openlineage":
        assert "kafka_streams" in result.stdout
    assert "never-export-runtime" not in result.stdout + result.stderr
    assert "broker-private" not in result.stdout + result.stderr
    assert not (tmp_path / "generated").exists()
