"""The installed init starter declares a useful topology without provider access."""

from __future__ import annotations

import json
import socket
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.templates import kafka_streams_starter_resources

IMAGE = "sha256:" + "a" * 64


def starter_args(directory: Path, *extra: str) -> list[str]:
    return [
        "-o",
        "json",
        "init",
        "-p",
        str(directory),
        "--project-name",
        "orders-starter",
        "--executor",
        "kafka_streams",
        "--runner-image",
        IMAGE,
        *extra,
    ]


@pytest.fixture
def deny_providers(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Offline init journey attempted provider access")

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    for target in (
        "confluent_kafka.Producer",
        "confluent_kafka.Consumer",
        "confluent_kafka.admin.AdminClient",
        "streamt.deployer.kafka.KafkaDeployer",
        "streamt.deployer.flink.FlinkDeployer",
        "streamt.deployer.schema_registry.SchemaRegistryDeployer",
        "streamt.deployer.kafka_streams_docker.LocalDockerRunner",
    ):
        monkeypatch.setattr(target, forbidden)


def test_kafka_streams_init_offline_topology_journey(deny_providers, tmp_path):
    runner = CliRunner()
    project_dir = tmp_path / "new-project"
    initialized = runner.invoke(
        main,
        starter_args(
            project_dir,
            "--kafka",
            "localhost:19092",
            "--kafka-internal",
            "broker:9092",
            "--docker-network",
            "existing-test-network",
            "--initial-offset",
            "latest",
        ),
    )
    assert initialized.exit_code == 0, initialized.output
    output = json.loads(initialized.stdout)["data"]
    assert output["support"] == "create_noop_predicate_update"
    assert output["managed_models"] == ["raw_orders", "eligible_orders"]
    assert output["metadata_only_applications"] == ["fraud_app"]
    assert set(output["created_files"]) == {
        "stream_project.yml",
        "README.md",
        "sample_events.jsonl",
        "sources/",
        "models/",
        "tests/",
    }
    config = yaml.safe_load((project_dir / "stream_project.yml").read_text())
    assert config["runtime"]["kafka"] == {
        "bootstrap_servers": "localhost:19092",
        "bootstrap_servers_internal": "broker:9092",
    }
    assert config["runtime"]["kafka_streams"] == {
        "backend": "docker",
        "image": IMAGE,
        "network": "existing-test-network",
        "initial_offset": "latest",
    }
    assert "flink" not in config["runtime"]
    assert all(model["ownership"] == {"mode": "managed"} for model in config["models"])
    assert "sql" not in config["models"][0]
    assert config["models"][1]["executor"] == "kafka_streams"
    for command in (
        ["validate", "--strict"],
        ["lineage"],
        ["compile", "--dry-run"],
        ["plan", "--offline"],
    ):
        result = runner.invoke(main, ["-o", "json", *command, "-p", str(project_dir)])
        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["status"] == "ok"
        if command[0] == "validate":
            assert data["warnings"] == []
        elif command[0] == "lineage":
            edges = {(edge["from"], edge["to"]) for edge in data["data"]["edges"]}
            assert ("raw_orders", "eligible_orders") in edges
            assert ("eligible_orders", "fraud_app") in edges
        elif command[0] == "plan":
            assert data["data"]["creates"] == 3
    project = ProjectParser(project_dir).parse()
    manifest = Compiler(project).compile(dry_run=True)
    assert manifest.artifacts["flink_jobs"] == []
    assert manifest.artifacts["schemas"] == []
    assert len(manifest.artifacts["kafka_streams_jobs"]) == 1
    assert len(manifest.artifacts["topics"]) == 2
    assert not (project_dir / "target").exists()


def test_kafka_streams_sample_matches_documented_strict_shape():
    resources = kafka_streams_starter_resources()
    records = [json.loads(line) for line in resources["sample_events.jsonl"].splitlines()]
    assert len(records) == 4
    for record in records:
        assert set(record) == {"id", "amount", "paid"}
        assert type(record["id"]) is str
        assert record["amount"] is None or type(record["amount"]) is int
        assert type(record["paid"]) is bool
    expected = [
        {"id": record["id"], "amount": record["amount"]}
        for record in records
        if record["amount"] is not None and record["amount"] >= 100 and record["paid"]
    ]
    assert expected == [{"id": "large-paid", "amount": 150}]


def test_kafka_streams_source_variant_stays_external(deny_providers, tmp_path):
    runner = CliRunner()
    result = runner.invoke(main, starter_args(tmp_path))
    assert result.exit_code == 0, result.output
    path = tmp_path / "stream_project.yml"
    config = yaml.safe_load(path.read_text())
    raw = config["models"].pop(0)
    config["sources"] = [
        {
            "name": "existing_orders",
            "topic": "existing.orders.v1",
            "ownership": {"mode": "external"},
            "columns": raw["columns"],
        }
    ]
    config["models"][0]["sql"] = config["models"][0]["sql"].replace(
        'ref("raw_orders")', 'source("existing_orders")'
    )
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    result = runner.invoke(main, ["-o", "json", "validate", "--strict", "-p", str(tmp_path)])
    assert result.exit_code == 0, result.output
    result = runner.invoke(main, ["-o", "json", "plan", "--offline", "-p", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["creates"] == 2
    manifest = Compiler(ProjectParser(tmp_path).parse()).compile(dry_run=True)
    assert len(manifest.artifacts["topics"]) == 1
    assert manifest.artifacts["topics"][0]["name"] == "orders-starter.eligible.orders.v1"


def test_kafka_streams_custom_app_column_break_is_visible_offline(deny_providers, tmp_path):
    runner = CliRunner()
    assert runner.invoke(main, starter_args(tmp_path)).exit_code == 0
    path = tmp_path / "stream_project.yml"
    config = yaml.safe_load(path.read_text())
    config["models"][1]["sql"] = config["models"][1]["sql"].replace(
        "SELECT id, amount", "SELECT id AS order_id, amount"
    )
    config["models"][1]["columns"][0]["name"] = "order_id"
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    result = runner.invoke(main, ["-o", "json", "validate", "--strict", "-p", str(tmp_path)])
    assert result.exit_code == 1, result.output
    assert "fraud_app" in result.output
    assert "id" in result.output


@pytest.mark.parametrize(
    "extra",
    [
        ["--discover"],
        ["--schema-registry", "https://secret.invalid"],
        ["--sasl-password", "must-not-leak"],
        ["--include", "existing.*"],
        ["--runner-image", "mutable:latest"],
        ["--runner-image", "https://user:must-not-leak@invalid"],
        ["--docker-network", "bad/network"],
        ["--project-name", "invalid project"],
        ["--kafka", "https://user:must-not-leak@invalid"],
    ],
)
def test_kafka_streams_options_fail_before_writes_or_provider_access(
    deny_providers, tmp_path, extra
):
    destination = tmp_path / "untouched"
    result = CliRunner().invoke(main, starter_args(destination, *extra))
    assert result.exit_code == 1
    assert json.loads(result.stdout)["status"] == "error"
    assert "must-not-leak" not in result.output
    assert "secret.invalid" not in result.output
    assert not destination.exists()


def test_kafka_streams_requires_explicit_immutable_image(deny_providers, tmp_path):
    result = CliRunner().invoke(main, ["init", "-p", str(tmp_path), "--executor", "kafka_streams"])
    assert result.exit_code == 1
    assert "--runner-image is required" in result.output
    assert not list(tmp_path.iterdir())


def test_flink_rejects_kafka_streams_only_flags_before_discovery(deny_providers, tmp_path):
    result = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "init",
            "-p",
            str(tmp_path),
            "--discover",
            "--kafka",
            "localhost:9092",
            "--runner-image",
            IMAGE,
        ],
    )
    assert result.exit_code == 1
    assert "require --executor kafka_streams" in result.output
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("exists", [False, True])
def test_kafka_streams_dry_run_has_no_writes(deny_providers, tmp_path, exists):
    directory = tmp_path / "preview"
    if exists:
        directory.mkdir()
        (directory / "README.md").write_text("user-owned")
    result = CliRunner().invoke(main, starter_args(directory, "--dry-run"))
    assert result.exit_code == 0, result.output
    if exists:
        assert (directory / "README.md").read_text() == "user-owned"
        assert sorted(path.name for path in directory.iterdir()) == ["README.md"]
    else:
        assert not directory.exists()


@pytest.mark.parametrize("collision", ["README.md", "sample_events.jsonl", "models"])
def test_kafka_streams_conflicts_preflight_all_files(tmp_path, collision):
    (tmp_path / collision).write_text("user-owned")
    result = CliRunner().invoke(main, starter_args(tmp_path))
    assert result.exit_code == 1
    assert (tmp_path / collision).read_text() == "user-owned"
    assert not (tmp_path / "stream_project.yml").exists()


def test_kafka_streams_force_preserves_unrelated_directory_content(tmp_path):
    (tmp_path / "README.md").write_text("old starter")
    (tmp_path / "models").mkdir()
    marker = tmp_path / "models" / ".gitkeep"
    marker.write_text("user marker")
    (tmp_path / "models" / "other.txt").write_text("user content")
    result = CliRunner().invoke(main, starter_args(tmp_path, "--force"))
    assert result.exit_code == 0, result.output
    assert marker.read_text() == "user marker"
    assert (tmp_path / "models" / "other.txt").read_text() == "user content"


@pytest.mark.parametrize("destination", ["README.md", "models"])
def test_kafka_streams_force_never_follows_destination_symlinks(tmp_path, destination):
    outside = tmp_path / "outside"
    outside.write_text("user-owned")
    directory = tmp_path / "project"
    directory.mkdir()
    (directory / destination).symlink_to(outside)
    result = CliRunner().invoke(main, starter_args(directory, "--force"))
    assert result.exit_code == 1
    assert outside.read_text() == "user-owned"
    assert not (directory / "stream_project.yml").exists()


def test_kafka_streams_text_onboarding_does_not_claim_application_execution(tmp_path):
    result = CliRunner().invoke(main, starter_args(tmp_path)[2:], env={"COLUMNS": "300"})
    assert result.exit_code == 0, result.output
    assert "fraud_app is metadata-only" in result.output
    assert "Predicate-only updates require a saved reviewed plan" in result.output
    assert "contact Kafka/Docker" in result.output
    assert "runtime.flink" not in result.output
    readme = (tmp_path / "README.md").read_text()
    assert "ownership.mode: external" in readme
    assert "streamt apply --plan reviewed-plan.json" in readme
    assert "streamt apply --plan filter-change.json" in readme
    assert "state runner-status --plan filter-change.json --operation-id <UUID>" in readme
    assert "state resume --plan filter-change.json --operation-id <UUID>" in readme
    assert "unknown outcome" in readme
    assert "streamt does not deploy" in readme
