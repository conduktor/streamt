"""Integration tests for streamt init --discover (requires Docker infra)."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
from confluent_kafka.admin import AdminClient, NewTopic

from streamt.cli import main
from tests.integration.helpers import INFRA_CONFIG


def parse_json_output(output: str) -> dict:
    idx = output.find("{")
    if idx == -1:
        raise ValueError(f"No JSON found in output: {output!r}")
    return json.loads(output[idx:])


DISCOVER_TOPICS = {
    "orders.raw.v1": {"partitions": 6, "replication": 1},
    "users.events.v1": {"partitions": 3, "replication": 1},
    "payments.processed.v1": {"partitions": 12, "replication": 1},
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "customer_id", "type": "string"},
        {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
}


@pytest.fixture(scope="module")
def setup_discover_topics(docker_services):
    """Create test topics and register schemas for discover tests."""
    admin = AdminClient({"bootstrap.servers": INFRA_CONFIG.kafka_bootstrap_servers})

    # Create topics
    new_topics = [
        NewTopic(name, num_partitions=cfg["partitions"], replication_factor=cfg["replication"])
        for name, cfg in DISCOVER_TOPICS.items()
    ]
    futures = admin.create_topics(new_topics)
    for _name, future in futures.items():
        try:
            future.result(timeout=10)
        except Exception:
            pass  # Topic may already exist

    # Register Avro schema for orders topic
    import requests
    sr_url = INFRA_CONFIG.schema_registry_url
    requests.post(
        f"{sr_url}/subjects/orders.raw.v1-value/versions",
        json={"schema": json.dumps(AVRO_SCHEMA), "schemaType": "AVRO"},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )

    yield

    # Cleanup: delete topics
    admin.delete_topics(list(DISCOVER_TOPICS.keys()))


class TestInitDiscover:
    """Tests for streamt init --discover."""

    def test_discover_finds_topics(self, setup_discover_topics):
        """--discover lists and generates sources from Kafka topics."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
            ])

            assert result.exit_code == 0, result.output

            # stream_project.yml should exist with runtime pointing to discovered Kafka
            config = yaml.safe_load((Path(tmpdir) / "stream_project.yml").read_text())
            assert config["runtime"]["kafka"]["bootstrap_servers"] == INFRA_CONFIG.kafka_bootstrap_servers

            # Sources should be generated
            sources_dir = Path(tmpdir) / "sources"
            assert sources_dir.is_dir()
            source_files = list(sources_dir.glob("*.yml"))
            assert len(source_files) > 0

            # All discover topics should appear as sources
            all_sources = []
            for f in source_files:
                data = yaml.safe_load(f.read_text())
                if "sources" in data:
                    all_sources.extend(data["sources"])

            source_topics = {s["topic"] for s in all_sources}
            for topic in DISCOVER_TOPICS:
                assert topic in source_topics, f"Topic {topic} not found in discovered sources"

    def test_discover_excludes_internal_topics(self, setup_discover_topics):
        """--discover excludes internal topics (__consumer_offsets, _schemas, etc.)."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
            ])

            assert result.exit_code == 0

            all_sources = []
            for f in (Path(tmpdir) / "sources").glob("*.yml"):
                data = yaml.safe_load(f.read_text())
                if "sources" in data:
                    all_sources.extend(data["sources"])

            source_topics = {s["topic"] for s in all_sources}
            assert "__consumer_offsets" not in source_topics
            assert "_schemas" not in source_topics

    def test_discover_with_schema_registry(self, setup_discover_topics):
        """--discover with --schema-registry extracts columns from Avro schemas."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
                "--schema-registry", INFRA_CONFIG.schema_registry_url,
            ])

            assert result.exit_code == 0, result.output

            # Find the orders source
            all_sources = []
            for f in (Path(tmpdir) / "sources").glob("*.yml"):
                data = yaml.safe_load(f.read_text())
                if "sources" in data:
                    all_sources.extend(data["sources"])

            orders = next((s for s in all_sources if s["topic"] == "orders.raw.v1"), None)
            assert orders is not None, "orders.raw.v1 source not found"

            # Should have columns from Avro schema
            assert "columns" in orders
            col_names = [c["name"] for c in orders["columns"]]
            assert "order_id" in col_names
            assert "amount" in col_names
            assert "customer_id" in col_names
            assert "created_at" in col_names

    def test_discover_include_filter(self, setup_discover_topics):
        """--include filters to matching topics only."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
                "--include", "orders.*",
            ])

            assert result.exit_code == 0

            all_sources = []
            for f in (Path(tmpdir) / "sources").glob("*.yml"):
                data = yaml.safe_load(f.read_text())
                if "sources" in data:
                    all_sources.extend(data["sources"])

            source_topics = {s["topic"] for s in all_sources}
            assert "orders.raw.v1" in source_topics
            assert "users.events.v1" not in source_topics
            assert "payments.processed.v1" not in source_topics

    def test_discover_exclude_filter(self, setup_discover_topics):
        """--exclude removes matching topics."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
                "--exclude", "payments.*",
            ])

            assert result.exit_code == 0

            all_sources = []
            for f in (Path(tmpdir) / "sources").glob("*.yml"):
                data = yaml.safe_load(f.read_text())
                if "sources" in data:
                    all_sources.extend(data["sources"])

            source_topics = {s["topic"] for s in all_sources}
            assert "orders.raw.v1" in source_topics
            assert "users.events.v1" in source_topics
            assert "payments.processed.v1" not in source_topics

    def test_discover_dry_run(self, setup_discover_topics):
        """--dry-run shows what would be created without writing files."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
                "--dry-run",
            ])

            assert result.exit_code == 0

            # No files should be created
            assert not (Path(tmpdir) / "stream_project.yml").exists()
            assert not (Path(tmpdir) / "sources").exists()

    def test_discover_json_output(self, setup_discover_topics):
        """--discover -o json returns structured discovery results."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "-o", "json", "init", "-p", tmpdir, "--discover",
                "--kafka", INFRA_CONFIG.kafka_bootstrap_servers,
                "--schema-registry", INFRA_CONFIG.schema_registry_url,
            ])

            assert result.exit_code == 0, result.output
            data = parse_json_output(result.output)
            assert data["status"] == "ok"
            assert data["command"] == "init"
            assert "discovered_topics" in data["data"]
            assert "created_files" in data["data"]

            topics = data["data"]["discovered_topics"]
            topic_names = [t["topic"] for t in topics]
            assert "orders.raw.v1" in topic_names

    def test_discover_without_kafka_flag_fails(self):
        """--discover without --kafka should fail."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "init", "-p", tmpdir, "--discover",
            ])

            assert result.exit_code != 0
