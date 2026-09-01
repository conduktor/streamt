"""Focused tests for the additive, read-only Kafka import command."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
import yaml
from click.testing import CliRunner, Result

from streamt.cli import main
from streamt.cli.commands.import_cmd import (
    ImportCommandError,
    _install_no_replace,
    _write_all,
    _write_exclusive,
)
from streamt.core.errors import ErrorCode
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka import TopicState
from streamt.deployer.schema_registry import SchemaState
from streamt.discovery import DiscoveredTopic


class FakeKafkaReader:
    """Read-only fake: it deliberately exposes no mutation methods or admin client."""

    def __init__(
        self,
        topics: list[str],
        *,
        states: dict[str, TopicState | Exception] | None = None,
    ) -> None:
        self.topics = topics
        self.states = states or {}
        self.calls: list[tuple[str, str | None]] = []
        self.close_count = 0

    def list_topic_names(self) -> list[str]:
        self.calls.append(("list_topic_names", None))
        return self.topics

    def get_topic_state(self, topic_name: str) -> TopicState:
        self.calls.append(("get_topic_state", topic_name))
        return self._state(topic_name)

    def get_topic_metadata_state(self, topic_name: str) -> TopicState:
        self.calls.append(("get_topic_metadata_state", topic_name))
        return self._state(topic_name)

    def _state(self, topic_name: str) -> TopicState:
        configured = self.states.get(topic_name)
        if isinstance(configured, Exception):
            raise configured
        if configured is not None:
            return configured
        return TopicState(
            name=topic_name,
            exists=True,
            partitions=3,
            replication_factor=2,
        )

    def close(self) -> None:
        self.close_count += 1


class FakeSchemaReader:
    def __init__(
        self,
        states: dict[str, SchemaState | Exception],
        *,
        list_denied: bool = False,
    ) -> None:
        self.states = states
        self.list_denied = list_denied
        self.calls: list[tuple[object, ...]] = []
        self.close_count = 0

    def list_subjects(self) -> list[str]:
        self.calls.append(("list_subjects",))
        if self.list_denied:
            raise PermissionError("subject listing denied")
        return sorted(self.states)

    def get_schema_state(
        self,
        subject: str,
        *,
        include_compatibility: bool = True,
    ) -> SchemaState:
        self.calls.append(("get_schema_state", subject, include_compatibility))
        configured = self.states.get(subject)
        if isinstance(configured, Exception):
            raise configured
        return configured or SchemaState(subject=subject, exists=False)

    def close(self) -> None:
        self.close_count += 1


def _write_project(
    path: Path,
    *,
    sources: list[dict[str, object]] | None = None,
    models: list[dict[str, object]] | None = None,
    schema_registry: bool = False,
    rules: dict[str, object] | None = None,
) -> None:
    runtime: dict[str, object] = {
        "kafka": {
            "bootstrap_servers": "broker:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "reader",
            "sasl_password": "secret",
        }
    }
    if schema_registry:
        runtime["schema_registry"] = {
            "url": "http://schema-registry:8081",
            "username": "reader",
            "password": "secret",
        }
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "import-test", "version": "1.0.0"},
        "runtime": runtime,
    }
    if sources:
        config["sources"] = sources
    if models:
        config["models"] = models
    if rules:
        config["rules"] = rules
    (path / "stream_project.yml").write_text(yaml.safe_dump(config, sort_keys=False))


def _payload(result: Result) -> dict[str, object]:
    start = result.output.find("{")
    assert start >= 0, result.output
    return json.loads(result.output[start:])


def _invoke(
    project_path: Path,
    kafka: FakeKafkaReader,
    *args: str,
    schema_registry: FakeSchemaReader | None = None,
) -> tuple[Result, object, object]:
    with (
        patch(
            "streamt.cli.commands.import_cmd.make_kafka_deployer",
            return_value=kafka,
        ) as make_kafka,
        patch(
            "streamt.cli.commands.import_cmd.make_sr_deployer",
            return_value=schema_registry,
        ) as make_sr,
    ):
        result = CliRunner().invoke(
            main,
            ["--output", "json", "import", "-p", str(project_path), *args],
        )
    return result, make_kafka, make_sr


def test_import_is_registered_with_bounded_options() -> None:
    result = CliRunner().invoke(main, ["import", "--help"])

    assert result.exit_code == 0
    assert "--include" in result.output
    assert "--exclude" in result.output
    assert "--schemas / --no-schemas" in result.output
    assert "--force" not in result.output


def test_import_writes_only_new_external_sources_in_stable_order(tmp_path: Path) -> None:
    _write_project(
        tmp_path,
        sources=[{"name": "orders_existing", "topic": "orders"}],
    )
    kafka = FakeKafkaReader(["z.events", "orders", "_schemas", "a.events"])

    result, make_kafka, make_sr = _invoke(tmp_path, kafka)

    assert result.exit_code == 0, result.output
    target = tmp_path / "sources" / "imported.kafka.yml"
    raw = yaml.safe_load(target.read_text())
    assert raw == {
        "sources": [
            {
                "name": "a_events",
                "topic": "a.events",
                "ownership": {"mode": "external"},
            },
            {
                "name": "z_events",
                "topic": "z.events",
                "ownership": {"mode": "external"},
            },
        ]
    }
    parsed = ProjectParser(tmp_path).parse()
    assert [(source.name, source.ownership.mode.value) for source in parsed.sources] == [
        ("orders_existing", "external"),
        ("a_events", "external"),
        ("z_events", "external"),
    ]

    payload = _payload(result)
    data = payload["data"]
    assert data["written"] is True
    assert data["created_files"] == ["sources/imported.kafka.yml"]
    assert data["imported_count"] == 2
    assert data["skipped_count"] == 1
    assert [item["topic"] for item in data["resources"]] == [
        "a.events",
        "orders",
        "z.events",
    ]
    assert kafka.calls == [
        ("list_topic_names", None),
        ("get_topic_metadata_state", "a.events"),
        ("get_topic_metadata_state", "orders"),
        ("get_topic_metadata_state", "z.events"),
    ]
    assert kafka.close_count == 1
    make_kafka.assert_called_once()
    project_arg = make_kafka.call_args.args[0]
    assert project_arg.runtime.kafka.bootstrap_servers == "broker:9092"
    assert project_arg.runtime.kafka.sasl_password.get_secret_value() == "secret"
    make_sr.assert_not_called()


def test_import_emits_pinned_avro_and_protobuf_value_schema_refs(tmp_path: Path) -> None:
    _write_project(tmp_path, schema_registry=True)
    kafka = FakeKafkaReader(["proto", "orders"])
    registry = FakeSchemaReader(
        {
            "orders-value": SchemaState(
                subject="orders-value",
                exists=True,
                version=4,
                schema_id=41,
                schema_type="AVRO",
                schema={"fields": [{"name": "id", "type": "string"}]},
            ),
            "proto-value": SchemaState(
                subject="proto-value",
                exists=True,
                version=2,
                schema_id=22,
                schema_type="PROTOBUF",
                schema='syntax = "proto3";',
            ),
        }
    )

    result, _make_kafka, make_sr = _invoke(
        tmp_path,
        kafka,
        schema_registry=registry,
    )

    assert result.exit_code == 0, result.output
    sources = yaml.safe_load((tmp_path / "sources" / "imported.kafka.yml").read_text())["sources"]
    by_topic = {source["topic"]: source for source in sources}
    assert by_topic["orders"]["schema"] == {
        "registry": "confluent",
        "subject": "orders-value",
        "version": 4,
        "format": "avro",
    }
    assert by_topic["orders"]["columns"] == [{"name": "id", "type": "STRING"}]
    assert by_topic["proto"]["schema"]["version"] == 2
    assert by_topic["proto"]["schema"]["format"] == "protobuf"
    assert "columns" not in by_topic["proto"]
    data = _payload(result)["data"]
    assert data["resources"][0]["schema"] == {
        "subject": "orders-value",
        "version": 4,
        "format": "avro",
        "id": 41,
    }
    assert registry.calls == [
        ("get_schema_state", "orders-value", False),
        ("get_schema_state", "proto-value", False),
    ]
    assert registry.close_count == 1
    make_sr.assert_called_once()
    assert make_sr.call_args.kwargs == {"required": False}


def test_import_repeatable_filters_are_or_then_exclude(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = FakeKafkaReader(["b.raw", "a.raw", "a.audit", "other"])

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        kafka,
        "--dry-run",
        "--include",
        "*.raw",
        "--include",
        "a.audit",
        "--exclude",
        "b.*",
    )

    assert result.exit_code == 0, result.output
    assert [item["topic"] for item in _payload(result)["data"]["resources"]] == [
        "a.audit",
        "a.raw",
    ]
    assert not (tmp_path / "sources").exists()


def test_import_refuses_existing_target_without_changing_bytes(tmp_path: Path) -> None:
    _write_project(tmp_path)
    target = tmp_path / "sources" / "imported.kafka.yml"
    target.parent.mkdir()
    target.write_text("sources:\n- name: prior\n  topic: prior\n")
    original = target.read_bytes()
    kafka = FakeKafkaReader(["new-topic"])

    result, _make_kafka, _make_sr = _invoke(tmp_path, kafka)

    assert result.exit_code == 1
    assert target.read_bytes() == original
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_TARGET_EXISTS
    assert kafka.close_count == 1


def test_import_dry_run_reports_existing_target_without_writing(tmp_path: Path) -> None:
    _write_project(tmp_path)
    target = tmp_path / "sources" / "imported.kafka.yml"
    target.parent.mkdir()
    target.write_text("sources:\n- name: prior\n  topic: prior\n")
    original = target.read_bytes()

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["new-topic"]),
        "--dry-run",
    )

    assert result.exit_code == 0, result.output
    assert target.read_bytes() == original
    payload = _payload(result)
    assert payload["data"]["target_exists"] is True
    assert payload["data"]["written"] is False
    assert payload["warnings"][0]["code"] == ErrorCode.IMPORT_TARGET_EXISTS_WARNING


def test_import_no_new_topics_is_successful_and_creates_nothing(tmp_path: Path) -> None:
    _write_project(tmp_path, sources=[{"name": "orders", "topic": "orders.v1"}])

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders.v1"]),
    )

    assert result.exit_code == 0, result.output
    assert not (tmp_path / "sources").exists()
    data = _payload(result)["data"]
    assert data["imported_count"] == 0
    assert data["skipped_count"] == 1
    assert data["created_files"] == []


def test_import_rejects_sanitized_name_collisions_transactionally(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["a-b", "a.b"]),
    )

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_NAME_COLLISION
    assert not (tmp_path / "sources").exists()


def test_import_rejects_collision_with_existing_source_name(tmp_path: Path) -> None:
    _write_project(tmp_path, sources=[{"name": "a_b", "topic": "something-else"}])

    result, _make_kafka, _make_sr = _invoke(tmp_path, FakeKafkaReader(["a-b"]))

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_NAME_COLLISION
    assert not (tmp_path / "sources").exists()


def test_import_rejects_collision_with_existing_model_name(tmp_path: Path) -> None:
    _write_project(tmp_path, models=[{"name": "a_b", "sql": "SELECT 1"}])

    result, _make_kafka, _make_sr = _invoke(tmp_path, FakeKafkaReader(["a-b"]))

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_NAME_COLLISION
    assert not (tmp_path / "sources").exists()


def test_import_rejects_output_path_escape_before_connecting(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result, make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        "--output-file",
        "../outside.yml",
    )

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_PATH_INVALID
    make_kafka.assert_not_called()
    assert not (tmp_path.parent / "outside.yml").exists()


def test_import_rejects_nested_source_path_before_connecting(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result, make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        "--output-file",
        "sources/nested/imported.yml",
    )

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_PATH_INVALID
    make_kafka.assert_not_called()
    assert not (tmp_path / "sources").exists()


def test_exclusive_writer_rejects_sources_symlink_swap_without_artifacts(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    moved_sources = tmp_path / "original-sources"
    outside = tmp_path / "outside"
    sources.mkdir()
    outside.mkdir()
    target = sources / "imported.kafka.yml"

    def swap_sources(fd: int, content: bytes) -> None:
        sources.rename(moved_sources)
        sources.symlink_to(outside, target_is_directory=True)
        _write_all(fd, content)

    with (
        patch(
            "streamt.cli.commands.import_cmd._write_all",
            side_effect=swap_sources,
        ),
        pytest.raises(ImportCommandError) as raised,
    ):
        _write_exclusive(target, "sources: []\n")

    assert raised.value.code == ErrorCode.IMPORT_PATH_INVALID
    assert not (outside / target.name).exists()
    assert list(moved_sources.iterdir()) == []


def test_exclusive_writer_removes_partial_stage_and_final_on_write_failure(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    sources.mkdir()
    target = sources / "imported.kafka.yml"

    def partial_write(fd: int, content: bytes) -> None:
        os.write(fd, content[:7])
        raise OSError("simulated ENOSPC")

    with (
        patch(
            "streamt.cli.commands.import_cmd._write_all",
            side_effect=partial_write,
        ),
        pytest.raises(ImportCommandError) as raised,
    ):
        _write_exclusive(target, "sources:\n- name: orders\n")

    assert raised.value.code == ErrorCode.IMPORT_WRITE_FAILED
    assert list(sources.iterdir()) == []


def test_exclusive_writer_removes_final_when_directory_fsync_fails(tmp_path: Path) -> None:
    sources = tmp_path / "sources"
    sources.mkdir()
    target = sources / "imported.kafka.yml"
    real_fsync = os.fsync
    calls = 0

    def fail_directory_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated directory fsync failure")
        real_fsync(fd)

    with (
        patch("streamt.cli.commands.import_cmd.os.fsync", side_effect=fail_directory_fsync),
        pytest.raises(ImportCommandError) as raised,
    ):
        _write_exclusive(target, "sources: []\n")

    assert raised.value.code == ErrorCode.IMPORT_WRITE_FAILED
    assert list(sources.iterdir()) == []


def test_exclusive_writer_removes_stage_on_close_and_interrupt_failures(
    tmp_path: Path,
) -> None:
    sources = tmp_path / "sources"
    sources.mkdir()
    target = sources / "imported.kafka.yml"

    def fail_after_close(fd: int) -> None:
        os.close(fd)
        raise OSError("simulated close failure")

    with (
        patch(
            "streamt.cli.commands.import_cmd._close_staged_file",
            side_effect=fail_after_close,
        ),
        pytest.raises(ImportCommandError) as close_error,
    ):
        _write_exclusive(target, "sources: []\n")
    assert close_error.value.code == ErrorCode.IMPORT_WRITE_FAILED
    assert list(sources.iterdir()) == []

    with (
        patch(
            "streamt.cli.commands.import_cmd._write_all",
            side_effect=KeyboardInterrupt,
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        _write_exclusive(target, "sources: []\n")
    assert list(sources.iterdir()) == []

    def link_then_interrupt(directory_fd: int, stage_name: str, filename: str) -> None:
        _install_no_replace(directory_fd, stage_name, filename)
        raise KeyboardInterrupt

    with (
        patch(
            "streamt.cli.commands.import_cmd._install_no_replace",
            side_effect=link_then_interrupt,
        ),
        pytest.raises(KeyboardInterrupt),
    ):
        _write_exclusive(target, "sources: []\n")
    assert list(sources.iterdir()) == []


def test_import_schema_registry_failure_warns_and_keeps_kafka_import(tmp_path: Path) -> None:
    _write_project(tmp_path, schema_registry=True)
    registry = FakeSchemaReader({"orders-value": requests.ConnectionError("registry unavailable")})

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        schema_registry=registry,
    )

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    assert payload["status"] == "ok"
    assert payload["warnings"][0]["code"] == ErrorCode.SCHEMA_ENRICHMENT_SKIPPED
    assert payload["data"]["sources"][0].get("schema") is None
    assert registry.calls == [("get_schema_state", "orders-value", False)]
    assert registry.close_count == 1


def test_import_reads_subject_without_list_or_compatibility_permission(tmp_path: Path) -> None:
    _write_project(tmp_path, schema_registry=True)

    class NarrowSchemaReader(FakeSchemaReader):
        def get_schema_state(
            self,
            subject: str,
            *,
            include_compatibility: bool = True,
        ) -> SchemaState:
            if include_compatibility:
                raise PermissionError("compatibility config denied")
            return super().get_schema_state(
                subject,
                include_compatibility=include_compatibility,
            )

    registry = NarrowSchemaReader(
        {
            "orders-value": SchemaState(
                subject="orders-value",
                exists=True,
                version=3,
                schema_id=30,
                schema_type="AVRO",
                schema={"fields": [{"name": "id", "type": "string"}]},
            )
        },
        list_denied=True,
    )

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        schema_registry=registry,
    )

    assert result.exit_code == 0, result.output
    source = _payload(result)["data"]["sources"][0]
    assert source["schema"]["version"] == 3
    assert registry.calls == [("get_schema_state", "orders-value", False)]


def test_import_bounds_schema_requests_after_service_outage(tmp_path: Path) -> None:
    _write_project(tmp_path, schema_registry=True)
    registry = FakeSchemaReader(
        {
            "a-value": requests.ConnectionError("registry unavailable"),
            "b-value": SchemaState(
                subject="b-value",
                exists=True,
                version=1,
                schema_id=2,
                schema_type="AVRO",
                schema={"fields": []},
            ),
        }
    )

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["c", "b", "a"]),
        schema_registry=registry,
    )

    assert result.exit_code == 0, result.output
    assert registry.calls == [("get_schema_state", "a-value", False)]
    payload = _payload(result)
    assert len(payload["warnings"]) == 1
    assert all(source.get("schema") is None for source in payload["data"]["sources"])


def test_import_no_schemas_never_opens_configured_registry(tmp_path: Path) -> None:
    _write_project(tmp_path, schema_registry=True)
    registry = FakeSchemaReader(
        {
            "orders-value": SchemaState(
                subject="orders-value",
                exists=True,
                version=1,
                schema_id=10,
                schema_type="AVRO",
                schema={"fields": []},
            )
        }
    )

    result, _make_kafka, make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        "--no-schemas",
        schema_registry=registry,
    )

    assert result.exit_code == 0, result.output
    make_sr.assert_not_called()
    assert registry.calls == []
    assert registry.close_count == 0
    assert _payload(result)["data"]["sources"][0].get("schema") is None


def test_import_discovery_failure_closes_reader_and_writes_nothing(tmp_path: Path) -> None:
    _write_project(tmp_path)
    kafka = FakeKafkaReader([])

    with patch.object(kafka, "list_topic_names", side_effect=RuntimeError("denied")):
        result, _make_kafka, _make_sr = _invoke(tmp_path, kafka)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_DISCOVERY_FAILED
    assert kafka.close_count == 1
    assert not (tmp_path / "sources").exists()


@pytest.mark.parametrize(
    "state",
    [
        TopicState(name="orders", exists=False),
        TopicState(name="orders", exists=True, partitions=0, replication_factor=1),
        TopicState(name="orders", exists=True, partitions=1, replication_factor=0),
    ],
)
def test_import_rejects_incomplete_topic_metadata_transactionally(
    tmp_path: Path,
    state: TopicState,
) -> None:
    _write_project(tmp_path)

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"], states={"orders": state}),
    )

    assert result.exit_code == 1
    assert _payload(result)["errors"][-1]["code"] == ErrorCode.IMPORT_DISCOVERY_FAILED
    assert not (tmp_path / "sources").exists()


@pytest.mark.parametrize(
    "source_rules",
    [
        {"require_schema": True},
        {"require_freshness": True},
    ],
)
def test_import_rejects_new_governance_failures_before_writing(
    tmp_path: Path,
    source_rules: dict[str, object],
) -> None:
    _write_project(tmp_path, rules={"sources": source_rules})

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
        "--no-schemas",
    )

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_VALIDATION_FAILED
    assert not (tmp_path / "sources").exists()


def test_import_ignores_changed_suggestion_for_preexisting_validation_error(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        models=[
            {
                "name": "already_broken",
                "sql": 'SELECT * FROM {{ source("ordres") }}',
            }
        ],
    )

    result, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["orders"]),
    )

    assert result.exit_code == 0, result.output
    assert (tmp_path / "sources" / "imported.kafka.yml").exists()


def test_import_strictly_rejects_invalid_generated_source(tmp_path: Path) -> None:
    _write_project(tmp_path)
    malformed = DiscoveredTopic(
        source={
            "name": "orders",
            "columns": [{"name": "id", "unsupported": True}],
        },
        topic="orders",
        partitions=1,
        replication_factor=1,
    )
    kafka = FakeKafkaReader([])

    with patch(
        "streamt.cli.commands.import_cmd.discover_topics",
        return_value=[malformed],
    ):
        result, _make_kafka, _make_sr = _invoke(tmp_path, kafka)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_VALIDATION_FAILED
    assert not (tmp_path / "sources").exists()
    assert kafka.close_count == 1


def test_import_json_is_byte_stable_for_identical_dry_runs(tmp_path: Path) -> None:
    _write_project(tmp_path)

    first, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["z", "a"]),
        "--dry-run",
    )
    second, _make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader(["a", "z"]),
        "--dry-run",
    )

    assert first.exit_code == second.exit_code == 0
    assert first.output == second.output


def test_import_redacts_unexpected_failures_in_structured_output(tmp_path: Path) -> None:
    _write_project(tmp_path)

    with patch(
        "streamt.cli.commands.import_cmd._serialize_strict_sources",
        side_effect=RuntimeError("password=TOPSECRET url=https://alice:URLSECRET@registry.invalid"),
    ):
        result, _make_kafka, _make_sr = _invoke(
            tmp_path,
            FakeKafkaReader(["orders"]),
            "--dry-run",
        )

    assert result.exit_code == 1
    assert "TOPSECRET" not in result.output
    assert "URLSECRET" not in result.output
    assert _payload(result)["errors"][0]["code"] == ErrorCode.IMPORT_DISCOVERY_FAILED


def test_import_redacts_invalid_runtime_url_in_single_environment(tmp_path: Path) -> None:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "redaction"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {"url": "ftp://alice:TOPSECRET@registry.invalid"},
                },
            }
        )
    )

    result = CliRunner().invoke(
        main,
        ["--output", "json", "import", "-p", str(tmp_path), "--dry-run"],
    )

    assert result.exit_code == 1
    assert "TOPSECRET" not in result.output
    assert _payload(result)["errors"][0]["code"] == ErrorCode.PARSE_ERROR


def test_import_redacts_invalid_runtime_url_in_selected_environment(tmp_path: Path) -> None:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "redaction"},
            }
        )
    )
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "prod.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "broker:9092"},
                    "schema_registry": {"url": "ftp://alice:TOPSECRET@registry.invalid"},
                },
            }
        )
    )

    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            "import",
            "-p",
            str(tmp_path),
            "--env",
            "prod",
            "--dry-run",
        ],
    )

    assert result.exit_code == 1
    assert "TOPSECRET" not in result.output
    assert _payload(result)["errors"][0]["code"] == ErrorCode.ENVIRONMENT_ERROR


def test_import_uses_selected_environment_runtime(tmp_path: Path) -> None:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "multi-import"},
            }
        )
    )
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "prod.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "prod"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "prod-broker:9093",
                        "security_protocol": "SASL_SSL",
                        "sasl_mechanism": "PLAIN",
                        "sasl_username": "prod-reader",
                        "sasl_password": "prod-secret",
                    }
                },
            }
        )
    )

    result, make_kafka, _make_sr = _invoke(
        tmp_path,
        FakeKafkaReader([]),
        "--env",
        "prod",
    )

    assert result.exit_code == 0, result.output
    project_arg = make_kafka.call_args.args[0]
    assert project_arg.runtime.kafka.bootstrap_servers == "prod-broker:9093"
    assert project_arg.runtime.kafka.sasl_username == "prod-reader"
    assert _payload(result)["data"]["environment"] == "prod"
