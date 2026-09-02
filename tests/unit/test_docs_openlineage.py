"""CLI contract tests for deterministic static OpenLineage export."""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

import streamt.cli.commands.docs as docs_command
import streamt.integrations.openlineage as openlineage
from streamt.cli import main
from streamt.compiler import Compiler

JOB_NAMESPACE = "https://lineage.example/namespaces/prod"
KAFKA_NAMESPACE = "kafka://catalog-broker.example:9092"
GATEWAY_NAMESPACE = "kafka://gateway-proxy.example:6969"
EVENT_TIME = "2026-09-01T12:34:56Z"


def _project(
    *,
    bootstrap_servers: str = "broker.example:9092",
    include_pipeline: bool = True,
    api_version: bool = True,
) -> dict[str, object]:
    project: dict[str, object] = {
        "project": {"name": "payments/streams", "version": "2.3.0"},
        "runtime": {
            "kafka": {
                "bootstrap_servers": bootstrap_servers,
                "sasl_password": "RUNTIME_PASSWORD_MUST_NOT_APPEAR",
            }
        },
        "sources": [],
        "models": [],
    }
    if api_version:
        project["apiVersion"] = "streamt.dev/v1alpha1"
    if include_pipeline:
        project["sources"] = [
            {
                "name": "payments_raw",
                "topic": "payments.raw.v1",
                "description": "Raw payments",
                "owner": "source-team",
                "columns": [{"name": "payment_id", "type": "STRING"}],
            }
        ]
        project["models"] = [
            {
                "name": "payments_clean",
                "materialized": "topic",
                "sql": 'SELECT payment_id FROM {{ source("payments_raw") }}',
                "description": "Clean payments",
                "owner": "model-team",
                "topic": {"name": "payments.clean.v2"},
                "columns": [{"name": "payment_id", "type": "STRING"}],
            },
            {
                "name": "warehouse_sink",
                "materialized": "sink",
                "from": "payments_clean",
                "sink": {
                    "connector": "jdbc-sink",
                    "config": {
                        "password": "CONNECTOR_PASSWORD_MUST_NOT_APPEAR",
                        "reviewed_plan": "REVIEWED_PLAN_MUST_NOT_APPEAR",
                    },
                },
            },
        ]
    return project


def _write_project(path: Path, project: dict[str, object]) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )


def _command(path: Path, *extra: str) -> list[str]:
    return [
        "docs",
        "openlineage",
        "--project-dir",
        str(path),
        "--job-namespace",
        JOB_NAMESPACE,
        *extra,
    ]


def _events(stdout: str) -> list[dict[str, object]]:
    assert stdout.endswith("\n")
    return [json.loads(line) for line in stdout.splitlines()]


def _freeze_compiled_at(monkeypatch: pytest.MonkeyPatch) -> list[bool]:
    calls: list[bool] = []
    real_compile = Compiler.compile

    def compile_once(self: Compiler, dry_run: bool = False):  # type: ignore[no-untyped-def]
        calls.append(dry_run)
        manifest = real_compile(self, dry_run=dry_run)
        manifest.compiled_at = EVENT_TIME
        return manifest

    monkeypatch.setattr(Compiler, "compile", compile_once)
    return calls


def test_help_exposes_exact_static_export_surface() -> None:
    runner = CliRunner()
    command_help = runner.invoke(main, ["docs", "openlineage", "--help"])
    group_help = runner.invoke(main, ["docs", "--help"])

    assert command_help.exit_code == group_help.exit_code == 0
    for option in (
        "--job-namespace",
        "--kafka-namespace",
        "--gateway-namespace",
        "--output-file",
        "--project-dir",
        "--env",
    ):
        assert option in command_help.output
    assert "openlineage" in group_help.output


def test_raw_stdout_is_compact_canonical_jsonl_from_one_dry_run_compile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    calls = _freeze_compiled_at(monkeypatch)

    result = CliRunner().invoke(main, _command(tmp_path))

    assert result.exit_code == 0, result.output
    assert calls == [True]
    events = _events(result.stdout)
    assert len(events) == 4
    assert ["dataset" in event for event in events] == [True, True, False, False]
    assert all(event["eventTime"] == EVENT_TIME for event in events)
    assert result.stdout == "".join(
        json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
        for event in events
    )
    datasets = [event["dataset"] for event in events if "dataset" in event]
    assert [dataset["name"] for dataset in datasets] == [  # type: ignore[index]
        "payments.clean.v2",
        "payments.raw.v1",
    ]
    assert all(
        dataset["namespace"] == "kafka://broker.example:9092"  # type: ignore[index]
        for dataset in datasets
    )
    assert "W111_OPENLINEAGE_SINK_OUTPUT_OMITTED" not in result.stdout
    assert "Sink model 'warehouse_sink'" in result.stderr
    for secret in (
        "RUNTIME_PASSWORD_MUST_NOT_APPEAR",
        "CONNECTOR_PASSWORD_MUST_NOT_APPEAR",
        "REVIEWED_PLAN_MUST_NOT_APPEAR",
    ):
        assert secret not in result.stdout
        assert secret not in result.stderr


def test_global_json_is_one_normal_envelope_and_has_no_warning_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project(api_version=False))
    _freeze_compiled_at(monkeypatch)

    result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])

    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    envelope = json.loads(result.stdout)
    assert envelope["status"] == "ok"
    assert envelope["command"] == "docs openlineage"
    assert envelope["errors"] == []
    assert envelope["data"] == {
        "standard": "OpenLineage",
        "release": "1.53.0",
        "core_schema": "2-0-2",
        "events": envelope["data"]["events"],
        "counts": {"total": 4, "datasets": 2, "jobs": 2},
    }
    assert [warning["code"] for warning in envelope["warnings"]] == [
        "W000_WARNING",
        "W111_OPENLINEAGE_SINK_OUTPUT_OMITTED",
    ]
    assert "[yellow]" not in envelope["warnings"][0]["message"]
    assert "warnings" not in envelope["data"]


def test_option_precedence_over_environment_and_explicit_physical_names(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        _command(
            tmp_path,
            "--job-namespace",
            "option-job-namespace",
            "--kafka-namespace",
            KAFKA_NAMESPACE,
        ),
        env={
            "OPENLINEAGE_NAMESPACE": "environment-job-namespace",
            "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE": "kafka://environment:19092",
        },
    )

    assert result.exit_code == 0, result.output
    events = _events(result.stdout)
    assert {
        event["job"]["namespace"]  # type: ignore[index]
        for event in events
        if "job" in event
    } == {"option-job-namespace"}
    assert {
        event["dataset"]["namespace"]  # type: ignore[index]
        for event in events
        if "dataset" in event
    } == {KAFKA_NAMESPACE}


def test_namespace_values_are_loaded_from_project_dotenv_after_parse(tmp_path: Path) -> None:
    _write_project(tmp_path, _project(bootstrap_servers="one:9092,two:9092"))
    (tmp_path / ".env").write_text(
        "OPENLINEAGE_NAMESPACE=dotenv-job\n"
        "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE=kafka://dotenv-broker:19092\n",
        encoding="utf-8",
    )
    names = (
        "OPENLINEAGE_NAMESPACE",
        "STREAMT_OPENLINEAGE_KAFKA_NAMESPACE",
    )
    saved = {name: os.environ.get(name) for name in names}
    for name in names:
        os.environ.pop(name, None)
    try:
        result = CliRunner().invoke(
            main,
            ["docs", "openlineage", "-p", str(tmp_path)],
        )
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value

    assert result.exit_code == 0, result.output
    events = _events(result.stdout)
    assert {
        event["job"]["namespace"]  # type: ignore[index]
        for event in events
        if "job" in event
    } == {"dotenv-job"}
    assert {
        event["dataset"]["namespace"]  # type: ignore[index]
        for event in events
        if "dataset" in event
    } == {"kafka://dotenv-broker:19092"}


@pytest.mark.parametrize(
    ("extra", "environment", "location"),
    [
        (
            ["--kafka-namespace", "not-a-kafka-uri"],
            {"STREAMT_OPENLINEAGE_KAFKA_NAMESPACE": KAFKA_NAMESPACE},
            "kafka_namespace",
        ),
        (
            ["--job-namespace", "   "],
            {"OPENLINEAGE_NAMESPACE": "environment-job"},
            "job_namespace",
        ),
    ],
)
def test_invalid_selected_option_never_falls_through_to_valid_environment(
    tmp_path: Path,
    extra: list[str],
    environment: dict[str, str],
    location: str,
) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, *extra)],
        env=environment,
    )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E506_OPENLINEAGE_INVALID"
    assert error["location"] == location


def test_explicit_unused_dataset_namespace_is_still_validated(tmp_path: Path) -> None:
    _write_project(tmp_path, _project(include_pipeline=False))
    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            *_command(tmp_path, "--gateway-namespace", "https://not-kafka.example"),
        ],
    )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E506_OPENLINEAGE_INVALID"
    assert error["location"] == "gateway_namespace"


def test_missing_job_namespace_and_ambiguous_bootstrap_fail_with_safe_locations(
    tmp_path: Path,
) -> None:
    missing_job = tmp_path / "missing-job"
    missing_job.mkdir()
    _write_project(missing_job, _project())
    multi_broker = tmp_path / "multi-broker"
    multi_broker.mkdir()
    _write_project(multi_broker, _project(bootstrap_servers="one:9092,two:9092"))
    runner = CliRunner()

    first = runner.invoke(
        main,
        ["--output", "json", "docs", "openlineage", "-p", str(missing_job)],
    )
    second = runner.invoke(
        main,
        ["--output", "json", *_command(multi_broker)],
    )

    assert first.exit_code == second.exit_code == 1
    first_error = json.loads(first.stdout)["errors"][0]
    second_error = json.loads(second.stdout)["errors"][0]
    assert first_error["location"] == "job_namespace"
    assert second_error["location"] == "runtime.kafka.bootstrap_servers"
    assert first_error["code"] == second_error["code"] == "E506_OPENLINEAGE_INVALID"


def test_output_file_bytes_equal_raw_stdout_and_confirmation_is_separate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    _write_project(project_dir, _project())
    _freeze_compiled_at(monkeypatch)
    target = tmp_path / "nested" / "lineage.jsonl"
    runner = CliRunner()

    raw = runner.invoke(main, _command(project_dir))
    written = runner.invoke(
        main,
        _command(project_dir, "--output-file", str(target)),
    )

    assert raw.exit_code == written.exit_code == 0
    assert target.read_text(encoding="utf-8") == raw.stdout
    assert written.stdout == f"OpenLineage events written to {target}\n"
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_global_json_with_output_file_retains_events_and_selected_path(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "lineage.jsonl"
    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            *_command(tmp_path, "--output-file", str(target)),
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    envelope = json.loads(result.stdout)
    assert envelope["data"]["output_file"] == str(target)
    assert _events(target.read_text(encoding="utf-8")) == envelope["data"]["events"]
    assert [warning["code"] for warning in envelope["warnings"]] == [
        "W111_OPENLINEAGE_SINK_OUTPUT_OMITTED"
    ]


def test_quiet_export_writes_file_without_stdout_or_stderr(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "lineage.jsonl"
    result = CliRunner().invoke(
        main,
        ["--quiet", *_command(tmp_path, "--output-file", str(target))],
    )

    assert result.exit_code == 0
    assert result.stdout == result.stderr == ""
    assert _events(target.read_text(encoding="utf-8"))


def test_atomic_replace_failure_preserves_target_cleans_stage_and_hides_cause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "SENSITIVE_TARGET_NAME.jsonl"
    target.write_text("original\n", encoding="utf-8")

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("UNSTRUCTURED_WRITE_SECRET")

    monkeypatch.setattr(docs_command.os, "replace", fail_replace)
    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            *_command(tmp_path, "--output-file", str(target)),
        ],
    )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error == {
        "code": "E506_OPENLINEAGE_INVALID",
        "message": "Could not write OpenLineage output file atomically",
        "location": "output_file",
    }
    assert "UNSTRUCTURED_WRITE_SECRET" not in result.stdout + result.stderr
    assert "SENSITIVE_TARGET_NAME" not in result.stdout + result.stderr
    assert target.read_text(encoding="utf-8") == "original\n"
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_staging_fsync_failure_leaves_no_target_or_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "lineage.jsonl"
    monkeypatch.setattr(
        docs_command.os,
        "fsync",
        lambda _fd: (_ for _ in ()).throw(OSError("UNSTRUCTURED_FSYNC_SECRET")),
    )

    result = CliRunner().invoke(
        main,
        _command(tmp_path, "--output-file", str(target)),
    )

    assert result.exit_code == 1
    assert "Could not write OpenLineage output file atomically" in result.stderr
    assert "UNSTRUCTURED_FSYNC_SECRET" not in result.stderr
    assert not target.exists()
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_compile_and_serialization_failures_are_generic_and_emit_no_partial_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())

    with patch.object(
        Compiler,
        "compile",
        side_effect=ValueError("SELECT SQL_LITERAL_SECRET, password='secret'"),
    ):
        compile_failure = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path)],
        )

    def fail_serialization(_events: object) -> str:
        raise ValueError("SERIALIZATION_SECRET")

    monkeypatch.setattr(openlineage, "serialize_static_jsonl", fail_serialization)
    serialization_failure = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path)],
    )

    assert compile_failure.exit_code == serialization_failure.exit_code == 1
    compile_error = json.loads(compile_failure.stdout)["errors"][0]
    serialization_error = json.loads(serialization_failure.stdout)["errors"][0]
    assert compile_error == {
        "code": "E506_OPENLINEAGE_INVALID",
        "message": "Could not compile project for OpenLineage export",
        "location": "models",
    }
    assert serialization_error == {
        "code": "E506_OPENLINEAGE_INVALID",
        "message": "Could not generate validated OpenLineage export",
        "location": "events",
    }
    combined = (
        compile_failure.stdout
        + compile_failure.stderr
        + serialization_failure.stdout
        + serialization_failure.stderr
    )
    assert "SQL_LITERAL_SECRET" not in combined
    assert "SERIALIZATION_SECRET" not in combined


def test_export_is_offline_and_does_not_create_generated_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("OpenLineage CLI attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    result = CliRunner().invoke(main, _command(tmp_path))

    assert result.exit_code == 0, result.output
    assert _events(result.stdout)
    assert not (tmp_path / "generated").exists()


def test_all_materializations_use_exact_physical_outputs_and_namespace_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _project(include_pipeline=False)
    project["runtime"] = {
        "kafka": {"bootstrap_servers": "deployment-broker.example:19092"},
        "conduktor": {
            "gateway": {"proxy_bootstrap": "gateway-proxy.example:6969"}
        },
    }
    project["sources"] = [
        {
            "name": "raw",
            "topic": "raw.physical.v1",
            "columns": [{"name": "id", "type": "BIGINT"}],
        }
    ]
    project["models"] = [
        {
            "name": "provision_only",
            "materialized": "topic",
            "topic": {"name": "provision.physical.v2"},
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
        {
            "name": "sql_topic",
            "materialized": "topic",
            "sql": 'SELECT id FROM {{ source("raw") }}',
            "topic": {"name": "sql.physical.v3"},
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
        {
            "name": "flink_rollup",
            "materialized": "flink",
            "sql": (
                'SELECT id, COUNT(*) AS records FROM {{ ref("sql_topic") }} '
                "GROUP BY id"
            ),
            "topic": {"name": "rollup.physical.v4"},
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "records", "type": "BIGINT"},
            ],
        },
        {
            "name": "gateway_view",
            "materialized": "virtual_topic",
            "from": [{"ref": "flink_rollup"}],
            "topic": {"name": "gateway.virtual.v5"},
            "gateway": {"virtual_topic": {"name": "gateway.virtual.v5"}},
            "columns": [
                {"name": "id", "type": "BIGINT"},
                {"name": "records", "type": "BIGINT"},
            ],
        },
        {
            "name": "gateway_sink",
            "materialized": "sink",
            "from": [{"ref": "gateway_view"}],
            "sink": {"connector": "jdbc-sink", "config": {}},
        },
    ]
    _write_project(tmp_path, project)
    calls = _freeze_compiled_at(monkeypatch)

    result = CliRunner().invoke(
        main,
        _command(tmp_path, "--kafka-namespace", KAFKA_NAMESPACE),
    )

    assert result.exit_code == 0, result.output
    assert calls == [True]
    events = _events(result.stdout)
    datasets = {
        (event["dataset"]["namespace"], event["dataset"]["name"])  # type: ignore[index]
        for event in events
        if "dataset" in event
    }
    assert datasets == {
        (KAFKA_NAMESPACE, "raw.physical.v1"),
        (KAFKA_NAMESPACE, "provision.physical.v2"),
        (KAFKA_NAMESPACE, "sql.physical.v3"),
        (KAFKA_NAMESPACE, "rollup.physical.v4"),
        (GATEWAY_NAMESPACE, "gateway.virtual.v5"),
    }

    jobs = {
        event["job"]["name"].rsplit("/", 1)[-1]: event  # type: ignore[index]
        for event in events
        if "job" in event
    }
    assert set(jobs) == {
        "sql_topic",
        "flink_rollup",
        "gateway_view",
        "gateway_sink",
    }
    assert "provision_only" not in jobs

    def identities(event: dict[str, object], key: str) -> list[tuple[str, str]]:
        return [
            (item["namespace"], item["name"])
            for item in event.get(key, [])  # type: ignore[union-attr]
        ]

    assert identities(jobs["sql_topic"], "inputs") == [
        (KAFKA_NAMESPACE, "raw.physical.v1")
    ]
    assert identities(jobs["sql_topic"], "outputs") == [
        (KAFKA_NAMESPACE, "sql.physical.v3")
    ]
    assert identities(jobs["flink_rollup"], "inputs") == [
        (KAFKA_NAMESPACE, "sql.physical.v3")
    ]
    assert identities(jobs["flink_rollup"], "outputs") == [
        (KAFKA_NAMESPACE, "rollup.physical.v4")
    ]
    assert identities(jobs["gateway_view"], "inputs") == [
        (KAFKA_NAMESPACE, "rollup.physical.v4")
    ]
    assert identities(jobs["gateway_view"], "outputs") == [
        (GATEWAY_NAMESPACE, "gateway.virtual.v5")
    ]
    assert identities(jobs["gateway_sink"], "inputs") == [
        (GATEWAY_NAMESPACE, "gateway.virtual.v5")
    ]
    assert "outputs" not in jobs["gateway_sink"]
    assert "Sink model 'gateway_sink'" in result.stderr


def test_rendered_macro_uses_both_direct_dependencies_without_transitive_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _project(include_pipeline=False)
    project["sources"] = [
        {
            "name": "raw",
            "topic": "raw.direct.v1",
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
        {
            "name": "base_raw",
            "topic": "base.transitive.v1",
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
    ]
    project["models"] = [
        {
            "name": "base",
            "materialized": "topic",
            "sql": 'SELECT id FROM {{ source("base_raw") }}',
            "topic": {"name": "base.direct.v2"},
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
        {
            "name": "combined",
            "materialized": "flink",
            "macro": "combine_inputs",
            "params": {"source_name": "raw", "model_name": "base"},
            "topic": {"name": "combined.physical.v3"},
            "columns": [{"name": "id", "type": "BIGINT"}],
        },
    ]
    _write_project(tmp_path, project)
    macros = tmp_path / "macros"
    macros.mkdir()
    (macros / "combine_inputs.sql.j2").write_text(
        "SELECT direct.id FROM {{ source(source_name) }} AS direct "
        "JOIN {{ ref(model_name) }} AS modeled ON direct.id = modeled.id",
        encoding="utf-8",
    )
    calls = _freeze_compiled_at(monkeypatch)

    result = CliRunner().invoke(main, _command(tmp_path))

    assert result.exit_code == 0, result.output
    assert calls == [True]
    combined = next(
        event
        for event in _events(result.stdout)
        if "job" in event
        and event["job"]["name"].endswith("/models/combined")  # type: ignore[index]
    )
    inputs = [
        (item["namespace"], item["name"])
        for item in combined["inputs"]  # type: ignore[union-attr]
    ]
    assert inputs == [
        ("kafka://broker.example:9092", "base.direct.v2"),
        ("kafka://broker.example:9092", "raw.direct.v1"),
    ]
    assert all(name != "base.transitive.v1" for _namespace, name in inputs)


def test_physical_dataset_collision_is_safe_e506_with_no_partial_raw_output(
    tmp_path: Path,
) -> None:
    project = _project(include_pipeline=False)
    project["sources"] = [
        {
            "name": "existing",
            "topic": "shared.physical.v1",
            "columns": [{"name": "id", "type": "BIGINT"}],
        }
    ]
    project["models"] = [
        {
            "name": "conflicting",
            "materialized": "topic",
            "topic": {"name": "shared.physical.v1"},
            "columns": [{"name": "id", "type": "BIGINT"}],
        }
    ]
    _write_project(tmp_path, project)
    runner = CliRunner()

    raw = runner.invoke(main, _command(tmp_path))
    structured = runner.invoke(
        main,
        ["--output", "json", *_command(tmp_path)],
    )

    assert raw.exit_code == structured.exit_code == 1
    assert raw.stdout == ""
    assert "Two declarations resolve to the same OpenLineage dataset identity" in raw.stderr
    error = json.loads(structured.stdout)["errors"][0]
    assert error == {
        "code": "E506_OPENLINEAGE_INVALID",
        "message": "Two declarations resolve to the same OpenLineage dataset identity",
        "location": "/models/0",
    }


@pytest.mark.parametrize(
    ("extra", "location"),
    [
        ([], "job_namespace"),
        (
            [
                "--job-namespace",
                JOB_NAMESPACE,
                "--kafka-namespace",
                "not-a-kafka-uri",
            ],
            "kafka_namespace",
        ),
    ],
)
def test_semantic_namespace_failures_happen_before_compilation(
    tmp_path: Path,
    extra: list[str],
    location: str,
) -> None:
    _write_project(tmp_path, _project())
    command = [
        "--output",
        "json",
        "docs",
        "openlineage",
        "-p",
        str(tmp_path),
        *extra,
    ]

    with patch.object(
        Compiler,
        "compile",
        side_effect=AssertionError("compiler must not run"),
    ) as compile_mock:
        result = CliRunner().invoke(main, command)

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E506_OPENLINEAGE_INVALID"
    assert error["location"] == location
    compile_mock.assert_not_called()
