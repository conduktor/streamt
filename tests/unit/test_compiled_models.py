"""Tests for the compiler's secret-free primary model projection."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict, fields
from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.compiled_models import (
    CompiledModelView,
    freeze_compiled_models,
)
from streamt.core.models import MaterializedType
from streamt.core.parser import ProjectParser


def _parse(tmp_path: Path, config: dict[str, object]):
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    return ProjectParser(tmp_path).parse()


def _base_config() -> dict[str, object]:
    return {
        "project": {"name": "projection-test", "version": "1.0.0"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": [
            {
                "name": "raw",
                "topic": "raw.physical",
                "columns": [{"name": "id", "type": "BIGINT"}],
            }
        ],
    }


def test_topic_views_distinguish_provisioning_from_sql_processor(tmp_path: Path) -> None:
    """Topic SQL creates a semantic model process without exposing its artifact suffix."""
    config = _base_config()
    config["models"] = [
        {"name": "plain", "topic": {"name": "plain.override"}},
        {
            "name": "processed",
            "sql": 'SELECT * FROM {{ source("raw") }}',
            "topic": {"name": "processed.override"},
        },
    ]
    compiler = Compiler(_parse(tmp_path, config))
    compiler.compile(dry_run=True)

    assert compiler.compiled_models["plain"] == CompiledModelView(
        model_name="plain",
        materialized=MaterializedType.TOPIC,
        process_kind=None,
        output_kind="kafka",
        output_name="plain.override",
        gateway_physical_input=None,
        connector_inputs=(),
    )
    assert compiler.compiled_models["processed"] == CompiledModelView(
        model_name="processed",
        materialized=MaterializedType.TOPIC,
        process_kind="flink",
        output_kind="kafka",
        output_name="processed.override",
        gateway_physical_input=None,
        connector_inputs=(),
    )
    assert "processed_processor" not in repr(compiler.compiled_models)


def test_effective_flink_view_uses_compiled_output_override(tmp_path: Path) -> None:
    """Gateway fallback is projected as the Flink path that actually compiled."""
    config = _base_config()
    config["models"] = [
        {
            "name": "projected",
            "sql": 'SELECT id FROM {{ source("raw") }}',
            "topic": {"name": "projected.override"},
        }
    ]
    compiler = Compiler(_parse(tmp_path, config))
    compiler.compile(dry_run=True)

    assert compiler.compiled_models["projected"] == CompiledModelView(
        model_name="projected",
        materialized=MaterializedType.FLINK,
        process_kind="flink",
        output_kind="kafka",
        output_name="projected.override",
        gateway_physical_input=None,
        connector_inputs=(),
    )


def test_gateway_view_uses_virtual_output_and_exact_physical_input(tmp_path: Path) -> None:
    """Gateway projection comes from the generated rule's successful path."""
    config = _base_config()
    config["runtime"] = {
        "kafka": {"bootstrap_servers": "localhost:9092"},
        "conduktor": {"gateway": {"proxy_bootstrap": "gateway:6969"}},
    }
    config["models"] = [
        {
            "name": "filtered",
            "sql": 'SELECT id FROM {{ source("raw") }} WHERE id > 0',
            "topic": {"name": "filtered.virtual"},
        }
    ]
    compiler = Compiler(_parse(tmp_path, config))
    compiler.compile(dry_run=True)

    assert compiler.compiled_models["filtered"] == CompiledModelView(
        model_name="filtered",
        materialized=MaterializedType.VIRTUAL_TOPIC,
        process_kind="gateway",
        output_kind="gateway",
        output_name="filtered.virtual",
        gateway_physical_input="raw.physical",
        connector_inputs=(),
    )


def test_sink_view_contains_only_physical_connector_inputs(tmp_path: Path) -> None:
    """Connect config and connection secrets never enter the primary projection."""
    config = _base_config()
    config["runtime"] = {
        "kafka": {
            "bootstrap_servers": "localhost:9092",
            "sasl_password": "runtime-super-secret",
        }
    }
    config["connections"] = {
        "warehouse": {
            "type": "snowflake",
            "config": {"password": "connection-super-secret"},
        }
    }
    config["models"] = [
        {
            "name": "warehouse_sink",
            "from": [{"source": "raw"}],
            "sink": {
                "connector": "snowflake-sink",
                "connection": "warehouse",
                "config": {"api.token": "connector-super-secret"},
            },
        },
        {
            "name": "sql_secret",
            "sql": "SELECT id, 'sql-super-secret' AS marker "
            'FROM {{ source("raw") }}',
        },
    ]
    compiler = Compiler(_parse(tmp_path, config))
    compiler.compile(dry_run=True)

    assert compiler.compiled_models["warehouse_sink"] == CompiledModelView(
        model_name="warehouse_sink",
        materialized=MaterializedType.SINK,
        process_kind="connect",
        output_kind=None,
        output_name=None,
        gateway_physical_input=None,
        connector_inputs=("raw.physical",),
    )
    rendered = json.dumps(
        [asdict(view) for view in compiler.compiled_models.values()],
        sort_keys=True,
    )
    assert "connection-super-secret" not in rendered
    assert "connector-super-secret" not in rendered
    assert "runtime-super-secret" not in rendered
    assert "sql-super-secret" not in rendered
    assert "password" not in rendered
    assert "api.token" not in rendered


def test_projection_excludes_continuous_tests_and_dlq_topics(tmp_path: Path) -> None:
    """Secondary test and DLQ artifacts cannot become model primary views."""
    config = _base_config()
    config["models"] = [
        {"name": "clean", "sql": 'SELECT * FROM {{ source("raw") }}'}
    ]
    config["tests"] = [
        {
            "name": "clean_ids",
            "model": "clean",
            "type": "continuous",
            "assertions": [{"not_null": {"columns": ["id"]}}],
            "on_failure": {
                "severity": "error",
                "actions": [{"dlq": {"topic": "clean.dead.letters"}}],
            },
        }
    ]
    compiler = Compiler(_parse(tmp_path, config))
    manifest = compiler.compile(dry_run=True)

    assert tuple(compiler.compiled_models) == ("clean",)
    assert compiler.compiled_models["clean"].output_name == "clean"
    assert any(
        job["name"] == "test_clean_ids"
        for job in manifest.artifacts["flink_jobs"]
    )
    assert any(
        topic["name"] == "clean.dead.letters"
        for topic in manifest.artifacts["topics"]
    )
    assert "test_clean_ids" not in repr(compiler.compiled_models)
    assert "clean.dead.letters" not in repr(compiler.compiled_models)


def test_projection_is_sorted_structurally_immutable_and_exact(tmp_path: Path) -> None:
    """The public mapping and records expose only the seven fixed safe fields."""
    config = _base_config()
    config["models"] = [{"name": "z_model"}, {"name": "a_model"}]
    compiler = Compiler(_parse(tmp_path, config))
    compiler.compile(dry_run=True)

    assert tuple(compiler.compiled_models) == ("a_model", "z_model")
    view = compiler.compiled_models["a_model"]
    assert tuple(field.name for field in fields(view)) == (
        "model_name",
        "materialized",
        "process_kind",
        "output_kind",
        "output_name",
        "gateway_physical_input",
        "connector_inputs",
    )
    with pytest.raises(TypeError):
        compiler.compiled_models["new"] = view  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        view.output_name = "changed"  # type: ignore[misc]


def test_repeat_and_write_failure_publication_boundary(tmp_path: Path, monkeypatch) -> None:
    """Only a complete invocation publishes, and later failure removes stale data."""
    config = _base_config()
    config["models"] = [{"name": "plain"}]
    compiler = Compiler(_parse(tmp_path, config), tmp_path / "generated")
    compiler.compile(dry_run=True)
    first = compiler.compiled_models

    compiler.compile(dry_run=True)
    assert compiler.compiled_models == first
    assert compiler.compiled_models is not first

    observed_during_write: list[dict[str, CompiledModelView]] = []

    def fail_write() -> None:
        observed_during_write.append(dict(compiler.compiled_models))
        raise OSError("forced write failure")

    monkeypatch.setattr(compiler, "_write_artifacts", fail_write)
    with pytest.raises(OSError, match="forced write failure"):
        compiler.compile(dry_run=False)

    assert observed_during_write == [{}]
    assert not compiler.compiled_models
    assert not compiler.resolved_models
    assert compiler.dag.nodes == {}


def test_freezer_requires_exactly_one_view_per_resolved_model() -> None:
    """Coverage and duplicate errors are deterministic before publication."""
    view = CompiledModelView(
        model_name="model",
        materialized=MaterializedType.TOPIC,
        process_kind=None,
        output_kind="kafka",
        output_name="topic",
        gateway_physical_input=None,
        connector_inputs=(),
    )

    with pytest.raises(ValueError, match=r"missing \['missing'\]"):
        freeze_compiled_models([view], expected_model_names=["model", "missing"])
    with pytest.raises(ValueError, match="Duplicate compiled model projection 'model'"):
        freeze_compiled_models(
            [view, view],
            expected_model_names=["model"],
        )
    with pytest.raises(ValueError, match="tuple of strings"):
        CompiledModelView(
            model_name="invalid",
            materialized=MaterializedType.TOPIC,
            process_kind=None,
            output_kind="kafka",
            output_name="topic",
            gateway_physical_input=None,
            connector_inputs=[],  # type: ignore[arg-type]
        )
