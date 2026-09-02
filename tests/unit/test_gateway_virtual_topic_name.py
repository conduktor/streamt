"""Documented Gateway virtual-topic alias resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.model_resolution import CompileError
from streamt.core.parser import ParseError, ProjectParser
from streamt.docs.asyncapi import generate_asyncapi_document
from streamt.docs.odcs import generate_odcs_document


def _project_config(models: list[dict[str, object]]) -> dict[str, object]:
    return {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "gateway-alias-test"},
        "runtime": {
            "kafka": {"bootstrap_servers": "broker.invalid:9092"},
            "conduktor": {
                "gateway": {"admin_url": "https://gateway.example.test"}
            },
        },
        "sources": [{"name": "orders", "topic": "orders.v1"}],
        "models": models,
    }


def _write_project(tmp_path: Path, models: list[dict[str, object]]) -> None:
    (tmp_path / "stream_project.yml").write_text(
        yaml.safe_dump(_project_config(models)),
        encoding="utf-8",
    )


def _gateway_artifacts(tmp_path: Path) -> list[dict[str, object]]:
    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    return manifest.artifacts["gateway_rules"]


def test_documented_gateway_name_drives_alias_and_downstream_input(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        [
            {
                "name": "orders_public",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.public"}},
                "sql": 'SELECT * FROM {{ source("orders") }}',
            },
            {
                "name": "orders_filtered",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.filtered"}},
                "sql": 'SELECT * FROM {{ ref("orders_public") }} WHERE active = true',
            },
        ],
    )

    artifacts = {artifact["name"]: artifact for artifact in _gateway_artifacts(tmp_path)}

    assert artifacts["orders_public"]["virtualTopic"] == "orders.public"
    assert artifacts["orders_filtered"]["virtualTopic"] == "orders.filtered"
    assert artifacts["orders_filtered"]["physicalTopic"] == "orders.public"


def test_documented_gateway_name_drives_asyncapi_and_odcs_output_identity(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        [
            {
                "name": "orders_public",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.public"}},
                "sql": 'SELECT * FROM {{ source("orders") }}',
            }
        ],
    )
    project = ProjectParser(tmp_path).parse()

    asyncapi = generate_asyncapi_document(project)
    odcs = generate_odcs_document(
        project,
        contract_id="urn:streamt:test:gateway-alias",
        status="active",
        contract_version="1.0.0",
    ).document
    schemas = odcs["schema"]
    assert isinstance(schemas, list)
    model_schema = next(item for item in schemas if item["name"] == "orders_public")

    assert asyncapi["channels"]["model.orders_public"]["address"] == "orders.public"
    assert model_schema["physicalName"] == "orders.public"


@pytest.mark.parametrize(
    ("gateway", "topic", "expected"),
    [
        (None, {"name": "orders.legacy"}, "orders.legacy"),
        (
            {"virtual_topic": {"name": "orders.shared"}},
            {"name": "orders.shared"},
            "orders.shared",
        ),
        ({"virtual_topic": {}}, None, "orders_view"),
    ],
    ids=["legacy-topic-fallback", "matching-duplicates", "model-name-fallback"],
)
def test_virtual_topic_alias_compatibility_precedence(
    tmp_path: Path,
    gateway: dict[str, object] | None,
    topic: dict[str, object] | None,
    expected: str,
) -> None:
    model: dict[str, object] = {
        "name": "orders_view",
        "materialized": "virtual_topic",
        "sql": 'SELECT * FROM {{ source("orders") }}',
    }
    if gateway is not None:
        model["gateway"] = gateway
    if topic is not None:
        model["topic"] = topic
    _write_project(tmp_path, [model])

    artifacts = _gateway_artifacts(tmp_path)

    assert len(artifacts) == 1
    assert artifacts[0]["virtualTopic"] == expected


def test_divergent_gateway_and_legacy_topic_aliases_are_rejected(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        [
            {
                "name": "orders_view",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.documented"}},
                "topic": {"name": "orders.legacy"},
                "sql": 'SELECT * FROM {{ source("orders") }}',
            }
        ],
    )

    with pytest.raises(ParseError) as failure:
        ProjectParser(tmp_path).parse()

    assert "gateway.virtual_topic.name and topic.name must match" in str(failure.value)


def test_compiler_defensively_rejects_aliases_diverged_after_parsing(
    tmp_path: Path,
) -> None:
    _write_project(
        tmp_path,
        [
            {
                "name": "orders_view",
                "materialized": "virtual_topic",
                "gateway": {"virtual_topic": {"name": "orders.shared"}},
                "topic": {"name": "orders.shared"},
                "sql": 'SELECT * FROM {{ source("orders") }}',
            }
        ],
    )
    project = ProjectParser(tmp_path).parse()
    topic = project.models[0].topic
    assert topic is not None
    topic.name = "orders.diverged"

    with pytest.raises(
        CompileError,
        match=r"gateway\.virtual_topic\.name and topic\.name must match",
    ):
        Compiler(project).compile(dry_run=True)
