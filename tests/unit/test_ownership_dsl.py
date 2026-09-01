"""Public lifecycle ownership defaults, strictness, and compiler propagation."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from streamt.compiler import Compiler
from streamt.compiler.manifest import ArtifactOwnership, Manifest
from streamt.core.parser import ParseError, ProjectParser
from streamt.deployer.kafka import TopicChange, TopicState
from streamt.deployer.planner import DeploymentPlanner


def _write_project(tmp_path: Path, data: dict[str, object]) -> None:
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(data))


def _base_project() -> dict[str, object]:
    return {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "ownership-test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": [{"name": "raw", "topic": "raw.v1"}],
        "models": [
            {
                "name": "clean",
                "materialized": "topic",
                "sql": 'SELECT * FROM {{ source("raw") }}',
            }
        ],
    }


def test_legacy_declarations_receive_safe_lifecycle_defaults(tmp_path: Path) -> None:
    _write_project(tmp_path, _base_project())

    project = ProjectParser(tmp_path).parse()

    assert project.sources[0].ownership.mode.value == "external"
    assert project.models[0].ownership.mode.value == "managed"


def test_explicit_modes_parse_without_conflating_human_owner(tmp_path: Path) -> None:
    data = _base_project()
    data["sources"][0].update(  # type: ignore[index, union-attr]
        {"owner": "upstream-team", "ownership": {"mode": "managed"}}
    )
    data["models"][0].update(  # type: ignore[index, union-attr]
        {"owner": "analytics-team", "ownership": {"mode": "adopted"}}
    )
    _write_project(tmp_path, data)

    project = ProjectParser(tmp_path).parse()

    assert project.sources[0].owner == "upstream-team"
    assert project.sources[0].ownership.mode.value == "managed"
    assert project.models[0].owner == "analytics-team"
    assert project.models[0].ownership.mode.value == "adopted"


@pytest.mark.parametrize(
    ("ownership", "expected"),
    [
        ({"mode": "claimed"}, "ownership → mode"),
        ({"mode": "external", "grant": True}, "ownership → grant"),
    ],
)
def test_unknown_ownership_modes_and_keys_fail_at_their_path(
    tmp_path: Path,
    ownership: dict[str, object],
    expected: str,
) -> None:
    data = _base_project()
    data["sources"][0]["ownership"] = ownership  # type: ignore[index]
    _write_project(tmp_path, data)

    with pytest.raises(ParseError) as exc_info:
        ProjectParser(tmp_path).parse()

    assert expected in str(exc_info.value)


def test_compiler_propagates_modes_to_every_artifact_kind(tmp_path: Path) -> None:
    data: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "ownership-test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "sources": [
            {
                "name": "raw",
                "topic": "raw.v1",
                "ownership": {"mode": "managed"},
                "schema": {
                    "format": "avro",
                    "definition": '{"type":"record","name":"Raw","fields":[]}',
                },
            }
        ],
        "models": [
            {
                "name": "topic_external",
                "materialized": "topic",
                "ownership": {"mode": "external"},
                "sql": 'SELECT * FROM {{ source("raw") }}',
            },
            {
                "name": "flink_adopted",
                "materialized": "flink",
                "ownership": {"mode": "adopted"},
                "sql": 'SELECT * FROM {{ source("raw") }}',
            },
            {
                "name": "sink_external",
                "ownership": {"mode": "external"},
                "from": [{"source": "raw"}],
                "sink": {"connector": "example.Sink"},
            },
            {
                "name": "gateway_external",
                "materialized": "virtual_topic",
                "ownership": {"mode": "external"},
                "gateway": {"virtual_topic": {"name": "gateway.external.v1"}},
                "sql": 'SELECT * FROM {{ source("raw") }} WHERE id IS NOT NULL',
            },
        ],
    }
    _write_project(tmp_path, data)

    manifest = Compiler(ProjectParser(tmp_path).parse()).compile(dry_run=True)

    schemas = {item["subject"]: item for item in manifest.artifacts["schemas"]}
    topics = {item["name"]: item for item in manifest.artifacts["topics"]}
    jobs = {item["name"]: item for item in manifest.artifacts["flink_jobs"]}
    connectors = {item["name"]: item for item in manifest.artifacts["connectors"]}
    gateways = {item["name"]: item for item in manifest.artifacts["gateway_rules"]}

    assert schemas["raw.v1-value"]["ownership"]["mode"] == "managed"
    assert topics["topic_external"]["ownership"]["mode"] == "external"
    assert jobs["topic_external_processor"]["ownership"]["mode"] == "external"
    assert topics["flink_adopted"]["ownership"]["mode"] == "adopted"
    assert jobs["flink_adopted"]["ownership"]["mode"] == "adopted"
    assert connectors["sink_external"]["ownership"]["mode"] == "external"
    assert gateways["gateway_external"]["ownership"]["mode"] == "external"


def test_declared_adopted_mode_without_state_cannot_create() -> None:
    ownership = ArtifactOwnership(
        project="ownership-test",
        owner_type="model",
        owner_name="clean",
        mode="adopted",
    ).to_dict()
    manifest = Manifest(
        version="1.0",
        project_name="ownership-test",
        artifacts={
            "topics": [
                {
                    "name": "clean.v1",
                    "partitions": 1,
                    "replication_factor": 1,
                    "config": {},
                    "ownership": ownership,
                }
            ]
        },
    )
    kafka = MagicMock()
    kafka.plan_topic.return_value = TopicChange(
        topic="clean.v1",
        action="create",
        current=TopicState(name="clean.v1", exists=False),
    )

    plan = DeploymentPlanner(manifest, kafka_deployer=kafka).plan()

    assert plan.topic_changes[0].action == "none"
    assert plan.ownership_requirements[0].reason == "requires_adoption"
    assert "does not grant authority" in plan.ownership_requirements[0].message
