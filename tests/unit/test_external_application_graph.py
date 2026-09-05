"""Declared custom applications participate in the graph without provider authority."""

from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.core.dag import DAGBuilder, DAGCycleError, NodeType
from streamt.core.models import Exposure, StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _project(tmp_path: Path, exposures: list[dict[str, object]]) -> StreamtProject:
    config = {
        "project": {"name": "external-applications"},
        "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
        "sources": [
            {
                "name": name,
                "topic": f"company.{name}",
                "ownership": {"mode": "external"},
                "columns": [{"name": "id", "type": "STRING"}],
            }
            for name in ("orders", "enriched")
        ],
        "models": [
            {
                "name": "cleaned",
                "materialized": "topic",
                "sql": "SELECT id FROM {{ source('enriched') }}",
            },
            {"name": "application_output", "materialized": "topic"},
        ],
        "exposures": exposures,
    }
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    return ProjectParser(tmp_path).parse()


@pytest.mark.parametrize("relationship", ["produces", "consumes", "depends_on"])
@pytest.mark.parametrize("reference", [{"source": "orders"}, {"ref": "application_output"}])
def test_each_application_relationship_accepts_sources_and_models(
    tmp_path: Path, relationship: str, reference: dict[str, str]
) -> None:
    project = _project(
        tmp_path,
        [{"name": "custom_app", "type": "application", relationship: [reference]}],
    )
    result = ProjectValidator(project).validate()
    assert result.is_valid, result.errors
    dag = DAGBuilder(project).build()
    target = next(iter(reference.values()))
    if relationship == "produces":
        assert dag.nodes["custom_app"].downstream == {target}
        assert "custom_app" in dag.nodes[target].upstream
    else:
        assert dag.nodes["custom_app"].upstream == {target}
        assert "custom_app" in dag.nodes[target].downstream


@pytest.mark.parametrize("relationship", ["produces", "consumes", "depends_on"])
@pytest.mark.parametrize("kind", ["source", "ref"])
def test_unknown_application_references_fail_locally(
    tmp_path: Path, relationship: str, kind: str
) -> None:
    project = _project(
        tmp_path,
        [{"name": "custom_app", "type": "application", relationship: [{kind: "missing"}]}],
    )
    errors = ProjectValidator(project).validate().errors
    expected = (
        "EXPOSURE_DEPENDENCY_NOT_FOUND"
        if relationship == "depends_on"
        else "EXPOSURE_SOURCE_NOT_FOUND" if kind == "source" else "EXPOSURE_MODEL_NOT_FOUND"
    )
    assert len(errors) == 1
    assert errors[0].code == expected
    assert errors[0].location == f"exposure 'custom_app'.{relationship}[0]"
    assert "missing" in errors[0].message


@pytest.mark.parametrize("relationship", ["produces", "consumes", "depends_on"])
@pytest.mark.parametrize(
    "reference",
    [
        {},
        {"source": "orders", "ref": "cleaned"},
        {"source": ""},
        {"ref": "   "},
        {"source": "orders", "ref": ""},
    ],
)
def test_ambiguous_or_empty_application_reference_fails_validation_not_parsing(
    tmp_path: Path, relationship: str, reference: dict[str, str]
) -> None:
    project = _project(
        tmp_path,
        [{"name": "custom_app", "type": "application", relationship: [reference]}],
    )
    errors = ProjectValidator(project).validate().errors
    assert len(errors) == 1
    assert errors[0].code == "EXPOSURE_REFERENCE_INVALID"
    assert errors[0].location == f"exposure 'custom_app'.{relationship}[0]"


@pytest.mark.parametrize("name", ["orders", "cleaned"])
def test_application_name_cannot_replace_source_or_model_identity(
    tmp_path: Path, name: str
) -> None:
    project = _project(tmp_path, [{"name": name, "type": "application"}])
    errors = ProjectValidator(project).validate().errors
    assert any(error.code == "NAME_COLLISION" for error in errors)
    with pytest.raises(ValueError, match="shares a name"):
        DAGBuilder(project).build()


def test_duplicate_application_names_cannot_be_collapsed_by_graph_builder(tmp_path: Path) -> None:
    project = _project(
        tmp_path,
        [{"name": "custom_app", "type": "application"}] * 2,
    )
    errors = ProjectValidator(project).validate().errors
    assert any(error.code == "DUPLICATE_EXPOSURE" for error in errors)
    with pytest.raises(ValueError, match="shares a name"):
        DAGBuilder(project).build()


@pytest.mark.parametrize("role", [None, "producer", "consumer", "both"])
def test_role_is_descriptive_and_does_not_drop_processor_edges(
    tmp_path: Path, role: str | None
) -> None:
    project = _project(
        tmp_path,
        [{
            "name": "custom_app",
            "type": "application",
            "role": role,
            "consumes": [{"source": "orders"}],
            "produces": [{"source": "enriched"}],
        }],
    )
    result = ProjectValidator(project).validate()
    assert result.is_valid, result.errors
    assert not [warning for warning in result.warnings if warning.code == "UNUSED_SOURCE"]
    dag = DAGBuilder(project).build()
    assert dag.get_downstream("orders") == {"custom_app", "enriched", "cleaned"}
    assert dag.get_upstream("cleaned") == {"orders", "custom_app", "enriched"}


@pytest.mark.parametrize(
    "references",
    [
        {"consumes": [{"source": "orders"}], "produces": [{"source": "orders"}]},
        {"consumes": [{"ref": "cleaned"}], "produces": [{"source": "enriched"}]},
        {"depends_on": [{"source": "orders"}], "produces": [{"source": "orders"}]},
    ],
)
def test_application_feedback_cycle_is_rejected_before_compilation(
    tmp_path: Path, references: dict[str, object]
) -> None:
    project = _project(
        tmp_path,
        [{"name": "custom_app", "type": "application", **references}],
    )
    errors = ProjectValidator(project).validate().errors
    assert len(errors) == 1
    assert errors[0].code == "CYCLE_DETECTED"
    assert "custom_app" in errors[0].message
    with pytest.raises(DAGCycleError, match="custom_app"):
        Compiler(project).compile(dry_run=True)


def test_two_custom_applications_cannot_hide_a_cycle(tmp_path: Path) -> None:
    project = _project(
        tmp_path,
        [
            {
                "name": "first_app",
                "type": "application",
                "consumes": [{"source": "orders"}],
                "produces": [{"source": "enriched"}],
            },
            {
                "name": "second_app",
                "type": "application",
                "consumes": [{"source": "enriched"}],
                "produces": [{"source": "orders"}],
            },
        ],
    )
    errors = ProjectValidator(project).validate().errors
    assert len(errors) == 1
    assert errors[0].code == "CYCLE_DETECTED"
    assert "first_app" in errors[0].message
    assert "second_app" in errors[0].message


def test_mixed_application_and_managed_model_are_local_and_keep_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_provider(*args: object, **kwargs: object) -> None:
        pytest.fail("A declaration-only operation attempted provider access")

    for constructor in (
        "streamt.deployer.kafka.KafkaDeployer",
        "streamt.deployer.schema_registry.SchemaRegistryDeployer",
        "streamt.deployer.flink.FlinkDeployer",
        "streamt.deployer.connect.ConnectDeployer",
    ):
        monkeypatch.setattr(constructor, unexpected_provider)

    project = _project(
        tmp_path,
        [
            {
                "name": "custom_app",
                "type": "application",
                "role": "both",
                "repo": "https://example.com/team/orders-app",
                "language": "java",
                "tool": "kafka-streams",
                "consumes": [{"source": "orders"}],
                "produces": [{"source": "enriched"}],
            },
            {
                "name": "consumer_app",
                "type": "application",
                "consumes": [{"ref": "cleaned"}],
            },
        ],
    )
    assert ProjectValidator(project).validate().is_valid
    manifest = Compiler(project).compile(dry_run=True)
    assert {item["name"] for item in manifest.artifacts["topics"]} == {
        "cleaned", "application_output"
    }
    assert {item["name"] for item in manifest.artifacts["flink_jobs"]} == {
        "cleaned_processor"
    }
    assert not manifest.artifacts["schemas"]
    assert all(source.ownership.mode.value == "external" for source in project.sources)
    assert all(model.ownership.mode.value == "managed" for model in project.models)
    dag = DAGBuilder(project).build()
    assert dag.nodes["custom_app"].type == NodeType.EXPOSURE
    assert "consumer_app" in dag.get_downstream("orders")

    runner = CliRunner()
    for command in (
        ["validate", "--strict"],
        ["compile", "--dry-run"],
        ["plan", "--offline"],
        ["lineage"],
    ):
        result = runner.invoke(main, [*command, "--project-dir", str(tmp_path)])
        assert result.exit_code == 0, result.output


def test_duplicate_references_make_one_deterministic_graph_edge(tmp_path: Path) -> None:
    project = _project(
        tmp_path,
        [{
            "name": "custom_app",
            "type": "application",
            "consumes": [{"source": "orders"}, {"source": "orders"}],
            "depends_on": [{"source": "orders"}],
        }],
    )
    assert ProjectValidator(project).validate().is_valid
    dag = DAGBuilder(project).build()
    assert dag.nodes["custom_app"].upstream == {"orders"}
    assert dag.nodes["orders"].downstream == {"custom_app"}


def test_adding_an_application_does_not_duplicate_existing_model_cycle_error(tmp_path: Path) -> None:
    project = _project(tmp_path, [])
    project.models[0].sql = "SELECT id FROM {{ ref('cleaned') }}"
    project.exposures = [Exposure(name="custom_app", type="application")]
    cycle_errors = [
        error for error in ProjectValidator(project).validate().errors
        if error.code == "CYCLE_DETECTED"
    ]
    assert len(cycle_errors) == 1
