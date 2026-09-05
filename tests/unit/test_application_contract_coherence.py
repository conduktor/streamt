"""Application consumers must agree with explicit local topology contracts."""

from __future__ import annotations

import importlib
import json
import socket
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.core.dag import DAGBuilder
from streamt.core.models import ColumnDefinition, StreamtProject
from streamt.core.parser import ProjectParser
from streamt.core.validator import ProjectValidator


def _project(
    path: Path,
    *,
    kind: str = "contract",
    declared: list[dict[str, object]] | None = None,
    consumed: list[dict[str, object]] | None = None,
    ownership: str = "managed",
    enforced: bool = True,
) -> StreamtProject:
    resource: dict[str, object] = {
        "name": "orders",
        "ownership": {"mode": ownership},
    }
    if kind == "source":
        resource["topic"] = "orders.v1"
        if declared is not None:
            resource["columns"] = declared
    else:
        resource["materialized"] = "topic"
        if declared is not None:
            if kind == "contract":
                resource["contract"] = {"enforced": enforced, "columns": declared}
            else:
                resource["columns"] = declared
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "application-contracts"},
        "runtime": {"kafka": {"bootstrap_servers": "unreachable.invalid:9092"}},
        "sources" if kind == "source" else "models": [resource],
        "exposures": [{
            "name": "fraud_service",
            "type": "application",
            "role": "consumer",
            "repo": "https://example.com/team/fraud-service",
            "language": "java",
            "tool": "kafka-streams",
            "consumes": [{"source" if kind == "source" else "ref": "orders"}],
            "columns": consumed if consumed is not None else [{"name": "amount", "type": "BIGINT"}],
        }],
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config), encoding="utf-8")
    return ProjectParser(path).parse()


@pytest.mark.parametrize("kind", ["contract", "columns", "source"])
@pytest.mark.parametrize("ownership", ["managed", "adopted", "external"])
def test_missing_declared_consumed_column_is_an_error(
    tmp_path: Path, kind: str, ownership: str,
) -> None:
    project = _project(tmp_path, kind=kind, ownership=ownership,
                       declared=[{"name": "id", "type": "STRING"}])
    result = ProjectValidator(project).validate()
    errors = [error for error in result.errors if error.code == "CONTRACT_BREAKING_CHANGE"]
    assert len(errors) == 1
    assert not result.is_valid
    assert errors[0].location == "exposure 'fraud_service'"
    assert all(name in errors[0].message for name in ("fraud_service", "orders", "amount"))
    assert "declared" in errors[0].message


@pytest.mark.parametrize("enforced", [True, False])
def test_advisory_model_contract_does_not_make_consumer_requirement_advisory(
    tmp_path: Path, enforced: bool,
) -> None:
    project = _project(tmp_path, declared=[], enforced=enforced)
    assert any(error.code == "CONTRACT_BREAKING_CHANGE"
               for error in ProjectValidator(project).validate().errors)


@pytest.mark.parametrize("kind", ["contract", "columns", "source"])
@pytest.mark.parametrize(("producer_type", "consumer_type"), [
    ("STRING", "BIGINT"), ("BOOLEAN", "VARCHAR(30)"), ("BIGINT", "BOOLEAN"),
    ("DATE", "TIMESTAMP(3)"),
])
def test_incompatible_known_declared_type_families_are_errors(
    tmp_path: Path, kind: str, producer_type: str, consumer_type: str,
) -> None:
    project = _project(tmp_path, kind=kind,
                       declared=[{"name": "amount", "type": producer_type}],
                       consumed=[{"name": "amount", "type": consumer_type}])
    errors = [error for error in ProjectValidator(project).validate().errors
              if error.code == "CONTRACT_CONSUMER_TYPE_MISMATCH"]
    assert len(errors) == 1
    assert errors[0].location == "exposure 'fraud_service'"
    assert all(value in errors[0].message for value in
               ("fraud_service", "orders", "amount", producer_type, consumer_type))


@pytest.mark.parametrize(("producer_type", "consumer_type"), [
    ("STRING", "varchar(30)"), ("TEXT", "STRING"), ("LONG", "BIGINT"),
    ("BOOL", "BOOLEAN"), ("INTEGER", "INT"),
])
def test_existing_contract_type_aliases_remain_compatible(
    tmp_path: Path, producer_type: str, consumer_type: str,
) -> None:
    project = _project(tmp_path,
                       declared=[{"name": "amount", "type": producer_type}],
                       consumed=[{"name": "amount", "type": consumer_type}])
    assert ProjectValidator(project).validate().is_valid


@pytest.mark.parametrize(("producer_type", "consumer_type"), [
    (None, "BIGINT"), ("BIGINT", None), ("", "BIGINT"),
    ("UNKNOWN", "STRING"), ("STRING", "UNKNOWN"),
    ("ARRAY<STRING>", "ARRAY<BIGINT>"), ("CUSTOM_TYPE", "STRING"),
    ("VARCHAR(unknown)", "BIGINT"), ("STRING[]", "BIGINT"),
    ("TIMESTAMP(3) WITH LOCAL TIME ZONE", "BOOLEAN"),
])
def test_absent_or_unrecognized_types_are_not_guessed(
    tmp_path: Path, producer_type: str | None, consumer_type: str | None,
) -> None:
    project = _project(tmp_path,
                       declared=[{"name": "amount", "type": producer_type}],
                       consumed=[{"name": "amount", "type": consumer_type}])
    assert ProjectValidator(project).validate().is_valid


@pytest.mark.parametrize("kind", ["contract", "columns", "source"])
def test_absent_schema_is_unknown_not_a_proven_missing_column(tmp_path: Path, kind: str) -> None:
    project = _project(tmp_path, kind=kind)
    assert ProjectValidator(project).validate().is_valid


def test_empty_source_columns_are_unknown_not_a_live_empty_schema(tmp_path: Path) -> None:
    project = _project(tmp_path, kind="source", declared=[])
    assert ProjectValidator(project).validate().is_valid


def test_model_contract_is_authoritative_over_supplementary_columns(tmp_path: Path) -> None:
    project = _project(tmp_path, declared=[{"name": "id", "type": "STRING"}])
    model = project.models[0]
    model.columns = [ColumnDefinition(name="amount", type="BIGINT")]
    assert any(error.code == "CONTRACT_BREAKING_CHANGE"
               for error in ProjectValidator(project).validate().errors)


def test_application_metadata_and_graph_are_unchanged_by_local_contract_check(tmp_path: Path) -> None:
    project = _project(tmp_path, declared=[{"name": "amount", "type": "BIGINT"}])
    application_before = project.exposures[0].model_dump()
    graph_before = DAGBuilder(project).build().to_dict()
    assert ProjectValidator(project).validate().is_valid
    assert project.exposures[0].model_dump() == application_before
    assert DAGBuilder(project).build().to_dict() == graph_before
    assert project.exposures[0].repo == "https://example.com/team/fraud-service"
    assert project.exposures[0].tool == "kafka-streams"


def test_flat_columns_remain_requirements_on_every_consumed_input(tmp_path: Path) -> None:
    """The current DSL has no per-input column mapping; do not invent one."""
    project = _project(tmp_path, declared=[{"name": "amount", "type": "BIGINT"}])
    other = project.models[0].model_copy(deep=True)
    other.name = "other_orders"
    assert other.contract is not None
    other.contract.columns[0].name = "other_amount"
    project.models.append(other)
    reference = project.exposures[0].consumes[0].model_copy(update={"ref": "other_orders"})
    project.exposures[0].consumes.append(reference)
    errors = ProjectValidator(project).validate().errors
    assert len(errors) == 1
    assert errors[0].code == "CONTRACT_BREAKING_CHANGE"
    assert "other_orders" in errors[0].message


@pytest.mark.parametrize("relationship", ["produces", "depends_on"])
def test_consumption_requirements_are_not_inferred_for_other_relationships(
    tmp_path: Path, relationship: str,
) -> None:
    project = _project(tmp_path, declared=[{"name": "id", "type": "STRING"}])
    exposure = project.exposures[0]
    setattr(exposure, relationship, exposure.consumes)
    exposure.consumes = []
    assert ProjectValidator(project).validate().is_valid


@pytest.mark.parametrize("command", [
    ["validate", "--strict"], ["plan", "--offline"], ["plan"],
    ["apply", "--dry-run"], ["apply"],
])
@pytest.mark.parametrize("failure", ["missing", "type"])
@pytest.mark.parametrize("kind", ["contract", "columns", "source"])
def test_invalid_application_contract_stops_cli_before_provider_or_file_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: list[str], failure: str, kind: str,
) -> None:
    project = _project(tmp_path, kind=kind, declared=[{
        "name": "id" if failure == "missing" else "amount", "type": "STRING",
    }])
    assert project.project_path == tmp_path
    project_path = tmp_path / "stream_project.yml"
    before = project_path.read_bytes()
    accesses: list[str] = []

    def forbidden(*_args: object, **_kwargs: object) -> None:
        accesses.append("provider access")
        raise AssertionError("Invalid topology attempted provider or subprocess access")

    for command_name in ("plan", "apply"):
        module = importlib.import_module(f"streamt.cli.commands.{command_name}")
        for name in dir(module):
            if name.startswith("make_") and name.endswith("_deployer"):
                monkeypatch.setattr(module, name, forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    result = CliRunner().invoke(main, ["-o", "json", *command, "-p", str(tmp_path)])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert any("fraud_service" in error["message"] and "amount" in error["message"]
               for error in payload["errors"])
    assert accesses == []
    assert project_path.read_bytes() == before
    assert sorted(path.name for path in tmp_path.iterdir()) == ["stream_project.yml"]
