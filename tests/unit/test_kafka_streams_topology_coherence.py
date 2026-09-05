"""Topology validation uses known executor output, not only handwritten columns."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler import Compiler
from streamt.compiler.model_resolution import CompileError
from streamt.core.models import StreamtProject
from streamt.core.validator import ProjectValidator
from tests.unit.test_kafka_streams_compiler import SOURCE_COLUMNS, _config


@pytest.mark.parametrize("column", [
    {"name": "removed", "type": "STRING"},
    {"name": "amount", "type": "STRING"},
    {"name": "amount", "type": "INT"},
])
def test_consumer_matches_inferred_projection_without_handwritten_model_columns(column: dict) -> None:
    config = _config()
    config["exposures"][0]["columns"] = [column]
    project = StreamtProject.model_validate(config)
    assert project.models[0].columns is None
    assert not ProjectValidator(project).validate().is_valid
    with pytest.raises(CompileError, match="Application"):
        Compiler(project).compile(dry_run=True)


def test_matching_consumer_alias_uses_known_inferred_output_without_schema_mutation() -> None:
    config = _config()
    config["exposures"][0]["columns"] = [
        {"name": "amount", "type": "LONG"}, {"name": "order_id", "type": "VARCHAR(80)"},
    ]
    project = StreamtProject.model_validate(config)
    before = project.model_dump()
    assert ProjectValidator(project).validate().is_valid
    Compiler(project).compile(dry_run=True)
    assert project.model_dump() == before


def test_separate_source_aliases_cannot_hide_physical_feedback() -> None:
    config = _config()
    config["sources"].append({"name": "feedback", "topic": "valuable_orders", "columns": copy.deepcopy(SOURCE_COLUMNS)})
    config["models"].append({
        "name": "back_to_input", "executor": "kafka_streams",
        "topic": {"name": "orders.raw.v1"},
        "sql": 'SELECT id, amount, active FROM {{ source("feedback") }}',
    })
    project = StreamtProject.model_validate(config)
    compiler = Compiler(project)
    with pytest.raises(CompileError, match="physical topic feedback"):
        compiler.compile(dry_run=True)
    assert compiler.kafka_streams_jobs == []
    assert compiler.compiled_models == {}


@pytest.mark.parametrize("command", [
    ["validate", "--strict"], ["compile", "--dry-run"], ["plan", "--offline"],
    ["plan"], ["apply", "--dry-run"], ["apply", "--confirm"],
])
@pytest.mark.parametrize("failure", ["consumer", "sql"])
def test_every_development_gate_rejects_incompatible_executor_work_before_providers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command: list[str], failure: str,
) -> None:
    config = _config()
    if failure == "consumer":
        config["exposures"][0]["columns"] = [{"name": "amount", "type": "STRING"}]
    else:
        config["models"][0]["sql"] = 'SELECT SUM(amount) FROM {{ source("orders") }}'
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Invalid topology contacted a provider")

    for target in (
        "socket.getaddrinfo", "socket.create_connection", "subprocess.run", "subprocess.Popen",
        "streamt.deployer.kafka.KafkaDeployer", "streamt.deployer.kafka_streams.KafkaStreamsDeployer",
    ):
        monkeypatch.setattr(target, forbidden)
    result = CliRunner().invoke(main, ["-o", "json", *command, "-p", str(tmp_path)])
    assert result.exit_code != 0
    assert result.stdout, repr(result.exception)
    assert json.loads(result.stdout)["status"] == "error"
    assert "Invalid topology contacted" not in result.output
    assert not (tmp_path / "generated").exists()
    assert not (tmp_path / ".streamt").exists()
