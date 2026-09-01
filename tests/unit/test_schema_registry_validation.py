"""Focused tests for opt-in, read-only Schema Registry validation."""

from unittest.mock import Mock, patch

import pytest
import yaml
from click.testing import CliRunner
from pydantic import ValidationError

from streamt.cli import main
from streamt.core.models import StreamtProject
from streamt.core.validator import ProjectValidator
from streamt.deployer.schema_registry import (
    SchemaReference,
    SchemaRegistryDeployer,
    SchemaRegistryResolutionError,
    SchemaState,
)


def _project(schema: dict[str, object]) -> StreamtProject:
    return StreamtProject.model_validate(
        {
            "project": {"name": "registry-validation"},
            "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
            "sources": [
                {
                    "name": "orders",
                    "topic": "orders.v1",
                    "schema": schema,
                }
            ],
        }
    )


def _registry() -> Mock:
    return Mock(spec=SchemaRegistryDeployer)


def test_schema_reference_version_and_format_are_strict() -> None:
    project = _project({"registry": "confluent", "subject": "orders-value", "format": "AVRO"})
    assert project.sources[0].schema_.format == "avro"

    for invalid_version in (0, -1, True):
        with pytest.raises(ValidationError, match="version must be a positive integer"):
            _project(
                {
                    "registry": "confluent",
                    "subject": "orders-value",
                    "version": invalid_version,
                }
            )


def test_offline_validation_remains_available() -> None:
    project = _project({"registry": "confluent", "subject": "orders-value", "version": 2})

    result = ProjectValidator(project).validate()

    assert not [error for error in result.errors if error.code.startswith("SCHEMA_")]


def test_missing_external_subject_is_an_actionable_error() -> None:
    registry = _registry()
    registry.resolve_schema_state.return_value = SchemaState(
        subject="orders-value",
        exists=False,
    )
    project = _project({"registry": "confluent", "subject": "orders-value", "version": 2})

    result = ProjectValidator(project, schema_registry_deployer=registry).validate()

    error = next(error for error in result.errors if error.code == "SCHEMA_SUBJECT_NOT_FOUND")
    assert "orders-value" in error.message
    assert "version '2'" in error.message
    assert error.location == "source 'orders'.schema"
    registry.resolve_schema_state.assert_called_once_with("orders-value", 2)


def test_resolved_subject_reports_version_type_and_reference_count() -> None:
    registry = _registry()
    registry.resolve_schema_state.return_value = SchemaState(
        subject="orders-value",
        exists=True,
        version=5,
        schema_id=101,
        schema={"type": "record", "name": "Order", "fields": []},
        schema_type="AVRO",
        compatibility="FULL_TRANSITIVE",
        references=[SchemaReference(name="money.avsc", subject="money-value", version=1)],
        resolved_references=[
            SchemaState(subject="money-value", exists=True, version=1, schema_id=7)
        ],
    )
    project = _project({"registry": "confluent", "subject": "orders-value", "format": "AVRO"})

    result = ProjectValidator(project, schema_registry_deployer=registry).validate()

    assert result.is_valid
    info = next(info for info in result.infos if info.code == "SCHEMA_SUBJECT_RESOLVED")
    assert "version 5" in info.message
    assert "type AVRO" in info.message
    assert "1 direct reference(s)" in info.message
    assert "compatibility FULL_TRANSITIVE" in info.message


def test_declared_format_must_match_resolved_format() -> None:
    registry = _registry()
    registry.resolve_schema_state.return_value = SchemaState(
        subject="orders-value",
        exists=True,
        version=1,
        schema_id=1,
        schema='syntax = "proto3";',
        schema_type="PROTOBUF",
    )
    project = _project({"registry": "confluent", "subject": "orders-value", "format": "json"})

    result = ProjectValidator(project, schema_registry_deployer=registry).validate()

    error = next(error for error in result.errors if error.code == "SCHEMA_FORMAT_MISMATCH")
    assert "declares schema format 'json'" in error.message
    assert "'protobuf'" in error.message


def test_broken_reference_graph_is_reported_at_source_path() -> None:
    registry = _registry()
    registry.resolve_schema_state.side_effect = SchemaRegistryResolutionError(
        "reference 'money.proto' points to missing subject 'money-value' version 3"
    )
    project = _project({"registry": "confluent", "subject": "orders-value"})

    result = ProjectValidator(project, schema_registry_deployer=registry).validate()

    error = next(
        error for error in result.errors if error.code == "SCHEMA_REGISTRY_RESOLUTION_FAILED"
    )
    assert "money-value" in error.message
    assert error.location == "source 'orders'.schema"


def test_inline_schema_is_not_required_to_exist_in_registry() -> None:
    registry = _registry()
    project = _project(
        {
            "registry": "confluent",
            "subject": "new-orders-value",
            "format": "avro",
            "definition": '{"type":"record","name":"Order","fields":[]}',
        }
    )

    result = ProjectValidator(project, schema_registry_deployer=registry).validate()

    assert result.is_valid
    registry.resolve_schema_state.assert_not_called()


def test_validate_cli_check_schemas_runs_live_read_only_check(tmp_path) -> None:
    project_data = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "registry-validation"},
        "runtime": {
            "kafka": {"bootstrap_servers": "localhost:9092"},
            "schema_registry": {"url": "http://registry:8081"},
        },
        "sources": [
            {
                "name": "orders",
                "topic": "orders.v1",
                "schema": {"registry": "confluent", "subject": "orders-value"},
            }
        ],
    }
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(project_data))
    registry = _registry()
    registry.resolve_schema_state.return_value = SchemaState(
        subject="orders-value",
        exists=True,
        version=7,
        schema_id=101,
        schema={"type": "record", "name": "Order", "fields": []},
        schema_type="AVRO",
    )

    with patch("streamt.cli.commands.validate.make_sr_deployer", return_value=registry):
        result = CliRunner().invoke(
            main,
            ["validate", "--project-dir", str(tmp_path), "--check-schemas"],
        )

    assert result.exit_code == 0, result.output
    assert "Resolved Schema Registry subject 'orders-value' version 7" in result.output
    registry.resolve_schema_state.assert_called_once_with("orders-value", "latest")
    registry.close.assert_called_once()


def test_validate_cli_check_schemas_requires_registry_config(tmp_path) -> None:
    project_data = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "offline-only"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    }
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(project_data))

    result = CliRunner().invoke(
        main,
        ["validate", "--project-dir", str(tmp_path), "--check-schemas"],
    )

    assert result.exit_code == 1
    assert "--check-schemas requires runtime.schema_registry" in result.output
