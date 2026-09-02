"""CLI contract tests for project-wide ODCS 3.1 export."""

from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

import streamt.cli.commands.docs as docs_command
from streamt.cli import main
from streamt.docs.odcs import (
    ODCS_API_VERSION,
    ODCS_INCOMPLETE_SCHEMA_WARNING,
    validate_odcs_document,
)

CONTRACT_ID = "urn:acme:data-contract:payments"


def _project(*, version: str | None = "2.3.0") -> dict[str, object]:
    project_metadata: dict[str, object] = {
        "name": "payments-streams",
        "description": "Purpose is deliberately not mapped.",
    }
    if version is not None:
        project_metadata["version"] = version
    return {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": project_metadata,
        "runtime": {
            "kafka": {
                "bootstrap_servers": "PRIVATE_BROKER_ENDPOINT:9092",
            }
        },
        "sources": [
            {
                "name": "payments_raw",
                "topic": "payments.raw.v1",
                "columns": [
                    {
                        "name": "payment_id",
                        "type": "VARCHAR(64)",
                        "required": True,
                    }
                ],
            }
        ],
        "models": [
            {
                "name": "payments_clean",
                "materialized": "topic",
                "sql": "SELECT SQL_MUST_NOT_APPEAR FROM payments_raw",
                "topic": {"name": "payments.clean.v1"},
                "contract": {
                    "enforced": True,
                    "columns": [
                        {
                            "name": "payment_id",
                            "type": "STRING",
                            "nullable": False,
                        }
                    ],
                },
            },
            {
                "name": "archive_sink",
                "from": "payments_clean",
                "sink": {
                    "connector": "jdbc",
                    "config": {"password": "SINK_SECRET_MUST_NOT_APPEAR"},
                },
                "columns": [{"name": "payment_id", "type": "STRING"}],
            },
        ],
    }


def _write_project(path: Path, project: dict[str, object]) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(project, sort_keys=False),
        encoding="utf-8",
    )


def _command(path: Path, *extra: str) -> list[str]:
    return [
        "docs",
        "odcs",
        "--project-dir",
        str(path),
        "--contract-id",
        CONTRACT_ID,
        "--status",
        "active",
        *extra,
    ]


def test_help_exposes_exact_surface_without_renaming_existing_commands() -> None:
    runner = CliRunner()
    command_help = runner.invoke(main, ["docs", "odcs", "--help"])
    group_help = runner.invoke(main, ["docs", "--help"])

    assert command_help.exit_code == group_help.exit_code == 0
    for option in (
        "--contract-id",
        "--status",
        "--contract-version",
        "--format",
        "--output-file",
        "--project-dir",
        "--env",
    ):
        assert option in command_help.output
    assert "yaml" in command_help.output
    assert "json" in command_help.output
    assert "asyncapi" in group_help.output
    assert "openapi" in group_help.output
    assert "odcs" in group_help.output


@pytest.mark.parametrize("serialization", ["yaml", "json"])
def test_raw_stdout_is_one_parseable_deterministic_document(
    tmp_path: Path,
    serialization: str,
) -> None:
    _write_project(tmp_path, _project())
    runner = CliRunner()

    first = runner.invoke(main, _command(tmp_path, "--format", serialization))
    second = runner.invoke(main, _command(tmp_path, "--format", serialization))

    assert first.exit_code == second.exit_code == 0, first.output
    assert first.stdout == second.stdout
    assert first.stdout.endswith("\n")
    assert first.stderr == ""
    document = (
        json.loads(first.stdout)
        if serialization == "json"
        else yaml.safe_load(first.stdout)
    )
    assert list(document) == [
        "apiVersion",
        "kind",
        "id",
        "name",
        "version",
        "status",
        "schema",
    ]
    assert document["apiVersion"] == ODCS_API_VERSION
    assert document["id"] == CONTRACT_ID
    assert "PRIVATE_BROKER_ENDPOINT" not in first.stdout
    assert "SQL_MUST_NOT_APPEAR" not in first.stdout
    assert "SINK_SECRET_MUST_NOT_APPEAR" not in first.stdout
    validate_odcs_document(document)


def test_raw_yaml_and_json_contain_the_same_document(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    runner = CliRunner()

    yaml_result = runner.invoke(main, _command(tmp_path))
    json_result = runner.invoke(main, _command(tmp_path, "--format", "json"))

    assert yaml_result.exit_code == json_result.exit_code == 0
    assert yaml.safe_load(yaml_result.stdout) == json.loads(json_result.stdout)


@pytest.mark.parametrize(
    ("args", "location"),
    [
        (["--status", "active"], "contract_id"),
        (["--contract-id", "   ", "--status", "active"], "contract_id"),
        (["--contract-id", CONTRACT_ID], "status"),
        (["--contract-id", CONTRACT_ID, "--status", "\t"], "status"),
    ],
)
def test_missing_or_blank_identity_and_status_use_e505_json_envelope(
    tmp_path: Path,
    args: list[str],
    location: str,
) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        ["--output", "json", "docs", "odcs", "-p", str(tmp_path), *args],
    )

    assert result.exit_code == 1
    envelope = json.loads(result.stdout)
    assert envelope["status"] == "error"
    assert envelope["command"] == "docs odcs"
    assert envelope["errors"] == [
        {
            "code": "E505_ODCS_INVALID",
            "message": envelope["errors"][0]["message"],
            "location": location,
        }
    ]
    assert "non-whitespace" in envelope["errors"][0]["message"]


def test_missing_required_option_uses_e505_in_text_mode(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        ["docs", "odcs", "-p", str(tmp_path), "--status", "active"],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "E505" not in result.stderr
    assert "ODCS contract ID" in result.stderr
    assert "Usage:" not in result.stderr


@pytest.mark.parametrize(
    ("version", "extra", "location", "message"),
    [
        (None, [], "project.version", "contract version is required"),
        ("   ", [], "project.version", "non-whitespace"),
        ("2.3.0", ["--contract-version", "  "], "contract_version", "non-whitespace"),
    ],
)
def test_missing_or_blank_version_uses_e505(
    tmp_path: Path,
    version: str | None,
    extra: list[str],
    location: str,
    message: str,
) -> None:
    _write_project(tmp_path, _project(version=version))
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, *extra)],
    )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E505_ODCS_INVALID"
    assert error["location"] == location
    assert message in error["message"]


def test_explicit_contract_version_overrides_missing_project_version(tmp_path: Path) -> None:
    _write_project(tmp_path, _project(version=None))
    result = CliRunner().invoke(
        main,
        _command(tmp_path, "--contract-version", "release-candidate-7"),
    )

    assert result.exit_code == 0, result.output
    assert yaml.safe_load(result.stdout)["version"] == "release-candidate-7"


def test_identity_status_and_version_are_copied_without_rewriting(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        [
            "docs",
            "odcs",
            "-p",
            str(tmp_path),
            "--contract-id",
            "  exact contract ID  ",
            "--status",
            " custom-status ",
            "--contract-version",
            " release-7 ",
        ],
    )

    assert result.exit_code == 0, result.output
    document = yaml.safe_load(result.stdout)
    assert document["id"] == "  exact contract ID  "
    assert document["status"] == " custom-status "
    assert document["version"] == " release-7 "


def test_global_json_is_one_normal_envelope_and_local_format_is_metadata(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    runner = CliRunner()
    raw = runner.invoke(main, _command(tmp_path))
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--format", "json")],
    )

    assert raw.exit_code == result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope == {
        "status": "ok",
        "command": "docs odcs",
        "data": {
            "standard": "odcs",
            "standard_version": "3.1.0",
            "document": envelope["data"]["document"],
            "serialization": "json",
            "output_file": None,
        },
        "errors": [],
        "warnings": [],
    }
    assert envelope["data"]["document"]["apiVersion"] == ODCS_API_VERSION
    assert envelope["data"]["document"] == yaml.safe_load(raw.stdout)
    validate_odcs_document(envelope["data"]["document"])


def test_parser_and_incomplete_schema_warnings_never_corrupt_stdout(
    tmp_path: Path,
) -> None:
    project = _project()
    project.pop("apiVersion")
    project["sources"] = [{"name": "raw", "topic": "raw.v1"}]
    project["models"] = []
    _write_project(tmp_path, project)
    runner = CliRunner()

    raw = runner.invoke(main, _command(tmp_path))
    structured = runner.invoke(
        main,
        ["--output", "json", *_command(tmp_path)],
    )

    assert raw.exit_code == structured.exit_code == 0
    assert yaml.safe_load(raw.stdout)["schema"][0]["name"] == "raw"
    assert "WARNING" not in raw.stdout
    assert "apiVersion" in raw.stderr
    assert "no declared export columns" in raw.stderr
    envelope = json.loads(structured.stdout)
    assert [warning["code"] for warning in envelope["warnings"]] == [
        "W000_WARNING",
        ODCS_INCOMPLETE_SCHEMA_WARNING,
    ]
    assert envelope["warnings"][1]["location"] == "source.raw"
    assert "apiVersion" in structured.stderr
    assert "no declared export columns" in structured.stderr


@pytest.mark.parametrize("serialization", ["yaml", "json"])
def test_output_file_bytes_equal_raw_stdout_and_confirmation_is_separate(
    tmp_path: Path,
    serialization: str,
) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    _write_project(project_dir, _project())
    target = tmp_path / "nested" / f"contract.{serialization}"
    runner = CliRunner()

    raw = runner.invoke(main, _command(project_dir, "--format", serialization))
    written = runner.invoke(
        main,
        _command(
            project_dir,
            "--format",
            serialization,
            "--output-file",
            str(target),
        ),
    )

    assert raw.exit_code == written.exit_code == 0
    assert target.read_text(encoding="utf-8") == raw.stdout
    assert "ODCS document written to" in written.stdout
    assert target.name in written.stdout
    assert written.stderr == ""
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_global_json_with_output_file_retains_document_and_selected_path(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "contract.json"
    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            *_command(
                tmp_path,
                "--format",
                "json",
                "--output-file",
                str(target),
            ),
        ],
    )

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["data"]["output_file"] == str(target)
    assert envelope["data"]["serialization"] == "json"
    assert json.loads(target.read_text(encoding="utf-8")) == envelope["data"]["document"]


def test_quiet_output_file_write_prints_no_confirmation(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "contract.yaml"
    result = CliRunner().invoke(
        main,
        ["--quiet", *_command(tmp_path, "--output-file", str(target))],
    )

    assert result.exit_code == 0
    assert result.stdout == result.stderr == ""
    validate_odcs_document(yaml.safe_load(target.read_text(encoding="utf-8")))


def test_forced_atomic_replace_failure_preserves_target_and_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "UNSTRUCTURED_FILENAME_SECRET.yaml"
    target.write_text("original bytes\n", encoding="utf-8")

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("UNSTRUCTURED_OS_SECRET replacement failed")

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
    envelope = json.loads(result.stdout)
    assert envelope["errors"][0]["code"] == "E505_ODCS_INVALID"
    assert envelope["errors"][0]["location"] == "output_file"
    assert envelope["errors"][0]["message"] == (
        "Could not write ODCS output file atomically"
    )
    assert "UNSTRUCTURED_FILENAME_SECRET" not in result.stdout
    assert "UNSTRUCTURED_FILENAME_SECRET" not in result.stderr
    assert "UNSTRUCTURED_OS_SECRET" not in result.stdout
    assert "UNSTRUCTURED_OS_SECRET" not in result.stderr
    assert target.read_text(encoding="utf-8") == "original bytes\n"
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_forced_staging_fsync_failure_leaves_no_target_or_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "contract.yaml"
    monkeypatch.setattr(
        docs_command.os,
        "fsync",
        lambda _fd: (_ for _ in ()).throw(OSError("staging failed")),
    )

    result = CliRunner().invoke(main, _command(tmp_path, "--output-file", str(target)))

    assert result.exit_code == 1
    assert "Could not write ODCS output file atomically" in result.stderr
    assert not target.exists()
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_serialization_failure_is_redacted_and_uses_e505(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())

    def fail_serialization(_document: object, _serialization: object) -> str:
        raise ValueError("UNSTRUCTURED_SERIALIZATION_SECRET")

    monkeypatch.setattr(docs_command, "_serialize_odcs_document", fail_serialization)
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path)],
    )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E505_ODCS_INVALID"
    assert error["location"] == "document"
    assert error["message"] == "Could not serialize ODCS document"
    assert "UNSTRUCTURED_SERIALIZATION_SECRET" not in result.stdout
    assert "UNSTRUCTURED_SERIALIZATION_SECRET" not in result.stderr


def test_multi_environment_export_is_offline_and_omits_runtime_endpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = _project()
    project.pop("runtime")
    _write_project(tmp_path, project)
    environments = tmp_path / "environments"
    environments.mkdir()
    (environments / "dev.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "dev"},
                "runtime": {
                    "kafka": {
                        "bootstrap_servers": "ENV_PRIVATE_ENDPOINT:9092",
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("ODCS CLI attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", fail_network)
    monkeypatch.setattr(socket, "create_connection", fail_network)
    result = CliRunner().invoke(main, _command(tmp_path, "--env", "dev"))

    assert result.exit_code == 0, result.output
    assert "ENV_PRIVATE_ENDPOINT" not in result.stdout
    assert "ENV_PRIVATE_ENDPOINT" not in result.stderr
    validate_odcs_document(yaml.safe_load(result.stdout))
