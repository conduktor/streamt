"""CLI acceptance tests for deterministic offline DataHub MCP export."""

from __future__ import annotations

import builtins
import json
import os
import socket
import stat
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

import streamt.cli.commands.docs as docs_commands
import streamt.integrations.catalog.datahub_export as datahub_export
import streamt.integrations.catalog.model as catalog_model
from streamt.cli import main
from streamt.compiler import Compiler
from streamt.integrations.catalog.datahub_export import DataHubExportError
from streamt.integrations.catalog.model import CatalogProjectionError

CATALOG_ID = "commerce"
FABRIC = "PROD"


def _project(
    *,
    owner_and_tags: bool = False,
    sink: bool = False,
    exposures: int = 0,
    gateway: bool = False,
) -> dict[str, object]:
    source: dict[str, object] = {"name": "orders", "topic": "Orders.Raw"}
    if owner_and_tags:
        source.update(
            {
                "owner": "OWNER_SECRET_MUST_NOT_APPEAR",
                "tags": ["TAG_SECRET_MUST_NOT_APPEAR"],
            }
        )
    models: list[dict[str, object]] = []
    if sink:
        models.append(
            {
                "name": "warehouse_sink",
                "from": [{"source": "orders"}],
                "sink": {
                    "connector": "jdbc-sink",
                    "config": {"password": "CONNECTOR_SECRET_MUST_NOT_APPEAR"},
                },
            }
        )
    if gateway:
        models.append(
            {
                "name": "public_orders",
                "sql": 'SELECT * FROM {{ source("orders") }} WHERE id > 0',
                "gateway": {"virtual_topic": {"name": "orders(public)"}},
            }
        )
    return {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "catalog-project", "description": "Catalog"},
        "runtime": {
            "kafka": {
                "bootstrap_servers": "PRIVATE_BROKER_MUST_NOT_APPEAR:9092",
                "sasl_password": "RUNTIME_SECRET_MUST_NOT_APPEAR",
            },
            "conduktor": {
                "gateway": {
                    "proxy_bootstrap": "PRIVATE_GATEWAY_MUST_NOT_APPEAR:6969",
                }
            },
        },
        "sources": [source],
        "models": models,
        "exposures": [
            {
                "name": f"consumer_{index}",
                "type": "dashboard",
                "description": "EXPOSURE_SECRET_MUST_NOT_APPEAR",
                "consumes": [{"source": "orders"}],
            }
            for index in range(exposures)
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
        "datahub",
        "--project-dir",
        str(path),
        "--catalog-id",
        CATALOG_ID,
        "--fabric",
        FABRIC,
        "--kafka-platform-instance",
        "main",
        *extra,
    ]


def _assert_object_keys_sorted(value: object) -> None:
    if isinstance(value, dict):
        assert list(value) == sorted(value)
        for item in value.values():
            _assert_object_keys_sorted(item)
    elif isinstance(value, list):
        for item in value:
            _assert_object_keys_sorted(item)


class _FailingBinaryTemporaryFile:
    def __init__(self, wrapped: Any, stage: str) -> None:
        self._wrapped = wrapped
        self._stage = stage
        self.name = wrapped.name

    def __enter__(self) -> _FailingBinaryTemporaryFile:
        self._wrapped.__enter__()
        return self

    def __exit__(self, *args: object) -> object:
        return self._wrapped.__exit__(*args)

    def write(self, content: bytes) -> int:
        if self._stage == "write":
            self._wrapped.write(content[:1])
            raise OSError("ATOMIC_WRITE_SECRET_MUST_NOT_APPEAR")
        return cast(int, self._wrapped.write(content))

    def flush(self) -> None:
        if self._stage == "flush":
            raise OSError("ATOMIC_FLUSH_SECRET_MUST_NOT_APPEAR")
        self._wrapped.flush()

    def fileno(self) -> int:
        return cast(int, self._wrapped.fileno())


def test_help_exposes_only_the_frozen_surface() -> None:
    result = CliRunner().invoke(main, ["docs", "datahub", "--help"])
    group = CliRunner().invoke(main, ["docs", "--help"])
    assert result.exit_code == group.exit_code == 0
    for option in (
        "--catalog-id",
        "--fabric",
        "--kafka-platform-instance",
        "--gateway-platform-id",
        "--gateway-platform-instance",
        "--output-file",
        "--project-dir",
        "--env",
    ):
        assert option in result.output
    for forbidden in ("--url", "--token", "--owner-map", "--force"):
        assert forbidden not in result.output
    assert "datahub" in group.output


def test_text_stdout_is_exact_canonical_json_and_warnings_stay_on_stderr(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project(owner_and_tags=True))
    first = CliRunner().invoke(main, _command(tmp_path))
    second = CliRunner().invoke(main, _command(tmp_path))
    assert first.exit_code == second.exit_code == 0, first.output
    assert first.stdout == second.stdout
    records = json.loads(first.stdout)
    assert (
        first.stdout
        == json.dumps(
            records,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    assert len(records) == 3
    assert first.stderr.count("WARNING") == 2
    assert "Declared owner is omitted" in first.stderr
    assert "Declared tags are omitted" in first.stderr
    for secret in (
        "OWNER_SECRET",
        "TAG_SECRET",
        "PRIVATE_BROKER",
        "RUNTIME_SECRET",
    ):
        assert secret not in first.stdout + first.stderr


def test_json_is_one_exact_envelope_with_counts_and_warnings(tmp_path: Path) -> None:
    _write_project(tmp_path, _project(owner_and_tags=True))
    result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])
    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    envelope = json.loads(result.stdout)
    assert envelope == {
        "status": "ok",
        "command": "docs datahub",
        "data": {
            "standard": "DataHub MCP",
            "release": "1.7.0",
            "api_version": "MetadataChangeProposal",
            "proposals": envelope["data"]["proposals"],
            "counts": {
                "proposals": 3,
                "entities": {"dataFlow": 1, "dataset": 1, "dataJob": 0},
                "aspects": {
                    "dataFlowInfo": 1,
                    "datasetProperties": 1,
                    "dataPlatformInstance": 1,
                    "dataJobInfo": 0,
                    "dataJobInputOutput": 0,
                },
            },
            "output_file": None,
        },
        "errors": [],
        "warnings": [
            {
                "code": "W118_DATAHUB_OWNER_OMITTED",
                "message": "Declared owner is omitted from DataHub export",
                "location": "sources/orders/owner",
            },
            {
                "code": "W117_DATAHUB_TAGS_OMITTED",
                "message": "Declared tags are omitted from DataHub export",
                "location": "sources/orders/tags",
            },
        ],
    }
    for proposal in envelope["data"]["proposals"]:
        _assert_object_keys_sorted(proposal)


@pytest.mark.parametrize(
    ("args", "location"),
    [
        (["--fabric", "PROD"], "catalog_id"),
        (["--catalog-id", "catalog"], "fabric"),
        (["--catalog-id", " ", "--fabric", "PROD"], "data_flow/catalog_id"),
        (["--catalog-id", "x" * 201, "--fabric", "PROD"], "data_flow/catalog_id"),
        (["--catalog-id", "catalog", "--fabric", "prod"], "fabric"),
        (
            [
                "--catalog-id",
                "catalog",
                "--fabric",
                "PROD",
                "--gateway-platform-id",
                "gateway",
            ],
            "gateway_platform",
        ),
    ],
)
def test_primitive_inputs_fail_before_parse_compile_or_output_creation(
    tmp_path: Path,
    args: list[str],
    location: str,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "must-not-exist.json"
    with (
        patch("streamt.core.parser.ProjectParser", side_effect=AssertionError("must not parse")),
        patch("streamt.compiler.Compiler", side_effect=AssertionError("must not compile")),
    ):
        result = CliRunner().invoke(
            main,
            [
                "--output",
                "json",
                "docs",
                "datahub",
                "-p",
                str(tmp_path),
                "--output-file",
                str(target),
                *args,
            ],
        )
    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E508_DATAHUB_INVALID"
    assert error["location"] == location
    assert not target.exists()
    assert "Usage:" not in result.stderr


def test_missing_project_path_is_a_bounded_e508_not_a_click_usage_error(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(missing)],
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"] == [
        {
            "code": "E508_DATAHUB_INVALID",
            "message": "Could not parse project for DataHub export",
            "location": "project",
        }
    ]
    assert "Usage:" not in result.stderr


def test_gateway_options_are_required_only_when_gateway_assets_exist(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    assert CliRunner().invoke(main, _command(tmp_path)).exit_code == 0

    gateway_dir = tmp_path / "gateway"
    gateway_dir.mkdir()
    _write_project(gateway_dir, _project(gateway=True))
    missing = CliRunner().invoke(main, ["--output", "json", *_command(gateway_dir)])
    assert missing.exit_code == 1
    assert json.loads(missing.stdout)["errors"][0] == {
        "code": "E508_DATAHUB_INVALID",
        "message": "Could not generate DataHub catalog",
        "location": "gateway_platform",
    }

    complete = CliRunner().invoke(
        main,
        _command(
            gateway_dir,
            "--gateway-platform-id",
            "conduktor-gateway",
            "--gateway-platform-instance",
            "edge",
        ),
    )
    assert complete.exit_code == 0, complete.output
    assert "conduktor-gateway" in complete.stdout


def test_exactly_one_dry_run_compile_without_external_runtime_seams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    calls: list[bool] = []
    real_compile = Compiler.compile
    real_import = builtins.__import__

    def compile_once(self: Compiler, dry_run: bool = False):  # type: ignore[no-untyped-def]
        calls.append(dry_run)
        return real_compile(self, dry_run=dry_run)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("offline export touched runtime infrastructure")

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "datahub" or name.startswith("datahub."):
            raise AssertionError("external DataHub SDK imported")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(Compiler, "compile", compile_once)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(builtins, "__import__", guarded_import)
    with ExitStack() as stack:
        for target in (
            "streamt.deployer.kafka.KafkaDeployer",
            "streamt.deployer.gateway.GatewayDeployer",
            "streamt.deployer.flink.FlinkDeployer",
            "streamt.deployer.connect.ConnectDeployer",
            "streamt.deployer.state_backend.DeploymentStateService",
        ):
            stack.enter_context(patch(target, side_effect=forbidden))
        result = CliRunner().invoke(main, _command(tmp_path))
    assert result.exit_code == 0, result.output
    assert calls == [True]


def test_sink_and_exposure_warnings_preserve_occurrences_without_secrets(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project(sink=True, exposures=2))
    raw = CliRunner().invoke(main, _command(tmp_path))
    structured = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])
    assert raw.exit_code == structured.exit_code == 0
    assert raw.stderr.count("Exposure metadata is omitted") == 2
    assert raw.stderr.count("Connector destination metadata is omitted") == 1
    assert [warning["code"] for warning in json.loads(structured.stdout)["warnings"]] == [
        "W116_DATAHUB_EXPOSURE_OMITTED",
        "W116_DATAHUB_EXPOSURE_OMITTED",
        "W115_DATAHUB_SINK_OUTPUT_OMITTED",
    ]
    assert structured.stderr == ""
    for secret in ("CONNECTOR_SECRET", "EXPOSURE_SECRET"):
        assert secret not in raw.stdout + raw.stderr + structured.stdout


def test_parser_warnings_preserve_multiplicity_without_forwarding_details(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "LOCAL_PATH_SECRET_MUST_NOT_APPEAR.sql").write_text(
        "SELECT 'SQL_FILE_SECRET_MUST_NOT_APPEAR'",
        encoding="utf-8",
    )
    monkeypatch.setenv("STREAMT_ENV", "IGNORED_ENV_SECRET_MUST_NOT_APPEAR")

    result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])

    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["warnings"] == [
        {
            "code": "W000_WARNING",
            "message": "Project parsing emitted a compatibility warning",
            "location": "project",
        },
        {
            "code": "W000_WARNING",
            "message": "Project parsing emitted a compatibility warning",
            "location": "project",
        },
    ]
    assert result.stderr == ""
    for secret in (
        "LOCAL_PATH_SECRET",
        "SQL_FILE_SECRET",
        "IGNORED_ENV_SECRET",
    ):
        assert secret not in result.stdout + result.stderr


def test_output_file_json_text_and_quiet_contract(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    raw = CliRunner().invoke(main, _command(tmp_path))
    target = tmp_path / "nested" / "catalog.json"
    structured = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
    )
    assert structured.exit_code == 0, structured.output
    envelope = json.loads(structured.stdout)
    assert target.read_bytes() == raw.stdout.encode()
    assert envelope["data"]["output_file"] == str(target)
    assert envelope["data"]["proposals"] == json.loads(target.read_text())

    text_target = tmp_path / "text.json"
    text = CliRunner().invoke(
        main,
        _command(tmp_path, "--output-file", str(text_target)),
    )
    assert text.exit_code == 0
    assert text.stdout == ""
    assert text_target.read_bytes() == raw.stdout.encode()

    quiet_target = tmp_path / "quiet.json"
    quiet = CliRunner().invoke(
        main,
        ["--quiet", *_command(tmp_path, "--output-file", str(quiet_target))],
    )
    assert quiet.exit_code == 0
    assert quiet.stdout == quiet.stderr == ""
    assert quiet_target.read_bytes() == raw.stdout.encode()

    quiet_json = CliRunner().invoke(
        main,
        ["--quiet", "--output", "json", *_command(tmp_path)],
    )
    assert quiet_json.exit_code == 0
    assert quiet_json.stdout == quiet_json.stderr == ""

    quiet_json_target = tmp_path / "quiet-json.json"
    quiet_json_file = CliRunner().invoke(
        main,
        [
            "--quiet",
            "--output",
            "json",
            *_command(tmp_path, "--output-file", str(quiet_json_target)),
        ],
    )
    assert quiet_json_file.exit_code == 0
    assert quiet_json_file.stdout == quiet_json_file.stderr == ""
    assert quiet_json_target.read_bytes() == raw.stdout.encode()


def test_quiet_json_suppresses_failure_envelope_but_keeps_bounded_error_text(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(
        main,
        ["--quiet", "--output", "json", *_command(tmp_path, "--fabric", "prod")],
    )
    assert result.exit_code == 1
    assert result.stdout == ""
    assert "Fabric must be an exact uppercase DataHub v1.7 FabricType" in result.stderr
    assert "prod" not in result.stderr


@pytest.mark.parametrize(
    "failure_stage",
    ["path_prepare", "create_open", "fdopen", "write", "flush", "fsync", "replace"],
)
def test_atomic_failures_preserve_destination_and_clean_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "output" / "catalog.json"
    target.parent.mkdir()
    target.write_text("original\n", encoding="utf-8")
    real_mkdir = Path.mkdir
    real_open_staging = docs_commands._open_datahub_staging

    if failure_stage == "path_prepare":

        def fail_parent(
            path: Path,
            mode: int = 0o777,
            parents: bool = False,
            exist_ok: bool = False,
        ) -> None:
            if path == target.parent:
                raise PermissionError("ATOMIC_PATH_SECRET_MUST_NOT_APPEAR")
            real_mkdir(path, mode=mode, parents=parents, exist_ok=exist_ok)

        monkeypatch.setattr(Path, "mkdir", fail_parent)
    elif failure_stage == "create_open":
        monkeypatch.setattr(
            docs_commands,
            "_open_datahub_staging",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                PermissionError("ATOMIC_OPEN_SECRET_MUST_NOT_APPEAR")
            ),
        )
    elif failure_stage == "fdopen":
        monkeypatch.setattr(
            os,
            "fdopen",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("ATOMIC_FDOPEN_SECRET_MUST_NOT_APPEAR")
            ),
        )
    elif failure_stage in {"write", "flush"}:

        def wrap_file(path: Path) -> object:
            return _FailingBinaryTemporaryFile(
                real_open_staging(path),
                failure_stage,
            )

        monkeypatch.setattr(docs_commands, "_open_datahub_staging", wrap_file)
    elif failure_stage == "fsync":
        monkeypatch.setattr(
            os,
            "fsync",
            lambda _fd: (_ for _ in ()).throw(OSError("ATOMIC_FSYNC_SECRET_MUST_NOT_APPEAR")),
        )
    else:
        monkeypatch.setattr(
            os,
            "replace",
            lambda *_args: (_ for _ in ()).throw(OSError("ATOMIC_REPLACE_SECRET_MUST_NOT_APPEAR")),
        )

    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"][0] == {
        "code": "E508_DATAHUB_INVALID",
        "message": "Could not write DataHub output file atomically",
        "location": "output_file",
    }
    assert "ATOMIC_" not in result.stdout + result.stderr
    assert target.read_text(encoding="utf-8") == "original\n"
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_atomic_staging_name_is_deterministic_and_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "output" / "catalog.json"
    expected_stage = target.parent / f".{target.name}.streamt-datahub.tmp"
    observed: list[tuple[Path, int]] = []
    real_open_staging = docs_commands._open_datahub_staging

    def record_staging(path: Path):  # type: ignore[no-untyped-def]
        handle = real_open_staging(path)
        observed.append((path, stat.S_IMODE(path.stat().st_mode)))
        return handle

    monkeypatch.setattr(docs_commands, "_open_datahub_staging", record_staging)
    result = CliRunner().invoke(
        main,
        _command(tmp_path, "--output-file", str(target)),
    )

    assert result.exit_code == 0, result.output
    assert observed == [(expected_stage, 0o600)]
    assert not expected_stage.exists()
    assert stat.S_IMODE(target.stat().st_mode) == 0o600


def test_atomic_writer_recovers_a_stale_deterministic_stage(tmp_path: Path) -> None:
    target = tmp_path / "catalog.json"
    stage = tmp_path / ".catalog.json.streamt-datahub.tmp"
    target.write_bytes(b"original\n")
    stage.write_bytes(b"stale\n")

    docs_commands._atomic_write_datahub(target, b"replacement\n")

    assert target.read_bytes() == b"replacement\n"
    assert not stage.exists()


def test_atomic_writer_serializes_concurrent_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "catalog.json"
    first_opened = threading.Event()
    release_first = threading.Event()
    second_opened = threading.Event()
    open_count = 0
    count_lock = threading.Lock()
    real_open_staging = docs_commands._open_datahub_staging

    def observe_open(path: Path):  # type: ignore[no-untyped-def]
        nonlocal open_count
        handle = real_open_staging(path)
        with count_lock:
            open_count += 1
            ordinal = open_count
        if ordinal == 1:
            first_opened.set()
            assert release_first.wait(timeout=2)
        else:
            second_opened.set()
        return handle

    monkeypatch.setattr(docs_commands, "_open_datahub_staging", observe_open)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(docs_commands._atomic_write_datahub, target, b"first\n")
        assert first_opened.wait(timeout=2)
        second = executor.submit(docs_commands._atomic_write_datahub, target, b"second\n")
        assert not second_opened.wait(timeout=0.1)
        release_first.set()
        first.result(timeout=2)
        second.result(timeout=2)

    assert target.read_bytes() == b"second\n"
    assert open_count == 2
    assert not (tmp_path / ".catalog.json.streamt-datahub.tmp").exists()


def test_parse_compile_projection_mapper_and_unexpected_errors_are_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "FAILURE_SECRET_MUST_NOT_APPEAR"
    _write_project(tmp_path, _project())

    scenarios: list[tuple[Any, str, str]] = [
        (
            patch.object(Compiler, "compile", side_effect=ValueError(secret)),
            "Could not compile project for DataHub export",
            "models",
        ),
        (
            patch.object(
                catalog_model,
                "build_catalog_snapshot",
                side_effect=CatalogProjectionError(secret, location="snapshot"),
            ),
            "Could not build catalog snapshot",
            "snapshot",
        ),
        (
            patch.object(
                datahub_export,
                "generate_datahub_catalog",
                side_effect=DataHubExportError(secret, location="proposals"),
            ),
            "Could not generate DataHub catalog",
            "proposals",
        ),
        (
            patch.object(
                datahub_export,
                "generate_datahub_catalog",
                side_effect=RuntimeError(secret),
            ),
            "Could not generate validated DataHub export",
            "proposals",
        ),
    ]
    outputs: list[str] = []
    for context, message, location in scenarios:
        with context:
            result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])
        assert result.exit_code == 1
        error = json.loads(result.stdout)["errors"][0]
        assert error == {
            "code": "E508_DATAHUB_INVALID",
            "message": message,
            "location": location,
        }
        outputs.append(result.stdout + result.stderr)

    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "stream_project.yml").write_text("project: [PARSE_SECRET]\n", encoding="utf-8")
    parsed = CliRunner().invoke(main, ["--output", "json", *_command(broken)])
    assert parsed.exit_code == 1
    assert json.loads(parsed.stdout)["errors"][0] == {
        "code": "E508_DATAHUB_INVALID",
        "message": "Could not parse project for DataHub export",
        "location": "project",
    }
    outputs.append(parsed.stdout + parsed.stderr)
    assert secret not in "".join(outputs)


def test_export_materialization_failure_cannot_replace_existing_destination(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "catalog.json"
    target.write_text("original\n", encoding="utf-8")

    class BrokenExport:
        @property
        def proposals(self) -> object:
            raise RuntimeError("MATERIALIZATION_SECRET_MUST_NOT_APPEAR")

    with patch.object(
        datahub_export,
        "generate_datahub_catalog",
        return_value=BrokenExport(),
    ):
        result = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
        )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"] == [
        {
            "code": "E508_DATAHUB_INVALID",
            "message": "Could not generate validated DataHub export",
            "location": "proposals",
        }
    ]
    assert "MATERIALIZATION_SECRET" not in result.stdout + result.stderr
    assert target.read_text(encoding="utf-8") == "original\n"


def test_warning_finalization_failure_cannot_replace_existing_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project(owner_and_tags=True))
    target = tmp_path / "catalog.json"
    target.write_text("original\n", encoding="utf-8")
    monkeypatch.setattr(
        docs_commands.OutputFormatter,
        "add_warning",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("WARNING_SECRET_MUST_NOT_APPEAR")
        ),
    )

    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
    )

    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"] == [
        {
            "code": "E508_DATAHUB_INVALID",
            "message": "Could not generate validated DataHub export",
            "location": "proposals",
        }
    ]
    assert "WARNING_SECRET" not in result.stdout + result.stderr
    assert target.read_text(encoding="utf-8") == "original\n"


def test_lazy_import_failure_is_bounded_and_secret_neutral(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    real_import = builtins.__import__

    def fail_datahub_export(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "streamt.integrations.catalog.datahub_export":
            raise ImportError("IMPORT_SECRET_MUST_NOT_APPEAR")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail_datahub_export)
    result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"] == [
        {
            "code": "E508_DATAHUB_INVALID",
            "message": "Could not initialize DataHub export",
            "location": "export",
        }
    ]
    assert "IMPORT_SECRET" not in result.stdout + result.stderr
