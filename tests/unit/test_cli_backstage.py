"""CLI acceptance tests for deterministic offline Backstage catalog export."""

from __future__ import annotations

import json
import os
import socket
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

import streamt.integrations.catalog.backstage as backstage
import streamt.integrations.catalog.model as catalog_model
from streamt.cli import main
from streamt.compiler import Compiler
from streamt.integrations.catalog.backstage import BackstageExportError
from streamt.integrations.catalog.model import CatalogProjectionError

CATALOG_ID = "catalog"
NAMESPACE = "data"
DEFAULT_OWNER = "group:teams/default-team"
KAFKA_CLUSTER = "resource:infra/kafka-main"
GATEWAY_CLUSTER = "resource:infra/gateway-main"


def _project(
    *,
    owner: str | None = None,
    sink: bool = False,
    exposures: int = 0,
) -> dict[str, object]:
    source: dict[str, object] = {"name": "orders", "topic": "orders.raw.v1"}
    if owner is not None:
        source["owner"] = owner
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
    return {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "catalog-project"},
        "runtime": {
            "kafka": {
                "bootstrap_servers": "PRIVATE_BROKER_MUST_NOT_APPEAR:9092",
                "sasl_password": "RUNTIME_SECRET_MUST_NOT_APPEAR",
            }
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
        "backstage",
        "--project-dir",
        str(path),
        "--catalog-id",
        CATALOG_ID,
        "--catalog-namespace",
        NAMESPACE,
        "--default-owner-ref",
        DEFAULT_OWNER,
        "--lifecycle",
        "production",
        "--kafka-cluster-ref",
        KAFKA_CLUSTER,
        *extra,
    ]


EXPECTED_YAML = """---
apiVersion: backstage.io/v1alpha1
kind: System
metadata:
  name: system-catalog-191ba80d727b4565
  namespace: data
  title: catalog-project
  annotations:
    streamt.dev/catalog-id: catalog
    streamt.dev/environment: default
    streamt.dev/logical-kind: project
    streamt.dev/logical-name: catalog-project
    streamt.dev/project: catalog-project
spec:
  owner: group:teams/default-team
---
apiVersion: backstage.io/v1alpha1
kind: Resource
metadata:
  name: topic-orders-raw-v1-d5a5b7499407decc
  namespace: data
  title: orders
  annotations:
    streamt.dev/catalog-id: catalog
    streamt.dev/environment: default
    streamt.dev/logical-kind: source
    streamt.dev/logical-name: orders
    streamt.dev/physical-name: orders.raw.v1
    streamt.dev/project: catalog-project
spec:
  type: kafka-topic
  owner: group:teams/default-team
  dependsOn:
  - resource:infra/kafka-main
"""


def test_help_exposes_exact_surface_and_preserves_adjacent_docs_commands() -> None:
    command_help = CliRunner().invoke(main, ["docs", "backstage", "--help"])
    group_help = CliRunner().invoke(main, ["docs", "--help"])

    assert command_help.exit_code == group_help.exit_code == 0
    for option in (
        "--catalog-id",
        "--catalog-namespace",
        "--default-owner-ref",
        "--lifecycle",
        "--owner-map",
        "--kafka-cluster-ref",
        "--gateway-cluster-ref",
        "--domain-ref",
        "--output-file",
        "--project-dir",
        "--env",
    ):
        assert option in command_help.output
    for command in ("asyncapi", "openapi", "odcs", "openlineage", "schema"):
        assert command in group_help.output


def test_text_stdout_is_exact_canonical_yaml(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    runner = CliRunner()

    first = runner.invoke(main, _command(tmp_path))
    second = runner.invoke(main, _command(tmp_path))

    assert first.exit_code == second.exit_code == 0, first.output
    assert first.stdout == second.stdout == EXPECTED_YAML
    assert first.stderr == ""
    assert list(yaml.safe_load_all(first.stdout)) == list(
        yaml.safe_load_all(EXPECTED_YAML)
    )
    assert "PRIVATE_BROKER" not in first.output
    assert "RUNTIME_SECRET" not in first.output


def test_json_is_one_exact_envelope_with_same_entities(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    result = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])

    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    envelope = json.loads(result.stdout)
    assert envelope == {
        "status": "ok",
        "command": "docs backstage",
        "data": {
            "standard": "Backstage Software Catalog",
            "release": "1.54.2",
            "api_version": "backstage.io/v1alpha1",
            "entities": envelope["data"]["entities"],
            "counts": {"System": 1, "Resource": 1, "Component": 0},
            "output_file": None,
        },
        "errors": [],
        "warnings": [],
    }
    assert envelope["data"]["entities"] == list(yaml.safe_load_all(EXPECTED_YAML))
    assert "---" not in result.stdout


@pytest.mark.parametrize(
    ("args", "location"),
    [
        (
            [
                "--catalog-namespace",
                NAMESPACE,
                "--default-owner-ref",
                DEFAULT_OWNER,
                "--lifecycle",
                "production",
            ],
            "catalog_id",
        ),
        (
            [
                "--catalog-id",
                CATALOG_ID,
                "--default-owner-ref",
                DEFAULT_OWNER,
                "--lifecycle",
                "production",
            ],
            "catalog_namespace",
        ),
        (
            [
                "--catalog-id",
                CATALOG_ID,
                "--catalog-namespace",
                NAMESPACE,
                "--lifecycle",
                "production",
            ],
            "default_owner_ref",
        ),
        (
            [
                "--catalog-id",
                CATALOG_ID,
                "--catalog-namespace",
                NAMESPACE,
                "--default-owner-ref",
                DEFAULT_OWNER,
            ],
            "lifecycle",
        ),
        (
            [
                "--catalog-id",
                CATALOG_ID,
                "--catalog-namespace",
                NAMESPACE,
                "--default-owner-ref",
                DEFAULT_OWNER,
                "--lifecycle",
                "production",
                "--gateway-cluster-ref",
                "group:wrong/kind",
            ],
            "gateway_cluster_ref",
        ),
    ],
)
def test_missing_and_invalid_options_fail_before_compiler_construction(
    tmp_path: Path,
    args: list[str],
    location: str,
) -> None:
    _write_project(tmp_path, _project())
    with patch("streamt.compiler.Compiler", side_effect=AssertionError("must not construct")):
        result = CliRunner().invoke(
            main,
            ["--output", "json", "docs", "backstage", "-p", str(tmp_path), *args],
        )

    assert result.exit_code == 1
    error = json.loads(result.stdout)["errors"][0]
    assert error["code"] == "E507_BACKSTAGE_INVALID"
    assert error["location"] == location
    assert "Usage:" not in result.stderr


def test_owner_map_is_preflighted_and_resolves_declared_owner(tmp_path: Path) -> None:
    _write_project(tmp_path, _project(owner="payments-team"))
    owner_map = tmp_path / "owners.json"
    owner_map.write_text(
        '{"version":1,"owners":{"payments-team":"group:teams/payments"}}',
        encoding="utf-8",
    )

    result = CliRunner().invoke(main, _command(tmp_path, "--owner-map", str(owner_map)))

    assert result.exit_code == 0, result.output
    resources = [
        entity
        for entity in yaml.safe_load_all(result.stdout)
        if entity["kind"] == "Resource"
    ]
    assert resources[0]["spec"]["owner"] == "group:teams/payments"

    owner_map.write_text(
        '{"version":1,"owners":{"team":"group:teams/a","team":"group:teams/b"}}',
        encoding="utf-8",
    )
    with patch("streamt.compiler.Compiler", side_effect=AssertionError("must not construct")):
        invalid = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path, "--owner-map", str(owner_map))],
        )
    assert invalid.exit_code == 1
    assert json.loads(invalid.stdout)["errors"][0]["location"] == "owner_map"


def test_cluster_refs_are_required_only_by_compiled_dataset_kinds(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    missing_kafka = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            *[
                argument
                for argument in _command(tmp_path)
                if argument not in {"--kafka-cluster-ref", KAFKA_CLUSTER}
            ],
        ],
    )
    assert missing_kafka.exit_code == 1
    assert json.loads(missing_kafka.stdout)["errors"][0] == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not generate Backstage catalog",
        "location": "kafka_cluster_ref",
    }

    gateway_dir = tmp_path / "gateway"
    gateway_dir.mkdir()
    project = _project()
    project["runtime"] = {
        "kafka": {"bootstrap_servers": "private:9092"},
        "conduktor": {"gateway": {"proxy_bootstrap": "private:6969"}},
    }
    project["models"] = [
        {
            "name": "filtered",
            "sql": 'SELECT * FROM {{ source("orders") }} WHERE id > 0',
            "gateway": {"virtual_topic": {"name": "orders.filtered"}},
        }
    ]
    _write_project(gateway_dir, project)
    missing_gateway = CliRunner().invoke(
        main,
        ["--output", "json", *_command(gateway_dir)],
    )
    assert missing_gateway.exit_code == 1
    assert json.loads(missing_gateway.stdout)["errors"][0] == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not generate Backstage catalog",
        "location": "gateway_cluster_ref",
    }

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    empty = _project()
    empty["sources"] = []
    _write_project(empty_dir, empty)
    args = _command(empty_dir)
    args = [item for item in args if item not in {"--kafka-cluster-ref", KAFKA_CLUSTER}]
    assert CliRunner().invoke(main, args).exit_code == 0


def test_mapper_warnings_preserve_duplicate_messages_and_never_touch_stdout(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path, _project(sink=True, exposures=2))
    raw = CliRunner().invoke(main, _command(tmp_path))
    structured = CliRunner().invoke(main, ["--output", "json", *_command(tmp_path)])

    assert raw.exit_code == structured.exit_code == 0
    assert len(list(yaml.safe_load_all(raw.stdout))) == 3
    assert raw.stderr.count("Exposure metadata is omitted") == 2
    assert "Connector destination metadata is omitted" in raw.stderr
    envelope = json.loads(structured.stdout)
    assert [warning["code"] for warning in envelope["warnings"]] == [
        "W114_BACKSTAGE_EXPOSURE_OMITTED",
        "W114_BACKSTAGE_EXPOSURE_OMITTED",
        "W113_BACKSTAGE_SINK_OUTPUT_OMITTED",
    ]
    assert structured.stderr == ""
    for secret in (
        "CONNECTOR_SECRET_MUST_NOT_APPEAR",
        "EXPOSURE_SECRET_MUST_NOT_APPEAR",
    ):
        assert secret not in raw.stdout + raw.stderr + structured.stdout


def test_default_and_selected_environment_are_exact_identity_inputs(tmp_path: Path) -> None:
    single = tmp_path / "single"
    single.mkdir()
    _write_project(single, _project())
    default_result = CliRunner().invoke(main, _command(single))

    multi = tmp_path / "multi"
    multi.mkdir()
    project = _project()
    project.pop("runtime")
    _write_project(multi, project)
    environments = multi / "environments"
    environments.mkdir()
    (environments / "production.yml").write_text(
        yaml.safe_dump(
            {
                "environment": {"name": "production"},
                "runtime": {"kafka": {"bootstrap_servers": "private:9092"}},
            }
        ),
        encoding="utf-8",
    )
    selected_result = CliRunner().invoke(main, _command(multi, "--env", "production"))

    assert default_result.exit_code == selected_result.exit_code == 0, selected_result.output
    default_system = next(yaml.safe_load_all(default_result.stdout))
    selected_system = next(yaml.safe_load_all(selected_result.stdout))
    assert default_system["metadata"]["annotations"]["streamt.dev/environment"] == "default"
    assert selected_system["metadata"]["annotations"]["streamt.dev/environment"] == "production"
    assert default_system["metadata"]["name"] != selected_system["metadata"]["name"]


def test_exactly_one_dry_run_compile_and_no_runtime_clients_or_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    calls: list[bool] = []
    real_compile = Compiler.compile

    def compile_once(self: Compiler, dry_run: bool = False):  # type: ignore[no-untyped-def]
        calls.append(dry_run)
        return real_compile(self, dry_run=dry_run)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("offline export touched runtime infrastructure")

    monkeypatch.setattr(Compiler, "compile", compile_once)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
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
    assert not (tmp_path / "generated").exists()


def test_output_file_json_quiet_and_atomic_failure_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_project(tmp_path, _project())
    target = tmp_path / "nested" / "catalog.yaml"
    result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
    )
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    assert target.read_text(encoding="utf-8") == EXPECTED_YAML
    assert envelope["data"]["output_file"] == str(target)
    assert envelope["data"]["entities"] == list(yaml.safe_load_all(EXPECTED_YAML))

    quiet_target = tmp_path / "quiet.yaml"
    quiet = CliRunner().invoke(
        main,
        ["--quiet", *_command(tmp_path, "--output-file", str(quiet_target))],
    )
    assert quiet.exit_code == 0
    assert quiet.stdout == quiet.stderr == ""
    assert quiet_target.read_text(encoding="utf-8") == EXPECTED_YAML

    target.write_text("original\n", encoding="utf-8")
    monkeypatch.setattr(
        os,
        "replace",
        lambda *_args: (_ for _ in ()).throw(OSError("WRITE_SECRET_MUST_NOT_APPEAR")),
    )
    failed = CliRunner().invoke(
        main,
        ["--output", "json", *_command(tmp_path, "--output-file", str(target))],
    )
    assert failed.exit_code == 1
    error = json.loads(failed.stdout)["errors"][0]
    assert error == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not write Backstage output file atomically",
        "location": "output_file",
    }
    assert "WRITE_SECRET" not in failed.stdout + failed.stderr
    assert target.read_text(encoding="utf-8") == "original\n"
    assert list(target.parent.glob(f".{target.name}.*.tmp")) == []


def test_parse_compile_projection_and_mapper_errors_are_safe(tmp_path: Path) -> None:
    _write_project(tmp_path, _project())
    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "stream_project.yml").write_text("project: [PARSE_SECRET]\n", encoding="utf-8")
    parse_result = CliRunner().invoke(
        main,
        ["--output", "json", *_command(broken)],
    )
    assert json.loads(parse_result.stdout)["errors"][0]["code"] == "E501_PARSE_ERROR"

    with patch.object(
        Compiler,
        "compile",
        side_effect=ValueError("COMPILE_SECRET_MUST_NOT_APPEAR"),
    ):
        compile_result = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path)],
        )
    assert json.loads(compile_result.stdout)["errors"][0] == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not compile project for Backstage export",
        "location": "models",
    }

    with patch.object(
        catalog_model,
        "build_catalog_snapshot",
        side_effect=CatalogProjectionError(
            "PROJECTION_SECRET_MUST_NOT_APPEAR",
            location="snapshot",
        ),
    ):
        projection_result = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path)],
        )
    with patch.object(
        backstage,
        "generate_backstage_catalog",
        side_effect=BackstageExportError(
            "MAPPER_SECRET_MUST_NOT_APPEAR",
            location="entities",
        ),
    ):
        mapper_result = CliRunner().invoke(
            main,
            ["--output", "json", *_command(tmp_path)],
        )

    assert json.loads(projection_result.stdout)["errors"][0] == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not build catalog snapshot",
        "location": "snapshot",
    }
    assert json.loads(mapper_result.stdout)["errors"][0] == {
        "code": "E507_BACKSTAGE_INVALID",
        "message": "Could not generate Backstage catalog",
        "location": "entities",
    }
    combined = (
        compile_result.stdout
        + compile_result.stderr
        + projection_result.stdout
        + projection_result.stderr
        + mapper_result.stdout
        + mapper_result.stderr
    )
    for secret in ("COMPILE_SECRET", "PROJECTION_SECRET", "MAPPER_SECRET"):
        assert secret not in combined


def test_existing_docs_schema_command_still_works() -> None:
    result = CliRunner().invoke(main, ["docs", "schema"])
    assert result.exit_code == 0
    assert json.loads(result.stdout)["title"] == "streamt project configuration"
