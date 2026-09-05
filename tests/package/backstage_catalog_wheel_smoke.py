"""Installed-wheel smoke gate for the offline Backstage catalog export."""

from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path
from typing import cast

import requests
import yaml
from click.testing import CliRunner, Result

import streamt
from streamt.cli import main
from streamt.integrations.catalog.backstage_validation import (
    validate_backstage_entity,
)

_CATALOG_ID = "payments-wheel"
_NAMESPACE = "streaming"
_DEFAULT_OWNER = "group:catalog/default"
_SOURCE_OWNER = "group:catalog/source-team"
_PIPELINE_OWNER = "group:catalog/pipeline-team"
_SINK_OWNER = "user:catalog/sink-owner"
_KAFKA_CLUSTER = "resource:infra/kafka-main"
_GATEWAY_CLUSTER = "resource:infra/gateway-main"
_DOMAIN = "domain:catalog/payments"
_LIFECYCLE = "production"

_SECRET_VALUES = (
    "broker-private-token.invalid:19092",
    "broker-private-user",
    "broker-private-password",
    "schema-private-token.invalid",
    "schema-private-user",
    "schema-private-password",
    "flink-private-token.invalid",
    "flink-private-api-key",
    "connect-private-token.invalid",
    "connect-private-password",
    "gateway-private-token.invalid",
    "gateway-private-user",
    "gateway-private-password",
    "console-private-token.invalid",
    "console-private-api-key",
    "warehouse-private-password",
    "connector-private-token",
    "compiled-sql-private-literal",
    "source-column-private-description",
    "contract-column-private-description",
    "exposure-private-description",
    "dashboard-private-token.invalid",
    "exposure-private-consumer-group",
    "preexisting-output-private-sentinel",
)


def _write_project(project_dir: Path) -> None:
    config: dict[str, object] = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {
            "name": "payments-wheel-catalog",
            "version": "1.0.0",
            "description": "Payments streaming catalog",
        },
        "runtime": {
            "kafka": {
                "bootstrap_servers": "broker-private-token.invalid:19092",
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_username": "broker-private-user",
                "sasl_password": "broker-private-password",
            },
            "schema_registry": {
                "url": "https://schema-private-token.invalid/api",
                "username": "schema-private-user",
                "password": "schema-private-password",
            },
            "flink": {
                "default": "private-flink",
                "clusters": {
                    "private-flink": {
                        "rest_url": "https://flink-private-token.invalid/rest",
                        "sql_gateway_url": "https://flink-private-token.invalid/sql",
                        "api_key": "flink-private-api-key",
                    }
                },
            },
            "connect": {
                "default": "private-connect",
                "clusters": {
                    "private-connect": {
                        "rest_url": "https://connect-private-token.invalid/api",
                        "password": "connect-private-password",
                    }
                },
            },
            "conduktor": {
                "gateway": {
                    "admin_url": "https://gateway-private-token.invalid/admin",
                    "proxy_bootstrap": "gateway-private-token.invalid:16969",
                    "username": "gateway-private-user",
                    "password": "gateway-private-password",
                    "virtual_cluster": "payments-private-vcluster",
                },
                "console": {
                    "url": "https://console-private-token.invalid/api",
                    "api_key": "console-private-api-key",
                },
            },
        },
        "connections": {
            "warehouse": {
                "type": "snowflake",
                "config": {"password": "warehouse-private-password"},
            }
        },
        "sources": [
            {
                "name": "raw_orders",
                "description": "Raw order events",
                "topic": "orders.raw.v1",
                "owner": "source-team",
                "tags": ["raw", "payments"],
                "columns": [
                    {
                        "name": "card_token",
                        "type": "STRING",
                        "description": "source-column-private-description",
                    }
                ],
            }
        ],
        "models": [
            {
                "name": "plain_orders",
                "description": "Provisioned order topic",
                "materialized": "topic",
                "topic": {"name": "orders.plain.v1"},
                "contract": {
                    "enforced": False,
                    "columns": [
                        {
                            "name": "id",
                            "description": "contract-column-private-description",
                        }
                    ],
                },
            },
            {
                "name": "enriched_orders",
                "description": "Enriched order events",
                "materialized": "flink",
                "sql": (
                    "SELECT *, 'compiled-sql-private-literal' AS private_marker "
                    'FROM {{ source("raw_orders") }}'
                ),
                "topic": {"name": "orders.enriched.v1"},
                "owner": "pipeline-team",
                "tags": ["payments", "enriched"],
                "contract": {
                    "enforced": True,
                    "columns": [
                        {
                            "name": "id",
                            "description": "contract-column-private-description",
                        }
                    ],
                },
            },
            {
                "name": "public_orders",
                "description": "Public order projection",
                "materialized": "virtual_topic",
                "sql": 'SELECT * FROM {{ ref("enriched_orders") }} WHERE id IS NOT NULL',
                "gateway": {"virtual_topic": {"name": "orders.public"}},
                "owner": "pipeline-team",
                "tags": ["public", "payments"],
                "contract": {
                    "enforced": False,
                    "columns": [
                        {
                            "name": "id",
                            "description": "contract-column-private-description",
                        }
                    ],
                },
            },
            {
                "name": "warehouse_sink",
                "description": "Warehouse delivery",
                "materialized": "sink",
                "from": [{"ref": "public_orders"}],
                "sink": {
                    "connector": "snowflake-sink",
                    "connection": "warehouse",
                    "config": {"api.token": "connector-private-token"},
                },
                "owner": "sink-team",
            },
        ],
        "exposures": [
            {
                "name": "orders_dashboard",
                "type": "dashboard",
                "description": "exposure-private-description",
                "url": "https://dashboard-private-token.invalid/view",
                "consumer_group": "exposure-private-consumer-group",
                "consumes": [{"ref": "public_orders"}],
            },
            {
                "name": "orders_application",
                "type": "application",
                "description": "exposure-private-description",
                "url": "https://dashboard-private-token.invalid/app",
                "consumer_group": "exposure-private-consumer-group",
                "consumes": [{"ref": "plain_orders"}],
            },
        ],
    }
    (project_dir / "stream_project.yml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )


def _write_owner_map(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "owners": {
                    "sink-team": _SINK_OWNER,
                    "pipeline-team": _PIPELINE_OWNER,
                    "source-team": _SOURCE_OWNER,
                },
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _assert_installed_wheel(checkout: Path) -> None:
    checkout_source = (checkout / "src").resolve()
    installed_module = Path(streamt.__file__).resolve()
    import_roots = {Path(entry).resolve() for entry in sys.path if entry}
    assert sys.flags.isolated == 1
    assert "PYTHONPATH" not in os.environ
    assert checkout_source not in import_roots, import_roots
    assert checkout not in installed_module.parents, installed_module
    assert checkout != Path.cwd()
    assert checkout not in Path.cwd().parents
    assert Path(sys.executable).with_name("streamt").is_file()


def _assert_source_checkout(checkout: Path) -> None:
    checkout_source = (checkout / "src").resolve()
    imported_module = Path(streamt.__file__).resolve()
    assert checkout_source in imported_module.parents, imported_module
    assert checkout != Path.cwd()
    assert checkout not in Path.cwd().parents


def _deny_external_access() -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError(
            "installed Backstage export attempted network or subprocess access"
        )

    socket.getaddrinfo = fail  # type: ignore[assignment]
    socket.gethostbyname = fail  # type: ignore[assignment]
    socket.gethostbyname_ex = fail  # type: ignore[assignment]
    socket.create_connection = fail  # type: ignore[assignment]
    socket.socket.connect = fail  # type: ignore[method-assign]
    socket.socket.connect_ex = fail  # type: ignore[assignment]
    http.client.HTTPConnection.connect = fail  # type: ignore[method-assign]
    http.client.HTTPConnection.request = fail  # type: ignore[method-assign]
    urllib.request.urlopen = fail
    requests.sessions.Session.request = fail  # type: ignore[assignment]
    subprocess.Popen = fail  # type: ignore[assignment,misc]
    subprocess.run = fail  # type: ignore[assignment]
    subprocess.call = fail  # type: ignore[assignment]
    subprocess.check_call = fail  # type: ignore[assignment]
    subprocess.check_output = fail  # type: ignore[assignment]


def _common_arguments(project_dir: Path, owner_map: Path) -> list[str]:
    return [
        "docs",
        "backstage",
        "--catalog-id",
        _CATALOG_ID,
        "--catalog-namespace",
        _NAMESPACE,
        "--default-owner-ref",
        _DEFAULT_OWNER,
        "--lifecycle",
        _LIFECYCLE,
        "--owner-map",
        str(owner_map),
        "--kafka-cluster-ref",
        _KAFKA_CLUSTER,
        "--gateway-cluster-ref",
        _GATEWAY_CLUSTER,
        "--domain-ref",
        _DOMAIN,
        "--project-dir",
        str(project_dir),
    ]


def _invoke(*arguments: str) -> Result:
    result = CliRunner().invoke(main, list(arguments), catch_exceptions=False)
    assert result.exit_code == 0, (
        f"streamt {' '.join(arguments)} returned {result.exit_code}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict), value
    assert all(isinstance(key, str) for key in value)
    return cast(dict[str, object], value)


def _annotations(entity: dict[str, object]) -> dict[str, object]:
    metadata = _mapping(entity["metadata"])
    return _mapping(metadata["annotations"])


def _entity_ref(entity: dict[str, object]) -> str:
    kind = entity["kind"]
    metadata = _mapping(entity["metadata"])
    name = metadata["name"]
    namespace = metadata["namespace"]
    assert isinstance(kind, str)
    assert isinstance(name, str)
    assert isinstance(namespace, str)
    return f"{kind.lower()}:{namespace}/{name}"


def _entity_by_logical_name(
    entities: list[dict[str, object]],
    *,
    kind: str,
    logical_name: str,
) -> dict[str, object]:
    matches = [
        entity
        for entity in entities
        if entity["kind"] == kind
        and _annotations(entity)["streamt.dev/logical-name"] == logical_name
    ]
    assert len(matches) == 1, (kind, logical_name, matches)
    return matches[0]


def _assert_entities(entities: list[dict[str, object]]) -> None:
    assert [entity["kind"] for entity in entities] == [
        "System",
        "Resource",
        "Resource",
        "Resource",
        "Resource",
        "Component",
        "Component",
        "Component",
    ]
    assert [
        cast(str, _mapping(entity["metadata"])["name"])
        for entity in entities[1:5]
    ] == sorted(
        cast(str, _mapping(entity["metadata"])["name"])
        for entity in entities[1:5]
    )
    assert [
        cast(str, _mapping(entity["metadata"])["name"])
        for entity in entities[5:]
    ] == sorted(
        cast(str, _mapping(entity["metadata"])["name"])
        for entity in entities[5:]
    )

    for entity in entities:
        assert set(entity) == {"apiVersion", "kind", "metadata", "spec"}
        assert entity["apiVersion"] == "backstage.io/v1alpha1"
        validate_backstage_entity(entity)

    system = entities[0]
    raw = _entity_by_logical_name(entities, kind="Resource", logical_name="raw_orders")
    plain = _entity_by_logical_name(
        entities,
        kind="Resource",
        logical_name="plain_orders",
    )
    enriched_resource = _entity_by_logical_name(
        entities,
        kind="Resource",
        logical_name="enriched_orders",
    )
    public_resource = _entity_by_logical_name(
        entities,
        kind="Resource",
        logical_name="public_orders",
    )
    enriched_component = _entity_by_logical_name(
        entities,
        kind="Component",
        logical_name="enriched_orders",
    )
    public_component = _entity_by_logical_name(
        entities,
        kind="Component",
        logical_name="public_orders",
    )
    sink_component = _entity_by_logical_name(
        entities,
        kind="Component",
        logical_name="warehouse_sink",
    )

    system_ref = _entity_ref(system)
    raw_ref = _entity_ref(raw)
    enriched_resource_ref = _entity_ref(enriched_resource)
    public_resource_ref = _entity_ref(public_resource)
    enriched_component_ref = _entity_ref(enriched_component)
    public_component_ref = _entity_ref(public_component)

    assert _mapping(system["spec"]) == {
        "owner": _DEFAULT_OWNER,
        "domain": _DOMAIN,
    }
    assert _mapping(raw["spec"]) == {
        "type": "kafka-topic",
        "owner": _SOURCE_OWNER,
        "dependsOn": [_KAFKA_CLUSTER],
    }
    assert _mapping(plain["spec"]) == {
        "type": "kafka-topic",
        "owner": _DEFAULT_OWNER,
        "system": system_ref,
        "dependsOn": [_KAFKA_CLUSTER],
    }
    assert _mapping(enriched_resource["spec"]) == {
        "type": "kafka-topic",
        "owner": _PIPELINE_OWNER,
        "system": system_ref,
        "dependsOn": [enriched_component_ref, _KAFKA_CLUSTER],
    }
    assert _mapping(public_resource["spec"]) == {
        "type": "kafka-virtual-topic",
        "owner": _PIPELINE_OWNER,
        "system": system_ref,
        "dependsOn": [public_component_ref, _GATEWAY_CLUSTER],
    }
    assert _mapping(enriched_component["spec"]) == {
        "type": "data-pipeline",
        "lifecycle": _LIFECYCLE,
        "owner": _PIPELINE_OWNER,
        "system": system_ref,
        "dependsOn": [raw_ref],
    }
    assert _mapping(public_component["spec"]) == {
        "type": "data-pipeline",
        "lifecycle": _LIFECYCLE,
        "owner": _PIPELINE_OWNER,
        "system": system_ref,
        "dependsOn": [enriched_resource_ref],
    }
    assert _mapping(sink_component["spec"]) == {
        "type": "data-pipeline",
        "lifecycle": _LIFECYCLE,
        "owner": _SINK_OWNER,
        "system": system_ref,
        "dependsOn": [public_resource_ref],
    }

    assert _annotations(raw).get("streamt.dev/contract") is None
    assert _annotations(plain)["streamt.dev/contract"] == "declared"
    assert _annotations(enriched_resource)["streamt.dev/contract"] == "enforced"
    assert _annotations(public_resource)["streamt.dev/contract"] == "declared"
    assert _annotations(enriched_component)["streamt.dev/process-kind"] == "flink"
    assert _annotations(public_component)["streamt.dev/process-kind"] == "gateway"
    assert _annotations(sink_component)["streamt.dev/process-kind"] == "connect"
    assert _mapping(raw["metadata"])["tags"] == ["payments", "raw"]
    assert _mapping(enriched_resource["metadata"])["tags"] == [
        "enriched",
        "payments",
    ]
    assert _mapping(public_resource["metadata"])["tags"] == ["payments", "public"]
    assert {
        _annotations(entity)["streamt.dev/logical-name"]
        for entity in entities
        if entity["kind"] == "Resource"
    } == {"raw_orders", "plain_orders", "enriched_orders", "public_orders"}


def _assert_warnings(payload: dict[str, object], text_stderr: str) -> None:
    warnings = payload["warnings"]
    assert warnings == [
        {
            "code": "W114_BACKSTAGE_EXPOSURE_OMITTED",
            "message": "Exposure metadata is omitted from Backstage export",
            "location": "exposures/orders_application",
        },
        {
            "code": "W114_BACKSTAGE_EXPOSURE_OMITTED",
            "message": "Exposure metadata is omitted from Backstage export",
            "location": "exposures/orders_dashboard",
        },
        {
            "code": "W113_BACKSTAGE_SINK_OUTPUT_OMITTED",
            "message": "Connector destination metadata is omitted from Backstage export",
            "location": "models/warehouse_sink",
        },
    ]
    assert text_stderr.count("Exposure metadata is omitted from Backstage export") == 2
    assert text_stderr.count(
        "Connector destination metadata is omitted from Backstage export"
    ) == 1


def _assert_secret_neutral(rendered: str, *, checkout: Path) -> None:
    forbidden = (
        *_SECRET_VALUES,
        str(checkout),
        "SELECT *",
        "bootstrap_servers",
        "sasl_password",
        "sql_gateway_url",
        "rest_url",
        "api.token",
        "card_token",
        "consumer_group",
        "contracts:",
        "connections:",
        "runtime:",
    )
    for value in forbidden:
        assert value not in rendered, value


def _write_source_baseline() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    baseline_path = Path(os.environ["STREAMT_BACKSTAGE_SOURCE_BASELINE_PATH"]).resolve()
    _assert_source_checkout(checkout)
    assert checkout != baseline_path
    assert checkout not in baseline_path.parents
    assert baseline_path.parent.is_dir()

    with tempfile.TemporaryDirectory(prefix="streamt-backstage-source-") as raw_root:
        root = Path(raw_root)
        project_dir = root / "project"
        project_dir.mkdir()
        owner_map = root / "owners.json"
        _write_project(project_dir)
        _write_owner_map(owner_map)

        _deny_external_access()
        result = _invoke(*_common_arguments(project_dir, owner_map))
        canonical = result.stdout
        assert canonical.startswith("---\n")
        assert canonical.endswith("\n")
        assert canonical.count("---\n") == 8
        _assert_secret_neutral(
            canonical + "\n" + result.stderr,
            checkout=checkout,
        )
        baseline_path.write_bytes(canonical.encode("utf-8"))


def _exercise_installed_export() -> None:
    checkout = Path(os.environ["STREAMT_CHECKOUT"]).resolve()
    parity_path = Path(os.environ["STREAMT_BACKSTAGE_PARITY_PATH"]).resolve()
    baseline_path = Path(os.environ["STREAMT_BACKSTAGE_SOURCE_BASELINE_PATH"]).resolve()
    _assert_installed_wheel(checkout)
    assert checkout != parity_path
    assert checkout not in parity_path.parents
    assert parity_path.parent.is_dir()
    assert baseline_path.is_file()
    assert checkout != baseline_path
    assert checkout not in baseline_path.parents
    assert baseline_path != parity_path

    with tempfile.TemporaryDirectory(prefix="streamt-backstage-wheel-") as raw_root:
        root = Path(raw_root)
        project_dir = root / "project"
        project_dir.mkdir()
        owner_map = root / "owners.json"
        _write_project(project_dir)
        _write_owner_map(owner_map)
        parity_path.write_text(
            "preexisting-output-private-sentinel",
            encoding="utf-8",
        )

        _deny_external_access()
        common = _common_arguments(project_dir, owner_map)
        first_text = _invoke(*common)
        second_text = _invoke(*common)
        assert first_text.stdout == second_text.stdout
        assert first_text.stderr == second_text.stderr
        canonical = first_text.stdout
        assert canonical.startswith("---\n")
        assert canonical.endswith("\n")
        assert canonical.count("---\n") == 8
        assert "\r" not in canonical
        assert "\n...\n" not in canonical
        assert "&id" not in canonical
        assert "*id" not in canonical
        assert canonical.encode("utf-8") == baseline_path.read_bytes()

        structured = _invoke(
            "-o",
            "json",
            *common,
            "--output-file",
            str(parity_path),
        )
        assert structured.stderr == ""
        payload = json.loads(structured.stdout)
        assert isinstance(payload, dict)
        assert set(payload) == {"status", "command", "data", "errors", "warnings"}
        assert payload["status"] == "ok"
        assert payload["command"] == "docs backstage"
        assert payload["errors"] == []
        data = _mapping(payload["data"])
        assert data["standard"] == "Backstage Software Catalog"
        assert data["release"] == "1.54.2"
        assert data["api_version"] == "backstage.io/v1alpha1"
        assert data["counts"] == {"System": 1, "Resource": 4, "Component": 3}
        assert data["output_file"] == str(parity_path)

        raw_entities = data["entities"]
        assert isinstance(raw_entities, list)
        entities = [_mapping(entity) for entity in raw_entities]
        round_tripped = list(yaml.safe_load_all(canonical))
        assert round_tripped == entities
        _assert_entities(entities)
        _assert_warnings(payload, first_text.stderr)

        file_bytes = parity_path.read_bytes()
        assert file_bytes == canonical.encode("utf-8")
        assert list(yaml.safe_load_all(file_bytes.decode("utf-8"))) == entities

        rendered = "\n".join(
            (
                first_text.stdout,
                first_text.stderr,
                second_text.stdout,
                second_text.stderr,
                structured.stdout,
                structured.stderr,
                file_bytes.decode("utf-8"),
            )
        )
        _assert_secret_neutral(rendered, checkout=checkout)


if __name__ == "__main__":
    if os.environ.get("STREAMT_BACKSTAGE_SOURCE_MODE") == "1":
        _write_source_baseline()
        print("source-checkout Backstage catalog baseline passed")
    else:
        _exercise_installed_export()
        print("installed-wheel Backstage catalog export passed")
