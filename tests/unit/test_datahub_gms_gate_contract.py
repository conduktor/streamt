"""Docker-free contract tests for the DataHub v1.7.0 GMS gate."""

from __future__ import annotations

import importlib.util
import json
import sys
import urllib.parse
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_GATE_PATH = _ROOT / "tests" / "integration" / "datahub" / "gms_v170_gate.py"
_TOPOLOGY_DIR = _ROOT / "tests" / "integration" / "datahub" / "v1.7.0"
_COMPOSE_OVERRIDE_PATH = _TOPOLOGY_DIR / "docker-compose.override.yml"
_IMAGE_LOCK_PATH = _TOPOLOGY_DIR / "images.lock.json"
_CI_WORKFLOW_PATH = _ROOT / ".github" / "workflows" / "ci.yml"

_EXPECTED_TOPOLOGY_IMAGES = {
    "datahub-gms-quickstart": (
        "acryldata/datahub-gms:v1.7.0@"
        "sha256:54bc4431402846a72d1c1bdb69fae1148f74a59425144aa947fdf1c3506461f7",
        "sha256:836697a5be715699e7796022cc1b0496ac409748a8e2b6fd8e23b18b12f5b764",
    ),
    "system-update-quickstart": (
        "acryldata/datahub-upgrade:v1.7.0@"
        "sha256:21e77ad964be64b2b5a7f74c9685897ed79e8854995242eaa5e5c426395b88c0",
        "sha256:985c0358c1c05a7966c10af1cf8fcfdc7aaea97abfbb64198981e7ce96926325",
    ),
    "kafka-broker": (
        "confluentinc/cp-kafka:8.2.2@"
        "sha256:8e01c0305844d6c05bfb8e86479f5f363bb6a53497625395943a9da780de67ce",
        "sha256:1c45591457f48e2b44ee4a5bbad288599651e5bbd6cd4410d8eac03003d57c01",
    ),
    "mysql": (
        "mysql:8.2@sha256:212fe73edca5df6ff14826d5eb975c914bfb91f82a2e923f9050568f99525da1",
        "sha256:5ba9d31938cfbfbcd6b29977181cfc246ce3f4b4923efc2af89c028d872fcc41",
    ),
    "opensearch": (
        "opensearchproject/opensearch:2.19.3@"
        "sha256:e96cc6ae1500a073d973c0906f30f7cf4d9c461f32f855f9242a2da933660cdd",
        "sha256:acc1f30665727eb5d186fd15f40079615e1c9a0ee9fd3db3a8180f41d9982f51",
    ),
}


def _load_compose_override() -> dict[str, Any]:
    value = yaml.load(_COMPOSE_OVERRIDE_PATH.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert isinstance(value, dict)
    return value


def _load_gate() -> ModuleType:
    spec = importlib.util.spec_from_file_location("streamt_test_datahub_gms_gate", _GATE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


def _proposal(
    entity_type: str, urn: str, aspect_name: str, value: dict[str, Any]
) -> dict[str, Any]:
    return {
        "aspect": {"json": value},
        "aspectName": aspect_name,
        "changeType": "UPSERT",
        "entityType": entity_type,
        "entityUrn": urn,
    }


def _artifact_records(variant: Any) -> list[dict[str, Any]]:
    flow = "urn:li:dataFlow:(streamt,payments-gms-gate,PROD)"
    kafka_prefix = "main." if variant.kafka_instance is not None else ""
    raw = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{kafka_prefix}Orders.Raw,PROD)"
    enriched = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{kafka_prefix}orders.enriched,PROD)"
    plain = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{kafka_prefix}orders.plain,PROD)"
    public = (
        "urn:li:dataset:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest.orders%28public%29,PROD)"
    )
    records = [
        _proposal(
            "dataFlow",
            flow,
            "dataFlowInfo",
            {
                "customProperties": {},
                "description": "Deterministic DataHub GMS acceptance fixture",
                "env": "PROD",
                "name": "payments-gms-gate",
            },
        )
    ]
    datasets = (
        (public, "public_orders", {"streamt.contract.status": "declared"}),
        (raw, "raw_orders", {}),
        (enriched, "enriched_orders", {"streamt.contract.status": "enforced"}),
        (plain, "plain_topic", {}),
    )
    for urn, name, custom_properties in datasets:
        records.append(
            _proposal(
                "dataset",
                urn,
                "datasetProperties",
                {
                    "customProperties": custom_properties,
                    "description": name,
                    "name": name,
                    "tags": [],
                },
            )
        )
        if urn == public:
            records.append(
                _proposal(
                    "dataset",
                    urn,
                    "dataPlatformInstance",
                    {
                        "instance": (
                            "urn:li:dataPlatformInstance:(urn:li:dataPlatform:"
                            "conduktor-gateway,edge%2Cwest)"
                        ),
                        "platform": "urn:li:dataPlatform:conduktor-gateway",
                    },
                )
            )
        elif variant.kafka_instance is not None:
            records.append(
                _proposal(
                    "dataset",
                    urn,
                    "dataPlatformInstance",
                    {
                        "instance": "urn:li:dataPlatformInstance:(urn:li:dataPlatform:kafka,main)",
                        "platform": "urn:li:dataPlatform:kafka",
                    },
                )
            )
    jobs = (
        ("enriched_orders", "flink", raw, enriched),
        ("public_orders", "gateway", enriched, public),
        ("warehouse_sink", "connect", public, None),
    )
    for name, kind, input_urn, output_urn in jobs:
        urn = f"urn:li:dataJob:({flow},{name})"
        records.extend(
            (
                _proposal(
                    "dataJob",
                    urn,
                    "dataJobInfo",
                    {
                        "customProperties": {},
                        "description": name,
                        "env": "PROD",
                        "flowUrn": flow,
                        "name": name,
                        "type": {"string": kind},
                    },
                ),
                _proposal(
                    "dataJob",
                    urn,
                    "dataJobInputOutput",
                    {
                        "inputDatasetEdges": [{"destinationUrn": input_urn}],
                        "inputDatasets": [],
                        "outputDatasetEdges": (
                            [{"destinationUrn": output_urn}] if output_urn is not None else []
                        ),
                        "outputDatasets": [],
                    },
                ),
            )
        )
    return records


def _write_artifact(path: Path, variant: Any) -> None:
    path.write_text(
        json.dumps(_artifact_records(variant), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    "url",
    ["http://datahub-gms:8080", "http://127.0.0.1:49153"],
)
def test_gms_url_accepts_only_reviewed_targets(url: str) -> None:
    assert gate.validate_gms_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "https://datahub-gms:8080",
        "http://datahub-gms:8081",
        "http://localhost:8080",
        "http://127.0.0.2:8080",
        "http://127.0.0.1:8080/",
        "http://127.0.0.1:8080?token=secret",
        "http://user@127.0.0.1:8080",
        "http://10.0.0.2:8080",
        "http://0.0.0.0:8080",
        "http://8.8.8.8:8080",
    ],
)
def test_gms_url_rejects_every_other_surface(url: str) -> None:
    with pytest.raises(gate.GateError, match="isolated gate boundary"):
        gate.validate_gms_url(url)


def test_environment_is_allowlisted_and_has_no_profile_or_proxy_secrets(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "secret")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid")

    environment = gate._minimal_environment(tmp_path, "http://127.0.0.1:8080")

    assert environment["DATAHUB_GMS_URL"] == "http://127.0.0.1:8080"
    assert environment["DATAHUB_SKIP_CONFIG"] == "true"
    assert environment["DATAHUB_TELEMETRY_ENABLED"] == "false"
    assert environment["NO_PROXY"] == "127.0.0.1"
    assert "DATAHUB_GMS_TOKEN" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    assert "HTTPS_PROXY" not in environment


def test_server_config_requires_exact_version_and_full_commit() -> None:
    exact = {
        "versions": {
            "acryldata/datahub": {
                "version": "v1.7.0",
                "commit": gate.SERVER_COMMIT,
            }
        }
    }
    assert gate.parse_server_config(exact) == ("v1.7.0", gate.SERVER_COMMIT)

    for invalid in (
        {"versions": {"acryldata/datahub": {"version": "1.7.0", "commit": gate.SERVER_COMMIT}}},
        {"versions": {"acryldata/datahub": {"version": "v1.7.0", "commit": "7f81ccb"}}},
        {"versions": {"linkedin/datahub": exact["versions"]["acryldata/datahub"]}},
    ):
        with pytest.raises(gate.GateError, match=r"exact v1\.7\.0"):
            gate.parse_server_config(invalid)


def test_cli_version_and_ingestion_report_parsers_are_exact() -> None:
    assert gate.parse_cli_version(b"DataHub CLI version: 1.7.0\nModels: bundled\n") == "1.7.0"
    assert (
        gate.parse_ingestion_report(
            b"Starting ingestion...\nPipeline finished successfully; produced 15 events in 1 second.\n",
            15,
        )
        == 15
    )
    with pytest.raises(gate.GateError, match="not exactly"):
        gate.parse_cli_version(b"DataHub CLI version: 1.7.1\n")
    with pytest.raises(gate.GateError, match="exact success"):
        gate.parse_ingestion_report(
            b"Pipeline finished with at least 1 warnings; produced 15 events in 1 second.\n",
            15,
        )
    with pytest.raises(gate.GateError, match="count differs"):
        gate.parse_ingestion_report(
            b"Pipeline finished successfully; produced 14 events in 1 second.\n", 15
        )


def test_commands_use_exact_executables_and_cover_both_variants(tmp_path: Path) -> None:
    streamt = Path("/opt/gate/bin/streamt")
    datahub = Path("/opt/datahub/bin/datahub")
    artifact = tmp_path / "artifact.json"
    commands = [
        gate.streamt_command(streamt, tmp_path, artifact, variant) for variant in gate.VARIANTS
    ]
    assert commands[0][0] == commands[1][0] == str(streamt)
    assert "--kafka-platform-instance" in commands[0]
    assert "--kafka-platform-instance" not in commands[1]
    assert gate.datahub_ingest_command(datahub, artifact) == [
        str(datahub),
        "ingest",
        "mcps",
        str(artifact),
    ]
    assert gate._selected_variants("all") == gate.VARIANTS


@pytest.mark.parametrize("variant", gate.VARIANTS, ids=lambda value: value.name)
def test_artifact_contract_covers_every_aspect_and_exact_sink_edges(
    tmp_path: Path, variant: Any
) -> None:
    artifact = tmp_path / "artifact.json"
    _write_artifact(artifact, variant)

    contract = gate.load_artifact_contract(artifact, variant)

    assert contract.proposal_count == variant.proposal_count
    assert contract.aspect_count == variant.proposal_count
    assert contract.relationship_count == 5
    sink_edges = next(
        by_type
        for urn, by_type in contract.expected_relationships.items()
        if urn.endswith(",warehouse_sink)")
    )
    assert len(sink_edges["Consumes"]) == 1
    assert sink_edges["Produces"] == frozenset()


def test_artifact_privacy_sentinel_is_fatal(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.json"
    variant = gate.VARIANTS[0]
    records = _artifact_records(variant)
    records[0]["aspect"]["json"]["description"] = "gms-gate-connector-token-omitted"
    artifact.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with pytest.raises(gate.GateError, match="fixture-private"):
        gate.load_artifact_contract(artifact, variant)


def test_entities_v2_parser_compares_exact_semantic_value() -> None:
    urn = "urn:li:dataJob:(urn:li:dataFlow:(streamt,gate,PROD),job)"
    expected = {"dataJobInfo": {"name": "job", "customProperties": {}}}
    response = {
        "urn": urn,
        "aspects": {
            "dataJobKey": {
                "name": "dataJobKey",
                "version": 0,
                "value": {"jobId": "job"},
            },
            "dataJobInfo": {
                "name": "dataJobInfo",
                "version": 0,
                "value": expected["dataJobInfo"],
            },
        },
    }
    assert gate.parse_entity_response(response, urn, expected) == expected
    response["aspects"]["dataJobInfo"]["value"] = {"name": "normalized"}
    with pytest.raises(gate.PollPendingError):
        gate.parse_entity_response(response, urn, expected)

    response["aspects"]["unexpected"] = {
        "name": "unexpected",
        "version": 0,
        "value": {},
    }
    with pytest.raises(gate.GateError, match="unexpected aspect"):
        gate.parse_entity_response(response, urn, expected)


def test_relationship_parser_uses_exact_v170_shape_and_empty_output() -> None:
    job = "urn:li:dataJob:(urn:li:dataFlow:(streamt,gate,PROD),sink)"
    dataset = "urn:li:dataset:(urn:li:dataPlatform:kafka,gate.input,PROD)"
    page = gate.parse_relationship_page(
        {
            "relationships": [{"type": "Consumes", "entity": dataset}],
            "start": 0,
            "count": 1,
            "total": 1,
        },
        source_urn=job,
        relationship_type="Consumes",
        requested_start=0,
    )
    assert page.destinations == (dataset,)
    assert page.next_start is None
    empty = gate.parse_relationship_page(
        {"relationships": [], "start": 0, "count": 0, "total": 0},
        source_urn=job,
        relationship_type="Produces",
        requested_start=0,
    )
    assert empty.destinations == ()
    legacy_shape = {
        "elements": [{"source": job, "type": "Consumes", "destination": dataset}],
        "paging": {"start": 0, "count": 1, "total": 1},
    }
    with pytest.raises(gate.GateError, match="shape"):
        gate.parse_relationship_page(
            legacy_shape,
            source_urn=job,
            relationship_type="Consumes",
            requested_start=0,
        )


def test_relationship_query_uses_v170_types_parameter() -> None:
    job = "urn:li:dataJob:(urn:li:dataFlow:(streamt,gate,PROD),job)"
    path = gate._relationship_path(job, "Consumes", 0)
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
    assert query["types"] == ["Consumes"]
    assert "relationshipTypes" not in query
    assert query["direction"] == ["OUTGOING"]
    assert query["urn"] == [job]


class _FakeResponse:
    def __init__(self, value: Any) -> None:
        self.payload = json.dumps(value).encode()

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def getcode(self) -> int:
        return 200

    def read(self, size: int) -> bytes:
        return self.payload[:size]


class _FakeOpener:
    def __init__(self, value: Any) -> None:
        self.value = value
        self.requests: list[tuple[Any, float]] = []

    def open(self, request: Any, timeout: float) -> _FakeResponse:
        self.requests.append((request, timeout))
        return _FakeResponse(self.value)


def test_http_client_is_get_only_bounded_and_uses_no_credentials() -> None:
    opener = _FakeOpener({"versions": {}})
    client = gate.JsonHttpClient("http://127.0.0.1:8080", opener=opener)
    assert client.get("/config") == {"versions": {}}
    request, timeout = opener.requests[0]
    assert request.get_method() == "GET"
    assert request.full_url == "http://127.0.0.1:8080/config"
    assert request.get_header("Authorization") is None
    assert request.get_header("X-restli-protocol-version") == "2.0.0"
    assert timeout == gate.REQUEST_TIMEOUT_SECONDS

    client.get("/relationships?types=Consumes", restli_v2=False)
    relationship_request, _timeout = opener.requests[1]
    assert relationship_request.get_header("X-restli-protocol-version") is None


def test_poll_discards_partial_snapshot_before_success() -> None:
    attempts: list[dict[str, int]] = []

    def reader() -> dict[str, int]:
        current = {"first": len(attempts)}
        attempts.append(current)
        if len(attempts) == 1:
            current["partial"] = 1
            raise gate.PollPendingError("not exact")
        return current

    ticks = iter((0.0, 0.0, 0.1))
    result = gate.poll_exact(
        reader,
        timeout_seconds=1,
        timeout_code="timeout",
        monotonic=lambda: next(ticks),
        sleep=lambda _seconds: None,
    )
    assert result == {"first": 1}
    assert attempts[0] == {"first": 0, "partial": 1}


def test_harness_contains_no_datahub_sdk_import_or_mutating_http_method() -> None:
    source = _GATE_PATH.read_text(encoding="utf-8")
    assert "import datahub" not in source
    assert "from datahub" not in source
    assert 'method="GET"' in source
    assert 'method="DELETE"' not in source
    assert "datahub delete" not in source
    assert "rollback" not in source
    assert "subprocess.run" not in source


def test_fixture_is_the_only_tracked_project_input() -> None:
    fixture = _ROOT / "tests" / "integration" / "datahub" / "v1.7.0" / "stream_project.yml"
    assert fixture == gate._FIXTURE
    text = fixture.read_text(encoding="utf-8")
    assert "materialized: virtual_topic" in text
    assert "materialized: sink" in text
    assert "owner:" in text
    assert "tags:" in text
    assert "password: gms-gate-connection-secret-omitted" in text


def test_topology_lock_pins_exact_upstream_and_linux_amd64_images() -> None:
    lock = json.loads(_IMAGE_LOCK_PATH.read_text(encoding="utf-8"))

    assert lock["schema_version"] == 1
    assert lock["datahub"] == {
        "release": "v1.7.0",
        "commit": gate.SERVER_COMMIT,
    }
    assert lock["upstream_compose"] == {
        "url": (
            "https://raw.githubusercontent.com/datahub-project/datahub/"
            f"{gate.SERVER_COMMIT}/docker/quickstart/"
            "docker-compose.quickstart-profile.yml"
        ),
        "sha256": "ec476d12f6f278c50d657a617357a050510565ef00b570a69cbe9123a932a7b7",
    }
    assert lock["compose_minimum_version"] == "2.24.4"
    assert lock["platform"] == "linux/amd64"
    assert lock["target_service"] == "datahub-gms-quickstart"
    assert set(lock["images"]) == set(_EXPECTED_TOPOLOGY_IMAGES)

    for service, (reference, manifest_digest) in _EXPECTED_TOPOLOGY_IMAGES.items():
        image = lock["images"][service]
        assert image["reference"] == reference
        assert image["index_digest"] == reference.rsplit("@", 1)[1]
        assert image["linux_amd64_manifest_digest"] == manifest_digest
        assert reference == f"{image['repository']}:{image['tag']}@{image['index_digest']}"


def test_topology_overlay_contains_only_the_reviewed_target_dependency_closure() -> None:
    overlay = _load_compose_override()
    services = overlay["services"]

    assert set(services) == set(_EXPECTED_TOPOLOGY_IMAGES)
    assert "datahub-actions-quickstart" not in services
    assert "frontend-quickstart" not in services
    assert not any("oracle" in name or "ingestion" in name for name in services)
    assert overlay["name"] == "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}"

    lock = json.loads(_IMAGE_LOCK_PATH.read_text(encoding="utf-8"))
    for service, config in services.items():
        assert config["image"] == lock["images"][service]["reference"]
        assert config["platform"] == "linux/amd64"
        assert config["pull_policy"] == "never"
        assert config["restart"] == "no"
        assert config["mem_limit"] in {"1g", "2g"}
        assert "profiles" not in config
        assert "container_name" not in config
        assert "privileged" not in config
        assert "network_mode" not in config


def test_topology_overlay_replaces_ports_and_host_mounts() -> None:
    overlay = _load_compose_override()
    services = overlay["services"]
    source = _COMPOSE_OVERRIDE_PATH.read_text(encoding="utf-8")

    assert services["datahub-gms-quickstart"]["ports"] == [
        "127.0.0.1:${DATAHUB_MAPPED_GMS_PORT:?set DATAHUB_MAPPED_GMS_PORT}:8080"
    ]
    assert services["datahub-gms-quickstart"]["volumes"] == []
    assert services["system-update-quickstart"]["volumes"] == []
    for service in ("kafka-broker", "mysql", "opensearch"):
        assert services[service]["ports"] == []

    assert services["kafka-broker"]["volumes"] == ["broker:/var/lib/kafka/data"]
    assert services["mysql"]["volumes"] == ["mysqldata:/var/lib/mysql"]
    assert services["opensearch"]["volumes"] == ["osdata:/usr/share/opensearch/data"]
    assert source.count("ports: !override") == 1
    assert source.count("ports: !reset []") == 3
    assert source.count("volumes: !override") == 3
    assert source.count("volumes: !reset []") == 2
    assert "${HOME}" not in source
    assert "/var/run/docker.sock" not in source


def test_topology_overlay_uses_unique_internal_storage_and_disables_usage() -> None:
    overlay = _load_compose_override()
    gms = overlay["services"]["datahub-gms-quickstart"]

    assert overlay["networks"] == {
        "default": {
            "name": "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}_network",
            "internal": "true",
        },
        "bootstrap": {
            "name": "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}_bootstrap",
            "internal": "false",
        },
    }
    for service in ("datahub-gms-quickstart", "system-update-quickstart"):
        assert overlay["services"][service]["networks"] == {
            "default": "null",
            "bootstrap": "null",
        }
    assert all(
        "networks" not in config
        for service, config in overlay["services"].items()
        if service not in {"datahub-gms-quickstart", "system-update-quickstart"}
    )
    assert overlay["volumes"] == {
        "broker": {"name": "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}_broker"},
        "mysqldata": {"name": "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}_mysqldata"},
        "osdata": {"name": "${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}_osdata"},
    }
    assert gms["environment"]["DATAHUB_TELEMETRY_ENABLED"] == "false"
    assert gms["environment"]["USAGE_AGGREGATION_ENABLED"] == "false"
    assert gms["healthcheck"] == {
        "test": ["CMD-SHELL", "curl -sS --fail http://datahub-gms:8080/health"],
        "interval": "2s",
        "timeout": "5s",
        "retries": "45",
        "start_period": "90s",
    }
    assert "${DATAHUB_MYSQL_PASSWORD:?set DATAHUB_MYSQL_PASSWORD}" in {
        value
        for config in overlay["services"].values()
        for value in config.get("environment", {}).values()
    }


def test_ci_runs_both_real_gms_variants_with_bounded_cleanup() -> None:
    workflow = yaml.safe_load(_CI_WORKFLOW_PATH.read_text(encoding="utf-8"))
    job = workflow["jobs"]["datahub-gms-v170"]
    source = _CI_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert job["needs"] == ["package", "datahub-release-oracle"]
    assert job["permissions"] == {"contents": "read"}
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["timeout-minutes"] == 45
    assert "acryl-datahub==1.7.0" in source
    assert "ec476d12f6f278c50d657a617357a050510565ef00b570a69cbe9123a932a7b7" in source
    for reference, _manifest_digest in _EXPECTED_TOPOLOGY_IMAGES.values():
        assert reference in source
    assert "run_variant with-kafka-instance" in source
    assert "run_variant without-kafka-instance" in source
    assert "port datahub-gms-quickstart 8080" in source
    assert '"127.0.0.1:${port}"' in source
    assert "down --volumes --remove-orphans" in source
    assert "jq -s 'map({Service, State, Health, ExitCode})'" in source
    assert source.count("--profile quickstart-backend") >= 2
    assert "label=com.docker.compose.project=${project}" in source
    assert "docker system prune" not in source
    assert "docker volume prune" not in source
    assert "${{ secrets." not in source

    steps = {step["name"]: step for step in job["steps"]}
    assert steps["Ensure exact DataHub projects are removed"]["if"] == "always()"
    upload = steps["Upload bounded DataHub GMS evidence"]
    assert upload["if"] == "always()"
    assert upload["with"]["retention-days"] == 14
