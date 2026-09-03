"""Keep the published DataHub project example executable and offline-valid."""

from __future__ import annotations

import builtins
import json
import re
import socket
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from streamt.cli import main

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATAHUB_REFERENCE = PROJECT_ROOT / "docs" / "reference" / "datahub-catalog.md"
DATAHUB_EXAMPLE_RE = re.compile(
    r"```yaml\n(?P<project># streamt:datahub-example\n.*?)```",
    re.DOTALL,
)

CATALOG_ID = "payments-prod"
FABRIC = "PROD"
KAFKA_INSTANCE = "main"
GATEWAY_PLATFORM = "conduktor-gateway"
GATEWAY_INSTANCE = "edge"

FLOW_URN = "urn:li:dataFlow:(streamt,payments-prod,PROD)"
SOURCE_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,main.Payments.Raw.v1,PROD)"
CLEAN_URN = "urn:li:dataset:(urn:li:dataPlatform:kafka,main.payments.clean.v1,PROD)"
PUBLIC_URN = "urn:li:dataset:(urn:li:dataPlatform:conduktor-gateway,edge.payments.public.v1,PROD)"


def _job_urn(name: str) -> str:
    return f"urn:li:dataJob:({FLOW_URN},{name})"


def test_published_project_exports_representative_datahub_mcp_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference = DATAHUB_REFERENCE.read_text(encoding="utf-8")
    matches = list(DATAHUB_EXAMPLE_RE.finditer(reference))
    assert len(matches) == 1, "published DataHub project example must be unique"
    (tmp_path / "stream_project.yml").write_text(
        matches[0].group("project"),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATAHUB_DOC_KAFKA_PASSWORD", "KAFKA_PASSWORD_MUST_NOT_APPEAR")
    monkeypatch.setenv(
        "DATAHUB_DOC_CONNECTOR_PASSWORD",
        "CONNECTOR_PASSWORD_MUST_NOT_APPEAR",
    )

    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("published DataHub example touched external infrastructure")

    real_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "datahub" or name.startswith("datahub."):
            raise AssertionError("published DataHub example imported the runtime SDK")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(socket, "gethostbyname", forbidden)
    monkeypatch.setattr(socket, "gethostbyname_ex", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    monkeypatch.setattr(socket.socket, "sendto", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(subprocess, "call", forbidden)
    monkeypatch.setattr(subprocess, "check_call", forbidden)
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    monkeypatch.setattr(builtins, "__import__", guarded_import)

    result = CliRunner().invoke(
        main,
        [
            "--output",
            "json",
            "docs",
            "datahub",
            "--project-dir",
            str(tmp_path),
            "--catalog-id",
            CATALOG_ID,
            "--fabric",
            FABRIC,
            "--kafka-platform-instance",
            KAFKA_INSTANCE,
            "--gateway-platform-id",
            GATEWAY_PLATFORM,
            "--gateway-platform-instance",
            GATEWAY_INSTANCE,
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    envelope = json.loads(result.stdout)
    proposals = envelope["data"]["proposals"]
    by_pair = {
        (proposal["entityUrn"], proposal["aspectName"]): proposal["aspect"]["json"]
        for proposal in proposals
    }

    assert (FLOW_URN, "dataFlowInfo") in by_pair
    assert (SOURCE_URN, "datasetProperties") in by_pair
    assert (CLEAN_URN, "datasetProperties") in by_pair
    assert (PUBLIC_URN, "datasetProperties") in by_pair
    assert by_pair[(CLEAN_URN, "datasetProperties")]["customProperties"] == {
        "streamt.contract.status": "enforced"
    }

    clean_edges = by_pair[(_job_urn("payments_clean"), "dataJobInputOutput")]
    public_edges = by_pair[(_job_urn("payments_public"), "dataJobInputOutput")]
    sink_edges = by_pair[(_job_urn("warehouse_archive"), "dataJobInputOutput")]
    assert clean_edges == {
        "inputDatasets": [],
        "outputDatasets": [],
        "inputDatasetEdges": [{"destinationUrn": SOURCE_URN}],
        "outputDatasetEdges": [{"destinationUrn": CLEAN_URN}],
    }
    assert public_edges["inputDatasetEdges"] == [{"destinationUrn": CLEAN_URN}]
    assert public_edges["outputDatasetEdges"] == [{"destinationUrn": PUBLIC_URN}]
    assert sink_edges["inputDatasetEdges"] == [{"destinationUrn": CLEAN_URN}]
    assert sink_edges["outputDatasetEdges"] == []
    assert SOURCE_URN not in json.dumps(public_edges) + json.dumps(sink_edges)

    assert all(proposal["changeType"] == "UPSERT" for proposal in proposals)
    assert Counter(warning["code"] for warning in envelope["warnings"]) == {
        "W115_DATAHUB_SINK_OUTPUT_OMITTED": 1,
        "W116_DATAHUB_EXPOSURE_OMITTED": 1,
        "W117_DATAHUB_TAGS_OMITTED": 2,
        "W118_DATAHUB_OWNER_OMITTED": 2,
    }

    rendered = result.stdout + result.stderr
    for private_value in (
        "kafka.private.example:9092",
        "gateway.private.example:6969",
        "DATAHUB_DOC_KAFKA_PASSWORD",
        "KAFKA_PASSWORD_MUST_NOT_APPEAR",
        "DATAHUB_DOC_CONNECTOR_PASSWORD",
        "CONNECTOR_PASSWORD_MUST_NOT_APPEAR",
        "jdbc:postgresql://warehouse.private.example/payments",
        "connection.url",
        "jdbc-sink",
        "2.3.0",
        "payments-platform",
        "restricted",
        "curated",
        "payment_id",
        "confidential",
        "finance_dashboard",
        "Internal finance dashboard definition",
        str(tmp_path),
    ):
        assert private_value not in rendered
    assert {path.name for path in tmp_path.iterdir()} == {"stream_project.yml"}
