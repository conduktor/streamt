#!/usr/bin/env python3
"""Standalone DataHub v1.7.0 GMS acceptance gate.

This test-only program deliberately imports neither streamt nor DataHub.  It
drives exact, caller-supplied executables and uses only the Python standard
library for server read-back.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SERVER_VERSION = "v1.7.0"
SERVER_COMMIT = "7f81ccbfe27b9acc947f5f600fcf9ddb72138a80"
CLI_VERSION = "1.7.0"
INTERNAL_GMS_URL = "http://datahub-gms:8080"
REQUEST_TIMEOUT_SECONDS = 10.0
ASPECT_POLL_TIMEOUT_SECONDS = 30.0
RELATIONSHIP_POLL_TIMEOUT_SECONDS = 90.0
POLL_INTERVAL_SECONDS = 2.0
PROCESS_TIMEOUT_SECONDS = 120.0
MAX_HTTP_BYTES = 1024 * 1024
MAX_PROCESS_OUTPUT_BYTES = 512 * 1024
MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
RELATIONSHIP_PAGE_SIZE = 100
MAX_RELATIONSHIP_PAGES = 10

_FIXTURE = Path(__file__).with_name("v1.7.0") / "stream_project.yml"
_ANSI_RE = re.compile(rb"\x1b\[[0-?]*[ -/]*[@-~]")
_CLI_VERSION_RE = re.compile(r"^DataHub CLI version: ([^\s]+)$", re.MULTILINE)
_INGESTION_SUMMARY_RE = re.compile(
    r"^Pipeline finished (successfully|with at least \d+ (?:warnings|failures)); "
    r"produced (\d+) events in [^\r\n]+\.$",
    re.MULTILINE,
)
_ROOT_KEYS = {"aspect", "aspectName", "changeType", "entityType", "entityUrn"}
_ALLOWED_ASPECTS = {
    ("dataFlow", "dataFlowInfo"),
    ("dataset", "datasetProperties"),
    ("dataset", "dataPlatformInstance"),
    ("dataJob", "dataJobInfo"),
    ("dataJob", "dataJobInputOutput"),
}
_ENTITY_PREFIXES = {
    "dataFlow": "urn:li:dataFlow:",
    "dataset": "urn:li:dataset:",
    "dataJob": "urn:li:dataJob:",
}
_SERVER_KEY_ASPECTS = {
    "urn:li:dataFlow:": "dataFlowKey",
    "urn:li:dataset:": "datasetKey",
    "urn:li:dataJob:": "dataJobKey",
}
_PRIVACY_SENTINELS = (
    "broker-not-contacted.invalid",
    "gateway-not-contacted.invalid",
    "gms-gate-connection-secret-omitted",
    "gms-gate-source-owner-omitted",
    "gms-gate-source-tag-omitted",
    "gms-gate-source-column-omitted",
    "gms-gate-topic-tag-omitted",
    "gms-gate-compiled-sql-omitted",
    "gms-gate-flink-owner-omitted",
    "gms-gate-flink-tag-omitted",
    "gms-gate-contract-column-omitted",
    "gms-gate-gateway-owner-omitted",
    "gms-gate-gateway-tag-omitted",
    "gms-gate-connector-omitted",
    "gms-gate-connector-token-omitted",
    "gms-gate-sink-owner-omitted",
    "gms-gate-sink-tag-omitted",
)


class GateError(RuntimeError):
    """A secret-neutral, bounded gate failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class PollPendingError(RuntimeError):
    """A well-formed read-back result that is not exact yet."""


class RetryableHttpError(RuntimeError):
    """A connection or server error that may clear inside a polling window."""


@dataclass(frozen=True)
class Variant:
    name: str
    kafka_instance: str | None
    proposal_count: int
    platform_instance_aspects: int


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    output: bytes


@dataclass(frozen=True)
class RelationshipPage:
    destinations: tuple[str, ...]
    total: int
    next_start: int | None


@dataclass(frozen=True)
class ArtifactContract:
    proposal_count: int
    expected_aspects: Mapping[str, Mapping[str, Any]]
    expected_relationships: Mapping[str, Mapping[str, frozenset[str]]]
    aspect_count: int
    relationship_count: int


VARIANTS = (
    Variant("with-kafka-instance", "main", 15, 4),
    Variant("without-kafka-instance", None, 12, 1),
)


def _require(condition: bool, code: str, message: str) -> None:
    if not condition:
        raise GateError(code, message)


def validate_gms_url(raw_url: str) -> str:
    """Allow only the reviewed internal GMS name or literal IPv4 loopback."""
    try:
        parsed = urllib.parse.urlsplit(raw_url)
        port = parsed.port
    except ValueError as error:
        raise GateError(
            "unsafe_gms_url", "GMS URL is outside the isolated gate boundary"
        ) from error

    _require(
        parsed.scheme == "http"
        and parsed.username is None
        and parsed.password is None
        and parsed.query == ""
        and parsed.fragment == ""
        and parsed.path == ""
        and port is not None,
        "unsafe_gms_url",
        "GMS URL is outside the isolated gate boundary",
    )
    if raw_url == INTERNAL_GMS_URL:
        return raw_url
    _require(
        parsed.hostname == "127.0.0.1"
        and parsed.netloc == f"127.0.0.1:{port}"
        and 1 <= port <= 65535,
        "unsafe_gms_url",
        "GMS URL is outside the isolated gate boundary",
    )
    return raw_url


def validate_executable(raw_path: str, expected_name: str) -> Path:
    """Resolve an exact absolute executable without consulting PATH."""
    path = Path(raw_path)
    _require(
        path.is_absolute() and path.name == expected_name,
        "unsafe_executable",
        f"{expected_name} executable must be an exact absolute path",
    )
    try:
        resolved = path.resolve(strict=True)
    except OSError as error:
        raise GateError(
            "unsafe_executable", f"{expected_name} executable is unavailable"
        ) from error
    _require(
        resolved.is_file() and os.access(resolved, os.X_OK),
        "unsafe_executable",
        f"{expected_name} executable is unavailable",
    )
    return resolved


def _read_limited(path: Path, limit: int, code: str) -> bytes:
    try:
        with path.open("rb") as stream:
            value = stream.read(limit + 1)
    except OSError as error:
        raise GateError(code, "A required gate input could not be read") from error
    _require(len(value) <= limit, code, "A required gate input exceeds its size bound")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(_read_limited(path, MAX_ARTIFACT_BYTES, "input_invalid")).hexdigest()


def _minimal_environment(home: Path, gms_url: str | None = None) -> dict[str, str]:
    """Build an allowlisted environment without inherited credentials or profiles."""
    environment: dict[str, str] = {}
    for key in ("LANG", "LC_ALL", "TZ", "PATH", "SSL_CERT_FILE", "SSL_CERT_DIR"):
        if key in os.environ:
            environment[key] = os.environ[key]
    environment.update(
        {
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "PYTHONNOUSERSITE": "1",
            "DATAHUB_DATASET_URN_TO_LOWER": "false",
            "DATAHUB_TELEMETRY_ENABLED": "false",
            "DATAHUB_SKIP_CONFIG": "true",
        }
    )
    if gms_url is not None:
        host = urllib.parse.urlsplit(gms_url).hostname
        assert host is not None
        environment["DATAHUB_GMS_URL"] = gms_url
        environment["NO_PROXY"] = host
        environment["no_proxy"] = host
    return environment


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, AttributeError):
        try:
            process.kill()
        except OSError:
            pass


def run_process(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: float = PROCESS_TIMEOUT_SECONDS,
) -> ProcessResult:
    """Run one exact command with a hard timeout and a hard capture bound."""
    _require(
        bool(command) and Path(command[0]).is_absolute(),
        "unsafe_executable",
        "Gate subprocess executable must be absolute",
    )
    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as error:
        raise GateError("subprocess_failed", "A gate subprocess could not start") from error

    captured = bytearray()
    overflow = threading.Event()

    def drain() -> None:
        assert process.stdout is not None
        while True:
            chunk = process.stdout.read(64 * 1024)
            if not chunk:
                return
            remaining = MAX_PROCESS_OUTPUT_BYTES - len(captured)
            if remaining > 0:
                captured.extend(chunk[:remaining])
            if len(chunk) > remaining:
                overflow.set()
                _terminate_process(process)
                return

    reader = threading.Thread(target=drain, name="gms-gate-output", daemon=True)
    reader.start()
    try:
        returncode = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        _terminate_process(process)
        process.wait(timeout=5)
        reader.join(timeout=5)
        raise GateError("subprocess_timeout", "A gate subprocess exceeded its timeout") from error
    reader.join(timeout=5)
    if reader.is_alive():
        _terminate_process(process)
        raise GateError("subprocess_failed", "A gate subprocess output reader did not stop")
    _require(
        not overflow.is_set(),
        "subprocess_output_limit",
        "A gate subprocess exceeded its output bound",
    )
    return ProcessResult(returncode=returncode, output=bytes(captured))


def _strip_ansi(output: bytes) -> str:
    return _ANSI_RE.sub(b"", output).decode("utf-8", errors="replace").replace("\r", "")


def parse_cli_version(output: bytes) -> str:
    matches = _CLI_VERSION_RE.findall(_strip_ansi(output))
    _require(
        matches == [CLI_VERSION],
        "cli_version_mismatch",
        "DataHub CLI version is not exactly 1.7.0",
    )
    return CLI_VERSION


def parse_ingestion_report(output: bytes, expected_count: int) -> int:
    matches = _INGESTION_SUMMARY_RE.findall(_strip_ansi(output))
    _require(
        len(matches) == 1 and matches[0][0] == "successfully",
        "ingestion_report_invalid",
        "Official DataHub ingestion report is not an exact success",
    )
    written = int(matches[0][1])
    _require(
        written == expected_count,
        "ingestion_count_mismatch",
        "Official DataHub ingestion count differs from the artifact",
    )
    return written


def streamt_command(
    executable: Path, project_dir: Path, artifact: Path, variant: Variant
) -> list[str]:
    command = [
        str(executable),
        "docs",
        "datahub",
        "--catalog-id",
        "payments-gms-gate",
        "--fabric",
        "PROD",
        "--gateway-platform-id",
        "conduktor-gateway",
        "--gateway-platform-instance",
        "edge,west",
        "--project-dir",
        str(project_dir),
        "--output-file",
        str(artifact),
    ]
    if variant.kafka_instance is not None:
        command[7:7] = ["--kafka-platform-instance", variant.kafka_instance]
    return command


def datahub_version_command(executable: Path) -> list[str]:
    return [str(executable), "version"]


def datahub_ingest_command(executable: Path, artifact: Path) -> list[str]:
    return [str(executable), "ingest", "mcps", str(artifact)]


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise GateError("artifact_invalid", "Artifact contains a duplicate JSON key")
        value[key] = item
    return value


def _require_text(value: Any, code: str = "artifact_invalid") -> str:
    _require(isinstance(value, str) and bool(value), code, "Expected non-empty text is absent")
    return value


def _edge_destinations(value: Any, field: str) -> frozenset[str]:
    _require(isinstance(value, list), "artifact_invalid", "Lineage edge array is invalid")
    destinations: list[str] = []
    for edge in value:
        _require(
            isinstance(edge, dict) and set(edge) == {"destinationUrn"},
            "artifact_invalid",
            "Lineage edge shape is invalid",
        )
        destinations.append(_require_text(edge["destinationUrn"]))
    _require(
        len(destinations) == len(set(destinations)),
        "artifact_invalid",
        "Lineage edge array contains duplicates",
    )
    _require(
        destinations == sorted(destinations, key=lambda item: item.encode("utf-8")),
        "artifact_invalid",
        f"{field} is not bytewise sorted",
    )
    return frozenset(destinations)


def load_artifact_contract(path: Path, variant: Variant) -> ArtifactContract:
    raw = _read_limited(path, MAX_ARTIFACT_BYTES, "artifact_invalid")
    for sentinel in _PRIVACY_SENTINELS:
        _require(
            sentinel.encode() not in raw,
            "artifact_privacy_failure",
            "Generated artifact contains fixture-private metadata",
        )
    try:
        proposals = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise GateError("artifact_invalid", "Artifact is not strict UTF-8 JSON") from error
    canonical = (json.dumps(proposals, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    _require(raw == canonical, "artifact_invalid", "Artifact bytes are not canonical JSON")
    _require(
        isinstance(proposals, list) and len(proposals) == variant.proposal_count,
        "artifact_invalid",
        "Artifact proposal count differs from the variant contract",
    )

    expected: dict[str, dict[str, Any]] = {}
    entity_types: dict[str, str] = {}
    for proposal in proposals:
        _require(isinstance(proposal, dict), "artifact_invalid", "Artifact proposal is invalid")
        _require(
            set(proposal) == _ROOT_KEYS,
            "artifact_invalid",
            "Artifact proposal root shape is invalid",
        )
        entity_type = _require_text(proposal["entityType"])
        aspect_name = _require_text(proposal["aspectName"])
        urn = _require_text(proposal["entityUrn"])
        _require(
            (entity_type, aspect_name) in _ALLOWED_ASPECTS,
            "artifact_invalid",
            "Artifact contains an unsupported entity/aspect pair",
        )
        _require(
            proposal["changeType"] == "UPSERT",
            "artifact_invalid",
            "Artifact change type is not UPSERT",
        )
        _require(
            urn.startswith(_ENTITY_PREFIXES[entity_type]),
            "artifact_invalid",
            "Artifact entity type and URN differ",
        )
        aspect = proposal["aspect"]
        _require(
            isinstance(aspect, dict) and set(aspect) == {"json"},
            "artifact_invalid",
            "Artifact aspect wrapper is invalid",
        )
        payload = aspect["json"]
        _require(isinstance(payload, dict), "artifact_invalid", "Artifact aspect is not an object")
        _require(
            aspect_name not in expected.setdefault(urn, {}),
            "artifact_invalid",
            "Artifact repeats an entity/aspect pair",
        )
        _require(
            entity_types.get(urn, entity_type) == entity_type,
            "artifact_invalid",
            "One artifact URN has multiple entity types",
        )
        entity_types[urn] = entity_type
        expected[urn][aspect_name] = payload

    counts = {
        kind: sum(value == kind for value in entity_types.values()) for kind in _ENTITY_PREFIXES
    }
    _require(
        counts == {"dataFlow": 1, "dataset": 4, "dataJob": 3},
        "artifact_invalid",
        "Artifact entity coverage differs from the gate fixture",
    )
    aspect_names = [name for aspects in expected.values() for name in aspects]
    _require(
        aspect_names.count("dataFlowInfo") == 1
        and aspect_names.count("datasetProperties") == 4
        and aspect_names.count("dataPlatformInstance") == variant.platform_instance_aspects
        and aspect_names.count("dataJobInfo") == 3
        and aspect_names.count("dataJobInputOutput") == 3,
        "artifact_invalid",
        "Artifact aspect coverage differs from the gate fixture",
    )

    dataset_urns = {urn for urn, kind in entity_types.items() if kind == "dataset"}
    kafka_urns = {urn for urn in dataset_urns if "urn:li:dataPlatform:kafka," in urn}
    gateway_urns = dataset_urns - kafka_urns
    _require(
        len(kafka_urns) == 3 and len(gateway_urns) == 1,
        "artifact_invalid",
        "Artifact Dataset platform coverage differs",
    )
    for urn in dataset_urns:
        _require(
            "datasetProperties" in expected[urn],
            "artifact_invalid",
            "Dataset properties are missing",
        )
        properties = expected[urn]["datasetProperties"]
        _require(
            properties.get("tags") == [],
            "artifact_invalid",
            "Legacy Dataset tags are not empty",
        )
    for urn in gateway_urns:
        _require(
            "dataPlatformInstance" in expected[urn],
            "artifact_invalid",
            "Gateway platform instance is missing",
        )
    for urn in kafka_urns:
        has_instance = "dataPlatformInstance" in expected[urn]
        _require(
            has_instance == (variant.kafka_instance is not None),
            "artifact_invalid",
            "Kafka platform-instance variant differs",
        )

    job_names: set[str] = set()
    relationships: dict[str, dict[str, frozenset[str]]] = {}
    for urn, kind in entity_types.items():
        if kind != "dataJob":
            continue
        _require(
            set(expected[urn]) == {"dataJobInfo", "dataJobInputOutput"},
            "artifact_invalid",
            "DataJob aspect coverage differs",
        )
        info = expected[urn]["dataJobInfo"]
        job_name = _require_text(info.get("name"))
        job_names.add(job_name)
        lineage = expected[urn]["dataJobInputOutput"]
        _require(
            set(lineage)
            == {"inputDatasets", "outputDatasets", "inputDatasetEdges", "outputDatasetEdges"}
            and lineage["inputDatasets"] == []
            and lineage["outputDatasets"] == [],
            "artifact_invalid",
            "DataJob lineage shape differs",
        )
        consumes = _edge_destinations(lineage["inputDatasetEdges"], "inputDatasetEdges")
        produces = _edge_destinations(lineage["outputDatasetEdges"], "outputDatasetEdges")
        _require(
            consumes | produces <= dataset_urns,
            "artifact_invalid",
            "DataJob lineage contains a dangling Dataset",
        )
        relationships[urn] = {"Consumes": consumes, "Produces": produces}
        if job_name == "warehouse_sink":
            _require(
                len(consumes) == 1 and not produces,
                "artifact_invalid",
                "Connect sink lineage does not have an empty output",
            )
    _require(
        job_names == {"enriched_orders", "public_orders", "warehouse_sink"},
        "artifact_invalid",
        "DataJob names differ or the process-free topic gained a DataJob",
    )
    statuses = {
        payload["name"]: payload["customProperties"]
        for aspects in expected.values()
        if (payload := aspects.get("datasetProperties")) is not None
    }
    _require(
        statuses.get("enriched_orders") == {"streamt.contract.status": "enforced"}
        and statuses.get("public_orders") == {"streamt.contract.status": "declared"},
        "artifact_invalid",
        "Dataset contract projection differs",
    )
    relationship_count = sum(
        len(destinations) for by_type in relationships.values() for destinations in by_type.values()
    )
    _require(
        relationship_count == 5,
        "artifact_invalid",
        "Direct relationship coverage differs from the gate fixture",
    )
    return ArtifactContract(
        proposal_count=len(proposals),
        expected_aspects=expected,
        expected_relationships=relationships,
        aspect_count=len(aspect_names),
        relationship_count=relationship_count,
    )


def parse_server_config(value: Any) -> tuple[str, str]:
    _require(isinstance(value, dict), "server_version_mismatch", "GMS /config is invalid")
    versions = value.get("versions")
    _require(isinstance(versions, dict), "server_version_mismatch", "GMS /config is invalid")
    datahub = versions.get("acryldata/datahub")
    _require(
        isinstance(datahub, dict)
        and datahub.get("version") == SERVER_VERSION
        and datahub.get("commit") == SERVER_COMMIT,
        "server_version_mismatch",
        "GMS server version or commit is not the exact v1.7.0 release",
    )
    return SERVER_VERSION, SERVER_COMMIT


def parse_entity_response(
    value: Any, urn: str, expected_aspects: Mapping[str, Any]
) -> dict[str, Any]:
    _require(isinstance(value, dict), "readback_invalid", "entitiesV2 response is invalid")
    _require(value.get("urn") == urn, "readback_invalid", "entitiesV2 returned another entity")
    aspects = value.get("aspects")
    _require(isinstance(aspects, dict), "readback_invalid", "entitiesV2 aspects are invalid")
    key_aspect = next(
        (name for prefix, name in _SERVER_KEY_ASPECTS.items() if urn.startswith(prefix)),
        None,
    )
    expected_names = set(expected_aspects)
    returned_names = set(aspects)
    if not expected_names <= returned_names:
        raise PollPendingError("Expected aspects have not converged")
    allowed_aspects = expected_names | ({key_aspect} if key_aspect is not None else set())
    _require(
        returned_names <= allowed_aspects,
        "readback_invalid",
        "entitiesV2 returned an unexpected aspect",
    )
    returned: dict[str, Any] = {}
    for name, expected in expected_aspects.items():
        wrapper = aspects[name]
        _require(
            isinstance(wrapper, dict)
            and wrapper.get("name") == name
            and wrapper.get("version") == 0
            and isinstance(wrapper.get("value"), dict),
            "readback_invalid",
            "entitiesV2 aspect wrapper is invalid",
        )
        returned[name] = wrapper["value"]
        if returned[name] != expected:
            raise PollPendingError("Aspect semantic value has not converged")
    return returned


def parse_relationship_page(
    value: Any, *, source_urn: str, relationship_type: str, requested_start: int
) -> RelationshipPage:
    _require(isinstance(value, dict), "readback_invalid", "Relationship response is invalid")
    relationships = value.get("relationships")
    _require(
        isinstance(relationships, list),
        "readback_invalid",
        "Relationship response shape is invalid",
    )
    start = value.get("start")
    count = value.get("count")
    total = value.get("total")
    _require(
        isinstance(start, int)
        and not isinstance(start, bool)
        and isinstance(count, int)
        and not isinstance(count, bool)
        and isinstance(total, int)
        and not isinstance(total, bool)
        and start == requested_start
        and count == len(relationships)
        and total >= 0,
        "readback_invalid",
        "Relationship paging metadata is invalid",
    )
    destinations: list[str] = []
    for relationship in relationships:
        _require(
            isinstance(relationship, dict) and relationship.get("type") == relationship_type,
            "readback_invalid",
            "Relationship type differs",
        )
        destination = _require_text(relationship.get("entity"), "readback_invalid")
        _require(
            destination.startswith("urn:li:dataset:"),
            "readback_invalid",
            "Relationship destination is not a Dataset",
        )
        destinations.append(destination)
    _require(
        len(destinations) == len(set(destinations)),
        "readback_invalid",
        "Relationship response contains duplicate Dataset edges",
    )
    # The v1.7 response does not repeat the queried source URN. Requiring it as
    # a non-empty DataJob here still keeps the parser bound to the exact query.
    _require(
        source_urn.startswith("urn:li:dataJob:"),
        "readback_invalid",
        "Relationship query source is not a DataJob",
    )
    consumed = start + count
    if consumed < total:
        _require(count > 0, "readback_invalid", "Relationship pagination made no progress")
        next_start: int | None = consumed
    else:
        _require(
            consumed == total,
            "readback_invalid",
            "Relationship paging total differs from returned elements",
        )
        next_start = None
    return RelationshipPage(tuple(destinations), total, next_start)


class JsonHttpClient:
    """Bounded GET-only client with proxy discovery disabled."""

    def __init__(self, base_url: str, opener: Any | None = None) -> None:
        self.base_url = validate_gms_url(base_url)
        self.opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def get(self, path_and_query: str, *, restli_v2: bool = True) -> Any:
        _require(
            path_and_query.startswith("/") and not path_and_query.startswith("//"),
            "http_request_invalid",
            "Gate HTTP path is invalid",
        )
        headers = {"Accept": "application/json"}
        if restli_v2:
            headers["X-RestLi-Protocol-Version"] = "2.0.0"
        request = urllib.request.Request(
            self.base_url + path_and_query,
            headers=headers,
            method="GET",
        )
        try:
            with self.opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                status = response.getcode()
                if status is not None and status >= 500:
                    raise RetryableHttpError("GMS server is temporarily unavailable")
                _require(status == 200, "http_request_failed", "GMS GET request failed")
                raw = response.read(MAX_HTTP_BYTES + 1)
        except urllib.error.HTTPError as error:
            if error.code >= 500:
                raise RetryableHttpError("GMS server is temporarily unavailable") from error
            raise GateError("http_request_failed", "GMS GET request failed") from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            raise RetryableHttpError("GMS server is temporarily unavailable") from error
        _require(
            len(raw) <= MAX_HTTP_BYTES,
            "http_response_limit",
            "GMS response exceeded its size bound",
        )
        try:
            return json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise GateError("readback_invalid", "GMS returned invalid JSON") from error


def _entity_path(urn: str, aspect_names: Iterable[str]) -> str:
    encoded_urn = urllib.parse.quote(urn, safe="")
    aspects = ",".join(sorted(aspect_names))
    return f"/entitiesV2/{encoded_urn}?aspects=List({aspects})"


def _relationship_path(urn: str, relationship_type: str, start: int) -> str:
    query = urllib.parse.urlencode(
        {
            "direction": "OUTGOING",
            "urn": urn,
            "types": relationship_type,
            "start": start,
            "count": RELATIONSHIP_PAGE_SIZE,
        },
        safe="(),",
    )
    return f"/relationships?{query}"


def read_aspect_snapshot_once(
    client: JsonHttpClient, contract: ArtifactContract
) -> dict[str, dict[str, Any]]:
    """Read one complete snapshot; discard all partial state on any mismatch."""
    snapshot: dict[str, dict[str, Any]] = {}
    for urn in sorted(contract.expected_aspects, key=lambda item: item.encode("utf-8")):
        expected = contract.expected_aspects[urn]
        value = client.get(_entity_path(urn, expected))
        snapshot[urn] = parse_entity_response(value, urn, expected)
    return snapshot


def read_relationship_set(
    client: JsonHttpClient, urn: str, relationship_type: str
) -> frozenset[str]:
    destinations: list[str] = []
    start = 0
    expected_total: int | None = None
    for _page_number in range(MAX_RELATIONSHIP_PAGES):
        # DataHub v1.7.0's relationship resource rejects the Rest.li v2 list
        # representation used by entitiesV2. Its exact working surface is the
        # default protocol with one scalar `types` value.
        value = client.get(_relationship_path(urn, relationship_type, start), restli_v2=False)
        page = parse_relationship_page(
            value,
            source_urn=urn,
            relationship_type=relationship_type,
            requested_start=start,
        )
        if expected_total is None:
            expected_total = page.total
        _require(
            expected_total == page.total,
            "readback_invalid",
            "Relationship total changed during pagination",
        )
        destinations.extend(page.destinations)
        _require(
            len(destinations) == len(set(destinations)),
            "readback_invalid",
            "Relationship pages contain duplicate Dataset edges",
        )
        if page.next_start is None:
            _require(
                len(destinations) == expected_total,
                "readback_invalid",
                "Relationship total differs from the complete result",
            )
            return frozenset(destinations)
        start = page.next_start
    raise GateError("readback_invalid", "Relationship response exceeded its page bound")


def read_relationship_snapshot_once(
    client: JsonHttpClient, contract: ArtifactContract
) -> dict[str, dict[str, frozenset[str]]]:
    """Read one complete graph snapshot without retaining prior poll results."""
    snapshot: dict[str, dict[str, frozenset[str]]] = {}
    for urn in sorted(contract.expected_relationships, key=lambda item: item.encode("utf-8")):
        by_type: dict[str, frozenset[str]] = {}
        for relationship_type in ("Consumes", "Produces"):
            observed = read_relationship_set(client, urn, relationship_type)
            expected = contract.expected_relationships[urn][relationship_type]
            if observed != expected:
                raise PollPendingError("Relationship set has not converged")
            by_type[relationship_type] = observed
        snapshot[urn] = by_type
    return snapshot


def poll_exact(
    reader: Callable[[], Any],
    *,
    timeout_seconds: float,
    timeout_code: str,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    deadline = monotonic() + timeout_seconds
    while True:
        try:
            return reader()
        except (PollPendingError, RetryableHttpError):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise GateError(
                    timeout_code, "GMS read-back did not converge before its deadline"
                ) from None
            sleep(min(POLL_INTERVAL_SECONDS, remaining))


def _canonical_snapshot(
    aspects: Mapping[str, Mapping[str, Any]],
    relationships: Mapping[str, Mapping[str, frozenset[str]]],
) -> dict[str, Any]:
    return {
        "aspects": aspects,
        "relationships": {
            urn: {name: sorted(values) for name, values in by_type.items()}
            for urn, by_type in relationships.items()
        },
    }


def read_exact_snapshot(client: JsonHttpClient, contract: ArtifactContract) -> dict[str, Any]:
    aspects = poll_exact(
        lambda: read_aspect_snapshot_once(client, contract),
        timeout_seconds=ASPECT_POLL_TIMEOUT_SECONDS,
        timeout_code="aspect_readback_timeout",
    )
    relationships = poll_exact(
        lambda: read_relationship_snapshot_once(client, contract),
        timeout_seconds=RELATIONSHIP_POLL_TIMEOUT_SECONDS,
        timeout_code="relationship_readback_timeout",
    )
    return _canonical_snapshot(aspects, relationships)


def _snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        snapshot, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _generate_artifact(
    streamt: Path,
    project_dir: Path,
    artifact: Path,
    variant: Variant,
    environment: Mapping[str, str],
) -> ArtifactContract:
    result = run_process(
        streamt_command(streamt, project_dir, artifact, variant),
        cwd=project_dir,
        environment=environment,
    )
    _require(
        result.returncode == 0,
        "streamt_export_failed",
        "Installed streamt executable did not generate the DataHub artifact",
    )
    contract = load_artifact_contract(artifact, variant)
    try:
        artifact.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    except OSError as error:
        raise GateError(
            "artifact_invalid", "Generated artifact could not be made read-only"
        ) from error
    return contract


def _ingest_once(
    datahub: Path,
    artifact: Path,
    contract: ArtifactContract,
    *,
    cwd: Path,
    environment: Mapping[str, str],
) -> int:
    before = _sha256(artifact)
    result = run_process(
        datahub_ingest_command(datahub, artifact),
        cwd=cwd,
        environment=environment,
    )
    _require(
        result.returncode == 0,
        "ingestion_failed",
        "Official DataHub ingestion command failed",
    )
    written = parse_ingestion_report(result.output, contract.proposal_count)
    _require(
        _sha256(artifact) == before,
        "artifact_mutated",
        "Official DataHub ingestion mutated the read-only artifact",
    )
    return written


def _selected_variants(name: str) -> tuple[Variant, ...]:
    if name == "all":
        return VARIANTS
    selected = tuple(variant for variant in VARIANTS if variant.name == name)
    _require(bool(selected), "variant_invalid", "Unknown gate variant")
    return selected


def run_gate(
    *,
    gms_url: str,
    streamt_executable: Path,
    datahub_executable: Path,
    variant_name: str,
) -> dict[str, Any]:
    client = JsonHttpClient(gms_url)
    version, commit = parse_server_config(client.get("/config"))

    fixture_bytes = _read_limited(_FIXTURE, MAX_ARTIFACT_BYTES, "input_invalid")
    fixture_hash = hashlib.sha256(fixture_bytes).hexdigest()
    variants_summary: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="streamt-datahub-gms-gate-") as directory:
        root = Path(directory)
        home = root / "home"
        project_dir = root / "project"
        artifacts_dir = root / "artifacts"
        home.mkdir(mode=0o700)
        project_dir.mkdir(mode=0o700)
        artifacts_dir.mkdir(mode=0o700)
        fixture_copy = project_dir / "stream_project.yml"
        fixture_copy.write_bytes(fixture_bytes)
        fixture_copy.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)

        datahub_environment = _minimal_environment(home, gms_url)
        version_result = run_process(
            datahub_version_command(datahub_executable),
            cwd=root,
            environment=datahub_environment,
            timeout_seconds=30,
        )
        _require(
            version_result.returncode == 0,
            "cli_version_mismatch",
            "DataHub CLI version command failed",
        )
        cli_version = parse_cli_version(version_result.output)

        streamt_environment = _minimal_environment(home)
        for variant in _selected_variants(variant_name):
            artifact = artifacts_dir / f"{variant.name}.json"
            repeated_artifact = artifacts_dir / f"{variant.name}.repeat.json"
            contract = _generate_artifact(
                streamt_executable,
                project_dir,
                artifact,
                variant,
                streamt_environment,
            )
            repeated_contract = _generate_artifact(
                streamt_executable,
                project_dir,
                repeated_artifact,
                variant,
                streamt_environment,
            )
            _require(
                contract == repeated_contract
                and _read_limited(artifact, MAX_ARTIFACT_BYTES, "artifact_invalid")
                == _read_limited(repeated_artifact, MAX_ARTIFACT_BYTES, "artifact_invalid"),
                "streamt_export_nondeterministic",
                "Repeated installed streamt exports differ",
            )
            artifact_hash = _sha256(artifact)
            snapshots: list[dict[str, Any]] = []
            written_counts: list[int] = []
            for _attempt in range(2):
                written_counts.append(
                    _ingest_once(
                        datahub_executable,
                        artifact,
                        contract,
                        cwd=root,
                        environment=datahub_environment,
                    )
                )
                snapshots.append(read_exact_snapshot(client, contract))
            _require(
                snapshots[0] == snapshots[1],
                "replay_changed_readback",
                "Second ingestion changed exact server read-back",
            )
            _require(
                _sha256(artifact) == artifact_hash,
                "artifact_mutated",
                "Artifact bytes changed during the gate",
            )
            variants_summary.append(
                {
                    "artifact_sha256": artifact_hash,
                    "aspects": contract.aspect_count,
                    "ingestions": 2,
                    "proposals_written": written_counts,
                    "relationships": contract.relationship_count,
                    "snapshot_sha256": _snapshot_digest(snapshots[1]),
                    "variant": variant.name,
                }
            )
        _require(
            _sha256(fixture_copy) == fixture_hash and _sha256(_FIXTURE) == fixture_hash,
            "fixture_mutated",
            "Gate fixture bytes changed during execution",
        )

    return {
        "cli_version": cli_version,
        "fixture_sha256": fixture_hash,
        "server_commit": commit,
        "server_version": version,
        "status": "ok",
        "variants": variants_summary,
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gms-url", required=True)
    parser.add_argument("--streamt-executable", required=True)
    parser.add_argument("--datahub-executable", required=True)
    parser.add_argument(
        "--variant",
        choices=("all", *(variant.name for variant in VARIANTS)),
        default="all",
    )
    return parser.parse_args(argv)


def _emit_summary(value: Mapping[str, Any], *, stream: Any = sys.stdout) -> None:
    print(
        json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        ),
        file=stream,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        gms_url = validate_gms_url(args.gms_url)
        streamt = validate_executable(args.streamt_executable, "streamt")
        datahub = validate_executable(args.datahub_executable, "datahub")
        summary = run_gate(
            gms_url=gms_url,
            streamt_executable=streamt,
            datahub_executable=datahub,
            variant_name=args.variant,
        )
    except GateError as error:
        _emit_summary(
            {"code": error.code, "message": str(error), "status": "error"},
            stream=sys.stderr,
        )
        return 1
    except Exception:
        _emit_summary(
            {
                "code": "internal_error",
                "message": "DataHub GMS gate failed unexpectedly",
                "status": "error",
            },
            stream=sys.stderr,
        )
        return 1
    _emit_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
