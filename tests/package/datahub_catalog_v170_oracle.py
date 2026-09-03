"""Offline release oracle for the DataHub 1.7.0 catalog artifact contract.

This file intentionally uses the official SDK only in an isolated test
environment.  It is not imported by streamt and is not part of the wheel.
"""

from __future__ import annotations

import argparse
import copy
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

_EXPECTED_DISTRIBUTION = "acryl-datahub"
_EXPECTED_VERSION = "1.7.0"
_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "datahub" / "v1.7.0"
_ROOT_KEYS = {"aspect", "aspectName", "changeType", "entityType", "entityUrn"}
_ALLOWED_PAIRS = {
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
_ASPECT_KEYS = {
    "dataFlowInfo": (
        {"name", "customProperties", "env"},
        {"description"},
    ),
    "datasetProperties": (
        {"name", "customProperties", "tags"},
        {"description"},
    ),
    "dataPlatformInstance": ({"platform", "instance"}, set()),
    "dataJobInfo": (
        {"name", "type", "customProperties", "env", "flowUrn"},
        {"description"},
    ),
    "dataJobInputOutput": (
        {
            "inputDatasets",
            "outputDatasets",
            "inputDatasetEdges",
            "outputDatasetEdges",
        },
        set(),
    ),
}


class OracleError(RuntimeError):
    """A bounded release-oracle failure."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OracleError(message)


def _scrub_datahub_environment() -> None:
    for key in tuple(os.environ):
        if key.startswith("DATAHUB_"):
            del os.environ[key]
    os.environ["DATAHUB_DATASET_URN_TO_LOWER"] = "false"
    os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


def _load_sdk() -> SimpleNamespace:
    _scrub_datahub_environment()
    try:
        version = importlib.metadata.version(_EXPECTED_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError as error:
        raise OracleError("exact acryl-datahub 1.7.0 is not installed") from error
    _require(version == _EXPECTED_VERSION, "acryl-datahub version is not exactly 1.7.0")

    # Imports must remain below environment sanitization: dataset URN casing is
    # selected by the SDK at import time.
    from datahub.emitter.mce_builder import (  # type: ignore[import-not-found]
        make_data_flow_urn,
        make_data_job_urn,
        make_data_platform_urn,
        make_dataset_urn_with_platform_instance,
    )
    from datahub.emitter.mcp import (  # type: ignore[import-not-found]
        MetadataChangeProposalWrapper,
    )
    from datahub.ingestion.source.file import (  # type: ignore[import-not-found]
        read_metadata_file,
    )
    from datahub.metadata.urns import (  # type: ignore[import-not-found]
        DataFlowUrn,
        DataJobUrn,
        DataPlatformInstanceUrn,
        DataPlatformUrn,
        DatasetUrn,
    )

    return SimpleNamespace(
        DataFlowUrn=DataFlowUrn,
        DataJobUrn=DataJobUrn,
        DataPlatformInstanceUrn=DataPlatformInstanceUrn,
        DataPlatformUrn=DataPlatformUrn,
        DatasetUrn=DatasetUrn,
        MetadataChangeProposalWrapper=MetadataChangeProposalWrapper,
        make_data_flow_urn=make_data_flow_urn,
        make_data_job_urn=make_data_job_urn,
        make_data_platform_urn=make_data_platform_urn,
        make_dataset_urn_with_platform_instance=make_dataset_urn_with_platform_instance,
        read_metadata_file=read_metadata_file,
    )


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise OracleError("artifact contains a duplicate JSON object key")
        value[key] = item
    return value


def _load_canonical_json(path: Path) -> Any:
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        value = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise OracleError("artifact is not strict UTF-8 JSON") from error
    canonical = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    _require(raw == canonical, "artifact bytes are not canonical JSON")
    return value


def _require_exact_keys(
    value: dict[str, object], required: set[str], optional: set[str], location: str
) -> None:
    _require(required <= value.keys(), f"{location} is missing a required field")
    _require(value.keys() <= required | optional, f"{location} has an unsupported field")


def _require_text(value: object, location: str) -> str:
    if not isinstance(value, str) or not value:
        raise OracleError(f"{location} must be non-empty text")
    return value


def _validate_payload(aspect_name: str, payload: dict[str, Any]) -> None:
    required, optional = _ASPECT_KEYS[aspect_name]
    _require_exact_keys(payload, required, optional, aspect_name)
    _require_text(payload.get("name"), f"{aspect_name}.name") if "name" in required else None
    if "description" in payload:
        _require_text(payload["description"], f"{aspect_name}.description")

    if aspect_name == "dataFlowInfo":
        _require(payload["customProperties"] == {}, "dataFlowInfo custom properties differ")
        _require_text(payload["env"], "dataFlowInfo.env")
    elif aspect_name == "datasetProperties":
        _require(payload["tags"] == [], "datasetProperties legacy tags must be empty")
        custom = payload["customProperties"]
        _require(
            custom
            in (
                {},
                {"streamt.contract.status": "declared"},
                {"streamt.contract.status": "enforced"},
            ),
            "datasetProperties custom properties differ",
        )
    elif aspect_name == "dataPlatformInstance":
        _require_text(payload["platform"], "dataPlatformInstance.platform")
        _require_text(payload["instance"], "dataPlatformInstance.instance")
    elif aspect_name == "dataJobInfo":
        _require(payload["customProperties"] == {}, "dataJobInfo custom properties differ")
        _require_text(payload["env"], "dataJobInfo.env")
        _require_text(payload["flowUrn"], "dataJobInfo.flowUrn")
        _require(
            payload["type"] in ({"string": "flink"}, {"string": "gateway"}, {"string": "connect"}),
            "dataJobInfo union type differs",
        )
    else:
        _require(payload["inputDatasets"] == [], "legacy inputDatasets must be empty")
        _require(payload["outputDatasets"] == [], "legacy outputDatasets must be empty")
        for field in ("inputDatasetEdges", "outputDatasetEdges"):
            edges = payload[field]
            _require(isinstance(edges, list), f"{field} must be an array")
            destinations: list[str] = []
            for edge in edges:
                _require(
                    isinstance(edge, dict) and edge.keys() == {"destinationUrn"},
                    f"{field} edge shape differs",
                )
                destinations.append(
                    _require_text(edge["destinationUrn"], f"{field}.destinationUrn")
                )
            _require(len(destinations) == len(set(destinations)), f"{field} contains duplicates")
            _require(
                destinations == sorted(destinations, key=lambda item: item.encode()),
                f"{field} is not sorted",
            )


def _parse_entity_urn(sdk: SimpleNamespace, entity_type: str, urn: str) -> Any:
    urn_class = {
        "dataFlow": sdk.DataFlowUrn,
        "dataset": sdk.DatasetUrn,
        "dataJob": sdk.DataJobUrn,
    }[entity_type]
    try:
        parsed = urn_class.from_string(urn)
    except Exception as error:
        raise OracleError("an entity URN fails official parsing") from error
    _require(str(parsed) == urn, "an entity URN fails official parse/string parity")
    return parsed


def _record_order_key(record: dict[str, Any]) -> tuple[int, bytes, int]:
    entity_type = str(record["entityType"])
    aspect_name = str(record["aspectName"])
    entity_rank = {"dataFlow": 0, "dataset": 1, "dataJob": 2}[entity_type]
    aspect_rank = {
        "dataFlowInfo": 0,
        "datasetProperties": 0,
        "dataPlatformInstance": 1,
        "dataJobInfo": 0,
        "dataJobInputOutput": 1,
    }[aspect_name]
    return entity_rank, str(record["entityUrn"]).encode(), aspect_rank


def validate_artifact(path: Path, sdk: SimpleNamespace) -> int:
    value = _load_canonical_json(path)
    _require(isinstance(value, list) and bool(value), "artifact root must be a non-empty array")
    records: list[dict[str, Any]] = []
    pairs: set[tuple[str, str]] = set()
    urn_types: dict[str, str] = {}
    parsed_urns: dict[str, Any] = {}
    payloads: dict[tuple[str, str], dict[str, Any]] = {}

    for position, item in enumerate(value):
        _require(isinstance(item, dict), f"proposal {position} must be an object")
        record = item
        _require(record.keys() == _ROOT_KEYS, f"proposal {position} root shape differs")
        entity_type = _require_text(record["entityType"], f"proposal {position}.entityType")
        aspect_name = _require_text(record["aspectName"], f"proposal {position}.aspectName")
        urn = _require_text(record["entityUrn"], f"proposal {position}.entityUrn")
        _require((entity_type, aspect_name) in _ALLOWED_PAIRS, "entity/aspect pair is unsupported")
        _require(record["changeType"] == "UPSERT", "changeType must be UPSERT")
        _require(urn.startswith(_ENTITY_PREFIXES[entity_type]), "entity type and URN differ")
        aspect = record["aspect"]
        _require(isinstance(aspect, dict) and aspect.keys() == {"json"}, "aspect wrapper differs")
        payload = aspect["json"]
        _require(isinstance(payload, dict), "aspect payload must be an object")
        _validate_payload(aspect_name, payload)

        pair = (urn, aspect_name)
        _require(pair not in pairs, "artifact contains a repeated entity/aspect pair")
        pairs.add(pair)
        _require(
            urn_types.get(urn, entity_type) == entity_type, "one URN has multiple entity types"
        )
        urn_types[urn] = entity_type
        parsed_urns.setdefault(urn, _parse_entity_urn(sdk, entity_type, urn))
        payloads[pair] = payload

        untouched = copy.deepcopy(record)
        try:
            rebuilt = sdk.MetadataChangeProposalWrapper.from_obj(copy.deepcopy(record))
            _require(rebuilt.validate() is True, "official wrapper validation failed")
            _require(
                rebuilt.to_obj(simplified_structure=True) == untouched,
                "official deepcopy from_obj round-trip differs",
            )
        except OracleError:
            raise
        except Exception as error:
            raise OracleError("official wrapper reconstruction failed") from error
        records.append(record)

    _require(records == sorted(records, key=_record_order_key), "proposal ordering differs")
    flow_urns = sorted(urn for urn, kind in urn_types.items() if kind == "dataFlow")
    dataset_urns = {urn for urn, kind in urn_types.items() if kind == "dataset"}
    job_urns = sorted(urn for urn, kind in urn_types.items() if kind == "dataJob")
    _require(len(flow_urns) == 1, "artifact must contain exactly one DataFlow")
    flow_urn = flow_urns[0]
    _require((flow_urn, "dataFlowInfo") in pairs, "DataFlow info is missing")

    for urn in dataset_urns:
        _require((urn, "datasetProperties") in pairs, "Dataset properties are missing")
        dpi = payloads.get((urn, "dataPlatformInstance"))
        if dpi is not None:
            try:
                instance = sdk.DataPlatformInstanceUrn.from_string(dpi["instance"])
            except Exception as error:
                raise OracleError("platform-instance URN fails official parsing") from error
            _require(
                str(instance) == dpi["instance"], "platform-instance parse/string parity differs"
            )
            _require(instance.platform == dpi["platform"], "platform-instance platform differs")
            _require(
                parsed_urns[urn].platform == dpi["platform"], "Dataset platform differs from aspect"
            )

    for urn in job_urns:
        _require((urn, "dataJobInfo") in pairs, "DataJob info is missing")
        _require((urn, "dataJobInputOutput") in pairs, "DataJob lineage is missing")
        info = payloads[(urn, "dataJobInfo")]
        lineage = payloads[(urn, "dataJobInputOutput")]
        _require(info["flowUrn"] == flow_urn, "DataJob info points to another DataFlow")
        _require(parsed_urns[urn].flow == flow_urn, "DataJob URN nests another DataFlow")
        for field in ("inputDatasetEdges", "outputDatasetEdges"):
            destinations = {edge["destinationUrn"] for edge in lineage[field]}
            _require(destinations <= dataset_urns, "DataJob lineage has a dangling Dataset")

    try:
        read_back = list(sdk.read_metadata_file(path))
        simplified = [item.to_obj(simplified_structure=True) for item in read_back]
    except Exception as error:
        raise OracleError("official metadata-file reader rejected the artifact") from error
    _require(simplified == records, "official metadata-file reader round-trip differs")
    return len(records)


def _validate_identity_vectors(path: Path, sdk: SimpleNamespace) -> None:
    value = _load_canonical_json(path)
    _require(isinstance(value, dict), "identity fixture root must be an object")
    _require(
        value.keys()
        == {"data_flows", "data_jobs", "data_platform_instances", "data_platforms", "datasets"},
        "identity fixture groups differ",
    )
    for vector in value["data_platforms"]:
        actual = sdk.make_data_platform_urn(vector["platform"])
        _require(actual == vector["urn"], "official DataPlatform identity differs")
        _require(
            str(sdk.DataPlatformUrn.from_string(actual)) == actual,
            "DataPlatform parse parity differs",
        )
    for vector in value["data_platform_instances"]:
        actual = str(
            sdk.DataPlatformInstanceUrn(sdk.DataPlatformUrn(vector["platform"]), vector["instance"])
        )
        _require(actual == vector["urn"], "official typed platform-instance identity differs")
        _require(
            str(sdk.DataPlatformInstanceUrn.from_string(actual)) == actual,
            "platform-instance parse parity differs",
        )
    for vector in value["datasets"]:
        actual = sdk.make_dataset_urn_with_platform_instance(
            vector["platform"], vector["name"], vector["instance"], vector["fabric"]
        )
        _require(actual == vector["urn"], "official Dataset identity differs")
        _require(str(sdk.DatasetUrn.from_string(actual)) == actual, "Dataset parse parity differs")
    for vector in value["data_flows"]:
        actual = sdk.make_data_flow_urn(vector["orchestrator"], vector["flow_id"], vector["fabric"])
        _require(actual == vector["urn"], "official DataFlow identity differs")
        _require(
            str(sdk.DataFlowUrn.from_string(actual)) == actual, "DataFlow parse parity differs"
        )
    for vector in value["data_jobs"]:
        actual = sdk.make_data_job_urn(
            vector["orchestrator"], vector["flow_id"], vector["job_id"], vector["fabric"]
        )
        _require(actual == vector["urn"], "official DataJob identity differs")
        _require(str(sdk.DataJobUrn.from_string(actual)) == actual, "DataJob parse parity differs")


def _assert_fixture_semantics(path: Path) -> None:
    records = json.loads(path.read_text(encoding="utf-8"))
    raw_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,Orders.Raw,PROD)"
    raw_aspects = [item["aspectName"] for item in records if item["entityUrn"] == raw_urn]
    _require(raw_aspects == ["datasetProperties"], "Kafka no-instance aspect omission differs")

    gateway_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest.orders%28public%29,PROD)"
    )
    gateway_dpi = next(
        item["aspect"]["json"]
        for item in records
        if item["entityUrn"] == gateway_urn and item["aspectName"] == "dataPlatformInstance"
    )
    _require(
        gateway_dpi["instance"]
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:conduktor-gateway,edge%2Cwest)",
        "typed reserved-character platform-instance fixture differs",
    )
    lineage = next(
        item["aspect"]["json"] for item in records if item["aspectName"] == "dataJobInputOutput"
    )
    _require(
        set(lineage)
        == {"inputDatasets", "outputDatasets", "inputDatasetEdges", "outputDatasetEdges"},
        "lineage fixture does not contain exactly all four arrays",
    )


def _run_offline_cli(path: Path, expected_events: int) -> None:
    if sys.version_info[:2] != (3, 11):
        return
    datahub = Path(sys.executable).with_name("datahub")
    _require(datahub.is_file(), "official datahub CLI is unavailable beside Python")
    with tempfile.TemporaryDirectory(prefix="streamt-datahub-oracle-") as directory:
        temp_dir = Path(directory)
        guard_dir = temp_dir / "guard"
        guard_dir.mkdir()
        guard_marker = temp_dir / "network-guard-loaded"
        (guard_dir / "sitecustomize.py").write_text(
            "import os\n"
            "import socket\n\n"
            "open(os.environ['STREAMT_DATAHUB_ORACLE_GUARD_MARKER'], 'x').close()\n\n"
            "def _blocked(*args, **kwargs):\n"
            "    raise RuntimeError('network disabled by streamt DataHub oracle')\n\n"
            "socket.socket.connect = _blocked\n"
            "socket.socket.connect_ex = _blocked\n"
            "socket.create_connection = _blocked\n"
            "socket.getaddrinfo = _blocked\n",
            encoding="utf-8",
        )
        sink = temp_dir / "dry-run-sink.json"
        recipe = temp_dir / "recipe.yml"
        recipe.write_text(
            json.dumps(
                {
                    "source": {"type": "file", "config": {"path": str(path.resolve())}},
                    "sink": {"type": "file", "config": {"filename": str(sink)}},
                }
            ),
            encoding="utf-8",
        )
        environment = {
            key: value for key, value in os.environ.items() if not key.startswith("DATAHUB_")
        }
        environment.update(
            {
                "DATAHUB_DATASET_URN_TO_LOWER": "false",
                "DATAHUB_TELEMETRY_ENABLED": "false",
                "PYTHONNOUSERSITE": "1",
                "PYTHONPATH": str(guard_dir),
                "STREAMT_DATAHUB_ORACLE_GUARD_MARKER": str(guard_marker),
            }
        )
        command = [
            str(datahub),
            "ingest",
            "run",
            "--dry-run",
            "--strict-warnings",
            "--no-default-report",
            "--no-spinner",
            "--no-progress",
            "-c",
            str(recipe),
        ]
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                timeout=45,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise OracleError("official offline CLI did not complete") from error
        output = result.stdout + result.stderr
        _require(guard_marker.is_file(), "offline CLI network guard was not loaded")
        _require(result.returncode == 0, "official offline CLI rejected the artifact")
        _require(
            "Pipeline finished successfully" in output, "official CLI success marker is absent"
        )
        _require(
            re.search(rf"events_produced['\"]?\s*[:=]\s*{expected_events}\b", output) is not None,
            "official CLI source event count differs",
        )
        _require(sink.read_text(encoding="utf-8").strip() == "[]", "dry-run sink result differs")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test", action="store_true", help="validate reviewed oracle fixtures")
    mode.add_argument("--artifact", type=Path, help="validate one canonical MCP artifact")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        sdk = _load_sdk()
        if args.self_test:
            _validate_identity_vectors(_FIXTURE_DIR / "identity-vectors.json", sdk)
            artifact = _FIXTURE_DIR / "representative-mcps.json"
            count = validate_artifact(artifact, sdk)
            _assert_fixture_semantics(artifact)
        else:
            artifact = args.artifact
            count = validate_artifact(artifact, sdk)
        _run_offline_cli(artifact, count)
    except OracleError as error:
        print(f"DataHub 1.7.0 oracle failed: {error}", file=sys.stderr)
        return 1
    cli_status = "passed" if sys.version_info[:2] == (3, 11) else "skipped (requires Python 3.11)"
    print(f"DataHub 1.7.0 oracle passed: {count} proposals; CLI {cli_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
