"""Offline schema, semantic, lifecycle, and package tests for OpenLineage."""

from __future__ import annotations

import base64
import gzip
import hashlib
import importlib.util
import os
import socket
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Callable, Iterator
from importlib.resources import files
from pathlib import Path
from typing import cast

import pytest
from jsonschema import Draft202012Validator
from referencing.exceptions import NoSuchResource

from streamt.core.errors import ErrorCode
from streamt.integrations.openlineage import (
    EVENT_SCHEMA_URLS,
    DatasetIdentity,
    JobIdentity,
    OpenLineageConstructionError,
    OpenLineageResourceError,
    OpenLineageValidationError,
    RunIdentity,
    build_dataset_event,
    build_job_event,
    build_run_event,
    command_job_name,
    model_job_name,
    standard_facet,
    validate_event,
    validate_event_sequence,
    validate_kafka_namespace,
    validate_standard_facet,
)
from streamt.integrations.openlineage import validation as ol_validation
from streamt.integrations.openlineage.events import FacetScope

EVENT_TIME = "2026-09-01T12:34:56.123456Z"
RUN_ID = "12345678-1234-5678-9234-567812345678"
RESOURCE_EXPECTATIONS = {
    "openlineage-1.53.0-core.json.gz.b64": (
        9_155,
        "69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2",
    ),
    "openlineage-1.53.0-job-type-job-facet.json.gz.b64": (
        3_072,
        "11c12cab95a411ca31066c80d2bb4aefd37bbcadbda5e4d343d2069853b907d5",
    ),
    "openlineage-1.53.0-schema-dataset-facet.json.gz.b64": (
        1_687,
        "50236a779aa64baa0bad0055391838bd22fcb36ce667c41d60ada80915e899b6",
    ),
    "openlineage-1.53.0-documentation-dataset-facet.json.gz.b64": (
        1_031,
        "bad5c041d679e73b2faea43506860428f48d80710ad1354a2d2393475143285f",
    ),
    "openlineage-1.53.0-documentation-job-facet.json.gz.b64": (
        944,
        "b5823685c20c712d9ee1e3b310ad6ca426be7b46db60acd9f23248811af3c8d4",
    ),
    "openlineage-1.53.0-dataset-type-dataset-facet.json.gz.b64": (
        1_008,
        "1a7d5106877151d52c4d3967f77b92788ea42ebf9843c6573d776a63c9a7d157",
    ),
    "openlineage-1.53.0-ownership-dataset-facet.json.gz.b64": (
        1_368,
        "9a18a508746627eff18fa84ca1694e1a9f2d556ac0052dbdb6970d8a35f75231",
    ),
    "openlineage-1.53.0-ownership-job-facet.json.gz.b64": (
        1_344,
        "c54ae771b1183efbe007a778eb5bf05e9e3f39f3d48f22a4beceea00950abdad",
    ),
    "openlineage-1.53.0-error-message-run-facet.json.gz.b64": (
        1_501,
        "b11b4ee8b0f99f6846264f87cad48390255c7ef142ce623216660c7097be0ea6",
    ),
}


def _assert_openlineage_schema_resources(
    *,
    member_names: set[str],
    read_member: Callable[[str], bytes],
    source_distribution: bool,
) -> None:
    for resource_name, (decoded_size, decoded_checksum) in RESOURCE_EXPECTATIONS.items():
        suffix = f"/src/streamt/docs/schemas/{resource_name}"
        if source_distribution:
            matches = [name for name in member_names if name.endswith(suffix)]
            assert len(matches) == 1, matches
            member = matches[0]
        else:
            member = f"streamt/docs/schemas/{resource_name}"
            assert member in member_names
        encoded = read_member(member)
        decoded = gzip.decompress(base64.b64decode(b"".join(encoded.split()), validate=True))
        assert len(decoded) == decoded_size
        assert hashlib.sha256(decoded).hexdigest() == decoded_checksum


@pytest.fixture(autouse=True)
def clear_openlineage_schema_caches() -> Iterator[None]:
    ol_validation._clear_schema_caches()
    yield
    ol_validation._clear_schema_caches()


def _representative_events() -> tuple[dict[str, object], ...]:
    kafka_namespace = "kafka://broker.internal:9092"
    input_dataset = DatasetIdentity(kafka_namespace, "orders.raw")
    output_dataset = DatasetIdentity(kafka_namespace, "orders.clean")
    job = JobIdentity("lineage.acme", model_job_name("acme", "orders"))
    run = RunIdentity(RUN_ID)

    dataset_facets = {
        "datasetType": standard_facet(
            "dataset", "datasetType", {"datasetType": "TOPIC"}
        ),
        "documentation": standard_facet(
            "dataset",
            "documentation",
            {"description": "Clean orders"},
        ),
        "ownership": standard_facet(
            "dataset",
            "ownership",
            {"owners": [{"name": "team:data", "type": "MAINTAINER"}]},
        ),
        "schema": standard_facet(
            "dataset",
            "schema",
            {
                "fields": [
                    {"name": "order_id", "type": "BIGINT"},
                    {
                        "name": "customer",
                        "type": "ROW",
                        "fields": [{"name": "id", "type": "BIGINT"}],
                    },
                ]
            },
        ),
    }
    job_facets = {
        "documentation": standard_facet(
            "job", "documentation", {"description": "Transform orders"}
        ),
        "jobType": standard_facet(
            "job",
            "jobType",
            {
                "processingType": "STREAMING",
                "integration": "STREAMT",
                "jobType": "MODEL",
            },
        ),
        "ownership": standard_facet(
            "job",
            "ownership",
            {"owners": [{"name": "team:data"}]},
        ),
    }
    run_facets = {
        "errorMessage": standard_facet(
            "run",
            "errorMessage",
            {"message": "safe failure", "programmingLanguage": "PYTHON"},
        )
    }
    return (
        build_dataset_event(
            event_time=EVENT_TIME,
            dataset=output_dataset,
            facets=dataset_facets,
        ),
        build_job_event(
            event_time=EVENT_TIME,
            job=job,
            facets=job_facets,
            inputs=[input_dataset],
            outputs=[output_dataset],
        ),
        build_run_event(
            event_time=EVENT_TIME,
            event_type="FAIL",
            run=run,
            job=job,
            run_facets=run_facets,
            job_facets=job_facets,
            inputs=[input_dataset],
            outputs=[output_dataset],
        ),
    )


@pytest.mark.parametrize(
    "namespace",
    [
        "kafka://Broker-01.Example.COM:09092",
        "kafka://127.0.0.1:9092",
        "kafka://[2001:DB8::1]:9092",
    ],
)
def test_kafka_namespace_accepts_and_preserves_strict_ascii_authorities(
    namespace: str,
) -> None:
    assert validate_kafka_namespace(namespace) == namespace


@pytest.mark.parametrize(
    "namespace",
    [
        "kafka://ho st:9092",
        "kafka://ho\tst:9092",
        "kafka://ho\nst:9092",
        "kafka://bröker:9092",
        "kafka://-broker.example:9092",
        "kafka://broker_.example:9092",
        "kafka://broker..example:9092",
        "kafka://999.1.1.1:9092",
        "kafka://2001:db8::1:9092",
    ],
)
def test_kafka_namespace_rejects_non_uri_or_invalid_host_authorities(
    namespace: str,
) -> None:
    with pytest.raises(OpenLineageConstructionError):
        validate_kafka_namespace(namespace)


def test_all_pinned_resources_have_exact_bytes_digests_and_valid_schemas() -> None:
    schema_package = files("streamt.docs.schemas")
    total_size = 0
    for resource_name, (expected_size, expected_sha256) in RESOURCE_EXPECTATIONS.items():
        encoded = b"".join(schema_package.joinpath(resource_name).read_bytes().split())
        compressed = base64.b64decode(encoded, validate=True)
        assert compressed[4:8] == b"\x00\x00\x00\x00"
        decoded = gzip.decompress(compressed)
        total_size += len(decoded)
        assert len(decoded) == expected_size
        assert hashlib.sha256(decoded).hexdigest() == expected_sha256

    assert total_size == 21_110
    loaded = ol_validation._official_schemas()
    assert len(loaded) == 9
    for schema in loaded.values():
        Draft202012Validator.check_schema(schema)


@pytest.mark.parametrize(
    "corrupt_payload",
    [b"not-valid-base64%%%", base64.b64encode(b"not a gzip stream")],
)
def test_invalid_resource_encoding_fails_closed_with_stable_safe_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    corrupt_payload: bytes,
) -> None:
    package = files("streamt.docs.schemas")
    for resource_name in RESOURCE_EXPECTATIONS:
        (tmp_path / resource_name).write_bytes(package.joinpath(resource_name).read_bytes())
    (tmp_path / "openlineage-1.53.0-core.json.gz.b64").write_bytes(corrupt_payload)
    monkeypatch.setattr(ol_validation, "files", lambda _package: tmp_path)
    ol_validation._clear_schema_caches()

    with pytest.raises(OpenLineageResourceError) as captured:
        ol_validation._official_schemas()

    assert str(captured.value) == "A bundled OpenLineage schema cannot be decoded"
    assert str(tmp_path) not in str(captured.value)


@pytest.mark.parametrize(
    ("tamper", "expected_message"),
    [
        ("wrong-size", "A bundled OpenLineage schema does not match its pinned size"),
        (
            "wrong-checksum",
            "A bundled OpenLineage schema does not match its pinned checksum",
        ),
    ],
)
def test_valid_gzip_tampering_fails_size_or_checksum_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
    expected_message: str,
) -> None:
    package = files("streamt.docs.schemas")
    for resource_name in RESOURCE_EXPECTATIONS:
        (tmp_path / resource_name).write_bytes(package.joinpath(resource_name).read_bytes())
    target = tmp_path / "openlineage-1.53.0-core.json.gz.b64"
    if tamper == "wrong-size":
        raw = b"{}"
    else:
        original = b"".join(target.read_bytes().split())
        raw = gzip.decompress(base64.b64decode(original, validate=True))
        assert raw.endswith(b"\n")
        raw = raw[:-1] + b" "
    target.write_bytes(base64.b64encode(gzip.compress(raw, mtime=0)))
    monkeypatch.setattr(ol_validation, "files", lambda _package: tmp_path)
    ol_validation._clear_schema_caches()

    with pytest.raises(OpenLineageResourceError) as captured:
        ol_validation._official_schemas()

    assert str(captured.value) == expected_message


def test_registry_has_no_remote_retrieval_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("unexpected network access")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(socket, "getaddrinfo", fail_network)

    for event in _representative_events():
        validate_event(event)
    with pytest.raises(NoSuchResource):
        ol_validation._schema_registry().get_or_retrieve(
            "https://example.invalid/unbundled-schema.json"
        )


def test_all_eight_standard_facets_are_validated_at_their_entity_scope() -> None:
    events = _representative_events()
    dataset = events[0]["dataset"]
    job = events[1]["job"]
    run = events[2]["run"]
    for scope_value, entity_value in (
        ("dataset", dataset),
        ("job", job),
        ("run", run),
    ):
        scope = cast(FacetScope, scope_value)
        entity = cast(dict[str, object], entity_value)
        facets = cast(dict[str, dict[str, object]], entity["facets"])
        for key, facet in facets.items():
            validate_standard_facet(scope, key, facet)


def test_facet_schema_url_and_entity_location_must_match_key() -> None:
    facet = standard_facet("dataset", "datasetType", {"datasetType": "TOPIC"})
    facet["_schemaURL"] = (
        "https://openlineage.io/spec/facets/1-1-0/"
        "DocumentationDatasetFacet.json#/$defs/DocumentationDatasetFacet"
    )
    with pytest.raises(OpenLineageValidationError, match="schema URL"):
        validate_standard_facet("dataset", "datasetType", facet)
    with pytest.raises(OpenLineageValidationError, match="not supported"):
        validate_standard_facet("job", "datasetType", facet)


@pytest.mark.parametrize(
    ("scope", "key", "values", "unsupported_field"),
    [
        ("dataset", "datasetType", {"datasetType": "TOPIC"}, "description"),
        ("dataset", "documentation", {"description": "docs"}, "datasetType"),
        ("dataset", "ownership", {"owners": []}, "message"),
        ("dataset", "schema", {"fields": []}, "integration"),
        (
            "job",
            "jobType",
            {"processingType": "BATCH", "integration": "STREAMT"},
            "stackTrace",
        ),
        (
            "run",
            "errorMessage",
            {"message": "safe", "programmingLanguage": "PYTHON"},
            "owners",
        ),
    ],
)
def test_each_standard_facet_rejects_cross_facet_or_extension_fields(
    scope: str,
    key: str,
    values: dict[str, object],
    unsupported_field: str,
) -> None:
    facet = standard_facet(scope, key, values)  # type: ignore[arg-type]
    facet[unsupported_field] = "must not pass"

    with pytest.raises(OpenLineageValidationError, match="unsupported fields"):
        validate_standard_facet(scope, key, facet)  # type: ignore[arg-type]


def test_nested_standard_facet_records_have_exact_allowed_fields() -> None:
    schema_facet = standard_facet(
        "dataset", "schema", {"fields": [{"name": "safe_name"}]}
    )
    schema_facet["fields"][0]["extension"] = "value"  # type: ignore[index]
    with pytest.raises(OpenLineageValidationError, match="unsupported fields"):
        validate_standard_facet("dataset", "schema", schema_facet)

    owner_facet = standard_facet(
        "job", "ownership", {"owners": [{"name": "team:data"}]}
    )
    owner_facet["owners"][0]["extension"] = "value"  # type: ignore[index]
    with pytest.raises(OpenLineageValidationError, match="unsupported fields"):
        validate_standard_facet("job", "ownership", owner_facet)

    job_type = standard_facet(
        "job",
        "jobType",
        {
            "processingType": "BATCH",
            "integration": "STREAMT",
            "emissionPattern": {
                "eventTrigger": "EVENT_BASED",
                "eventContentMode": "ACCUMULATIVE",
            },
        },
    )
    job_type["emissionPattern"]["extension"] = "value"  # type: ignore[index]
    with pytest.raises(OpenLineageValidationError, match="unsupported fields"):
        validate_standard_facet("job", "jobType", job_type)


def test_official_schema_errors_are_sorted_and_report_safe_json_pointers() -> None:
    facet = standard_facet("dataset", "documentation", {"description": "docs"})
    facet["description"] = 7
    facet["contentType"] = 8
    with pytest.raises(OpenLineageValidationError) as captured:
        validate_standard_facet("dataset", "documentation", facet)

    assert str(captured.value) == (
        "Official OpenLineage facet schema validation failed at /contentType (type)"
    )

    invalid_event = _representative_events()[0]
    invalid_event["eventTime"] = "not-a-time"
    invalid_event["schemaURL"] = 7
    with pytest.raises(OpenLineageValidationError) as event_error:
        validate_event(invalid_event)
    assert str(event_error.value) == (
        "Official OpenLineage event schema validation failed at /eventTime (format)"
    )


def test_format_checker_and_local_event_kind_allowlists_are_enforced() -> None:
    dataset_event = _representative_events()[0]

    invalid_time = dict(dataset_event)
    invalid_time["eventTime"] = "not-an-rfc3339-time"
    with pytest.raises(OpenLineageValidationError, match="Official"):
        validate_event(invalid_time)

    wrong_url = dict(dataset_event)
    wrong_url["schemaURL"] = EVENT_SCHEMA_URLS["job"]
    with pytest.raises(OpenLineageValidationError, match="event kind"):
        validate_event(wrong_url)

    unexpected = dict(dataset_event)
    unexpected["eventType"] = "OTHER"
    with pytest.raises(OpenLineageValidationError, match="fields"):
        validate_event(unexpected)


def test_core_permitted_missing_run_event_type_fails_local_semantics() -> None:
    _, _, run_event = _representative_events()
    missing_event_type = dict(run_event)
    del missing_event_type["eventType"]

    assert not list(ol_validation._event_validator().iter_errors(missing_event_type))
    with pytest.raises(OpenLineageValidationError, match=r"fields|required"):
        validate_event(missing_event_type)


def test_schema_field_collision_fails_without_exposing_field_value() -> None:
    facet = standard_facet("dataset", "schema", {"fields": [{"name": "id"}]})
    facet["fields"] = [
        {"name": "api_key_should_not_leak"},
        {"name": "api_key_should_not_leak"},
    ]

    with pytest.raises(OpenLineageValidationError) as captured:
        validate_standard_facet("dataset", "schema", facet)

    assert "api_key_should_not_leak" not in str(captured.value)
    assert "unique" in str(captured.value)


def test_duplicate_dataset_references_fail_semantic_validation() -> None:
    _, job_event, _ = _representative_events()
    invalid = dict(job_event)
    invalid["outputs"] = list(cast(list[object], job_event["inputs"]))

    with pytest.raises(OpenLineageValidationError, match="both"):
        validate_event(invalid)


def _batch_events(
    event_types: tuple[str, ...],
    *,
    processing_type: str = "BATCH",
) -> list[dict[str, object]]:
    job = JobIdentity("lineage.acme", command_job_name("acme", "apply"))
    run = RunIdentity(RUN_ID)
    job_type = standard_facet(
        "job",
        "jobType",
        {
            "processingType": processing_type,
            "integration": "STREAMT",
            "jobType": "COMMAND",
        },
    )
    return [
        build_run_event(
            event_time=f"2026-09-01T12:{index:02d}:00Z",
            event_type=event_type,  # type: ignore[arg-type]
            run=run,
            job=job,
            job_facets={"jobType": job_type},
        )
        for index, event_type in enumerate(event_types)
    ]


@pytest.mark.parametrize("terminal", ["COMPLETE", "FAIL", "ABORT"])
def test_finite_batch_run_requires_one_start_then_one_terminal(terminal: str) -> None:
    validate_event_sequence(_batch_events(("START", "RUNNING", terminal)))


@pytest.mark.parametrize(
    "event_types",
    [
        ("START",),
        ("COMPLETE",),
        ("START", "START", "COMPLETE"),
        ("START", "FAIL", "ABORT"),
        ("COMPLETE", "START"),
    ],
)
def test_invalid_finite_batch_pairing_fails(event_types: tuple[str, ...]) -> None:
    with pytest.raises(OpenLineageValidationError):
        validate_event_sequence(_batch_events(event_types))


def test_streaming_run_does_not_require_a_terminal_event() -> None:
    validate_event_sequence(_batch_events(("START",), processing_type="STREAMING"))


def test_static_dataset_and_job_event_identities_must_be_unique() -> None:
    dataset_event, job_event, _ = _representative_events()
    with pytest.raises(OpenLineageValidationError, match="DatasetEvent identities"):
        validate_event_sequence([dataset_event, dataset_event])
    with pytest.raises(OpenLineageValidationError, match="JobEvent identities"):
        validate_event_sequence([job_event, job_event])


def test_one_run_identity_cannot_cross_job_identities() -> None:
    events = _batch_events(("START", "COMPLETE"))
    second = dict(events[1])
    second["job"] = {
        "namespace": "lineage.acme",
        "name": command_job_name("other", "apply"),
        "facets": events[1]["job"]["facets"],  # type: ignore[index]
    }

    with pytest.raises(OpenLineageValidationError, match="different jobs"):
        validate_event_sequence([events[0], second])


def test_installed_wheel_contains_resources_and_exports_openlineage_offline(
    tmp_path: Path,
) -> None:
    repository = Path(__file__).parents[2]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(tmp_path),
            str(repository),
        ],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(tmp_path.glob("streamt-*.whl"))

    with zipfile.ZipFile(wheel) as archive:
        _assert_openlineage_schema_resources(
            member_names=set(archive.namelist()),
            read_member=archive.read,
            source_distribution=False,
        )

    virtualenv = tmp_path / "venv"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "venv",
            str(virtualenv),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    scripts = virtualenv / ("Scripts" if os.name == "nt" else "bin")
    venv_python = scripts / ("python.exe" if os.name == "nt" else "python")
    streamt_executable = scripts / ("streamt.exe" if os.name == "nt" else "streamt")
    subprocess.run(
        [
            str(venv_python),
            "-m",
            "pip",
            "install",
            "--force-reinstall",
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    environment["PYTHONNOUSERSITE"] = "1"
    environment["STREAMT_WHEEL_VIRTUALENV"] = str(virtualenv)
    help_result = subprocess.run(
        [str(streamt_executable), "docs", "openlineage", "--help"],
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--job-namespace" in help_result.stdout
    assert "--kafka-namespace" in help_result.stdout
    assert "--gateway-namespace" in help_result.stdout

    project = tmp_path / "project"
    project.mkdir()
    (project / "stream_project.yml").write_text(
        """apiVersion: streamt.dev/v1alpha1
project:
  name: wheel-smoke
  version: 1.0.0
runtime:
  kafka:
    bootstrap_servers: offline.invalid:9092
sources:
  - name: events
    topic: events.v1
    columns:
      - name: id
        type: BIGINT
""",
        encoding="utf-8",
    )
    smoke_script = f"""
import json
import os
import socket
from pathlib import Path

import streamt
from click.testing import CliRunner

assert Path(streamt.__file__).resolve().is_relative_to(
    Path(os.environ["STREAMT_WHEEL_VIRTUALENV"]).resolve()
)

def fail_network(*_args, **_kwargs):
    raise AssertionError("installed OpenLineage export attempted network access")

socket.getaddrinfo = fail_network
socket.create_connection = fail_network
socket.socket.connect = fail_network
from streamt.cli import main

result = CliRunner().invoke(
    main,
    [
        "docs",
        "openlineage",
        "--project-dir",
        {str(project)!r},
        "--job-namespace",
        "lineage.test",
    ],
    catch_exceptions=False,
)
assert result.exit_code == 0, result.output
events = [json.loads(line) for line in result.stdout.splitlines()]
assert len(events) == 1
assert "DatasetEvent" in events[0]["schemaURL"]
assert events[0]["producer"] == "https://github.com/conduktor/streamt"
assert events[0]["dataset"] == {{
    "namespace": "kafka://offline.invalid:9092",
    "name": "events.v1",
    "facets": events[0]["dataset"]["facets"],
}}
"""
    smoke = subprocess.run(
        [str(venv_python), "-c", smoke_script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert smoke.returncode == 0, smoke.stderr
    assert smoke.stdout == ""


def test_built_wheel_and_source_distribution_contain_openlineage_resources(
    tmp_path: Path,
) -> None:
    configured_dist = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if configured_dist is None:
        if importlib.util.find_spec("build") is None:
            pytest.skip("the build frontend is required for source-distribution inspection")
        distribution_dir = tmp_path / "dist"
        repository = Path(__file__).parents[2]
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--sdist",
                "--outdir",
                str(distribution_dir),
            ],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        distribution_dir = Path(configured_dist)

    wheels = list(distribution_dir.glob("streamt-*.whl"))
    source_distributions = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(source_distributions) == 1, source_distributions

    with zipfile.ZipFile(wheels[0]) as wheel:
        _assert_openlineage_schema_resources(
            member_names=set(wheel.namelist()),
            read_member=wheel.read,
            source_distribution=False,
        )

    with tarfile.open(source_distributions[0], "r:gz") as source_distribution:

        def read_source_member(name: str) -> bytes:
            extracted = source_distribution.extractfile(name)
            assert extracted is not None
            return extracted.read()

        _assert_openlineage_schema_resources(
            member_names=set(source_distribution.getnames()),
            read_member=read_source_member,
            source_distribution=True,
        )


def test_openlineage_error_and_warning_identifiers_are_stable() -> None:
    assert ErrorCode.OPENLINEAGE_INVALID == "E506_OPENLINEAGE_INVALID"
    assert ErrorCode.OPENLINEAGE_SCHEMA_INCOMPLETE == "W110_OPENLINEAGE_SCHEMA_INCOMPLETE"
    assert ErrorCode.OPENLINEAGE_SINK_OUTPUT_OMITTED == "W111_OPENLINEAGE_SINK_OUTPUT_OMITTED"
