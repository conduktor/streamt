"""Review-only contract checks for the frozen Strimzi 1.2.0 fixtures."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path

import yaml

from streamt.compiler.manifest import Manifest
from streamt.deployer.plan_file import manifest_checksum

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "strimzi" / "1.2.0"
JSON_FIXTURES = (
    "contract.json",
    "empty-manifest.json",
    "expected-documents.json",
    "expected-empty-documents.json",
    "manifest-nonsecret-variant.json",
    "manifest-secret-variant.json",
    "manifest.json",
)
OTHER_ARTIFACT_KINDS = (
    "schemas",
    "flink_jobs",
    "test_jobs",
    "connectors",
    "connector_removals",
    "gateway_rules",
    "gateway_rule_removals",
)
SENSITIVE_KEY = re.compile(
    r"(^|[._-])(?:password|passwd|secret|token|api[_-]?key|authorization|credentials?"
    r"|basic[._-]auth[._-]user[._-]info|sasl[._-]jaas[._-]config)($|[._-])",
    re.IGNORECASE,
)


class _CanonicalDumper(yaml.SafeDumper):
    def ignore_aliases(self, _data: object) -> bool:
        return True


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate fixture key: {key}")
        result[key] = value
    return result


def _load_json(name: str) -> object:
    return json.loads(
        (FIXTURE_DIR / name).read_text(encoding="utf-8"),
        object_pairs_hook=_strict_object,
    )


def _as_dict(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    assert all(isinstance(key, str) for key in value)
    return value


def _as_list(value: object) -> list[object]:
    assert isinstance(value, list)
    return value


def _redact_sensitive(value: object) -> object:
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for raw_key, item in value.items():
            assert isinstance(raw_key, str)
            result[raw_key] = (
                "<redacted>" if SENSITIVE_KEY.search(raw_key) else _redact_sensitive(item)
            )
        return result
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _fixture_manifest_checksum(value: object) -> str:
    normalized = _as_dict(_redact_sensitive(value))
    normalized.pop("compiled_at", None)
    payload = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _sha256(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _canonical_yaml(documents: list[object]) -> bytes:
    if not documents:
        return b""
    return yaml.dump_all(
        documents,
        Dumper=_CanonicalDumper,
        allow_unicode=True,
        default_flow_style=False,
        explicit_start=True,
        explicit_end=False,
        sort_keys=False,
        line_break="\n",
        width=4096,
    ).encode("utf-8")


def _warnings_and_counts(
    manifest: dict[str, object],
) -> tuple[list[dict[str, str]], dict[str, int]]:
    artifacts = _as_dict(manifest["artifacts"])
    assert not set(artifacts).difference({"topics", *OTHER_ARTIFACT_KINDS})
    topics = _as_list(artifacts.get("topics", []))
    managed_count = 0
    external_indexes: list[int] = []
    for index, raw_topic in enumerate(topics):
        topic = _as_dict(raw_topic)
        ownership = _as_dict(topic["ownership"])
        if ownership["mode"] == "managed":
            managed_count += 1
        elif ownership["mode"] == "external":
            external_indexes.append(index)

    omitted_counts = {kind: len(_as_list(artifacts.get(kind, []))) for kind in OTHER_ARTIFACT_KINDS}
    other_count = sum(omitted_counts.values())
    warnings: list[dict[str, str]] = []
    if other_count:
        location_counts = ",".join(
            f"{kind}={omitted_counts[kind]}" for kind in OTHER_ARTIFACT_KINDS
        )
        warnings.append(
            {
                "code": "W121_STRIMZI_ARTIFACTS_OMITTED",
                "message": "Non-topic artifacts omitted from Strimzi export",
                "location": f"artifacts/omitted/{location_counts}",
            }
        )
    warnings.extend(
        {
            "code": "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
            "message": "External topic artifact omitted from Strimzi export",
            "location": f"artifacts/topics/{index}/ownership",
        }
        for index in external_indexes
    )
    warnings.sort(key=lambda warning: (warning["location"], warning["code"]))
    return warnings, {
        "emitted_topics": managed_count,
        "external_topics_omitted": len(external_indexes),
        "other_artifacts_omitted": other_count,
    }


def test_all_reviewed_json_fixtures_are_strict_utf8_objects_or_arrays() -> None:
    for name in JSON_FIXTURES:
        fixture_bytes = (FIXTURE_DIR / name).read_bytes()
        assert fixture_bytes.endswith(b"\n")
        assert b"\r" not in fixture_bytes
        assert isinstance(_load_json(name), (dict, list))


def test_manifest_checksum_vectors_freeze_secret_neutral_boundary() -> None:
    contract = _as_dict(_load_json("contract.json"))
    expected = _as_dict(contract["manifest_checksums"])
    fixture_names = {
        "baseline": "manifest.json",
        "secret_variant": "manifest-secret-variant.json",
        "nonsecret_variant": "manifest-nonsecret-variant.json",
        "empty": "empty-manifest.json",
    }

    raw_manifests: dict[str, dict[str, object]] = {}
    for vector, name in fixture_names.items():
        raw = _as_dict(_load_json(name))
        raw_manifests[vector] = raw
        assert _fixture_manifest_checksum(raw) == expected[vector]
        assert manifest_checksum(Manifest.load(FIXTURE_DIR / name)) == expected[vector]

    baseline = raw_manifests["baseline"]
    secret_variant = raw_manifests["secret_variant"]
    nonsecret_variant = raw_manifests["nonsecret_variant"]
    assert baseline != secret_variant
    assert _redact_sensitive(baseline) == _redact_sensitive(secret_variant)
    assert expected["baseline"] == expected["secret_variant"]
    assert _redact_sensitive(baseline) != _redact_sensitive(nonsecret_variant)
    assert expected["baseline"] != expected["nonsecret_variant"]

    different_compile_time = copy.deepcopy(baseline)
    different_compile_time["compiled_at"] = "2099-12-31T23:59:59Z"
    assert _fixture_manifest_checksum(different_compile_time) == expected["baseline"]


def test_reviewed_documents_and_canonical_yaml_are_exact() -> None:
    contract = _as_dict(_load_json("contract.json"))
    documents = _as_list(_load_json("expected-documents.json"))
    expected_yaml = (FIXTURE_DIR / "expected.yaml").read_bytes()

    assert _canonical_yaml(documents) == expected_yaml
    assert _sha256(expected_yaml) == contract["canonical_yaml_sha256"]
    assert list(yaml.safe_load_all(expected_yaml)) == documents
    assert expected_yaml.startswith(b"---\n")
    assert expected_yaml.count(b"---\n") == len(documents)
    assert expected_yaml.endswith(b"\n")
    assert not expected_yaml.endswith(b"\n\n")
    assert b"\r" not in expected_yaml
    assert b"...\n" not in expected_yaml
    assert b"!!" not in expected_yaml
    assert b"&" not in expected_yaml
    assert b"*" not in expected_yaml

    confidential = _as_list(contract["confidential_input_sentinels"])
    omitted_identities = _as_list(contract["omitted_external_public_identities"])
    expected_json = (FIXTURE_DIR / "expected-documents.json").read_bytes()
    warning_surface = json.dumps(contract["warnings"], ensure_ascii=False).encode("utf-8")
    for sentinel in [*confidential, *omitted_identities]:
        assert isinstance(sentinel, str)
        assert sentinel.encode("utf-8") not in expected_yaml
        assert sentinel.encode("utf-8") not in expected_json
        assert sentinel.encode("utf-8") not in warning_surface
    assert len(confidential) == 2
    baseline_sentinel, variant_sentinel = confidential
    assert isinstance(baseline_sentinel, str)
    assert isinstance(variant_sentinel, str)
    assert baseline_sentinel in (FIXTURE_DIR / "manifest.json").read_text(encoding="utf-8")
    assert variant_sentinel in (FIXTURE_DIR / "manifest-secret-variant.json").read_text(
        encoding="utf-8"
    )


def test_document_shape_identity_order_and_config_scalars_are_frozen() -> None:
    contract = _as_dict(_load_json("contract.json"))
    documents = [_as_dict(item) for item in _as_list(_load_json("expected-documents.json"))]
    identities = [_as_dict(item) for item in _as_list(contract["identities"])]
    checksum = _as_dict(contract["manifest_checksums"])["baseline"]

    assert [_as_dict(document["spec"])["topicName"] for document in documents] == [
        "Orders_READY_v1",
        "orders-ready-v1",
    ]
    for document, identity in zip(documents, identities, strict=True):
        assert list(document) == ["apiVersion", "kind", "metadata", "spec"]
        assert document["apiVersion"] == contract["api_version"]
        assert document["kind"] == contract["kind"]
        metadata = _as_dict(document["metadata"])
        assert list(metadata) == ["name", "namespace", "labels", "annotations"]
        assert metadata["name"] == identity["metadata_name"]
        assert metadata["namespace"] == contract["namespace"]
        topic_name = _as_dict(document["spec"])["topicName"]
        assert isinstance(topic_name, str)
        assert (
            hashlib.sha256(topic_name.encode("utf-8")).hexdigest() == identity["topic_name_sha256"]
        )

        labels = _as_dict(metadata["labels"])
        assert list(labels) == ["strimzi.io/cluster", "app.kubernetes.io/managed-by"]
        assert labels == {
            "strimzi.io/cluster": contract["cluster_name"],
            "app.kubernetes.io/managed-by": "streamt",
        }
        annotations = _as_dict(metadata["annotations"])
        assert list(annotations) == [
            "streamt.dev/manifest-checksum",
            "streamt.dev/owner-name",
            "streamt.dev/owner-type",
            "streamt.dev/ownership-mode",
            "streamt.dev/project",
            "streamt.dev/strimzi-release",
        ]
        assert annotations["streamt.dev/manifest-checksum"] == checksum
        assert annotations["streamt.dev/ownership-mode"] == "managed"
        assert annotations["streamt.dev/project"] == "payments-streaming"
        assert annotations["streamt.dev/strimzi-release"] == contract["target_release"]

        spec = _as_dict(document["spec"])
        assert list(spec) == ["topicName", "partitions", "replicas", "config"]
        config = _as_dict(spec["config"])
        assert list(config) == sorted(config)
        assert all(isinstance(value, str) for value in config.values())

    hashed = identities[0]
    digest = hashed["topic_name_sha256"]
    assert isinstance(digest, str)
    assert len(digest) == 64
    assert hashed["metadata_name"] == f"streamt-topic-{digest}"
    assert hashed["naming_path"] == "full_sha256"
    assert identities[1]["metadata_name"] == identities[1]["topic_name"]
    assert identities[1]["naming_path"] == "direct_dns1123"
    assert _as_dict(_as_dict(documents[0]["spec"])["config"]) == {
        "compression.type": "lz4",
        "min.insync.replicas": "2",
        "unclean.leader.election.enable": "true",
    }
    assert _as_dict(_as_dict(documents[1]["spec"])["config"]) == {
        "cleanup.policy": "compact",
        "delete.retention.ms": "86400000",
        "remote.storage.enable": "false",
    }


def test_exact_warnings_counts_and_zero_byte_empty_stream_are_frozen() -> None:
    contract = _as_dict(_load_json("contract.json"))
    manifest = _as_dict(_load_json("manifest.json"))
    warnings, counts = _warnings_and_counts(manifest)
    expected_warnings = [_as_dict(item) for item in _as_list(contract["warnings"])]

    assert warnings == expected_warnings
    assert len(warnings) == contract["warning_count"]
    assert warnings == sorted(warnings, key=lambda warning: (warning["location"], warning["code"]))
    assert all(list(warning) == ["code", "message", "location"] for warning in warnings)
    assert counts == contract["counts"]

    empty_manifest = _as_dict(_load_json("empty-manifest.json"))
    empty_warnings, empty_counts = _warnings_and_counts(empty_manifest)
    empty_documents = _as_list(_load_json("expected-empty-documents.json"))
    empty_yaml = (FIXTURE_DIR / "expected-empty.yaml").read_bytes()
    assert empty_documents == []
    assert empty_warnings == contract["empty_warnings"] == []
    assert empty_counts == contract["empty_counts"]
    assert empty_yaml == _canonical_yaml(empty_documents) == b""
    assert _sha256(empty_yaml) == contract["empty_yaml_sha256"]
