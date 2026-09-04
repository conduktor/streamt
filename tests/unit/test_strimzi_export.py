"""Contract tests for the pure Strimzi KafkaTopic mapper."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

import streamt.compiler.topic_artifact as topic_artifact_module
import streamt.integrations.gitops.strimzi as strimzi_module
import streamt.integrations.gitops.strimzi_validation as strimzi_validation_module
from streamt.integrations.gitops import (
    STRIMZI_API_VERSION,
    STRIMZI_KIND,
    STRIMZI_RELEASE,
    StrimziExportCounts,
    StrimziExportError,
    StrimziExportTarget,
    StrimziExportWarning,
    StrimziKafkaTopicExport,
    generate_strimzi_export,
)

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "strimzi" / "1.2.0"
CHECKSUM = "sha256:" + "a" * 64
BASE_COLLECTIONS = {
    "schemas": [],
    "topics": [],
    "flink_jobs": [],
    "test_jobs": [],
    "connectors": [],
    "gateway_rules": [],
}


class _HostileTruthValue:
    def __bool__(self) -> bool:
        raise RuntimeError("private-hostile-truth-sentinel")

    def __repr__(self) -> str:
        raise RuntimeError("private-hostile-repr-sentinel")


def _load_json(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _as_dict(value: object) -> dict[str, object]:
    assert type(value) is dict
    return value  # type: ignore[return-value]


def _as_list(value: object) -> list[object]:
    assert type(value) is list
    return value  # type: ignore[return-value]


def _topic(
    name: str = "managed-topic",
    *,
    mode: str = "managed",
    owner_type: str = "model",
    owner_name: str = "managed_model",
    config: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "partitions": 3,
        "replication_factor": 2,
        "config": {} if config is None else config,
        "ownership": {
            "mode": mode,
            "project": "public-project",
            "type": owner_type,
            "name": owner_name,
        },
    }


def _collections(*topics: dict[str, object]) -> dict[str, list[object]]:
    result: dict[str, list[object]] = copy.deepcopy(BASE_COLLECTIONS)
    result["topics"] = list(topics)
    return result


def _export(
    artifacts: object,
    *,
    project_name: object = "public-project",
    checksum: object = CHECKSUM,
    target: object | None = None,
) -> StrimziKafkaTopicExport:
    return generate_strimzi_export(
        artifacts,
        project_name=project_name,
        manifest_checksum=checksum,
        target=target or StrimziExportTarget("public-namespace", "public-cluster"),
    )


def _error_surface(error: BaseException) -> str:
    return "\n".join((str(error), repr(error), repr(error.args)))


def test_reviewed_fixture_documents_yaml_warnings_and_counts_are_exact() -> None:
    manifest = _as_dict(_load_json("manifest.json"))
    contract = _as_dict(_load_json("contract.json"))
    checksums = _as_dict(contract["manifest_checksums"])
    result = generate_strimzi_export(
        manifest["artifacts"],
        project_name=manifest["project"],
        manifest_checksum=checksums["baseline"],
        target=StrimziExportTarget(
            namespace=contract["namespace"],  # type: ignore[arg-type]
            cluster_name=contract["cluster_name"],  # type: ignore[arg-type]
        ),
    )

    expected_documents = tuple(_as_list(_load_json("expected-documents.json")))
    expected_warnings = tuple(
        StrimziExportWarning(**_as_dict(warning)) for warning in _as_list(contract["warnings"])
    )
    expected_counts = StrimziExportCounts(**_as_dict(contract["counts"]))
    expected_yaml = (FIXTURE_DIR / "expected.yaml").read_bytes()

    assert result.target_release == contract["target_release"] == STRIMZI_RELEASE
    assert result.api_version == contract["api_version"] == STRIMZI_API_VERSION
    assert result.kind == contract["kind"] == STRIMZI_KIND
    assert result.manifest_checksum == checksums["baseline"]
    assert result.documents == expected_documents
    assert result.warnings == expected_warnings
    assert result.counts == expected_counts
    assert result.counts.to_dict() == contract["counts"]
    assert result.yaml_bytes == expected_yaml
    assert result.yaml_text == result.yaml == expected_yaml.decode("utf-8")
    assert (
        "sha256:" + hashlib.sha256(result.yaml_bytes).hexdigest()
        == contract["canonical_yaml_sha256"]
    )
    assert list(yaml.safe_load_all(result.yaml_bytes)) == list(expected_documents)


def test_supplied_secret_neutral_checksum_is_the_only_whole_manifest_binding() -> None:
    contract = _as_dict(_load_json("contract.json"))
    checksums = _as_dict(contract["manifest_checksums"])
    baseline = _as_dict(_load_json("manifest.json"))
    secret_variant = _as_dict(_load_json("manifest-secret-variant.json"))
    nonsecret_variant = _as_dict(_load_json("manifest-nonsecret-variant.json"))
    target = StrimziExportTarget(
        namespace=contract["namespace"],  # type: ignore[arg-type]
        cluster_name=contract["cluster_name"],  # type: ignore[arg-type]
    )

    baseline_result = generate_strimzi_export(
        baseline["artifacts"],
        project_name=baseline["project"],
        manifest_checksum=checksums["baseline"],
        target=target,
    )
    secret_result = generate_strimzi_export(
        secret_variant["artifacts"],
        project_name=secret_variant["project"],
        manifest_checksum=checksums["secret_variant"],
        target=target,
    )
    nonsecret_result = generate_strimzi_export(
        nonsecret_variant["artifacts"],
        project_name=nonsecret_variant["project"],
        manifest_checksum=checksums["nonsecret_variant"],
        target=target,
    )

    assert baseline_result.yaml_bytes == secret_result.yaml_bytes
    assert baseline_result.documents == secret_result.documents
    assert baseline_result.yaml_bytes != nonsecret_result.yaml_bytes
    for document in nonsecret_result.documents:
        annotations = document["metadata"]["annotations"]  # type: ignore[index]
        assert (
            annotations["streamt.dev/manifest-checksum"]
            == checksums[  # type: ignore[index]
                "nonsecret_variant"
            ]
        )


def test_empty_export_is_exactly_zero_bytes_with_optional_removals_absent() -> None:
    manifest = _as_dict(_load_json("empty-manifest.json"))
    contract = _as_dict(_load_json("contract.json"))
    checksum = _as_dict(contract["manifest_checksums"])["empty"]
    result = generate_strimzi_export(
        manifest["artifacts"],
        project_name=manifest["project"],
        manifest_checksum=checksum,
        target=StrimziExportTarget("empty-namespace", "empty-cluster"),
    )

    assert result.documents == ()
    assert result.warnings == ()
    assert result.counts.to_dict() == contract["empty_counts"]
    assert result.yaml_bytes == (FIXTURE_DIR / "expected-empty.yaml").read_bytes() == b""
    assert result.yaml_text == result.yaml == ""


def test_empty_export_does_not_load_the_pinned_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_schema_load() -> object:
        raise AssertionError("empty document tuples do not need the CRD schema")

    monkeypatch.setattr(
        strimzi_validation_module,
        "_kafkatopic_validator",
        unexpected_schema_load,
    )
    result = _export(_collections())
    assert result.documents == ()
    assert result.yaml_bytes == b""


def test_topics_are_sorted_by_physical_then_kubernetes_name_across_runs() -> None:
    artifacts = _collections(
        _topic("z-topic", owner_name="z-model"),
        _topic("A_TOPIC", owner_type="source", owner_name="source_dlq"),
        _topic("a-topic", owner_name="a-model"),
    )
    first = _export(artifacts)
    second = _export(copy.deepcopy(artifacts))

    assert [document["spec"]["topicName"] for document in first.documents] == [  # type: ignore[index]
        "A_TOPIC",
        "a-topic",
        "z-topic",
    ]
    assert first.documents[0]["metadata"]["name"].startswith("streamt-topic-")  # type: ignore[index,union-attr]
    assert first.yaml_bytes == second.yaml_bytes
    assert first.documents == second.documents


@pytest.mark.parametrize("owner_type", ["model", "source"])
def test_compiler_created_managed_dlq_owner_types_are_emitted(owner_type: str) -> None:
    result = _export(
        _collections(
            _topic(
                "source-test-dlq",
                owner_type=owner_type,
                owner_name="public-dlq-owner",
            )
        )
    )
    annotations = result.documents[0]["metadata"]["annotations"]  # type: ignore[index]
    assert annotations["streamt.dev/owner-type"] == owner_type  # type: ignore[index]
    assert annotations["streamt.dev/owner-name"] == "public-dlq-owner"  # type: ignore[index]
    assert annotations["streamt.dev/ownership-mode"] == "managed"  # type: ignore[index]


def test_external_topics_are_omitted_with_exact_sorted_index_warnings() -> None:
    topics = [_topic(f"managed-{index}") for index in range(11)]
    topics[2] = _topic(
        "External_Two", mode="external", owner_type="source", owner_name="external-two"
    )
    topics[10] = _topic(
        "External_Ten", mode="external", owner_type="source", owner_name="external-ten"
    )
    result = _export(_collections(*topics))

    assert result.counts == StrimziExportCounts(9, 2, 0)
    assert result.warnings == (
        StrimziExportWarning(
            code="W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
            message="External topic artifact omitted from Strimzi export",
            location="artifacts/topics/10/ownership",
        ),
        StrimziExportWarning(
            code="W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
            message="External topic artifact omitted from Strimzi export",
            location="artifacts/topics/2/ownership",
        ),
    )
    surfaces = result.yaml_text + repr(result.warnings)
    for omitted_identity in ("External_Two", "external-two", "External_Ten", "external-ten"):
        assert omitted_identity not in surfaces


def test_all_seven_non_topic_counts_include_zeroes_and_both_removals() -> None:
    artifacts = _collections(_topic())
    artifacts.update(
        {
            "schemas": [{}],
            "flink_jobs": [{}, {}],
            "test_jobs": [{}, {}, {}],
            "connectors": [{}, {}, {}, {}],
            "connector_removals": [{}, {}, {}, {}, {}],
            "gateway_rules": [{}, {}, {}, {}, {}, {}],
            "gateway_rule_removals": [{}, {}, {}, {}, {}, {}, {}],
        }
    )
    result = _export(artifacts)

    assert result.counts == StrimziExportCounts(1, 0, 28)
    assert result.warnings == (
        StrimziExportWarning(
            code="W121_STRIMZI_ARTIFACTS_OMITTED",
            message="Non-topic artifacts omitted from Strimzi export",
            location=(
                "artifacts/omitted/schemas=1,flink_jobs=2,test_jobs=3,connectors=4,"
                "connector_removals=5,gateway_rules=6,gateway_rule_removals=7"
            ),
        ),
    )


@pytest.mark.parametrize("present_kind", ["connector_removals", "gateway_rule_removals"])
def test_each_additive_removal_collection_is_independently_optional(
    present_kind: str,
) -> None:
    artifacts = _collections()
    artifacts[present_kind] = [{"confidential": "private-removal-payload"}]
    result = _export(artifacts)
    expected_counts = {
        "connector_removals": 1 if present_kind == "connector_removals" else 0,
        "gateway_rule_removals": 1 if present_kind == "gateway_rule_removals" else 0,
    }
    assert result.counts == StrimziExportCounts(0, 0, 1)
    assert result.warnings[0].location == (
        "artifacts/omitted/schemas=0,flink_jobs=0,test_jobs=0,connectors=0,"
        f"connector_removals={expected_counts['connector_removals']},gateway_rules=0,"
        f"gateway_rule_removals={expected_counts['gateway_rule_removals']}"
    )
    assert "private-removal-payload" not in repr(result.warnings)


@pytest.mark.parametrize("kind", ["connector_removals", "gateway_rule_removals"])
def test_present_additive_removal_collection_must_still_be_an_exact_list(kind: str) -> None:
    artifacts: dict[str, object] = _collections()
    artifacts[kind] = ()
    with pytest.raises(StrimziExportError) as raised:
        _export(artifacts)
    assert raised.value.location == f"artifacts/{kind}"


@pytest.mark.parametrize(
    ("mutation", "expected_location"),
    [
        (lambda value: value.update({"unknown": []}), "artifacts"),
        (lambda value: value.pop("schemas"), "artifacts"),
        (lambda value: value.__setitem__("schemas", ()), "artifacts/schemas"),
        (lambda value: value.__setitem__(1, []), "artifacts"),
    ],
)
def test_artifact_collection_boundary_is_closed(
    mutation: object,
    expected_location: str,
) -> None:
    artifacts: dict[object, object] = _collections()
    mutation(artifacts)  # type: ignore[operator]
    with pytest.raises(StrimziExportError) as raised:
        _export(artifacts)
    assert raised.value.location == expected_location
    assert str(raised.value) == "Strimzi export input is invalid"


@pytest.mark.parametrize(
    ("field", "value", "location"),
    [
        ("project_name", "", "project"),
        ("project_name", "project\x00private", "project"),
        ("checksum", "sha256:not-a-checksum", "manifest_checksum"),
        ("target", object(), "target"),
    ],
)
def test_primitive_inputs_fail_at_safe_structural_locations(
    field: str,
    value: object,
    location: str,
) -> None:
    kwargs: dict[str, object] = {}
    kwargs[field] = value
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(), **kwargs)
    assert raised.value.location == location
    assert "private" not in _error_surface(raised.value)


@pytest.mark.parametrize("field", ["project", "owner"])
def test_hostile_truthiness_is_contained_without_rendering(field: str) -> None:
    hostile = _HostileTruthValue()
    topic = _topic()
    kwargs: dict[str, object] = {}
    if field == "project":
        kwargs["project_name"] = hostile
    else:
        ownership = topic["ownership"]
        assert isinstance(ownership, dict)
        ownership["name"] = hostile

    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(topic), **kwargs)
    assert raised.value.location == ("project" if field == "project" else "artifacts/topics")
    assert "private-hostile" not in _error_surface(raised.value)


@pytest.mark.parametrize(
    ("namespace", "cluster_name", "location"),
    [
        ("UPPER", "valid", "target.namespace"),
        ("valid", "bad_cluster", "target.cluster_name"),
        ("a" * 64, "valid", "target.namespace"),
        ("valid", "", "target.cluster_name"),
    ],
)
def test_target_is_exact_dns1123_and_secret_neutral(
    namespace: str,
    cluster_name: str,
    location: str,
) -> None:
    with pytest.raises(StrimziExportError) as raised:
        StrimziExportTarget(namespace, cluster_name)
    assert raised.value.location == location
    for sentinel in ("UPPER", "bad_cluster", "a" * 64):
        assert sentinel not in _error_surface(raised.value)


@pytest.mark.parametrize(
    ("project_name", "owner_name", "location"),
    [
        ("public-project\x00private", "owner", "project"),
        ("public-project\ud800", "owner", "project"),
        ("public-project\ufeffprivate", "owner", "project"),
        ("public-project\ufffeprivate", "owner", "project"),
        ("public-project\uffffprivate", "owner", "project"),
        ("public-project\U0010ffffprivate", "owner", "project"),
        ("public-project", "owner\x01private", "artifacts/topics"),
        ("public-project", "owner\udfffprivate", "artifacts/topics"),
        ("public-project", "owner\ufeffprivate", "artifacts/topics"),
        ("public-project", "owner\ufffeprivate", "artifacts/topics"),
        ("public-project", "owner\uffffprivate", "artifacts/topics"),
        ("public-project", "owner\U0010ffffprivate", "artifacts/topics"),
    ],
)
def test_control_and_surrogate_public_identities_fail_without_echo(
    project_name: str,
    owner_name: str,
    location: str,
) -> None:
    topic = _topic(owner_name=owner_name)
    topic["ownership"]["project"] = project_name  # type: ignore[index]
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(topic), project_name=project_name)
    assert raised.value.location == location
    assert "private" not in _error_surface(raised.value)


@pytest.mark.parametrize("long_field", ["project", "owner"])
def test_annotation_limit_rejects_long_public_identities_without_echo(long_field: str) -> None:
    long_identity = "public-identity-" + "x" * (256 * 1024)
    project_name = long_identity if long_field == "project" else "public-project"
    owner_name = long_identity if long_field == "owner" else "public-owner"
    topic = _topic(owner_name=owner_name)
    topic["ownership"]["project"] = project_name  # type: ignore[index]
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(topic), project_name=project_name)
    assert raised.value.location == "documents"
    assert long_identity not in _error_surface(raised.value)


def test_safe_unicode_project_and_owner_identities_are_preserved_exactly() -> None:
    topic = _topic(owner_name="Équipe-Modèle")
    topic["ownership"]["project"] = "Projet-Événements"  # type: ignore[index]
    result = _export(_collections(topic), project_name="Projet-Événements")
    annotations = result.documents[0]["metadata"]["annotations"]  # type: ignore[index]
    assert annotations["streamt.dev/project"] == "Projet-Événements"  # type: ignore[index]
    assert annotations["streamt.dev/owner-name"] == "Équipe-Modèle"  # type: ignore[index]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda topic: topic.__setitem__("partitions", True),
        lambda topic: topic.__setitem__("extra", "confidential-rejected-value"),
        lambda topic: topic.__setitem__("config", {"api.token": "confidential-rejected-value"}),
        lambda topic: topic["ownership"].__setitem__("mode", "adopted"),
        lambda topic: topic["ownership"].__setitem__("project", "wrong-project"),
    ],
)
def test_malformed_and_adopted_topics_fail_without_rejected_values(
    mutation: object,
) -> None:
    topic = _topic()
    mutation(topic)  # type: ignore[operator]
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(topic))
    surface = _error_surface(raised.value)
    assert raised.value.location == "artifacts/topics"
    for sentinel in ("confidential-rejected-value", "adopted", "wrong-project"):
        assert sentinel not in surface


def test_external_topics_are_still_strictly_parsed_before_omission() -> None:
    external = _topic(
        "External_Public", mode="external", owner_type="source", owner_name="external-owner"
    )
    external["config"] = {"password": "private-external-config"}
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(external))
    assert raised.value.location == "artifacts/topics"
    assert "private-external-config" not in _error_surface(raised.value)


@pytest.mark.parametrize("value", ["\ufeff", "\ufffe", "\uffff", "\U0010ffff"])
def test_config_text_that_would_require_unicode_escaping_fails_closed(value: str) -> None:
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(_topic(config={"ordinary": f"private{value}"})))
    assert raised.value.location == "artifacts/topics"
    assert "private" not in _error_surface(raised.value)


def test_duplicate_kafka_and_generated_kubernetes_identities_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    duplicate = _collections(_topic("same-topic"), _topic("same-topic"))
    with pytest.raises(StrimziExportError) as raised:
        _export(duplicate)
    assert raised.value.location == "artifacts/topics"

    monkeypatch.setattr(topic_artifact_module, "kafka_topic_metadata_name", lambda _name: "same")
    collision = _collections(_topic("first-topic"), _topic("second-topic"))
    with pytest.raises(StrimziExportError) as collision_raised:
        _export(collision)
    assert collision_raised.value.location == "artifacts/topics"


def test_caller_and_returned_document_mutations_cannot_change_result() -> None:
    source_config = {"cleanup.policy": "compact"}
    artifacts = _collections(_topic(config=source_config))
    result = _export(artifacts)
    expected_documents = result.documents
    expected_yaml = result.yaml_bytes

    source_config["cleanup.policy"] = "delete"
    artifacts["topics"][0]["name"] = "mutated-topic"  # type: ignore[index]
    returned = result.documents
    returned[0]["metadata"]["name"] = "mutated-name"  # type: ignore[index]
    returned[0]["spec"]["config"]["cleanup.policy"] = "mutated"  # type: ignore[index]

    assert result.documents == expected_documents
    assert result.yaml_bytes == expected_yaml
    assert result.documents is not result.documents
    assert result.documents[0] is not result.documents[0]


def test_immutable_records_cannot_be_assigned() -> None:
    target = StrimziExportTarget("public-namespace", "public-cluster")
    warning = StrimziExportWarning("code", "message", "location")
    counts = StrimziExportCounts(1, 2, 3)
    result = _export(_collections())
    for value, field, replacement in (
        (target, "namespace", "changed"),
        (warning, "code", "changed"),
        (counts, "emitted_topics", 4),
        (result, "manifest_checksum", "changed"),
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(value, field, replacement)


def test_result_constructor_revalidates_payloads_and_yaml_parity() -> None:
    result = _export(_collections(_topic()))
    with pytest.raises(TypeError):
        StrimziKafkaTopicExport()
    with pytest.raises(StrimziExportError) as raised:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=result.manifest_checksum,
            counts=result.counts,
            document_payloads=result._document_payloads,
            warnings=result.warnings,
            yaml_bytes=b"---\nnot: the canonical document\n",
        )
    assert raised.value.location == "export.yaml"


def test_result_factory_rejects_arbitrary_or_leaking_warning_payloads() -> None:
    result = _export(_collections())
    fabricated = StrimziExportWarning(
        code="W999_PRIVATE",
        message="confidential-warning-payload",
        location="private/location",
    )
    with pytest.raises(StrimziExportError) as raised:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=result.manifest_checksum,
            counts=result.counts,
            document_payloads=result._document_payloads,
            warnings=(fabricated,),
            yaml_bytes=result.yaml_bytes,
        )
    assert raised.value.location == "export.warnings"
    assert "confidential-warning-payload" not in _error_surface(raised.value)

    non_string_warning = StrimziExportWarning(  # type: ignore[arg-type]
        code=1,
        message="fixed",
        location="fixed",
    )
    with pytest.raises(StrimziExportError) as non_string_error:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=result.manifest_checksum,
            counts=result.counts,
            document_payloads=result._document_payloads,
            warnings=(non_string_warning, fabricated),
            yaml_bytes=result.yaml_bytes,
        )
    assert non_string_error.value.location == "export.warnings"


@pytest.mark.parametrize(
    ("warning", "counts", "location"),
    [
        (
            StrimziExportWarning(
                "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
                "External topic artifact omitted from Strimzi export",
                "artifacts/topics/01/ownership",
            ),
            StrimziExportCounts(0, 1, 0),
            "export.warnings",
        ),
        (
            StrimziExportWarning(
                "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
                "wrong fixed message",
                "artifacts/topics/1/ownership",
            ),
            StrimziExportCounts(0, 1, 0),
            "export.warnings",
        ),
        (
            StrimziExportWarning(
                "W121_STRIMZI_ARTIFACTS_OMITTED",
                "Non-topic artifacts omitted from Strimzi export",
                (
                    "artifacts/omitted/schemas=1,flink_jobs=0,test_jobs=0,connectors=0,"
                    "connector_removals=0,gateway_rules=0,gateway_rule_removals=0"
                ),
            ),
            StrimziExportCounts(0, 0, 2),
            "export.counts",
        ),
        (
            StrimziExportWarning(
                "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED",
                "External topic artifact omitted from Strimzi export",
                "artifacts/topics/2/ownership",
            ),
            StrimziExportCounts(0, 1, 0),
            "export.warnings",
        ),
    ],
)
def test_result_factory_rejects_malformed_or_inconsistent_warnings(
    warning: StrimziExportWarning,
    counts: StrimziExportCounts,
    location: str,
) -> None:
    empty = _export(_collections())
    with pytest.raises(StrimziExportError) as raised:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=empty.manifest_checksum,
            counts=counts,
            document_payloads=empty._document_payloads,
            warnings=(warning,),
            yaml_bytes=empty.yaml_bytes,
        )
    assert raised.value.location == location


def test_result_factory_binds_counts_warnings_and_manifest_checksum() -> None:
    result = _export(_collections(_topic()))
    with pytest.raises(StrimziExportError) as count_error:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=result.manifest_checksum,
            counts=StrimziExportCounts(2, 0, 0),
            document_payloads=result._document_payloads,
            warnings=result.warnings,
            yaml_bytes=result.yaml_bytes,
        )
    assert count_error.value.location == "export.counts"

    with pytest.raises(StrimziExportError) as checksum_error:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum="sha256:" + "b" * 64,
            counts=result.counts,
            document_payloads=result._document_payloads,
            warnings=result.warnings,
            yaml_bytes=result.yaml_bytes,
        )
    assert checksum_error.value.location == "export.manifest_checksum"

    for inconsistent_counts in (
        StrimziExportCounts(1, 1, 0),
        StrimziExportCounts(1, 0, 1),
    ):
        with pytest.raises(StrimziExportError) as warning_count_error:
            StrimziKafkaTopicExport._from_validated(
                manifest_checksum=result.manifest_checksum,
                counts=inconsistent_counts,
                document_payloads=result._document_payloads,
                warnings=(),
                yaml_bytes=result.yaml_bytes,
            )
        assert warning_count_error.value.location == "export.counts"


def test_result_factory_rejects_noncanonical_document_payload() -> None:
    result = _export(_collections(_topic()))
    noncanonical = tuple(f" {payload}\n" for payload in result._document_payloads)
    with pytest.raises(StrimziExportError) as raised:
        StrimziKafkaTopicExport._from_validated(
            manifest_checksum=result.manifest_checksum,
            counts=result.counts,
            document_payloads=noncanonical,
            warnings=result.warnings,
            yaml_bytes=result.yaml_bytes,
        )
    assert raised.value.location == "export.documents"


def test_mapper_validates_defensive_document_copies_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[dict[str, object], ...]] = []
    real_validate = strimzi_module.validate_kafkatopic_documents

    def capture(documents: object) -> None:
        assert type(documents) is tuple
        seen.append(documents)  # type: ignore[arg-type]
        real_validate(documents)

    monkeypatch.setattr(strimzi_module, "validate_kafkatopic_documents", capture)
    result = _export(_collections(_topic()))

    assert len(seen) == 2
    assert seen[0] == seen[1] == result.documents
    assert seen[0] is not seen[1]
    assert seen[0][0] is not seen[1][0]


def test_yaml_is_safe_canonical_alias_free_and_string_preserving() -> None:
    result = _export(
        _collections(
            _topic(
                "Unicode_Topic",
                owner_name="Équipe",
                config={
                    "a.boolean": True,
                    "b.integer": -42,
                    "c.string": "001: value # literal",
                    "d.unicode": "café",
                },
            )
        )
    )
    payload = result.yaml_bytes
    parsed = list(yaml.safe_load_all(payload))
    config = parsed[0]["spec"]["config"]

    assert config == {
        "a.boolean": "true",
        "b.integer": "-42",
        "c.string": "001: value # literal",
        "d.unicode": "café",
    }
    assert all(type(value) is str for value in config.values())
    assert payload.startswith(b"---\n")
    assert payload.endswith(b"\n")
    assert not payload.endswith(b"\n\n")
    assert b"\r" not in payload
    assert b"...\n" not in payload
    assert b"!!" not in payload
    assert b"&id" not in payload
    assert b"*id" not in payload
    assert "café" in result.yaml_text


@pytest.mark.parametrize(
    "value",
    [
        "1e3",
        "1E+3",
        "0o7",
        "+0O7",
        "08",
        "1_0e3",
        "2026-9-4",
        "yes",
        "<<",
        "_1",
        "2026-_9-4",
        "1e99999",
    ],
)
def test_conservative_kubernetes_go_yaml_predicate_requires_quotes(value: str) -> None:
    assert strimzi_module._requires_quoted_yaml_string(value)


@pytest.mark.parametrize("value", ["1.2.0", "orders-v1", "1e", "0o8"])
def test_unambiguous_fixture_strings_do_not_gain_forced_quotes(value: str) -> None:
    assert not strimzi_module._requires_quoted_yaml_string(value)


def test_kubernetes_go_yaml_ambiguous_fields_remain_explicit_strings() -> None:
    topic = _topic("1e3", owner_name="0o7", config={"0o7": "1E+3"})
    topic["ownership"]["project"] = "1E+3"  # type: ignore[index]
    result = _export(
        _collections(topic),
        project_name="1E+3",
        target=StrimziExportTarget("1e3", "0o7"),
    )

    assert "name: '1e3'" in result.yaml_text
    assert "namespace: '1e3'" in result.yaml_text
    assert "strimzi.io/cluster: '0o7'" in result.yaml_text
    assert "streamt.dev/owner-name: '0o7'" in result.yaml_text
    assert "streamt.dev/project: '1E+3'" in result.yaml_text
    assert "topicName: '1e3'" in result.yaml_text
    assert "'0o7': '1E+3'" in result.yaml_text
    assert "streamt.dev/strimzi-release: 1.2.0" in result.yaml_text

    document = result.documents[0]
    metadata = document["metadata"]
    spec = document["spec"]
    assert isinstance(metadata, dict)
    assert isinstance(spec, dict)
    assert metadata["name"] == "1e3"
    assert metadata["namespace"] == "1e3"
    assert spec["topicName"] == "1e3"
    assert spec["config"] == {"0o7": "1E+3"}


def test_success_and_failure_surfaces_exclude_confidential_omitted_inputs() -> None:
    private_sentinels = (
        "private-runtime-endpoint",
        "private-schema-registry",
        "private-flink-sql",
        "private-connect-config",
        "private-gateway-rule",
        "private-state-value",
        "private-connection-config",
        "private-omitted-tag",
    )
    artifacts = _collections(
        _topic(
            "Public_Topic",
            owner_name="PublicOwner",
            config={"retention.ms": 1000},
        )
    )
    artifacts.update(
        {
            "schemas": [
                {
                    "runtime": private_sentinels[0],
                    "registry": private_sentinels[1],
                    "state": private_sentinels[5],
                    "connection": private_sentinels[6],
                }
            ],
            "flink_jobs": [{"sql": private_sentinels[2]}],
            "connectors": [{"config": private_sentinels[3]}],
            "gateway_rules": [{"interceptor": private_sentinels[4]}],
            "test_jobs": [{"tag": private_sentinels[7]}],
        }
    )
    result = _export(artifacts)
    success_surface = "\n".join(
        (
            result.yaml_text,
            repr(result),
            repr(result.warnings),
            json.dumps(result.counts.to_dict()),
        )
    )
    for sentinel in private_sentinels:
        assert sentinel not in success_surface

    assert success_surface.count("Public_Topic") == 1
    assert success_surface.count("PublicOwner") == 1
    assert "public-project" in result.yaml_text
    assert "public-project" not in repr(result)
    assert "public-project" not in repr(result.warnings)


def test_yaml_serializer_failure_is_bounded_and_secret_neutral(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> str:
        raise yaml.YAMLError("confidential-library-exception")

    monkeypatch.setattr(strimzi_module.yaml, "dump_all", fail)
    with pytest.raises(StrimziExportError) as raised:
        _export(_collections(_topic()))
    assert raised.value.location == "documents"
    assert "confidential-library-exception" not in _error_surface(raised.value)


def test_public_import_boundary_does_not_load_runtime_or_deployment_modules(
    tmp_path: Path,
) -> None:
    script = """
import sys
import streamt.integrations.gitops

forbidden = (
    'streamt.core.runtime',
    'streamt.core.deployment_state',
    'streamt.deployer',
    'streamt.planner',
    'streamt.provider',
    'streamt.providers',
    'streamt.state',
)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + '.') for prefix in forbidden)
)
if loaded:
    raise SystemExit(','.join(loaded))
"""
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    completed = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
