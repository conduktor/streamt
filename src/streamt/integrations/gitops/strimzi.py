"""Pure compiled-topic mapping to canonical Strimzi KafkaTopic documents."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import cast

import yaml

from streamt.compiler.topic_artifact import (
    ParsedTopicArtifact,
    TopicArtifactFormatError,
    is_canonical_yaml_text,
    parse_compiled_topic_artifacts,
    validate_dns1123_label,
)
from streamt.integrations.gitops.strimzi_validation import (
    STRIMZI_API_VERSION,
    STRIMZI_KIND,
    STRIMZI_RELEASE,
    StrimziResourceError,
    StrimziValidationError,
    validate_kafkatopic_documents,
)

_CHECKSUM = re.compile(r"sha256:[0-9a-f]{64}\Z", re.ASCII)
_REQUIRED_ARTIFACT_KINDS = frozenset(
    {"schemas", "topics", "flink_jobs", "test_jobs", "connectors", "gateway_rules"}
)
_REMOVAL_ARTIFACT_KINDS = frozenset({"connector_removals", "gateway_rule_removals"})
_OTHER_ARTIFACT_KINDS = (
    "schemas",
    "flink_jobs",
    "test_jobs",
    "connectors",
    "connector_removals",
    "gateway_rules",
    "gateway_rule_removals",
)
_EXTERNAL_WARNING_CODE = "W120_STRIMZI_EXTERNAL_TOPIC_OMITTED"
_EXTERNAL_WARNING_MESSAGE = "External topic artifact omitted from Strimzi export"
_OMITTED_WARNING_CODE = "W121_STRIMZI_ARTIFACTS_OMITTED"
_OMITTED_WARNING_MESSAGE = "Non-topic artifacts omitted from Strimzi export"
_FAILURE_MESSAGE = "Strimzi export input is invalid"
_EXTERNAL_WARNING_LOCATION = re.compile(
    r"artifacts/topics/(0|[1-9][0-9]*)/ownership\Z",
    re.ASCII,
)
_OMITTED_WARNING_LOCATION = re.compile(
    r"artifacts/omitted/"
    r"schemas=(0|[1-9][0-9]*),"
    r"flink_jobs=(0|[1-9][0-9]*),"
    r"test_jobs=(0|[1-9][0-9]*),"
    r"connectors=(0|[1-9][0-9]*),"
    r"connector_removals=(0|[1-9][0-9]*),"
    r"gateway_rules=(0|[1-9][0-9]*),"
    r"gateway_rule_removals=(0|[1-9][0-9]*)\Z",
    re.ASCII,
)
_GO_YAML_RESOLVED_WORDS = frozenset(
    {
        "",
        "~",
        "<<",
        ".nan",
        ".NaN",
        ".NAN",
        ".inf",
        ".Inf",
        ".INF",
        "+.inf",
        "+.Inf",
        "+.INF",
        "-.inf",
        "-.Inf",
        "-.INF",
        "null",
        "Null",
        "NULL",
        "y",
        "Y",
        "yes",
        "Yes",
        "YES",
        "true",
        "True",
        "TRUE",
        "on",
        "On",
        "ON",
        "n",
        "N",
        "no",
        "No",
        "NO",
        "false",
        "False",
        "FALSE",
        "off",
        "Off",
        "OFF",
    }
)
_GO_YAML_NUMBER = re.compile(
    r"[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?\Z",
    re.ASCII,
)
_GO_YAML_BASE_INTEGER = re.compile(
    r"[-+]?0(?:[bB][01]+|[oO][0-7]+|[xX][0-9a-fA-F]+)\Z",
    re.ASCII,
)
_GO_YAML_TIMESTAMP_PREFIX = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}(?:\Z|[Tt ])", re.ASCII)


class StrimziExportError(ValueError):
    """A bounded, secret-neutral Strimzi mapping failure."""

    def __init__(self, message: str, *, location: str) -> None:
        super().__init__(message)
        self.location = location


@dataclass(frozen=True, slots=True, repr=False)
class StrimziExportTarget:
    """Explicit offline Strimzi target identity."""

    namespace: str
    cluster_name: str

    def __post_init__(self) -> None:
        try:
            validate_dns1123_label(self.namespace)
        except TopicArtifactFormatError:
            raise StrimziExportError(
                _FAILURE_MESSAGE,
                location="target.namespace",
            ) from None
        try:
            validate_dns1123_label(self.cluster_name)
        except TopicArtifactFormatError:
            raise StrimziExportError(
                _FAILURE_MESSAGE,
                location="target.cluster_name",
            ) from None

    def __repr__(self) -> str:
        return "StrimziExportTarget(<validated>)"


@dataclass(frozen=True, slots=True, order=True)
class StrimziExportWarning:
    """One deterministic warning for an intentionally omitted artifact."""

    code: str
    message: str
    location: str


@dataclass(frozen=True, slots=True)
class StrimziExportCounts:
    """Exact export and omission cardinalities."""

    emitted_topics: int
    external_topics_omitted: int
    other_artifacts_omitted: int

    def to_dict(self) -> dict[str, int]:
        return {
            "emitted_topics": self.emitted_topics,
            "external_topics_omitted": self.external_topics_omitted,
            "other_artifacts_omitted": self.other_artifacts_omitted,
        }


@dataclass(frozen=True, slots=True, repr=False, init=False)
class StrimziKafkaTopicExport:
    """Immutable validated documents, warnings, counts, and canonical YAML."""

    manifest_checksum: str
    counts: StrimziExportCounts
    _document_payloads: tuple[str, ...]
    _warnings: tuple[StrimziExportWarning, ...]
    _yaml_bytes: bytes

    def __init__(self) -> None:
        raise TypeError("StrimziKafkaTopicExport is created by generate_strimzi_export")

    @classmethod
    def _from_validated(
        cls,
        *,
        manifest_checksum: str,
        counts: StrimziExportCounts,
        document_payloads: tuple[str, ...],
        warnings: tuple[StrimziExportWarning, ...],
        yaml_bytes: bytes,
    ) -> StrimziKafkaTopicExport:
        """Revalidate defensive copies and prove YAML parity at construction."""
        if type(manifest_checksum) is not str or _CHECKSUM.fullmatch(manifest_checksum) is None:
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.manifest_checksum")
        if type(counts) is not StrimziExportCounts or any(
            type(value) is not int or value < 0
            for value in (
                counts.emitted_topics,
                counts.external_topics_omitted,
                counts.other_artifacts_omitted,
            )
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.counts")
        if type(document_payloads) is not tuple or any(
            type(payload) is not str for payload in document_payloads
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.documents")
        if type(warnings) is not tuple or any(
            type(warning) is not StrimziExportWarning for warning in warnings
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
        if any(
            type(value) is not str
            for warning in warnings
            for value in (warning.code, warning.message, warning.location)
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
        if warnings != tuple(sorted(warnings, key=lambda item: (item.location, item.code))):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
        _validate_result_warnings(warnings, counts=counts)
        if type(yaml_bytes) is not bytes:
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.yaml")

        documents = _decode_document_payloads(document_payloads)
        if any(
            _serialize_document_payload(document) != payload
            for document, payload in zip(documents, document_payloads, strict=True)
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.documents")
        try:
            validate_kafkatopic_documents(documents)
        except (StrimziResourceError, StrimziValidationError):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.documents") from None
        if _serialize_documents(documents) != yaml_bytes:
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.yaml")
        if counts.emitted_topics != len(documents):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.counts")
        if any(
            _document_manifest_checksum(document) != manifest_checksum for document in documents
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.manifest_checksum")

        result = object.__new__(cls)
        object.__setattr__(result, "manifest_checksum", manifest_checksum)
        object.__setattr__(result, "counts", counts)
        object.__setattr__(result, "_document_payloads", document_payloads)
        object.__setattr__(result, "_warnings", warnings)
        object.__setattr__(result, "_yaml_bytes", yaml_bytes)
        return result

    @property
    def target_release(self) -> str:
        return STRIMZI_RELEASE

    @property
    def api_version(self) -> str:
        return STRIMZI_API_VERSION

    @property
    def kind(self) -> str:
        return STRIMZI_KIND

    @property
    def documents(self) -> tuple[dict[str, object], ...]:
        """Return fresh JSON-compatible document copies in canonical order."""
        return _decode_document_payloads(self._document_payloads)

    @property
    def warnings(self) -> tuple[StrimziExportWarning, ...]:
        return self._warnings

    @property
    def yaml_bytes(self) -> bytes:
        return self._yaml_bytes

    @property
    def yaml_text(self) -> str:
        return self._yaml_bytes.decode("utf-8")

    @property
    def yaml(self) -> str:
        return self.yaml_text

    def __repr__(self) -> str:
        return (
            "StrimziKafkaTopicExport("
            f"document_count={len(self._document_payloads)}, "
            f"warning_count={len(self._warnings)})"
        )


class _CanonicalDumper(yaml.SafeDumper):
    """Safe deterministic dumper that never emits aliases or anchors."""

    def ignore_aliases(self, _data: object) -> bool:
        return True


def _requires_quoted_yaml_string(value: str) -> bool:
    """Prevent Kubernetes' go-yaml v2 resolver from changing string types."""
    if value in _GO_YAML_RESOLVED_WORDS:
        return True
    plain = value.replace("_", "")
    return (
        _GO_YAML_NUMBER.fullmatch(plain) is not None
        or _GO_YAML_BASE_INTEGER.fullmatch(plain) is not None
        or _GO_YAML_TIMESTAMP_PREFIX.match(plain) is not None
    )


def _represent_canonical_string(
    dumper: _CanonicalDumper,
    value: str,
) -> yaml.nodes.ScalarNode:
    style = "'" if _requires_quoted_yaml_string(value) else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_CanonicalDumper.add_representer(str, _represent_canonical_string)


def _safe_project_name(value: object) -> str:
    if type(value) is not str or not value or not is_canonical_yaml_text(value):
        raise StrimziExportError(_FAILURE_MESSAGE, location="project")
    assert isinstance(value, str)
    return value


def _safe_manifest_checksum(value: object) -> str:
    if type(value) is not str or _CHECKSUM.fullmatch(value) is None:
        raise StrimziExportError(_FAILURE_MESSAGE, location="manifest_checksum")
    return value


def _validate_result_warnings(
    warnings: tuple[StrimziExportWarning, ...],
    *,
    counts: StrimziExportCounts,
) -> None:
    external_indexes: set[int] = set()
    omitted_totals: list[int] = []
    for warning in warnings:
        if any(
            type(value) is not str for value in (warning.code, warning.message, warning.location)
        ):
            raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
        if warning.code == _EXTERNAL_WARNING_CODE:
            match = _EXTERNAL_WARNING_LOCATION.fullmatch(warning.location)
            if warning.message != _EXTERNAL_WARNING_MESSAGE or match is None:
                raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
            index_text = match.group(1)
            if len(index_text) > 19:
                raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
            index = int(index_text)
            if (
                index in external_indexes
                or index >= counts.emitted_topics + counts.external_topics_omitted
            ):
                raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
            external_indexes.add(index)
            continue
        if warning.code == _OMITTED_WARNING_CODE:
            match = _OMITTED_WARNING_LOCATION.fullmatch(warning.location)
            if warning.message != _OMITTED_WARNING_MESSAGE or match is None:
                raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
            if any(len(value) > 19 for value in match.groups()):
                raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")
            omitted_totals.append(sum(int(value) for value in match.groups()))
            continue
        raise StrimziExportError(_FAILURE_MESSAGE, location="export.warnings")

    if len(external_indexes) != counts.external_topics_omitted:
        raise StrimziExportError(_FAILURE_MESSAGE, location="export.counts")
    if (counts.other_artifacts_omitted == 0 and omitted_totals) or (
        counts.other_artifacts_omitted > 0 and omitted_totals != [counts.other_artifacts_omitted]
    ):
        raise StrimziExportError(_FAILURE_MESSAGE, location="export.counts")


def _document_manifest_checksum(document: dict[str, object]) -> object:
    metadata = cast(dict[str, object], document["metadata"])
    annotations = cast(dict[str, object], metadata["annotations"])
    return annotations["streamt.dev/manifest-checksum"]


def _artifact_collections(value: object) -> dict[str, list[object]]:
    if type(value) is not dict:
        raise StrimziExportError(_FAILURE_MESSAGE, location="artifacts")
    assert isinstance(value, dict)
    if any(type(key) is not str for key in value):
        raise StrimziExportError(_FAILURE_MESSAGE, location="artifacts")
    keys = set(value)
    if not _REQUIRED_ARTIFACT_KINDS.issubset(keys) or not keys.issubset(
        _REQUIRED_ARTIFACT_KINDS | _REMOVAL_ARTIFACT_KINDS
    ):
        raise StrimziExportError(_FAILURE_MESSAGE, location="artifacts")

    collections: dict[str, list[object]] = {}
    for kind, collection in value.items():
        if type(collection) is not list:
            raise StrimziExportError(_FAILURE_MESSAGE, location=f"artifacts/{kind}")
        assert isinstance(kind, str)
        assert isinstance(collection, list)
        collections[kind] = collection
    return collections


def _topic_document(
    topic: ParsedTopicArtifact,
    *,
    target: StrimziExportTarget,
    manifest_checksum: str,
) -> dict[str, object]:
    ownership = topic.ownership
    return {
        "apiVersion": STRIMZI_API_VERSION,
        "kind": STRIMZI_KIND,
        "metadata": {
            "name": topic.metadata_name,
            "namespace": target.namespace,
            "labels": {
                "strimzi.io/cluster": target.cluster_name,
                "app.kubernetes.io/managed-by": "streamt",
            },
            "annotations": {
                "streamt.dev/manifest-checksum": manifest_checksum,
                "streamt.dev/owner-name": ownership.owner_name,
                "streamt.dev/owner-type": ownership.owner_type,
                "streamt.dev/ownership-mode": "managed",
                "streamt.dev/project": ownership.project,
                "streamt.dev/strimzi-release": STRIMZI_RELEASE,
            },
        },
        "spec": {
            "topicName": topic.name,
            "partitions": topic.partitions,
            "replicas": topic.replication_factor,
            "config": dict(topic.config_items),
        },
    }


def _serialize_document_payload(document: dict[str, object]) -> str:
    try:
        return json.dumps(
            document,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    except (RecursionError, TypeError, ValueError):
        raise StrimziExportError(_FAILURE_MESSAGE, location="documents") from None


def _decode_document_payloads(payloads: tuple[str, ...]) -> tuple[dict[str, object], ...]:
    documents: list[dict[str, object]] = []
    try:
        for payload in payloads:
            decoded: object = json.loads(payload)
            if type(decoded) is not dict:
                raise ValueError
            documents.append(cast(dict[str, object], decoded))
    except (json.JSONDecodeError, RecursionError, TypeError, ValueError):
        raise StrimziExportError(_FAILURE_MESSAGE, location="export.documents") from None
    return tuple(documents)


def _serialize_documents(documents: tuple[dict[str, object], ...]) -> bytes:
    if not documents:
        return b""
    try:
        rendered = yaml.dump_all(
            documents,
            Dumper=_CanonicalDumper,
            allow_unicode=True,
            default_flow_style=False,
            explicit_start=True,
            explicit_end=False,
            sort_keys=False,
            line_break="\n",
            width=4_096,
        )
    except (RecursionError, TypeError, yaml.YAMLError):
        raise StrimziExportError(_FAILURE_MESSAGE, location="documents") from None
    rendered = rendered.replace("\r\n", "\n").replace("\r", "\n")
    if not rendered.endswith("\n"):
        rendered += "\n"
    lines = rendered.splitlines()
    if (
        not lines
        or lines[0] != "---"
        or rendered.endswith("\n\n")
        or sum(line == "---" for line in lines) != len(documents)
        or any(line != "---" for line in lines if line.startswith("---"))
        or any(line == "..." for line in lines)
    ):
        raise StrimziExportError(_FAILURE_MESSAGE, location="documents")
    try:
        return rendered.encode("utf-8")
    except UnicodeEncodeError:
        raise StrimziExportError(_FAILURE_MESSAGE, location="documents") from None


def _warnings_and_counts(
    topics: tuple[ParsedTopicArtifact, ...],
    collections: dict[str, list[object]],
) -> tuple[tuple[StrimziExportWarning, ...], StrimziExportCounts]:
    external_indexes = [
        index for index, topic in enumerate(topics) if topic.ownership.mode == "external"
    ]
    other_counts = {kind: len(collections.get(kind, [])) for kind in _OTHER_ARTIFACT_KINDS}
    other_total = sum(other_counts.values())
    warnings: list[StrimziExportWarning] = []
    if other_total:
        location_counts = ",".join(f"{kind}={other_counts[kind]}" for kind in _OTHER_ARTIFACT_KINDS)
        warnings.append(
            StrimziExportWarning(
                code=_OMITTED_WARNING_CODE,
                message=_OMITTED_WARNING_MESSAGE,
                location=f"artifacts/omitted/{location_counts}",
            )
        )
    warnings.extend(
        StrimziExportWarning(
            code=_EXTERNAL_WARNING_CODE,
            message=_EXTERNAL_WARNING_MESSAGE,
            location=f"artifacts/topics/{index}/ownership",
        )
        for index in external_indexes
    )
    managed_count = len(topics) - len(external_indexes)
    return (
        tuple(sorted(warnings, key=lambda item: (item.location, item.code))),
        StrimziExportCounts(
            emitted_topics=managed_count,
            external_topics_omitted=len(external_indexes),
            other_artifacts_omitted=other_total,
        ),
    )


def generate_strimzi_export(
    artifacts: object,
    *,
    project_name: object,
    manifest_checksum: object,
    target: object,
) -> StrimziKafkaTopicExport:
    """Map strict compiled artifact collections to canonical KafkaTopic YAML."""
    project = _safe_project_name(project_name)
    checksum = _safe_manifest_checksum(manifest_checksum)
    if type(target) is not StrimziExportTarget:
        raise StrimziExportError(_FAILURE_MESSAGE, location="target")
    collections = _artifact_collections(artifacts)
    try:
        topics = parse_compiled_topic_artifacts(
            collections["topics"],
            expected_project=project,
        )
    except TopicArtifactFormatError:
        raise StrimziExportError(_FAILURE_MESSAGE, location="artifacts/topics") from None

    warnings, counts = _warnings_and_counts(topics, collections)
    documents = tuple(
        _topic_document(topic, target=target, manifest_checksum=checksum)
        for topic in sorted(
            (item for item in topics if item.ownership.mode == "managed"),
            key=lambda item: (item.name, item.metadata_name),
        )
    )
    document_payloads = tuple(_serialize_document_payload(document) for document in documents)
    defensive_documents = _decode_document_payloads(document_payloads)
    try:
        validate_kafkatopic_documents(defensive_documents)
    except (StrimziResourceError, StrimziValidationError):
        raise StrimziExportError(_FAILURE_MESSAGE, location="documents") from None
    yaml_bytes = _serialize_documents(defensive_documents)
    return StrimziKafkaTopicExport._from_validated(
        manifest_checksum=checksum,
        counts=counts,
        document_payloads=document_payloads,
        warnings=warnings,
        yaml_bytes=yaml_bytes,
    )


__all__ = [
    "StrimziExportCounts",
    "StrimziExportError",
    "StrimziExportTarget",
    "StrimziExportWarning",
    "StrimziKafkaTopicExport",
    "generate_strimzi_export",
]
