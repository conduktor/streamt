"""Manifest for compiled streamt projects."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class ConnectorArtifactFormatError(ValueError):
    """A compiled connector artifact cannot be represented unambiguously."""


@dataclass(frozen=True)
class ArtifactOwnership:
    """Lifecycle ownership for a compiled deployment artifact.

    The owner identifies the source or model whose selection should include the
    artifact. ``mode`` preserves the declaration's lifecycle authority so an
    emitted external artifact remains observe-only.
    """

    project: str
    owner_type: str
    owner_name: str
    mode: str = "managed"

    def to_dict(self) -> dict[str, str]:
        return {
            "mode": self.mode,
            "project": self.project,
            "type": self.owner_type,
            "name": self.owner_name,
        }

    @classmethod
    def from_dict(cls, data: object) -> ArtifactOwnership | None:
        """Parse ownership stored in a manifest, tolerating legacy artifacts."""
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            return None
        project = data.get("project")
        owner_type = data.get("type")
        owner_name = data.get("name")
        if not isinstance(project, str) or not project:
            return None
        if not isinstance(owner_type, str) or not owner_type:
            return None
        if not isinstance(owner_name, str) or not owner_name:
            return None
        mode = data.get("mode", "managed")
        if not isinstance(mode, str):
            return None
        return cls(
            project=project,
            owner_type=owner_type,
            owner_name=owner_name,
            mode=mode,
        )


def _with_ownership(
    data: dict[str, object],
    ownership: ArtifactOwnership | dict[str, str] | None,
) -> dict[str, object]:
    """Add normalized ownership metadata when an artifact has an owner."""
    parsed = ArtifactOwnership.from_dict(ownership)
    if parsed:
        data["ownership"] = parsed.to_dict()
    return data


@dataclass
class TopicArtifact:
    """Compiled topic artifact."""

    name: str
    partitions: int
    replication_factor: int
    config: dict[str, object] = field(default_factory=dict)
    ownership: ArtifactOwnership | dict[str, str] | None = None

    def to_dict(self) -> dict[str, object]:
        return _with_ownership({
            "name": self.name,
            "partitions": self.partitions,
            "replication_factor": self.replication_factor,
            "config": self.config,
        }, self.ownership)


@dataclass
class FlinkJobArtifact:
    """Compiled Flink job artifact."""

    name: str
    sql: str
    cluster: Optional[str] = None
    parallelism: Optional[int] = None
    checkpoint_interval_ms: Optional[int] = None
    state_backend: Optional[str] = None
    state_ttl_ms: Optional[int] = None
    ownership: ArtifactOwnership | dict[str, str] | None = None

    def to_dict(self) -> dict[str, object]:
        return _with_ownership({
            "name": self.name,
            "sql": self.sql,
            "cluster": self.cluster,
            "parallelism": self.parallelism,
            "checkpoint_interval_ms": self.checkpoint_interval_ms,
            "state_backend": self.state_backend,
            "state_ttl_ms": self.state_ttl_ms,
        }, self.ownership)


@dataclass
class ConnectorArtifact:
    """Compiled Connect connector artifact."""

    name: str
    connector_class: str
    topics: list[str]
    config: dict[str, object] = field(default_factory=dict)
    cluster: Optional[str] = None
    ownership: ArtifactOwnership | dict[str, str] | None = None

    def to_dict(self) -> dict[str, object]:
        reserved_config = {
            "name": self.name,
            "connector.class": self.connector_class,
            "topics": ",".join(self.topics),
        }
        for key, expected in reserved_config.items():
            if key in self.config and self.config[key] != expected:
                raise ConnectorArtifactFormatError(
                    f"connector config field {key!r} conflicts with its canonical field"
                )
        config = {**self.config, **reserved_config}
        return _with_ownership({
            "name": self.name,
            "connector_class": self.connector_class,
            "topics": self.topics,
            "cluster": self.cluster,
            "config": config,
        }, self.ownership)


def _require_connector_removal_text(
    value: object,
    *,
    field_name: str,
    max_length: int,
    forbid_slash: bool = False,
) -> str:
    """Defensively validate an immutable compiled Connector removal field."""
    import unicodedata

    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ValueError(f"Connector removal {field_name} is invalid")
    if forbid_slash and "/" in value:
        raise ValueError(f"Connector removal {field_name} is invalid")
    if any(unicodedata.category(character) in {"Cc", "Cs"} for character in value):
        raise ValueError(f"Connector removal {field_name} is invalid")
    return value


@dataclass(frozen=True)
class ConnectorRemovalArtifact:
    """Immutable, secret-neutral identity for one explicit Connector removal."""

    logical_owner: str
    connector_name: str
    cluster_alias: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "logical_owner",
            _require_connector_removal_text(
                self.logical_owner,
                field_name="logical_owner",
                max_length=128,
                forbid_slash=True,
            ),
        )
        object.__setattr__(
            self,
            "connector_name",
            _require_connector_removal_text(
                self.connector_name,
                field_name="name",
                max_length=256,
            ),
        )
        object.__setattr__(
            self,
            "cluster_alias",
            _require_connector_removal_text(
                self.cluster_alias,
                field_name="cluster",
                max_length=128,
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "logicalOwner": self.logical_owner,
            "name": self.connector_name,
            "cluster": self.cluster_alias,
        }


@dataclass
class GatewayRuleArtifact:
    """Compiled Gateway rule artifact."""

    name: str
    virtual_topic: str
    physical_topic: str
    interceptors: list[dict[str, object]] = field(default_factory=list)
    ownership: ArtifactOwnership | dict[str, str] | None = None

    def to_dict(self) -> dict[str, object]:
        return _with_ownership({
            "name": self.name,
            "virtualTopic": self.virtual_topic,
            "physicalTopic": self.physical_topic,
            "interceptors": self.interceptors,
        }, self.ownership)


@dataclass(frozen=True, init=False)
class GatewayRuleRemovalArtifact:
    """Immutable explicit removal of one complete prior Gateway artifact.

    The prior artifact is retained as serialized compiler data so callers only
    receive independent parsed copies and cannot mutate the manifest value.
    """

    logical_owner: str
    _prior_artifact_json: str = field(repr=False)

    def __init__(
        self,
        *,
        logical_owner: str,
        prior_artifact: GatewayRuleArtifact,
    ) -> None:
        if (
            not isinstance(logical_owner, str)
            or not logical_owner.strip()
            or "/" in logical_owner
        ):
            raise ValueError(
                "Gateway removal logical owner must be a non-empty string without '/'"
            )
        from streamt.compiler.gateway_artifact import (
            parse_compiled_gateway_rule_artifact,
        )

        parsed_prior = parse_compiled_gateway_rule_artifact(prior_artifact.to_dict())
        ownership = ArtifactOwnership.from_dict(parsed_prior.ownership)
        if (
            ownership is None
            or ownership.mode != "managed"
            or ownership.owner_type != "model"
            or ownership.owner_name != logical_owner
        ):
            raise ValueError(
                "Gateway removal prior artifact must have matching managed model ownership"
            )
        object.__setattr__(self, "logical_owner", logical_owner)
        object.__setattr__(
            self,
            "_prior_artifact_json",
            json.dumps(
                parsed_prior.to_dict(),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )

    @property
    def prior_artifact(self) -> GatewayRuleArtifact:
        from streamt.compiler.gateway_artifact import (
            parse_compiled_gateway_rule_artifact,
        )

        return parse_compiled_gateway_rule_artifact(
            json.loads(self._prior_artifact_json)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "logicalOwner": self.logical_owner,
            "priorArtifact": json.loads(self._prior_artifact_json),
        }


@dataclass
class SchemaArtifact:
    """Compiled schema artifact."""

    subject: str
    schema: dict[str, object]
    schema_type: str = "AVRO"  # AVRO, JSON, PROTOBUF
    compatibility: Optional[str] = None  # BACKWARD, FORWARD, FULL, NONE
    ownership: ArtifactOwnership | dict[str, str] | None = None

    def to_dict(self) -> dict[str, object]:
        return _with_ownership({
            "subject": self.subject,
            "schema": self.schema,
            "schema_type": self.schema_type,
            "compatibility": self.compatibility,
        }, self.ownership)


@dataclass
class Manifest:
    """Manifest of compiled streamt project."""

    version: str
    project_name: str
    compiled_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )
    sources: list[dict[str, object]] = field(default_factory=list)
    models: list[dict[str, object]] = field(default_factory=list)
    tests: list[dict[str, object]] = field(default_factory=list)
    exposures: list[dict[str, object]] = field(default_factory=list)
    dag: dict[str, object] = field(default_factory=dict)
    artifacts: dict[str, list[dict[str, object]]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "project": self.project_name,
            "compiled_at": self.compiled_at,
            "sources": self.sources,
            "models": self.models,
            "tests": self.tests,
            "exposures": self.exposures,
            "dag": self.dag,
            "artifacts": self.artifacts,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def _safe_artifacts(self) -> dict[str, list[dict[str, object]]]:
        """Return artifacts with credentials redacted from Flink SQL (for disk storage)."""
        from streamt.compiler.flink_ddl import redact_ddl_credentials

        result = dict(self.artifacts)
        if "flink_jobs" in result:
            safe_jobs: list[dict[str, object]] = []
            for job in result["flink_jobs"]:
                safe_job = dict(job)
                if "sql" in job:
                    sql = job["sql"]
                    if not isinstance(sql, str):
                        raise TypeError("Flink job SQL must be a string")
                    safe_job["sql"] = redact_ddl_credentials(sql)
                safe_jobs.append(safe_job)
            result["flink_jobs"] = safe_jobs
        return result

    def save(self, path: Path) -> None:
        """Save manifest to file (atomic write, credentials redacted)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        safe_dict = self.to_dict()
        safe_dict["artifacts"] = self._safe_artifacts()
        tmp_name = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", dir=path.parent, suffix=".tmp", delete=False
            ) as fd:
                tmp_name = fd.name
                fd.write(json.dumps(safe_dict, indent=2))
                fd.flush()
                os.fsync(fd.fileno())
            Path(tmp_name).replace(path)
        except Exception:
            if tmp_name:
                try:
                    Path(tmp_name).unlink(missing_ok=True)
                except Exception:
                    pass
            raise

    @classmethod
    def load(cls, path: Path) -> Manifest:
        """Load manifest from file."""
        with open(path) as f:
            data = json.load(f)
        return cls(
            version=data["version"],
            project_name=data["project"],
            compiled_at=data["compiled_at"],
            sources=data.get("sources", []),
            models=data.get("models", []),
            tests=data.get("tests", []),
            exposures=data.get("exposures", []),
            dag=data.get("dag", {}),
            artifacts=data.get("artifacts", {}),
        )
