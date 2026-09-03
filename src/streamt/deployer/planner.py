"""Deployment planner for streamt projects."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Literal, Optional, Protocol

from streamt.compiler.connector_artifact import (
    ConnectorRemovalArtifactFormatError,
    ConnectorRemovalClusterReferenceError,
    ConnectorRemovalPreflightError,
    ConnectorRemovalRuntimeRequiredError,
    parse_compiled_connector_artifact,
    parse_compiled_connector_removal_artifact,
)
from streamt.compiler.gateway_artifact import (
    GatewayArtifactFormatError,
    parse_compiled_gateway_rule_artifact,
)
from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    ConnectorArtifactFormatError,
    ConnectorRemovalArtifact,
    GatewayRuleArtifact,
    Manifest,
)
from streamt.core.deployment_state import (
    PostgresDeploymentStateConfig,
    RemoteStateRequiredError,
)
from streamt.core.models import StreamtProject
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ConnectClusterBindingError,
    ConnectDeployer,
    ConnectorChange,
    ConnectorState,
    ManagedConnectorObservation,
    bind_connector_artifact,
    is_connect_backend_identity,
    managed_connector_absence_fingerprint,
    secret_neutral_connector_changes,
)
from streamt.deployer.flink import FlinkDeployer, FlinkJobChange
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayBindingError,
    GatewayDeployer,
    GatewayDesiredAggregateError,
    GatewayManagedMutationError,
    GatewayManifestResolutionError,
    GatewayRuleChange,
    ManagedGatewayRuleObservation,
    ResolvedManagedGatewayRule,
    build_desired_gateway_rule,
    classify_gateway_interceptor_name,
    generate_gateway_interceptor_name,
    is_gateway_backend_identity,
    is_gateway_resource_name,
    plan_managed_gateway_rule,
    plan_managed_gateway_rule_deletion,
    resolve_managed_gateway_rules,
    secret_neutral_gateway_changes,
)
from streamt.deployer.kafka import KafkaDeployer, TopicChange
from streamt.deployer.schema_registry import SchemaChange, SchemaRegistryDeployer
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateError,
    StateIdentityError,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
)

logger = logging.getLogger(__name__)


class _PlannedChange(Protocol):
    """Common mutable action carried by backend-specific change records."""

    action: str


@dataclass(frozen=True)
class ResolvedConnectorRemoval:
    """One compiled Connector tombstone bound only to runtime and prior state."""

    resource_id: str
    logical_owner: str
    connector_name: str
    binding: ConnectClusterBinding
    prior_record: ManagedResourceRecord | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.resource_id) is not str
            or type(self.logical_owner) is not str
            or type(self.connector_name) is not str
            or type(self.binding) is not ConnectClusterBinding
        ):
            raise ConnectorRemovalPreflightError(
                "Connector removal contains invalid identity value types"
            )
        try:
            identity = ResourceIdentity.parse(self.resource_id)
            declaration = ConnectorRemovalArtifact(
                logical_owner=self.logical_owner,
                connector_name=self.connector_name,
                cluster_alias=self.binding.cluster_alias,
            )
        except (StateError, TypeError, ValueError, AttributeError):
            raise ConnectorRemovalPreflightError(
                "Connector removal has an invalid canonical resource identity"
            ) from None
        if (
            len(self.resource_id) > 512
            or identity.kind != "connector"
            or identity.logical_name != declaration.logical_owner
            or declaration.connector_name != self.connector_name
            or declaration.cluster_alias != self.binding.cluster_alias
            or (
                self.prior_record is not None
                and (
                    type(self.prior_record) is not ManagedResourceRecord
                    or self.prior_record.ownership != "managed"
                    or self.prior_record.physical_name != self.connector_name
                    or self.prior_record.backend != self.binding.backend_identity
                )
            )
        ):
            raise ConnectorRemovalPreflightError(
                "Connector removal contains mismatched identity evidence"
            )


@dataclass(frozen=True)
class ConnectorPlanningTargets:
    """Immutable provider-free desired and removal Connector targets."""

    binding: ConnectClusterBinding
    desired_connectors: tuple[ConnectorArtifact, ...] = ()
    removals: tuple[ResolvedConnectorRemoval, ...] = ()

    def __post_init__(self) -> None:
        if type(self.binding) is not ConnectClusterBinding:
            raise ConnectorRemovalPreflightError(
                "Connector planning targets require a canonical binding"
            )
        if not isinstance(self.desired_connectors, tuple) or any(
            type(artifact) is not ConnectorArtifact for artifact in self.desired_connectors
        ):
            raise ConnectorRemovalPreflightError(
                "Connector planning targets require immutable desired artifacts"
            )
        if not isinstance(self.removals, tuple) or any(
            type(removal) is not ResolvedConnectorRemoval for removal in self.removals
        ):
            raise ConnectorRemovalPreflightError(
                "Connector planning targets require immutable removal targets"
            )
        if any(
            artifact.cluster != self.binding.cluster_alias for artifact in self.desired_connectors
        ):
            raise ConnectorRemovalPreflightError(
                "Connector planning targets contain a mismatched desired binding"
            )
        if any(removal.binding != self.binding for removal in self.removals):
            raise ConnectorRemovalPreflightError(
                "Connector planning targets contain a mismatched removal binding"
            )


ConnectorRemovalAssessmentStatus = Literal[
    "already_absent",
    "state_provider_drift",
    "ownership_required",
]
_CONNECTOR_REMOVAL_ASSESSMENT_STATUSES = frozenset(
    {
        "already_absent",
        "state_provider_drift",
        "ownership_required",
    }
)


@dataclass(frozen=True)
class ConnectorRemovalAssessment:
    """One immutable, secret-neutral non-actionable Connector removal outcome."""

    resource_id: str
    logical_owner: str
    connector_name: str
    backend_identity: str
    status: ConnectorRemovalAssessmentStatus

    def __post_init__(self) -> None:
        try:
            identity = ResourceIdentity.parse(self.resource_id)
            binding = ConnectClusterBinding.from_backend_identity(self.backend_identity)
            declaration = ConnectorRemovalArtifact(
                logical_owner=self.logical_owner,
                connector_name=self.connector_name,
                cluster_alias=binding.cluster_alias,
            )
        except (ConnectClusterBindingError, StateError, TypeError, ValueError):
            raise ConnectorRemovalPreflightError(
                "Connector removal assessment has invalid canonical identity evidence"
            ) from None
        if (
            identity.kind != "connector"
            or identity.logical_name != declaration.logical_owner
            or declaration.connector_name != self.connector_name
            or not is_connect_backend_identity(self.backend_identity)
            or type(self.status) is not str
            or self.status not in _CONNECTOR_REMOVAL_ASSESSMENT_STATUSES
        ):
            raise ConnectorRemovalPreflightError(
                "Connector removal assessment contains mismatched identity evidence"
            )

    def to_dict(self) -> dict[str, str]:
        """Return the stable secret-neutral reviewed representation."""
        return {
            "resource_id": self.resource_id,
            "logical_owner": self.logical_owner,
            "connector_name": self.connector_name,
            "backend_identity": self.backend_identity,
            "status": self.status,
        }


def _connector_removal_assessment(
    removal: ResolvedConnectorRemoval,
    status: ConnectorRemovalAssessmentStatus,
) -> ConnectorRemovalAssessment:
    return ConnectorRemovalAssessment(
        resource_id=removal.resource_id,
        logical_owner=removal.logical_owner,
        connector_name=removal.connector_name,
        backend_identity=removal.binding.backend_identity,
        status=status,
    )


def _reconstruct_connector_removal_artifact(
    removal: ResolvedConnectorRemoval,
    current: ManagedConnectorObservation,
) -> ConnectorArtifact:
    """Reconstruct the exact prior compiled artifact without exposing live config."""
    if (
        type(current) is not ManagedConnectorObservation
        or type(current.binding) is not ConnectClusterBinding
        or not current.exists
        or current.binding != removal.binding
        or current.name != removal.connector_name
    ):
        raise ConnectorRemovalPreflightError(
            "Connector removal observation does not match its exact target"
        )
    try:
        identity = ResourceIdentity.parse(removal.resource_id)
        config = current.config_dict()
        connector_class = config.get("connector.class")
        topics = config.get("topics")
        if type(connector_class) is not str or type(topics) is not str:
            raise ConnectorArtifactFormatError(
                "managed Connector observation has invalid reserved fields"
            )
        return parse_compiled_connector_artifact(
            {
                "name": removal.connector_name,
                "connector_class": connector_class,
                "topics": topics.split(","),
                "cluster": removal.binding.cluster_alias,
                "config": config,
                "ownership": ArtifactOwnership(
                    project=identity.project,
                    owner_type="model",
                    owner_name=removal.logical_owner,
                    mode="managed",
                ).to_dict(),
            }
        )
    except (ConnectorArtifactFormatError, StateError, TypeError, ValueError):
        raise ConnectorRemovalPreflightError(
            "Connector removal prior artifact cannot be reconstructed from the exact observation"
        ) from None


def _connector_removal_action_evidence(
    removal: ResolvedConnectorRemoval,
    current: ManagedConnectorObservation,
) -> ConnectorActionEvidence:
    """Build one checksum-bound present-to-absent action value."""
    prior_record = removal.prior_record
    if prior_record is None:
        raise StateIdentityError("Connector deletion requires exact managed prior ownership")
    prior_artifact = _reconstruct_connector_removal_artifact(removal, current)
    if artifact_checksum(prior_artifact.to_dict()) != prior_record.artifact_checksum:
        raise StateIdentityError(
            "Connector deletion current artifact differs from exact managed prior ownership"
        )
    return ConnectorActionEvidence(
        version=1,
        backend_identity=removal.binding.backend_identity,
        connector_name=removal.connector_name,
        prior_artifact_checksum=prior_record.artifact_checksum,
        current=ConnectorActionSurfaceEvidence(
            exists=True,
            fingerprint=current.fingerprint,
        ),
        desired=ConnectorActionSurfaceEvidence(
            exists=False,
            fingerprint=managed_connector_absence_fingerprint(
                removal.binding.backend_identity,
                removal.connector_name,
            ),
        ),
    )


def connector_removals_requested(raw_removals: object) -> bool:
    """Return whether a collection attempts to declare Connector removals."""
    return type(raw_removals) is not list or bool(raw_removals)


def require_connector_removal_postgres_state(
    raw_removals: object,
    deployment_state: object,
) -> None:
    """Enforce the intrinsic PostgreSQL-v2 configuration boundary."""
    if not connector_removals_requested(raw_removals):
        return
    if type(deployment_state) is not PostgresDeploymentStateConfig:
        raise RemoteStateRequiredError("Connector removals require PostgreSQL deployment state")


def _connector_binding_from_parsed_project(
    project: StreamtProject,
    *,
    explicit_alias: str,
) -> ConnectClusterBinding:
    connect = project.runtime.connect
    if connect is None or connect.default is None or not connect.clusters:
        raise ConnectorRemovalRuntimeRequiredError(
            "Connector removal requires a configured Kafka Connect runtime"
        )
    if explicit_alias != connect.default or explicit_alias not in connect.clusters:
        raise ConnectorRemovalClusterReferenceError(
            "Connector removal cluster must be the configured default Connect cluster"
        )
    try:
        return ConnectClusterBinding.from_endpoint(
            explicit_alias,
            connect.clusters[explicit_alias].rest_url,
        )
    except ConnectClusterBindingError:
        raise ConnectorRemovalPreflightError(
            "Connector removal runtime binding is invalid"
        ) from None


def _parse_connector_removals(raw_removals: object) -> tuple[ConnectorRemovalArtifact, ...]:
    if type(raw_removals) is not list:
        raise ConnectorRemovalPreflightError(
            "Connector removal manifest collection must be an exact list"
        )
    try:
        return tuple(parse_compiled_connector_removal_artifact(removal) for removal in raw_removals)
    except ConnectorRemovalArtifactFormatError:
        raise ConnectorRemovalPreflightError(
            "Connector removal manifest contains an invalid compiled artifact"
        ) from None


def _connector_provider_locator(
    binding: ConnectClusterBinding,
    connector_name: str,
) -> tuple[str, str]:
    return (binding.endpoint_fingerprint, connector_name)


def _connector_resource_uri(
    project_name: str,
    environment: str,
    logical_owner: str,
) -> str:
    try:
        resource_uri = resource_id(
            project_name,
            environment,
            "connector",
            logical_owner,
        )
    except StateError:
        raise ConnectorRemovalPreflightError(
            "Connector removal has an invalid canonical resource identity"
        ) from None
    if len(resource_uri) > 512:
        raise ConnectorRemovalPreflightError(
            "Connector removal resource identity exceeds the durable action boundary"
        )
    return resource_uri


def resolve_connector_planning_targets(
    manifest: Manifest,
    project: object,
    *,
    environment: str,
    prior_state: LocalState | None,
    require_authoritative_state: bool,
) -> ConnectorPlanningTargets:
    """Resolve complete Connector identities without constructing or reading a provider."""
    if not isinstance(manifest, Manifest) or not isinstance(project, StreamtProject):
        raise ConnectorRemovalPreflightError(
            "Connector removal preflight requires parsed project and manifest"
        )
    if type(require_authoritative_state) is not bool:
        raise ConnectorRemovalPreflightError(
            "Connector removal preflight requires an exact state policy"
        )
    if project.project.name != manifest.project_name:
        raise ConnectorRemovalPreflightError(
            "Connector removal project does not match deployment manifest"
        )

    raw_removals = manifest.artifacts.get("connector_removals", [])
    removals = _parse_connector_removals(raw_removals)
    require_connector_removal_postgres_state(raw_removals, project.deployment_state)
    if not removals:
        raise ConnectorRemovalPreflightError(
            "Connector removal preflight requires at least one removal"
        )
    if require_authoritative_state and type(prior_state) is not LocalState:
        raise ConnectorRemovalPreflightError(
            "Online Connector removal planning requires authoritative ownership state"
        )
    if prior_state is not None:
        if type(prior_state) is not LocalState:
            raise ConnectorRemovalPreflightError(
                "Connector removal preflight received invalid ownership state"
            )
        if prior_state.project != manifest.project_name or prior_state.environment != environment:
            raise ConnectorRemovalPreflightError(
                "Connector removal state belongs to another project environment"
            )

    binding = _connector_binding_from_parsed_project(
        project,
        explicit_alias=removals[0].cluster_alias,
    )
    if any(removal.cluster_alias != binding.cluster_alias for removal in removals):
        raise ConnectorRemovalClusterReferenceError(
            "Connector removal cluster must be the configured default Connect cluster"
        )

    raw_desired_connectors = manifest.artifacts.get("connectors", [])
    if type(raw_desired_connectors) is not list:
        raise ConnectorRemovalPreflightError("Connector manifest collection must be an exact list")
    try:
        desired_connectors = tuple(
            bind_connector_artifact(
                parse_compiled_connector_artifact(raw_connector),
                binding,
            )
            for raw_connector in raw_desired_connectors
        )
    except (ConnectorArtifactFormatError, ConnectClusterBindingError):
        raise ConnectorRemovalPreflightError(
            "Connector removal manifest contains an invalid desired Connector artifact"
        ) from None

    resource_claims: dict[str, str] = {}
    provider_claims: dict[tuple[str, str], str] = {}
    target_claims: list[tuple[str, tuple[str, str]]] = []

    def claim(logical_owner: str, connector_name: str, label: str) -> str:
        resource_uri = _connector_resource_uri(
            manifest.project_name,
            environment,
            logical_owner,
        )
        provider_locator = _connector_provider_locator(binding, connector_name)
        if resource_uri in resource_claims:
            raise ConnectorRemovalPreflightError(
                "Connector planning targets collide on canonical resource identity"
            )
        if provider_locator in provider_claims:
            raise ConnectorRemovalPreflightError(
                "Connector planning targets collide on provider locator"
            )
        resource_claims[resource_uri] = label
        provider_claims[provider_locator] = label
        target_claims.append((resource_uri, provider_locator))
        return resource_uri

    for index, artifact in enumerate(desired_connectors):
        ownership = ArtifactOwnership.from_dict(artifact.ownership)
        if ownership is not None and ownership.project != manifest.project_name:
            raise ConnectorRemovalPreflightError(
                "Desired Connector ownership belongs to another project"
            )
        logical_owner = ownership.owner_name if ownership is not None else artifact.name
        claim(logical_owner, artifact.name, f"desired[{index}]")

    removal_resource_uris: list[str] = []
    for index, removal in enumerate(removals):
        removal_resource_uris.append(
            claim(removal.logical_owner, removal.connector_name, f"removal[{index}]")
        )

    prior_provider_claims: dict[
        tuple[str, str],
        list[tuple[str, ManagedResourceRecord]],
    ] = {}
    legacy_name_claims: dict[str, list[str]] = {}
    if prior_state is not None:
        for prior_resource_uri, prior_record in prior_state.resources.items():
            try:
                identity = ResourceIdentity.parse(prior_resource_uri)
            except StateError:
                raise ConnectorRemovalPreflightError(
                    "Connector prior state contains an invalid resource identity"
                ) from None
            if (
                identity.project != prior_state.project
                or identity.environment != prior_state.environment
                or type(prior_record) is not ManagedResourceRecord
            ):
                raise ConnectorRemovalPreflightError(
                    "Connector prior state contains mismatched identity evidence"
                )
            if identity.kind != "connector":
                continue
            try:
                prior_binding = ConnectClusterBinding.from_backend_identity(prior_record.backend)
            except ConnectClusterBindingError:
                legacy_name_claims.setdefault(prior_record.physical_name, []).append(
                    prior_resource_uri
                )
                continue
            locator = _connector_provider_locator(
                prior_binding,
                prior_record.physical_name,
            )
            prior_provider_claims.setdefault(locator, []).append((prior_resource_uri, prior_record))

    for resource_uri, provider_locator in target_claims:
        connector_name = provider_locator[1]
        if any(
            legacy_resource_uri != resource_uri
            for legacy_resource_uri in legacy_name_claims.get(connector_name, [])
        ):
            raise ConnectorRemovalPreflightError(
                "Connector prior state contains an ambiguous legacy provider claim"
            )
        matching_records = prior_provider_claims.get(provider_locator, [])
        if len(matching_records) > 1 or any(
            prior_resource_uri != resource_uri for prior_resource_uri, _record in matching_records
        ):
            raise ConnectorRemovalPreflightError(
                "Connector prior state contains a conflicting provider claim"
            )

    resolved_removals: list[ResolvedConnectorRemoval] = []
    for removal, resource_uri in zip(removals, removal_resource_uris, strict=True):
        prior_record = prior_state.resources.get(resource_uri) if prior_state is not None else None
        if prior_record is not None:
            if prior_record.ownership != "managed":
                raise ConnectorRemovalPreflightError(
                    "Connector removal requires managed prior ownership"
                )
            try:
                ConnectClusterBinding.from_backend_identity(prior_record.backend)
            except ConnectClusterBindingError:
                raise ConnectorRemovalPreflightError(
                    "Connector removal cannot use legacy ownership state"
                ) from None
            if prior_record.backend != binding.backend_identity:
                raise ConnectorRemovalPreflightError(
                    "Connector removal prior ownership has a different provider binding"
                )
            if prior_record.physical_name != removal.connector_name:
                raise ConnectorRemovalPreflightError(
                    "Connector removal prior ownership has a different physical name"
                )
        resolved_removals.append(
            ResolvedConnectorRemoval(
                resource_id=resource_uri,
                logical_owner=removal.logical_owner,
                connector_name=removal.connector_name,
                binding=binding,
                prior_record=prior_record,
            )
        )

    return ConnectorPlanningTargets(
        binding=binding,
        desired_connectors=desired_connectors,
        removals=tuple(resolved_removals),
    )


@dataclass(frozen=True)
class PlannedAction:
    """One ordered runtime action bound to its canonical logical resource."""

    resource_id: str
    runtime_label: str
    action: str
    gateway_evidence: GatewayActionEvidence | None = None
    connector_evidence: ConnectorActionEvidence | None = None

    def __post_init__(self) -> None:
        ResourceIdentity.parse(self.resource_id)
        if not isinstance(self.runtime_label, str) or not self.runtime_label:
            raise StateIdentityError("planned action runtime label must be non-empty")
        if not isinstance(self.action, str) or not self.action:
            raise StateIdentityError("planned action action must be non-empty")
        if (
            self.gateway_evidence is not None
            and type(
                self.gateway_evidence,
            )
            is not GatewayActionEvidence
        ):
            raise StateIdentityError("planned action Gateway evidence is invalid")
        if (
            self.connector_evidence is not None
            and type(self.connector_evidence) is not ConnectorActionEvidence
        ):
            raise StateIdentityError("planned action Connector evidence is invalid")
        try:
            OperationAction(
                index=0,
                resource_id=self.resource_id,
                action=self.action,
                gateway_evidence=self.gateway_evidence,
                connector_evidence=self.connector_evidence,
            )
        except StateError as error:
            raise StateIdentityError(str(error)) from None


@dataclass(frozen=True)
class GatewayRecoveryObservation:
    """One exact live Gateway observation bound to a durable action identity."""

    resource_id: str
    observation: ManagedGatewayRuleObservation = field(repr=False)

    def __post_init__(self) -> None:
        identity = ResourceIdentity.parse(self.resource_id)
        if identity.kind != "gateway_rule":
            raise StateIdentityError(
                "Gateway recovery observation requires a gateway_rule identity"
            )
        if type(self.observation) is not ManagedGatewayRuleObservation:
            raise StateIdentityError(
                "Gateway recovery observation requires an exact managed surface"
            )


@dataclass(frozen=True, init=False)
class ResolvedGatewayRuleRemoval:
    """One exact compiled Gateway tombstone bound to state and runtime identity."""

    resource_id: str
    logical_owner: str
    _prior_artifact_json: str = field(repr=False)
    prior_artifact_checksum: str
    binding: GatewayBackendBinding
    rule_name: str
    alias_name: str

    def __init__(
        self,
        *,
        resource_id: str,
        logical_owner: str,
        prior_artifact: GatewayRuleArtifact,
        prior_artifact_checksum: str,
        binding: GatewayBackendBinding,
        rule_name: str,
        alias_name: str,
    ) -> None:
        if type(prior_artifact) is not GatewayRuleArtifact:
            raise StateIdentityError("Resolved Gateway removal contains invalid compiled identity")
        try:
            identity = ResourceIdentity.parse(resource_id)
            prior_artifact = parse_compiled_gateway_rule_artifact(prior_artifact.to_dict())
            expected_checksum = artifact_checksum(prior_artifact.to_dict())
        except (AttributeError, GatewayArtifactFormatError, StateError, TypeError):
            raise StateIdentityError(
                "Resolved Gateway removal contains invalid compiled identity"
            ) from None
        ownership = ArtifactOwnership.from_dict(prior_artifact.ownership)
        if (
            identity.kind != "gateway_rule"
            or identity.logical_name != logical_owner
            or type(binding) is not GatewayBackendBinding
            or not is_gateway_resource_name(rule_name)
            or not is_gateway_resource_name(alias_name)
            or rule_name != prior_artifact.name
            or alias_name != prior_artifact.virtual_topic
            or prior_artifact_checksum != expected_checksum
            or ownership is None
            or ownership.mode != "managed"
            or ownership.project != identity.project
            or ownership.owner_type != "model"
            or ownership.owner_name != logical_owner
        ):
            raise StateIdentityError(
                "Resolved Gateway removal contains mismatched compiled identity"
            )
        object.__setattr__(self, "resource_id", resource_id)
        object.__setattr__(self, "logical_owner", logical_owner)
        object.__setattr__(
            self,
            "_prior_artifact_json",
            json.dumps(
                prior_artifact.to_dict(),
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )
        object.__setattr__(self, "prior_artifact_checksum", prior_artifact_checksum)
        object.__setattr__(self, "binding", binding)
        object.__setattr__(self, "rule_name", rule_name)
        object.__setattr__(self, "alias_name", alias_name)

    @property
    def prior_artifact(self) -> GatewayRuleArtifact:
        """Return an independent strict copy of the checksum-bound prior artifact."""
        return parse_compiled_gateway_rule_artifact(json.loads(self._prior_artifact_json))


@dataclass(frozen=True)
class GatewayPlanningTargets:
    """Immutable provider-free Gateway desired and removal target set."""

    binding: GatewayBackendBinding
    desired_rules: tuple[ResolvedManagedGatewayRule, ...] = ()
    removals: tuple[ResolvedGatewayRuleRemoval, ...] = ()

    def __post_init__(self) -> None:
        if type(self.binding) is not GatewayBackendBinding:
            raise StateIdentityError("Gateway planning targets require a canonical binding")
        if not isinstance(self.desired_rules, tuple) or any(
            type(rule) is not ResolvedManagedGatewayRule for rule in self.desired_rules
        ):
            raise StateIdentityError("Gateway planning targets require immutable desired rules")
        if not isinstance(self.removals, tuple) or any(
            type(removal) is not ResolvedGatewayRuleRemoval for removal in self.removals
        ):
            raise StateIdentityError("Gateway planning targets require immutable removal targets")
        if any(rule.desired.binding != self.binding for rule in self.desired_rules) or any(
            removal.binding != self.binding for removal in self.removals
        ):
            raise StateIdentityError(
                "Gateway planning targets contain a mismatched provider binding"
            )


GatewayRemovalAssessmentStatus = Literal[
    "already_absent",
    "state_provider_drift",
    "offline_unverified",
    "ownership_required",
]
_GATEWAY_REMOVAL_ASSESSMENT_STATUSES = frozenset(
    {
        "already_absent",
        "state_provider_drift",
        "offline_unverified",
        "ownership_required",
    }
)


@dataclass(frozen=True)
class GatewayRemovalAssessment:
    """One immutable secret-neutral outcome for a non-actionable removal."""

    resource_id: str
    logical_owner: str
    rule_name: str
    alias_name: str
    backend_identity: str
    status: GatewayRemovalAssessmentStatus

    def __post_init__(self) -> None:
        try:
            identity = ResourceIdentity.parse(self.resource_id)
        except StateError:
            raise StateIdentityError(
                "Gateway removal assessment has an invalid canonical identity"
            ) from None
        if (
            identity.kind != "gateway_rule"
            or identity.logical_name != self.logical_owner
            or not is_gateway_resource_name(self.rule_name)
            or not is_gateway_resource_name(self.alias_name)
            or not is_gateway_backend_identity(self.backend_identity)
            or type(self.status) is not str
            or self.status not in _GATEWAY_REMOVAL_ASSESSMENT_STATUSES
        ):
            raise StateIdentityError(
                "Gateway removal assessment contains mismatched identity evidence"
            )

    def to_dict(self) -> dict[str, str]:
        """Return the stable secret-neutral reviewed representation."""
        return {
            "resource_id": self.resource_id,
            "logical_owner": self.logical_owner,
            "rule_name": self.rule_name,
            "alias_name": self.alias_name,
            "backend_identity": self.backend_identity,
            "status": self.status,
        }


def _gateway_removal_assessment(
    removal: ResolvedGatewayRuleRemoval,
    status: GatewayRemovalAssessmentStatus,
) -> GatewayRemovalAssessment:
    return GatewayRemovalAssessment(
        resource_id=removal.resource_id,
        logical_owner=removal.logical_owner,
        rule_name=removal.rule_name,
        alias_name=removal.alias_name,
        backend_identity=removal.binding.backend_identity,
        status=status,
    )


@dataclass(frozen=True)
class _ParsedGatewayRuleRemoval:
    logical_owner: str
    prior_artifact: GatewayRuleArtifact = field(repr=False)
    prior_artifact_checksum: str


def _gateway_binding_from_parsed_project(
    project: StreamtProject,
) -> GatewayBackendBinding:
    conduktor = project.runtime.conduktor
    gateway = conduktor.gateway if conduktor is not None else None
    if gateway is None or gateway.admin_url is None:
        raise GatewayBindingError("Gateway planning requires a configured project runtime")
    return GatewayBackendBinding.from_endpoint(
        gateway.admin_url,
        virtual_cluster=gateway.virtual_cluster,
    )


def _parse_gateway_rule_removals(
    raw_removals: object,
    *,
    project_name: str,
) -> tuple[_ParsedGatewayRuleRemoval, ...]:
    if type(raw_removals) is not list:
        raise StateIdentityError("Gateway removal manifest collection must be an exact list")
    parsed: list[_ParsedGatewayRuleRemoval] = []
    for raw_removal in raw_removals:
        if not isinstance(raw_removal, dict) or set(raw_removal) != {
            "logicalOwner",
            "priorArtifact",
        }:
            raise StateIdentityError(
                "Gateway removal manifest entry must have exact compiled fields"
            )
        logical_owner = raw_removal["logicalOwner"]
        if not isinstance(logical_owner, str) or not logical_owner.strip() or "/" in logical_owner:
            raise StateIdentityError("Gateway removal manifest has an invalid logical owner")
        try:
            prior_artifact = parse_compiled_gateway_rule_artifact(raw_removal["priorArtifact"])
        except GatewayArtifactFormatError:
            raise StateIdentityError(
                "Gateway removal manifest has an invalid prior artifact"
            ) from None
        ownership = ArtifactOwnership.from_dict(prior_artifact.ownership)
        if ownership != ArtifactOwnership(
            project=project_name,
            owner_type="model",
            owner_name=logical_owner,
            mode="managed",
        ):
            raise StateIdentityError("Gateway removal manifest has mismatched managed ownership")
        if not all(
            is_gateway_resource_name(name)
            for name in (
                prior_artifact.name,
                prior_artifact.virtual_topic,
                prior_artifact.physical_topic,
            )
        ):
            raise StateIdentityError("Gateway removal manifest has an invalid provider identity")
        parsed.append(
            _ParsedGatewayRuleRemoval(
                logical_owner=logical_owner,
                prior_artifact=prior_artifact,
                prior_artifact_checksum=artifact_checksum(prior_artifact.to_dict()),
            )
        )
    return tuple(parsed)


def _gateway_prior_records(
    prior_state: LocalState | None,
) -> tuple[tuple[ResourceIdentity, ManagedResourceRecord], ...]:
    if prior_state is None:
        return ()
    records: list[tuple[ResourceIdentity, ManagedResourceRecord]] = []
    canonical_claims: dict[tuple[str, str], ResourceIdentity] = {}
    for resource_uri, record in prior_state.resources.items():
        identity = ResourceIdentity.parse(resource_uri)
        if identity.kind != "gateway_rule":
            continue
        records.append((identity, record))
        if not is_gateway_backend_identity(record.backend):
            continue
        claim = (record.backend, record.physical_name)
        previous = canonical_claims.get(claim)
        if previous is not None and previous != identity:
            raise StateIdentityError(
                "Gateway prior state contains duplicate canonical provider claims"
            )
        canonical_claims[claim] = identity
    return tuple(records)


def _generated_removal_interceptor_names(
    removal: _ParsedGatewayRuleRemoval,
) -> tuple[str, ...]:
    names: list[str] = []
    for ordinal, declaration in enumerate(removal.prior_artifact.interceptors):
        declaration_type = declaration.get("type")
        if not isinstance(declaration_type, str):  # pragma: no cover - strict parser
            raise StateIdentityError("Gateway removal manifest has an invalid interceptor identity")
        try:
            names.append(
                generate_gateway_interceptor_name(
                    removal.prior_artifact.name,
                    declaration_type,
                    ordinal,
                )
            )
        except ValueError:
            raise StateIdentityError(
                "Gateway removal manifest has an invalid interceptor identity"
            ) from None
    return tuple(names)


def _validate_gateway_planning_target_collisions(
    *,
    project_name: str,
    environment: str,
    binding: GatewayBackendBinding,
    desired_rules: tuple[ResolvedManagedGatewayRule, ...],
    removals: tuple[_ParsedGatewayRuleRemoval, ...],
) -> None:
    owner_claims: dict[object, str] = {}
    resource_claims: dict[object, str] = {}
    rule_claims: dict[object, str] = {}
    alias_claims: dict[object, str] = {}
    interceptor_claims: dict[object, str] = {}
    generated_names: list[tuple[str, str]] = []
    all_rule_names: list[str] = []

    def claim(
        *,
        label: str,
        logical_owner: str,
        rule_name: str,
        alias_name: str,
        interceptor_names: Sequence[str],
    ) -> None:
        try:
            resource_uri = resource_id(
                project_name,
                environment,
                "gateway_rule",
                logical_owner,
            )
        except StateError:
            raise StateIdentityError(
                "Gateway planning target has an invalid canonical resource identity"
            ) from None
        claims: tuple[tuple[dict[object, str], object, str], ...] = (
            (owner_claims, logical_owner, "logical owner"),
            (resource_claims, resource_uri, "canonical resource"),
            (
                rule_claims,
                (binding.backend_identity, rule_name),
                "rule locator",
            ),
            (
                alias_claims,
                (binding.backend_identity, alias_name),
                "alias locator",
            ),
        )
        for registry, identity, identity_label in claims:
            previous = registry.get(identity)
            if previous is not None:
                raise StateIdentityError(f"Gateway planning targets collide on {identity_label}")
            registry[identity] = label
        for interceptor_name in interceptor_names:
            locator = (binding.backend_identity, interceptor_name)
            if locator in interceptor_claims:
                raise StateIdentityError(
                    "Gateway planning targets collide on generated interceptor locator"
                )
            interceptor_claims[locator] = label
            generated_names.append((rule_name, interceptor_name))
        all_rule_names.append(rule_name)

    for rule in desired_rules:
        claim(
            label="desired",
            logical_owner=rule.logical_owner,
            rule_name=rule.artifact.name,
            alias_name=rule.artifact.virtual_topic,
            interceptor_names=tuple(interceptor.name for interceptor in rule.desired.interceptors),
        )
    for index, removal in enumerate(removals):
        claim(
            label=f"removal[{index}]",
            logical_owner=removal.logical_owner,
            rule_name=removal.prior_artifact.name,
            alias_name=removal.prior_artifact.virtual_topic,
            interceptor_names=_generated_removal_interceptor_names(removal),
        )

    for owning_rule, generated_name in generated_names:
        classified_owners: list[str] = []
        for candidate_rule in all_rule_names:
            try:
                classified = classify_gateway_interceptor_name(
                    candidate_rule,
                    generated_name,
                )
            except ValueError:
                raise StateIdentityError(
                    "Gateway planning targets contain an ambiguous generated namespace"
                ) from None
            if classified is not None:
                classified_owners.append(candidate_rule)
        if classified_owners != [owning_rule]:
            raise StateIdentityError(
                "Gateway generated interceptor identity maps to multiple planning targets"
            )


def resolve_gateway_planning_targets(
    manifest: Manifest,
    project: object,
    *,
    environment: str,
    prior_state: LocalState | None,
    require_authoritative_state: bool,
) -> GatewayPlanningTargets:
    """Resolve all Gateway manifest targets without constructing or reading a provider."""
    if not isinstance(manifest, Manifest) or not isinstance(project, StreamtProject):
        raise StateIdentityError(
            "Gateway planning target resolution requires parsed project and manifest"
        )
    if type(require_authoritative_state) is not bool:
        raise StateIdentityError(
            "Gateway planning target resolution requires an exact state policy"
        )
    project_name = manifest.project_name
    if project.project.name != project_name:
        raise StateIdentityError("Gateway project runtime does not match deployment manifest")
    try:
        ResourceIdentity(project_name, environment, "gateway_rule", "validation")
    except StateError:
        raise StateIdentityError(
            "Gateway planning target has an invalid project environment"
        ) from None

    raw_removals = manifest.artifacts.get("gateway_rule_removals", [])
    parsed_removals = _parse_gateway_rule_removals(
        raw_removals,
        project_name=project_name,
    )
    if parsed_removals and require_authoritative_state and type(prior_state) is not LocalState:
        raise StateIdentityError(
            "Online Gateway removal planning requires authoritative ownership state"
        )
    if prior_state is not None:
        if not isinstance(prior_state, LocalState):
            raise StateIdentityError("Gateway removal planning received invalid ownership state")
        if prior_state.project != project_name or prior_state.environment != environment:
            raise StateIdentityError(
                "Gateway removal planning state belongs to another project environment"
            )

    binding = _gateway_binding_from_parsed_project(project)
    try:
        desired_rules = resolve_managed_gateway_rules(
            manifest.artifacts.get("gateway_rules", []),
            binding,
        )
    except GatewayManifestResolutionError as error:
        raise StateIdentityError(str(error)) from None

    _validate_gateway_planning_target_collisions(
        project_name=project_name,
        environment=environment,
        binding=binding,
        desired_rules=desired_rules,
        removals=parsed_removals,
    )

    prior_records = _gateway_prior_records(prior_state)
    for rule in desired_rules:
        for identity, record in prior_records:
            if identity.logical_name == rule.logical_owner:
                continue
            if record.physical_name == rule.desired.alias_name and record.backend in (
                binding.backend_identity,
                "conduktor-gateway",
            ):
                raise StateIdentityError(
                    "Gateway desired alias is claimed by another logical record"
                )

    resolved_removals: list[ResolvedGatewayRuleRemoval] = []
    for removal in parsed_removals:
        resource_uri = resource_id(
            project_name,
            environment,
            "gateway_rule",
            removal.logical_owner,
        )
        prior_record = prior_state.resources.get(resource_uri) if prior_state is not None else None
        if prior_record is not None:
            if prior_record.ownership != "managed":
                raise StateIdentityError("Gateway removal requires managed prior ownership")
            if prior_record.backend == "conduktor-gateway":
                raise StateIdentityError(
                    "Gateway removal cannot use legacy unbound ownership state"
                )
            if prior_record.backend != binding.backend_identity:
                raise StateIdentityError(
                    "Gateway removal prior ownership has a different provider binding"
                )
            if prior_record.physical_name != removal.prior_artifact.virtual_topic:
                raise StateIdentityError("Gateway removal prior ownership has a different alias")
            if prior_record.artifact_checksum != removal.prior_artifact_checksum:
                raise StateIdentityError(
                    "Gateway removal prior ownership has a different artifact checksum"
                )
        if any(
            identity.uri != resource_uri
            and record.physical_name == removal.prior_artifact.virtual_topic
            and record.backend in (binding.backend_identity, "conduktor-gateway")
            for identity, record in prior_records
        ):
            raise StateIdentityError("Gateway removal alias is claimed by another logical record")
        resolved_removals.append(
            ResolvedGatewayRuleRemoval(
                resource_id=resource_uri,
                logical_owner=removal.logical_owner,
                prior_artifact=removal.prior_artifact,
                prior_artifact_checksum=removal.prior_artifact_checksum,
                binding=binding,
                rule_name=removal.prior_artifact.name,
                alias_name=removal.prior_artifact.virtual_topic,
            )
        )

    return GatewayPlanningTargets(
        binding=binding,
        desired_rules=desired_rules,
        removals=tuple(resolved_removals),
    )


_SENSITIVE_KV = re.compile(
    r"(password|passwd|secret|token|api_key|apikey)\s*[=:]\s*\S+",
    re.IGNORECASE,
)
_SENSITIVE_AUTH = re.compile(
    r"(authorization|bearer)\s*[=:]\s*\S+(?:\s+\S+)?",
    re.IGNORECASE,
)
_SENSITIVE_URL = re.compile(
    r"://([^:@/\s]+):([^@/\s]+)@",
)


def _sanitize_error(msg: str) -> str:
    """Strip credentials/tokens from error messages."""
    result = _SENSITIVE_KV.sub(r"\1=***", str(msg))
    result = _SENSITIVE_AUTH.sub(r"\1=***", result)
    return _SENSITIVE_URL.sub(r"://***:***@", result)


@dataclass
class ImpactEntry:
    """An entry in the impact radius for a planned change."""

    resource: str
    change_type: str
    downstream_models: list[str] = field(default_factory=list)
    consumers: list[dict[str, object]] = field(default_factory=list)
    logical_type: str | None = None
    logical_name: str | None = None
    logical_resource: str | None = None
    exposures: list[dict[str, object]] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    identity_evidence: dict[str, object] = field(
        default_factory=lambda: {
            "status": "unavailable",
            "reason": "manifest_ownership_missing",
        }
    )
    graph_evidence: dict[str, object] = field(
        default_factory=lambda: {
            "status": "unavailable",
            "reason": "project_not_provided",
        }
    )
    consumer_evidence: dict[str, object] = field(
        default_factory=lambda: {
            "status": "unavailable",
            "source": "kafka_consumer_groups",
            "reason": "kafka_not_configured",
            "failures": [],
        }
    )


@dataclass(frozen=True)
class ChangeRisk:
    """A deterministic risk assessment for one effective resource change."""

    kind: str
    resource: str
    action: str
    assessment: str
    risk_flags: tuple[str, ...] = ()
    evidence: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return the stable machine-readable representation."""
        return {
            "kind": self.kind,
            "resource": self.resource,
            "action": self.action,
            "assessment": self.assessment,
            "risk_flags": list(self.risk_flags),
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class OwnershipRequirement:
    """A resource that cannot be mutated without an ownership decision."""

    resource_id: str
    kind: str
    logical_name: str
    physical_name: str
    reason: str
    observed_action: str
    ownership_mode: str
    message: str

    def to_dict(self) -> dict[str, str]:
        """Return a stable machine-readable representation."""
        return {
            "resource_id": self.resource_id,
            "kind": self.kind,
            "logical_name": self.logical_name,
            "physical_name": self.physical_name,
            "reason": self.reason,
            "observed_action": self.observed_action,
            "ownership_mode": self.ownership_mode,
            "message": self.message,
        }


@dataclass(frozen=True)
class SafetyBlocker:
    """A deterministic policy decision that forbids applying one unsafe change."""

    code: str
    kind: str
    resource: str
    action: str
    message: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        """Return a stable machine-readable representation."""
        return {
            "code": self.code,
            "kind": self.kind,
            "resource": self.resource,
            "action": self.action,
            "message": self.message,
            "details": dict(self.details),
        }


_SAFETY_KIND_ORDER = {
    "schema": 0,
    "topic": 1,
    "flink_job": 2,
    "connector": 3,
    "gateway_rule": 4,
}
_CHANGE_KIND_ORDER = {
    "schema": 0,
    "topic": 1,
    "flink_job": 2,
    "connector": 3,
    "gateway_rule": 4,
}
_RISK_ASSESSMENTS = (
    "safe",
    "risky",
    "schema_breaking",
    "state_migration_required",
    "destructive",
    "unknown",
)
_RISK_PRECEDENCE = {
    "safe": 0,
    "risky": 1,
    "schema_breaking": 2,
    "state_migration_required": 3,
    "destructive": 4,
    # Unknown is deliberately highest: incomplete evidence cannot produce a
    # reassuring plan-level classification.
    "unknown": 5,
}


def _safety_blocker_sort_key(blocker: SafetyBlocker) -> tuple[int, str, str, str]:
    """Order blockers in backend apply order, then by stable resource identity."""
    return (
        _SAFETY_KIND_ORDER.get(blocker.kind, 99),
        blocker.resource,
        blocker.code,
        blocker.action,
    )


@dataclass
class DeploymentPlan:
    """A deployment plan."""

    schema_changes: list[SchemaChange] = field(default_factory=list)
    topic_changes: list[TopicChange] = field(default_factory=list)
    flink_changes: list[FlinkJobChange] = field(default_factory=list)
    connector_changes: list[ConnectorChange] = field(default_factory=list)
    gateway_changes: list[GatewayRuleChange] = field(default_factory=list)
    connector_removal_assessments: tuple[ConnectorRemovalAssessment, ...] = field(
        default_factory=tuple,
        kw_only=True,
    )
    gateway_removal_assessments: tuple[GatewayRemovalAssessment, ...] = field(
        default_factory=tuple,
        kw_only=True,
    )
    gateway_recovery_observations: tuple[GatewayRecoveryObservation, ...] = field(
        default_factory=tuple,
        repr=False,
        compare=False,
        kw_only=True,
    )
    impact_radius: list[ImpactEntry] = field(default_factory=list)
    ownership_requirements: list[OwnershipRequirement] = field(default_factory=list)
    safety_blockers: list[SafetyBlocker] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Derive blockers for plans constructed with their changes up front."""
        if not isinstance(self.connector_removal_assessments, tuple) or any(
            type(assessment) is not ConnectorRemovalAssessment
            for assessment in self.connector_removal_assessments
        ):
            raise StateIdentityError("deployment plan Connector removal assessments are invalid")
        if not isinstance(self.gateway_removal_assessments, tuple) or any(
            type(assessment) is not GatewayRemovalAssessment
            for assessment in self.gateway_removal_assessments
        ):
            raise StateIdentityError("deployment plan Gateway removal assessments are invalid")
        if not isinstance(self.gateway_recovery_observations, tuple) or any(
            type(observation) is not GatewayRecoveryObservation
            for observation in self.gateway_recovery_observations
        ):
            raise StateIdentityError("deployment plan Gateway recovery observations are invalid")
        if (
            self.connector_removal_assessments
            or self.gateway_removal_assessments
            or not self.safety_blockers
        ):
            self.refresh_safety_blockers()

    def refresh_safety_blockers(self) -> None:
        """Rebuild blockers from final effective backend actions."""
        blockers: list[SafetyBlocker] = []

        for change in self.schema_changes:
            changes = change.changes or {}
            incompatible = changes.get("schema_incompatible")
            if change.action != "update" or incompatible is None:
                continue
            current = change.current
            desired = change.desired
            details: dict[str, object] = {}
            if isinstance(incompatible, dict):
                current_version = incompatible.get("current_version")
                if current_version is not None:
                    details["current_version"] = current_version
            compatibility = getattr(current, "compatibility", None) or getattr(
                desired, "compatibility", None
            )
            if compatibility is not None:
                details["compatibility"] = compatibility
            blockers.append(
                SafetyBlocker(
                    code="schema_incompatible",
                    kind="schema",
                    resource=change.subject,
                    action=change.action,
                    message=(
                        "Schema is incompatible with the subject's configured "
                        "compatibility policy; apply is blocked."
                    ),
                    details=details,
                )
            )

        for change in self.topic_changes:
            changes = change.changes or {}
            if change.action != "update" or "partitions_error" not in changes:
                continue
            details = {"field": "partitions"}
            current_partitions = getattr(change.current, "partitions", None)
            desired_partitions = getattr(change.desired, "partitions", None)
            if current_partitions is not None:
                details["current"] = current_partitions
            if desired_partitions is not None:
                details["desired"] = desired_partitions
            blockers.append(
                SafetyBlocker(
                    code="kafka_partition_reduction",
                    kind="topic",
                    resource=change.topic,
                    action=change.action,
                    message="Kafka topic partitions cannot be reduced; apply is blocked.",
                    details=details,
                )
            )

        for change in self.flink_changes:
            current_exists = getattr(change.current, "exists", False) is True
            if change.action not in ("update", "submit") or (
                change.action == "submit" and not current_exists
            ):
                continue
            details = {}
            current_status = getattr(change.current, "status", None)
            if current_status is not None:
                details["current_status"] = current_status
            code = (
                "flink_update_requires_savepoint"
                if change.action == "update"
                else "flink_resubmit_requires_state_evidence"
            )
            blockers.append(
                SafetyBlocker(
                    code=code,
                    kind="flink_job",
                    resource=change.job_name,
                    action=change.action,
                    message=(
                        "Existing Flink job changes are blocked until a savepoint-safe or "
                        "explicitly stateless upgrade workflow is implemented."
                    ),
                    details=details,
                )
            )

        connector_removal_blockers = {
            "state_provider_drift": (
                "connector_removal_state_provider_drift",
                "Provider state conflicts with exact Connector ownership state; apply is blocked.",
            ),
            "ownership_required": (
                "connector_removal_ownership_required",
                "Live Connector has no exact managed ownership state; apply is blocked.",
            ),
        }
        for assessment in self.connector_removal_assessments:
            blocker = connector_removal_blockers.get(assessment.status)
            if blocker is None:
                continue
            code, message = blocker
            blockers.append(
                SafetyBlocker(
                    code=code,
                    kind="connector",
                    resource=assessment.resource_id,
                    action="delete",
                    message=message,
                    details={"status": assessment.status},
                )
            )

        gateway_removal_blockers = {
            "state_provider_drift": (
                "gateway_removal_state_provider_drift",
                "Provider absence conflicts with exact Gateway ownership state; apply is blocked.",
            ),
            "offline_unverified": (
                "gateway_removal_offline_unverified",
                "Gateway removal requires a complete live provider observation; "
                "offline apply is blocked.",
            ),
        }
        for assessment in self.gateway_removal_assessments:
            blocker = gateway_removal_blockers.get(assessment.status)
            if blocker is None:
                continue
            code, message = blocker
            blockers.append(
                SafetyBlocker(
                    code=code,
                    kind="gateway_rule",
                    resource=assessment.resource_id,
                    action="delete",
                    message=message,
                    details={"status": assessment.status},
                )
            )

        self.safety_blockers = sorted(blockers, key=_safety_blocker_sort_key)

    @property
    def ordered_safety_blockers(self) -> list[SafetyBlocker]:
        """Return blockers in their canonical review and execution order."""
        return sorted(self.safety_blockers, key=_safety_blocker_sort_key)

    @staticmethod
    def _risk_evidence(
        status: str,
        *reasons: str,
        sources: tuple[str, ...] = (),
    ) -> dict[str, object]:
        """Build canonical evidence without carrying raw backend error text."""
        return {
            "status": status,
            "sources": sorted(set(sources)),
            "reasons": sorted(set(reasons)),
        }

    @staticmethod
    def _absence_is_verified(current: object) -> bool:
        """Whether a backend explicitly observed that a resource is absent."""
        return current is not None and getattr(current, "exists", None) is False

    def _topic_risk(
        self,
        change: TopicChange,
        impact_by_resource: dict[str, ImpactEntry],
    ) -> ChangeRisk:
        changes = change.changes or {}
        if "partitions_error" in changes:
            return ChangeRisk(
                kind="topic",
                resource=change.topic,
                action=change.action,
                assessment="destructive",
                risk_flags=("destructive", "policy_violation"),
                evidence=self._risk_evidence(
                    "verified",
                    "partition_reduction_requested",
                    sources=("kafka_topic_diff",),
                ),
            )
        if not changes:
            return ChangeRisk(
                kind="topic",
                resource=change.topic,
                action=change.action,
                assessment="unknown",
                risk_flags=("live_state_unverified",),
                evidence=self._risk_evidence(
                    "unavailable",
                    "topic_update_diff_missing",
                ),
            )

        flags: set[str] = set()
        reasons = {"topic_configuration_or_partition_change"}
        status = "verified"
        sources = {"kafka_topic_diff"}
        impact = impact_by_resource.get(change.topic)
        if impact is None:
            flags.update(("impact_unverified", "live_state_unverified"))
            reasons.add("canonical_topic_impact_missing")
            status = "partial"
        else:
            sources.add("canonical_topic_impact")
            if impact.downstream_models or impact.exposures or impact.consumers:
                flags.add("consumer_impact")
            identity_status = impact.identity_evidence.get("status")
            graph_status = impact.graph_evidence.get("status")
            consumer_status = impact.consumer_evidence.get("status")
            if identity_status != "verified" or graph_status != "verified":
                flags.add("impact_unverified")
                reasons.add("declared_impact_evidence_incomplete")
                status = "partial"
            if consumer_status != "verified":
                flags.add("live_state_unverified")
                reasons.add("live_consumer_evidence_incomplete")
                status = "partial"
        return ChangeRisk(
            kind="topic",
            resource=change.topic,
            action=change.action,
            assessment="risky",
            risk_flags=tuple(sorted(flags)),
            evidence=self._risk_evidence(
                status,
                *reasons,
                sources=tuple(sources),
            ),
        )

    def _schema_risk(self, change: SchemaChange) -> ChangeRisk:
        changes = change.changes or {}
        if "schema_incompatible" in changes:
            return ChangeRisk(
                kind="schema",
                resource=change.subject,
                action=change.action,
                assessment="schema_breaking",
                risk_flags=("policy_violation", "schema_breaking"),
                evidence=self._risk_evidence(
                    "verified",
                    "registry_compatibility_rejected",
                    sources=("schema_registry_compatibility",),
                ),
            )
        if not changes:
            return ChangeRisk(
                kind="schema",
                resource=change.subject,
                action=change.action,
                assessment="unknown",
                risk_flags=("schema_impact_unverified",),
                evidence=self._risk_evidence(
                    "unavailable",
                    "schema_update_diff_missing",
                ),
            )

        schema_diff = changes.get("schema")
        if isinstance(schema_diff, dict) and schema_diff.get("compatible") is True:
            return ChangeRisk(
                kind="schema",
                resource=change.subject,
                action=change.action,
                assessment="risky",
                risk_flags=("schema_impact_unverified",),
                evidence=self._risk_evidence(
                    "partial",
                    "registry_compatibility_verified",
                    "downstream_contract_impact_unverified",
                    sources=("schema_registry_compatibility",),
                ),
            )
        if set(changes).issubset({"compatibility"}):
            return ChangeRisk(
                kind="schema",
                resource=change.subject,
                action=change.action,
                assessment="risky",
                evidence=self._risk_evidence(
                    "verified",
                    "compatibility_policy_change",
                    sources=("schema_registry_diff",),
                ),
            )
        return ChangeRisk(
            kind="schema",
            resource=change.subject,
            action=change.action,
            assessment="unknown",
            risk_flags=("schema_impact_unverified",),
            evidence=self._risk_evidence(
                "unavailable",
                "schema_compatibility_not_proven",
            ),
        )

    def _classify_change(
        self,
        *,
        kind: str,
        resource: str,
        action: str,
        current: object = None,
        changes: dict[str, object] | None = None,
        impact_by_resource: dict[str, ImpactEntry],
        topic_change: TopicChange | None = None,
        schema_change: SchemaChange | None = None,
    ) -> ChangeRisk:
        """Classify one effective backend action using only explicit evidence."""
        if action in ("delete", "cancel", "remove"):
            return ChangeRisk(
                kind=kind,
                resource=resource,
                action=action,
                assessment="destructive",
                risk_flags=("destructive",),
                evidence=self._risk_evidence(
                    "verified",
                    "resource_removal",
                    sources=("planned_action",),
                ),
            )
        if action in ("create", "register", "submit"):
            if kind == "flink_job" and getattr(current, "exists", None) is True:
                return ChangeRisk(
                    kind=kind,
                    resource=resource,
                    action=action,
                    assessment="state_migration_required",
                    risk_flags=(
                        "live_state_unverified",
                        "savepoint_required",
                        "stateful_upgrade",
                    ),
                    evidence=self._risk_evidence(
                        "unavailable",
                        "existing_job_resubmission_state_compatibility_unproven",
                        sources=("flink_job_state",),
                    ),
                )
            if self._absence_is_verified(current):
                return ChangeRisk(
                    kind=kind,
                    resource=resource,
                    action=action,
                    assessment="safe",
                    evidence=self._risk_evidence(
                        "verified",
                        "resource_absence_verified",
                        sources=("live_resource_state",),
                    ),
                )
            return ChangeRisk(
                kind=kind,
                resource=resource,
                action=action,
                assessment="unknown",
                risk_flags=("live_state_unverified",),
                evidence=self._risk_evidence(
                    "unavailable",
                    "resource_absence_not_verified",
                ),
            )
        if action == "update":
            if topic_change is not None:
                return self._topic_risk(topic_change, impact_by_resource)
            if schema_change is not None:
                return self._schema_risk(schema_change)
            if kind == "flink_job":
                return ChangeRisk(
                    kind=kind,
                    resource=resource,
                    action=action,
                    assessment="state_migration_required",
                    risk_flags=(
                        "live_state_unverified",
                        "savepoint_required",
                        "stateful_upgrade",
                    ),
                    evidence=self._risk_evidence(
                        "unavailable",
                        "operator_state_compatibility_unproven",
                        "savepoint_availability_unproven",
                        sources=("flink_job_diff",),
                    ),
                )
            if changes:
                flags: tuple[str, ...] = ()
                if any(
                    isinstance(value, dict)
                    and (
                        (kind == "connector" and value.get("change") == "removed")
                        or (kind != "connector" and value.get("to") is None)
                    )
                    for value in changes.values()
                ):
                    flags = ("destructive",)
                return ChangeRisk(
                    kind=kind,
                    resource=resource,
                    action=action,
                    assessment="risky",
                    risk_flags=flags,
                    evidence=self._risk_evidence(
                        "verified",
                        "backend_configuration_change",
                        sources=(f"{kind}_diff",),
                    ),
                )
            return ChangeRisk(
                kind=kind,
                resource=resource,
                action=action,
                assessment="unknown",
                risk_flags=("live_state_unverified",),
                evidence=self._risk_evidence(
                    "unavailable",
                    "update_diff_missing",
                ),
            )
        return ChangeRisk(
            kind=kind,
            resource=resource,
            action=action,
            assessment="unknown",
            risk_flags=("policy_violation",),
            evidence=self._risk_evidence(
                "unavailable",
                "unsupported_planned_action",
            ),
        )

    @property
    def ordered_change_risks(self) -> list[ChangeRisk]:
        """Derive canonical per-resource assessments from the current plan."""
        impact_by_resource = {entry.resource: entry for entry in self.impact_radius}
        assessments: list[ChangeRisk] = []
        for change in self.schema_changes:
            if change.action != "none":
                assessments.append(
                    self._classify_change(
                        kind="schema",
                        resource=change.subject,
                        action=change.action,
                        current=change.current,
                        changes=change.changes,
                        impact_by_resource=impact_by_resource,
                        schema_change=change,
                    )
                )
        for change in self.topic_changes:
            if change.action != "none":
                assessments.append(
                    self._classify_change(
                        kind="topic",
                        resource=change.topic,
                        action=change.action,
                        current=change.current,
                        changes=change.changes,
                        impact_by_resource=impact_by_resource,
                        topic_change=change,
                    )
                )
        for change in self.flink_changes:
            if change.action != "none":
                assessments.append(
                    self._classify_change(
                        kind="flink_job",
                        resource=change.job_name,
                        action=change.action,
                        current=change.current,
                        impact_by_resource=impact_by_resource,
                    )
                )
        for change in self.connector_changes:
            if change.action != "none":
                assessments.append(
                    self._classify_change(
                        kind="connector",
                        resource=change.connector_name,
                        action=change.action,
                        current=change.current,
                        changes=change.changes,
                        impact_by_resource=impact_by_resource,
                    )
                )
        for change in self.gateway_changes:
            if change.action != "none":
                changes = secret_neutral_gateway_changes(change.changes)
                assessments.append(
                    self._classify_change(
                        kind="gateway_rule",
                        resource=change.name,
                        action=change.action,
                        current=change.current,
                        changes=changes,
                        impact_by_resource=impact_by_resource,
                    )
                )
        return sorted(
            assessments,
            key=lambda item: (
                _CHANGE_KIND_ORDER.get(item.kind, 99),
                item.resource,
                item.action,
            ),
        )

    @property
    def risk_summary(self) -> dict[str, object]:
        """Return the plan-level risk assessment and fixed-shape counts."""
        changes = self.ordered_change_risks
        counts = {
            assessment: sum(1 for item in changes if item.assessment == assessment)
            for assessment in _RISK_ASSESSMENTS
        }
        overall = (
            max(changes, key=lambda item: _RISK_PRECEDENCE[item.assessment]).assessment
            if changes
            else "safe"
        )
        return {
            "overall": overall,
            "counts": counts,
            "risk_flags": sorted({flag for item in changes for flag in item.risk_flags}),
            "evidence_complete": not any(
                item.assessment == "unknown" or item.evidence.get("status") != "verified"
                for item in changes
            ),
        }

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return (
            any(c.action != "none" for c in self.schema_changes)
            or any(c.action != "none" for c in self.topic_changes)
            or any(c.action != "none" for c in self.flink_changes)
            or any(c.action != "none" for c in self.connector_changes)
            or any(c.action != "none" for c in self.gateway_changes)
        )

    @property
    def creates(self) -> int:
        """Count of resources to create."""
        return (
            sum(1 for c in self.schema_changes if c.action == "register")
            + sum(1 for c in self.topic_changes if c.action == "create")
            + sum(1 for c in self.flink_changes if c.action == "submit")
            + sum(1 for c in self.connector_changes if c.action == "create")
            + sum(1 for c in self.gateway_changes if c.action == "create")
        )

    @property
    def updates(self) -> int:
        """Count of resources to update."""
        return (
            sum(1 for c in self.schema_changes if c.action == "update")
            + sum(1 for c in self.topic_changes if c.action == "update")
            + sum(1 for c in self.flink_changes if c.action == "update")
            + sum(1 for c in self.connector_changes if c.action == "update")
            + sum(1 for c in self.gateway_changes if c.action == "update")
        )

    @property
    def deletes(self) -> int:
        """Count of resources to delete."""
        return (
            sum(1 for c in self.schema_changes if c.action == "delete")
            + sum(1 for c in self.topic_changes if c.action == "delete")
            + sum(1 for c in self.flink_changes if c.action == "cancel")
            + sum(1 for c in self.connector_changes if c.action == "delete")
            + sum(1 for c in self.gateway_changes if c.action == "delete")
        )

    @property
    def has_ownership_requirements(self) -> bool:
        """Whether explicit ownership decisions are required before mutation."""
        return bool(self.ownership_requirements)

    @property
    def blocking_ownership_requirements(self) -> list[OwnershipRequirement]:
        """Requirements that must block apply until ownership is resolved.

        ``external`` is an explicit observe-only decision, so it neutralizes
        that resource without preventing unrelated managed creates or updates.
        Every other ownership requirement is an apply blocker.
        """
        return [
            requirement
            for requirement in self.ownership_requirements
            if requirement.reason != "external"
        ]

    @property
    def is_apply_blocked(self) -> bool:
        """Whether apply must refuse this plan for ownership or safety policy."""
        return bool(self.blocking_ownership_requirements or self.safety_blockers)

    def summary(self) -> str:
        """Get a summary of the plan."""
        summary = (
            f"Plan: {self.creates} to create, {self.updates} to update, {self.deletes} to delete"
        )
        summary += f", risk: {self.risk_summary['overall']}"
        if self.ownership_requirements:
            summary += f", {len(self.ownership_requirements)} ownership requirement(s)"
        if self.safety_blockers:
            summary += f", {len(self.safety_blockers)} safety blocker(s)"
        return summary

    def details(self, color: bool = True) -> str:
        """Get detailed plan output with colored diff markers."""
        lines = [self.summary(), ""]

        def _add(prefix: str) -> str:
            return f"[green]{prefix}[/green]" if color else prefix

        def _upd(prefix: str) -> str:
            return f"[yellow]{prefix}[/yellow]" if color else prefix

        def _rm(prefix: str) -> str:
            return f"[red]{prefix}[/red]" if color else prefix

        for change in self.schema_changes:
            if change.action == "register":
                lines.append(_add(f"+ schema: {change.subject}"))
                if change.desired:
                    lines.append(f"    type: {change.desired.schema_type}")
            elif change.action == "update":
                lines.append(_upd(f"~ schema: {change.subject}"))
                for key, val in (change.changes or {}).items():
                    if key == "schema":
                        lines.append(f"    version: {val['from_version']} -> {val['to_version']}")
                    elif key == "compatibility":
                        lines.append(f"    compatibility: {val['from']} -> {val['to']}")
                    elif key == "schema_incompatible":
                        message = (
                            val.get("message", "schema is incompatible")
                            if isinstance(val, dict)
                            else "schema is incompatible"
                        )
                        lines.append(f"    blocked: {message}")
            elif change.action == "delete":
                lines.append(_rm(f"- schema: {change.subject}"))

        for change in self.topic_changes:
            if change.action == "create":
                lines.append(_add(f"+ topic: {change.topic}"))
                if change.desired:
                    lines.append(f"    partitions: {change.desired.partitions}")
                    lines.append(f"    replication_factor: {change.desired.replication_factor}")
            elif change.action == "update":
                lines.append(_upd(f"~ topic: {change.topic}"))
                for key, val in (change.changes or {}).items():
                    if key == "partitions_error":
                        message = (
                            val.get("message", "partitions cannot be reduced")
                            if isinstance(val, dict)
                            else "partitions cannot be reduced"
                        )
                        lines.append(f"    blocked: {message}")
                    elif isinstance(val, dict) and "from" in val and "to" in val:
                        lines.append(f"    {key}: {val['from']} -> {val['to']}")
            elif change.action == "delete":
                lines.append(_rm(f"- topic: {change.topic}"))

        for change in self.flink_changes:
            if change.action == "submit":
                lines.append(_add(f"+ flink_job: {change.job_name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ flink_job: {change.job_name}"))
            elif change.action == "cancel":
                lines.append(_rm(f"- flink_job: {change.job_name}"))

        for change in self.connector_changes:
            if change.action == "create":
                lines.append(_add(f"+ connector: {change.connector_name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ connector: {change.connector_name}"))
                for key, evidence in secret_neutral_connector_changes(change.changes).items():
                    from_evidence = "present" if evidence.get("from_present") is True else "absent"
                    to_evidence = "present" if evidence.get("to_present") is True else "absent"
                    lines.append(
                        f"    {key}: {evidence['change']} ({from_evidence} -> {to_evidence})"
                    )
            elif change.action == "delete":
                lines.append(_rm(f"- connector: {change.connector_name}"))

        for change in self.gateway_changes:
            if change.action == "create":
                lines.append(_add(f"+ gateway_rule: {change.name}"))
            elif change.action == "update":
                lines.append(_upd(f"~ gateway_rule: {change.name}"))
                evidence = secret_neutral_gateway_changes(change.changes)
                categories = evidence.get("categories", [])
                lines.append("    drift: " + ", ".join(str(category) for category in categories))
                for surface in ("current", "desired"):
                    value = evidence.get(surface)
                    if isinstance(value, dict):
                        lines.append(
                            f"    {surface}: {value['fingerprint']} "
                            f"({value['managed_interceptor_count']} interceptor(s))"
                        )
            elif change.action == "delete":
                lines.append(_rm(f"- gateway_rule: {change.name}"))

        if not self.has_changes:
            lines.append("No changes detected.")

        if self.ordered_change_risks:
            lines.append("")
            lines.append("Risk Classification:")
            lines.append(f"  overall: {self.risk_summary['overall']}")
            for risk in self.ordered_change_risks:
                flag_suffix = f" flags={','.join(risk.risk_flags)}" if risk.risk_flags else ""
                lines.append(
                    f"  {risk.kind}: {risk.resource} ({risk.action}) "
                    f"[{risk.assessment}]{flag_suffix}"
                )
                reasons = risk.evidence.get("reasons", [])
                reason_suffix = (
                    f" ({','.join(str(reason) for reason in reasons)})"
                    if isinstance(reasons, list) and reasons
                    else ""
                )
                lines.append(
                    f"    evidence: {risk.evidence.get('status', 'unavailable')}{reason_suffix}"
                )

        if self.ownership_requirements:
            lines.append("")
            lines.append("Ownership Requirements:")
            for requirement in self.ownership_requirements:
                lines.append(f"  ! {requirement.kind}: {requirement.logical_name}")
                lines.append(f"    {requirement.message}")
                lines.append(f"    resource_id: {requirement.resource_id}")

        if self.safety_blockers:
            lines.append("")
            lines.append("Safety Blockers:")
            for blocker in self.ordered_safety_blockers:
                lines.append(f"  ! [{blocker.code}] {blocker.kind}: {blocker.resource}")
                lines.append(f"    {blocker.message}")

        if self.impact_radius:
            lines.append("")
            lines.append("Impact Analysis:")
            for entry in self.impact_radius:
                logical = entry.logical_resource or "logical identity unavailable"
                lines.append(f"  {entry.resource} ({entry.change_type}) [{logical}]")
                if entry.identity_evidence.get("status") != "verified":
                    lines.append(
                        "    identity evidence: "
                        f"{entry.identity_evidence.get('status', 'unavailable')} "
                        f"({entry.identity_evidence.get('reason', 'unknown')})"
                    )
                if entry.graph_evidence.get("status") != "verified":
                    lines.append(
                        "    graph evidence: "
                        f"{entry.graph_evidence.get('status', 'unavailable')} "
                        f"({entry.graph_evidence.get('reason', 'unknown')})"
                    )
                if entry.downstream_models:
                    lines.append(f"    downstream: {', '.join(entry.downstream_models)}")
                if entry.owners:
                    lines.append(f"    owners: {', '.join(entry.owners)}")
                for exposure in entry.exposures:
                    owners = exposure.get("owners", [])
                    owner_suffix = (
                        f" owners={','.join(str(owner) for owner in owners)}"
                        if isinstance(owners, list) and owners
                        else ""
                    )
                    lines.append(f"    exposure: {exposure.get('name', 'unknown')}{owner_suffix}")
                evidence_status = entry.consumer_evidence.get("status", "unavailable")
                evidence_reason = entry.consumer_evidence.get("reason")
                evidence_suffix = f" ({evidence_reason})" if evidence_reason else ""
                lines.append(f"    consumer evidence: {evidence_status}{evidence_suffix}")
                if entry.consumers:
                    for c in entry.consumers:
                        declared = "" if c.get("declared", True) else " [undeclared]"
                        lines.append(
                            f"    consumer: {c['group_id']}{declared} lag={c.get('lag', 0)}"
                        )
                failures = entry.consumer_evidence.get("failures", [])
                if isinstance(failures, list):
                    for failure in failures:
                        if isinstance(failure, dict):
                            lines.append(
                                "    evidence failure: "
                                f"{failure.get('scope', 'kafka')} "
                                f"{failure.get('message', 'unavailable')}"
                            )

        return "\n".join(lines)


class DeploymentPlanner:
    """Plans and executes deployments."""

    def __init__(
        self,
        manifest: Manifest,
        schema_registry_deployer: Optional[SchemaRegistryDeployer] = None,
        kafka_deployer: Optional[KafkaDeployer] = None,
        flink_deployer: Optional[FlinkDeployer] = None,
        connect_deployer: Optional[ConnectDeployer] = None,
        gateway_deployer: Optional[GatewayDeployer] = None,
        project: Optional[object] = None,
        prior_state: Optional[LocalState] = None,
        project_name: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> None:
        """Initialize deployment planner."""
        self.manifest = manifest
        self.schema_registry_deployer = schema_registry_deployer
        self.kafka_deployer = kafka_deployer
        self.flink_deployer = flink_deployer
        self.connect_deployer = connect_deployer
        self.gateway_deployer = gateway_deployer
        self.project = project
        self.prior_state = prior_state
        self.project_name = project_name or manifest.project_name
        self.environment = environment or (prior_state.environment if prior_state else "default")
        if prior_state and prior_state.project != self.project_name:
            raise StateIdentityError(
                f"prior state belongs to project {prior_state.project!r}, "
                f"expected {self.project_name!r}"
            )
        if prior_state and prior_state.environment != self.environment:
            raise StateIdentityError(
                f"prior state belongs to environment {prior_state.environment!r}, "
                f"expected {self.environment!r}"
            )

    @staticmethod
    def _resource_exists(
        current: object,
        observed_action: str,
        create_actions: frozenset[str],
    ) -> bool:
        """Determine existence without confusing Flink re-submission with creation."""
        if current is not None and hasattr(current, "exists"):
            return bool(current.exists)  # type: ignore[attr-defined]
        return observed_action not in create_actions

    def _apply_ownership_policy(
        self,
        plan: DeploymentPlan,
        *,
        kind: str,
        logical_name: str,
        physical_name: str,
        ownership: ArtifactOwnership | dict[str, str] | None,
        change: _PlannedChange,
        current: object = None,
        create_actions: frozenset[str],
        expected_backend: str | None = None,
    ) -> None:
        """Neutralize changes that lack explicit authority over a live resource."""
        observed_action = str(change.action)
        parsed_ownership = ArtifactOwnership.from_dict(ownership)
        ownership_mode = parsed_ownership.mode if parsed_ownership else "managed"
        if parsed_ownership:
            logical_name = parsed_ownership.owner_name
        resource_uri = resource_id(
            self.project_name,
            self.environment,
            kind,
            logical_name,
        )

        reason: str | None = None
        message: str | None = None
        if parsed_ownership and parsed_ownership.project != self.project_name:
            reason = "ownership_mismatch"
            message = (
                f"Artifact declares project {parsed_ownership.project!r}, but this plan is for "
                f"{self.project_name!r}; mutation is blocked."
            )
        elif ownership_mode == "external":
            reason = "external"
            message = "Resource is declared external and is observe-only."
        elif ownership_mode not in ("managed", "adopted"):
            reason = "invalid_ownership"
            message = f"Unknown ownership mode {ownership_mode!r}; mutation is blocked."
        else:
            prior_record = (
                self.prior_state.resources.get(resource_uri) if self.prior_state else None
            )
            exists = self._resource_exists(current, observed_action, create_actions)
            if (
                prior_record is not None
                and expected_backend is not None
                and (
                    prior_record.backend != expected_backend
                    or prior_record.physical_name != physical_name
                )
            ):
                reason = "state_mismatch"
                message = (
                    "Prior ownership belongs to a different provider identity; "
                    "explicit ownership reconciliation is required."
                )
            elif ownership_mode == "adopted" and prior_record is None:
                reason = "requires_adoption"
                message = (
                    "Declaring ownership.mode 'adopted' does not grant authority; "
                    "matching persisted ownership state is required before mutation."
                )
            elif exists:
                if prior_record is None:
                    reason = "requires_adoption"
                    message = (
                        "Live resource has no matching prior streamt ownership; "
                        "explicit adoption is required before mutation."
                    )
                elif prior_record.physical_name != physical_name:
                    reason = "state_mismatch"
                    message = (
                        f"Prior ownership points to {prior_record.physical_name!r}, not "
                        f"{physical_name!r}; explicit ownership reconciliation is required."
                    )

        if reason is None or message is None:
            return

        change.action = "none"
        plan.ownership_requirements.append(
            OwnershipRequirement(
                resource_id=resource_uri,
                kind=kind,
                logical_name=logical_name,
                physical_name=physical_name,
                reason=reason,
                observed_action=observed_action,
                ownership_mode=ownership_mode,
                message=message,
            )
        )

    def _connect_binding_from_project(self) -> ConnectClusterBinding:
        """Resolve the exact default Connect binding without constructing a deployer."""
        if not isinstance(self.project, StreamtProject):
            raise ConnectClusterBindingError(
                "Connector planning requires parsed project runtime configuration"
            )
        connect = self.project.runtime.connect
        if connect is None or connect.default is None:
            raise ConnectClusterBindingError(
                "Connector planning requires an effective default Connect cluster"
            )
        cluster = connect.clusters.get(connect.default)
        if cluster is None:
            raise ConnectClusterBindingError(
                "Connector planning requires an effective default Connect cluster"
            )
        return ConnectClusterBinding.from_endpoint(connect.default, cluster.rest_url)

    def _resolved_connector_artifacts(
        self,
        binding: ConnectClusterBinding,
        *,
        resolver: Callable[[ConnectorArtifact], ConnectorArtifact] | None = None,
    ) -> list[ConnectorArtifact]:
        """Parse, bind, and reject Connector identity collisions before observation."""
        artifacts = []
        provider_locators: set[tuple[str, str]] = set()
        logical_owners: set[str] = set()
        for connector_data in self.manifest.artifacts.get("connectors", []):
            parsed = parse_compiled_connector_artifact(connector_data)
            artifact = (
                resolver(parsed)
                if resolver is not None
                else bind_connector_artifact(parsed, binding)
            )
            provider_locator = _connector_provider_locator(binding, artifact.name)
            if provider_locator in provider_locators:
                raise StateIdentityError(
                    "deployment manifest contains a duplicate Connector provider locator"
                )
            provider_locators.add(provider_locator)
            ownership = ArtifactOwnership.from_dict(artifact.ownership)
            logical_owner = ownership.owner_name if ownership is not None else artifact.name
            if logical_owner in logical_owners:
                raise StateIdentityError(
                    "deployment manifest maps one logical owner to multiple Connector artifacts"
                )
            logical_owners.add(logical_owner)
            artifacts.append(artifact)
        return artifacts

    def _gateway_binding_from_project(self) -> GatewayBackendBinding:
        """Resolve the exact configured Gateway endpoint and effective vCluster."""
        if not isinstance(self.project, StreamtProject):
            raise GatewayBindingError(
                "Gateway planning requires parsed project runtime configuration"
            )
        return _gateway_binding_from_parsed_project(self.project)

    def _prior_gateway_records(
        self,
    ) -> list[tuple[ResourceIdentity, ManagedResourceRecord]]:
        """Return canonical prior Gateway identities after rejecting claim collisions."""
        return list(_gateway_prior_records(self.prior_state))

    def _gateway_rules_with_prior_state_checks(
        self,
        binding: GatewayBackendBinding,
    ) -> tuple[ResolvedManagedGatewayRule, ...]:
        """Resolve manifest identities, then enforce planner-only prior claims."""
        try:
            resolved = resolve_managed_gateway_rules(
                self.manifest.artifacts.get("gateway_rules", []),
                binding,
            )
        except GatewayManifestResolutionError as error:
            raise StateIdentityError(str(error)) from None

        prior_records = self._prior_gateway_records()
        for rule in resolved:
            for identity, record in prior_records:
                if identity.logical_name == rule.logical_owner:
                    continue
                claims_desired_backend = record.backend == binding.backend_identity
                is_legacy_unbound = record.backend == "conduktor-gateway"
                if record.physical_name == rule.desired.alias_name and (
                    claims_desired_backend or is_legacy_unbound
                ):
                    raise StateIdentityError(
                        "Gateway desired alias is claimed by another logical record"
                    )
        return resolved

    def _validated_gateway_recovery_actions(
        self,
        *,
        actions: tuple[OperationAction, ...],
        binding: GatewayBackendBinding,
        rules: tuple[ResolvedManagedGatewayRule, ...],
    ) -> tuple[OperationAction, ...]:
        """Validate durable Gateway targets before any provider can be read."""
        if not isinstance(actions, tuple) or any(
            type(action) is not OperationAction for action in actions
        ):
            raise StateIdentityError(
                "Gateway recovery actions must be an exact immutable action tuple"
            )

        rules_by_owner = {rule.logical_owner: rule for rule in rules}
        resource_claims: dict[str, tuple[str, str, str]] = {}
        rule_claims: dict[tuple[str, str], str] = {}
        alias_claims: dict[tuple[str, str], str] = {}

        def claim(
            *,
            target_resource_id: str,
            backend_identity: str,
            rule_name: str,
            alias_name: str,
        ) -> None:
            provider_identity = (backend_identity, rule_name, alias_name)
            previous_resource = resource_claims.get(target_resource_id)
            if previous_resource is not None and previous_resource != provider_identity:
                raise StateIdentityError(
                    "Gateway recovery target collides on canonical resource identity"
                )
            resource_claims[target_resource_id] = provider_identity
            for claims, locator, label in (
                (rule_claims, (backend_identity, rule_name), "rule name"),
                (alias_claims, (backend_identity, alias_name), "alias"),
            ):
                previous = claims.get(locator)
                if previous is not None and previous != target_resource_id:
                    raise StateIdentityError(
                        f"Gateway recovery target collides on canonical {label} locator"
                    )
                claims[locator] = target_resource_id

        for rule in rules:
            desired = rule.desired
            claim(
                target_resource_id=resource_id(
                    self.project_name,
                    self.environment,
                    "gateway_rule",
                    rule.logical_owner,
                ),
                backend_identity=desired.binding.backend_identity,
                rule_name=desired.logical_name,
                alias_name=desired.alias_name,
            )

        if not actions:
            return actions
        prior_gateway_records = self._prior_gateway_records()
        action_resources: set[str] = set()
        for action in actions:
            try:
                identity = ResourceIdentity.parse(action.resource_id)
            except StateError as error:
                raise StateIdentityError(
                    "Gateway recovery action has an invalid canonical resource identity"
                ) from error
            if (
                identity.project != self.project_name
                or identity.environment != self.environment
                or identity.kind != "gateway_rule"
            ):
                raise StateIdentityError(
                    "Gateway recovery action belongs to another resource address"
                )
            if action.resource_id in action_resources:
                raise StateIdentityError(
                    "Gateway recovery actions contain a duplicate canonical resource"
                )
            action_resources.add(action.resource_id)
            if action.action not in ("create", "update", "delete", "adopt"):
                raise StateIdentityError("Gateway recovery action has an unsupported verb")
            evidence = action.gateway_evidence
            if type(evidence) is not GatewayActionEvidence:
                raise StateIdentityError("Gateway recovery action requires exact durable evidence")
            if evidence.backend_identity != binding.backend_identity:
                raise StateIdentityError(
                    "Gateway recovery action belongs to another provider binding"
                )

            prior_record = (
                self.prior_state.resources.get(action.resource_id)
                if self.prior_state is not None
                else None
            )
            exact_prior = prior_record is not None and (
                prior_record.backend == evidence.backend_identity
                and prior_record.physical_name == evidence.alias_name
            )
            if action.action == "adopt":
                if self.prior_state is None:
                    raise StateIdentityError(
                        "Gateway recovery adoption requires authoritative prior state"
                    )
                if prior_record is not None:
                    raise StateIdentityError(
                        "Gateway recovery adoption requires absent prior ownership"
                    )
            elif action.action == "create":
                if prior_record is not None and not exact_prior:
                    raise StateIdentityError(
                        "Gateway recovery create has mismatched prior ownership evidence"
                    )
            elif not exact_prior:
                raise StateIdentityError(
                    "Gateway recovery mutation requires exact prior ownership evidence"
                )

            if action.action == "delete" and any(
                prior_identity.uri != action.resource_id
                and record.physical_name == evidence.alias_name
                and record.backend in (binding.backend_identity, "conduktor-gateway")
                for prior_identity, record in prior_gateway_records
            ):
                raise StateIdentityError(
                    "Gateway recovery delete alias is claimed by another logical record"
                )

            desired_rule = rules_by_owner.get(identity.logical_name)
            if action.action in ("create", "update", "adopt"):
                if desired_rule is None:
                    raise StateIdentityError(
                        "Gateway recovery action has no exact desired manifest rule"
                    )
                desired = desired_rule.desired
                desired_ownership = ArtifactOwnership.from_dict(desired_rule.artifact.ownership)
                if action.action == "adopt" and (
                    desired_ownership
                    != ArtifactOwnership(
                        project=self.project_name,
                        owner_type="model",
                        owner_name=identity.logical_name,
                        mode="adopted",
                    )
                    or desired_rule.artifact.interceptors != []
                ):
                    raise StateIdentityError(
                        "Gateway recovery adoption requires exact adopted model ownership "
                        "and no desired interceptors"
                    )
                expected_surface = GatewayActionSurfaceEvidence(
                    exists=True,
                    fingerprint=desired.fingerprint,
                    managed_interceptor_count=len(desired.interceptors),
                )
                if (
                    evidence.rule_name != desired.logical_name
                    or evidence.alias_name != desired.alias_name
                    or evidence.backend_identity != desired.binding.backend_identity
                    or evidence.desired != expected_surface
                ):
                    raise StateIdentityError(
                        "Gateway recovery action differs from its exact desired rule"
                    )
            elif desired_rule is not None:
                raise StateIdentityError(
                    "Gateway recovery delete target is still present in the manifest"
                )

            claim(
                target_resource_id=action.resource_id,
                backend_identity=evidence.backend_identity,
                rule_name=evidence.rule_name,
                alias_name=evidence.alias_name,
            )
        return actions

    @staticmethod
    def _absent_gateway_rule(
        rule: ResolvedManagedGatewayRule,
    ) -> ManagedGatewayRuleObservation:
        """Construct explicit complete absence for deterministic offline planning."""
        return ManagedGatewayRuleObservation(
            binding=rule.desired.binding,
            logical_name=rule.artifact.name,
            alias_name=rule.artifact.virtual_topic,
            exists=False,
        )

    @staticmethod
    def _append_planned_connector_removal(
        plan: DeploymentPlan,
        removal: ResolvedConnectorRemoval,
        current: ManagedConnectorObservation,
        assessments: list[ConnectorRemovalAssessment],
    ) -> None:
        """Classify one preflighted tombstone from one exact live observation."""
        if (
            type(current) is not ManagedConnectorObservation
            or type(current.binding) is not ConnectClusterBinding
            or current.binding != removal.binding
            or current.name != removal.connector_name
        ):
            raise ConnectorRemovalPreflightError(
                "Connector removal observation does not match its exact target"
            )

        prior_record = removal.prior_record
        if not current.exists:
            assessments.append(
                _connector_removal_assessment(
                    removal,
                    "state_provider_drift" if prior_record is not None else "already_absent",
                )
            )
            return
        if prior_record is None:
            assessments.append(_connector_removal_assessment(removal, "ownership_required"))
            return

        prior_artifact = _reconstruct_connector_removal_artifact(removal, current)
        if artifact_checksum(prior_artifact.to_dict()) != prior_record.artifact_checksum:
            assessments.append(_connector_removal_assessment(removal, "state_provider_drift"))
            return
        plan.connector_changes.append(
            ConnectorChange(
                connector_name=removal.connector_name,
                action="delete",
                current=current,
                backend_identity=removal.binding.backend_identity,
            )
        )

    def _append_planned_gateway_rule(
        self,
        plan: DeploymentPlan,
        rule: ResolvedManagedGatewayRule,
        current: ManagedGatewayRuleObservation,
    ) -> None:
        """Purely plan and apply ownership policy to one resolved Gateway rule."""
        change = plan_managed_gateway_rule(rule.artifact, rule.desired, current)
        self._apply_ownership_policy(
            plan,
            kind="gateway_rule",
            logical_name=rule.logical_owner,
            physical_name=rule.artifact.virtual_topic,
            ownership=rule.artifact.ownership,
            change=change,
            current=change.current,
            create_actions=frozenset({"create"}),
            expected_backend=rule.desired.binding.backend_identity,
        )
        plan.gateway_changes.append(change)

    def _append_planned_gateway_removal(
        self,
        plan: DeploymentPlan,
        removal: ResolvedGatewayRuleRemoval,
        current: ManagedGatewayRuleObservation,
        assessments: list[GatewayRemovalAssessment],
    ) -> None:
        """Classify one validated tombstone from an exact shared observation."""
        if (
            type(current) is not ManagedGatewayRuleObservation
            or current.binding != removal.binding
            or current.logical_name != removal.rule_name
            or current.alias_name != removal.alias_name
        ):
            raise StateIdentityError("Gateway removal observation does not match its exact target")
        prior_record = (
            self.prior_state.resources.get(removal.resource_id)
            if self.prior_state is not None
            else None
        )
        has_prior_ownership = prior_record is not None

        if not current.exists:
            assessments.append(
                _gateway_removal_assessment(
                    removal,
                    "state_provider_drift" if has_prior_ownership else "already_absent",
                )
            )
            return

        change = plan_managed_gateway_rule_deletion(current)
        requirement_count = len(plan.ownership_requirements)
        self._apply_ownership_policy(
            plan,
            kind="gateway_rule",
            logical_name=removal.logical_owner,
            physical_name=removal.alias_name,
            ownership=removal.prior_artifact.ownership,
            change=change,
            current=current,
            create_actions=frozenset({"create"}),
            expected_backend=removal.binding.backend_identity,
        )
        if has_prior_ownership:
            if change.action != "delete" or len(plan.ownership_requirements) != requirement_count:
                raise StateIdentityError("Gateway removal lost exact ownership eligibility")
            plan.gateway_changes.append(change)
            return

        new_requirements = plan.ownership_requirements[requirement_count:]
        if (
            change.action != "none"
            or len(new_requirements) != 1
            or new_requirements[0].reason != "requires_adoption"
        ):
            raise StateIdentityError(
                "Unowned Gateway removal did not produce an ownership requirement"
            )
        assessments.append(_gateway_removal_assessment(removal, "ownership_required"))

    def offline_plan(self) -> DeploymentPlan:
        """Create a plan assuming no current state (all creates).

        Useful when infrastructure is unavailable — shows what a fresh
        deployment would look like without connecting to Kafka/SR/Flink.
        """
        from streamt.compiler.manifest import (
            FlinkJobArtifact,
            TopicArtifact,
        )
        from streamt.deployer.schema_registry import SchemaArtifact as SRArtifact

        plan = DeploymentPlan()
        raw_connector_removals = self.manifest.artifacts.get(
            "connector_removals",
            [],
        )
        if connector_removals_requested(raw_connector_removals):
            raise ConnectorRemovalPreflightError(
                "Connector removals require a complete online reviewed plan"
            )
        raw_gateway_removals = self.manifest.artifacts.get(
            "gateway_rule_removals",
            [],
        )
        gateway_targets = (
            resolve_gateway_planning_targets(
                self.manifest,
                self.project,
                environment=self.environment,
                prior_state=self.prior_state,
                require_authoritative_state=False,
            )
            if type(raw_gateway_removals) is not list or raw_gateway_removals
            else None
        )

        for schema_data in self.manifest.artifacts.get("schemas", []):
            try:
                artifact = SRArtifact(
                    subject=schema_data["subject"],
                    schema=schema_data["schema"],
                    schema_type=schema_data.get("schema_type", "AVRO"),
                    compatibility=schema_data.get("compatibility"),
                    ownership=ArtifactOwnership.from_dict(schema_data.get("ownership")),
                )
                change = SchemaChange(
                    subject=artifact.subject,
                    action="register",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="schema",
                    logical_name=artifact.subject,
                    physical_name=artifact.subject,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"register"}),
                )
                plan.schema_changes.append(change)
            except KeyError:
                pass

        for topic_data in self.manifest.artifacts.get("topics", []):
            try:
                artifact = TopicArtifact(**topic_data)
                change = TopicChange(topic=artifact.name, action="create", desired=artifact)
                self._apply_ownership_policy(
                    plan,
                    kind="topic",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"create"}),
                )
                plan.topic_changes.append(change)
            except (KeyError, TypeError):
                pass

        for job_data in self.manifest.artifacts.get("flink_jobs", []):
            try:
                artifact = FlinkJobArtifact(**job_data)
                change = FlinkJobChange(
                    job_name=artifact.name,
                    action="submit",
                    desired=artifact,
                )
                self._apply_ownership_policy(
                    plan,
                    kind="flink_job",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    create_actions=frozenset({"submit"}),
                )
                plan.flink_changes.append(change)
            except (KeyError, TypeError):
                pass

        connector_data = self.manifest.artifacts.get("connectors", [])
        connector_binding = self._connect_binding_from_project() if connector_data else None
        connector_artifacts = (
            self._resolved_connector_artifacts(connector_binding)
            if connector_binding is not None
            else []
        )
        for artifact in connector_artifacts:
            change = ConnectorChange(
                connector_name=artifact.name,
                action="create",
                desired=artifact,
                backend_identity=connector_binding.backend_identity,
            )
            self._apply_ownership_policy(
                plan,
                kind="connector",
                logical_name=artifact.name,
                physical_name=artifact.name,
                ownership=artifact.ownership,
                change=change,
                create_actions=frozenset({"create"}),
                expected_backend=connector_binding.backend_identity,
            )
            plan.connector_changes.append(change)

        gateway_data = self.manifest.artifacts.get("gateway_rules", [])
        if gateway_data:
            if gateway_targets is None:
                binding = self._gateway_binding_from_project()
                gateway_rules = self._gateway_rules_with_prior_state_checks(binding)
            else:
                gateway_rules = gateway_targets.desired_rules
            for rule in gateway_rules:
                self._append_planned_gateway_rule(
                    plan,
                    rule,
                    self._absent_gateway_rule(rule),
                )

        if gateway_targets is not None:
            plan.gateway_removal_assessments = tuple(
                _gateway_removal_assessment(removal, "offline_unverified")
                for removal in gateway_targets.removals
            )

        plan.refresh_safety_blockers()
        self._compute_impact_radius(plan)
        return plan

    def plan(
        self,
        *,
        gateway_recovery_actions: tuple[OperationAction, ...] = (),
    ) -> DeploymentPlan:
        """Create a deployment plan."""
        plan = DeploymentPlan()

        raw_connector_removals = self.manifest.artifacts.get(
            "connector_removals",
            [],
        )
        connector_targets = (
            resolve_connector_planning_targets(
                self.manifest,
                self.project,
                environment=self.environment,
                prior_state=self.prior_state,
                require_authoritative_state=True,
            )
            if connector_removals_requested(raw_connector_removals)
            else None
        )

        raw_gateway_removals = self.manifest.artifacts.get(
            "gateway_rule_removals",
            [],
        )
        gateway_targets = (
            resolve_gateway_planning_targets(
                self.manifest,
                self.project,
                environment=self.environment,
                prior_state=self.prior_state,
                require_authoritative_state=True,
            )
            if type(raw_gateway_removals) is not list or raw_gateway_removals
            else None
        )

        # Gateway identities are a whole-manifest preflight. Complete strict
        # parsing, binding, and collision checks before any provider is read.
        if not isinstance(gateway_recovery_actions, tuple):
            raise StateIdentityError(
                "Gateway recovery actions must be an exact immutable action tuple"
            )
        gateway_rules: tuple[ResolvedManagedGatewayRule, ...] = ()
        gateway_removals: tuple[ResolvedGatewayRuleRemoval, ...] = ()
        validated_gateway_recovery_actions: tuple[OperationAction, ...] = ()
        gateway_deployer = self.gateway_deployer
        gateway_data = self.manifest.artifacts.get("gateway_rules", [])
        configured_gateway_binding: GatewayBackendBinding | None = None
        if gateway_data or gateway_targets is not None or gateway_recovery_actions:
            configured_gateway_binding = (
                gateway_targets.binding
                if gateway_targets is not None
                else self._gateway_binding_from_project()
            )
            if gateway_deployer is None:
                raise GatewayBindingError("Live Gateway planning requires a bound Gateway deployer")
            if gateway_deployer.cluster_binding != configured_gateway_binding:
                raise GatewayBindingError(
                    "Gateway deployer binding does not match project runtime configuration"
                )
            gateway_rules = (
                gateway_targets.desired_rules
                if gateway_targets is not None
                else self._gateway_rules_with_prior_state_checks(configured_gateway_binding)
            )
            gateway_removals = gateway_targets.removals if gateway_targets is not None else ()
            validated_gateway_recovery_actions = self._validated_gateway_recovery_actions(
                actions=gateway_recovery_actions,
                binding=configured_gateway_binding,
                rules=gateway_rules,
            )

        # Plan schemas first (before topics that may depend on them)
        if self.schema_registry_deployer:
            from streamt.deployer.schema_registry import SchemaArtifact as SRArtifact

            for schema_data in self.manifest.artifacts.get("schemas", []):
                try:
                    artifact = SRArtifact(
                        subject=schema_data["subject"],
                        schema=schema_data["schema"],
                        schema_type=schema_data.get("schema_type", "AVRO"),
                        compatibility=schema_data.get("compatibility"),
                        ownership=ArtifactOwnership.from_dict(schema_data.get("ownership")),
                    )
                    change = self.schema_registry_deployer.plan_schema(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="schema",
                        logical_name=artifact.subject,
                        physical_name=artifact.subject,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"register"}),
                    )
                    plan.schema_changes.append(change)
                except KeyError as e:
                    logger.error("Malformed schema artifact, missing key %s: %s", e, schema_data)

        # Plan topics
        if self.kafka_deployer:
            from streamt.compiler.manifest import TopicArtifact

            for topic_data in self.manifest.artifacts.get("topics", []):
                try:
                    artifact = TopicArtifact(**topic_data)
                    change = self.kafka_deployer.plan_topic(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="topic",
                        logical_name=artifact.name,
                        physical_name=artifact.name,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"create"}),
                    )
                    plan.topic_changes.append(change)
                except (KeyError, TypeError) as e:
                    logger.error("Malformed topic artifact: %s in %s", e, topic_data)

        # Plan Flink jobs
        if self.flink_deployer:
            from streamt.compiler.manifest import FlinkJobArtifact

            for job_data in self.manifest.artifacts.get("flink_jobs", []):
                try:
                    artifact = FlinkJobArtifact(**job_data)
                    change = self.flink_deployer.plan_job(artifact)
                    self._apply_ownership_policy(
                        plan,
                        kind="flink_job",
                        logical_name=artifact.name,
                        physical_name=artifact.name,
                        ownership=artifact.ownership,
                        change=change,
                        current=change.current,
                        create_actions=frozenset({"submit"}),
                    )
                    plan.flink_changes.append(change)
                except (KeyError, TypeError) as e:
                    logger.error("Malformed flink_job artifact: %s in %s", e, job_data)

        # Plan connectors only through one exact bound cluster.
        connector_data = self.manifest.artifacts.get("connectors", [])
        if connector_data or connector_targets is not None:
            if self.connect_deployer is None:
                raise ConnectClusterBindingError(
                    "Live Connector planning requires a bound Connect deployer"
                )
            connector_binding = self.connect_deployer.require_cluster_binding()
            if type(connector_binding) is not ConnectClusterBinding:
                raise ConnectClusterBindingError(
                    "Connect deployer returned an invalid exact cluster binding"
                )
            if connector_targets is not None:
                configured_binding = connector_targets.binding
                if connector_binding != configured_binding:
                    raise ConnectClusterBindingError(
                        "Connect deployer binding does not match removal preflight"
                    )
            elif isinstance(self.project, StreamtProject):
                configured_binding = self._connect_binding_from_project()
                if connector_binding != configured_binding:
                    raise ConnectClusterBindingError(
                        "Connect deployer binding does not match project runtime configuration"
                    )
            connector_artifacts = (
                list(connector_targets.desired_connectors)
                if connector_targets is not None
                else self._resolved_connector_artifacts(
                    connector_binding,
                    resolver=self.connect_deployer.resolve_connector_artifact,
                )
            )
            for artifact in connector_artifacts:
                change = self.connect_deployer.plan_connector(artifact)
                if (
                    change.backend_identity != connector_binding.backend_identity
                    or change.desired != artifact
                ):
                    raise ConnectClusterBindingError(
                        "Connect deployer returned a change for a different provider identity"
                    )
                self._apply_ownership_policy(
                    plan,
                    kind="connector",
                    logical_name=artifact.name,
                    physical_name=artifact.name,
                    ownership=artifact.ownership,
                    change=change,
                    current=change.current,
                    create_actions=frozenset({"create"}),
                    expected_backend=connector_binding.backend_identity,
                )
                plan.connector_changes.append(change)

            if connector_targets is not None:
                observations: dict[
                    tuple[str, str],
                    ManagedConnectorObservation,
                ] = {}

                def observe_connector_removal(
                    removal: ResolvedConnectorRemoval,
                ) -> ManagedConnectorObservation:
                    locator = _connector_provider_locator(
                        removal.binding,
                        removal.connector_name,
                    )
                    if locator in observations:
                        return observations[locator]
                    try:
                        current = self.connect_deployer.observe_managed_connector(
                            removal.connector_name
                        )
                    except Exception:
                        raise ConnectorRemovalPreflightError(
                            "Connector removal live observation failed"
                        ) from None
                    if (
                        type(current) is not ManagedConnectorObservation
                        or type(current.binding) is not ConnectClusterBinding
                        or current.binding != removal.binding
                        or current.name != removal.connector_name
                    ):
                        raise ConnectorRemovalPreflightError(
                            "Connector removal observation does not match its exact target"
                        )
                    observations[locator] = current
                    return current

                removal_assessments: list[ConnectorRemovalAssessment] = []
                for removal in connector_targets.removals:
                    self._append_planned_connector_removal(
                        plan,
                        removal,
                        observe_connector_removal(removal),
                        removal_assessments,
                    )
                plan.connector_removal_assessments = tuple(removal_assessments)

        # Plan all Gateway rules from one exact, complete two-list snapshot.
        if gateway_rules or gateway_removals or validated_gateway_recovery_actions:
            if gateway_deployer is None:  # pragma: no cover - preflight narrows this
                raise GatewayBindingError("Live Gateway planning requires a bound Gateway deployer")
            snapshot = gateway_deployer.observe_managed_gateway_snapshot()
            if configured_gateway_binding is None:  # pragma: no cover - preflight narrows this
                raise GatewayBindingError("Gateway planning requires a configured project runtime")
            if snapshot.binding != configured_gateway_binding:
                raise GatewayBindingError(
                    "Gateway observation does not match project runtime configuration"
                )
            observations: dict[tuple[str, str], ManagedGatewayRuleObservation] = {}

            def observe(
                rule_name: str,
                alias_name: str,
            ) -> ManagedGatewayRuleObservation:
                locator = (rule_name, alias_name)
                current = observations.get(locator)
                if current is None:
                    current = snapshot.rule(rule_name, alias_name)
                    observations[locator] = current
                return current

            for rule in gateway_rules:
                current = observe(
                    rule.artifact.name,
                    rule.artifact.virtual_topic,
                )
                self._append_planned_gateway_rule(plan, rule, current)
            removal_assessments: list[GatewayRemovalAssessment] = []
            for removal in gateway_removals:
                current = observe(
                    removal.rule_name,
                    removal.alias_name,
                )
                self._append_planned_gateway_removal(
                    plan,
                    removal,
                    current,
                    removal_assessments,
                )
            plan.gateway_removal_assessments = tuple(removal_assessments)
            recovery_observations: list[GatewayRecoveryObservation] = []
            for action in validated_gateway_recovery_actions:
                evidence = action.gateway_evidence
                if evidence is None:  # pragma: no cover - strict preflight narrows this
                    raise StateIdentityError(
                        "Gateway recovery action requires exact durable evidence"
                    )
                recovery_observations.append(
                    GatewayRecoveryObservation(
                        resource_id=action.resource_id,
                        observation=observe(
                            evidence.rule_name,
                            evidence.alias_name,
                        ),
                    )
                )
            plan.gateway_recovery_observations = tuple(recovery_observations)

        # Compute impact radius for planned changes
        plan.refresh_safety_blockers()
        self._compute_impact_radius(plan)

        return plan

    def _compute_impact_radius(self, plan: DeploymentPlan) -> None:
        """Compute canonical graph and live-consumer evidence for changed topics."""
        plan.impact_radius.clear()
        changed_topics = sorted(
            (change.topic, change.action)
            for change in plan.topic_changes
            if change.action in ("create", "update")
        )
        if not changed_topics:
            return

        ownership_by_topic: dict[str, ArtifactOwnership] = {}
        ambiguous_topics: set[str] = set()
        for artifact in self.manifest.artifacts.get("topics", []):
            physical_name = artifact.get("name")
            ownership = ArtifactOwnership.from_dict(artifact.get("ownership"))
            if not isinstance(physical_name, str) or ownership is None:
                continue
            if ownership.owner_type not in ("source", "model"):
                continue
            previous = ownership_by_topic.get(physical_name)
            if previous is not None and previous != ownership:
                ambiguous_topics.add(physical_name)
                ownership_by_topic.pop(physical_name, None)
                continue
            if physical_name not in ambiguous_topics:
                ownership_by_topic[physical_name] = ownership

        dag = None
        graph_failure: dict[str, object] | None = None
        if self.project is None:
            graph_failure = {
                "status": "unavailable",
                "reason": "project_not_provided",
            }
        else:
            try:
                from streamt.core.dag import DAGBuilder

                dag = DAGBuilder(self.project).build()  # type: ignore[arg-type]
            except Exception as error:
                safe_error = _sanitize_error(str(error))
                logger.debug("Could not build DAG for impact analysis: %s", safe_error)
                graph_failure = {
                    "status": "failed",
                    "reason": "dag_build_failed",
                    "message": safe_error,
                }

        exposure_by_name = {
            exposure.name: exposure
            for exposure in getattr(self.project, "exposures", [])
            if isinstance(getattr(exposure, "name", None), str)
        }

        for topic_name, action in changed_topics:
            ownership = ownership_by_topic.get(topic_name)
            if ownership is not None:
                logical_type = ownership.owner_type
                logical_name = ownership.owner_name
                logical_resource = f"{logical_type}/{logical_name}"
                identity_evidence: dict[str, object] = {
                    "status": "verified",
                    "source": "manifest_artifact_ownership",
                }
            else:
                logical_type = None
                logical_name = None
                logical_resource = None
                identity_evidence = {
                    "status": "failed" if topic_name in ambiguous_topics else "unavailable",
                    "reason": (
                        "ambiguous_manifest_ownership"
                        if topic_name in ambiguous_topics
                        else "manifest_ownership_missing"
                    ),
                }

            downstream_models: list[str] = []
            exposure_entries: list[dict[str, object]] = []
            owners: set[str] = set()
            graph_evidence = dict(graph_failure) if graph_failure else {"status": "verified"}
            if self.project is not None and logical_name is not None:
                declaration = self._logical_declaration(logical_type, logical_name)
                declared_owner = getattr(declaration, "owner", None)
                if isinstance(declared_owner, str) and declared_owner:
                    owners.add(_sanitize_error(declared_owner))
            changed_declaration_owners = set(owners)
            if dag is not None and logical_name is not None:
                if dag.get_node(logical_name) is None:
                    graph_evidence = {
                        "status": "failed",
                        "reason": "logical_identity_not_in_dag",
                    }
                    consumers, consumer_evidence = self._consumer_impact(topic_name, {})
                    plan.impact_radius.append(
                        ImpactEntry(
                            resource=topic_name,
                            logical_type=logical_type,
                            logical_name=logical_name,
                            logical_resource=logical_resource,
                            change_type=("topic_create" if action == "create" else "topic_update"),
                            owners=sorted(owners),
                            consumers=consumers,
                            identity_evidence=identity_evidence,
                            graph_evidence=graph_evidence,
                            consumer_evidence=consumer_evidence,
                        )
                    )
                    continue
                try:
                    downstream_nodes = dag.get_downstream(logical_name)
                    downstream_models = sorted(
                        name for name in downstream_nodes if dag.nodes[name].type.value == "model"
                    )
                    for model_name in downstream_models:
                        model = self._logical_declaration("model", model_name)
                        model_owner = getattr(model, "owner", None)
                        if isinstance(model_owner, str) and model_owner:
                            owners.add(_sanitize_error(model_owner))
                    exposure_names = sorted(
                        name
                        for name in downstream_nodes
                        if dag.nodes[name].type.value == "exposure"
                    )
                    for exposure_name in exposure_names:
                        exposure = exposure_by_name.get(exposure_name)
                        if exposure is None:
                            continue
                        exposure_owners = self._exposure_owners(exposure)
                        owners.update(exposure_owners)
                        exposure_entries.append(
                            {
                                "name": exposure_name,
                                "owners": exposure_owners,
                                "consumer_group": getattr(exposure, "consumer_group", None),
                            }
                        )
                    graph_evidence = {
                        "status": "verified",
                        "source": "declared_project_dag",
                    }
                except Exception as error:
                    safe_error = _sanitize_error(str(error))
                    logger.debug("Could not traverse DAG for impact analysis: %s", safe_error)
                    downstream_models = []
                    exposure_entries = []
                    owners = changed_declaration_owners
                    graph_evidence = {
                        "status": "failed",
                        "reason": "dag_traversal_failed",
                        "message": safe_error,
                    }
            elif graph_failure is None:
                graph_evidence = {
                    "status": "unavailable",
                    "reason": "logical_identity_unavailable",
                }

            declared_exposures: dict[str, list[str]] = {}
            for exposure in exposure_entries:
                group_id = exposure.get("consumer_group")
                exposure_name = exposure.get("name")
                if isinstance(group_id, str) and isinstance(exposure_name, str):
                    declared_exposures.setdefault(group_id, []).append(exposure_name)

            consumers, consumer_evidence = self._consumer_impact(
                topic_name,
                declared_exposures,
            )
            plan.impact_radius.append(
                ImpactEntry(
                    resource=topic_name,
                    logical_type=logical_type,
                    logical_name=logical_name,
                    logical_resource=logical_resource,
                    change_type="topic_create" if action == "create" else "topic_update",
                    downstream_models=downstream_models,
                    exposures=exposure_entries,
                    owners=sorted(owners),
                    consumers=consumers,
                    identity_evidence=identity_evidence,
                    graph_evidence=graph_evidence,
                    consumer_evidence=consumer_evidence,
                )
            )

    def _logical_declaration(self, logical_type: str | None, name: str) -> object | None:
        """Resolve a source or model declaration without assuming a project subtype."""
        getter_name = "get_source" if logical_type == "source" else "get_model"
        getter = getattr(self.project, getter_name, None)
        return getter(name) if callable(getter) else None

    @staticmethod
    def _exposure_owners(exposure: object) -> list[str]:
        """Return every declared exposure owner as a deterministic identity list."""
        result: set[str] = set()
        owner = getattr(exposure, "owner", None)
        if isinstance(owner, str) and owner:
            result.add(_sanitize_error(owner))
        declared = getattr(exposure, "owners", None)
        if isinstance(declared, list):
            for item in declared:
                if isinstance(item, dict):
                    name = item.get("name")
                    if isinstance(name, str) and name:
                        result.add(_sanitize_error(name))
        return sorted(result)

    def _consumer_impact(
        self,
        topic_name: str,
        declared_exposures: dict[str, list[str]],
    ) -> tuple[list[dict[str, object]], dict[str, object]]:
        """Discover live topic consumers without converting failures into absence."""
        source = "kafka_consumer_groups"
        if self.kafka_deployer is None:
            return [], {
                "status": "unavailable",
                "source": source,
                "reason": "kafka_not_configured",
                "failures": [],
            }

        try:
            raw_groups = self.kafka_deployer.get_consumer_groups()
        except Exception as error:
            safe_error = _sanitize_error(str(error))
            logger.debug("Could not list consumer groups for impact analysis: %s", safe_error)
            return [], {
                "status": "unavailable",
                "source": source,
                "reason": "consumer_group_listing_failed",
                "failures": [
                    {
                        "scope": "consumer_group_list",
                        "code": "consumer_group_listing_failed",
                        "message": safe_error,
                    }
                ],
            }

        if not isinstance(raw_groups, list):
            return [], {
                "status": "unavailable",
                "source": source,
                "reason": "invalid_consumer_group_response",
                "failures": [
                    {
                        "scope": "consumer_group_list",
                        "code": "invalid_consumer_group_response",
                        "message": "Kafka returned a non-list consumer group response.",
                    }
                ],
            }

        failures: list[dict[str, object]] = []
        group_ids: set[str] = set()
        for group_id in raw_groups:
            if isinstance(group_id, str) and group_id:
                group_ids.add(group_id)
            else:
                failures.append(
                    {
                        "scope": "consumer_group_list",
                        "code": "invalid_consumer_group_identity",
                        "message": "Kafka returned a non-string consumer group identity.",
                    }
                )

        consumers: list[dict[str, object]] = []
        for group_id in sorted(group_ids):
            try:
                lag = self.kafka_deployer.get_consumer_group_lag(group_id, topic_name)
                if lag is None:
                    continue
                total_lag = lag.total_lag
                if not isinstance(total_lag, int) or isinstance(total_lag, bool):
                    raise ValueError("Kafka returned a non-integer consumer lag.")
            except Exception as error:
                safe_group = _sanitize_error(group_id)
                safe_error = _sanitize_error(str(error))
                logger.debug(
                    "Could not query consumer group %s for impact analysis: %s",
                    safe_group,
                    safe_error,
                )
                failures.append(
                    {
                        "scope": f"consumer_group/{safe_group}",
                        "code": "consumer_group_lag_failed",
                        "message": safe_error,
                    }
                )
                continue
            safe_group = _sanitize_error(group_id)
            declared_by = sorted(declared_exposures.get(group_id, []))
            consumers.append(
                {
                    "group_id": safe_group,
                    "lag": total_lag,
                    "declared": bool(declared_by),
                    "declared_exposures": declared_by,
                }
            )

        failures.sort(
            key=lambda failure: (
                str(failure.get("scope", "")),
                str(failure.get("code", "")),
                str(failure.get("message", "")),
            )
        )
        return consumers, {
            "status": "partial" if failures else "verified",
            "source": source,
            "reason": "consumer_queries_failed" if failures else None,
            "failures": failures,
        }

    @staticmethod
    def _bucket_for(result: str, create_verb: str) -> str:
        """Map an apply-result string to a result-bucket key."""
        if result == create_verb:
            return "created"
        return "updated" if result == "updated" else "unchanged"

    @staticmethod
    def _require_managed_gateway_action(
        change: object,
    ) -> tuple[
        ManagedGatewayRuleObservation,
        ManagedGatewayRuleObservation | None,
        str,
    ]:
        """Validate one actionable Gateway change and return its exact locator surfaces."""
        action = str(getattr(change, "action", ""))
        if action not in {"create", "update", "delete"}:
            raise StateIdentityError("Gateway change is not actionable")
        if not isinstance(change, GatewayRuleChange):
            raise StateIdentityError(
                "Actionable Gateway change requires the normalized change model"
            )

        current = getattr(change, "current", None)
        desired = getattr(change, "desired_managed", None)
        backend_identity = getattr(change, "backend_identity", None)
        logical_name = getattr(change, "name", None)
        if (
            not isinstance(current, ManagedGatewayRuleObservation)
            or not isinstance(logical_name, str)
            or not is_gateway_backend_identity(backend_identity)
            or backend_identity != current.binding.backend_identity
            or logical_name != current.logical_name
        ):
            raise StateIdentityError(
                "Actionable Gateway change requires complete normalized aggregate evidence"
            )

        try:
            if action == "delete":
                if (
                    desired is not None
                    or getattr(change, "desired", None) is not None
                    or not current.exists
                ):
                    raise StateIdentityError(
                        "Actionable Gateway change has incoherent normalized aggregate evidence"
                    )
                canonical = plan_managed_gateway_rule_deletion(current)
                if (
                    logical_name != canonical.name
                    or action != canonical.action
                    or getattr(change, "current_alias", None) is not None
                    or getattr(change, "current_interceptors", None) is not None
                    or getattr(change, "desired", None) is not canonical.desired
                    or desired is not canonical.desired_managed
                    or current != canonical.current
                    or getattr(change, "changes", None) != canonical.changes
                    or backend_identity != canonical.backend_identity
                ):
                    raise StateIdentityError(
                        "Actionable Gateway change differs from its canonical normalized evidence"
                    )
                return current, None, current.alias_name

            artifact = getattr(change, "desired", None)
            if (
                not isinstance(artifact, GatewayRuleArtifact)
                or not isinstance(desired, ManagedGatewayRuleObservation)
                or backend_identity != desired.binding.backend_identity
                or artifact.name != logical_name
                or artifact.virtual_topic != desired.alias_name
                or build_desired_gateway_rule(artifact, desired.binding) != desired
            ):
                raise StateIdentityError(
                    "Actionable Gateway change requires complete normalized aggregate evidence"
                )
            canonical = plan_managed_gateway_rule(
                artifact,
                desired,
                current=current,
            )
            if (
                logical_name != canonical.name
                or action != canonical.action
                or getattr(change, "current_alias", None) is not None
                or getattr(change, "current_interceptors", None) is not None
                or artifact != canonical.desired
                or desired != canonical.desired_managed
                or current != canonical.current
                or getattr(change, "changes", None) != canonical.changes
                or backend_identity != canonical.backend_identity
            ):
                raise StateIdentityError(
                    "Actionable Gateway change differs from its canonical normalized evidence"
                )
            return current, desired, desired.alias_name
        except (GatewayDesiredAggregateError, GatewayManagedMutationError) as exc:
            raise StateIdentityError(
                "Actionable Gateway change has incoherent normalized aggregate evidence"
            ) from exc

    @staticmethod
    def _gateway_action_evidence(
        *,
        action: str,
        current: ManagedGatewayRuleObservation,
        desired: ManagedGatewayRuleObservation | None,
    ) -> GatewayActionEvidence:
        """Freeze the exact secret-neutral Gateway transition before mutation."""
        candidate = desired
        if action == "delete":
            if desired is not None:
                raise StateIdentityError(
                    "Gateway delete evidence must have an explicit absent candidate"
                )
            candidate = ManagedGatewayRuleObservation(
                binding=current.binding,
                logical_name=current.logical_name,
                alias_name=current.alias_name,
                exists=False,
            )
        if candidate is None:
            raise StateIdentityError("Gateway action evidence requires an exact desired candidate")
        return GatewayActionEvidence(
            version=1,
            backend_identity=current.binding.backend_identity,
            rule_name=current.logical_name,
            alias_name=current.alias_name,
            current=GatewayActionSurfaceEvidence(
                exists=current.exists,
                fingerprint=current.fingerprint,
                managed_interceptor_count=len(current.interceptors),
            ),
            desired=GatewayActionSurfaceEvidence(
                exists=candidate.exists,
                fingerprint=candidate.fingerprint,
                managed_interceptor_count=len(candidate.interceptors),
            ),
        )

    def _apply_resource_changes(
        self,
        results: dict[str, object],
        deployer: Optional[object],
        changes: list[object],
        *,
        upsert_actions: tuple[str, ...],
        label_fn: Callable[[object], str],
        apply_fn: Callable[[object], str],
        create_verb: str,
        delete_action: str = "delete",
        delete_fn: Optional[Callable[[object], None]] = None,
        before_action: Callable[[str, int], None] | None = None,
        after_action: Callable[[str, int, bool], None] | None = None,
        action_index: list[int] | None = None,
        stop_on_error: bool = False,
        stop_requested: list[bool] | None = None,
    ) -> None:
        """Apply a homogeneous list of resource changes, recording outcomes into results."""
        if not deployer:
            return
        if action_index is None:
            action_index = [0]
        if stop_requested is None:
            stop_requested = [False]
        for change in changes:
            if stop_requested[0]:
                break
            label = label_fn(change)
            if change.action in upsert_actions and getattr(change, "desired", None):
                current_index = action_index[0]
                if before_action is not None:
                    before_action(label, current_index)
                try:
                    result = apply_fn(change.desired)
                    results[self._bucket_for(result, create_verb)].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")
                    if after_action is not None:
                        after_action(label, current_index, False)
                    if stop_on_error:
                        stop_requested[0] = True
                else:
                    if after_action is not None:
                        after_action(label, current_index, True)
                action_index[0] += 1
            elif change.action == delete_action and delete_fn is not None:
                current_index = action_index[0]
                if before_action is not None:
                    before_action(label, current_index)
                try:
                    delete_fn(change)
                    results["deleted"].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")
                    if after_action is not None:
                        after_action(label, current_index, False)
                    if stop_on_error:
                        stop_requested[0] = True
                else:
                    if after_action is not None:
                        after_action(label, current_index, True)
                action_index[0] += 1

    def operation_actions(self, plan: DeploymentPlan) -> list[tuple[str, str]]:
        """Return the exact ordered runtime actions apply will attempt."""
        actions: list[tuple[str, str]] = []
        gateway_changes = list(getattr(plan, "gateway_changes", []))
        actionable_gateway_changes = [
            change
            for change in gateway_changes
            if str(change.action) in {"create", "update", "delete"}
        ]
        if actionable_gateway_changes and self.gateway_deployer is None:
            raise StateIdentityError(
                "Actionable Gateway plan requires a configured Gateway deployer"
            )

        def add_changes(
            deployer: object | None,
            changes: list[object],
            *,
            label_fn: Callable[[object], str],
            upsert_actions: tuple[str, ...],
            delete_action: str = "delete",
            delete_ready: Callable[[object], bool] | None = None,
        ) -> None:
            if deployer is None:
                return
            for change in changes:
                action = str(change.action)
                if (action in upsert_actions and getattr(change, "desired", None)) or (
                    action == delete_action and (delete_ready is None or delete_ready(change))
                ):
                    actions.append((label_fn(change), action))

        add_changes(
            self.schema_registry_deployer,
            list(getattr(plan, "schema_changes", [])),
            label_fn=lambda change: f"schema:{change.subject}",
            upsert_actions=("register", "update"),
        )
        add_changes(
            self.kafka_deployer,
            list(getattr(plan, "topic_changes", [])),
            label_fn=lambda change: f"topic:{change.topic}",
            upsert_actions=("create", "update"),
        )
        add_changes(
            self.flink_deployer,
            list(getattr(plan, "flink_changes", [])),
            label_fn=lambda change: f"flink_job:{change.job_name}",
            upsert_actions=("submit", "update"),
            delete_action="cancel",
            delete_ready=lambda change: bool(change.current and change.current.job_id),
        )
        add_changes(
            self.connect_deployer,
            list(getattr(plan, "connector_changes", [])),
            label_fn=lambda change: f"connector:{change.connector_name}",
            upsert_actions=("create", "update"),
        )
        if self.gateway_deployer is not None:
            for change in actionable_gateway_changes:
                action = str(change.action)
                self._require_managed_gateway_action(change)
                actions.append((f"gateway_rule:{change.name}", action))
        return actions

    def _planned_resource_id(
        self,
        *,
        kind: str,
        change: object,
        physical_name: str,
        expected_backend: str | None = None,
    ) -> str:
        """Resolve an action to ownership identity, never to its runtime label."""
        desired = getattr(change, "desired", None)
        ownership = ArtifactOwnership.from_dict(getattr(desired, "ownership", None))
        if ownership is not None:
            if ownership.project != self.project_name:
                raise StateIdentityError("planned action ownership belongs to another project")
            logical_name = ownership.owner_name
        else:
            logical_names: set[str] = set()
            if self.prior_state is not None:
                for prior_resource_id, record in self.prior_state.resources.items():
                    identity = ResourceIdentity.parse(prior_resource_id)
                    if (
                        identity.kind == kind
                        and record.physical_name == physical_name
                        and (expected_backend is None or record.backend == expected_backend)
                    ):
                        logical_names.add(identity.logical_name)
            if len(logical_names) > 1:
                raise StateIdentityError(
                    "planned action physical resource has ambiguous ownership identity"
                )
            if not logical_names:
                raise StateIdentityError("planned action has no canonical ownership identity")
            logical_name = next(iter(logical_names))
        return resource_id(
            self.project_name,
            self.environment,
            kind,
            logical_name,
        )

    def _planned_connector_removal_action(
        self,
        change: ConnectorChange,
    ) -> PlannedAction:
        """Bind one exact planned delete back to its explicit tombstone and state."""
        raw_removals = self.manifest.artifacts.get("connector_removals", [])
        if not connector_removals_requested(raw_removals):
            raise StateIdentityError("Connector deletion requires exact action evidence")
        if (
            type(change) is not ConnectorChange
            or type(change.action) is not str
            or change.action != "delete"
            or type(change.connector_name) is not str
            or type(change.backend_identity) is not str
            or type(change.current) is not ManagedConnectorObservation
            or type(change.current.binding) is not ConnectClusterBinding
            or type(change.current.binding.version) is not int
            or type(change.current.binding.cluster_alias) is not str
            or type(change.current.binding.endpoint_fingerprint) is not str
            or type(change.current.name) is not str
            or type(change.current.exists) is not bool
            or type(change.current.config) is not tuple
            or any(
                type(entry) is not tuple
                or len(entry) != 2
                or type(entry[0]) is not str
                or type(entry[1]) not in (str, bool, int, float)
                for entry in change.current.config
            )
            or change.desired is not None
            or type(change.changes) is not dict
            or change.changes
            or change.backend_identity != change.current.binding.backend_identity
        ):
            raise StateIdentityError(
                "Connector deletion requires an exact managed removal observation"
            )
        try:
            targets = resolve_connector_planning_targets(
                self.manifest,
                self.project,
                environment=self.environment,
                prior_state=self.prior_state,
                require_authoritative_state=True,
            )
        except (ConnectorRemovalPreflightError, RemoteStateRequiredError):
            raise StateIdentityError(
                "Connector deletion is not backed by exact removal preflight"
            ) from None
        matching = [
            removal
            for removal in targets.removals
            if removal.connector_name == change.connector_name
            and removal.binding.backend_identity == change.backend_identity
        ]
        if len(matching) != 1:
            raise StateIdentityError(
                "Connector deletion does not match one exact removal declaration"
            )
        removal = matching[0]
        return PlannedAction(
            resource_id=removal.resource_id,
            runtime_label=f"connector:{removal.connector_name}",
            action="delete",
            connector_evidence=_connector_removal_action_evidence(
                removal,
                change.current,
            ),
        )

    def planned_actions(self, plan: DeploymentPlan) -> list[PlannedAction]:
        """Return ordered runtime actions with canonical ownership identities."""
        actions: list[PlannedAction] = []
        gateway_changes = list(getattr(plan, "gateway_changes", []))
        actionable_gateway_changes = [
            change
            for change in gateway_changes
            if str(change.action) in {"create", "update", "delete"}
        ]
        if actionable_gateway_changes and self.gateway_deployer is None:
            raise StateIdentityError(
                "Actionable Gateway plan requires a configured Gateway deployer"
            )

        def add_changes(
            deployer: object | None,
            changes: list[object],
            *,
            kind: str,
            label_fn: Callable[[object], str],
            physical_name_fn: Callable[[object], str],
            upsert_actions: tuple[str, ...],
            delete_action: str = "delete",
            delete_ready: Callable[[object], bool] | None = None,
        ) -> None:
            if deployer is None:
                return
            for change in changes:
                action = str(change.action)
                if not (
                    (action in upsert_actions and getattr(change, "desired", None))
                    or (action == delete_action and (delete_ready is None or delete_ready(change)))
                ):
                    continue
                actions.append(
                    PlannedAction(
                        resource_id=self._planned_resource_id(
                            kind=kind,
                            change=change,
                            physical_name=physical_name_fn(change),
                        ),
                        runtime_label=label_fn(change),
                        action=action,
                    )
                )

        add_changes(
            self.schema_registry_deployer,
            list(getattr(plan, "schema_changes", [])),
            kind="schema",
            label_fn=lambda change: f"schema:{change.subject}",
            physical_name_fn=lambda change: str(change.subject),
            upsert_actions=("register", "update"),
        )
        add_changes(
            self.kafka_deployer,
            list(getattr(plan, "topic_changes", [])),
            kind="topic",
            label_fn=lambda change: f"topic:{change.topic}",
            physical_name_fn=lambda change: str(change.topic),
            upsert_actions=("create", "update"),
        )
        add_changes(
            self.flink_deployer,
            list(getattr(plan, "flink_changes", [])),
            kind="flink_job",
            label_fn=lambda change: f"flink_job:{change.job_name}",
            physical_name_fn=lambda change: str(change.job_name),
            upsert_actions=("submit", "update"),
            delete_action="cancel",
            delete_ready=lambda change: bool(change.current and change.current.job_id),
        )
        if self.connect_deployer is not None:
            for connector_change in list(getattr(plan, "connector_changes", [])):
                connector_action = str(connector_change.action)
                if connector_action in {"create", "update"} and connector_change.desired:
                    actions.append(
                        PlannedAction(
                            resource_id=self._planned_resource_id(
                                kind="connector",
                                change=connector_change,
                                physical_name=str(connector_change.connector_name),
                            ),
                            runtime_label=f"connector:{connector_change.connector_name}",
                            action=connector_action,
                        )
                    )
                elif connector_action == "delete":
                    actions.append(self._planned_connector_removal_action(connector_change))
        if self.gateway_deployer is not None:
            for change in actionable_gateway_changes:
                action = str(change.action)
                current, desired, alias_name = self._require_managed_gateway_action(change)
                actions.append(
                    PlannedAction(
                        resource_id=self._planned_resource_id(
                            kind="gateway_rule",
                            change=change,
                            physical_name=alias_name,
                            expected_backend=change.backend_identity,
                        ),
                        runtime_label=f"gateway_rule:{change.name}",
                        action=action,
                        gateway_evidence=self._gateway_action_evidence(
                            action=action,
                            current=current,
                            desired=desired,
                        ),
                    )
                )

        seen_resource_ids: set[str] = set()
        for planned_action in actions:
            identity = ResourceIdentity.parse(planned_action.resource_id)
            if identity.project != self.project_name or identity.environment != self.environment:
                raise StateIdentityError("planned action identity belongs to another state address")
            if planned_action.resource_id in seen_resource_ids:
                raise StateIdentityError(
                    "deployment plan contains duplicate canonical action identity"
                )
            seen_resource_ids.add(planned_action.resource_id)
        return actions

    def apply(
        self,
        plan: Optional[DeploymentPlan] = None,
        *,
        before_action: Callable[[str, int], None] | None = None,
        after_action: Callable[[str, int, bool], None] | None = None,
        stop_on_error: bool = False,
    ) -> dict[str, object]:
        """Apply a deployment plan."""
        if plan is None:
            plan = self.plan()
        cd = self.connect_deployer
        connector_removal_source = connector_removals_requested(
            self.manifest.artifacts.get("connector_removals", [])
        )
        connector_actions: list[
            tuple[
                Literal["create", "update", "managed_delete", "legacy_delete", "none"],
                str,
                ConnectorArtifact | ManagedConnectorObservation | None,
            ]
        ] = []
        managed_connector_resource_ids: set[str] = set()
        for change in plan.connector_changes:
            if (
                type(change) is not ConnectorChange
                or type(change.action) is not str
                or change.action not in {"create", "update", "delete", "none"}
                or type(change.connector_name) is not str
                or not change.connector_name.strip()
                or type(change.changes) is not dict
                or (
                    change.backend_identity is not None
                    and type(change.backend_identity) is not str
                )
            ):
                raise StateIdentityError("Connector plan contains an invalid action shape")
            action = change.action
            connector_name = change.connector_name
            if action in {"create", "update"}:
                if type(change.desired) is not ConnectorArtifact:
                    raise StateIdentityError(
                        "Connector upsert requires an exact compiled artifact"
                    )
                connector_actions.append((action, connector_name, change.desired))
                continue
            if action == "none":
                connector_actions.append(("none", connector_name, None))
                continue
            if change.desired is not None or change.changes:
                raise StateIdentityError("Connector delete contains an invalid action shape")

            managed_candidate = connector_removal_source or isinstance(
                change.current,
                ManagedConnectorObservation,
            )
            if not managed_candidate:
                if change.current is not None and type(change.current) is not ConnectorState:
                    raise StateIdentityError(
                        "Legacy Connector delete contains an invalid current state"
                    )
                connector_actions.append(("legacy_delete", connector_name, None))
                continue
            if cd is None:
                raise StateIdentityError(
                    "Managed Connector deletion requires a configured Connect deployer"
                )
            current = change.current
            if (
                type(current) is not ManagedConnectorObservation
                or type(current.binding) is not ConnectClusterBinding
                or type(current.binding.version) is not int
                or type(current.binding.cluster_alias) is not str
                or type(current.binding.endpoint_fingerprint) is not str
                or type(current.name) is not str
                or type(current.exists) is not bool
                or type(current.config) is not tuple
                or any(
                    type(entry) is not tuple
                    or len(entry) != 2
                    or type(entry[0]) is not str
                    or type(entry[1]) not in (str, bool, int, float)
                    for entry in current.config
                )
            ):
                raise StateIdentityError(
                    "Connector deletion requires an exact managed removal observation"
                )
            try:
                managed_current = ManagedConnectorObservation(
                    binding=ConnectClusterBinding(
                        version=current.binding.version,
                        cluster_alias=current.binding.cluster_alias,
                        endpoint_fingerprint=current.binding.endpoint_fingerprint,
                    ),
                    name=current.name,
                    exists=current.exists,
                    config=tuple((key, value) for key, value in current.config),
                )
                managed_change = ConnectorChange(
                    connector_name=connector_name,
                    action=action,
                    current=managed_current,
                    desired=None,
                    changes={},
                    backend_identity=change.backend_identity,
                )
            except Exception:
                raise StateIdentityError(
                    "Connector deletion requires an exact managed removal observation"
                ) from None
            # Resolve every lifecycle delete back to its exact tombstone and
            # authoritative prior-state checksum before any provider mutation.
            planned_action = self._planned_connector_removal_action(managed_change)
            if planned_action.resource_id in managed_connector_resource_ids:
                raise StateIdentityError(
                    "deployment plan contains duplicate canonical action identity"
                )
            managed_connector_resource_ids.add(planned_action.resource_id)
            connector_actions.append(("managed_delete", connector_name, managed_current))

        gd = self.gateway_deployer
        actionable_gateway_changes = [
            change
            for change in plan.gateway_changes
            if change.action in {"create", "update", "delete"}
        ]
        if actionable_gateway_changes and gd is None:
            raise StateIdentityError(
                "Actionable Gateway plan requires a configured Gateway deployer"
            )
        gateway_actions: list[
            tuple[
                GatewayRuleChange,
                ManagedGatewayRuleObservation,
                ManagedGatewayRuleObservation | None,
            ]
        ] = []
        if gd is not None:
            for change in actionable_gateway_changes:
                current, desired, _ = self._require_managed_gateway_action(change)
                gateway_actions.append((change, current, desired))

        results: dict[str, object] = {
            "created": [],
            "updated": [],
            "deleted": [],
            "unchanged": [],
            "errors": [],
        }
        action_index = [0]
        stop_requested = [False]

        # Apply schemas first (before topics that may use them)
        sr = self.schema_registry_deployer
        self._apply_resource_changes(
            results,
            sr,
            plan.schema_changes,
            upsert_actions=("register", "update"),
            label_fn=lambda c: f"schema:{c.subject}",
            apply_fn=lambda desired: sr.apply_schema(desired),  # type: ignore[union-attr]
            create_verb="registered",
            delete_fn=lambda c: sr.delete_subject(c.subject),  # type: ignore[union-attr]
            before_action=before_action,
            after_action=after_action,
            action_index=action_index,
            stop_on_error=stop_on_error,
            stop_requested=stop_requested,
        )

        kd = self.kafka_deployer
        self._apply_resource_changes(
            results,
            kd,
            plan.topic_changes,
            upsert_actions=("create", "update"),
            label_fn=lambda c: f"topic:{c.topic}",
            apply_fn=lambda desired: kd.apply_topic(desired),  # type: ignore[union-attr]
            create_verb="created",
            delete_fn=lambda c: kd.delete_topic(c.topic),  # type: ignore[union-attr]
            before_action=before_action,
            after_action=after_action,
            action_index=action_index,
            stop_on_error=stop_on_error,
            stop_requested=stop_requested,
        )

        # Flink: "submitted" maps to created/updated based on action; delete is "cancel"
        if self.flink_deployer:
            for change in plan.flink_changes:
                if stop_requested[0]:
                    break
                label = f"flink_job:{change.job_name}"
                if change.action in ("submit", "update") and change.desired:
                    current_index = action_index[0]
                    if before_action is not None:
                        before_action(label, current_index)
                    try:
                        result = self.flink_deployer.apply_job(change.desired)
                        if result == "submitted":
                            results["updated" if change.action == "update" else "created"].append(
                                label
                            )
                        else:
                            results["unchanged"].append(label)
                    except Exception as e:
                        results["errors"].append(f"{label}: {_sanitize_error(e)}")
                        if after_action is not None:
                            after_action(label, current_index, False)
                        if stop_on_error:
                            stop_requested[0] = True
                    else:
                        if after_action is not None:
                            after_action(label, current_index, True)
                    action_index[0] += 1
                elif change.action == "cancel" and change.current and change.current.job_id:
                    current_index = action_index[0]
                    if before_action is not None:
                        before_action(label, current_index)
                    try:
                        self.flink_deployer.cancel_job(change.current.job_id)
                        results["deleted"].append(label)
                    except Exception as e:
                        results["errors"].append(f"{label}: {_sanitize_error(e)}")
                        if after_action is not None:
                            after_action(label, current_index, False)
                        if stop_on_error:
                            stop_requested[0] = True
                    else:
                        if after_action is not None:
                            after_action(label, current_index, True)
                    action_index[0] += 1

        if cd is not None:
            for action, connector_name, value in connector_actions:
                if stop_requested[0]:
                    break
                if action == "none":
                    continue
                label = f"connector:{connector_name}"
                current_index = action_index[0]
                if before_action is not None:
                    before_action(label, current_index)
                try:
                    if action in {"create", "update"}:
                        if type(value) is not ConnectorArtifact:  # pragma: no cover
                            raise StateIdentityError(
                                "Connector upsert requires an exact compiled artifact"
                            )
                        result = cd.apply_connector(value)
                        results[self._bucket_for(result, "created")].append(label)
                    elif action == "managed_delete":
                        if type(value) is not ManagedConnectorObservation:  # pragma: no cover
                            raise StateIdentityError(
                                "Connector deletion requires exact managed evidence"
                            )
                        result = cd.delete_managed_connector(value)
                        if type(result) is not str or result != "deleted":
                            raise StateIdentityError(
                                "Connector managed delete returned an invalid result"
                            )
                        results["deleted"].append(label)
                    else:
                        cd.delete_connector(connector_name)
                        results["deleted"].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")
                    if after_action is not None:
                        after_action(label, current_index, False)
                    if stop_on_error or action == "managed_delete":
                        stop_requested[0] = True
                else:
                    if after_action is not None:
                        after_action(label, current_index, True)
                action_index[0] += 1

        if gd is not None:
            for change, current, desired in gateway_actions:
                if stop_requested[0]:
                    break
                label = f"gateway_rule:{change.name}"
                current_index = action_index[0]
                if before_action is not None:
                    before_action(label, current_index)
                try:
                    if change.action == "delete":
                        result = gd.delete_managed_gateway_rule(current)
                        if result != "deleted":
                            raise StateIdentityError(
                                "Gateway managed delete returned an invalid result"
                            )
                        results["deleted"].append(label)
                    else:
                        if desired is None:
                            raise StateIdentityError(
                                "Actionable Gateway change requires a desired aggregate"
                            )
                        result = gd.apply_managed_gateway_rule(current, desired)
                        expected_result = "created" if change.action == "create" else "updated"
                        if result != expected_result:
                            raise StateIdentityError(
                                "Gateway managed apply returned an invalid result"
                            )
                        results["created" if change.action == "create" else "updated"].append(label)
                except Exception as e:
                    results["errors"].append(f"{label}: {_sanitize_error(e)}")
                    if after_action is not None:
                        after_action(label, current_index, False)
                    if stop_on_error:
                        stop_requested[0] = True
                else:
                    if after_action is not None:
                        after_action(label, current_index, True)
                action_index[0] += 1

        # Track rollback candidates (newly created resources that could be undone)
        results["rollback_candidates"] = list(results["created"]) if results["errors"] else []
        results["summary"] = {
            "total": sum(len(v) for v in results.values() if isinstance(v, list)),
            "succeeded": len(results["created"])
            + len(results["updated"])
            + len(results["deleted"]),
            "failed": len(results["errors"]),
            "unchanged": len(results["unchanged"]),
        }

        return results

    def rollback(
        self,
        labels: list[str],
        *,
        plan: DeploymentPlan | None = None,
        before_action: Callable[[str, int], None] | None = None,
        after_action: Callable[[str, int, bool], None] | None = None,
        stop_on_error: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Attempt to delete previously created resources.

        Returns (rolled_back, rollback_errors) lists.
        """
        rolled_back: list[str] = []
        errors: list[str] = []
        gateway_rollbacks: dict[int, ManagedGatewayRuleObservation] = {}
        gateway_preflight_errors: list[tuple[int, str, Exception]] = []
        seen_gateway_labels: set[str] = set()
        for index, label in enumerate(labels):
            if label.partition(":")[0] != "gateway_rule":
                continue
            if label in seen_gateway_labels:
                gateway_preflight_errors.append(
                    (
                        index,
                        label,
                        StateIdentityError(
                            "Gateway rollback labels must identify unique exact creates"
                        ),
                    )
                )
                continue
            seen_gateway_labels.add(label)
            try:
                gateway_rollbacks[index] = self._resolve_gateway_create_rollback(
                    label,
                    plan,
                )
            except Exception as exc:
                gateway_preflight_errors.append((index, label, exc))

        if gateway_preflight_errors:
            failures = gateway_preflight_errors[:1] if stop_on_error else gateway_preflight_errors
            for index, label, exc in failures:
                if before_action is not None:
                    before_action(label, index)
                errors.append(f"{label}: {_sanitize_error(exc)}")
                if after_action is not None:
                    after_action(label, index, False)
            return rolled_back, errors

        for index, label in enumerate(labels):
            if before_action is not None:
                before_action(label, index)
            try:
                self._rollback_resource(
                    label,
                    gateway_rollback=gateway_rollbacks.get(index),
                )
                rolled_back.append(label)
            except Exception as e:
                errors.append(f"{label}: {_sanitize_error(e)}")
                if after_action is not None:
                    after_action(label, index, False)
                if stop_on_error:
                    break
            else:
                if after_action is not None:
                    after_action(label, index, True)
        return rolled_back, errors

    def _resolve_gateway_create_rollback(
        self,
        label: str,
        plan: DeploymentPlan | None,
    ) -> ManagedGatewayRuleObservation:
        """Resolve one Gateway create rollback from the exact reviewed plan."""
        if self.gateway_deployer is None:
            raise StateIdentityError("Gateway rollback requires a configured Gateway deployer")
        if plan is None:
            raise StateIdentityError("Gateway rollback requires the exact reviewed plan")
        matches = [
            change
            for change in plan.gateway_changes
            if change.action == "create" and f"gateway_rule:{change.name}" == label
        ]
        if len(matches) != 1:
            raise StateIdentityError("Gateway rollback requires one exact normalized create change")
        _, desired, _ = self._require_managed_gateway_action(matches[0])
        if desired is None:
            raise StateIdentityError("Gateway rollback requires the exact desired aggregate")
        return desired

    def _rollback_resource(
        self,
        label: str,
        *,
        gateway_rollback: ManagedGatewayRuleObservation | None = None,
    ) -> None:
        """Attempt to delete a resource by its apply label (e.g. 'topic:foo')."""
        kind, _, name = label.partition(":")
        if kind == "schema" and self.schema_registry_deployer:
            self.schema_registry_deployer.delete_subject(name)
        elif kind == "topic" and self.kafka_deployer:
            self.kafka_deployer.delete_topic(name)
        elif kind == "connector" and self.connect_deployer:
            self.connect_deployer.delete_connector(name)
        elif kind == "gateway_rule":
            if self.gateway_deployer is None or gateway_rollback is None:
                raise StateIdentityError(
                    "Gateway rollback requires exact normalized mutation evidence"
                )
            result = self.gateway_deployer.delete_managed_gateway_rule(gateway_rollback)
            if result != "deleted":
                raise StateIdentityError(
                    "Gateway managed rollback delete returned an invalid result"
                )
