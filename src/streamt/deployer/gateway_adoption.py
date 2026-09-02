"""Pure target and review evidence for alias-only Gateway adoption.

This module deliberately owns no provider handle.  Callers resolve and validate
all manifest, runtime, and state identities here before constructing a Gateway
deployer, then pass only strict observations into the review helpers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from streamt.compiler.gateway_artifact import (
    GatewayArtifactFormatError,
    parse_compiled_gateway_rule_artifact,
)
from streamt.compiler.manifest import ArtifactOwnership, GatewayRuleArtifact, Manifest
from streamt.core.models import StreamtProject
from streamt.deployer.gateway import (
    GatewayBackendBinding,
    GatewayBindingError,
    GatewayDesiredAggregateError,
    GatewayManagedObservationError,
    ManagedGatewayRuleObservation,
    build_desired_gateway_rule,
    is_gateway_resource_name,
)
from streamt.deployer.planner import resolve_gateway_planning_targets
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateError,
    artifact_checksum,
)
from streamt.deployer.state_backend import (
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
)

_CHECKSUM = re.compile(r"^sha256:[0-9a-f]{64}$")
_PENDING_CHANGE_CATEGORIES = frozenset({"alias_mapping"})


class GatewayAdoptionError(ValueError):
    """One alias-only Gateway adoption invariant was not satisfied."""


class GatewayAdoptionLiveNotFoundError(GatewayAdoptionError):
    """The exact alias selected for adoption is absent."""


class GatewayAdoptionDriftError(GatewayAdoptionError):
    """The confirmed alias aggregate changed before the state transaction."""


def gateway_alias_mapping_checksum(
    physical_topic: str,
    physical_cluster: str,
) -> str:
    """Hash one canonical alias mapping without exposing it in review output."""
    if not is_gateway_resource_name(physical_topic) or physical_cluster != "main":
        raise GatewayAdoptionError(
            "Gateway adoption mapping must use a named topic and canonical main cluster"
        )
    return artifact_checksum(
        {
            "physicalTopic": physical_topic,
            "physicalCluster": physical_cluster,
        }
    )


@dataclass(frozen=True, init=False)
class GatewayAdoptionTarget:
    """One provider-free, state-bound alias-only Gateway adoption target."""

    resource_id: str
    logical_owner: str
    binding: GatewayBackendBinding
    rule_name: str
    alias_name: str
    desired: ManagedGatewayRuleObservation = field(repr=False)
    desired_artifact_checksum: str
    _artifact_json: str = field(repr=False)
    existing_record: ManagedResourceRecord | None = field(default=None, repr=False)

    def __init__(
        self,
        *,
        resource_id: str,
        logical_owner: str,
        binding: GatewayBackendBinding,
        artifact: GatewayRuleArtifact,
        desired: ManagedGatewayRuleObservation,
        desired_artifact_checksum: str,
        existing_record: ManagedResourceRecord | None,
    ) -> None:
        try:
            identity = ResourceIdentity.parse(resource_id)
            parsed_artifact = parse_compiled_gateway_rule_artifact(artifact.to_dict())
            parsed_ownership = ArtifactOwnership.from_dict(parsed_artifact.ownership)
            detached_binding = GatewayBackendBinding(
                virtual_cluster=binding.virtual_cluster,
                endpoint_fingerprint=binding.endpoint_fingerprint,
                api_version=binding.api_version,
                version=binding.version,
            )
            expected_desired = build_desired_gateway_rule(
                parsed_artifact,
                detached_binding,
            )
            expected_checksum = artifact_checksum(parsed_artifact.to_dict())
        except (
            AttributeError,
            GatewayArtifactFormatError,
            GatewayBindingError,
            GatewayDesiredAggregateError,
            GatewayManagedObservationError,
            StateError,
            TypeError,
            ValueError,
        ):
            raise GatewayAdoptionError("Gateway adoption target is malformed") from None

        expected_ownership = ArtifactOwnership(
            project=identity.project,
            owner_type="model",
            owner_name=logical_owner,
            mode="adopted",
        )
        if (
            identity.kind != "gateway_rule"
            or identity.logical_name != logical_owner
            or parsed_ownership != expected_ownership
            or parsed_artifact.interceptors
            or desired != expected_desired
            or desired_artifact_checksum != expected_checksum
            or not expected_desired.exists
            or expected_desired.physical_cluster != "main"
            or expected_desired.interceptors
        ):
            raise GatewayAdoptionError(
                "Gateway adoption target is not one exact adopted alias-only rule"
            )

        expected_record = ManagedResourceRecord(
            physical_name=parsed_artifact.virtual_topic,
            ownership="adopted",
            artifact_checksum=expected_checksum,
            backend=detached_binding.backend_identity,
        )
        if existing_record is not None:
            if type(existing_record) is not ManagedResourceRecord or not (
                existing_record.physical_name == expected_record.physical_name
                and existing_record.artifact_checksum == expected_record.artifact_checksum
                and existing_record.backend == expected_record.backend
                and existing_record.ownership in ("managed", "adopted")
            ):
                raise GatewayAdoptionError(
                    "Gateway adoption target conflicts with its existing ownership record"
                )

        object.__setattr__(self, "resource_id", identity.uri)
        object.__setattr__(self, "logical_owner", logical_owner)
        object.__setattr__(self, "binding", detached_binding)
        object.__setattr__(self, "rule_name", parsed_artifact.name)
        object.__setattr__(self, "alias_name", parsed_artifact.virtual_topic)
        object.__setattr__(self, "desired", expected_desired)
        object.__setattr__(self, "desired_artifact_checksum", expected_checksum)
        object.__setattr__(self, "existing_record", existing_record)
        object.__setattr__(
            self,
            "_artifact_json",
            json.dumps(
                parsed_artifact.to_dict(),
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )

    @property
    def artifact(self) -> GatewayRuleArtifact:
        """Return an independent strict copy of the compiled desired artifact."""
        return parse_compiled_gateway_rule_artifact(json.loads(self._artifact_json))

    @property
    def desired_record(self) -> ManagedResourceRecord:
        """Return the exact ownership record a later state transaction may claim."""
        return ManagedResourceRecord(
            physical_name=self.alias_name,
            ownership="adopted",
            artifact_checksum=self.desired_artifact_checksum,
            backend=self.binding.backend_identity,
        )


@dataclass(frozen=True)
class GatewayAdoptionReviewEvidence:
    """Secret-neutral review of one exact present alias-only observation."""

    resource_id: str
    effective_vcluster: str
    endpoint_fingerprint: str
    alias_name: str
    physical_cluster: str
    observed_mapping_checksum: str
    desired_mapping_checksum: str
    desired_artifact_checksum: str
    pending_change_categories: tuple[str, ...]
    observed_aggregate_fingerprint: str
    desired_aggregate_fingerprint: str

    def __post_init__(self) -> None:
        try:
            identity = ResourceIdentity.parse(self.resource_id)
            GatewayBackendBinding(
                virtual_cluster=self.effective_vcluster,
                endpoint_fingerprint=self.endpoint_fingerprint,
            )
        except (GatewayBindingError, StateError):
            raise GatewayAdoptionError(
                "Gateway adoption review resource identity is invalid"
            ) from None
        checksums = (
            self.endpoint_fingerprint,
            self.observed_mapping_checksum,
            self.desired_mapping_checksum,
            self.desired_artifact_checksum,
            self.observed_aggregate_fingerprint,
            self.desired_aggregate_fingerprint,
        )
        if (
            identity.kind != "gateway_rule"
            or not is_gateway_resource_name(self.alias_name)
            or self.physical_cluster != "main"
            or any(
                not isinstance(value, str) or _CHECKSUM.fullmatch(value) is None
                for value in checksums
            )
            or type(self.pending_change_categories) is not tuple
            or tuple(sorted(self.pending_change_categories)) != self.pending_change_categories
            or len(set(self.pending_change_categories)) != len(self.pending_change_categories)
            or not set(self.pending_change_categories).issubset(_PENDING_CHANGE_CATEGORIES)
            or (
                ("alias_mapping" in self.pending_change_categories)
                != (self.observed_mapping_checksum != self.desired_mapping_checksum)
            )
        ):
            raise GatewayAdoptionError("Gateway adoption review evidence is malformed")

    def to_dict(self) -> dict[str, object]:
        """Return the stable secret-neutral review payload."""
        return {
            "resource_id": self.resource_id,
            "effective_vcluster": self.effective_vcluster,
            "endpoint_fingerprint": self.endpoint_fingerprint,
            "alias_name": self.alias_name,
            "physical_cluster": self.physical_cluster,
            "observed_mapping_checksum": self.observed_mapping_checksum,
            "desired_mapping_checksum": self.desired_mapping_checksum,
            "desired_artifact_checksum": self.desired_artifact_checksum,
            "pending_change_categories": list(self.pending_change_categories),
            "observed_aggregate_fingerprint": (self.observed_aggregate_fingerprint),
            "desired_aggregate_fingerprint": self.desired_aggregate_fingerprint,
        }


def resolve_gateway_adoption_target(
    manifest: Manifest,
    project: StreamtProject,
    *,
    environment: str,
    logical_name: str,
    prior_state: LocalState,
) -> GatewayAdoptionTarget:
    """Resolve one alias-only target with full-manifest/state collision checks."""
    if type(prior_state) is not LocalState:
        raise GatewayAdoptionError(
            "Gateway adoption target resolution requires exact authoritative state"
        )
    if not isinstance(manifest, Manifest) or not isinstance(project, StreamtProject):
        raise GatewayAdoptionError(
            "Gateway adoption target resolution requires a parsed project and manifest"
        )
    if not isinstance(logical_name, str) or not logical_name:
        raise GatewayAdoptionError("Gateway adoption target resolution requires one logical owner")

    try:
        targets = resolve_gateway_planning_targets(
            manifest,
            project,
            environment=environment,
            prior_state=prior_state,
            require_authoritative_state=True,
        )
    except (
        GatewayArtifactFormatError,
        GatewayBindingError,
        GatewayDesiredAggregateError,
        GatewayManagedObservationError,
        StateError,
        TypeError,
        ValueError,
    ):
        raise GatewayAdoptionError(
            "Gateway adoption target preflight rejected the manifest, runtime, or state"
        ) from None

    matches = tuple(rule for rule in targets.desired_rules if rule.logical_owner == logical_name)
    if len(matches) != 1:
        raise GatewayAdoptionError(
            "Gateway adoption requires exactly one compiled rule for the logical owner"
        )
    rule = matches[0]
    ownership = ArtifactOwnership.from_dict(rule.artifact.ownership)
    if ownership != ArtifactOwnership(
        project=manifest.project_name,
        owner_type="model",
        owner_name=logical_name,
        mode="adopted",
    ):
        raise GatewayAdoptionError("Gateway adoption requires exact adopted model ownership")
    if rule.desired.interceptors:
        raise GatewayAdoptionError(
            "Gateway adoption supports only rules with no desired interceptors"
        )

    desired_checksum = artifact_checksum(rule.artifact.to_dict())
    resource_uri = ResourceIdentity(
        manifest.project_name,
        environment,
        "gateway_rule",
        logical_name,
    ).uri
    existing_record = prior_state.resources.get(resource_uri)
    expected_record = ManagedResourceRecord(
        physical_name=rule.desired.alias_name,
        ownership="adopted",
        artifact_checksum=desired_checksum,
        backend=targets.binding.backend_identity,
    )
    if existing_record is not None:
        if not (
            existing_record.physical_name == expected_record.physical_name
            and existing_record.artifact_checksum == expected_record.artifact_checksum
            and existing_record.backend == expected_record.backend
            and existing_record.ownership in ("managed", "adopted")
        ):
            raise GatewayAdoptionError(
                "Gateway adoption target conflicts with existing ownership state"
            )

    return GatewayAdoptionTarget(
        resource_id=resource_uri,
        logical_owner=logical_name,
        binding=targets.binding,
        artifact=rule.artifact,
        desired=rule.desired,
        desired_artifact_checksum=desired_checksum,
        existing_record=existing_record,
    )


def validate_gateway_adoption_observation(
    target: GatewayAdoptionTarget,
    observation: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    """Validate and detach one exact present alias-only live observation."""
    if (
        type(target) is not GatewayAdoptionTarget
        or type(observation) is not ManagedGatewayRuleObservation
    ):
        raise GatewayAdoptionError("Gateway adoption requires one exact managed observation")
    if (
        observation.binding != target.binding
        or observation.logical_name != target.rule_name
        or observation.alias_name != target.alias_name
    ):
        raise GatewayAdoptionError(
            "Gateway adoption observation does not match the resolved target"
        )
    if not observation.exists:
        raise GatewayAdoptionLiveNotFoundError(
            "Gateway adoption requires an existing alias and never creates it"
        )
    if observation.physical_cluster != "main":
        raise GatewayAdoptionError("Gateway adoption requires canonical physical cluster 'main'")
    if observation.interceptors:
        raise GatewayAdoptionError("Gateway alias-only adoption cannot claim owned interceptors")
    physical_name = observation.physical_name
    if not isinstance(physical_name, str) or not physical_name:
        raise GatewayAdoptionError(
            "Gateway adoption observation is not one canonical alias mapping"
        )
    try:
        gateway_alias_mapping_checksum(
            physical_name,
            "main",
        )
        return ManagedGatewayRuleObservation(
            binding=GatewayBackendBinding(
                virtual_cluster=observation.binding.virtual_cluster,
                endpoint_fingerprint=observation.binding.endpoint_fingerprint,
                api_version=observation.binding.api_version,
                version=observation.binding.version,
            ),
            logical_name=observation.logical_name,
            alias_name=observation.alias_name,
            exists=True,
            physical_name=physical_name,
            physical_cluster="main",
            interceptors=(),
        )
    except (
        GatewayAdoptionError,
        GatewayBindingError,
        GatewayManagedObservationError,
        TypeError,
        ValueError,
    ):
        raise GatewayAdoptionError(
            "Gateway adoption observation is not one canonical alias mapping"
        ) from None


def build_gateway_adoption_review(
    target: GatewayAdoptionTarget,
    observation: ManagedGatewayRuleObservation,
) -> GatewayAdoptionReviewEvidence:
    """Build the exact secret-neutral review for one validated observation."""
    current = validate_gateway_adoption_observation(target, observation)
    desired = target.desired
    if current.physical_name is None or desired.physical_name is None:
        raise GatewayAdoptionError("Gateway adoption mapping evidence is incomplete")
    observed_mapping_checksum = gateway_alias_mapping_checksum(
        current.physical_name,
        "main",
    )
    desired_mapping_checksum = gateway_alias_mapping_checksum(
        desired.physical_name,
        "main",
    )
    pending_categories = (
        ("alias_mapping",) if observed_mapping_checksum != desired_mapping_checksum else ()
    )
    return GatewayAdoptionReviewEvidence(
        resource_id=target.resource_id,
        effective_vcluster=target.binding.virtual_cluster,
        endpoint_fingerprint=target.binding.endpoint_fingerprint,
        alias_name=target.alias_name,
        physical_cluster="main",
        observed_mapping_checksum=observed_mapping_checksum,
        desired_mapping_checksum=desired_mapping_checksum,
        desired_artifact_checksum=target.desired_artifact_checksum,
        pending_change_categories=pending_categories,
        observed_aggregate_fingerprint=current.fingerprint,
        desired_aggregate_fingerprint=desired.fingerprint,
    )


def require_unchanged_gateway_adoption_observation(
    target: GatewayAdoptionTarget,
    first: ManagedGatewayRuleObservation,
    second: ManagedGatewayRuleObservation,
) -> ManagedGatewayRuleObservation:
    """Validate both snapshots and require the confirmed aggregate to be stable."""
    confirmed = validate_gateway_adoption_observation(target, first)
    current = validate_gateway_adoption_observation(target, second)
    if current.fingerprint != confirmed.fingerprint:
        raise GatewayAdoptionDriftError(
            "Gateway adoption observation changed after confirmation; rerun adoption"
        )
    return current


def build_gateway_adoption_action_evidence(
    target: GatewayAdoptionTarget,
    observation: ManagedGatewayRuleObservation,
) -> GatewayActionEvidence:
    """Build exact v1 recovery evidence for a state-only Gateway adoption."""
    current = validate_gateway_adoption_observation(target, observation)
    desired = target.desired
    return GatewayActionEvidence(
        version=1,
        backend_identity=target.binding.backend_identity,
        rule_name=target.rule_name,
        alias_name=target.alias_name,
        current=GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint=current.fingerprint,
            managed_interceptor_count=0,
        ),
        desired=GatewayActionSurfaceEvidence(
            exists=True,
            fingerprint=desired.fingerprint,
            managed_interceptor_count=0,
        ),
    )
