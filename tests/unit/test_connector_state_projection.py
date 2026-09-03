"""Exact ownership-state projection for completed managed Connector deletion."""

from __future__ import annotations

import copy

import pytest

from streamt.compiler.manifest import ArtifactOwnership, ConnectorArtifact
from streamt.deployer.connect import ConnectClusterBinding, ConnectorChange
from streamt.deployer.planner import (
    ConnectorRemovalAssessment,
    DeploymentPlan,
    OwnershipRequirement,
)
from streamt.deployer.state import (
    LocalState,
    ManagedConnectorResourceDeletion,
    ManagedResourceRecord,
    StateFormatError,
    StateIdentityError,
    artifact_checksum,
    resource_id,
    updated_local_state,
)

_BINDING = ConnectClusterBinding.from_endpoint(
    "production",
    "https://connect.example.test/api",
)
_OTHER_BINDING = ConnectClusterBinding.from_endpoint(
    "production",
    "https://other-connect.example.test/api",
)
_SAME_ENDPOINT_OTHER_ALIAS_BINDING = ConnectClusterBinding(
    cluster_alias="shadow",
    endpoint_fingerprint=_BINDING.endpoint_fingerprint,
)


def _artifact(
    *,
    owner_name: str = "archive_orders",
    connector_name: str = "archive-orders-sink",
    cluster: str = "production",
    ownership_mode: str = "managed",
) -> ConnectorArtifact:
    return ConnectorArtifact(
        name=connector_name,
        connector_class="com.example.ArchiveSink",
        topics=["orders.events.v1"],
        cluster=cluster,
        config={"tasks.max": "1"},
        ownership=ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name=owner_name,
            mode=ownership_mode,
        ),
    )


def _connector_record(
    *,
    physical_name: str = "archive-orders-sink",
    ownership: str = "managed",
    backend: str = _BINDING.backend_identity,
    checksum: str | None = None,
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership=ownership,  # type: ignore[arg-type]
        artifact_checksum=checksum or artifact_checksum(_artifact().to_dict()),
        backend=backend,
    )


def _claim(
    *,
    owner_name: str = "archive_orders",
    project: str = "payments",
    environment: str = "prod",
    connector_name: str = "archive-orders-sink",
    backend: str = _BINDING.backend_identity,
    checksum: str | None = None,
) -> ManagedConnectorResourceDeletion:
    return ManagedConnectorResourceDeletion(
        resource_id=resource_id(
            project,
            environment,
            "connector",
            owner_name,
        ),
        backend_identity=backend,
        connector_name=connector_name,
        prior_artifact_checksum=checksum or artifact_checksum(_artifact().to_dict()),
    )


def _state(*, serial: int = 17) -> LocalState:
    target_id = _claim().resource_id
    unrelated_connector_id = resource_id(
        "payments",
        "prod",
        "connector",
        "active_orders",
    )
    topic_id = resource_id("payments", "prod", "topic", "orders_events")
    gateway_id = resource_id("payments", "prod", "gateway_rule", "orders_public")
    return LocalState(
        project="payments",
        environment="prod",
        serial=serial,
        resources={
            target_id: _connector_record(),
            unrelated_connector_id: _connector_record(
                physical_name="active-orders-sink",
                checksum=artifact_checksum(
                    _artifact(
                        owner_name="active_orders",
                        connector_name="active-orders-sink",
                    ).to_dict()
                ),
            ),
            topic_id: ManagedResourceRecord(
                physical_name="orders.events.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "orders.events.v1"}),
                backend="direct-kafka",
            ),
            gateway_id: ManagedResourceRecord(
                physical_name="orders.public",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "orders_rule"}),
                backend="conduktor-gateway:v1:sha256:"
                + "1" * 64
                + ":payments-prod",
            ),
        },
    )


def _desired_connector_plan(
    *,
    owner_name: str,
    connector_name: str,
    backend: str = _BINDING.backend_identity,
    ownership_mode: str = "managed",
) -> DeploymentPlan:
    binding = ConnectClusterBinding.from_backend_identity(backend)
    artifact = _artifact(
        owner_name=owner_name,
        connector_name=connector_name,
        cluster=binding.cluster_alias,
        ownership_mode=ownership_mode,
    )
    return DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name=connector_name,
                action="create",
                desired=artifact,
                backend_identity=backend,
            )
        ]
    )


class TestManagedConnectorDeletionValue:
    @pytest.mark.parametrize(
        ("values", "message"),
        [
            (
                {
                    "resource_id": resource_id(
                        "payments",
                        "prod",
                        "topic",
                        "archive_orders",
                    )
                },
                "identify a connector",
            ),
            ({"resource_id": "not-a-resource-id"}, "canonical"),
            ({"backend_identity": "kafka-connect"}, "canonical"),
            ({"connector_name": " "}, "connector_name"),
            ({"connector_name": "bad\nname"}, "connector_name"),
            ({"connector_name": "x" * 257}, "connector_name"),
            ({"prior_artifact_checksum": "sha256:nope"}, "checksum"),
        ],
    )
    def test_requires_exact_canonical_fields(
        self,
        values: dict[str, str],
        message: str,
    ) -> None:
        defaults = {
            "resource_id": _claim().resource_id,
            "backend_identity": _BINDING.backend_identity,
            "connector_name": "archive-orders-sink",
            "prior_artifact_checksum": artifact_checksum(_artifact().to_dict()),
            **values,
        }

        with pytest.raises(StateFormatError, match=message):
            ManagedConnectorResourceDeletion(**defaults)

    @pytest.mark.parametrize(
        "field_name",
        [
            "resource_id",
            "backend_identity",
            "connector_name",
            "prior_artifact_checksum",
        ],
    )
    def test_rejects_string_subclasses(self, field_name: str) -> None:
        class StringSubclass(str):
            pass

        values = {
            "resource_id": _claim().resource_id,
            "backend_identity": _BINDING.backend_identity,
            "connector_name": "archive-orders-sink",
            "prior_artifact_checksum": artifact_checksum(_artifact().to_dict()),
        }
        values[field_name] = StringSubclass(values[field_name])

        with pytest.raises(StateFormatError):
            ManagedConnectorResourceDeletion(**values)


class TestManagedConnectorStateProjection:
    def test_exact_completed_deletion_removes_only_matching_record(self) -> None:
        prior = _state()
        prior_payload = copy.deepcopy(prior.to_dict())
        prior_record_objects = dict(prior.resources)
        target_id = _claim().resource_id

        updated = updated_local_state(
            prior,
            DeploymentPlan(),
            managed_connector_deletions=(_claim(),),
        )

        assert updated is not None
        assert updated.serial == prior.serial + 1
        assert target_id not in updated.resources
        assert prior.to_dict() == prior_payload
        for resource_uri, record in prior_record_objects.items():
            if resource_uri != target_id:
                assert updated.resources[resource_uri] is record

    def test_two_exact_deletions_advance_serial_only_once(self) -> None:
        prior = _state(serial=8)
        second_artifact = _artifact(
            owner_name="active_orders",
            connector_name="active-orders-sink",
        )
        second_claim = _claim(
            owner_name="active_orders",
            connector_name="active-orders-sink",
            checksum=artifact_checksum(second_artifact.to_dict()),
        )

        updated = updated_local_state(
            prior,
            DeploymentPlan(),
            managed_connector_deletions=(_claim(), second_claim),
        )

        assert updated is not None
        assert updated.serial == 9
        assert all(
            resource_uri not in updated.resources
            for resource_uri in (_claim().resource_id, second_claim.resource_id)
        )

    def test_deletion_and_unrelated_desired_update_advance_serial_once(self) -> None:
        prior = _state(serial=3)
        desired = _desired_connector_plan(
            owner_name="new_orders",
            connector_name="new-orders-sink",
        )

        updated = updated_local_state(
            prior,
            desired,
            managed_connector_deletions=(_claim(),),
        )

        assert updated is not None
        assert updated.serial == 4
        assert _claim().resource_id not in updated.resources
        assert resource_id(
            "payments",
            "prod",
            "connector",
            "new_orders",
        ) in updated.resources

    def test_tombstone_assessment_and_manual_delete_are_inert_without_claim(self) -> None:
        prior = _state()
        manual_delete = DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name="archive-orders-sink",
                    action="delete",
                    backend_identity=_BINDING.backend_identity,
                )
            ]
        )
        assessed_absence = DeploymentPlan(
            connector_removal_assessments=(
                ConnectorRemovalAssessment(
                    resource_id=_claim().resource_id,
                    logical_owner="archive_orders",
                    connector_name="archive-orders-sink",
                    backend_identity=_BINDING.backend_identity,
                    status="already_absent",
                ),
            )
        )

        assert updated_local_state(prior, DeploymentPlan()) is None
        assert updated_local_state(prior, manual_delete) is None
        assert updated_local_state(prior, assessed_absence) is None
        assert _claim().resource_id in prior.resources

    @pytest.mark.parametrize(
        "deletions",
        [
            [],
            (_claim(), object()),
        ],
    )
    def test_input_requires_exact_tuple_of_exact_values(self, deletions: object) -> None:
        with pytest.raises(StateFormatError, match="exact"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=deletions,  # type: ignore[arg-type]
            )

    def test_rejects_deletion_value_subclass(self) -> None:
        class DeletionSubclass(ManagedConnectorResourceDeletion):
            pass

        claim = _claim()
        subclass = DeletionSubclass(
            resource_id=claim.resource_id,
            backend_identity=claim.backend_identity,
            connector_name=claim.connector_name,
            prior_artifact_checksum=claim.prior_artifact_checksum,
        )

        with pytest.raises(StateFormatError, match="exact deletion values"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=(subclass,),
            )

    @pytest.mark.parametrize(
        "deletion",
        [
            _claim(project="other"),
            _claim(environment="dev"),
        ],
    )
    def test_claim_must_belong_to_prior_state(
        self,
        deletion: ManagedConnectorResourceDeletion,
    ) -> None:
        with pytest.raises(StateIdentityError, match="current state"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=(deletion,),
            )

    @pytest.mark.parametrize(
        "deletion",
        [
            _claim(owner_name="missing_owner"),
            _claim(connector_name="other-sink"),
            _claim(backend=_OTHER_BINDING.backend_identity),
            _claim(checksum=artifact_checksum({"different": True})),
        ],
    )
    def test_claim_must_match_all_four_prior_fields(
        self,
        deletion: ManagedConnectorResourceDeletion,
    ) -> None:
        with pytest.raises(StateIdentityError, match="exact prior managed record"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=(deletion,),
            )

    @pytest.mark.parametrize("ownership", ["adopted"])
    def test_only_managed_prior_record_can_be_removed(self, ownership: str) -> None:
        prior = _state()
        prior.resources[_claim().resource_id] = _connector_record(ownership=ownership)

        with pytest.raises(StateIdentityError, match="exact prior managed record"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    def test_legacy_prior_record_cannot_be_removed(self) -> None:
        prior = _state()
        prior.resources[_claim().resource_id] = _connector_record(
            backend="kafka-connect",
        )

        with pytest.raises(StateIdentityError, match="invalid exact identity evidence"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    def test_requires_an_exact_prior_record_value(self) -> None:
        class RecordSubclass(ManagedResourceRecord):
            pass

        prior = _state()
        target = prior.resources[_claim().resource_id]
        prior.resources[_claim().resource_id] = RecordSubclass(
            physical_name=target.physical_name,
            ownership=target.ownership,
            artifact_checksum=target.artifact_checksum,
            backend=target.backend,
        )

        with pytest.raises(StateIdentityError, match="invalid exact identity evidence"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    def test_duplicate_resource_and_provider_claims_are_rejected(self) -> None:
        duplicate = _claim()
        with pytest.raises(StateIdentityError, match="duplicate resource identity"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=(duplicate, duplicate),
            )

        provider_duplicate = _claim(owner_name="other_owner")
        with pytest.raises(StateIdentityError, match="duplicate provider identity"):
            updated_local_state(
                _state(),
                DeploymentPlan(),
                managed_connector_deletions=(duplicate, provider_duplicate),
            )

    def test_duplicate_prior_provider_owner_is_rejected(self) -> None:
        prior = _state()
        duplicate_id = resource_id(
            "payments",
            "prod",
            "connector",
            "duplicate_owner",
        )
        prior.resources[duplicate_id] = _connector_record()

        with pytest.raises(StateIdentityError, match="exact prior managed record"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    @pytest.mark.parametrize(
        "malformed_field",
        [
            "resource_id",
            "backend",
            "physical_name",
            "malformed_backend",
        ],
    )
    def test_malformed_duplicate_prior_provider_cannot_evade_collision(
        self,
        malformed_field: str,
    ) -> None:
        class StringSubclass(str):
            pass

        prior = _state()
        duplicate_id: str = resource_id(
            "payments",
            "prod",
            "connector",
            "duplicate_owner",
        )
        backend: str = _BINDING.backend_identity
        physical_name: str = "archive-orders-sink"
        if malformed_field == "resource_id":
            duplicate_id = StringSubclass(duplicate_id)
        elif malformed_field == "backend":
            backend = StringSubclass(backend)
        elif malformed_field == "physical_name":
            physical_name = StringSubclass(physical_name)
        else:
            backend = "kafka-connect"
        prior.resources[duplicate_id] = _connector_record(
            backend=backend,
            physical_name=physical_name,
        )

        with pytest.raises(StateIdentityError, match="invalid exact identity evidence"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    def test_alias_independent_duplicate_prior_provider_is_rejected(self) -> None:
        prior = _state()
        duplicate_id = resource_id(
            "payments",
            "prod",
            "connector",
            "duplicate_owner",
        )
        prior.resources[duplicate_id] = _connector_record(
            backend=_SAME_ENDPOINT_OTHER_ALIAS_BINDING.backend_identity,
        )

        with pytest.raises(StateIdentityError, match="exact prior managed record"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_connector_deletions=(_claim(),),
            )

    def test_desired_resource_and_provider_claims_are_rejected(self) -> None:
        prior = _state()

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                _desired_connector_plan(
                    owner_name="archive_orders",
                    connector_name="replacement-sink",
                ),
                managed_connector_deletions=(_claim(),),
            )

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                _desired_connector_plan(
                    owner_name="replacement_owner",
                    connector_name="archive-orders-sink",
                    backend=_SAME_ENDPOINT_OTHER_ALIAS_BINDING.backend_identity,
                ),
                managed_connector_deletions=(_claim(),),
            )

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                _desired_connector_plan(
                    owner_name="replacement_owner",
                    connector_name="archive-orders-sink",
                ),
                managed_connector_deletions=(_claim(),),
            )

    def test_blocked_desired_connector_still_conflicts_with_deletion(self) -> None:
        prior = _state()
        plan = _desired_connector_plan(
            owner_name="archive_orders",
            connector_name="archive-orders-sink",
        )
        plan.ownership_requirements.append(
            OwnershipRequirement(
                resource_id=_claim().resource_id,
                kind="connector",
                logical_name="archive_orders",
                physical_name="archive-orders-sink",
                reason="requires_adoption",
                observed_action="update",
                ownership_mode="managed",
                message="blocked desired Connector",
            )
        )

        # The generic desired-state projection filters ownership requirements;
        # deletion collision detection must independently retain this claim.
        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                plan,
                managed_connector_deletions=(_claim(),),
            )

    def test_external_desired_connector_still_conflicts_with_deletion(self) -> None:
        prior = _state()
        external = _desired_connector_plan(
            owner_name="replacement_owner",
            connector_name="archive-orders-sink",
            ownership_mode="external",
        )

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                external,
                managed_connector_deletions=(_claim(),),
            )

    def test_malformed_desired_connector_fails_closed(self) -> None:
        prior = _state()
        malformed = _desired_connector_plan(
            owner_name="unrelated_owner",
            connector_name="unrelated-sink",
        )
        malformed.connector_changes[0].backend_identity = None

        with pytest.raises(StateFormatError, match="desired"):
            updated_local_state(
                prior,
                malformed,
                managed_connector_deletions=(_claim(),),
            )
