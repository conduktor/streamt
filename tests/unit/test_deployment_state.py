"""Tests for the inert persisted ownership-state foundation."""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path
from typing import Protocol
from unittest.mock import patch

import pytest

from streamt.compiler.manifest import (
    ArtifactOwnership,
    ConnectorArtifact,
    GatewayRuleArtifact,
    TopicArtifact,
)
from streamt.deployer.connect import ConnectClusterBinding, ConnectorChange
from streamt.deployer.gateway import GatewayBackendBinding, GatewayRuleChange
from streamt.deployer.kafka import TopicChange
from streamt.deployer.planner import DeploymentPlan
from streamt.deployer.state import (
    CURRENT_STATE_VERSION,
    LocalState,
    ManagedGatewayResourceDeletion,
    ManagedResourceRecord,
    ResourceIdentity,
    StateConflictError,
    StateFormatError,
    StateIdentityError,
    StateVersionError,
    artifact_checksum,
    desired_managed_records,
    local_state_operation_lock,
    local_state_path,
    resource_id,
    updated_local_state,
)


class _ProcessEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


def _hold_operation_lock(
    state_path: str,
    attempting: _ProcessEvent,
    entered: _ProcessEvent,
    release: _ProcessEvent,
) -> None:
    """Process target used to prove flock contention, not thread serialization."""
    attempting.set()
    with local_state_operation_lock(Path(state_path)):
        entered.set()
        if not release.wait(10):
            raise TimeoutError("test did not release local operation lock")


def _record(
    physical_name: str,
    *,
    ownership: str = "managed",
    artifact: dict[str, object] | None = None,
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership=ownership,  # type: ignore[arg-type]
        artifact_checksum=artifact_checksum(artifact or {"name": physical_name}),
        backend="direct-kafka",
    )


def _state(serial: int = 12) -> LocalState:
    topic_id = resource_id("payments", "prod", "topic", "payments_clean")
    schema_id = resource_id("payments", "prod", "schema", "payments_clean_value")
    return LocalState(
        project="payments",
        environment="prod",
        serial=serial,
        resources={
            topic_id: _record("payments.clean.v1"),
            schema_id: _record("payments.clean.v1-value", ownership="adopted"),
        },
    )


_GATEWAY_BACKEND = GatewayBackendBinding.from_endpoint(
    "https://gateway.example.test/admin",
    virtual_cluster="payments-prod",
).backend_identity


def _gateway_record(
    physical_name: str = "orders.public",
    *,
    backend: str = _GATEWAY_BACKEND,
) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name=physical_name,
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "orders_rule"}),
        backend=backend,
    )


def _gateway_deletion(
    owner_name: str = "orders_owner",
    *,
    project: str = "payments",
    environment: str = "prod",
    alias_name: str = "orders.public",
    backend: str = _GATEWAY_BACKEND,
) -> ManagedGatewayResourceDeletion:
    return ManagedGatewayResourceDeletion(
        resource_id=resource_id(
            project,
            environment,
            "gateway_rule",
            owner_name,
        ),
        backend_identity=backend,
        alias_name=alias_name,
    )


class TestStableResourceIdentity:
    def test_uri_matches_spec_and_round_trips(self):
        uri = resource_id("payments", "prod", "topic", "payments_clean")

        assert uri == "streamt://payments/prod/topic/payments_clean"
        assert ResourceIdentity.parse(uri) == ResourceIdentity(
            project="payments",
            environment="prod",
            kind="topic",
            logical_name="payments_clean",
        )

    def test_rejects_ambiguous_identity_segments(self):
        with pytest.raises(StateFormatError, match="must not contain"):
            resource_id("payments", "prod", "topic", "team/payments")

    def test_artifact_checksum_is_deterministic(self):
        first = artifact_checksum({"name": "events", "config": {"b": 2, "a": 1}})
        second = artifact_checksum({"config": {"a": 1, "b": 2}, "name": "events"})

        assert first == second
        assert first.startswith("sha256:")
        assert len(first) == len("sha256:") + 64


class TestStatePersistence:
    def test_atomic_save_and_load_round_trip(self, tmp_path: Path):
        path = local_state_path(tmp_path, environment="prod")
        state = _state()

        state.save(path)
        loaded = LocalState.load(
            path,
            expected_project="payments",
            expected_environment="prod",
        )

        assert loaded == state
        assert json.loads(path.read_text())["state_version"] == CURRENT_STATE_VERSION
        assert not list(path.parent.glob(f".{path.name}.*.tmp"))

    def test_state_paths_are_environment_namespaced_and_filename_safe(self, tmp_path: Path):
        assert local_state_path(tmp_path, environment="dev") == (
            tmp_path / ".streamt" / "state" / "dev.json"
        )
        assert local_state_path(tmp_path, environment="prod") != local_state_path(
            tmp_path,
            environment="dev",
        )
        with pytest.raises(StateFormatError, match="environment"):
            local_state_path(tmp_path, environment="../prod")

    def test_failed_atomic_replace_preserves_prior_state(self, tmp_path: Path):
        path = tmp_path / "state.json"
        original = _state(serial=1)
        replacement = _state(serial=2)
        original.save(path)
        before = path.read_bytes()

        with (
            patch("streamt.deployer.state.Path.replace", side_effect=OSError("swap failed")),
            pytest.raises(OSError, match="swap failed"),
        ):
            replacement.save(path)

        assert path.read_bytes() == before
        assert LocalState.load(path).serial == 1
        assert not list(tmp_path.glob(f".{path.name}.*.tmp"))

    def test_locked_save_rejects_stale_expected_serial_and_preserves_newer_state(
        self,
        tmp_path: Path,
    ):
        path = local_state_path(tmp_path, environment="prod")
        newer = _state(serial=2)
        newer.save(path)
        before = path.read_bytes()
        stale_replacement = _state(serial=2)

        with pytest.raises(StateConflictError, match="changed from 1 to 2"):
            stale_replacement.save_if_serial(path, expected_serial=1)

        assert path.read_bytes() == before

    def test_locked_save_advances_exact_expected_serial(self, tmp_path: Path):
        path = local_state_path(tmp_path, environment="prod")
        _state(serial=1).save(path)
        replacement = _state(serial=2)

        replacement.save_if_serial(path, expected_serial=1)

        assert LocalState.load(path).serial == 2

    def test_operation_lock_serializes_mutators_across_processes(
        self,
        tmp_path: Path,
    ) -> None:
        path = local_state_path(tmp_path, environment="prod")
        context = multiprocessing.get_context("spawn")
        first_attempting = context.Event()
        first_entered = context.Event()
        first_release = context.Event()
        second_attempting = context.Event()
        second_entered = context.Event()
        second_release = context.Event()
        second_release.set()
        first = context.Process(
            target=_hold_operation_lock,
            args=(str(path), first_attempting, first_entered, first_release),
        )
        second = context.Process(
            target=_hold_operation_lock,
            args=(str(path), second_attempting, second_entered, second_release),
        )

        first.start()
        try:
            assert first_attempting.wait(5)
            assert first_entered.wait(5)
            second.start()
            assert second_attempting.wait(5)
            assert not second_entered.wait(0.25)

            first_release.set()
            assert second_entered.wait(5)
            first.join(5)
            second.join(5)
            assert first.exitcode == 0
            assert second.exitcode == 0
        finally:
            first_release.set()
            second_release.set()
            for process in (first, second):
                if process.pid is not None and process.is_alive():
                    process.terminate()
                if process.pid is not None:
                    process.join(5)

    def test_operation_lock_releases_after_exception(self, tmp_path: Path) -> None:
        path = local_state_path(tmp_path, environment="prod")

        with (
            pytest.raises(RuntimeError, match="mutation failed"),
            local_state_operation_lock(path),
        ):
            raise RuntimeError("mutation failed")

        with local_state_operation_lock(path):
            pass

    def test_operation_lock_cas_save_does_not_reacquire_or_deadlock(
        self,
        tmp_path: Path,
    ) -> None:
        path = local_state_path(tmp_path, environment="prod")
        _state(serial=1).save(path)
        replacement = _state(serial=2)

        with local_state_operation_lock(path) as operation_lock:
            operation_lock.save_if_serial(replacement, expected_serial=1)

        assert LocalState.load(path).serial == 2

    def test_invalid_json_is_reported_as_state_error(self, tmp_path: Path):
        path = tmp_path / "state.json"
        path.write_text("{not-json")

        with pytest.raises(StateFormatError, match="not valid JSON"):
            LocalState.load(path)


class TestStateVersionAndIdentity:
    def test_rejects_unsupported_state_version(self):
        data = _state().to_dict()
        data["state_version"] = 2

        with pytest.raises(StateVersionError, match="unsupported state version 2"):
            LocalState.from_dict(data)

    def test_load_rejects_wrong_project_or_environment(self, tmp_path: Path):
        path = tmp_path / "state.json"
        _state().save(path)

        with pytest.raises(StateIdentityError, match="expected 'other'"):
            LocalState.load(path, expected_project="other")
        with pytest.raises(StateIdentityError, match="expected 'dev'"):
            LocalState.load(path, expected_environment="dev")

    def test_resource_identity_must_match_state_identity(self):
        wrong_id = resource_id("other", "prod", "topic", "payments_clean")

        with pytest.raises(StateIdentityError, match="does not belong"):
            LocalState(
                project="payments",
                environment="prod",
                resources={wrong_id: _record("payments.clean.v1")},
            )

    def test_external_resources_cannot_enter_owned_state(self):
        with pytest.raises(StateFormatError, match=r"managed.*adopted"):
            _record("payments.raw.v1", ownership="external")


class TestRemovalCandidates:
    def test_reports_only_previous_owned_resources_absent_from_full_desired_state(self):
        state = _state()
        topic_id = resource_id("payments", "prod", "topic", "payments_clean")
        schema_id = resource_id("payments", "prod", "schema", "payments_clean_value")

        candidates = state.removal_candidates({topic_id})

        assert [candidate.resource_id for candidate in candidates] == [schema_id]
        assert candidates[0].record.ownership == "adopted"

    def test_partial_comparison_is_bounded_to_explicit_scope(self):
        state = _state()
        topic_id = resource_id("payments", "prod", "topic", "payments_clean")
        schema_id = resource_id("payments", "prod", "schema", "payments_clean_value")

        candidates = state.removal_candidates(
            desired_resource_ids=set(),
            comparison_scope={topic_id},
        )

        assert [candidate.resource_id for candidate in candidates] == [topic_id]
        assert schema_id not in {candidate.resource_id for candidate in candidates}

    def test_candidates_are_inert_data_not_deployment_actions(self):
        candidate = _state().removal_candidates(set())[0]

        assert not hasattr(candidate, "action")

    def test_desired_identity_from_other_environment_is_rejected(self):
        other = resource_id("payments", "dev", "topic", "payments_clean")

        with pytest.raises(StateIdentityError, match="does not belong"):
            _state().removal_candidates({other})


class TestStateUpdates:
    @staticmethod
    def _plan(*, mode: str = "managed", partitions: int = 3) -> DeploymentPlan:
        ownership = ArtifactOwnership(
            project="payments",
            owner_type="model",
            owner_name="payments_clean",
            mode=mode,
        )
        artifact = TopicArtifact(
            name="payments.clean.v1",
            partitions=partitions,
            replication_factor=1,
            ownership=ownership,
        )
        return DeploymentPlan(
            topic_changes=[
                TopicChange(
                    topic=artifact.name,
                    action="create",
                    desired=artifact,
                )
            ]
        )

    def test_update_advances_serial_and_retains_absent_prior_records(self):
        prior = _state(serial=4)
        prior_schema_ids = {
            identity for identity in prior.resources if "/schema/" in identity
        }

        updated = updated_local_state(prior, self._plan(partitions=6))

        assert updated is not None
        assert updated.serial == 5
        assert prior_schema_ids <= set(updated.resources)

    def test_unchanged_records_do_not_advance_serial(self):
        plan = self._plan()
        records = desired_managed_records(
            plan,
            project="payments",
            environment="prod",
        )
        prior = LocalState(
            project="payments",
            environment="prod",
            serial=2,
            resources=records,
        )

        assert updated_local_state(prior, plan) is None

    def test_external_and_unowned_artifacts_are_not_recorded(self):
        external = self._plan(mode="external")
        unowned_artifact = TopicArtifact(
            name="legacy.v1",
            partitions=1,
            replication_factor=1,
        )
        external.topic_changes.append(
            TopicChange(
                topic=unowned_artifact.name,
                action="create",
                desired=unowned_artifact,
            )
        )

        assert desired_managed_records(
            external,
            project="payments",
            environment="prod",
        ) == {}


class TestManagedGatewayStateDeletion:
    @staticmethod
    def _state_with_gateway(*, serial: int = 7) -> LocalState:
        gateway_id = resource_id(
            "payments",
            "prod",
            "gateway_rule",
            "orders_owner",
        )
        topic_id = resource_id("payments", "prod", "topic", "payments_clean")
        return LocalState(
            project="payments",
            environment="prod",
            serial=serial,
            resources={
                gateway_id: _gateway_record(),
                topic_id: _record("payments.clean.v1"),
            },
        )

    @staticmethod
    def _desired_gateway_plan(
        *,
        owner_name: str = "orders_owner",
        alias_name: str = "orders.public",
    ) -> DeploymentPlan:
        artifact = GatewayRuleArtifact(
            name="orders_rule",
            virtual_topic=alias_name,
            physical_topic="orders.v1",
            ownership=ArtifactOwnership(
                project="payments",
                owner_type="model",
                owner_name=owner_name,
                mode="managed",
            ),
        )
        change = GatewayRuleChange(
            name=artifact.name,
            action="create",
            desired=artifact,
        )
        change.backend_identity = _GATEWAY_BACKEND
        return DeploymentPlan(gateway_changes=[change])

    def test_exact_explicit_deletion_removes_only_matching_gateway_record(self) -> None:
        prior = self._state_with_gateway()
        gateway_id = _gateway_deletion().resource_id
        topic_id = resource_id("payments", "prod", "topic", "payments_clean")

        updated = updated_local_state(
            prior,
            DeploymentPlan(),
            managed_gateway_deletions=(_gateway_deletion(),),
        )

        assert updated is not None
        assert updated.serial == prior.serial + 1
        assert gateway_id not in updated.resources
        assert updated.resources[topic_id] == prior.resources[topic_id]
        assert gateway_id in prior.resources

    def test_desired_update_and_explicit_deletion_advance_serial_once(self) -> None:
        prior = self._state_with_gateway(serial=3)

        updated = updated_local_state(
            prior,
            TestStateUpdates._plan(partitions=9),
            managed_gateway_deletions=(_gateway_deletion(),),
        )

        assert updated is not None
        assert updated.serial == 4
        topic_id = resource_id("payments", "prod", "topic", "payments_clean")
        assert updated.resources[topic_id].artifact_checksum != (
            prior.resources[topic_id].artifact_checksum
        )

    def test_manifest_absence_and_legacy_delete_retain_gateway_state(self) -> None:
        prior = self._state_with_gateway()
        legacy_delete = DeploymentPlan(
            gateway_changes=[GatewayRuleChange(name="orders_rule", action="delete")]
        )

        assert updated_local_state(prior, DeploymentPlan()) is None
        assert updated_local_state(prior, legacy_delete) is None

    @pytest.mark.parametrize(
        "deletions",
        [
            [],
            (_gateway_deletion(), object()),
        ],
    )
    def test_deletion_input_requires_an_exact_tuple_of_exact_values(
        self,
        deletions: object,
    ) -> None:
        with pytest.raises(StateFormatError, match="exact"):
            updated_local_state(
                self._state_with_gateway(),
                DeploymentPlan(),
                managed_gateway_deletions=deletions,  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            (
                {
                    "resource_id": resource_id(
                        "payments",
                        "prod",
                        "topic",
                        "orders_owner",
                    )
                },
                "gateway_rule",
            ),
            ({"backend_identity": "conduktor-gateway"}, "canonical"),
            ({"alias_name": "orders/public"}, "resource name"),
        ],
    )
    def test_deletion_value_requires_canonical_gateway_identity(
        self,
        kwargs: dict[str, str],
        message: str,
    ) -> None:
        values = {
            "resource_id": _gateway_deletion().resource_id,
            "backend_identity": _GATEWAY_BACKEND,
            "alias_name": "orders.public",
            **kwargs,
        }

        with pytest.raises(StateFormatError, match=message):
            ManagedGatewayResourceDeletion(**values)

    @pytest.mark.parametrize(
        "deletion",
        [
            _gateway_deletion(project="other"),
            _gateway_deletion(environment="dev"),
        ],
    )
    def test_deletion_must_belong_to_prior_state(
        self,
        deletion: ManagedGatewayResourceDeletion,
    ) -> None:
        with pytest.raises(StateIdentityError, match="current state"):
            updated_local_state(
                self._state_with_gateway(),
                DeploymentPlan(),
                managed_gateway_deletions=(deletion,),
            )

    @pytest.mark.parametrize(
        "deletion",
        [
            _gateway_deletion(owner_name="missing_owner"),
            _gateway_deletion(alias_name="other.public"),
            _gateway_deletion(
                backend=GatewayBackendBinding.from_endpoint(
                    "https://other-gateway.example.test/admin",
                    virtual_cluster="payments-prod",
                ).backend_identity
            ),
        ],
    )
    def test_deletion_must_match_exact_prior_record(
        self,
        deletion: ManagedGatewayResourceDeletion,
    ) -> None:
        with pytest.raises(StateIdentityError, match="exact prior-state record"):
            updated_local_state(
                self._state_with_gateway(),
                DeploymentPlan(),
                managed_gateway_deletions=(deletion,),
            )

    def test_duplicate_resource_or_provider_deletion_is_rejected(self) -> None:
        prior = self._state_with_gateway()
        duplicate = _gateway_deletion()

        with pytest.raises(StateIdentityError, match="duplicate resource identity"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_gateway_deletions=(duplicate, duplicate),
            )

        provider_duplicate = _gateway_deletion(owner_name="other_owner")
        with pytest.raises(StateIdentityError, match="duplicate provider identity"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_gateway_deletions=(duplicate, provider_duplicate),
            )

        second_id = resource_id(
            "payments",
            "prod",
            "gateway_rule",
            "other_owner",
        )
        prior.resources[second_id] = _gateway_record()
        with pytest.raises(StateIdentityError, match="exact prior-state record"):
            updated_local_state(
                prior,
                DeploymentPlan(),
                managed_gateway_deletions=(duplicate,),
            )

    def test_deletion_rejects_desired_resource_or_provider_claim(self) -> None:
        prior = self._state_with_gateway()

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                self._desired_gateway_plan(),
                managed_gateway_deletions=(_gateway_deletion(),),
            )

        with pytest.raises(StateIdentityError, match="desired resource claim"):
            updated_local_state(
                prior,
                self._desired_gateway_plan(owner_name="replacement_owner"),
                managed_gateway_deletions=(_gateway_deletion(),),
            )


class TestConnectorStateBinding:
    @staticmethod
    def _artifact() -> ConnectorArtifact:
        return ConnectorArtifact(
            name="payments-sink",
            connector_class="com.example.PaymentsSink",
            topics=["payments.events.v1"],
            cluster="production",
            ownership=ArtifactOwnership(
                project="payments",
                owner_type="model",
                owner_name="payments",
                mode="managed",
            ),
        )

    def test_desired_record_persists_exact_backend_and_resolved_checksum(self):
        artifact = self._artifact()
        binding = ConnectClusterBinding.from_endpoint(
            "production",
            "https://connect.example.test/api",
        )
        plan = DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=artifact.name,
                    action="create",
                    desired=artifact,
                    backend_identity=binding.backend_identity,
                )
            ]
        )

        records = desired_managed_records(
            plan,
            project="payments",
            environment="prod",
        )

        record = records[resource_id("payments", "prod", "connector", "payments")]
        assert record.backend == binding.backend_identity
        assert record.artifact_checksum == artifact_checksum(artifact.to_dict())

    def test_desired_connector_without_canonical_backend_fails_closed(self):
        artifact = self._artifact()
        plan = DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name=artifact.name,
                    action="create",
                    desired=artifact,
                )
            ]
        )

        with pytest.raises(StateFormatError, match="canonical Connect backend identity"):
            desired_managed_records(
                plan,
                project="payments",
                environment="prod",
            )

    def test_delete_without_desired_artifact_needs_no_backend_identity(self):
        plan = DeploymentPlan(
            connector_changes=[
                ConnectorChange(
                    connector_name="obsolete-sink",
                    action="delete",
                )
            ]
        )

        assert desired_managed_records(
            plan,
            project="payments",
            environment="prod",
        ) == {}
