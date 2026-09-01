"""Tests for the inert persisted ownership-state foundation."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from streamt.deployer.state import (
    CURRENT_STATE_VERSION,
    LocalState,
    ManagedResourceRecord,
    ResourceIdentity,
    StateFormatError,
    StateIdentityError,
    StateVersionError,
    artifact_checksum,
    resource_id,
)


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
        path = tmp_path / ".streamt" / "state.json"
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
