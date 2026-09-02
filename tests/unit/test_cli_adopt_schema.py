"""Adversarial CLI tests for fail-closed Schema Registry adoption."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import ArtifactOwnership, Manifest, SchemaArtifact
from streamt.deployer.schema_registry import SchemaState
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)


def _write_project(path: Path) -> None:
    (path / "stream_project.yml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "streamt.dev/v1alpha1",
                "project": {"name": "adoption-test"},
                "runtime": {
                    "kafka": {"bootstrap_servers": "localhost:9092"},
                    "schema_registry": {"url": "https://registry.example.test"},
                },
            }
        )
    )


def _schema(
    *,
    logical_name: str = "orders",
    subject: str = "orders.v1-value",
    mode: str = "adopted",
    schema: dict[str, object] | None = None,
) -> SchemaArtifact:
    return SchemaArtifact(
        subject=subject,
        schema=schema
        or {
            "type": "record",
            "name": "Order",
            "doc": "desired schema contains desired-schema-secret",
            "fields": [{"name": "id", "type": "string"}],
        },
        schema_type="AVRO",
        compatibility="BACKWARD",
        ownership=ArtifactOwnership(
            project="adoption-test",
            owner_type="source",
            owner_name=logical_name,
            mode=mode,
        ),
    )


def _manifest(*artifacts: SchemaArtifact | dict[str, object]) -> Manifest:
    return Manifest(
        version="1.0",
        project_name="adoption-test",
        artifacts={
            "schemas": [
                artifact.to_dict() if isinstance(artifact, SchemaArtifact) else artifact
                for artifact in artifacts
            ]
        },
    )


def _registry(*, exists: bool = True) -> MagicMock:
    deployer = MagicMock()
    deployer.get_schema_state.return_value = SchemaState(
        subject="orders.v1-value",
        exists=exists,
        version=4 if exists else None,
        schema_id=81 if exists else None,
        schema={
            "type": "record",
            "name": "Order",
            "doc": "live schema contains live-schema-secret",
            "fields": [{"name": "id", "type": "long"}],
        }
        if exists
        else None,
        schema_type="AVRO" if exists else None,
        compatibility="FULL" if exists else None,
    )
    return deployer


def _invoke(
    path: Path,
    *,
    extra: list[str] | None = None,
    output: str = "json",
    input_text: str | None = None,
):
    resource_uri = resource_id("adoption-test", "default", "schema", "orders")
    args = [
        "-o",
        output,
        "adopt",
        "-p",
        str(path),
        "-e",
        "default",
        "--kind",
        "schema",
        "--name",
        "orders",
    ]
    if extra is None:
        args.extend(
            [
                "--confirm-resource",
                resource_uri,
                "--confirm-env",
                "default",
            ]
        )
    else:
        args.extend(extra)
    return CliRunner().invoke(main, args, input=input_text)


def _payload(result) -> dict[str, object]:
    return json.loads(result.stdout)


def _patch_adoption(manifest: Manifest, registry: MagicMock):
    return (
        patch("streamt.compiler.Compiler.compile", return_value=manifest),
        patch(
            "streamt.cli.commands.adopt.make_sr_deployer",
            return_value=registry,
        ),
    )


def test_success_observes_exact_subject_without_schema_or_mutation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    artifact = _schema()
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(artifact), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    payload = _payload(result)
    data = payload["data"]
    assert data["adopted"] is True
    assert data["resource_id"] == resource_id("adoption-test", "default", "schema", "orders")
    assert data["physical_name"] == "orders.v1-value"
    assert data["observed"] == {
        "subject": "orders.v1-value",
        "schema_type": "AVRO",
        "version": 4,
        "schema_id": 81,
        "compatibility": "FULL",
        "schema_checksum": data["observed"]["schema_checksum"],
    }
    assert data["desired_managed_attributes"]["subject"] == "orders.v1-value"
    assert data["desired_managed_attributes"]["schema_type"] == "AVRO"
    assert data["desired_managed_attributes"]["compatibility"] == "BACKWARD"
    assert data["pending_diffs"]["compatibility"] == {
        "from": "FULL",
        "to": "BACKWARD",
    }
    serialized = json.dumps(payload)
    assert "desired-schema-secret" not in serialized
    assert "live-schema-secret" not in serialized

    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    record = state.resources[data["resource_id"]]
    assert state.serial == 1
    assert record == ManagedResourceRecord(
        physical_name="orders.v1-value",
        ownership="adopted",
        artifact_checksum=artifact_checksum(artifact.to_dict()),
        backend="schema-registry",
    )
    registry.get_schema_state.assert_called_once_with("orders.v1-value")
    registry.list_subjects.assert_not_called()
    for method in (
        "apply_schema",
        "register_schema",
        "set_compatibility",
        "check_compatibility",
        "delete_subject",
        "apply",
    ):
        getattr(registry, method).assert_not_called()
    registry.close.assert_called_once_with()


def test_unspecified_compatibility_is_not_reported_as_a_pending_clear(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    base = _schema()
    artifact = SchemaArtifact(
        subject=base.subject,
        schema=base.schema,
        schema_type=base.schema_type,
        compatibility=None,
        ownership=base.ownership,
    )
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(artifact), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    assert "compatibility" not in _payload(result)["data"]["pending_diffs"]


def test_rejects_zero_or_ambiguous_logical_subject_mappings(tmp_path: Path) -> None:
    _write_project(tmp_path)
    registry = _registry()
    no_owner = _schema().to_dict()
    no_owner.pop("ownership")

    for manifest in (
        _manifest(no_owner),
        _manifest(_schema(), _schema(subject="orders.v2-value")),
    ):
        compiler_patch, registry_patch = _patch_adoption(manifest, registry)
        with compiler_patch, registry_patch:
            result = _invoke(tmp_path)

        assert result.exit_code == 1
        assert _payload(result)["errors"][0]["code"] == ("E412_ADOPTION_TARGET_INVALID")

    registry.get_schema_state.assert_not_called()
    assert not local_state_path(tmp_path, environment="default").exists()


@pytest.mark.parametrize("mode", ["external", "managed"])
def test_rejects_schema_without_explicit_adopted_mode(
    tmp_path: Path,
    mode: str,
) -> None:
    _write_project(tmp_path)
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(
        _manifest(_schema(mode=mode)),
        registry,
    )

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E412_ADOPTION_TARGET_INVALID"
    registry.get_schema_state.assert_not_called()
    registry.close.assert_not_called()


def test_missing_live_subject_never_writes_or_mutates(tmp_path: Path) -> None:
    _write_project(tmp_path)
    registry = _registry(exists=False)
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E413_ADOPTION_LIVE_NOT_FOUND"
    assert not local_state_path(tmp_path, environment="default").exists()
    registry.register_schema.assert_not_called()
    registry.close.assert_called_once_with()


def test_auth_error_is_redacted_and_closes_registry(tmp_path: Path) -> None:
    _write_project(tmp_path)
    registry = _registry()
    registry.get_schema_state.side_effect = RuntimeError(
        "401 unauthorized password=live-auth-secret at "
        "https://alice:url-auth-secret@registry.example.test"
    )
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    payload = _payload(result)
    assert payload["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "live-auth-secret" not in result.output
    assert "url-auth-secret" not in result.output
    assert not local_state_path(tmp_path, environment="default").exists()
    registry.close.assert_called_once_with()


def test_malformed_live_state_fails_closed_without_schema_output(tmp_path: Path) -> None:
    _write_project(tmp_path)
    registry = _registry()
    registry.get_schema_state.return_value = SchemaState(
        subject="orders.v1-value",
        exists=True,
        version=4,
        schema_id=81,
        schema={"secret": "malformed-live-secret"},
        schema_type="AVRO",
        compatibility=None,
    )
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E416_ADOPTION_FAILED"
    assert "malformed-live-secret" not in result.output
    assert not local_state_path(tmp_path, environment="default").exists()
    registry.close.assert_called_once_with()


def test_confirmation_mismatch_closes_registry_without_writing(tmp_path: Path) -> None:
    _write_project(tmp_path)
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(
            tmp_path,
            extra=[
                "--confirm-resource",
                "streamt://adoption-test/default/schema/wrong",
                "--confirm-env",
                "default",
            ],
        )

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == ("E414_ADOPTION_CONFIRMATION_REQUIRED")
    assert not local_state_path(tmp_path, environment="default").exists()
    registry.close.assert_called_once_with()


def test_interactive_schema_confirmation_requires_exact_resource_and_environment(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    registry = _registry()
    resource_uri = resource_id("adoption-test", "default", "schema", "orders")
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with (
        compiler_patch,
        registry_patch,
        patch("streamt.cli.commands.adopt._stdin_is_interactive", return_value=True),
    ):
        result = _invoke(
            tmp_path,
            extra=[],
            output="text",
            input_text=f"adopt {resource_uri} in default\n",
        )

    assert result.exit_code == 0, result.output
    assert "Schema Registry was not modified" in result.output
    registry.register_schema.assert_not_called()
    registry.close.assert_called_once_with()


def test_different_schema_record_conflicts_before_observation(tmp_path: Path) -> None:
    _write_project(tmp_path)
    resource_uri = resource_id("adoption-test", "default", "schema", "orders")
    LocalState(
        project="adoption-test",
        environment="default",
        serial=3,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name="orders.old-value",
                ownership="adopted",
                artifact_checksum=artifact_checksum({"subject": "orders.old-value"}),
                backend="schema-registry",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 3
    registry.get_schema_state.assert_not_called()


@pytest.mark.parametrize("prior_ownership", ["managed", "adopted"])
def test_identical_schema_claim_is_idempotent_without_confirmation(
    tmp_path: Path,
    prior_ownership: str,
) -> None:
    _write_project(tmp_path)
    artifact = _schema()
    resource_uri = resource_id("adoption-test", "default", "schema", "orders")
    LocalState(
        project="adoption-test",
        environment="default",
        serial=8,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name=artifact.subject,
                ownership=prior_ownership,  # type: ignore[arg-type]
                artifact_checksum=artifact_checksum(artifact.to_dict()),
                backend="schema-registry",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(artifact), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path, extra=[])

    assert result.exit_code == 0, result.output
    assert _payload(result)["data"]["already_owned"] is True
    assert LocalState.load(local_state_path(tmp_path, environment="default")).serial == 8
    registry.get_schema_state.assert_called_once_with(artifact.subject)
    registry.close.assert_called_once_with()


def test_schema_adoption_preserves_unrelated_state_records(tmp_path: Path) -> None:
    _write_project(tmp_path)
    other_uri = resource_id("adoption-test", "default", "topic", "payments")
    other_record = ManagedResourceRecord(
        physical_name="payments.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum({"name": "payments.v1"}),
        backend="direct-kafka",
    )
    LocalState(
        project="adoption-test",
        environment="default",
        serial=2,
        resources={other_uri: other_record},
    ).save(local_state_path(tmp_path, environment="default"))
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    with compiler_patch, registry_patch:
        result = _invoke(tmp_path)

    assert result.exit_code == 0, result.output
    state = LocalState.load(local_state_path(tmp_path, environment="default"))
    assert state.serial == 3
    assert state.resources[other_uri] == other_record
    assert set(state.resources) == {
        other_uri,
        resource_id("adoption-test", "default", "schema", "orders"),
    }


def test_schema_adoption_cas_conflict_preserves_concurrent_state(tmp_path: Path) -> None:
    _write_project(tmp_path)
    state_path = local_state_path(tmp_path, environment="default")
    other_uri = resource_id("adoption-test", "default", "topic", "payments")
    newer = LocalState(
        project="adoption-test",
        environment="default",
        serial=1,
        resources={
            other_uri: ManagedResourceRecord(
                physical_name="payments.v1",
                ownership="managed",
                artifact_checksum=artifact_checksum({"name": "payments.v1"}),
                backend="direct-kafka",
            )
        },
    )
    registry = _registry()
    compiler_patch, registry_patch = _patch_adoption(_manifest(_schema()), registry)

    def write_concurrent_state(**_kwargs: object) -> None:
        newer.save(state_path)

    with (
        compiler_patch,
        registry_patch,
        patch(
            "streamt.cli.commands.adopt._require_confirmation",
            side_effect=write_concurrent_state,
        ),
    ):
        result = _invoke(tmp_path)

    assert result.exit_code == 1
    assert _payload(result)["errors"][0]["code"] == "E415_ADOPTION_STATE_CONFLICT"
    assert LocalState.load(state_path) == newer
    registry.close.assert_called_once_with()
