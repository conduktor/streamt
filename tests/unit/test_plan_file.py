"""Tests for deterministic reviewed plan files and their CLI workflow."""

from __future__ import annotations

import hashlib
import json
import shlex
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from streamt.cli import main
from streamt.compiler.manifest import Manifest, TopicArtifact
from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.connect import (
    ConnectorChange,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.gateway import managed_gateway_absence_fingerprint
from streamt.deployer.kafka import TopicChange
from streamt.deployer.plan_file import (
    PLAN_FILE_VERSION,
    PlanFileError,
    ReviewedPlanFile,
    StalePlanError,
    StateReference,
    canonical_json,
    deployment_plan_payload,
)
from streamt.deployer.planner import (
    DeploymentPlan,
    GatewayRemovalAssessment,
    OwnershipRequirement,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    local_state_path,
    resource_id,
)
from streamt.deployer.state_backend import (
    CURRENT_CONTROL_VERSION,
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    make_deployment_state_service,
)

_TEST_STORE_ID = "00000000-0000-4000-8000-000000000001"
_GATEWAY_BACKEND = "conduktor-gateway:v1:p:sha256:" + "a" * 64
_CONNECTOR_BACKEND = "kafka-connect:v1:primary:sha256:" + "d" * 64
_CONNECTOR_NAME = "archive-orders-sink"


def _manifest(*, compiled_at: str = "2026-01-01T00:00:00Z") -> Manifest:
    return Manifest(
        version="1.0",
        project_name="payments",
        compiled_at=compiled_at,
        artifacts={
            "topics": [
                TopicArtifact(
                    name="payments.clean.v1", partitions=3, replication_factor=1
                ).to_dict()
            ]
        },
    )


def _deployment_plan() -> DeploymentPlan:
    topic = TopicArtifact(name="payments.clean.v1", partitions=3, replication_factor=1)
    return DeploymentPlan(
        topic_changes=[
            TopicChange(topic=topic.name, action="create", desired=topic),
            TopicChange(topic="unchanged.v1", action="none"),
        ]
    )


def _reviewed_actions() -> tuple[OperationAction, ...]:
    return (
        OperationAction(
            index=0,
            resource_id="streamt://payments/prod/topic/payments_clean",
            action="create",
        ),
    )


def _gateway_delete_actions() -> tuple[OperationAction, ...]:
    rule_name = "orders_rule"
    alias_name = "orders.public"
    return (
        OperationAction(
            index=0,
            resource_id="streamt://payments/prod/gateway_rule/orders_view",
            action="delete",
            gateway_evidence=GatewayActionEvidence(
                version=1,
                backend_identity=_GATEWAY_BACKEND,
                rule_name=rule_name,
                alias_name=alias_name,
                current=GatewayActionSurfaceEvidence(
                    exists=True,
                    fingerprint="sha256:" + "b" * 64,
                    managed_interceptor_count=2,
                ),
                desired=GatewayActionSurfaceEvidence(
                    exists=False,
                    fingerprint=managed_gateway_absence_fingerprint(
                        _GATEWAY_BACKEND,
                        rule_name,
                        alias_name,
                    ),
                    managed_interceptor_count=0,
                ),
            ),
        ),
    )


def _connector_delete_actions() -> tuple[OperationAction, ...]:
    return (
        OperationAction(
            index=0,
            resource_id="streamt://payments/prod/connector/archive_orders",
            action="delete",
            connector_evidence=ConnectorActionEvidence(
                version=1,
                backend_identity=_CONNECTOR_BACKEND,
                connector_name=_CONNECTOR_NAME,
                prior_artifact_checksum="sha256:" + "e" * 64,
                current=ConnectorActionSurfaceEvidence(
                    exists=True,
                    fingerprint="sha256:" + "f" * 64,
                ),
                desired=ConnectorActionSurfaceEvidence(
                    exists=False,
                    fingerprint=managed_connector_absence_fingerprint(
                        _CONNECTOR_BACKEND,
                        _CONNECTOR_NAME,
                    ),
                ),
            ),
        ),
    )


def _state_observation(
    *,
    project: str = "payments",
    environment: str = "prod",
    serial: int = 0,
    store_id: str = _TEST_STORE_ID,
    backend: str = "local",
    namespace: str = "local",
    resources: dict[str, ManagedResourceRecord] | None = None,
    revision: str = "provider-revision-not-portable",
) -> StateObservation:
    return StateObservation(
        store=StateStoreIdentity(backend=backend, store_id=store_id),
        address=StateAddress(
            namespace=namespace,
            project=project,
            environment=environment,
        ),
        state=LocalState(
            project=project,
            environment=environment,
            serial=serial,
            resources=resources or {},
        ),
        revision=StateRevision(revision),
    )


def _state_reference(
    *,
    project: str = "payments",
    environment: str = "prod",
    serial: int = 0,
    store_id: str = _TEST_STORE_ID,
    backend: str = "local",
    namespace: str = "local",
    resources: dict[str, ManagedResourceRecord] | None = None,
    revision: str = "provider-revision-not-portable",
) -> StateReference:
    return StateReference.from_observation(
        _state_observation(
            project=project,
            environment=environment,
            serial=serial,
            store_id=store_id,
            backend=backend,
            namespace=namespace,
            resources=resources,
            revision=revision,
        )
    )


def _reviewed_plan() -> ReviewedPlanFile:
    return ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
        state=_state_reference(),
        actions=_reviewed_actions(),
    )


def _ownership_requirement(reason: str = "requires_adoption") -> OwnershipRequirement:
    return OwnershipRequirement(
        resource_id="streamt://payments/prod/topic/payments_clean",
        kind="topic",
        logical_name="payments_clean",
        physical_name="payments.clean.v1",
        reason=reason,
        observed_action="update",
        ownership_mode="external" if reason == "external" else "managed",
        message="An explicit ownership decision is required.",
    )


def _write_project(path: Path) -> None:
    config = {
        "apiVersion": "streamt.dev/v1alpha1",
        "project": {"name": "plan-test"},
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
    }
    (path / "stream_project.yml").write_text(yaml.safe_dump(config))


def _write_environment_project(
    path: Path,
    *,
    protected: bool,
    require_reviewed_plan: bool = False,
) -> None:
    _write_project(path)
    environments = path / "environments"
    environments.mkdir()
    environment = {
        "environment": {
            "name": "prod",
            "description": "Production",
            "protected": protected,
        },
        "runtime": {"kafka": {"bootstrap_servers": "localhost:9092"}},
        "safety": {
            "confirm_apply": protected,
            "allow_destructive": False,
            "require_reviewed_plan": require_reviewed_plan,
        },
    }
    (environments / "prod.yml").write_text(yaml.safe_dump(environment))


def _json_output(result: object) -> dict[str, object]:
    return json.loads(result.stdout)


def _resign_plan_data(data: dict[str, object]) -> None:
    unsigned = {key: value for key, value in data.items() if key != "checksum"}
    digest = hashlib.sha256(canonical_json(unsigned).encode("utf-8")).hexdigest()
    data["checksum"] = f"sha256:{digest}"


def _managed_topic_record(*, partitions: int) -> dict[str, ManagedResourceRecord]:
    identity = resource_id("payments", "prod", "topic", "payments_clean")
    return {
        identity: ManagedResourceRecord(
            physical_name="payments.clean.v1",
            ownership="managed",
            artifact_checksum=artifact_checksum(
                {"name": "payments.clean.v1", "partitions": partitions}
            ),
            backend="direct-kafka",
        )
    }


def test_plan_file_is_deterministic_and_excludes_compile_time(tmp_path: Path) -> None:
    first = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(compiled_at="2026-01-01T00:00:00Z"),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
        state=_state_reference(),
        actions=_reviewed_actions(),
    )
    second = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(compiled_at="2026-09-01T12:34:56Z"),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
        state=_state_reference(),
        actions=_reviewed_actions(),
    )
    first_path = tmp_path / "first.plan.json"
    second_path = tmp_path / "second.plan.json"
    first.save(first_path)
    second.save(second_path)

    assert first.checksum == second.checksum
    assert first.manifest_checksum == second.manifest_checksum
    assert first_path.read_bytes() == second_path.read_bytes()
    assert not list(tmp_path.glob(".*.tmp"))


def test_v5_online_state_reference_is_exact_and_excludes_provider_revision() -> None:
    observation = _state_observation(
        serial=7,
        resources=_managed_topic_record(partitions=3),
        revision="postgres://alice:provider-password@db/revision-token",
    )
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=StateReference.from_observation(observation),
        actions=_reviewed_actions(),
    )
    payload = reviewed.to_dict()

    assert PLAN_FILE_VERSION == 5
    assert payload["actions"] == [
        {
            "index": 0,
            "resource_id": "streamt://payments/prod/topic/payments_clean",
            "action": "create",
            "gateway_evidence": None,
            "connector_evidence": None,
        }
    ]
    assert payload["state"] == {
        "backend": "local",
        "store_id": _TEST_STORE_ID,
        "address": "streamt-state://local/payments/prod",
        "serial": 7,
        "checksum": StateReference.from_observation(observation).checksum,
    }
    assert "state_serial" not in payload
    serialized = json.dumps(payload)
    assert "provider-password" not in serialized
    assert "revision-token" not in serialized


def test_v5_actions_round_trip_with_exact_gateway_evidence(tmp_path: Path) -> None:
    actions = _gateway_delete_actions()
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=_state_reference(),
        actions=actions,
    )
    path = tmp_path / "gateway-actions.plan.json"

    reviewed.save(path)
    loaded = ReviewedPlanFile.load(path)

    assert type(loaded.actions) is tuple
    assert loaded.actions == actions
    assert loaded.to_dict()["actions"] == [
        actions[0].to_dict(control_version=CURRENT_CONTROL_VERSION)
    ]


def test_v5_connector_evidence_round_trips_and_binds_plan_checksum(
    tmp_path: Path,
) -> None:
    actions = _connector_delete_actions()
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={"connect": {"default": "primary"}},
        state=_state_reference(),
        actions=actions,
    )
    path = tmp_path / "connector-actions.plan.json"
    reviewed.save(path)

    loaded = ReviewedPlanFile.load(path)
    assert loaded == reviewed
    assert loaded.actions == actions
    assert set(loaded.to_dict()["actions"][0]) == {
        "index",
        "resource_id",
        "action",
        "gateway_evidence",
        "connector_evidence",
    }
    serialized = path.read_text(encoding="utf-8")
    for forbidden in (
        "connector-secret",
        "provider-response",
        "connect.example.test",
        "postgresql://",
        "dsn_env",
        "config",
    ):
        assert forbidden not in serialized

    data = loaded.to_dict()
    evidence = data["actions"][0]["connector_evidence"]
    evidence["current"]["fingerprint"] = "sha256:" + "0" * 64
    tampered = tmp_path / "connector-actions-tampered.plan.json"
    tampered.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(tampered)


def test_create_requires_explicit_actions_argument() -> None:
    with pytest.raises(TypeError, match="actions"):
        ReviewedPlanFile.create(  # type: ignore[call-arg]
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
        )


def test_actions_require_contiguous_indexes_and_unique_resource_ids() -> None:
    with pytest.raises(PlanFileError, match="exact immutable action tuple"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=list(_reviewed_actions()),  # type: ignore[arg-type]
        )

    with pytest.raises(PlanFileError, match="contiguous from zero"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=(
                OperationAction(
                    index=1,
                    resource_id="streamt://payments/prod/topic/payments_clean",
                    action="create",
                ),
            ),
        )

    with pytest.raises(PlanFileError, match="duplicate resource identity"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=(
                OperationAction(
                    index=0,
                    resource_id="streamt://payments/prod/topic/payments_clean",
                    action="create",
                ),
                OperationAction(
                    index=1,
                    resource_id="streamt://payments/prod/topic/payments_clean",
                    action="update",
                ),
            ),
        )


@pytest.mark.parametrize(
    ("resource", "message"),
    [
        ("topic:payments_clean", "invalid canonical resource identity"),
        (
            "streamt://payments/staging/topic/payments_clean",
            "another project environment",
        ),
        (
            "streamt://other/prod/topic/payments_clean",
            "another project environment",
        ),
    ],
)
def test_actions_require_current_canonical_project_environment(
    resource: str,
    message: str,
) -> None:
    with pytest.raises(PlanFileError, match=message):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=(OperationAction(index=0, resource_id=resource, action="create"),),
        )


def test_gateway_mutation_actions_require_exact_evidence() -> None:
    with pytest.raises(PlanFileError, match="require exact action evidence"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=(
                OperationAction(
                    index=0,
                    resource_id=("streamt://payments/prod/gateway_rule/orders_view"),
                    action="delete",
                ),
            ),
        )


def test_loaded_gateway_mutation_without_evidence_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "missing-gateway-evidence.plan.json"
    data = _reviewed_plan().to_dict()
    actions = data["actions"]
    assert isinstance(actions, list)
    action = actions[0]
    assert isinstance(action, dict)
    action["resource_id"] = "streamt://payments/prod/gateway_rule/orders_view"
    action["action"] = "delete"
    action["gateway_evidence"] = None
    _resign_plan_data(data)
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match="require exact action evidence"):
        ReviewedPlanFile.load(path)


def test_offline_plan_requires_null_state_and_online_plan_requires_reference() -> None:
    offline = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=None,
        actions=(),
        offline=True,
    )

    assert offline.to_dict()["state"] is None
    assert offline.to_dict()["actions"] == []
    with pytest.raises(PlanFileError, match="must not contain actions"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=None,
            actions=_reviewed_actions(),
            offline=True,
        )
    with pytest.raises(PlanFileError, match="Offline reviewed plans must encode state as null"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=_state_reference(),
            actions=(),
            offline=True,
        )
    with pytest.raises(PlanFileError, match="require an exact ownership-state reference"):
        ReviewedPlanFile.create(
            _deployment_plan(),
            _manifest(),
            project="payments",
            environment="prod",
            runtime={},
            state=None,
            actions=(),
        )


@pytest.mark.parametrize("version", [1, 2, 3, 4])
def test_v1_through_v4_plans_require_explicit_regeneration(
    tmp_path: Path,
    version: int,
) -> None:
    path = tmp_path / f"v{version}.plan.json"
    data = _reviewed_plan().to_dict()
    data["format_version"] = version
    if version == 2:
        state = data.pop("state")
        assert isinstance(state, dict)
        data["state_serial"] = state["serial"]
    path.write_text(json.dumps(data))

    with pytest.raises(
        PlanFileError,
        match=(
            rf"format version {version} predates exact reviewed action binding.*"
            r"regenerate.*streamt plan --out"
        ),
    ):
        ReviewedPlanFile.load(path)


def test_state_reference_strictly_rejects_unknown_and_credential_shaped_fields() -> None:
    valid = _state_reference().to_dict()

    with pytest.raises(PlanFileError, match=r"unknown field.*revision"):
        StateReference.from_dict({**valid, "revision": "provider-secret"})
    with pytest.raises(PlanFileError, match=r"missing field.*checksum"):
        StateReference.from_dict({key: value for key, value in valid.items() if key != "checksum"})
    with pytest.raises(PlanFileError, match="canonical UUID"):
        StateReference.from_dict({**valid, "store_id": "password=provider-secret"})
    with pytest.raises(PlanFileError, match="non-negative integer"):
        StateReference.from_dict({**valid, "serial": True})


def test_loaded_v4_plan_strictly_rejects_extra_state_fields(tmp_path: Path) -> None:
    path = tmp_path / "extra-state.plan.json"
    data = _reviewed_plan().to_dict()
    state = data["state"]
    assert isinstance(state, dict)
    state["provider_revision"] = "must-not-be-portable"
    _resign_plan_data(data)
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match=r"unknown field.*provider_revision"):
        ReviewedPlanFile.load(path)


def test_plan_file_load_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "reviewed.plan.json"
    _reviewed_plan().save(path)
    data = json.loads(path.read_text())
    data["plan"]["resources"][0]["action"] = "delete"
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(path)


def test_plan_file_checksum_covers_actions(tmp_path: Path) -> None:
    path = tmp_path / "reviewed.plan.json"
    _reviewed_plan().save(path)
    data = json.loads(path.read_text())
    data["actions"][0]["action"] = "update"
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match="checksum mismatch"):
        ReviewedPlanFile.load(path)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda actions: actions.pop("actions"), "missing field.*actions"),
        (lambda actions: actions.__setitem__("actions", {}), "must be an array"),
        (
            lambda actions: actions["actions"][0].pop("gateway_evidence"),
            "missing field.*gateway_evidence",
        ),
        (
            lambda actions: actions["actions"][0].__setitem__("future", True),
            "unknown field.*future",
        ),
    ],
)
def test_loaded_v4_plan_strictly_rejects_malformed_actions(
    tmp_path: Path,
    mutate: object,
    message: str,
) -> None:
    path = tmp_path / "malformed-actions.plan.json"
    data = _reviewed_plan().to_dict()
    assert callable(mutate)
    mutate(data)
    _resign_plan_data(data)
    path.write_text(json.dumps(data))

    with pytest.raises(PlanFileError, match=message):
        ReviewedPlanFile.load(path)


def test_plan_file_rejects_duplicate_json_fields(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.plan.json"
    path.write_text('{"kind":"streamt.reviewed-plan","kind":"changed"}')

    with pytest.raises(PlanFileError, match="duplicate field 'kind'"):
        ReviewedPlanFile.load(path)


def test_context_verification_detects_project_environment_and_manifest_drift() -> None:
    reviewed = _reviewed_plan()
    runtime = {"kafka": {"bootstrap_servers": "broker:9092"}}

    reviewed.verify_context(
        _manifest(),
        project="payments",
        environment="prod",
        runtime=runtime,
        state_observation=_state_observation(),
    )
    with pytest.raises(StalePlanError, match="does not match current project"):
        reviewed.verify_context(
            _manifest(),
            project="other",
            environment="prod",
            runtime=runtime,
            state_observation=_state_observation(),
        )
    with pytest.raises(StalePlanError, match="does not match 'stage'"):
        reviewed.verify_context(
            _manifest(),
            project="payments",
            environment="stage",
            runtime=runtime,
            state_observation=_state_observation(),
        )
    with pytest.raises(StalePlanError, match="runtime endpoints"):
        reviewed.verify_context(
            _manifest(),
            project="payments",
            environment="prod",
            runtime={"kafka": {"bootstrap_servers": "other:9092"}},
            state_observation=_state_observation(),
        )

    changed_manifest = _manifest()
    changed_manifest.artifacts["topics"][0]["partitions"] = 12
    with pytest.raises(StalePlanError, match="project content changed"):
        reviewed.verify_context(
            changed_manifest,
            project="payments",
            environment="prod",
            runtime=runtime,
            state_observation=_state_observation(),
        )


def test_context_verification_checks_state_serial() -> None:
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={"kafka": {"bootstrap_servers": "broker:9092"}},
        state=_state_reference(serial=7),
        actions=_reviewed_actions(),
    )
    runtime = {"kafka": {"bootstrap_servers": "broker:9092"}}

    reviewed.verify_context(
        _manifest(),
        project="payments",
        environment="prod",
        runtime=runtime,
        state_observation=_state_observation(serial=7),
    )
    with pytest.raises(StalePlanError, match="state serial 7"):
        reviewed.verify_context(
            _manifest(),
            project="payments",
            environment="prod",
            runtime=runtime,
            state_observation=_state_observation(serial=8),
        )


def test_exact_state_reference_rejects_every_identity_and_content_drift() -> None:
    reviewed_observation = _state_observation(
        serial=7,
        resources=_managed_topic_record(partitions=3),
    )
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=StateReference.from_observation(reviewed_observation),
        actions=_reviewed_actions(),
    )

    reviewed.verify_state(
        _state_observation(
            serial=7,
            resources=_managed_topic_record(partitions=3),
            revision="a-different-provider-revision",
        )
    )
    with pytest.raises(StalePlanError, match=r"backend 'local'.*'postgres'"):
        reviewed.verify_state(
            _state_observation(
                backend="postgres",
                serial=7,
                resources=_managed_topic_record(partitions=3),
            )
        )
    with pytest.raises(StalePlanError, match="backend instance"):
        reviewed.verify_state(
            _state_observation(
                store_id="00000000-0000-4000-8000-000000000002",
                serial=7,
                resources=_managed_topic_record(partitions=3),
            )
        )
    with pytest.raises(StalePlanError, match="state address"):
        reviewed.verify_state(
            _state_observation(
                namespace="another-store",
                serial=7,
                resources=_managed_topic_record(partitions=3),
            )
        )
    with pytest.raises(StalePlanError, match=r"state serial 7.*8"):
        reviewed.verify_state(
            _state_observation(
                serial=8,
                resources=_managed_topic_record(partitions=3),
            )
        )
    with pytest.raises(StalePlanError, match="state checksum"):
        reviewed.verify_state(
            _state_observation(
                serial=7,
                resources=_managed_topic_record(partitions=6),
            )
        )


def test_current_plan_verification_rechecks_exact_state_reference() -> None:
    reviewed = _reviewed_plan()

    with pytest.raises(StalePlanError, match="state checksum"):
        reviewed.verify_current_plan(
            _deployment_plan(),
            actions=_reviewed_actions(),
            state_observation=_state_observation(resources=_managed_topic_record(partitions=6)),
        )


def test_live_action_drift_is_rejected_but_impact_metrics_may_change() -> None:
    reviewed = _reviewed_plan()
    same_actions = _deployment_plan()
    same_actions.impact_radius = []
    reviewed.verify_current_plan(
        same_actions,
        actions=_reviewed_actions(),
        state_observation=_state_observation(),
    )

    drifted = DeploymentPlan(topic_changes=[TopicChange(topic="payments.clean.v1", action="none")])
    with pytest.raises(StalePlanError, match="live resource actions"):
        reviewed.verify_current_plan(
            drifted,
            actions=_reviewed_actions(),
            state_observation=_state_observation(),
        )


def test_current_plan_verification_rejects_ordered_action_and_evidence_drift() -> None:
    reviewed_actions = _gateway_delete_actions()
    reviewed = ReviewedPlanFile.create(
        _deployment_plan(),
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=_state_reference(),
        actions=reviewed_actions,
    )
    changed_data = reviewed_actions[0].to_dict(control_version=CURRENT_CONTROL_VERSION)
    evidence = changed_data["gateway_evidence"]
    assert isinstance(evidence, dict)
    current = evidence["current"]
    assert isinstance(current, dict)
    current["fingerprint"] = "sha256:" + "c" * 64
    changed_actions = (
        OperationAction.from_dict(
            changed_data,
            control_version=CURRENT_CONTROL_VERSION,
        ),
    )

    reviewed.verify_current_plan(
        _deployment_plan(),
        actions=reviewed_actions,
        state_observation=_state_observation(),
    )
    with pytest.raises(StalePlanError, match="ordered action identity or evidence"):
        reviewed.verify_current_plan(
            _deployment_plan(),
            actions=changed_actions,
            state_observation=_state_observation(),
        )


def test_plan_payload_redacts_sensitive_change_evidence() -> None:
    plan = DeploymentPlan(
        connector_changes=[
            ConnectorChange(
                connector_name="sink",
                action="update",
                changes={
                    "config": {
                        "password": {"from": "old-secret", "to": "new-secret"},
                        "basic.auth.user.info": "alice:kafka-password",
                        "sasl.jaas.config": "username=alice password=jaas-password",
                        "url": "https://alice:super-secret@example.test/path",
                    }
                },
            )
        ]
    )

    serialized = json.dumps(deployment_plan_payload(plan))
    assert "old-secret" not in serialized
    assert "new-secret" not in serialized
    assert "kafka-password" not in serialized
    assert "jaas-password" not in serialized
    assert "super-secret" not in serialized
    assert '"config"' in serialized


def test_plan_payload_includes_sorted_ownership_requirements() -> None:
    plan = DeploymentPlan(
        ownership_requirements=[
            OwnershipRequirement(
                resource_id="streamt://payments/prod/topic/z",
                kind="topic",
                logical_name="z",
                physical_name="z.v1",
                reason="requires_adoption",
                observed_action="update",
                ownership_mode="managed",
                message="Adopt z first.",
            ),
            OwnershipRequirement(
                resource_id="streamt://payments/prod/topic/a",
                kind="topic",
                logical_name="a",
                physical_name="a.v1",
                reason="external",
                observed_action="none",
                ownership_mode="external",
                message="a is observe-only.",
            ),
        ]
    )

    payload = deployment_plan_payload(plan)

    assert payload["summary"]["ownership_requirements"] == 2
    assert [requirement["logical_name"] for requirement in payload["ownership_requirements"]] == [
        "a",
        "z",
    ]


def _removal_assessment(
    owner: str,
    *,
    status: str = "already_absent",
) -> GatewayRemovalAssessment:
    return GatewayRemovalAssessment(
        resource_id=f"streamt://payments/prod/gateway_rule/{owner}",
        logical_owner=owner,
        rule_name=f"{owner}_rule",
        alias_name=f"{owner}.public",
        backend_identity=_GATEWAY_BACKEND,
        status=status,  # type: ignore[arg-type]
    )


def test_plan_payload_preserves_removal_assessment_order_and_verifies_drift() -> None:
    assessments = (
        _removal_assessment("z_owner"),
        _removal_assessment("a_owner"),
    )
    plan = DeploymentPlan(gateway_removal_assessments=assessments)
    payload = deployment_plan_payload(plan)

    assert payload["summary"]["gateway_removal_assessments"] == 2
    assert [item["logical_owner"] for item in payload["gateway_removal_assessments"]] == [
        "z_owner",
        "a_owner",
    ]

    reviewed = ReviewedPlanFile.create(
        plan,
        _manifest(),
        project="payments",
        environment="prod",
        runtime={},
        state=_state_reference(),
        actions=(),
    )
    changed = DeploymentPlan(
        gateway_removal_assessments=(
            _removal_assessment("z_owner", status="state_provider_drift"),
            _removal_assessment("a_owner"),
        )
    )

    with pytest.raises(StalePlanError, match="Gateway removal assessments"):
        reviewed.verify_current_plan(
            changed,
            actions=(),
            state_observation=_state_observation(),
        )


def test_cli_saves_offline_plan_but_rejects_it_for_apply(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()

    planned = runner.invoke(
        main,
        ["-o", "json", "plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)],
    )
    assert planned.exit_code == 0, planned.output
    planned_output = _json_output(planned)
    assert planned_output["data"]["plan_file"] == str(plan_path)
    assert plan_path.exists()
    offline_review = ReviewedPlanFile.load(plan_path)
    assert offline_review.state is None
    assert offline_review.to_dict()["state"] is None

    with (
        patch("streamt.cli.commands.apply.make_kafka_deployer") as make_kafka,
        patch("streamt.cli.commands.apply.make_deployment_state_service") as make_state_service,
    ):
        applied = runner.invoke(
            main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
        )

    assert applied.exit_code == 1
    applied_output = _json_output(applied)
    assert applied_output["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"
    assert "preview-only" in applied_output["errors"][0]["message"]
    make_kafka.assert_not_called()
    make_state_service.assert_not_called()


@pytest.mark.parametrize(
    "extra_args",
    [[], ["--confirm"], ["--confirm-env", "prod"], ["--force"], ["--dry-run"]],
)
def test_protected_environment_rejects_every_direct_apply_before_backend_setup(
    tmp_path: Path,
    extra_args: list[str],
) -> None:
    project_path = tmp_path / "project with spaces"
    project_path.mkdir()
    _write_environment_project(project_path, protected=True)
    resolved_project = project_path.resolve()
    project_arg = shlex.quote(str(resolved_project))
    plan_path = resolved_project / ".streamt" / "reviewed-plan.json"
    plan_arg = shlex.quote(str(plan_path))
    plan_command = f"streamt plan --project-dir {project_arg} --env prod --out {plan_arg}"
    apply_command = f"streamt apply --project-dir {project_arg} --env prod --plan {plan_arg}"

    with patch("streamt.cli.commands.apply.make_kafka_deployer") as make_kafka:
        result = CliRunner().invoke(
            main,
            [
                "-o",
                "json",
                "apply",
                "-p",
                str(project_path),
                "--env",
                "prod",
                *extra_args,
            ],
        )

    assert result.exit_code == 1
    payload = _json_output(result)
    assert payload["errors"][0]["code"] == "E418_REVIEWED_PLAN_REQUIRED"
    assert payload["data"] == {
        "environment": "prod",
        "policy": "environment.protected",
        "required_workflow": "reviewed_plan",
        "next_steps": [plan_command, apply_command],
    }
    assert plan_command in payload["errors"][0]["suggestion"]
    make_kafka.assert_not_called()


def test_explicit_shared_environment_policy_requires_reviewed_plan(
    tmp_path: Path,
) -> None:
    _write_environment_project(
        tmp_path,
        protected=False,
        require_reviewed_plan=True,
    )

    result = CliRunner().invoke(
        main,
        ["-o", "json", "apply", "-p", str(tmp_path), "--env", "prod"],
    )

    assert result.exit_code == 1
    payload = _json_output(result)
    assert payload["errors"][0]["code"] == "E418_REVIEWED_PLAN_REQUIRED"
    assert payload["data"]["policy"] == "safety.require_reviewed_plan"


def test_unprotected_confirmation_policy_fails_before_backend_setup(
    tmp_path: Path,
) -> None:
    _write_environment_project(tmp_path, protected=False)
    environment_path = tmp_path / "environments" / "prod.yml"
    environment = yaml.safe_load(environment_path.read_text())
    environment["safety"]["confirm_apply"] = True
    environment_path.write_text(yaml.safe_dump(environment))

    with patch("streamt.cli.commands.apply.make_kafka_deployer") as make_kafka:
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--env", "prod"],
        )

    assert result.exit_code == 1
    payload = _json_output(result)
    assert payload["errors"][0]["code"] == "E503_ENVIRONMENT_ERROR"
    assert "requires confirmation" in payload["errors"][0]["message"]
    make_kafka.assert_not_called()


def test_protected_environment_accepts_integrity_checked_reviewed_plan(
    tmp_path: Path,
) -> None:
    _write_environment_project(tmp_path, protected=True)
    plan_path = tmp_path / "prod.plan.json"
    runner = CliRunner()
    planned = runner.invoke(
        main,
        [
            "-o",
            "json",
            "plan",
            "-p",
            str(tmp_path),
            "--env",
            "prod",
            "--out",
            str(plan_path),
        ],
    )
    assert planned.exit_code == 0, planned.output

    applied = runner.invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--env",
            "prod",
            "--confirm-env",
            "prod",
            "--plan",
            str(plan_path),
        ],
    )

    assert applied.exit_code == 0, applied.output
    payload = _json_output(applied)
    assert payload["data"]["plan_checksum"] == ReviewedPlanFile.load(plan_path).checksum


def test_protected_environment_still_rejects_tampered_reviewed_plan(
    tmp_path: Path,
) -> None:
    _write_environment_project(tmp_path, protected=True)
    plan_path = tmp_path / "prod.plan.json"
    runner = CliRunner()
    planned = runner.invoke(
        main,
        [
            "plan",
            "-p",
            str(tmp_path),
            "--env",
            "prod",
            "--offline",
            "--out",
            str(plan_path),
        ],
    )
    assert planned.exit_code == 0, planned.output
    plan_data = json.loads(plan_path.read_text())
    plan_data["environment"] = "staging"
    plan_path.write_text(json.dumps(plan_data))

    applied = runner.invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--env",
            "prod",
            "--confirm-env",
            "prod",
            "--plan",
            str(plan_path),
        ],
    )

    assert applied.exit_code == 1
    assert _json_output(applied)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_cli_rejects_tampered_and_stale_plan_files(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    result = runner.invoke(main, ["plan", "-p", str(tmp_path), "--out", str(plan_path)])
    assert result.exit_code == 0, result.output

    original = plan_path.read_text()
    data = json.loads(original)
    data["offline"] = True
    plan_path.write_text(json.dumps(data))
    tampered = runner.invoke(
        main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
    )
    assert tampered.exit_code == 1
    assert _json_output(tampered)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"

    plan_path.write_text(original)
    config = yaml.safe_load((tmp_path / "stream_project.yml").read_text())
    config["project"]["name"] = "renamed-project"
    (tmp_path / "stream_project.yml").write_text(yaml.safe_dump(config))
    stale = runner.invoke(
        main, ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)]
    )
    assert stale.exit_code == 1
    assert _json_output(stale)["errors"][0]["code"] == "E409_PLAN_STALE"


def test_cli_rejects_selection_with_reviewed_plan(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    result = CliRunner().invoke(
        main, ["plan", "-p", str(tmp_path), "--offline", "--out", str(plan_path)]
    )
    assert result.exit_code == 0, result.output

    applied = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--plan",
            str(plan_path),
            "--target",
            "anything",
        ],
    )
    assert applied.exit_code == 1
    payload = _json_output(applied)
    assert payload["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_cli_reports_missing_plan_file_as_structured_error(tmp_path: Path) -> None:
    _write_project(tmp_path)

    result = CliRunner().invoke(
        main,
        [
            "-o",
            "json",
            "apply",
            "-p",
            str(tmp_path),
            "--plan",
            str(tmp_path / "missing.plan.json"),
        ],
    )

    assert result.exit_code == 1
    assert _json_output(result)["errors"][0]["code"] == "E408_PLAN_FILE_INVALID"


def test_cli_rejects_live_plan_drift_before_apply(tmp_path: Path) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    result = runner.invoke(main, ["plan", "-p", str(tmp_path), "--out", str(plan_path)])
    assert result.exit_code == 0, result.output

    changed_live_plan = DeploymentPlan(
        topic_changes=[TopicChange(topic="new-live-topic", action="create")]
    )
    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=changed_live_plan,
        ),
        patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
    ):
        applied = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert applied.exit_code == 1
    assert _json_output(applied)["errors"][0]["code"] == "E409_PLAN_STALE"
    apply_plan.assert_not_called()


def test_cli_rejects_same_serial_state_content_drift_before_runtime_setup(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    planned = runner.invoke(
        main,
        ["plan", "-p", str(tmp_path), "--out", str(plan_path)],
    )
    assert planned.exit_code == 0, planned.output
    resource_uri = resource_id(
        "plan-test",
        "default",
        "topic",
        "concurrent_owner",
    )
    LocalState(
        project="plan-test",
        environment="default",
        serial=0,
        resources={
            resource_uri: ManagedResourceRecord(
                physical_name="concurrent.v1",
                ownership="adopted",
                artifact_checksum=artifact_checksum({"name": "concurrent.v1"}),
                backend="direct-kafka",
            )
        },
    ).save(local_state_path(tmp_path, environment="default"))

    with patch("streamt.cli.commands.apply.make_kafka_deployer") as make_kafka:
        applied = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert applied.exit_code == 1
    payload = _json_output(applied)
    assert payload["errors"][0]["code"] == "E409_PLAN_STALE"
    assert "state checksum" in payload["errors"][0]["message"]
    make_kafka.assert_not_called()


def test_cli_rereads_state_after_live_plan_and_blocks_before_mutation(
    tmp_path: Path,
) -> None:
    _write_project(tmp_path)
    plan_path = tmp_path / "reviewed.plan.json"
    runner = CliRunner()
    planned = runner.invoke(
        main,
        ["plan", "-p", str(tmp_path), "--out", str(plan_path)],
    )
    assert planned.exit_code == 0, planned.output
    state_path = local_state_path(tmp_path, environment="default")

    def live_plan_with_concurrent_state_change() -> DeploymentPlan:
        resource_uri = resource_id(
            "plan-test",
            "default",
            "topic",
            "changed_during_live_plan",
        )
        LocalState(
            project="plan-test",
            environment="default",
            serial=0,
            resources={
                resource_uri: ManagedResourceRecord(
                    physical_name="changed.v1",
                    ownership="adopted",
                    artifact_checksum=artifact_checksum({"name": "changed.v1"}),
                    backend="direct-kafka",
                )
            },
        ).save(state_path)
        return DeploymentPlan()

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            side_effect=live_plan_with_concurrent_state_change,
        ),
        patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
    ):
        applied = runner.invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert applied.exit_code == 1
    payload = _json_output(applied)
    assert payload["errors"][0]["code"] == "E409_PLAN_STALE"
    assert "state checksum" in payload["errors"][0]["message"]
    apply_plan.assert_not_called()


def test_cli_saved_plan_fails_closed_on_blocking_ownership_requirement(
    tmp_path: Path,
) -> None:
    from streamt.compiler import Compiler
    from streamt.core.parser import ProjectParser

    _write_project(tmp_path)
    project = ProjectParser(tmp_path).parse()
    manifest = Compiler(project).compile(dry_run=True)
    blocked_plan = DeploymentPlan(ownership_requirements=[_ownership_requirement()])
    reviewed = ReviewedPlanFile.create(
        blocked_plan,
        manifest,
        project=project.project.name,
        environment="default",
        runtime=project.runtime,
        state=StateReference.from_observation(
            make_deployment_state_service(
                tmp_path,
                project=project.project.name,
                environment="default",
                config=local_deployment_state_config(),
            ).read()
        ),
        actions=(),
    )
    plan_path = tmp_path / "blocked.plan.json"
    reviewed.save(plan_path)

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=blocked_plan,
        ),
        patch("streamt.deployer.planner.DeploymentPlanner.apply") as apply_plan,
    ):
        result = CliRunner().invoke(
            main,
            ["-o", "json", "apply", "-p", str(tmp_path), "--plan", str(plan_path)],
        )

    assert result.exit_code == 1
    payload = _json_output(result)
    assert payload["errors"][0]["code"] == "E410_OWNERSHIP_REQUIRED"
    assert payload["data"]["blocking_ownership_requirements"][0]["reason"] == ("requires_adoption")
    apply_plan.assert_not_called()


def test_cli_external_ownership_visibility_does_not_block_other_apply(tmp_path: Path) -> None:
    _write_project(tmp_path)
    visible_plan = DeploymentPlan(
        ownership_requirements=[_ownership_requirement(reason="external")]
    )
    results = {
        "created": [],
        "updated": [],
        "deleted": [],
        "unchanged": [],
        "errors": [],
        "rollback_candidates": [],
        "summary": {"total": 0, "succeeded": 0, "failed": 0, "unchanged": 0},
    }

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=visible_plan,
        ),
        patch(
            "streamt.deployer.planner.DeploymentPlanner.apply",
            return_value=results,
        ) as apply_plan,
    ):
        result = CliRunner().invoke(main, ["apply", "-p", str(tmp_path)])

    assert result.exit_code == 0, result.output
    apply_plan.assert_called_once()
    assert apply_plan.call_args.args == (visible_plan,)
    assert set(apply_plan.call_args.kwargs) == {
        "before_action",
        "after_action",
        "stop_on_error",
    }
    assert apply_plan.call_args.kwargs["stop_on_error"] is True


def test_cli_rollback_receives_the_exact_deployment_plan(tmp_path: Path) -> None:
    _write_project(tmp_path)
    deployment_plan = DeploymentPlan()
    results = {
        "created": ["gateway_rule:orders_rule"],
        "updated": [],
        "deleted": [],
        "unchanged": [],
        "errors": ["gateway_rule:archive_rule: failed"],
        "rollback_candidates": ["gateway_rule:orders_rule"],
        "summary": {"total": 2, "succeeded": 1, "failed": 1, "unchanged": 0},
    }

    with (
        patch(
            "streamt.deployer.planner.DeploymentPlanner.plan",
            return_value=deployment_plan,
        ),
        patch(
            "streamt.deployer.planner.DeploymentPlanner.apply",
            return_value=results,
        ),
        patch(
            "streamt.deployer.planner.DeploymentPlanner.rollback",
            return_value=(["gateway_rule:orders_rule"], []),
        ) as rollback,
    ):
        result = CliRunner().invoke(main, ["apply", "-p", str(tmp_path)])

    assert result.exit_code == 1
    rollback.assert_called_once()
    assert rollback.call_args.args == (["gateway_rule:orders_rule"],)
    assert rollback.call_args.kwargs["plan"] is deployment_plan
    assert set(rollback.call_args.kwargs) == {
        "plan",
        "before_action",
        "after_action",
        "stop_on_error",
    }
    assert rollback.call_args.kwargs["stop_on_error"] is True
