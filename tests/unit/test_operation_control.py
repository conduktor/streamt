"""Durable local operation-control protocol characterization tests."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from streamt.core.deployment_state import local_deployment_state_config
from streamt.deployer.connect import (
    ConnectClusterBinding,
    ManagedConnectorObservation,
    managed_connector_absence_fingerprint,
)
from streamt.deployer.gateway import managed_gateway_absence_fingerprint
from streamt.deployer.state import LocalState, StateFormatError, local_state_path
from streamt.deployer.state_backend import (
    CURRENT_CONTROL_VERSION,
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    GatewayActionEvidence,
    GatewayActionSurfaceEvidence,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateBackendConflictError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    local_control_path,
    make_deployment_state_service,
    operation_timestamp,
    state_checksum,
)

GATEWAY_BACKEND = "conduktor-gateway:v1:p:sha256:" + "1" * 64
CURRENT_FINGERPRINT = "sha256:" + "2" * 64
DESIRED_FINGERPRINT = "sha256:" + "3" * 64
ABSENCE_FINGERPRINT = managed_gateway_absence_fingerprint(
    GATEWAY_BACKEND,
    "orders_rule",
    "orders.public",
)
CONNECTOR_BACKEND = "kafka-connect:v1:primary:sha256:" + "4" * 64
CONNECTOR_NAME = "archive-orders-sink"
CONNECTOR_CURRENT_FINGERPRINT = "sha256:" + "5" * 64
CONNECTOR_ABSENCE_FINGERPRINT = managed_connector_absence_fingerprint(
    CONNECTOR_BACKEND,
    CONNECTOR_NAME,
)
CONNECTOR_PRIOR_CHECKSUM = "sha256:" + "6" * 64


def _intent(state: LocalState) -> OperationIntent:
    return OperationIntent(
        operation_id=str(uuid4()),
        kind="apply",
        started_at=operation_timestamp(),
        actor="test-runner",
        prior_state_serial=state.serial,
        prior_state_checksum=state_checksum(state),
        reviewed_plan_checksum=None,
        actions=(OperationAction(0, "topic:orders", "create"),),
    )


def _gateway_evidence(
    *,
    current_exists: bool = True,
    desired_exists: bool = True,
    current_fingerprint: str | None = None,
    desired_fingerprint: str | None = None,
    current_interceptor_count: int | None = None,
    desired_interceptor_count: int | None = None,
) -> GatewayActionEvidence:
    return GatewayActionEvidence(
        version=1,
        backend_identity=GATEWAY_BACKEND,
        rule_name="orders_rule",
        alias_name="orders.public",
        current=GatewayActionSurfaceEvidence(
            exists=current_exists,
            fingerprint=(
                current_fingerprint
                if current_fingerprint is not None
                else CURRENT_FINGERPRINT if current_exists else ABSENCE_FINGERPRINT
            ),
            managed_interceptor_count=(
                current_interceptor_count
                if current_interceptor_count is not None
                else 1 if current_exists else 0
            ),
        ),
        desired=GatewayActionSurfaceEvidence(
            exists=desired_exists,
            fingerprint=(
                desired_fingerprint
                if desired_fingerprint is not None
                else DESIRED_FINGERPRINT if desired_exists else ABSENCE_FINGERPRINT
            ),
            managed_interceptor_count=(
                desired_interceptor_count
                if desired_interceptor_count is not None
                else 1 if desired_exists else 0
            ),
        ),
    )


def _gateway_action(*, action: str = "update") -> OperationAction:
    evidence = _gateway_evidence(
        current_exists=action != "create",
        desired_exists=action != "delete",
        current_interceptor_count=0 if action == "adopt" else None,
        desired_interceptor_count=0 if action == "adopt" else None,
    )
    return OperationAction(
        index=0,
        resource_id="streamt://payments/dev/gateway_rule/orders_owner",
        action=action,
        gateway_evidence=evidence,
    )


def _connector_evidence() -> ConnectorActionEvidence:
    return ConnectorActionEvidence(
        version=1,
        backend_identity=CONNECTOR_BACKEND,
        connector_name=CONNECTOR_NAME,
        prior_artifact_checksum=CONNECTOR_PRIOR_CHECKSUM,
        current=ConnectorActionSurfaceEvidence(
            exists=True,
            fingerprint=CONNECTOR_CURRENT_FINGERPRINT,
        ),
        desired=ConnectorActionSurfaceEvidence(
            exists=False,
            fingerprint=CONNECTOR_ABSENCE_FINGERPRINT,
        ),
    )


def _connector_action() -> OperationAction:
    return OperationAction(
        index=0,
        resource_id="streamt://payments/dev/connector/archive_orders",
        action="delete",
        connector_evidence=_connector_evidence(),
    )


def test_gateway_action_evidence_has_one_strict_secret_neutral_v3_shape() -> None:
    action = _gateway_action()

    serialized = action.to_dict()

    assert serialized == {
        "index": 0,
        "resource_id": "streamt://payments/dev/gateway_rule/orders_owner",
        "action": "update",
        "gateway_evidence": {
            "version": 1,
            "backend_identity": GATEWAY_BACKEND,
            "rule_name": "orders_rule",
            "alias_name": "orders.public",
            "current": {
                "exists": True,
                "fingerprint": CURRENT_FINGERPRINT,
                "managed_interceptor_count": 1,
            },
            "desired": {
                "exists": True,
                "fingerprint": DESIRED_FINGERPRINT,
                "managed_interceptor_count": 1,
            },
        },
        "connector_evidence": None,
    }
    assert OperationAction.from_dict(serialized, control_version=3) == action
    assert action.resource_id.endswith("/orders_owner")
    assert action.gateway_evidence is not None
    assert action.gateway_evidence.rule_name == "orders_rule"
    rendered = repr(action)
    assert "http" not in rendered
    assert "config" not in rendered


def test_connector_absence_fingerprint_freezes_exact_canonical_preimage() -> None:
    binding = ConnectClusterBinding.from_backend_identity(CONNECTOR_BACKEND)
    preimage = json.dumps(
        {
            "binding": binding.backend_identity,
            "config": (),
            "exists": False,
            "name": CONNECTOR_NAME,
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    expected = "sha256:" + hashlib.sha256(preimage.encode("utf-8")).hexdigest()

    assert preimage == (
        '{"binding":"kafka-connect:v1:primary:sha256:'
        + "4" * 64
        + '","config":[],"exists":false,"name":"archive-orders-sink"}'
    )
    assert expected == CONNECTOR_ABSENCE_FINGERPRINT


def test_connector_present_fingerprint_freezes_exact_canonical_preimage() -> None:
    observation = ManagedConnectorObservation(
        binding=ConnectClusterBinding.from_backend_identity(CONNECTOR_BACKEND),
        name=CONNECTOR_NAME,
        exists=True,
        config=(
            ("connector.class", "com.example.ArchiveSink"),
            ("name", CONNECTOR_NAME),
            ("tasks.max", 2),
            ("topics", "orders.v1"),
        ),
    )
    preimage = json.dumps(
        {
            "binding": CONNECTOR_BACKEND,
            "config": observation.config,
            "exists": True,
            "name": CONNECTOR_NAME,
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    assert preimage == (
        '{"binding":"kafka-connect:v1:primary:sha256:'
        + "4" * 64
        + '","config":[["connector.class","com.example.ArchiveSink"],'
        '["name","archive-orders-sink"],["tasks.max",2],'
        '["topics","orders.v1"]],"exists":true,'
        '"name":"archive-orders-sink"}'
    )
    assert observation.fingerprint == (
        "sha256:672988ba5add90cceda3e3615b4c86518ee4e7a293efe298e04db0e63466fb9c"
    )


def test_connector_action_evidence_has_one_strict_secret_neutral_v3_shape() -> None:
    action = _connector_action()
    serialized = action.to_dict()

    assert serialized == {
        "index": 0,
        "resource_id": "streamt://payments/dev/connector/archive_orders",
        "action": "delete",
        "gateway_evidence": None,
        "connector_evidence": {
            "version": 1,
            "backend_identity": CONNECTOR_BACKEND,
            "connector_name": CONNECTOR_NAME,
            "prior_artifact_checksum": CONNECTOR_PRIOR_CHECKSUM,
            "current": {
                "exists": True,
                "fingerprint": CONNECTOR_CURRENT_FINGERPRINT,
            },
            "desired": {
                "exists": False,
                "fingerprint": CONNECTOR_ABSENCE_FINGERPRINT,
            },
        },
    }
    assert OperationAction.from_dict(serialized, control_version=3) == action
    rendered = repr(action)
    assert "config" not in rendered
    assert "endpoint" not in rendered
    assert "postgres" not in rendered


def test_connector_action_evidence_rejects_malformed_or_secret_fields() -> None:
    original = _connector_evidence().to_dict()

    def add_raw_config(data: dict[str, Any]) -> None:
        data["config"] = {"password": "connector-secret"}

    def add_surface_config(data: dict[str, Any]) -> None:
        data["current"]["config"] = {"token": "provider-secret"}

    def remove_name(data: dict[str, Any]) -> None:
        data.pop("connector_name")

    def endpoint_backend(data: dict[str, Any]) -> None:
        data["backend_identity"] = "https://alice:provider-secret@connect.example"

    def dsn_name(data: dict[str, Any]) -> None:
        data["connector_name"] = "postgresql://alice:dsn-secret@db/state"

    def inline_secret_name(data: dict[str, Any]) -> None:
        data["connector_name"] = "password=connector-secret"

    def unsafe_name(data: dict[str, Any]) -> None:
        data["connector_name"] = "connector\ud800"

    def bad_checksum(data: dict[str, Any]) -> None:
        data["prior_artifact_checksum"] = "SHA256:" + "A" * 64

    def nonboolean_exists(data: dict[str, Any]) -> None:
        data["current"]["exists"] = 1

    def wrong_absence(data: dict[str, Any]) -> None:
        data["desired"]["fingerprint"] = "sha256:" + "7" * 64

    def absent_current(data: dict[str, Any]) -> None:
        data["current"] = dict(data["desired"])

    def present_desired(data: dict[str, Any]) -> None:
        data["desired"] = {
            "exists": True,
            "fingerprint": "sha256:" + "7" * 64,
        }

    def unknown_version(data: dict[str, Any]) -> None:
        data["version"] = 2

    for mutate in (
        add_raw_config,
        add_surface_config,
        remove_name,
        endpoint_backend,
        dsn_name,
        inline_secret_name,
        unsafe_name,
        bad_checksum,
        nonboolean_exists,
        wrong_absence,
        absent_current,
        present_desired,
        unknown_version,
    ):
        payload = deepcopy(original)
        mutate(payload)
        with pytest.raises(StateFormatError) as captured:
            ConnectorActionEvidence.from_dict(payload)
        message = str(captured.value)
        assert "connector-secret" not in message
        assert "provider-secret" not in message
        assert "dsn-secret" not in message


@pytest.mark.parametrize(
    ("resource_id", "action"),
    [
        ("streamt://payments/dev/topic/orders", "delete"),
        ("streamt://payments/dev/connector/archive_orders", "update"),
        ("connector:archive_orders", "delete"),
    ],
)
def test_connector_action_evidence_is_connector_delete_only(
    resource_id: str,
    action: str,
) -> None:
    with pytest.raises(StateFormatError, match=r"Connector|evidenced"):
        OperationAction(
            index=0,
            resource_id=resource_id,
            action=action,
            connector_evidence=_connector_evidence(),
        )


def test_gateway_and_connector_action_evidence_are_mutually_exclusive() -> None:
    with pytest.raises(StateFormatError, match="mutually exclusive"):
        OperationAction(
            index=0,
            resource_id="streamt://payments/dev/connector/archive_orders",
            action="delete",
            gateway_evidence=_gateway_evidence(),
            connector_evidence=_connector_evidence(),
        )


def test_connector_delete_requires_v3_evidence_and_legacy_wires_cannot_authorize() -> None:
    resource_id = "streamt://payments/dev/connector/archive_orders"
    with pytest.raises(StateFormatError, match="Connector deletion requires"):
        OperationAction(
            index=0,
            resource_id=resource_id,
            action="delete",
        )
    for version, payload in (
        (
            1,
            {
                "index": 0,
                "resource_id": resource_id,
                "action": "delete",
            },
        ),
        (
            2,
            {
                "index": 0,
                "resource_id": resource_id,
                "action": "delete",
                "gateway_evidence": None,
            },
        ),
    ):
        with pytest.raises(StateFormatError, match="Connector"):
            OperationAction.from_dict(payload, control_version=version)

    for version in (1, 2):
        with pytest.raises(StateFormatError, match=r"evidence|Connector"):
            _connector_action().to_dict(control_version=version)


def test_gateway_action_evidence_rejects_noncanonical_or_unsafe_fields() -> None:
    original = _gateway_evidence().to_dict()

    def add_raw_config(data: dict[str, Any]) -> None:
        data["configuration"] = {"sql": "select secret"}

    def remove_rule_name(data: dict[str, Any]) -> None:
        data.pop("rule_name")

    def add_surface_config(data: dict[str, Any]) -> None:
        data["current"]["config"] = {"password": "secret"}

    def malformed_fingerprint(data: dict[str, Any]) -> None:
        data["current"]["fingerprint"] = "SHA256:" + "A" * 64

    def missing_surface_field(data: dict[str, Any]) -> None:
        data["desired"].pop("exists")

    def nonboolean_exists(data: dict[str, Any]) -> None:
        data["desired"]["exists"] = 1

    def boolean_count(data: dict[str, Any]) -> None:
        data["desired"]["managed_interceptor_count"] = True

    def negative_count(data: dict[str, Any]) -> None:
        data["desired"]["managed_interceptor_count"] = -1

    def absent_nonzero_count(data: dict[str, Any]) -> None:
        data["current"]["exists"] = False

    def wrong_absence_fingerprint(data: dict[str, Any]) -> None:
        data["current"] = {
            "exists": False,
            "fingerprint": CURRENT_FINGERPRINT,
            "managed_interceptor_count": 0,
        }

    def present_with_absence_fingerprint(data: dict[str, Any]) -> None:
        data["desired"]["fingerprint"] = ABSENCE_FINGERPRINT

    def malformed_backend(data: dict[str, Any]) -> None:
        data["backend_identity"] = "https://alice:secret@gateway.example"

    def unsafe_alias(data: dict[str, Any]) -> None:
        data["alias_name"] = "orders/secret"

    def unsafe_rule_name(data: dict[str, Any]) -> None:
        data["rule_name"] = "orders/rule"

    def unknown_version(data: dict[str, Any]) -> None:
        data["version"] = 2

    for mutate in (
        add_raw_config,
        remove_rule_name,
        add_surface_config,
        malformed_fingerprint,
        missing_surface_field,
        nonboolean_exists,
        boolean_count,
        negative_count,
        absent_nonzero_count,
        wrong_absence_fingerprint,
        present_with_absence_fingerprint,
        malformed_backend,
        unsafe_alias,
        unsafe_rule_name,
        unknown_version,
    ):
        payload = deepcopy(original)
        mutate(payload)
        with pytest.raises(StateFormatError) as captured:
            GatewayActionEvidence.from_dict(payload)
        assert "alice" not in str(captured.value)
        assert "secret" not in str(captured.value)


@pytest.mark.parametrize(
    ("resource_id", "action", "evidence"),
    [
        ("streamt://payments/dev/topic/orders", "update", _gateway_evidence()),
        ("gateway_rule:orders", "update", _gateway_evidence()),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "noop",
            _gateway_evidence(),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "create",
            _gateway_evidence(),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "update",
            _gateway_evidence(desired_fingerprint=CURRENT_FINGERPRINT),
        ),
        (
            "streamt://payments/dev/gateway_rule/orders",
            "delete",
            _gateway_evidence(),
        ),
    ],
)
def test_gateway_action_evidence_rejects_kind_action_and_transition_mismatch(
    resource_id: str,
    action: str,
    evidence: GatewayActionEvidence,
) -> None:
    with pytest.raises(StateFormatError, match=r"Gateway|evidenced"):
        OperationAction(
            index=0,
            resource_id=resource_id,
            action=action,
            gateway_evidence=evidence,
        )


@pytest.mark.parametrize(
    "desired_fingerprint",
    [CURRENT_FINGERPRINT, DESIRED_FINGERPRINT],
    ids=["equal-surface", "different-desired-surface"],
)
def test_gateway_adopt_allows_present_zero_interceptor_surfaces(
    desired_fingerprint: str,
) -> None:
    evidence = _gateway_evidence(
        desired_fingerprint=desired_fingerprint,
        current_interceptor_count=0,
        desired_interceptor_count=0,
    )

    action = OperationAction(
        index=0,
        resource_id="streamt://payments/dev/gateway_rule/orders_owner",
        action="adopt",
        gateway_evidence=evidence,
    )

    serialized = action.to_dict(control_version=2)
    assert OperationAction.from_dict(serialized, control_version=2) == action
    assert serialized["gateway_evidence"] == evidence.to_dict()


@pytest.mark.parametrize(
    "evidence",
    [
        _gateway_evidence(
            current_exists=False,
            current_interceptor_count=0,
            desired_interceptor_count=0,
        ),
        _gateway_evidence(
            current_interceptor_count=1,
            desired_interceptor_count=0,
        ),
        _gateway_evidence(
            current_interceptor_count=0,
            desired_interceptor_count=1,
        ),
    ],
    ids=["absent-current", "current-interceptors", "desired-interceptors"],
)
def test_gateway_adopt_rejects_non_state_only_surfaces(
    evidence: GatewayActionEvidence,
) -> None:
    with pytest.raises(StateFormatError, match="Gateway"):
        OperationAction(
            index=0,
            resource_id="streamt://payments/dev/gateway_rule/orders_owner",
            action="adopt",
            gateway_evidence=evidence,
        )


def test_v1_control_roundtrips_exactly_and_rejects_gateway_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    intent = _intent(state)
    legacy = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
        control_version=1,
    )
    payload = legacy.to_dict()
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)

    assert set(payload["intent"]["actions"][0]) == {
        "index",
        "resource_id",
        "action",
    }
    loaded = OperationControlState.from_dict(payload, expected_address=address)
    assert loaded.control_version == 1
    assert loaded.to_dict() == payload
    assert json.dumps(
        loaded.to_dict(), separators=(",", ":"), sort_keys=True
    ) == canonical

    gateway_intent = replace(intent, actions=(_gateway_action(),))
    with pytest.raises(StateFormatError, match="version 1"):
        OperationControlState(
            address=address,
            status="in_progress",
            intent=gateway_intent,
            control_version=1,
        )
    gateway_payload = OperationControlState(
        address=address,
        status="in_progress",
        intent=gateway_intent,
    ).to_dict()
    gateway_payload["control_version"] = 1
    with pytest.raises(StateFormatError, match="unknown field"):
        OperationControlState.from_dict(gateway_payload, expected_address=address)


def test_v2_active_and_clear_controls_preserve_exact_legacy_bytes() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    active = OperationControlState(
        address=address,
        status="in_progress",
        intent=_intent(state),
        control_version=2,
    )
    active_payload = active.to_dict()
    active_bytes = json.dumps(active_payload, separators=(",", ":"), sort_keys=True)
    loaded_active = OperationControlState.from_dict(
        active_payload,
        expected_address=address,
    )

    assert loaded_active.control_version == 2
    assert loaded_active.to_dict() == active_payload
    assert (
        json.dumps(loaded_active.to_dict(), separators=(",", ":"), sort_keys=True) == active_bytes
    )
    assert loaded_active.intent is not None
    inherited = OperationControlState(
        address=address,
        status="in_progress",
        intent=loaded_active.intent,
    )
    assert inherited.control_version == 2
    assert inherited.to_dict() == active_payload

    clear_payload = OperationControlState(address=address, control_version=2).to_dict()
    loaded_clear = OperationControlState.from_dict(
        clear_payload,
        expected_address=address,
    )
    assert loaded_clear.control_version == 2
    assert loaded_clear.to_dict() == clear_payload
    assert OperationControlState.clear(address).control_version == CURRENT_CONTROL_VERSION


def test_v3_control_requires_exact_action_members_and_connector_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    ordinary = OperationControlState(
        address=address,
        status="in_progress",
        intent=_intent(state),
    )
    ordinary_payload = ordinary.to_dict()
    assert ordinary_payload["control_version"] == 3
    assert ordinary_payload["intent"]["actions"][0] == {
        "index": 0,
        "resource_id": "topic:orders",
        "action": "create",
        "gateway_evidence": None,
        "connector_evidence": None,
    }

    connector_intent = replace(_intent(state), actions=(_connector_action(),))
    connector = OperationControlState(
        address=address,
        status="in_progress",
        intent=connector_intent,
    )
    payload = connector.to_dict()
    loaded = OperationControlState.from_dict(payload, expected_address=address)
    assert loaded == connector
    assert loaded.to_dict() == payload

    with pytest.raises(StateFormatError, match="Connector deletion requires"):
        OperationAction(
            index=0,
            resource_id="streamt://payments/dev/connector/archive_orders",
            action="delete",
        )
    with pytest.raises(StateFormatError, match="another state address"):
        OperationControlState(
            address=StateAddress("local", "other", "dev"),
            status="in_progress",
            intent=connector_intent,
        )


def test_v2_control_requires_explicit_action_evidence_member() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=_intent(state),
        control_version=2,
    )
    payload = control.to_dict()

    assert payload["control_version"] == 2
    assert payload["intent"]["actions"][0]["gateway_evidence"] is None
    del payload["intent"]["actions"][0]["gateway_evidence"]
    with pytest.raises(StateFormatError, match="missing field"):
        OperationControlState.from_dict(payload, expected_address=address)


def test_v2_control_requires_and_roundtrips_gateway_mutation_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    base_intent = _intent(state)
    missing_evidence = replace(
        base_intent,
        actions=(
            OperationAction(
                index=0,
                resource_id="streamt://payments/dev/gateway_rule/orders_owner",
                action="update",
            ),
        ),
    )
    with pytest.raises(StateFormatError, match="require action evidence"):
        OperationControlState(
            address=address,
            status="in_progress",
            intent=missing_evidence,
            control_version=2,
        )

    intent = replace(base_intent, actions=(_gateway_action(),))
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
        control_version=2,
    )
    payload = control.to_dict()

    loaded = OperationControlState.from_dict(payload, expected_address=address)
    assert loaded == control
    assert loaded.to_dict() == payload
    assert loaded.intent is not None
    assert loaded.intent.actions[0].gateway_evidence == _gateway_evidence()

    with pytest.raises(StateFormatError, match="another state address"):
        OperationControlState(
            address=StateAddress("local", "other", "dev"),
            status="in_progress",
            intent=intent,
            control_version=2,
        )


def test_v2_control_requires_and_roundtrips_gateway_adopt_evidence() -> None:
    address = StateAddress("local", "payments", "dev")
    state = LocalState(project="payments", environment="dev")
    base_intent = replace(_intent(state), kind="adopt")
    missing_evidence = replace(
        base_intent,
        actions=(
            OperationAction(
                index=0,
                resource_id="streamt://payments/dev/gateway_rule/orders_owner",
                action="adopt",
            ),
        ),
    )

    with pytest.raises(StateFormatError, match="require action evidence"):
        OperationControlState(
            address=address,
            status="in_progress",
            intent=missing_evidence,
            control_version=2,
        )

    intent = replace(base_intent, actions=(_gateway_action(action="adopt"),))
    control = OperationControlState(
        address=address,
        status="in_progress",
        intent=intent,
        control_version=2,
    )

    assert OperationControlState.from_dict(
        control.to_dict(), expected_address=address
    ) == control


def _crash_after_mock_mutation(project_path: str, runtime_marker: str) -> None:
    service = make_deployment_state_service(
        Path(project_path),
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        control = operation.read_control()
        intent = _intent(operation.read().state)
        control = operation.begin_operation(control, intent)
        control = operation.record_progress(
            control,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        # This file write stands in for a runtime backend call that returned.
        Path(runtime_marker).write_text("mutated", encoding="utf-8")
        os._exit(23)


def test_control_lifecycle_is_strict_atomic_and_does_not_change_v1_state(
    tmp_path: Path,
) -> None:
    state_path = local_state_path(tmp_path, environment="dev")
    state = LocalState(project="payments", environment="dev")
    state.save(state_path)
    ownership_bytes = state_path.read_bytes()
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        initial = operation.read_control()
        operation.ensure_ready(initial)
        intent = _intent(state)
        active = operation.begin_operation(initial, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="completed",
                succeeded=True,
                recorded_at=operation_timestamp(),
            ),
        )
        cleared = operation.clear_operation(active)

    assert cleared.control == OperationControlState.clear(service.address)
    control_path = local_control_path(tmp_path, environment="dev")
    assert control_path.stat().st_mode & 0o777 == 0o600
    assert state_path.read_bytes() == ownership_bytes
    assert not list(control_path.parent.glob(f".{control_path.name}.*.tmp"))


def test_typed_snapshot_lifecycle_commits_state_before_clearing_control(
    tmp_path: Path,
) -> None:
    state_path = local_state_path(tmp_path, environment="dev")
    prior = LocalState(project="payments", environment="dev")
    prior.save(state_path)
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        assert isinstance(snapshot, OperationSnapshot)
        assert snapshot.address == service.address
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="completed",
                succeeded=True,
                recorded_at=operation_timestamp(),
            ),
        )
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        committed = operation.commit_operation(active, replacement)

    assert committed.state.state == replacement
    assert committed.control.control == OperationControlState.clear(service.address)
    assert LocalState.load(state_path) == replacement


def test_snapshot_begin_rejects_intent_or_state_revision_drift(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        mismatched = replace(
            _intent(snapshot.state.state),
            prior_state_serial=1,
        )
        with pytest.raises(StateBackendConflictError, match="prior state snapshot"):
            operation.begin_operation(snapshot, mismatched)
        checksum_mismatch = replace(
            _intent(snapshot.state.state),
            prior_state_checksum=state_checksum(
                LocalState(project="payments", environment="dev", serial=1)
            ),
        )
        with pytest.raises(StateBackendConflictError, match="prior state snapshot"):
            operation.begin_operation(snapshot, checksum_mismatch)

        changed = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        changed.save(local_state_path(tmp_path, environment="dev"))
        with pytest.raises(StateBackendConflictError, match="revision changed"):
            operation.begin_operation(snapshot, _intent(snapshot.state.state))

    assert service.read_control().control.status == "clear"


def test_commit_clear_failure_preserves_written_state_and_active_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = replace(_intent(snapshot.state.state), actions=())
        active = operation.begin_operation(snapshot, intent)
        backend = service.backend
        original_write = backend._write_control  # type: ignore[attr-defined]

        def fail_clear(
            path: Path,
            control: OperationControlState,
            *,
            operation_id: str | None,
        ) -> None:
            if control.status == "clear":
                raise StateBackendUnknownCommitError(
                    "local operation control state commit could not be confirmed",
                    operation_id=operation_id,
                )
            original_write(path, control, operation_id=operation_id)

        monkeypatch.setattr(backend, "_write_control", fail_clear)
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        with pytest.raises(StateBackendUnknownCommitError) as captured:
            operation.commit_operation(active, replacement)

        assert captured.value.operation_id == intent.operation_id
        assert operation.read().state == replacement
        assert operation.read_control().control.status == "in_progress"


def test_commit_rejects_incomplete_action_without_state_write_or_clear(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        replacement = LocalState(
            project="payments",
            environment="dev",
            serial=1,
        )
        with pytest.raises(StateBackendRecoveryRequiredError, match="incomplete"):
            operation.commit_operation(active, replacement)

        assert operation.read().revision.is_absent is True
        assert operation.read_control().control.status == "in_progress"


def test_clear_before_mutation_rejects_started_progress(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = _intent(snapshot.state.state)
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id="topic:orders",
                action="create",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        with pytest.raises(StateBackendRecoveryRequiredError, match="may have started"):
            operation.clear_before_mutation(active)

    assert service.read_control().control.status == "in_progress"


def test_clear_before_mutation_clears_intent_without_progress(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        active = operation.begin_operation(snapshot, _intent(snapshot.state.state))
        cleared = operation.clear_before_mutation(active)

    assert cleared.state == snapshot.state
    assert cleared.control.control.status == "clear"
    assert not local_state_path(tmp_path, environment="dev").exists()


def test_commit_without_ownership_change_clears_completed_empty_intent(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    with service.operation() as operation:
        snapshot = operation.observe()
        intent = replace(_intent(snapshot.state.state), actions=())
        active = operation.begin_operation(snapshot, intent)
        committed = operation.commit_operation(active, None)

    assert committed.state == snapshot.state
    assert committed.control.control.status == "clear"
    assert not local_state_path(tmp_path, environment="dev").exists()


def test_recovery_record_is_sanitized_and_blocks_successor(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        state = operation.read().state
        intent = _intent(state)
        active = operation.begin_operation(operation.read_control(), intent)
        recovery = operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_action_failed",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )

    payload = json.loads(
        local_control_path(tmp_path, environment="dev").read_text(encoding="utf-8")
    )
    assert recovery.safe_status() == {
        "status": "recovery_required",
        "operation_id": intent.operation_id,
        "kind": "apply",
        "failure_code": "runtime_action_failed",
        "last_completed_action_index": None,
    }
    assert "message" not in json.dumps(payload)
    with (
        service.operation() as successor,
        pytest.raises(StateBackendRecoveryRequiredError, match="explicit recovery"),
    ):
        successor.ensure_ready(successor.read_control())


def test_local_backend_persists_connector_evidence_through_recovery_boundary(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as operation:
        snapshot = operation.observe()
        intent = replace(
            _intent(snapshot.state.state),
            actions=(_connector_action(),),
        )
        active = operation.begin_operation(snapshot, intent)
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id=intent.actions[0].resource_id,
                action="delete",
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        recovery = operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_action_failed",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )

    raw = local_control_path(tmp_path, environment="dev").read_text(encoding="utf-8")
    payload = json.loads(raw)
    action_payload = payload["intent"]["actions"][0]
    assert payload["control_version"] == 3
    assert action_payload == _connector_action().to_dict(control_version=3)
    assert (
        OperationControlState.from_dict(
            payload,
            expected_address=service.address,
        )
        == recovery.control.control
    )
    for secret in (
        "connector-secret",
        "provider-secret",
        "https://connect.internal/api",
        "postgresql://owner:state-secret@db/state",
    ):
        assert secret not in raw


def test_local_backend_preserves_loaded_active_v2_shape_through_recovery(
    tmp_path: Path,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    state = LocalState(project="payments", environment="dev")
    control = OperationControlState(
        address=service.address,
        status="in_progress",
        intent=_intent(state),
        control_version=2,
    )
    original = control.to_dict()
    path = local_control_path(tmp_path, environment="dev")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(original), encoding="utf-8")

    with service.operation() as operation:
        active = operation.read_control()
        intent = active.control.intent
        assert intent is not None
        active = operation.record_progress(
            active,
            OperationProgress(
                operation_id=intent.operation_id,
                action_index=0,
                resource_id=intent.actions[0].resource_id,
                action=intent.actions[0].action,
                status="started",
                succeeded=None,
                recorded_at=operation_timestamp(),
            ),
        )
        operation.mark_recovery_required(
            active,
            RecoveryRecord(
                operation_id=intent.operation_id,
                failure_code="runtime_action_failed",
                failed_at=operation_timestamp(),
                last_completed_action_index=None,
            ),
        )

    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted["control_version"] == 2
    assert persisted["intent"] == original["intent"]
    assert set(persisted["intent"]["actions"][0]) == {
        "index",
        "resource_id",
        "action",
        "gateway_evidence",
    }
    assert persisted["intent"]["prior_state_checksum"] == original["intent"]["prior_state_checksum"]


def test_control_parser_rejects_unknown_and_duplicate_fields(tmp_path: Path) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    path = local_control_path(tmp_path, environment="dev")
    path.parent.mkdir(parents=True)
    clear = OperationControlState.clear(service.address).to_dict()
    clear["unexpected"] = True
    path.write_text(json.dumps(clear), encoding="utf-8")
    with pytest.raises(StateFormatError):
        service.read_control()

    path.write_text(
        '{"control_version":1,"control_version":1}',
        encoding="utf-8",
    )
    with pytest.raises(StateFormatError):
        service.read_control()


def test_atomic_control_failure_removes_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("token=do-not-leak")

    monkeypatch.setattr(os, "replace", fail_replace)
    with service.operation() as operation:
        state = operation.read().state
        with pytest.raises(
            StateBackendUnknownCommitError,
            match="commit could not be confirmed",
        ) as captured:
            operation.begin_operation(operation.read_control(), _intent(state))
    assert "do-not-leak" not in str(captured.value)
    control_path = local_control_path(tmp_path, environment="dev")
    assert not control_path.exists()
    assert not list(control_path.parent.glob(f".{control_path.name}.*.tmp"))


def test_cleanup_failure_cannot_mask_sanitized_unknown_commit_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )

    def fail_replace(_source: object, _target: object) -> None:
        raise OSError("authorization=replace-secret")

    def fail_unlink(_path: Path, *, missing_ok: bool = False) -> None:
        del missing_ok
        raise OSError("password=cleanup-secret /sensitive/path")

    monkeypatch.setattr(os, "replace", fail_replace)
    monkeypatch.setattr(Path, "unlink", fail_unlink)
    with service.operation() as operation:
        state = operation.read().state
        with pytest.raises(StateBackendUnknownCommitError) as captured:
            operation.begin_operation(operation.read_control(), _intent(state))

    assert str(captured.value) == (
        "local operation control state commit could not be confirmed"
    )


def test_process_crash_after_first_mock_mutation_leaves_blocking_marker(
    tmp_path: Path,
) -> None:
    runtime_marker = tmp_path / "runtime-mutated"
    process = multiprocessing.get_context("spawn").Process(
        target=_crash_after_mock_mutation,
        args=(str(tmp_path), str(runtime_marker)),
    )
    process.start()
    process.join(timeout=15)

    assert process.exitcode == 23
    assert runtime_marker.read_text(encoding="utf-8") == "mutated"
    service = make_deployment_state_service(
        tmp_path,
        project="payments",
        environment="dev",
        config=local_deployment_state_config(),
    )
    with service.operation() as successor:
        observation = successor.read_control()
        assert observation.control.status == "in_progress"
        assert observation.control.progress[-1].status == "started"
        with pytest.raises(StateBackendRecoveryRequiredError):
            successor.ensure_ready(observation)
