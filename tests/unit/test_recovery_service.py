"""Contract tests for provider-neutral recovery orchestration."""

from __future__ import annotations

import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import pytest

from streamt.deployer.connect import managed_connector_absence_fingerprint
from streamt.deployer.recovery import (
    RecoveryResolution,
    RecoveryResolutionRecord,
    RecoverySnapshotEvidence,
    RecoveryTargetEvidence,
)
from streamt.deployer.recovery_plan import RecoveryPlanError, RecoveryPlanFile
from streamt.deployer.recovery_service import (
    RecoveryLiveObservation,
    RecoveryProjectContext,
    RecoveryService,
    RecoveryServiceError,
)
from streamt.deployer.state import (
    LocalState,
    ManagedResourceRecord,
    artifact_checksum,
    resource_id,
)
from streamt.deployer.state_backend import (
    ConnectorActionEvidence,
    ConnectorActionSurfaceEvidence,
    ControlObservation,
    DeploymentStateBackend,
    DeploymentStateService,
    OperationAction,
    OperationControlState,
    OperationIntent,
    OperationProgress,
    OperationSnapshot,
    RecoveryRecord,
    StateAddress,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    state_checksum,
)

BLOCKED_OPERATION_ID = "00000000-0000-4000-8000-000000000101"
RECOVERY_OPERATION_ID = "00000000-0000-4000-8000-000000000102"
STORE_ID = "00000000-0000-4000-8000-000000000103"
ENVIRONMENT_FINGERPRINT = "sha256:" + "a" * 64
MANIFEST_CHECKSUM = "sha256:" + "b" * 64
TARGET_FINGERPRINT = "sha256:" + "c" * 64
RESOLVED_AT = "2026-09-02T18:00:00Z"
CONNECTOR_BACKEND = "kafka-connect:v1:primary:sha256:" + "d" * 64
CONNECTOR_NAME = "archive-orders-sink"
CONNECTOR_RESOURCE = resource_id(
    "payments",
    "prod",
    "connector",
    "archive_orders",
)


def _address(*, environment: str = "prod") -> StateAddress:
    return StateAddress(
        namespace="platform",
        project="payments",
        environment=environment,
    )


def _store(*, store_id: str = STORE_ID) -> StateStoreIdentity:
    return StateStoreIdentity(backend="postgres", store_id=store_id)


def _resource() -> str:
    return resource_id("payments", "prod", "topic", "payments_clean")


def _record(*, partitions: int) -> ManagedResourceRecord:
    return ManagedResourceRecord(
        physical_name="payments.clean.v1",
        ownership="managed",
        artifact_checksum=artifact_checksum(
            {"name": "payments.clean.v1", "partitions": partitions}
        ),
        backend="direct-kafka",
    )


def _state(*, serial: int = 1, partitions: int = 3) -> LocalState:
    return LocalState(
        project="payments",
        environment="prod",
        serial=serial,
        resources={_resource(): _record(partitions=partitions)},
    )


def _action() -> OperationAction:
    return OperationAction(index=0, resource_id=_resource(), action="update")


def _control(
    *,
    status: str = "in_progress",
    with_progress: bool = True,
) -> OperationControlState:
    prior = _state()
    action = _action()
    intent = OperationIntent(
        operation_id=BLOCKED_OPERATION_ID,
        kind="apply",
        started_at="2026-09-02T12:00:00Z",
        actor="operator",
        prior_state_serial=prior.serial,
        prior_state_checksum=state_checksum(prior),
        reviewed_plan_checksum=None,
        actions=(action,),
    )
    progress = (
        (
            OperationProgress(
                operation_id=BLOCKED_OPERATION_ID,
                action_index=0,
                resource_id=action.resource_id,
                action=action.action,
                status="started",
                succeeded=None,
                recorded_at="2026-09-02T12:01:00Z",
            ),
        )
        if with_progress
        else ()
    )
    recovery = (
        RecoveryRecord(
            operation_id=BLOCKED_OPERATION_ID,
            failure_code="unknown_commit",
            failed_at="2026-09-02T12:02:00Z",
            last_completed_action_index=None,
        )
        if status == "recovery_required"
        else None
    )
    return OperationControlState(
        address=_address(),
        status=cast(object, status),  # type: ignore[arg-type]
        intent=intent,
        progress=progress,
        recovery=recovery,
    )


def _snapshot(
    *,
    state: LocalState | None = None,
    control: OperationControlState | None = None,
    store: StateStoreIdentity | None = None,
    address: StateAddress | None = None,
    revision: str = "state-r1",
    control_revision: str = "control-r1",
) -> OperationSnapshot:
    state = state or _state()
    address = address or _address()
    return OperationSnapshot(
        state=StateObservation(
            store=store or _store(),
            address=address,
            state=state,
            revision=StateRevision(revision),
        ),
        control=ControlObservation(
            control=control or _control(),
            revision=StateRevision(control_revision),
        ),
    )


def _connector_snapshot(
    *,
    with_progress: bool = True,
    store_backend: str = "postgres",
) -> OperationSnapshot:
    prior_checksum = "sha256:" + "e" * 64
    prior = LocalState(
        project="payments",
        environment="prod",
        serial=1,
        resources={
            CONNECTOR_RESOURCE: ManagedResourceRecord(
                physical_name=CONNECTOR_NAME,
                ownership="managed",
                artifact_checksum=prior_checksum,
                backend=CONNECTOR_BACKEND,
            )
        },
    )
    action = OperationAction(
        index=0,
        resource_id=CONNECTOR_RESOURCE,
        action="delete",
        connector_evidence=ConnectorActionEvidence(
            version=1,
            backend_identity=CONNECTOR_BACKEND,
            connector_name=CONNECTOR_NAME,
            prior_artifact_checksum=prior_checksum,
            current=ConnectorActionSurfaceEvidence(
                exists=True,
                fingerprint="sha256:" + "f" * 64,
            ),
            desired=ConnectorActionSurfaceEvidence(
                exists=False,
                fingerprint=managed_connector_absence_fingerprint(
                    CONNECTOR_BACKEND,
                    CONNECTOR_NAME,
                ),
            ),
        ),
    )
    control = OperationControlState(
        address=_address(),
        status="in_progress",
        intent=OperationIntent(
            operation_id=BLOCKED_OPERATION_ID,
            kind="apply",
            started_at="2026-09-02T12:00:00Z",
            actor="operator",
            prior_state_serial=prior.serial,
            prior_state_checksum=state_checksum(prior),
            reviewed_plan_checksum=None,
            actions=(action,),
        ),
        progress=(
            OperationProgress(
                operation_id=BLOCKED_OPERATION_ID,
                action_index=0,
                resource_id=action.resource_id,
                action=action.action,
                status="started",
                succeeded=None,
                recorded_at="2026-09-02T12:01:00Z",
            ),
        )
        if with_progress
        else (),
    )
    return _snapshot(
        state=prior,
        control=control,
        store=StateStoreIdentity(backend=store_backend, store_id=STORE_ID),
    )


def _connector_live(
    snapshot: OperationSnapshot,
    resolution: RecoveryResolution,
) -> RecoveryLiveObservation:
    intent = snapshot.control.control.intent
    assert intent is not None
    action = intent.actions[0]
    evidence = action.connector_evidence
    assert evidence is not None
    if resolution == "observed":
        return RecoveryLiveObservation(
            targets=(
                RecoveryTargetEvidence(
                    action=action,
                    presence="absent",
                    accepted_as="candidate",
                    fingerprint=evidence.desired.fingerprint,
                ),
            ),
            candidate_state=LocalState(
                project="payments",
                environment="prod",
                serial=snapshot.state.state.serial + 1,
            ),
        )
    return RecoveryLiveObservation(
        targets=(
            RecoveryTargetEvidence(
                action=action,
                presence="present",
                accepted_as="prior",
                fingerprint=evidence.current.fingerprint,
            ),
        ),
        candidate_state=None,
    )


def _target(*, accepted_as: str) -> RecoveryTargetEvidence:
    return RecoveryTargetEvidence(
        action=_action(),
        presence="present",
        accepted_as=cast(object, accepted_as),  # type: ignore[arg-type]
        fingerprint=TARGET_FINGERPRINT,
    )


def _live(resolution: RecoveryResolution) -> RecoveryLiveObservation:
    if resolution == "observed":
        return RecoveryLiveObservation(
            targets=(_target(accepted_as="candidate"),),
            candidate_state=_state(serial=2, partitions=6),
        )
    if resolution == "rolled_back":
        return RecoveryLiveObservation(
            targets=(_target(accepted_as="prior"),),
            candidate_state=None,
        )
    return RecoveryLiveObservation(targets=(), candidate_state=None)


def _plan(
    resolution: RecoveryResolution = "observed",
    *,
    snapshot: OperationSnapshot | None = None,
) -> RecoveryPlanFile:
    active = snapshot or _snapshot(
        control=_control(with_progress=resolution != "abandoned_before_mutation")
    )
    live = _live(resolution)
    return RecoveryPlanFile.create(
        resolution=resolution,
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=RecoverySnapshotEvidence.from_operation_snapshot(active),
        targets=live.targets,
        candidate_state=live.candidate_state,
        environment_fingerprint=(
            None if resolution == "abandoned_before_mutation" else ENVIRONMENT_FINGERPRINT
        ),
        manifest_checksum=(
            None if resolution == "abandoned_before_mutation" else MANIFEST_CHECKSUM
        ),
    )


class _ContextReader:
    def __init__(
        self,
        log: list[str],
        *,
        context: RecoveryProjectContext | None = None,
        contexts: list[RecoveryProjectContext] | None = None,
        error: Exception | None = None,
    ) -> None:
        self.log = log
        self.context = context or RecoveryProjectContext(
            environment_fingerprint=ENVIRONMENT_FINGERPRINT,
            manifest_checksum=MANIFEST_CHECKSUM,
        )
        self.contexts = list(contexts or [])
        self.error = error

    def read_recovery_context(self) -> RecoveryProjectContext:
        self.log.append("context")
        if self.error is not None:
            raise self.error
        if self.contexts:
            return self.contexts.pop(0)
        return self.context


class _Observer:
    def __init__(
        self,
        log: list[str],
        live: RecoveryLiveObservation,
        *,
        error: Exception | None = None,
    ) -> None:
        self.log = log
        self.live = live
        self.error = error
        self.calls: list[tuple[RecoveryResolution, RecoverySnapshotEvidence]] = []

    def observe_recovery_targets(
        self,
        *,
        resolution: RecoveryResolution,
        snapshot: RecoverySnapshotEvidence,
    ) -> RecoveryLiveObservation:
        self.log.append("targets")
        self.calls.append((resolution, snapshot))
        if self.error is not None:
            raise self.error
        return self.live


class _FakeOperation:
    def __init__(self, backend: _FakeBackend) -> None:
        self.backend = backend

    def observe(self) -> OperationSnapshot:
        self.backend.log.append("observe")
        if self.backend.observations:
            return self.backend.observations.pop(0)
        return self.backend.current

    def check_lock(self) -> None:
        self.backend.log.append("check_lock")

    def finalize_recovery(
        self,
        observation: OperationSnapshot,
        evidence: RecoverySnapshotEvidence,
        resolution: RecoveryResolutionRecord,
        replacement: LocalState | None,
    ) -> OperationSnapshot:
        self.backend.log.append("finalize")
        self.backend.finalize_calls.append((observation, evidence, resolution, replacement))
        if self.backend.finalize_error is not None:
            raise self.backend.finalize_error
        if self.backend.final_result is not None:
            return self.backend.final_result
        expected = replacement or evidence.state
        result = _snapshot(
            state=expected,
            control=OperationControlState.clear(self.backend.address),
            store=self.backend.store,
            revision="state-result",
        )
        self.backend.current = result
        return result


class _FakeBackend:
    def __init__(self, current: OperationSnapshot) -> None:
        self.current = current
        self.store = current.state.store
        self.address = current.address
        self.log: list[str] = []
        self.observations: list[OperationSnapshot] = []
        self.finalize_calls: list[
            tuple[
                OperationSnapshot,
                RecoverySnapshotEvidence,
                RecoveryResolutionRecord,
                LocalState | None,
            ]
        ] = []
        self.final_result: OperationSnapshot | None = None
        self.finalize_error: Exception | None = None

    def describe(self) -> StateStoreIdentity:
        return self.store

    def read(self, address: StateAddress) -> StateObservation:
        assert address == self.address
        return self.current.state

    def read_control(self, address: StateAddress) -> ControlObservation:
        assert address == self.address
        return self.current.control

    @contextmanager
    def operation(self, address: StateAddress) -> Iterator[_FakeOperation]:
        assert address == self.address
        self.log.append("enter")
        try:
            yield _FakeOperation(self)
        finally:
            self.log.append("exit")


def _service(backend: _FakeBackend) -> RecoveryService:
    state = DeploymentStateService(
        backend=cast(DeploymentStateBackend, backend),
        address=backend.address,
    )
    return RecoveryService(
        state=state,
        operation_id_factory=lambda: RECOVERY_OPERATION_ID,
        resolved_at_factory=lambda: RESOLVED_AT,
    )


def _execute(
    service: RecoveryService,
    plan: RecoveryPlanFile,
    *,
    observer: _Observer | None = None,
    context_reader: _ContextReader | None = None,
) -> OperationSnapshot:
    return service.execute_plan(
        plan,
        confirm_operation_id=plan.blocked_operation_id,
        confirm_resolution=plan.resolution,
        confirm_evidence_checksum=plan.evidence_checksum,
        observer=observer,
        context_reader=context_reader,
    )


def test_create_observed_plan_keeps_lock_through_atomic_save(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = _FakeBackend(_snapshot())
    service = _service(backend)
    observer = _Observer(backend.log, _live("observed"))
    context = _ContextReader(backend.log)
    original_save = RecoveryPlanFile.save

    def logged_save(plan: RecoveryPlanFile, path: Path) -> None:
        backend.log.append("save")
        original_save(plan, path)

    monkeypatch.setattr(RecoveryPlanFile, "save", logged_save)

    plan = service.create_plan(
        resolution="observed",
        destination=tmp_path / "recovery.json",
        observer=observer,
        context_reader=context,
    )

    assert backend.log == [
        "enter",
        "observe",
        "context",
        "targets",
        "context",
        "observe",
        "check_lock",
        "save",
        "check_lock",
        "exit",
    ]
    assert RecoveryPlanFile.load(tmp_path / "recovery.json") == plan
    assert observer.calls == [("observed", plan.snapshot)]


def test_create_abandoned_plan_does_not_observe_runtime_or_context(tmp_path: Path) -> None:
    backend = _FakeBackend(_snapshot(control=_control(with_progress=False)))
    service = _service(backend)
    observer = _Observer(
        backend.log,
        _live("abandoned_before_mutation"),
        error=AssertionError("must not observe"),
    )
    context = _ContextReader(backend.log, error=AssertionError("must not read"))

    plan = service.create_plan(
        resolution="abandoned_before_mutation",
        destination=tmp_path / "abandoned.json",
        observer=observer,
        context_reader=context,
    )

    assert plan.targets == ()
    assert plan.candidate_state is None
    assert "targets" not in backend.log
    assert "context" not in backend.log


@pytest.mark.parametrize("resolution", ["observed", "rolled_back"])
def test_create_live_connector_recovery_uses_exact_observer_evidence(
    tmp_path: Path,
    resolution: RecoveryResolution,
) -> None:
    snapshot = _connector_snapshot()
    backend = _FakeBackend(snapshot)
    observer = _Observer(
        backend.log,
        _connector_live(snapshot, resolution),
    )
    destination = tmp_path / f"{resolution}.json"

    plan = _service(backend).create_plan(
        resolution=resolution,
        destination=destination,
        observer=observer,
        context_reader=_ContextReader(backend.log),
    )

    assert destination.exists()
    assert plan.targets == _connector_live(snapshot, resolution).targets
    assert observer.calls == [(resolution, plan.snapshot)]


@pytest.mark.parametrize("resolution", ["observed", "rolled_back"])
def test_create_live_connector_recovery_rejects_local_authority_before_reads(
    tmp_path: Path,
    resolution: RecoveryResolution,
) -> None:
    snapshot = _connector_snapshot(store_backend="local")
    backend = _FakeBackend(snapshot)
    observer = _Observer(
        backend.log,
        _connector_live(snapshot, resolution),
        error=AssertionError("must not observe Connector recovery"),
    )
    context = _ContextReader(
        backend.log,
        error=AssertionError("must not read Connector recovery context"),
    )

    with pytest.raises(RecoveryServiceError, match="PostgreSQL state authority"):
        _service(backend).create_plan(
            resolution=resolution,
            destination=tmp_path / f"{resolution}.json",
            observer=observer,
            context_reader=context,
        )

    assert backend.log == ["enter", "observe", "exit"]
    assert observer.calls == []
    assert not (tmp_path / f"{resolution}.json").exists()


@pytest.mark.parametrize("status", ["in_progress", "recovery_required"])
def test_create_plan_accepts_both_active_control_states(
    tmp_path: Path,
    status: str,
) -> None:
    backend = _FakeBackend(_snapshot(control=_control(status=status)))

    plan = _service(backend).create_plan(
        resolution="rolled_back",
        destination=tmp_path / f"{status}.json",
        observer=_Observer(backend.log, _live("rolled_back")),
        context_reader=_ContextReader(backend.log),
    )

    assert plan.snapshot.control.status == status


def test_create_plan_rejects_clear_control_without_observing_targets(tmp_path: Path) -> None:
    backend = _FakeBackend(_snapshot(control=OperationControlState.clear(_address())))
    observer = _Observer(backend.log, _live("observed"))

    with pytest.raises(RecoveryServiceError, match="active blocked operation"):
        _service(backend).create_plan(
            resolution="observed",
            destination=tmp_path / "clear.json",
            observer=observer,
            context_reader=_ContextReader(backend.log),
        )

    assert observer.calls == []
    assert backend.log == ["enter", "observe", "exit"]


def test_create_abandoned_plan_rejects_any_started_progress(tmp_path: Path) -> None:
    backend = _FakeBackend(_snapshot(control=_control(with_progress=True)))

    with pytest.raises(RecoveryPlanError, match="empty durable progress"):
        _service(backend).create_plan(
            resolution="abandoned_before_mutation",
            destination=tmp_path / "unsafe.json",
        )

    assert not (tmp_path / "unsafe.json").exists()


def test_create_plan_rejects_state_or_control_change_before_save(tmp_path: Path) -> None:
    first = _snapshot()
    changed = _snapshot(revision="state-r2")
    backend = _FakeBackend(first)
    backend.observations = [first, changed]

    with pytest.raises(RecoveryServiceError, match="changed while"):
        _service(backend).create_plan(
            resolution="rolled_back",
            destination=tmp_path / "stale.json",
            observer=_Observer(backend.log, _live("rolled_back")),
            context_reader=_ContextReader(backend.log),
        )

    assert "check_lock" not in backend.log
    assert not (tmp_path / "stale.json").exists()


def test_create_plan_rejects_context_change_during_target_observation(
    tmp_path: Path,
) -> None:
    backend = _FakeBackend(_snapshot())
    original = RecoveryProjectContext(
        environment_fingerprint=ENVIRONMENT_FINGERPRINT,
        manifest_checksum=MANIFEST_CHECKSUM,
    )
    changed = RecoveryProjectContext(
        environment_fingerprint=ENVIRONMENT_FINGERPRINT,
        manifest_checksum="sha256:" + "d" * 64,
    )

    with pytest.raises(RecoveryServiceError, match="changed while"):
        _service(backend).create_plan(
            resolution="observed",
            destination=tmp_path / "context-drift.json",
            observer=_Observer(backend.log, _live("observed")),
            context_reader=_ContextReader(
                backend.log,
                contexts=[original, changed],
            ),
        )

    assert backend.log == [
        "enter",
        "observe",
        "context",
        "targets",
        "context",
        "exit",
    ]
    assert not (tmp_path / "context-drift.json").exists()


@pytest.mark.parametrize("source", ["observer", "context"])
def test_create_plan_sanitizes_collaborator_exceptions(
    tmp_path: Path,
    source: str,
) -> None:
    backend = _FakeBackend(_snapshot())
    secret = "postgresql://owner:provider-secret@db.internal/state"
    observer = _Observer(
        backend.log,
        _live("observed"),
        error=RuntimeError(secret) if source == "observer" else None,
    )
    context = _ContextReader(
        backend.log,
        error=RuntimeError(secret) if source == "context" else None,
    )

    with pytest.raises(RecoveryServiceError) as raised:
        _service(backend).create_plan(
            resolution="observed",
            destination=tmp_path / "error.json",
            observer=observer,
            context_reader=context,
        )

    assert secret not in str(raised.value)
    assert "provider-secret" not in str(raised.value)
    rendered = "".join(traceback.format_exception(raised.type, raised.value, raised.tb))
    assert secret not in rendered
    assert "provider-secret" not in rendered


@pytest.mark.parametrize(
    "resolution",
    ["observed", "rolled_back", "abandoned_before_mutation"],
)
def test_execute_all_recovery_outcomes_and_exact_finalize_arguments(
    resolution: RecoveryResolution,
) -> None:
    plan = _plan(resolution)
    backend = _FakeBackend(
        _snapshot(control=_control(with_progress=resolution != "abandoned_before_mutation"))
    )
    observer = None if not plan.targets else _Observer(backend.log, _live(resolution))
    context = None if plan.environment_fingerprint is None else _ContextReader(backend.log)

    result = _execute(
        _service(backend),
        plan,
        observer=observer,
        context_reader=context,
    )

    assert result.control.control.status == "clear"
    assert backend.log[-1] == "exit"
    assert backend.log[-3:] == ["finalize", "check_lock", "exit"]
    assert len(backend.finalize_calls) == 1
    current, evidence, record, replacement = backend.finalize_calls[0]
    assert current.control.control.status == "in_progress"
    assert evidence == plan.snapshot
    assert record == plan.make_resolution_record(resolved_at=RESOLVED_AT)
    assert replacement == (plan.candidate_state if resolution == "observed" else None)


@pytest.mark.parametrize(
    ("field", "wrong", "message"),
    [
        ("operation", RECOVERY_OPERATION_ID, "operation confirmation"),
        ("resolution", "rolled_back", "Resolution confirmation"),
        ("checksum", "sha256:" + "f" * 64, "checksum confirmation"),
    ],
)
def test_execute_rejects_wrong_exact_confirmation_before_lock(
    field: str,
    wrong: str,
    message: str,
) -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    values = {
        "confirm_operation_id": plan.blocked_operation_id,
        "confirm_resolution": plan.resolution,
        "confirm_evidence_checksum": plan.evidence_checksum,
    }
    values[
        {
            "operation": "confirm_operation_id",
            "resolution": "confirm_resolution",
            "checksum": "confirm_evidence_checksum",
        }[field]
    ] = wrong

    with pytest.raises(RecoveryServiceError, match=message):
        _service(backend).execute_plan(plan, **values)  # type: ignore[arg-type]

    assert backend.log == []


def test_execute_loads_and_validates_plan_file(tmp_path: Path) -> None:
    plan = _plan("rolled_back")
    path = tmp_path / "recovery.json"
    plan.save(path)
    backend = _FakeBackend(_snapshot())

    _service(backend).execute_plan(
        path,
        confirm_operation_id=plan.blocked_operation_id,
        confirm_resolution=plan.resolution,
        confirm_evidence_checksum=plan.evidence_checksum,
        observer=_Observer(backend.log, _live("rolled_back")),
        context_reader=_ContextReader(backend.log),
    )

    assert len(backend.finalize_calls) == 1


def test_execute_rejects_unchecksummed_plan_object_before_lock() -> None:
    valid = _plan()
    unchecked = RecoveryPlanFile(
        resolution=valid.resolution,
        blocked_operation_id=valid.blocked_operation_id,
        recovery_operation_id=valid.recovery_operation_id,
        snapshot=valid.snapshot,
        targets=valid.targets,
        candidate_state=valid.candidate_state,
        environment_fingerprint=valid.environment_fingerprint,
        manifest_checksum=valid.manifest_checksum,
    )
    backend = _FakeBackend(_snapshot())

    with pytest.raises(RecoveryPlanError, match="integrity evidence"):
        _service(backend).execute_plan(
            unchecked,
            confirm_operation_id=unchecked.blocked_operation_id,
            confirm_resolution=unchecked.resolution,
            confirm_evidence_checksum="",
        )

    assert backend.log == []


def test_execute_rejects_other_store_or_address_before_lock() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    backend.store = _store(store_id="00000000-0000-4000-8000-000000000999")

    with pytest.raises(RecoveryServiceError, match="configured store and address"):
        _execute(_service(backend), plan)

    assert backend.log == []


def test_execute_rejects_stale_project_context_inside_lock() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    context = _ContextReader(
        backend.log,
        context=RecoveryProjectContext(
            environment_fingerprint="sha256:" + "d" * 64,
            manifest_checksum=MANIFEST_CHECKSUM,
        ),
    )

    with pytest.raises(RecoveryServiceError, match="Project inputs changed"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(backend.log, _live("observed")),
            context_reader=context,
        )

    assert backend.log == ["enter", "observe", "context", "exit"]


def test_execute_rejects_stale_target_evidence_and_releases_lock() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    stale = RecoveryLiveObservation(
        targets=(
            RecoveryTargetEvidence(
                action=_action(),
                presence="present",
                accepted_as="candidate",
                fingerprint="sha256:" + "d" * 64,
            ),
        ),
        candidate_state=plan.candidate_state,
    )

    with pytest.raises(RecoveryServiceError, match="Live targets changed"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(backend.log, stale),
            context_reader=_ContextReader(backend.log),
        )

    assert backend.log == [
        "enter",
        "observe",
        "context",
        "targets",
        "context",
        "exit",
    ]
    assert backend.finalize_calls == []


def test_execute_rejects_stale_candidate_state_even_when_target_hashes_match() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    stale = RecoveryLiveObservation(
        targets=plan.targets,
        candidate_state=_state(serial=2, partitions=9),
    )

    with pytest.raises(RecoveryServiceError, match="Live targets changed"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(backend.log, stale),
            context_reader=_ContextReader(backend.log),
        )

    assert backend.finalize_calls == []


def test_execute_rejects_context_change_during_target_observation() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())
    original = RecoveryProjectContext(
        environment_fingerprint=ENVIRONMENT_FINGERPRINT,
        manifest_checksum=MANIFEST_CHECKSUM,
    )
    changed = RecoveryProjectContext(
        environment_fingerprint="sha256:" + "d" * 64,
        manifest_checksum=MANIFEST_CHECKSUM,
    )

    with pytest.raises(RecoveryServiceError, match="changed while"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(backend.log, _live("observed")),
            context_reader=_ContextReader(
                backend.log,
                contexts=[original, changed],
            ),
        )

    assert backend.log == [
        "enter",
        "observe",
        "context",
        "targets",
        "context",
        "exit",
    ]
    assert backend.finalize_calls == []


def test_execute_rejects_missing_or_ambiguous_observer() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot())

    with pytest.raises(RecoveryServiceError, match="fresh live target evidence"):
        _execute(
            _service(backend),
            plan,
            context_reader=_ContextReader(backend.log),
        )

    class AmbiguousObserver:
        def observe_recovery_targets(self, **_kwargs: object) -> object:
            return (plan.targets, plan.candidate_state)

    with pytest.raises(RecoveryServiceError, match="ambiguous"):
        _execute(
            _service(backend),
            plan,
            observer=cast(_Observer, AmbiguousObserver()),
            context_reader=_ContextReader(backend.log),
        )

    assert backend.finalize_calls == []


def test_execute_sanitizes_observer_and_context_exceptions() -> None:
    plan = _plan()
    secret = "password=provider-secret"
    for source in ("observer", "context"):
        backend = _FakeBackend(_snapshot())
        observer = _Observer(
            backend.log,
            _live("observed"),
            error=RuntimeError(secret) if source == "observer" else None,
        )
        context = _ContextReader(
            backend.log,
            error=RuntimeError(secret) if source == "context" else None,
        )

        with pytest.raises(RecoveryServiceError) as raised:
            _execute(
                _service(backend),
                plan,
                observer=observer,
                context_reader=context,
            )

        assert secret not in str(raised.value)
        assert "provider-secret" not in str(raised.value)
        rendered = "".join(traceback.format_exception(raised.type, raised.value, raised.tb))
        assert secret not in rendered
        assert "provider-secret" not in rendered
        assert backend.finalize_calls == []


def test_execute_abandoned_never_calls_supplied_observer_or_context() -> None:
    plan = _plan("abandoned_before_mutation")
    backend = _FakeBackend(_snapshot(control=_control(with_progress=False)))
    observer = _Observer(
        backend.log,
        _live("abandoned_before_mutation"),
        error=AssertionError("must not observe"),
    )
    context = _ContextReader(backend.log, error=AssertionError("must not read"))

    _execute(
        _service(backend),
        plan,
        observer=observer,
        context_reader=context,
    )

    assert "targets" not in backend.log
    assert "context" not in backend.log


@pytest.mark.parametrize("resolution", ["observed", "rolled_back"])
def test_execute_live_connector_recovery_revalidates_and_finalizes(
    resolution: RecoveryResolution,
) -> None:
    snapshot = _connector_snapshot()
    live = _connector_live(snapshot, resolution)
    plan = RecoveryPlanFile.create(
        resolution=resolution,
        recovery_operation_id=RECOVERY_OPERATION_ID,
        snapshot=RecoverySnapshotEvidence.from_operation_snapshot(snapshot),
        targets=live.targets,
        candidate_state=live.candidate_state,
        environment_fingerprint=ENVIRONMENT_FINGERPRINT,
        manifest_checksum=MANIFEST_CHECKSUM,
    )
    backend = _FakeBackend(snapshot)
    result = _execute(
        _service(backend),
        plan,
        observer=_Observer(backend.log, live),
        context_reader=_ContextReader(backend.log),
    )

    expected_state = live.candidate_state or snapshot.state.state
    assert result.state.state == expected_state
    assert result.control.control.status == "clear"
    assert backend.finalize_calls[0][3] == live.candidate_state


def test_execute_rejects_mutated_in_memory_candidate_checksum_before_state_access() -> None:
    plan = _plan()
    candidate = cast(LocalState, plan.candidate_state)
    candidate.resources[_resource()] = _record(partitions=9)
    backend = _FakeBackend(_snapshot())

    with pytest.raises(RecoveryPlanError, match="checksum"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(
                backend.log,
                RecoveryLiveObservation(
                    targets=plan.targets,
                    candidate_state=candidate,
                ),
            ),
            context_reader=_ContextReader(backend.log),
        )

    assert backend.log == []
    assert backend.finalize_calls == []


def test_execute_allows_blocked_partial_finalization_at_exact_candidate_state() -> None:
    plan = _plan()
    candidate = cast(LocalState, plan.candidate_state)
    backend = _FakeBackend(_snapshot(state=candidate, revision="state-after-intent"))
    observer = _Observer(backend.log, _live("observed"))
    context = _ContextReader(backend.log)

    result = _execute(
        _service(backend),
        plan,
        observer=observer,
        context_reader=context,
    )

    assert result.control.control.status == "clear"
    assert observer.calls == [("observed", plan.snapshot)]
    assert backend.finalize_calls[0][0].state.state == candidate


def test_execute_rejects_unreviewed_blocked_state_drift_before_live_reads() -> None:
    plan = _plan()
    backend = _FakeBackend(_snapshot(state=_state(serial=2, partitions=9)))
    observer = _Observer(backend.log, _live("observed"))
    context = _ContextReader(backend.log)

    with pytest.raises(RecoveryServiceError, match="changed after recovery evidence"):
        _execute(
            _service(backend),
            plan,
            observer=observer,
            context_reader=context,
        )

    assert backend.log == ["enter", "observe", "exit"]
    assert observer.calls == []
    assert backend.finalize_calls == []


def test_execute_rejects_blocked_control_drift_before_live_reads() -> None:
    plan = _plan()
    backend = _FakeBackend(
        _snapshot(control=_control(status="recovery_required"))
    )
    observer = _Observer(backend.log, _live("observed"))
    context = _ContextReader(backend.log)

    with pytest.raises(RecoveryServiceError, match="changed after recovery evidence"):
        _execute(
            _service(backend),
            plan,
            observer=observer,
            context_reader=context,
        )

    assert backend.log == ["enter", "observe", "exit"]
    assert observer.calls == []
    assert backend.finalize_calls == []


def test_execute_accepts_revision_only_drift() -> None:
    plan = _plan("rolled_back")
    backend = _FakeBackend(
        _snapshot(
            revision="state-new-revision",
            control_revision="control-new-revision",
        )
    )

    result = _execute(
        _service(backend),
        plan,
        observer=_Observer(backend.log, _live("rolled_back")),
        context_reader=_ContextReader(backend.log),
    )

    assert result.control.control.status == "clear"
    assert len(backend.finalize_calls) == 1


def test_execute_allows_backend_to_verify_an_already_completed_clear_plan() -> None:
    plan = _plan()
    candidate = cast(LocalState, plan.candidate_state)
    backend = _FakeBackend(
        _snapshot(
            state=candidate,
            control=OperationControlState.clear(_address()),
            revision="state-complete",
        )
    )
    observer = _Observer(
        backend.log,
        _live("observed"),
        error=AssertionError("must not observe completed recovery targets"),
    )
    context = _ContextReader(
        backend.log,
        error=AssertionError("must not read completed recovery context"),
    )

    result = _execute(
        _service(backend),
        plan,
        observer=observer,
        context_reader=context,
    )

    assert result.control.control.status == "clear"
    assert backend.finalize_calls[0][0].control.control.status == "clear"
    assert observer.calls == []
    assert "context" not in backend.log
    assert "targets" not in backend.log


def test_execute_verifies_backend_result_before_releasing_lock() -> None:
    plan = _plan("rolled_back")
    backend = _FakeBackend(_snapshot())
    backend.final_result = _snapshot(
        state=_state(serial=2, partitions=6),
        control=OperationControlState.clear(_address()),
    )

    with pytest.raises(RecoveryServiceError, match="reviewed ownership state"):
        _execute(
            _service(backend),
            plan,
            observer=_Observer(backend.log, _live("rolled_back")),
            context_reader=_ContextReader(backend.log),
        )

    assert backend.log[-2:] == ["finalize", "exit"]


def test_execute_releases_lock_before_returning_success() -> None:
    plan = _plan("rolled_back")
    backend = _FakeBackend(_snapshot())

    result = _execute(
        _service(backend),
        plan,
        observer=_Observer(backend.log, _live("rolled_back")),
        context_reader=_ContextReader(backend.log),
    )

    assert result.control.control.status == "clear"
    assert backend.log[-1] == "exit"
