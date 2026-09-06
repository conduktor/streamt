"""Reviewed context and real local journal coordination with guarded fake runtime."""

from __future__ import annotations

import uuid
from dataclasses import replace
from types import SimpleNamespace

import pytest

from streamt.compiler import Compiler
from streamt.core.models import StreamtProject
from streamt.deployer.kafka_streams_replacement_coordinator import (
    KafkaStreamsReplacementCoordinator,
)
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    ReplacementExecutionState,
)
from streamt.deployer.plan_file import PlanFileError, ReviewedPlanFile, StateReference
from streamt.deployer.state import LocalState, StateError
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationResumeRecord,
    RecoveryRecord,
    StateBackendUnknownCommitError,
    operation_timestamp,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    IMAGE,
    OPERATION,
    RESOURCE,
    _boundaries,
    _desired_state,
    _evidence,
    _intent,
    _progress,
)
from tests.unit.test_kafka_streams_replacement_executor import World
from tests.unit.test_kafka_streams_resume_local import _backend
from tests.unit.test_kafka_streams_reviewed_plan import _actions, _plan


def project():
    return StreamtProject.model_validate({
        "project": {"name": "payments"}, "environment_name": "prod",
        "runtime": {"kafka": {"bootstrap_servers": "fixture:9092"}, "kafka_streams": {"image": IMAGE}},
        "sources": [{"name": "orders", "topic": "orders.input", "columns": [
            {"name": "id", "type": "STRING", "required": True}, {"name": "amount", "type": "BIGINT", "required": True},
        ]}],
        "models": [{"name": "filtered", "executor": "kafka_streams", "topic": {"name": "orders.output"},
                    "sql": 'SELECT id, amount FROM {{ source("orders") }} WHERE amount >= 75'}],
    })


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    backend = _backend(tmp_path)
    current = project()
    events = []
    world = World(events)
    coordinator = KafkaStreamsReplacementCoordinator(world.observer, lambda: current)

    def attach(operation):
        check = operation.check_lock

        def checked():
            check()
            events.append("lock")

        monkeypatch.setattr(operation, "check_lock", checked)
        return ReplacementExecutionState(operation.observe())

    with backend.operation(ADDRESS) as operation:
        initial = operation.observe()
    plan = ReviewedPlanFile.create(
        _plan(), Compiler(current).compile(dry_run=True), project="payments", environment="prod",
        runtime=current.runtime, state=StateReference.from_observation(initial.state), actions=_actions(),
    )
    return SimpleNamespace(backend=backend, project=current, events=events, world=world, coordinator=coordinator,
                           plan=plan, attach=attach, path=tmp_path)


def execute(fixture, operation, state, **changes):
    args = {"plan": fixture.plan, "current_plan": _plan(), "current_actions": _actions(), "operation_id": OPERATION, "actor": "operator"}
    args.update(changes)
    return fixture.coordinator.execute(operation, state, **args)


def resume(fixture, operation, state, **changes):
    args = {"plan": fixture.plan, "operation_id": OPERATION, "actor": "resuming-operator"}
    args.update(changes)
    return fixture.coordinator.resume(operation, state, **args)


def completed(fixture, operation, *, blocked=False):
    initial = operation.observe()
    intent = replace(_intent(initial.state.state), reviewed_plan_checksum=fixture.plan.checksum)
    active = operation.begin_operation(initial, intent)
    for boundary in _boundaries(exit_code=143):
        active = operation.record_progress(active, boundary)
    if blocked:
        active = operation.mark_recovery_required(active, RecoveryRecord(OPERATION, "after_completion", operation_timestamp(), 0))
    fixture.world.old = None
    fixture.world.candidate = "running"
    return ReplacementExecutionState(active)


def test_execute_binds_original_plan_then_commits_once_and_retains_completion_receipt(fixture):
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        result = execute(fixture, operation, state)
        assert result is state.snapshot
        assert result.control.control.status == "clear"
        assert result.state.state == _desired_state()
    history = fixture.backend._read_recovery_history(ADDRESS)
    completion = history.events[-1].record
    assert completion.control.intent.reviewed_plan_checksum == fixture.plan.checksum
    assert completion.control.intent.actions == fixture.plan.actions
    assert completion.control.progress[1].kafka_streams_checkpoint.exit_code == 143
    assert fixture.world.commands == ["term", "remove", "start"]
    assert len(fixture.world.creates) == 1


def test_new_lock_resumes_lost_create_with_original_tuple_and_no_second_create(fixture):
    fixture.world.failure = "create"
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        with pytest.raises(KafkaStreamsReplacementExecutionError):
            execute(fixture, operation, state)
        assert state.snapshot.control.control.status == "recovery_required"
        assert len(state.snapshot.control.control.progress) == 3
    fixture.world.failure = None
    reopened = LocalDeploymentStateBackend(fixture.path)
    with reopened.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        result = resume(fixture, operation, state)
    assert result.state.state == _desired_state()
    assert len(fixture.world.creates) == 1
    assert fixture.world.commands == ["term", "remove", "start"]
    completion = reopened._read_recovery_history(ADDRESS).events[-1].record
    assert completion.control.resume_history[0].recovery.failure_code == "runner_operation_interrupted"
    assert completion.control.intent.actions == fixture.plan.actions


def test_resume_reuses_previously_archived_authorization_without_changing_actor_or_uuid(fixture, monkeypatch):
    fixture.world.failure = "create"
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        with pytest.raises(KafkaStreamsReplacementExecutionError):
            execute(fixture, operation, state)
        first = OperationResumeRecord.create(state.snapshot, resume_id=str(uuid.uuid4()), actor="original-authorizer", resumed_at=operation_timestamp())
        with monkeypatch.context() as context:
            context.setattr(fixture.backend, "_write_control", lambda *_args, **_kw: (_ for _ in ()).throw(StateBackendUnknownCommitError("lost")))
            with pytest.raises(StateBackendUnknownCommitError):
                operation.resume_operation(state.snapshot, first)
    fixture.world.failure = None
    with LocalDeploymentStateBackend(fixture.path).operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        resume(fixture, operation, state)
    completion = fixture.backend._read_recovery_history(ADDRESS).events[-1].record
    assert completion.control.resume_history == (first,)


@pytest.mark.parametrize("change", ["sql", "endpoint", "environment", "checksum", "action_vs_manifest", "state"])
def test_changed_reviewed_context_rejects_before_intent_or_provider_reads(fixture, change):
    if change == "sql":
        fixture.project.models[0].sql += " AND amount < 100"
    elif change == "endpoint":
        fixture.project.runtime.kafka.bootstrap_servers = "other:9092"
    elif change == "environment":
        fixture.project.environment_name = "other"
    elif change == "checksum":
        fixture.plan = replace(fixture.plan, checksum="sha256:" + "0" * 64)
    elif change == "action_vs_manifest":
        fixture.project.models[0].sql += " AND amount < 100"
        fixture.plan = ReviewedPlanFile.create(
            _plan(), Compiler(fixture.project).compile(dry_run=True), project="payments", environment="prod",
            runtime=fixture.project.runtime, state=fixture.plan.state, actions=_actions(),
        )
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        if change == "state":
            state.snapshot.state.state.resources.pop(RESOURCE)
        with pytest.raises((StateError, PlanFileError, KafkaStreamsReplacementExecutionError)):
            execute(fixture, operation, state)
        assert operation.observe().control.control.status == "clear"
    assert fixture.world.read_count == 0
    assert not fixture.world.commands


@pytest.mark.parametrize("after_term", [False, True])
def test_project_edit_between_observation_and_transition_stops_before_next_write(fixture, after_term):
    def change():
        if not after_term or fixture.world.commands == ["term"]:
            fixture.project.models[0].sql += " AND amount < 100"

    fixture.world.after_observe = change
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        with pytest.raises(PlanFileError):
            execute(fixture, operation, state)
        control = operation.observe().control.control
        assert control.status == "recovery_required"
        assert len(control.progress) == int(after_term)
        assert all(item.status != "completed" for item in control.progress)
    assert fixture.world.commands == (["term"] if after_term else [])
    assert not fixture.world.creates


@pytest.mark.parametrize("blocked", [False, True])
def test_completed_journal_finalizes_without_replaying_runtime_or_authorizing_resume(fixture, blocked):
    with fixture.backend.operation(ADDRESS) as operation:
        fixture.attach(operation)
        completed(fixture, operation, blocked=blocked)
    with LocalDeploymentStateBackend(fixture.path).operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        result = resume(fixture, operation, state)
    assert result.state.state == _desired_state()
    assert not fixture.world.commands
    assert not fixture.world.creates
    receipt = fixture.backend._read_recovery_history(ADDRESS).events[-1].record
    assert not receipt.control.resume_history
    assert bool(receipt.control.recovery) == blocked


def test_local_written_result_is_only_cleared_after_reverse_proof_not_rewritten(fixture, monkeypatch):
    with fixture.backend.operation(ADDRESS) as operation:
        fixture.attach(operation)
        state = completed(fixture, operation)
        with monkeypatch.context() as context:
            original = fixture.backend._write_control

            def lose_clear(path, control, *, operation_id):
                if control.status == "clear":
                    raise StateBackendUnknownCommitError("clear unavailable")
                original(path, control, operation_id=operation_id)

            context.setattr(fixture.backend, "_write_control", lose_clear)
            with pytest.raises(StateBackendUnknownCommitError):
                operation.commit_operation(state.snapshot, _desired_state())
    with LocalDeploymentStateBackend(fixture.path).operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        assert state.snapshot.state.state == _desired_state()
        monkeypatch.setattr(LocalState, "_save_if_serial_locked", lambda *_a, **_k: pytest.fail("rewrote already-committed ownership"))
        result = resume(fixture, operation, state)
        assert result.state.state.serial == 2
    assert not fixture.world.commands


@pytest.mark.parametrize("after_clear", [False, True])
def test_uncertain_finalization_keeps_terminal_holder_and_never_marks_new_incident(fixture, monkeypatch, after_clear):
    with fixture.backend.operation(ADDRESS) as operation:
        fixture.attach(operation)
        state = completed(fixture, operation)
        terminal = state.snapshot
        real = operation.finalize_completed_runner
        error = StateBackendUnknownCommitError("lost finalization")

        def uncertain(snapshot):
            if after_clear:
                real(snapshot)
            raise error

        monkeypatch.setattr(operation, "finalize_completed_runner", uncertain)
        monkeypatch.setattr(operation, "mark_recovery_required", lambda *_a, **_k: pytest.fail("changed terminal incident after uncertain finalizer"))
        with pytest.raises(StateBackendUnknownCommitError) as caught:
            resume(fixture, operation, state)
        assert caught.value is error
        assert state.snapshot == terminal
        assert operation.observe().control.control.status == ("clear" if after_clear else "in_progress")


@pytest.mark.parametrize("change", ["operation", "plan", "sql", "members", "failed"])
def test_resume_rejects_foreign_original_tuple_context_or_unhealthy_completion(fixture, change):
    with fixture.backend.operation(ADDRESS) as operation:
        fixture.attach(operation)
        state = completed(fixture, operation)
        original = operation.observe()
        arguments = {}
        if change == "operation":
            arguments["operation_id"] = str(uuid.uuid4())
        elif change == "plan":
            arguments["plan"] = replace(fixture.plan, checksum="sha256:" + "0" * 64)
        elif change == "sql":
            fixture.project.models[0].sql += " AND amount < 100"
        elif change == "members":
            fixture.world.members = 2
        else:
            control = state.snapshot.control.control
            control = replace(control, progress=(*control.progress[:-1], replace(control.progress[-1], succeeded=False)))
            fixture.backend._write_control(fixture.backend._control_path(ADDRESS), control, operation_id=OPERATION)
            state.snapshot = operation.observe()
            original = state.snapshot
        with pytest.raises((StateError, PlanFileError, KafkaStreamsReplacementExecutionError)):
            resume(fixture, operation, state, **arguments)
        assert operation.observe() == original
    assert not fixture.world.commands
    assert not fixture.world.creates


@pytest.mark.parametrize("timeout", [0, -1, 601, True, float("nan"), float("inf")])
def test_invalid_timeout_cannot_write_an_intent(fixture, timeout):
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        with pytest.raises(KafkaStreamsReplacementExecutionError):
            execute(fixture, operation, state, timeout_seconds=timeout)
        assert operation.observe() == state.snapshot
    assert not fixture.world.read_count


def test_newer_planning_progress_does_not_replace_the_original_reviewed_tuple(fixture):
    newer = replace(_evidence(), progress=_progress(committed=11))
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        execute(fixture, operation, state, current_plan=_plan(newer), current_actions=_actions(newer))
    receipt = fixture.backend._read_recovery_history(ADDRESS).events[-1].record
    assert receipt.control.intent.actions == fixture.plan.actions
    assert receipt.control.intent.actions[0].kafka_streams_evidence.progress == _evidence().progress


@pytest.mark.parametrize("position", range(1, 6))
@pytest.mark.parametrize("after_write", [False, True])
def test_uncertain_progress_ack_never_invents_a_boundary_or_overwrites_newer_control(fixture, monkeypatch, position, after_write):
    error = StateBackendUnknownCommitError("uncertain progress")
    with fixture.backend.operation(ADDRESS) as operation:
        state = fixture.attach(operation)
        original = fixture.backend._write_control

        def uncertain(path, control, *, operation_id):
            if control.status == "in_progress" and len(control.progress) == position:
                if after_write:
                    original(path, control, operation_id=operation_id)
                raise error
            original(path, control, operation_id=operation_id)

        with monkeypatch.context() as context:
            context.setattr(fixture.backend, "_write_control", uncertain)
            with pytest.raises(StateBackendUnknownCommitError) as caught:
                execute(fixture, operation, state)
            assert caught.value is error
        current = operation.observe().control.control
        assert len(state.snapshot.control.control.progress) == position - 1
        assert len(current.progress) == position if after_write else len(current.progress) == position - 1
        assert current.status == ("in_progress" if after_write else "recovery_required")
        assert not any(item.succeeded is False for item in current.progress)
    if position == 5 and after_write:
        writes = list(fixture.world.commands)
        with LocalDeploymentStateBackend(fixture.path).operation(ADDRESS) as operation:
            state = fixture.attach(operation)
            resume(fixture, operation, state)
        assert fixture.world.commands == writes
