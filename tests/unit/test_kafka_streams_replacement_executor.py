"""Fault every journal/provider frontier without starting Docker or Kafka."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_docker import LocalDockerRunner
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    KafkaStreamsReplacementExecutor,
    ReplacementExecutionState,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.state_backend import (
    ControlObservation,
    OperationAction,
    OperationSnapshot,
    StateBackendConflictError,
    StateBackendLockLostError,
    StateBackendRecoveryRequiredError,
    StateBackendUnknownCommitError,
    StateObservation,
    StateRevision,
    StateStoreIdentity,
    _control_revision,
    state_checksum,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    ADDRESS,
    APP,
    BACKEND,
    IMAGE,
    NETWORK,
    NEW_ID,
    OLD_ID,
    OPERATION,
    RESOURCE,
    TOKEN,
    _boundaries,
    _control,
    _evidence,
    _intent,
    _local,
    _postgres,
    _state,
)
from tests.unit.test_kafka_streams_replacement import _container, _observation, _recovering

SECRET = "private-password-never-render"


class MemoryOperation:
    def __init__(self, events, prefix=0):
        self.events = events
        protected = _state()
        control = _control(_boundaries(exit_code=143)[:prefix])
        self.snapshot = OperationSnapshot(
            StateObservation(StateStoreIdentity("local", TOKEN), ADDRESS, protected, StateRevision(state_checksum(protected))),
            ControlObservation(control, _control_revision(control)),
        )
        self.fail_record = None
        self.commit_then_fail = False
        self.fail_lock_number = None
        self.locks = 0
        self.lock_error = StateBackendLockLostError("lock lost", operation_id=OPERATION)
        self.write_error = StateBackendUnknownCommitError("uncertain journal acknowledgement", operation_id=OPERATION)

    def check_lock(self):
        self.events.append("lock")
        self.locks += 1
        if self.locks == self.fail_lock_number:
            raise self.lock_error

    def observe(self):
        self.events.append("state_read")
        return self.snapshot

    def record_progress(self, snapshot, progress):
        assert type(snapshot) is OperationSnapshot
        assert snapshot == self.snapshot
        assert self.events[-1] == "lock"
        position = len(snapshot.control.control.progress)
        self.events.append("record_" + str(position))
        if self.fail_record == position and not self.commit_then_fail:
            raise self.write_error
        # Real control validation enforces action ownership, phase order and
        # strict checkpoint evidence, not a permissive dictionary journal.
        control = replace(snapshot.control.control, progress=(*snapshot.control.control.progress, progress))
        self.snapshot = replace(snapshot, control=ControlObservation(control, _control_revision(control)))
        if self.fail_record == position:
            raise self.write_error
        return self.snapshot


class World:
    def __init__(self, events, *, prefix=0):
        self.events = events
        self.old = "running" if prefix < 2 else "exited" if prefix == 2 else None
        self.candidate = "created" if prefix == 4 else "running" if prefix == 5 else None
        self.failure = None
        self.close_code = 143
        self.members = None
        self.term_stays_ready = False
        self.start_stays_created = False
        self.start_exits = False
        self.start_missing_reads = 0
        self.read_count = 0
        self.after_observe = None
        self.commands = []
        self.creates = []
        self.runtime = object.__new__(KafkaStreamsDeployer)
        self.runtime.docker = MagicMock(spec=LocalDockerRunner)
        self.runtime.docker.owned_command.side_effect = self.command
        self.runtime.docker.create.side_effect = self.create
        self.runtime._private_inputs = MagicMock(side_effect=self.inputs)
        self.runtime.progress = MagicMock()
        self.observer = object.__new__(KafkaStreamsReplacementObserver)
        self.observer.deployer = self.runtime
        self.observer.observe = MagicMock(side_effect=self.observe)

    def observe(self, evidence, record):
        self.events.append("provider_read")
        self.read_count += 1
        assert evidence == _evidence()
        assert record == _state().resources[RESOURCE]
        if self.candidate == "running" and self.start_missing_reads:
            self.start_missing_reads -= 1
            raise ValueError(SECRET)
        observed = _observation(
            old=self.old, candidate=self.candidate, members=self.members,
            prior_container=_container(state=self.old, exit_code=self.close_code) if self.old == "exited"
            else _container() if self.old else None,
        )
        if self.after_observe is not None:
            self.after_observe()
        return observed

    def inputs(self, artifact):
        assert self.events[-1] == "lock"
        assert artifact.to_dict() == _evidence().desired_artifact.to_dict()
        self.events.append("inputs")
        if self.failure == "inputs":
            raise OSError(SECRET)
        return Path("/private/plan.json"), Path("/private/client.properties")

    def command(self, command, application_id, backend, *, expected_id):
        assert self.events[-1] == "lock"
        assert (application_id, backend) == (APP, BACKEND)
        assert command in {"term", "remove", "start"}
        assert expected_id == (NEW_ID if command == "start" else OLD_ID)
        self.events.append(command)
        self.commands.append(command)
        if command == "term" and not self.term_stays_ready:
            self.old = "exited"
        elif command == "remove":
            assert self.old == "exited"
            self.old = None
        elif command == "start" and not self.start_stays_created:
            self.candidate = "exited" if self.start_exits else "running"
        if self.failure == command:
            # Deliberately fail after changing the provider: acknowledgement
            # loss must not produce automatic cleanup, retry or a checkpoint.
            raise ValueError(SECRET)

    def create(self, **kwargs):
        assert self.events[-1] == "lock"
        assert self.old is None
        assert self.candidate is None
        self.events.append("create")
        self.creates.append(kwargs)
        self.candidate = "created"
        if self.failure == "create":
            raise ValueError(SECRET)
        return NEW_ID


@pytest.fixture
def clock(monkeypatch):
    elapsed = [0.0]
    monkeypatch.setattr("streamt.deployer.kafka_streams_replacement_executor.time.monotonic", lambda: elapsed[0])
    monkeypatch.setattr("streamt.deployer.kafka_streams_replacement_executor.time.sleep", lambda delay: elapsed.__setitem__(0, elapsed[0] + delay))
    return elapsed


def fixture(prefix=0):
    events = []
    operation = MemoryOperation(events, prefix)
    state = ReplacementExecutionState(operation.snapshot)
    world = World(events, prefix=prefix)
    executor = KafkaStreamsReplacementExecutor(world.observer)
    return operation, state, world, executor, events


def run(operation, state, executor, *, mode="execute", **kwargs):
    return executor.run(operation, state, operation_id=OPERATION, mode=mode, timeout_seconds=1, poll_seconds=0.1, **kwargs)


def test_complete_ordered_replacement_preserves_evidence_and_leaves_commit_to_caller(clock):
    operation, state, world, executor, events = fixture()
    original = state.snapshot.control.control.intent
    result = run(operation, state, executor)
    assert result is state.snapshot
    assert result.control.control.intent == original
    assert result.control.control.status == "in_progress"
    assert result.control.control.actions_completed
    assert result.state.state == _state()
    assert result.control.control.progress[1].kafka_streams_checkpoint.exit_code == 143
    assert [event for event in events if event not in {"lock", "state_read", "provider_read"}] == [
        "record_0", "term", "record_1", "remove", "record_2", "inputs", "create", "record_3", "start", "record_4",
    ]
    creation = world.creates[0]
    evidence = _evidence()
    assert creation == {
        "application_id": APP, "image_id": IMAGE, "network": NETWORK,
        "plan_file": Path("/private/plan.json"), "properties_file": Path("/private/client.properties"),
        "state_volume": evidence.volume.name, "artifact_hash": evidence.desired_artifact.checksum,
        "plan_hash": evidence.desired_artifact.plan_hash, "backend": BACKEND,
        "input_topic_id": evidence.progress.input_topic_id, "output_topic_id": evidence.progress.output_topic_id,
        "cluster_id": evidence.progress.cluster_id, "expected_volume": evidence.volume,
        "generation": creation["generation"],
    }
    assert creation["generation"].operation_id == OPERATION
    assert creation["generation"].action_index == 0
    assert creation["generation"].evidence_fingerprint == evidence.immutable_fingerprint
    world.runtime.docker.ensure_state_volume.assert_not_called()
    world.runtime.progress.initialize.assert_not_called()


@pytest.mark.parametrize("backend", ["local", "postgres"])
def test_driver_uses_full_snapshots_with_existing_state_provider_protocol(backend, tmp_path, monkeypatch, clock):
    def execute(locked):
        active = locked.begin_operation(locked.observe(), _intent())
        events = []
        world = World(events)

        class TracedOperation:
            def check_lock(self):
                events.append("lock")
                locked.check_lock()

            def observe(self):
                events.append("state_read")
                return locked.observe()

            def record_progress(self, snapshot, progress):
                assert type(snapshot) is OperationSnapshot
                assert events[-1] == "lock"
                events.append("record_" + str(len(snapshot.control.control.progress)))
                return locked.record_progress(snapshot, progress)

        state = ReplacementExecutionState(active)
        result = run(TracedOperation(), state, KafkaStreamsReplacementExecutor(world.observer))
        durable = locked.observe()
        assert durable == result
        assert durable.control.control.status == "in_progress"
        assert durable.control.control.actions_completed
        assert durable.control.control.progress[1].kafka_streams_checkpoint.exit_code == 143
        assert durable.state.state == _state()
        assert world.commands == ["term", "remove", "start"]

    if backend == "local":
        with _local(tmp_path).operation() as locked:
            execute(locked)
    else:
        locked, _database, _owner, _driver = _postgres(monkeypatch)
        execute(locked)


@pytest.mark.parametrize("prefix", range(6))
@pytest.mark.parametrize("mode", ["execute", "resume"])
def test_every_existing_durable_frontier_reuses_original_intent(prefix, mode, clock):
    operation, state, world, executor, _events = fixture(prefix)
    original = state.snapshot.control.control.intent
    result = run(operation, state, executor, mode=mode)
    assert result.control.control.actions_completed
    assert result.control.control.intent == original
    assert world.commands.count("term") == (1 if prefix < 2 else 0)
    assert world.commands.count("remove") == (1 if prefix < 3 else 0)
    assert len(world.creates) == (1 if prefix < 4 else 0)
    assert world.commands.count("start") == (1 if prefix < 5 else 0)


@pytest.mark.parametrize("position", range(5))
@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_journal_write_keeps_last_ack_and_never_invents_failure(position, committed, clock):
    operation, state, world, executor, events = fixture()
    operation.fail_record, operation.commit_then_fail = position, committed
    with pytest.raises(StateBackendUnknownCommitError) as error:
        run(operation, state, executor)
    assert error.value is operation.write_error
    assert len(state.snapshot.control.control.progress) == position
    assert len(operation.snapshot.control.control.progress) == position + int(committed)
    assert all(item.succeeded is not False for item in state.snapshot.control.control.progress)
    assert events[-1] == "record_" + str(position)
    assert world.commands == ["term", "remove", "start"][:0 if position == 0 else 1 if position == 1 else 2 if position < 4 else 3]


@pytest.mark.parametrize("position", range(5))
@pytest.mark.parametrize("committed", [False, True])
def test_explicit_resume_after_journal_response_loss_uses_actual_durable_boundary(position, committed, clock):
    operation, state, world, executor, _events = fixture()
    operation.fail_record, operation.commit_then_fail = position, committed
    with pytest.raises(StateBackendUnknownCommitError):
        run(operation, state, executor)
    operation.fail_record = None
    # A new invocation must freshly observe actual durable state; it cannot
    # speculate that a failed acknowledgement did or did not persist.
    resumed = ReplacementExecutionState(operation.observe())
    result = run(operation, resumed, executor, mode="resume")
    assert result.control.control.actions_completed
    assert world.commands == ["term", "remove", "start"]
    assert len(world.creates) == 1
    assert result.control.control.intent == _intent()


@pytest.mark.parametrize("bad_ack", ["control_only", "unchanged", "rebased_intent"])
def test_invalid_record_acknowledgement_never_advances_holder(bad_ack, clock):
    operation, state, world, executor, _events = fixture()
    original = operation.record_progress
    initial = state.snapshot

    def invalid(snapshot, progress):
        written = original(snapshot, progress)
        if bad_ack == "control_only":
            return written.control
        if bad_ack == "unchanged":
            return snapshot
        control = replace(written.control.control, intent=replace(written.control.control.intent, actor="changed"))
        return replace(written, control=ControlObservation(control, _control_revision(control)))

    operation.record_progress = invalid
    with pytest.raises(StateBackendUnknownCommitError):
        run(operation, state, executor)
    assert state.snapshot is initial
    assert not world.commands
    assert len(operation.snapshot.control.control.progress) == 1


@pytest.mark.parametrize(("failure", "position"), [("term", 1), ("remove", 2), ("inputs", 3), ("create", 3), ("start", 4)])
def test_unknown_runtime_write_stops_without_retry_cleanup_or_synthetic_checkpoint(failure, position, clock):
    operation, state, world, executor, events = fixture()
    world.failure = failure
    with pytest.raises(KafkaStreamsReplacementExecutionError) as error:
        run(operation, state, executor)
    assert SECRET not in str(error.value)
    assert len(state.snapshot.control.control.progress) == position
    assert state.snapshot == operation.snapshot
    assert events[-1] == failure
    assert world.commands.count("term") == 1
    assert world.commands.count("start") <= 1
    assert len(world.creates) <= 1
    assert all(item.succeeded is not False for item in state.snapshot.control.control.progress)


@pytest.mark.parametrize("failure", ["term", "remove", "create", "start"])
def test_separate_explicit_resume_reobserves_unknown_write_without_repeating_it(failure, clock):
    operation, state, world, executor, _events = fixture()
    world.failure = failure
    with pytest.raises(KafkaStreamsReplacementExecutionError):
        run(operation, state, executor)
    world.failure = None
    resumed = ReplacementExecutionState(operation.observe())
    result = run(operation, resumed, executor, mode="resume")
    assert result.control.control.actions_completed
    assert world.commands == ["term", "remove", "start"]
    assert len(world.creates) == 1
    assert result.control.control.intent == _intent()


def test_each_lock_failure_propagates_unchanged_and_stops_before_any_following_action(clock):
    operation, state, _world, executor, _events = fixture()
    run(operation, state, executor)
    for index in range(1, operation.locks + 1):
        faulty, cursor, _provider, driver, events = fixture()
        faulty.fail_lock_number = index
        with pytest.raises(StateBackendLockLostError) as error:
            run(faulty, cursor, driver)
        assert error.value is faulty.lock_error
        assert events[-1] == "lock"
        assert cursor.snapshot == faulty.snapshot


@pytest.mark.parametrize("missing_reads", [1, 3, 100])
def test_missing_startup_status_only_retries_reads_until_ready_or_deadline(missing_reads, clock):
    operation, state, world, executor, _events = fixture()
    world.start_missing_reads = missing_reads
    if missing_reads == 100:
        with pytest.raises(KafkaStreamsReplacementExecutionError, match="deadline"):
            run(operation, state, executor)
        assert len(state.snapshot.control.control.progress) == 4
    else:
        assert run(operation, state, executor).control.control.actions_completed
    assert world.commands == ["term", "remove", "start"]
    assert len(world.creates) == 1


@pytest.mark.parametrize("missing_reads", [1, 3, 100])
def test_explicit_resume_waits_for_already_started_candidate_using_only_reads(missing_reads, clock):
    operation, state, world, executor, _events = fixture(4)
    world.candidate = "running"
    world.start_missing_reads = missing_reads
    if missing_reads == 100:
        with pytest.raises(KafkaStreamsReplacementExecutionError, match="deadline"):
            run(operation, state, executor, mode="resume")
        assert len(state.snapshot.control.control.progress) == 4
    else:
        assert run(operation, state, executor, mode="resume").control.control.actions_completed
    assert not world.commands
    assert not world.creates
    world.runtime._private_inputs.assert_not_called()


@pytest.mark.parametrize(("flag", "position"), [("term_stays_ready", 1), ("start_stays_created", 4)])
def test_acknowledged_term_or_start_is_not_resent_for_repeated_observation(flag, position, clock):
    operation, state, world, executor, _events = fixture()
    setattr(world, flag, True)
    with pytest.raises(KafkaStreamsReplacementExecutionError, match="deadline"):
        run(operation, state, executor)
    assert len(state.snapshot.control.control.progress) == position
    assert world.commands.count("term") == 1
    assert world.commands.count("start") <= 1


@pytest.mark.parametrize("mode", ["execute", "resume"])
def test_candidate_exit_after_our_start_never_triggers_auto_restart(mode, clock):
    operation, state, world, executor, _events = fixture()
    world.start_exits = True
    with pytest.raises(KafkaStreamsReplacementExecutionError):
        run(operation, state, executor, mode=mode)
    assert world.commands == ["term", "remove", "start"]
    assert len(state.snapshot.control.control.progress) == 4


@pytest.mark.parametrize("mode", ["execute", "resume"])
def test_previously_cleanly_stopped_exact_candidate_only_restarts_on_explicit_resume(mode, clock):
    operation, state, world, executor, _events = fixture(4)
    world.candidate = "exited"
    if mode == "execute":
        with pytest.raises(KafkaStreamsReplacementExecutionError):
            run(operation, state, executor, mode=mode)
        assert not world.commands
    else:
        assert run(operation, state, executor, mode=mode).control.control.actions_completed
        assert world.commands == ["start"]
    assert not world.creates
    world.runtime._private_inputs.assert_not_called()
    world.runtime.progress.initialize.assert_not_called()


@pytest.mark.parametrize("exit_code", [130, 137, None])
def test_unsafe_old_close_never_removes_or_forces_container(exit_code, clock):
    operation, state, world, executor, _events = fixture()
    world.close_code = exit_code
    with pytest.raises(KafkaStreamsReplacementExecutionError):
        run(operation, state, executor)
    assert world.commands == ["term"]
    assert len(state.snapshot.control.control.progress) == 1
    assert not world.creates


@pytest.mark.parametrize("mode", ["execute", "resume"])
def test_recovery_required_never_gains_resume_authority_locally(mode, clock):
    operation, state, world, executor, events = fixture()
    control = _recovering(1)
    operation.snapshot = replace(operation.snapshot, control=ControlObservation(control, _control_revision(control)))
    state.snapshot = operation.snapshot
    with pytest.raises(StateBackendRecoveryRequiredError):
        run(operation, state, executor, mode=mode)
    assert not events
    world.observer.observe.assert_not_called()


@pytest.mark.parametrize("change", ["control_revision", "store", "state_revision", "state_content"])
def test_snapshot_changes_abort_before_any_provider_observation(change, clock):
    operation, state, world, executor, _events = fixture()
    if change == "control_revision":
        operation.snapshot = replace(operation.snapshot, control=replace(operation.snapshot.control, revision=StateRevision("changed")))
    elif change == "store":
        operation.snapshot = replace(operation.snapshot, state=replace(operation.snapshot.state, store=StateStoreIdentity("local", OPERATION)))
    elif change == "state_revision":
        operation.snapshot = replace(operation.snapshot, state=replace(operation.snapshot.state, revision=StateRevision("changed")))
    else:
        changed = _state()
        changed.serial += 1
        operation.snapshot = replace(operation.snapshot, state=replace(operation.snapshot.state, state=changed))
    with pytest.raises(StateBackendConflictError):
        run(operation, state, executor)
    world.observer.observe.assert_not_called()


def test_snapshot_change_during_provider_read_blocks_next_write(clock):
    operation, state, world, executor, events = fixture(1)
    world.after_observe = lambda: setattr(operation, "snapshot", replace(operation.snapshot, control=replace(operation.snapshot.control, revision=StateRevision("changed"))))
    with pytest.raises(StateBackendConflictError):
        run(operation, state, executor)
    assert not world.commands
    assert "provider_read" in events


@pytest.mark.parametrize("case", ["wrong_operation", "mixed_actions", "topic_action", "bad_ownership", "terminal_failure"])
def test_invalid_scope_or_ownership_never_mutates(case, clock):
    operation, state, world, executor, _events = fixture()
    if case == "wrong_operation":
        with pytest.raises(KafkaStreamsReplacementExecutionError):
            executor.run(operation, state, operation_id=TOKEN, mode="execute")
        return
    if case == "mixed_actions":
        control = replace(_control(), intent=replace(_intent(), actions=(*_intent().actions, OperationAction(1, "topic:other", "create"))))
    elif case == "topic_action":
        control = replace(_control(), intent=replace(_intent(), actions=(OperationAction(0, "topic:orders", "update"),)))
    elif case == "terminal_failure":
        control = _control((_boundaries()[0], replace(_boundaries()[-1], succeeded=False)))
    else:
        protected = _state()
        protected.resources[RESOURCE] = replace(protected.resources[RESOURCE], backend="other")
        operation.snapshot = replace(operation.snapshot, state=replace(operation.snapshot.state, state=protected))
        control = replace(_control(), intent=replace(_intent(), prior_state_checksum=state_checksum(protected)))
    operation.snapshot = replace(operation.snapshot, control=ControlObservation(control, _control_revision(control)))
    state.snapshot = operation.snapshot
    with pytest.raises((KafkaStreamsReplacementExecutionError, StateBackendConflictError)):
        run(operation, state, executor)
    assert not world.commands
    assert not world.creates


@pytest.mark.parametrize(("name", "value"), [
    ("mode", "recover"), ("mode", True), ("timeout_seconds", 0), ("timeout_seconds", 601),
    ("timeout_seconds", float("inf")), ("timeout_seconds", float("nan")), ("timeout_seconds", True),
    ("timeout_seconds", 10**1000),
    ("poll_seconds", 0), ("poll_seconds", 11), ("poll_seconds", True),
])
def test_invalid_poll_bounds_or_mode_fail_before_io(name, value):
    operation, state, world, executor, events = fixture()
    options = {"mode": "execute", "timeout_seconds": 1, "poll_seconds": 0.1, name: value}
    with pytest.raises(KafkaStreamsReplacementExecutionError):
        executor.run(operation, state, operation_id=OPERATION, **options)
    assert not events
    world.observer.observe.assert_not_called()


@pytest.mark.parametrize("failure", [ValueError(SECRET), RuntimeError(SECRET)])
def test_initial_unknown_observation_is_not_assumed_absent_or_retried(failure, clock):
    operation, state, world, executor, _events = fixture()
    world.observer.observe.side_effect = failure
    with pytest.raises(KafkaStreamsReplacementExecutionError) as error:
        run(operation, state, executor)
    assert SECRET not in str(error.value)
    assert world.observer.observe.call_count == 1
    assert not world.commands
    assert not state.snapshot.control.control.progress


def test_created_id_ack_must_match_observed_exact_candidate_before_checkpoint(clock):
    operation, state, world, executor, _events = fixture()
    create = world.create

    def changed_ack(**kwargs):
        create(**kwargs)
        return "f" * 64

    world.runtime.docker.create.side_effect = changed_ack
    with pytest.raises(KafkaStreamsReplacementExecutionError, match="identity changed"):
        run(operation, state, executor)
    assert len(state.snapshot.control.control.progress) == 3
    assert world.commands == ["term", "remove"]
    assert len(world.creates) == 1
