"""Real local durable resume across two fresh OS worker processes.

The first worker loses one real Docker-create acknowledgement, durably records
an interruption, releases its lock and exits. A new worker reads that pending
operation, archives explicit resume authority, resumes the exact candidate and
commits ownership. The reviewed checksum is synthetic test-internal evidence,
not validation of a public reviewed-plan or public CLI replacement workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import runpy
import subprocess
import sys
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from confluent_kafka import __version__ as kafka_client_version
from confluent_kafka import libversion

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_evidence import KafkaStreamsActionEvidence
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    KafkaStreamsReplacementExecutor,
    ReplacementExecutionState,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.state import LocalState, artifact_checksum
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationAction,
    OperationIntent,
    OperationResumeRecord,
    RecoveryRecord,
    StateAddress,
    operation_timestamp,
    state_checksum,
)


def helpers():
    return runpy.run_path(str(Path(__file__).with_name("kafka_streams_replacement_executor_probe.py")))


def open_runtime(directory):
    project = ProjectParser(directory).parse()
    runtime = KafkaStreamsDeployer(project.runtime.kafka_streams, project.runtime.kafka, state_dir=directory / ".streamt")
    backend = LocalDeploymentStateBackend(directory)
    address = StateAddress("local", project.project.name, "default")
    return project, runtime, backend, address


@contextmanager
def resume_provider_guard(runtime, operation, candidate_id, proof):
    raw_run = runtime.docker._run
    writes = proof["docker_mutations"] = []
    denied = proof["forbidden_write_attempts"] = []
    reads = {("container", "ls"), ("container", "inspect"), ("container", "cp"),
             ("volume", "ls"), ("volume", "inspect"), ("network", "inspect"),
             ("image", "inspect"), ("context", "inspect")}

    def forbidden(*args, **kwargs):
        denied.append("unexpected_provider_write")
        raise AssertionError("Resume attempted volume, create or offset mutation")

    def run(args, **kwargs):
        if tuple(args[:2]) in reads or args[0] == "info":
            return raw_run(args, **kwargs)
        operation.check_lock()
        assert args == ["container", "start", candidate_id]
        assert not writes, "Resume repeated its start"
        writes.append(args)
        return raw_run(args, **kwargs)

    with ExitStack() as stack:
        stack.enter_context(patch.object(runtime.docker, "_run", run))
        for name in ("create", "ensure_state_volume"):
            stack.enter_context(patch.object(runtime.docker, name, forbidden))
        stack.enter_context(patch.object(runtime.progress, "initialize", forbidden))
        for name in ("alter_consumer_group_offsets", "create_topics", "delete_topics", "alter_configs",
                     "incremental_alter_configs", "create_partitions", "delete_consumer_groups"):
            stack.enter_context(patch.object(runtime.progress.admin, name, forbidden))
        yield


def worker_interrupt(root):
    directory = root / "fresh"
    project, runtime, backend, address = open_runtime(directory)
    changed = project.model_copy(deep=True)
    model = next(item for item in changed.models if item.name == "eligible_orders")
    assert "amount >= 100" in model.sql
    model.sql = model.sql.replace("amount >= 100", "amount >= 200", 1)
    desired = parse_compiled_kafka_streams_job_artifact(Compiler(changed).compile(dry_run=True).artifacts["kafka_streams_jobs"][0])
    observer = KafkaStreamsReplacementObserver(runtime)
    proof = {"worker_pid": os.getpid(), "operation_id": str(uuid.uuid4()), "phase": "interrupt"}
    try:
        with backend.operation(address) as operation:
            initial = operation.observe()
            operation.ensure_ready(initial)
            matches = [(name, record) for name, record in initial.state.state.resources.items()
                       if record.physical_name == desired.application_id]
            assert len(matches) == 1
            resource_id, owner = matches[0]
            prepared = observer.prepare(desired, owner)
            assert prepared.progress.partitions[0].committed == 5
            synthetic_checksum = artifact_checksum({"test_internal_resume_fixture": proof["operation_id"], "prepared": prepared.to_dict()})
            intent = OperationIntent(
                proof["operation_id"], "apply", operation_timestamp(), "resume-probe-worker-one",
                initial.state.state_serial, state_checksum(initial.state.state), synthetic_checksum,
                (OperationAction(0, resource_id, "update", kafka_streams_evidence=prepared),),
            )
            state = ReplacementExecutionState(operation.begin_operation(initial, intent))
            with helpers()["replacement_guard"](runtime, operation, prepared, proof):
                try:
                    KafkaStreamsReplacementExecutor(observer).run(operation, state, operation_id=intent.operation_id,
                                                                 mode="execute", timeout_seconds=120)
                except KafkaStreamsReplacementExecutionError:
                    assert proof.get("injected_create_ack_loss") is True
                else:
                    raise AssertionError("Lost-create response injection did not interrupt the driver")
                assert len(state.snapshot.control.control.progress) == 3
                assert state.snapshot.control.control.progress[-1].kafka_streams_checkpoint.phase == "old_removed"
                assert state.snapshot.control.control.progress[1].kafka_streams_checkpoint.exit_code == 143
                interruption = RecoveryRecord(intent.operation_id, "runner_create_ack_lost", operation_timestamp(), None)
                blocked = operation.mark_recovery_required(state.snapshot, interruption)
                candidate = observer.observe(prepared, owner)
                assert candidate.prior_container is None
                assert candidate.candidate_container.process_state == "created"
                assert candidate.candidate_container.container_id == proof["created_container_ids"][0]
                assert candidate.progress.active_members == 0
                assert candidate.progress.partitions[0].committed == 5
                assert candidate.volume == prepared.volume
                assert blocked.state == initial.state
                assert blocked.control.control.status == "recovery_required"
                assert blocked.control.control.control_version == 4
                assert not backend._read_recovery_history(address).events
                proof.update({
                    "prepared": prepared.to_dict(), "interrupted_control": blocked.control.control.to_dict(),
                    "interrupted_state": blocked.state.state.to_dict(), "state_store": {"backend": blocked.state.store.backend, "store_id": blocked.state.store.store_id},
                    "synthetic_test_internal_reviewed_checksum": synthetic_checksum,
                    "candidate_id": candidate.candidate_container.container_id,
                    "candidate_never_started": True, "prior_container_absent": True,
                    "real_local_lock_held_for_interruption_record": True,
                })
        proof["lock_context_exited_before_worker_exit"] = True
    finally:
        runtime.close()
    (root / "worker-interrupt.json").write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


def worker_resume(root):
    directory = root / "fresh"
    _project, runtime, backend, address = open_runtime(directory)
    prior = json.loads((root / "worker-interrupt.json").read_text())
    observer = KafkaStreamsReplacementObserver(runtime)
    proof = {"worker_pid": os.getpid(), "phase": "resume"}
    assert proof["worker_pid"] != prior["worker_pid"]
    try:
        with backend.operation(address) as operation:
            blocked = operation.observe()
            assert blocked.control.control.to_dict() == prior["interrupted_control"]
            assert blocked.state.state.to_dict() == prior["interrupted_state"]
            assert blocked.control.control.status == "recovery_required"
            intent = blocked.control.control.intent
            assert intent.reviewed_plan_checksum == prior["synthetic_test_internal_reviewed_checksum"]
            evidence = intent.actions[0].kafka_streams_evidence
            assert evidence.to_dict() == prior["prepared"]
            resource_id = intent.actions[0].resource_id
            owner = blocked.state.state.resources[resource_id]
            observed = observer.observe(evidence, owner)
            assert observed.prior_container is None
            assert observed.candidate_container.container_id == prior["candidate_id"]
            assert observed.candidate_container.process_state == "created"
            assert observed.progress.partitions[0].committed == 5
            assert observed.progress.active_members == 0
            assert observed.volume == evidence.volume
            record = OperationResumeRecord.create(blocked, resume_id=str(uuid.uuid4()), actor="resume-probe-worker-two", resumed_at=operation_timestamp())
            with resume_provider_guard(runtime, operation, prior["candidate_id"], proof):
                resumed = operation.resume_operation(blocked, record)
                assert resumed.control.control.control_version == 5
                assert resumed.control.control.resume_history == (record,)
                assert resumed.control.control.intent == intent
                assert resumed.control.control.progress == blocked.control.control.progress
                assert resumed.state == blocked.state
                assert backend._read_recovery_history(address).resumes_for(intent.operation_id) == (record,)
                state = ReplacementExecutionState(resumed)
                completed = KafkaStreamsReplacementExecutor(observer).run(
                    operation, state, operation_id=intent.operation_id, mode="resume", timeout_seconds=120,
                )
                assert completed.control.control.actions_completed
                assert completed.control.control.resume_history == (record,)
                assert completed.state == blocked.state
                resources = dict(blocked.state.state.resources)
                resources[resource_id] = replace(owner, artifact_checksum=evidence.desired_artifact.checksum)
                desired_state = LocalState(project=blocked.state.state.project, environment=blocked.state.state.environment,
                                           serial=blocked.state.state_serial + 1, resources=resources)
                final = operation.commit_operation(completed, desired_state)
                assert final.control.control.status == "clear"
                assert final.state.state == desired_state
                history = backend._read_recovery_history(address)
                assert history.history_version == 2
                assert history.resumes_for(intent.operation_id) == (record,)
                assert history.events[0].record.recovery.to_dict() == prior["interrupted_control"]["recovery"]
                proof.update({
                    "fresh_process_and_reacquired_lock": True, "resume_record": record.to_dict(),
                    "completed_control_before_commit": completed.control.control.to_dict(),
                    "final_control": final.control.control.to_dict(), "final_state": final.state.state.to_dict(),
                    "resume_history_after_clear": history.to_dict(),
                    "candidate_id": prior["candidate_id"], "original_intent_and_offsets_retained": True,
                })
        proof["lock_context_exited_before_worker_exit"] = True
    finally:
        runtime.close()
    (root / "worker-resume.json").write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


def prove_process_resume(journey):
    workers = []
    for phase in ("interrupt", "resume"):
        command = [sys.executable, *(["-I"] if journey.mode == "installed" else []), str(Path(__file__).resolve()),
                   "--worker", phase, "--fixture-root", str(journey.root)]
        try:
            result = journey.run(command, env=journey.cli_environment, timeout=180)
        except subprocess.CalledProcessError as error:
            (journey.root / f"worker-{phase}.stdout").write_text(error.stdout or "")
            (journey.root / f"worker-{phase}.stderr").write_text(error.stderr or "")
            raise
        (journey.root / f"worker-{phase}.stdout").write_text(result.stdout)
        (journey.root / f"worker-{phase}.stderr").write_text(result.stderr)
        proof = json.loads((journey.root / f"worker-{phase}.json").read_text())
        assert proof["worker_pid"] != os.getpid()
        assert proof["lock_context_exited_before_worker_exit"]
        workers.append(proof)
        journey.evidence["workers"] = workers
        journey.save()
        print(f"Fresh {phase} worker {proof['worker_pid']} exited after releasing its lock", flush=True)
    first, second = workers
    assert first["worker_pid"] != second["worker_pid"]
    directory = journey.root / "fresh"
    _project, runtime, backend, address = open_runtime(directory)
    try:
        prepared = KafkaStreamsActionEvidence.from_dict(first["prepared"])
        with backend.operation(address) as operation:
            before = operation.observe()
            assert before.control.control.status == "clear"
            assert before.state.state.to_dict() == second["final_state"]
            resources = before.state.state.resources
            owned = next(record for record in resources.values() if record.physical_name == prepared.application_id)
            assert owned.artifact_checksum == prepared.desired_artifact.checksum
            observer = KafkaStreamsReplacementObserver(runtime)
            ready = observer.observe(prepared, owned)
            assert ready.prior_container is None
            assert ready.candidate_container.ready
            assert ready.candidate_container.container_id == first["candidate_id"]
            assert ready.candidate_container.generation.operation_id == first["operation_id"]
            assert ready.candidate_container.generation.action_index == 0
            assert ready.candidate_container.generation.evidence_fingerprint == prepared.immutable_fingerprint
            assert ready.progress.partitions[0].committed == 5
            assert ready.volume == prepared.volume
            output, progress = helpers()["verify_updated_records"](SimpleNamespace(bootstrap=journey.bootstrap, token=journey.token), runtime, prepared)
            assert operation.observe() == before
            history = backend._read_recovery_history(address)
            assert history.to_dict() == second["resume_history_after_clear"]
            journey.evidence["durable_resume_probe"] = {
                "accepted": True, "parent_pid": os.getpid(), "worker_pids": [first["worker_pid"], second["worker_pid"]],
                "operation_id": first["operation_id"], "resume_id": second["resume_record"]["resume_id"],
                "source_control_version": 4, "resumed_control_version": 5, "audit_history_version": 2,
                "old_close_raw_exit_code": first["interrupted_control"]["progress"][1]["kafka_streams_checkpoint"]["exit_code"],
                "candidate_id": ready.candidate_container.container_id, "same_volume": ready.volume.to_dict(),
                "committed_before": 5, "committed_after": progress.partitions[0].committed,
                "read_committed_outputs": output, "same_topic_and_cluster_ids": True,
                "desired_ownership_committed": True, "final_control_status": "clear", "original_incident_survives_clear": True,
                "worker_one_provider_mutations": first["docker_commands"], "worker_two_provider_mutations": second["docker_mutations"],
                "forbidden_provider_write_attempts": first["forbidden_provider_write_attempts"] + second["forbidden_write_attempts"],
                "public_cli_update_resume_or_reviewed_plan_verification": False,
                "synthetic_test_internal_reviewed_checksum": first["synthetic_test_internal_reviewed_checksum"],
                "worker_one_exit_was_controlled_not_sigkill": True,
            }
            journey.save()
    finally:
        runtime.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path)
    parser.add_argument("--mode", choices=["source", "installed"])
    parser.add_argument("--image")
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--worker", choices=["interrupt", "resume"])
    parser.add_argument("--fixture-root", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        assert args.fixture_root is not None
        (worker_interrupt if args.worker == "interrupt" else worker_resume)(args.fixture_root)
        return
    assert args.checkout is not None
    assert args.mode is not None
    assert args.image is not None
    assert args.evidence_dir is not None
    utility = runpy.run_path(str(Path(__file__).with_name("kafka_streams_journey.py")))
    journey = utility["Journey"](args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence.update({
        "scope": "Test-internal durable local resume in a fresh OS worker; no public CLI update or reviewed-plan verification",
        "process_umask": "0077", "kafka_client_version": kafka_client_version, "librdkafka": libversion()[0],
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    })
    try:
        journey.setup()
        journey.exercise("fresh")
        prove_process_resume(journey)
        journey.evidence["accepted"] = True
    except BaseException as error:
        journey.evidence["accepted"] = False
        journey.evidence["failure"] = str(error)
        raise
    finally:
        journey.evidence["source_hashes_after"] = journey.source_hashes()
        journey.evidence["source_unchanged_during_run"] = journey.evidence["source_hashes_before"] == journey.evidence["source_hashes_after"]
        journey.save()
        helpers()["cleanup_without_force"](journey)
    assert journey.evidence["source_unchanged_during_run"]
    print(f"Real two-process durable resume passed; exact no-force cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
