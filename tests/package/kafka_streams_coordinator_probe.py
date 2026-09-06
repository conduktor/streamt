"""Actual reviewed SQL replacement across fresh coordinator worker processes.

Public init/create/no-op prepares the fixture. The internal coordinator then
executes a genuine on-disk reviewed plan, resumes a lost Docker-create response,
and finalizes after a second interruption before control clear. Workers exit
normally after recording their observed boundaries; this is not SIGKILL or a
public CLI update/resume proof.
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
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml
from confluent_kafka import __version__ as kafka_client_version
from confluent_kafka import libversion

from streamt.compiler import Compiler
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.kafka_streams_replacement_coordinator import (
    KafkaStreamsReplacementCoordinator,
)
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    ReplacementExecutionState,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.operation_actions import operation_actions_from_planned
from streamt.deployer.plan_file import ReviewedPlanFile, StateReference
from streamt.deployer.planner import DeploymentPlanner
from streamt.deployer.state import LocalState, local_state_path
from streamt.deployer.state_backend import StateBackendUnknownCommitError


def helper(filename):
    return runpy.run_path(str(Path(__file__).with_name(filename)))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_worker(root, phase, proof):
    (root / f"worker-{phase}.json").write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


def snapshot_data(snapshot):
    return {
        "state": snapshot.state.state.to_dict(), "control": snapshot.control.control.to_dict(),
        "state_revision": snapshot.state.revision.value, "control_revision": snapshot.control.revision.value,
        "store": {"backend": snapshot.state.store.backend, "store_id": snapshot.state.store.store_id},
    }


def load_context(root):
    directory = root / "fresh"
    project, runtime, backend, address = helper("kafka_streams_resume_probe.py")["open_runtime"](directory)
    observer = KafkaStreamsReplacementObserver(runtime)
    coordinator = KafkaStreamsReplacementCoordinator(observer, lambda: ProjectParser(directory).parse())
    return directory, project, runtime, backend, address, observer, coordinator


def worker_execute(root):
    directory = root / "fresh"
    project_file = directory / "stream_project.yml"
    original = yaml.safe_load(project_file.read_text())
    changed = json.loads(json.dumps(original))
    model = next(item for item in changed["models"] if item["name"] == "eligible_orders")
    assert model["sql"].count("amount >= 100") == 1
    model["sql"] = model["sql"].replace("amount >= 100", "amount >= 200", 1)
    proof = {"phase": "execute", "worker_pid": os.getpid(), "operation_id": str(uuid.uuid4()),
             "project_before_sha256": digest(project_file), "on_disk_sql_before": next(item["sql"] for item in original["models"] if item["name"] == "eligible_orders")}
    project_file.write_text(yaml.safe_dump(changed, sort_keys=False))
    assert yaml.safe_load(project_file.read_text()) == changed
    proof["project_after_sha256"] = digest(project_file)
    proof["on_disk_sql_after"] = model["sql"]
    directory, project, runtime, backend, address, observer, coordinator = load_context(root)
    kafka_config = project.runtime.kafka.to_confluent_config()
    bootstrap = kafka_config.pop("bootstrap.servers")
    try:
        with KafkaDeployer(bootstrap, **kafka_config) as kafka, backend.operation(address) as operation:
            initial = operation.observe()
            operation.ensure_ready(initial)
            manifest = Compiler(project).compile(dry_run=True)
            planner = DeploymentPlanner(
                manifest, kafka_deployer=kafka, kafka_streams_deployer=runtime, project=project,
                prior_state=initial.state.state, allow_kafka_streams_replacement=True,
            )
            current_plan = planner.plan()
            actions = operation_actions_from_planned(planner.planned_actions(current_plan))
            assert len(actions) == 1
            assert actions[0].action == "update"
            reviewed = ReviewedPlanFile.create(
                current_plan, manifest, project=project.project.name, environment=project.environment_name,
                runtime=project.runtime, state=StateReference.from_observation(initial.state), actions=actions,
            )
            assert reviewed.format_version == 6
            reviewed_path = root / "reviewed-plan.json"
            reviewed.save(reviewed_path)
            reviewed = ReviewedPlanFile.load(reviewed_path)
            assert reviewed.actions == actions
            evidence = reviewed.actions[0].kafka_streams_evidence
            assert evidence.progress.partitions[0].committed == 5
            holder = ReplacementExecutionState(initial)
            with helper("kafka_streams_replacement_executor_probe.py")["replacement_guard"](runtime, operation, evidence, proof):
                try:
                    coordinator.execute(
                        operation, holder, plan=reviewed, current_plan=current_plan, current_actions=actions,
                        operation_id=proof["operation_id"], actor="coordinator-worker-execute", timeout_seconds=120,
                    )
                except KafkaStreamsReplacementExecutionError:
                    assert proof.get("injected_create_ack_loss") is True
                else:
                    raise AssertionError("Successful real create did not lose its response")
                blocked = operation.observe()
                assert blocked == holder.snapshot
                control = blocked.control.control
                assert control.status == "recovery_required"
                assert control.recovery.failure_code == "runner_operation_interrupted"
                assert control.intent.reviewed_plan_checksum == reviewed.checksum
                assert control.intent.actions == reviewed.actions
                assert len(control.progress) == 3
                assert control.progress[-1].kafka_streams_checkpoint.phase == "old_removed"
                assert control.progress[1].kafka_streams_checkpoint.exit_code == 143
                observed = observer.observe(evidence, initial.state.state.resources[actions[0].resource_id])
                assert observed.prior_container is None
                assert observed.candidate_container.process_state == "created"
                assert observed.candidate_container.container_id == proof["created_container_ids"][0]
                assert observed.progress.active_members == 0
                assert observed.progress.partitions[0].committed == 5
                assert observed.volume == evidence.volume
                assert blocked.state == initial.state
                assert backend._read_recovery_history(address).events == ()
                proof.update({"reviewed_plan_sha256": digest(reviewed_path), "reviewed_checksum": reviewed.checksum,
                              "reviewed_format": 6, "synthetic_reviewed_checksum": False,
                              "interrupted_snapshot": snapshot_data(blocked), "candidate_id": observed.candidate_container.container_id,
                              "candidate_never_started": True, "volume": observed.volume.to_dict()})
        proof["lock_context_exited_before_worker_exit"] = True
    finally:
        runtime.close()
    save_worker(root, "execute", proof)


def worker_resume(root):
    first = json.loads((root / "worker-execute.json").read_text())
    directory, _project, runtime, backend, address, _observer, coordinator = load_context(root)
    reviewed_path = root / "reviewed-plan.json"
    reviewed = ReviewedPlanFile.load(reviewed_path)
    assert digest(reviewed_path) == first["reviewed_plan_sha256"]
    assert digest(directory / "stream_project.yml") == first["project_after_sha256"]
    proof = {"phase": "resume", "worker_pid": os.getpid(), "candidate_id": first["candidate_id"]}
    assert proof["worker_pid"] != first["worker_pid"]
    try:
        with backend.operation(address) as operation:
            blocked = operation.observe()
            assert snapshot_data(blocked) == first["interrupted_snapshot"]
            holder = ReplacementExecutionState(blocked)
            real_write = backend._write_control

            def fail_before_clear(path, control, *, operation_id):
                if control.status == "clear":
                    proof["injected_pre_clear_failure"] = True
                    raise StateBackendUnknownCommitError("Test interrupted completion before control clear")
                return real_write(path, control, operation_id=operation_id)

            with helper("kafka_streams_resume_probe.py")["resume_provider_guard"](runtime, operation, first["candidate_id"], proof), patch.object(backend, "_write_control", fail_before_clear):
                try:
                    coordinator.resume(operation, holder, plan=reviewed, operation_id=first["operation_id"],
                                       actor="coordinator-worker-resume", timeout_seconds=120)
                except StateBackendUnknownCommitError:
                    assert proof.get("injected_pre_clear_failure") is True
                else:
                    raise AssertionError("Finalizer did not reach the injected pre-clear interruption")
                # The holder records only acknowledged boundaries; the fresh
                # read separately proves the already-written ownership result.
                actual = operation.observe()
                assert holder.snapshot.control.control.actions_completed
                assert holder.snapshot.state == blocked.state
                assert actual.control == holder.snapshot.control
                assert actual.control.control.status == "in_progress"
                assert actual.state.state.serial == blocked.state.state.serial + 1
                history = backend._read_recovery_history(address)
                assert [event.kind for event in history.events] == ["operation_resumed", "runner_completed"]
                completion = history.completion_for(first["operation_id"])
                assert completion.control == actual.control.control
                assert completion.control.intent.reviewed_plan_checksum == reviewed.checksum
                assert completion.control.intent.actions == reviewed.actions
                assert completion.control.resume_history[0].recovery.to_dict() == first["interrupted_snapshot"]["control"]["recovery"]
                proof.update({"last_acknowledged_snapshot": snapshot_data(holder.snapshot),
                              "written_result_snapshot": snapshot_data(actual), "history_before_clear": history.to_dict(),
                              "ownership_file_sha256": digest(local_state_path(directory, environment=address.environment)),
                              "reviewed_plan_sha256": digest(reviewed_path)})
        proof["lock_context_exited_before_worker_exit"] = True
    finally:
        runtime.close()
    save_worker(root, "resume", proof)


@contextmanager
def finalization_guard(runtime, operation, backend, proof):
    raw_run = runtime.docker._run
    proof["forbidden_write_attempts"] = []
    reads = {("container", "ls"), ("container", "inspect"), ("container", "cp"),
             ("volume", "ls"), ("volume", "inspect"), ("network", "inspect"),
             ("image", "inspect"), ("context", "inspect")}

    def forbidden(*args, **kwargs):
        proof["forbidden_write_attempts"].append("unexpected_write")
        raise AssertionError("Fresh finalization attempted runtime, ownership, audit or authorization writes")

    def run(args, **kwargs):
        if tuple(args[:2]) not in reads and args[0] != "info":
            return forbidden()
        operation.check_lock()
        return raw_run(args, **kwargs)

    with ExitStack() as stack:
        stack.enter_context(patch.object(runtime.docker, "_run", run))
        for name in ("create", "ensure_state_volume"):
            stack.enter_context(patch.object(runtime.docker, name, forbidden))
        stack.enter_context(patch.object(runtime.progress, "initialize", forbidden))
        for name in ("alter_consumer_group_offsets", "create_topics", "delete_topics", "alter_configs",
                     "incremental_alter_configs", "create_partitions", "delete_consumer_groups"):
            stack.enter_context(patch.object(runtime.progress.admin, name, forbidden))
        for name in ("resume_operation", "mark_recovery_required", "compare_and_swap"):
            stack.enter_context(patch.object(operation, name, forbidden))
        stack.enter_context(patch.object(backend, "_append_recovery_history_locked", forbidden))
        stack.enter_context(patch.object(LocalState, "_save_if_serial_locked", forbidden))
        yield


def worker_finalize(root):
    first = json.loads((root / "worker-execute.json").read_text())
    second = json.loads((root / "worker-resume.json").read_text())
    directory, _project, runtime, backend, address, _observer, coordinator = load_context(root)
    reviewed_path = root / "reviewed-plan.json"
    reviewed = ReviewedPlanFile.load(reviewed_path)
    assert digest(reviewed_path) == first["reviewed_plan_sha256"]
    assert digest(directory / "stream_project.yml") == first["project_after_sha256"]
    proof = {"phase": "finalize", "worker_pid": os.getpid()}
    assert proof["worker_pid"] not in (first["worker_pid"], second["worker_pid"])
    try:
        with backend.operation(address) as operation:
            observed = operation.observe()
            assert snapshot_data(observed) == second["written_result_snapshot"]
            holder = ReplacementExecutionState(observed)
            with finalization_guard(runtime, operation, backend, proof):
                final = coordinator.resume(operation, holder, plan=reviewed, operation_id=first["operation_id"],
                                           actor="coordinator-worker-finalize", timeout_seconds=120)
            assert final is holder.snapshot
            assert final.control.control.status == "clear"
            assert final.state == observed.state
            assert digest(local_state_path(directory, environment=address.environment)) == second["ownership_file_sha256"]
            history = backend._read_recovery_history(address)
            assert history.to_dict() == second["history_before_clear"]
            proof.update({"final_snapshot": snapshot_data(final), "history_after_clear": history.to_dict(),
                          "ownership_file_not_rewritten": True, "runtime_writes": [],
                          "no_new_authorization_or_audit_event": True, "reviewed_plan_sha256": digest(reviewed_path)})
        proof["lock_context_exited_before_worker_exit"] = True
    finally:
        runtime.close()
    save_worker(root, "finalize", proof)


def prove_coordinator(journey):
    workers = []
    for phase in ("execute", "resume", "finalize"):
        command = [sys.executable, *(["-I"] if journey.mode == "installed" else []), str(Path(__file__).resolve()),
                   "--worker", phase, "--fixture-root", str(journey.root)]
        try:
            result = journey.run(command, env=journey.cli_environment, timeout=240)
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
        print(f"Coordinator {phase} worker {proof['worker_pid']} exited after releasing its lock", flush=True)
    assert len({proof["worker_pid"] for proof in workers}) == 3
    _directory, _project, runtime, backend, address, observer, _coordinator = load_context(journey.root)
    reviewed_path = journey.root / "reviewed-plan.json"
    reviewed = ReviewedPlanFile.load(reviewed_path)
    evidence = reviewed.actions[0].kafka_streams_evidence
    first, second, third = workers
    assert digest(reviewed_path) == first["reviewed_plan_sha256"] == second["reviewed_plan_sha256"] == third["reviewed_plan_sha256"]
    try:
        with backend.operation(address) as operation:
            final = operation.observe()
            assert snapshot_data(final) == third["final_snapshot"]
            owned = final.state.state.resources[reviewed.actions[0].resource_id]
            assert owned.artifact_checksum == evidence.desired_artifact.checksum
            ready = observer.observe(evidence, owned)
            assert ready.prior_container is None
            assert ready.candidate_container.ready
            assert ready.candidate_container.container_id == first["candidate_id"]
            assert ready.candidate_container.generation.operation_id == first["operation_id"]
            assert ready.candidate_container.generation.action_index == 0
            assert ready.candidate_container.generation.evidence_fingerprint == evidence.immutable_fingerprint
            assert ready.volume == evidence.volume
            assert ready.progress.partitions[0].committed == 5
            outputs, progress = helper("kafka_streams_replacement_executor_probe.py")["verify_updated_records"](
                SimpleNamespace(bootstrap=journey.bootstrap, token=journey.token), runtime, evidence,
            )
            assert operation.observe() == final
            history = backend._read_recovery_history(address)
            assert history.to_dict() == third["history_after_clear"]
            journey.evidence["coordinator_probe"] = {
                "accepted": True, "parent_pid": os.getpid(), "worker_pids": [worker["worker_pid"] for worker in workers],
                "operation_id": first["operation_id"], "candidate_id": first["candidate_id"],
                "reviewed_format": 6, "reviewed_checksum": reviewed.checksum, "reviewed_file_sha256": digest(reviewed_path),
                "actual_on_disk_sql_edit": True, "synthetic_reviewed_checksum": False,
                "old_close_raw_exit_code": 143, "same_volume": ready.volume.to_dict(),
                "new_generation": ready.candidate_container.generation.to_dict(), "same_topic_and_cluster_ids": True,
                "committed_before": 5, "committed_after": progress.partitions[0].committed,
                "read_committed_outputs": outputs, "final_state_serial": final.state.state.serial,
                "final_control_status": "clear", "audit_history_version": history.history_version,
                "audit_event_kinds": [event.kind for event in history.events],
                "incident_and_completed_original_tuple_survive_clear": True,
                "ownership_not_rewritten_on_finalization_retry": True,
                "forbidden_provider_write_attempts": first["forbidden_provider_write_attempts"] + second["forbidden_write_attempts"] + third["forbidden_write_attempts"],
                "public_cli_update_or_resume": False, "worker_exits_controlled_not_sigkill": True,
            }
            journey.save()
        # Exercise the ordinary public path after releasing the parent lock:
        # desired SQL, protected ownership and the live replacement must now
        # agree without another provider or ownership mutation.
        providers_before = journey.provider_snapshot()
        public_plan = journey.command(journey.root / "fresh", "plan")
        assert public_plan["has_changes"] is False
        assert public_plan["is_apply_blocked"] is False
        public_apply = journey.command(journey.root / "fresh", "apply")
        assert public_apply["committed"] is True
        assert public_apply["state_serial"] == final.state.state.serial
        assert journey.provider_snapshot() == providers_before
        with backend.operation(address) as operation:
            assert operation.observe() == final
            assert backend._read_recovery_history(address).to_dict() == third["history_after_clear"]
        journey.evidence["coordinator_probe"]["ordinary_public_plan_apply_after_finalize"] = {
            "plan_has_changes": False, "plan_apply_blocked": False,
            "apply_committed": True, "providers_and_ownership_unchanged": True,
            "incident_and_completion_archive_unchanged": True,
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
    parser.add_argument("--worker", choices=["execute", "resume", "finalize"])
    parser.add_argument("--fixture-root", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        assert args.fixture_root is not None
        {"execute": worker_execute, "resume": worker_resume, "finalize": worker_finalize}[args.worker](args.fixture_root)
        return
    assert args.checkout is not None
    assert args.mode is not None
    assert args.image is not None
    assert args.evidence_dir is not None
    journey = helper("kafka_streams_journey.py")["Journey"](args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence.update({
        "scope": "Internal actual-reviewed coordinator with fresh-process resume and finalization; no public CLI update",
        "process_umask": "0077", "kafka_client_version": kafka_client_version, "librdkafka": libversion()[0],
        "harness_sha256": digest(Path(__file__)),
        "helper_sha256": {name: digest(Path(__file__).with_name(name)) for name in (
            "kafka_streams_journey.py", "kafka_streams_resume_probe.py", "kafka_streams_replacement_executor_probe.py",
        )},
    })
    try:
        journey.setup()
        journey.exercise("fresh")
        prove_coordinator(journey)
        journey.evidence["accepted"] = True
    except BaseException as error:
        journey.evidence["accepted"] = False
        journey.evidence["failure"] = str(error)
        raise
    finally:
        journey.evidence["source_hashes_after"] = journey.source_hashes()
        journey.evidence["source_unchanged_during_run"] = journey.evidence["source_hashes_before"] == journey.evidence["source_hashes_after"]
        journey.save()
        helper("kafka_streams_replacement_executor_probe.py")["cleanup_without_force"](journey)
    assert journey.evidence["source_unchanged_during_run"]
    print(f"Real actual-reviewed coordinator passed; exact no-force cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
