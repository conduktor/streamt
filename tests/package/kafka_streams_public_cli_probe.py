"""Real public plan/apply/status/resume with controlled fresh-process failures.

The public CLI owns every operation, authorization and completion. Fixture-only
hooks lose successful Docker-create or local clear acknowledgements and forbid
unrelated writes. Workers exit normally; this is not SIGKILL or PostgreSQL
acceptance. Historical helpers and verification files remain unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import runpy
import subprocess
import sys
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml
from confluent_kafka import __version__ as kafka_client_version
from confluent_kafka import libversion
from confluent_kafka.admin import AdminClient

from streamt.core.parser import ProjectParser
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_docker import KafkaStreamsDockerError, LocalDockerRunner
from streamt.deployer.kafka_streams_progress import KafkaStreamsProgress
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.plan_file import ReviewedPlanFile
from streamt.deployer.state import LocalState
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    StateAddress,
    StateBackendUnknownCommitError,
)


def helper(filename):
    return runpy.run_path(str(Path(__file__).with_name(filename)))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot_data(snapshot):
    return {
        "state": snapshot.state.state.to_dict(), "control": snapshot.control.control.to_dict(),
        "state_revision": snapshot.state.revision.value, "control_revision": snapshot.control.revision.value,
        "store": {"backend": snapshot.state.store.backend, "store_id": snapshot.state.store.store_id},
    }


def open_state(directory):
    project = ProjectParser(directory).parse()
    assert project.deployment_state.backend == "local"
    return LocalDeploymentStateBackend(directory), StateAddress("local", project.project.name, "default")


def read_state(directory):
    backend, address = open_state(directory)
    with backend.operation(address) as operation:
        snapshot = operation.observe()
        history = backend._read_recovery_history(address)
    return snapshot, history


def invoke_cli(arguments):
    stdout, stderr = io.StringIO(), io.StringIO()
    with patch.object(sys, "argv", ["streamt", "-o", "json", *arguments]), redirect_stdout(stdout), redirect_stderr(stderr):
        try:
            runpy.run_module("streamt", run_name="__main__")
        except SystemExit as result:
            code = result.code or 0
        else:
            code = 0
    assert type(code) is int
    return {"argv": arguments, "exit_code": code, "payload": json.loads(stdout.getvalue()),
            "stdout": stdout.getvalue(), "stderr": stderr.getvalue()}


@contextmanager
def command_guard(phase, reviewed, expected_operation_id, candidate_id, proof):
    """Permit only this phase's exact writes while the real CLI lock is held."""
    evidence = reviewed.actions[0].kafka_streams_evidence
    readonly = phase in {"status_pending", "status_completed", "retry_completed"}
    active = []
    raw_operation = LocalDeploymentStateBackend.operation
    raw_run = LocalDockerRunner._run
    raw_create = LocalDockerRunner.create
    raw_write_control = LocalDeploymentStateBackend._write_control
    mutations = proof["docker_mutations"] = []
    denied = proof["forbidden_write_attempts"] = []
    created = proof["created_container_ids"] = []
    proof["lock_contexts_exited"] = 0
    reads = {("container", "ls"), ("container", "inspect"), ("container", "cp"),
             ("volume", "ls"), ("volume", "inspect"), ("network", "inspect"),
             ("image", "inspect"), ("context", "inspect")}

    def forbidden(*_args, **_kwargs):
        denied.append("unexpected_write")
        raise AssertionError("Public runner command attempted an unauthorized fixture mutation")

    @contextmanager
    def operation(backend, address):
        assert not active
        held = None
        try:
            with raw_operation(backend, address) as held, ExitStack() as guarded:
                active.append(held)
                if readonly:
                    for name in ("begin_operation", "record_progress", "resume_operation", "mark_recovery_required",
                                 "commit_operation", "finalize_completed_runner", "compare_and_swap", "clear_before_mutation"):
                        guarded.enter_context(patch.object(held, name, forbidden))
                try:
                    yield held
                finally:
                    active.pop()
        finally:
            # The expected unknown-write exception exits the real context too;
            # code after `with` would skip counting that successful release.
            if held is not None:
                assert held._lock.is_held is False
                proof["lock_contexts_exited"] += 1

    def validate_authority():
        assert len(active) == 1
        active[0].check_lock()
        current = active[0].observe().control.control
        assert current.status == "in_progress"
        assert current.intent.actions == reviewed.actions
        assert current.intent.reviewed_plan_checksum == reviewed.checksum
        if expected_operation_id is not None:
            assert current.intent.operation_id == expected_operation_id
        if "operation_id" in proof:
            assert proof["operation_id"] == current.intent.operation_id
        proof["operation_id"] = current.intent.operation_id
        return current

    def run(docker, args, **kwargs):
        prefix = tuple(args[:2])
        if prefix == ("container", "cp"):
            assert len(args) == 4
            assert args[-1] == "-", "Only container-to-stdout reads are permitted"
            identity, path = args[2].split(":", 1)
            assert identity in {evidence.prior_container_id, candidate_id, *created}
            assert path in {"/run/streamt/plan.json", "/var/lib/streamt/state/status.json"}
        if prefix in reads or args[0] == "info":
            return raw_run(docker, args, **kwargs)
        if readonly:
            return forbidden()
        control = validate_authority()
        if phase.startswith("apply_"):
            if prefix == ("container", "kill"):
                assert args == ["container", "kill", "--signal=TERM", evidence.prior_container_id]
            elif prefix == ("container", "rm"):
                assert args == ["container", "rm", evidence.prior_container_id]
            elif prefix == ("container", "create"):
                assert "--pull=never" in args
                assert "--restart=no" in args
                assert f"io.streamt.operation-id={control.intent.operation_id}" in args
                assert f"io.streamt.replacement-fingerprint={evidence.immutable_fingerprint}" in args
            elif prefix == ("container", "start"):
                assert phase == "apply_direct"
                assert args == ["container", "start", created[0]]
            else:
                return forbidden()
        else:
            assert phase in {"resume", "resume_lost_clear"}
            assert args == ["container", "start", candidate_id]
        assert prefix not in [tuple(item[:2]) for item in mutations], "CLI repeated a provider write"
        mutations.append(list(args[:2]) + ([args[-1]] if prefix != ("container", "create") else []))
        return raw_run(docker, args, **kwargs)

    def create(docker, **kwargs):
        assert phase in {"apply_lost_create", "apply_direct"}
        assert not created
        assert kwargs["expected_volume"] == evidence.volume
        generation = kwargs["generation"]
        assert generation.action_index == 0
        assert generation.evidence_fingerprint == evidence.immutable_fingerprint
        identity = raw_create(docker, **kwargs)
        created.append(identity)
        if phase == "apply_lost_create":
            proof["injected_create_ack_loss"] = True
            raise KafkaStreamsDockerError("Test lost acknowledgement after the exact successful create")
        return identity

    def write_control(backend, path, control, *, operation_id):
        result = raw_write_control(path, control, operation_id=operation_id)
        if phase == "resume_lost_clear" and control.status == "clear":
            assert not proof.get("injected_clear_ack_loss")
            proof["injected_clear_ack_loss"] = True
            raise StateBackendUnknownCommitError("Test lost acknowledgement after control clear", operation_id=operation_id)
        return result

    with ExitStack() as stack:
        stack.enter_context(patch.object(LocalDeploymentStateBackend, "operation", operation))
        stack.enter_context(patch.object(LocalDockerRunner, "_run", run))
        stack.enter_context(patch.object(LocalDockerRunner, "create", forbidden if readonly or phase.startswith("resume") else create))
        stack.enter_context(patch.object(LocalDockerRunner, "ensure_state_volume", forbidden))
        stack.enter_context(patch.object(KafkaStreamsProgress, "initialize", forbidden))
        for name in ("alter_consumer_group_offsets", "create_topics", "delete_topics", "alter_configs",
                     "incremental_alter_configs", "create_partitions", "delete_consumer_groups"):
            stack.enter_context(patch.object(AdminClient, name, forbidden))
        stack.enter_context(patch.object(LocalDeploymentStateBackend, "_write_control", forbidden if readonly else write_control))
        if readonly:
            stack.enter_context(patch.object(LocalDeploymentStateBackend, "_append_recovery_history_locked", forbidden))
            stack.enter_context(patch.object(LocalState, "_save_if_serial_locked", forbidden))
        yield
    assert not active
    assert proof["lock_contexts_exited"] >= 1


def worker(root, kind, phase, operation_id, candidate_id):
    directory = root / kind
    plan_path = root / f"reviewed-{kind}.json"
    reviewed = ReviewedPlanFile.load(plan_path)
    assert reviewed.format_version == 6
    before, history_before = read_state(directory)
    proof = {"worker_pid": os.getpid(), "kind": kind, "phase": phase,
             "reviewed_file_sha256": digest(plan_path), "before": snapshot_data(before)}
    if phase.startswith("apply_"):
        arguments = ["apply", "--plan", str(plan_path), "--runner-timeout", "120"]
    else:
        assert operation_id is not None
        command = "runner-status" if phase.startswith("status_") else "resume"
        arguments = ["state", command, "--plan", str(plan_path), "--operation-id", operation_id]
        if command == "resume":
            arguments.extend(["--timeout", "120"])
    arguments.extend(["--project-dir", str(directory)])
    with command_guard(phase, reviewed, operation_id, candidate_id, proof):
        result = invoke_cli(arguments)
    after, history_after = read_state(directory)
    proof.update({"command": result, "after": snapshot_data(after), "history_after": history_after.to_dict(),
                  "worker_exit_controlled_after_lock_release": True})
    payload = result["payload"]
    expected_failure = phase in {"apply_lost_create", "resume_lost_clear"}
    assert result["exit_code"] == (1 if expected_failure else 0), result
    assert payload["status"] == ("error" if expected_failure else "ok"), result
    data = payload["data"]
    assert data["plan_checksum"] == reviewed.checksum
    if operation_id is not None:
        assert data["operation_id"] == operation_id
    if phase == "apply_lost_create":
        assert proof["injected_create_ack_loss"]
        assert data["committed"] is None
        assert data["last_acknowledged_boundary"] == "old_removed"
        control = after.control.control
        assert control.status == "recovery_required"
        assert len(control.progress) == 3
        assert control.progress[1].kafka_streams_checkpoint.exit_code == 143
        assert control.intent.operation_id == data["operation_id"] == proof["operation_id"]
        assert after.state == before.state
        assert history_after.events == history_before.events
    elif phase == "status_pending":
        assert data["status"] == "pending"
        assert data["lifecycle_phase"] == "old_removed"
        assert data["next_step"] == "record_replacement_created"
        assert data["resumable"] is True
        assert data["committed"] is False
        assert data["read_only"] is True
        assert after == before
        assert history_after == history_before
    elif phase == "resume_lost_clear":
        assert proof["injected_clear_ack_loss"]
        assert data["committed"] is None
        assert after.control.control.status == "clear"
        assert after.state.state.serial == before.state.state.serial + 1
    elif phase in {"status_completed", "retry_completed"}:
        assert data["status"] == "completed"
        assert data["committed"] is True
        assert data["read_only"] is True
        assert data["resumable"] is False
        assert data["next_action"] == "none"
        assert after == before
        assert history_after == history_before
        assert proof["docker_mutations"] == []
    else:
        assert data["committed"] is True
        assert after.control.control.status == "clear"
        assert after.state.state.serial == before.state.state.serial + 1
    assert proof["forbidden_write_attempts"] == []
    (root / f"worker-{kind}-{phase}.json").write_text(json.dumps(proof, indent=2, sort_keys=True) + "\n")


def run_worker(journey, kind, phase, *, operation_id=None, candidate_id=None):
    command = [sys.executable, *(["-I"] if journey.mode == "installed" else []), str(Path(__file__).resolve()),
               "--worker", phase, "--fixture-root", str(journey.root), "--kind", kind]
    if operation_id:
        command.extend(["--operation-id", operation_id])
    if candidate_id:
        command.extend(["--candidate-id", candidate_id])
    try:
        result = journey.run(command, env=journey.cli_environment, timeout=240)
    except subprocess.CalledProcessError as error:
        (journey.root / f"worker-{kind}-{phase}.stdout").write_text(error.stdout or "")
        (journey.root / f"worker-{kind}-{phase}.stderr").write_text(error.stderr or "")
        raise
    (journey.root / f"worker-{kind}-{phase}.stdout").write_text(result.stdout)
    (journey.root / f"worker-{kind}-{phase}.stderr").write_text(result.stderr)
    proof = json.loads((journey.root / f"worker-{kind}-{phase}.json").read_text())
    assert proof["worker_pid"] != os.getpid()
    assert proof["worker_exit_controlled_after_lock_release"]
    journey.evidence.setdefault("public_workers", []).append(proof)
    journey.save()
    print(f"{kind}: public {phase} worker {proof['worker_pid']} exited after releasing its lock", flush=True)
    return proof


def prove_public(journey, kind):
    directory = journey.root / kind
    baseline, _history = read_state(directory)
    source_record = next(item for item in journey.evidence["journeys"] if item["kind"] == kind)
    external_before = None
    if kind == "existing":
        external_before = {"uuid": journey.progress.topic_id(source_record["input_topic"]),
                           "config": journey.topic_config(source_record["input_topic"])}
    project_file = directory / "stream_project.yml"
    original = yaml.safe_load(project_file.read_text())
    original_sha = digest(project_file)
    model = next(item for item in original["models"] if item["name"] == "eligible_orders")
    before_sql = model["sql"]
    assert before_sql.count("amount >= 100") == 1
    model["sql"] = before_sql.replace("amount >= 100", "amount >= 200", 1)
    project_file.write_text(yaml.safe_dump(original, sort_keys=False))
    project_after_sha = digest(project_file)
    plan_path = journey.root / f"reviewed-{kind}.json"
    plan_output = journey.command(directory, "plan", "--out", str(plan_path))
    assert plan_output["plan_format_version"] == 6
    assert plan_output["kafka_streams_replacement"]["requires_plan_file"] is True
    reviewed = ReviewedPlanFile.load(plan_path)
    assert reviewed.checksum == plan_output["plan_checksum"]
    evidence = reviewed.actions[0].kafka_streams_evidence
    assert len(reviewed.actions) == 1
    assert evidence.progress.partitions[0].committed == 5
    assert evidence.prior_container_id == source_record["container_id"]
    phase = "apply_direct" if kind == "direct" else "apply_lost_create"
    first = run_worker(journey, kind, phase)
    operation_id = first["command"]["payload"]["data"]["operation_id"]
    candidate_id = first["created_container_ids"][0]
    if kind != "direct":
        providers_before = journey.provider_snapshot()
        pending = run_worker(journey, kind, "status_pending", operation_id=operation_id, candidate_id=candidate_id)
        assert journey.provider_snapshot() == providers_before
        assert pending["docker_mutations"] == []
        run_worker(journey, kind, "resume_lost_clear" if kind == "fresh" else "resume",
                   operation_id=operation_id, candidate_id=candidate_id)
    for phase in ("status_completed", "retry_completed"):
        providers_before = journey.provider_snapshot()
        run_worker(journey, kind, phase, operation_id=operation_id, candidate_id=candidate_id)
        assert journey.provider_snapshot() == providers_before
    final, history = read_state(directory)
    assert final.control.control.status == "clear"
    assert final.state.state.serial == baseline.state.state.serial + 1
    expected_kinds = ["runner_completed"] if kind == "direct" else ["operation_resumed", "runner_completed"]
    assert [event.kind for event in history.events] == expected_kinds
    receipt = history.completion_for(operation_id)
    assert receipt.control.intent.actions == reviewed.actions
    assert receipt.control.intent.reviewed_plan_checksum == reviewed.checksum
    assert len(receipt.control.progress) == 5
    assert receipt.control.progress[1].kafka_streams_checkpoint.exit_code == 143
    project = ProjectParser(directory).parse()
    runtime = KafkaStreamsDeployer(project.runtime.kafka_streams, project.runtime.kafka, state_dir=directory / ".streamt")
    try:
        backend, address = open_state(directory)
        with backend.operation(address) as operation:
            assert operation.observe() == final
            observed = KafkaStreamsReplacementObserver(runtime).observe(evidence, final.state.state.resources[reviewed.actions[0].resource_id])
            assert observed.prior_container is None
            assert observed.candidate_container.ready
            assert observed.candidate_container.container_id == candidate_id
            assert observed.candidate_container.generation.operation_id == operation_id
            assert observed.volume == evidence.volume
            outputs, progress = helper("kafka_streams_replacement_executor_probe.py")["verify_updated_records"](
                SimpleNamespace(bootstrap=journey.bootstrap, token=journey.token + "-" + kind), runtime, evidence,
            )
            assert operation.observe() == final
    finally:
        runtime.close()
    providers_before = journey.provider_snapshot()
    assert journey.command(directory, "plan")["has_changes"] is False
    assert journey.command(directory, "apply")["state_serial"] == final.state.state.serial
    assert journey.provider_snapshot() == providers_before
    final_again, history_again = read_state(directory)
    assert final_again == final
    assert history_again == history
    assert digest(project_file) == project_after_sha
    assert digest(plan_path) == first["reviewed_file_sha256"]
    if external_before is not None:
        assert {"uuid": journey.progress.topic_id(source_record["input_topic"]),
                "config": journey.topic_config(source_record["input_topic"])} == external_before
    journey.evidence.setdefault("public_cycles", []).append({
        "kind": kind, "accepted": True, "operation_id": operation_id, "candidate_id": candidate_id,
        "actual_public_cli": True, "synthetic_reviewed_checksum": False,
        "sql_before": before_sql, "sql_after": model["sql"], "project_before_sha256": original_sha,
        "project_after_sha256": project_after_sha, "reviewed_file_sha256": digest(plan_path),
        "reviewed_checksum": reviewed.checksum, "reviewed_format": 6,
        "committed_before": 5, "committed_after": progress.partitions[0].committed,
        "read_committed_outputs": outputs, "same_volume": evidence.volume.to_dict(),
        "same_topic_and_cluster_ids": True, "old_closed_raw_exit_code": 143,
        "initial_state_serial": baseline.state.state.serial, "final_state_serial": final.state.state.serial,
        "final_control_status": "clear", "retained_audit_event_kinds": expected_kinds,
        "external_source_unchanged": external_before is not None,
        "public_status_and_completed_retry_read_only": True, "ordinary_noop_preserves_provider_state_and_audit": True,
    })
    journey.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path)
    parser.add_argument("--mode", choices=["source", "installed"])
    parser.add_argument("--image")
    parser.add_argument("--evidence-dir", type=Path)
    parser.add_argument("--worker", choices=["apply_lost_create", "apply_direct", "status_pending", "resume",
                                           "resume_lost_clear", "status_completed", "retry_completed"])
    parser.add_argument("--fixture-root", type=Path)
    parser.add_argument("--kind", choices=["existing", "fresh", "direct"])
    parser.add_argument("--operation-id")
    parser.add_argument("--candidate-id")
    args = parser.parse_args()
    os.umask(0o077)
    if args.worker:
        assert args.fixture_root is not None
        assert args.kind is not None
        worker(args.fixture_root, args.kind, args.worker, args.operation_id, args.candidate_id)
        return
    assert args.checkout is not None
    assert args.mode is not None
    assert args.image is not None
    assert args.evidence_dir is not None
    journey = helper("kafka_streams_journey.py")["Journey"](args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence.update({
        "scope": "Public Kafka Streams CLI replacement, read-only status, fresh-process resume and completed receipt retry; local state only",
        "process_umask": "0077", "kafka_client_version": kafka_client_version, "librdkafka": libversion()[0],
        "harness_sha256": digest(Path(__file__)), "helper_sha256": {
            name: digest(Path(__file__).with_name(name)) for name in (
                "kafka_streams_journey.py", "kafka_streams_replacement_executor_probe.py",
            )
        },
    })
    try:
        journey.setup()
        for kind in ("existing", "fresh", "direct"):
            journey.exercise(kind)
            prove_public(journey, kind)
        pids = [item["worker_pid"] for item in journey.evidence["public_workers"]]
        assert len(pids) == len(set(pids)) == 13
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
    print(f"Public create/import, reviewed update, status/resume and receipt retry passed; exact cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
