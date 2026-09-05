"""Real low-level replacement proof, not a public CLI update/resume workflow.

Use the public fresh starter/create journey, then a test-internal exact v4
intent under its real local lock. Lose one successful Docker-create response,
resume the same in_progress operation, and retain its completed pending journal.
Only uniquely owned disposable resources are provisioned or removed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import runpy
import time
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

from confluent_kafka import Consumer, Producer, TopicPartition, libversion

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_docker import KafkaStreamsDockerError
from streamt.deployer.kafka_streams_replacement_executor import (
    KafkaStreamsReplacementExecutionError,
    KafkaStreamsReplacementExecutor,
    ReplacementExecutionState,
)
from streamt.deployer.kafka_streams_replacement_observer import KafkaStreamsReplacementObserver
from streamt.deployer.kafka_streams_time import parse_utc_timestamp
from streamt.deployer.state_backend import (
    LocalDeploymentStateBackend,
    OperationAction,
    OperationIntent,
    OperationSnapshot,
    StateAddress,
    operation_timestamp,
    state_checksum,
)


def inspect_owned(journey, container_id, application_id=None):
    data = json.loads(journey.docker("container", "inspect", "--format", "{{json .}}", container_id))
    assert data["Id"] == container_id
    labels = data["Config"]["Labels"]
    if application_id is None:
        assert container_id == journey.broker_id
        assert data["Name"] == "/" + journey.broker
        assert labels["io.streamt.journey.owner"] == journey.token
    else:
        assert application_id in journey.applications
        assert data["Name"] == "/" + application_id
        assert labels["io.streamt.application-id"] == application_id
        assert labels["io.streamt.backend"] == journey.backend
    return data


def graceful_stop(journey, container_id, application_id=None):
    data = inspect_owned(journey, container_id, application_id)
    if data["State"]["Running"]:
        journey.docker("container", "kill", "--signal=TERM", container_id)
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        data = inspect_owned(journey, container_id, application_id)
        if data["State"]["Running"] is False:
            return
        time.sleep(0.25)
    raise AssertionError("Owned fixture did not stop; no forced removal is authorized")


def cleanup_without_force(journey):
    # Stop runners while their broker can still complete graceful shutdown.
    for app in journey.applications:
        ids = journey.docker("container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{app}$", "--format", "{{.ID}}").split()
        assert len(ids) <= 1
        if ids:
            graceful_stop(journey, ids[0], app)
    if journey.broker_id:
        graceful_stop(journey, journey.broker_id)
    docker = journey.docker

    def cleanup_command(*args, **kwargs):
        assert "-f" not in args
        assert "--force" not in args
        if args[:2] == ("container", "stop"):
            # Journey.cleanup normally uses Docker stop (which can escalate).
            # Its fixture broker is already TERM-stopped here: verify that
            # exact outcome and skip the redundant stop command entirely.
            assert args == ("container", "stop", "--time", "15", journey.broker_id)
            assert inspect_owned(journey, journey.broker_id)["State"]["Running"] is False
            return journey.broker_id
        return docker(*args, **kwargs)

    with patch.object(journey, "docker", cleanup_command):
        journey.cleanup()
    removed = journey.evidence["cleanup"]["removed"]
    for resource in removed:
        if resource["kind"] in {"runner", "broker"}:
            assert not journey.docker("container", "ls", "--all", "--no-trunc", "--filter", f"id={resource['id']}", "--format", "{{.ID}}")
        elif resource["kind"] == "runner-state-volume":
            assert not journey.docker("volume", "ls", "--filter", f"name=^{resource['name']}$", "--format", "{{.Name}}")
        elif resource["kind"] == "network":
            assert not journey.docker("network", "ls", "--no-trunc", "--filter", f"id={resource['id']}", "--format", "{{.ID}}")
    journey.evidence["cleanup"]["term_only_no_force_or_stop_escalation"] = True
    journey.save()


@contextmanager
def replacement_guard(runtime, operation, evidence, proof):
    raw_run = runtime.docker._run
    real_create = runtime.docker.create
    commands = proof["docker_commands"] = []
    created_ids = proof["created_container_ids"] = []
    denied = proof["forbidden_provider_write_attempts"] = []
    reads = {("container", "ls"), ("container", "inspect"), ("container", "cp"),
             ("volume", "ls"), ("volume", "inspect"), ("network", "inspect"),
             ("image", "inspect"), ("context", "inspect")}

    def forbidden(*args, **kwargs):
        denied.append("offset_or_volume_or_admin_write")
        raise AssertionError("Replacement attempted a forbidden provider mutation")

    def guarded_run(args, **kwargs):
        prefix = tuple(args[:2])
        if prefix in reads or args[0] == "info":
            return raw_run(args, **kwargs)
        operation.check_lock()
        if prefix == ("container", "create"):
            assert "--pull=never" in args
            assert "--restart=no" in args
            assert f"io.streamt.operation-id={proof['operation_id']}" in args
            assert f"io.streamt.replacement-fingerprint={evidence.immutable_fingerprint}" in args
        elif prefix == ("container", "kill"):
            assert args == ["container", "kill", "--signal=TERM", evidence.prior_container_id]
        elif prefix == ("container", "rm"):
            assert args == ["container", "rm", evidence.prior_container_id]
        elif prefix == ("container", "start"):
            assert args == ["container", "start", created_ids[0]]
        else:
            denied.append("unexpected_docker_write")
            raise AssertionError("Replacement attempted an unexpected Docker mutation")
        commands.append(list(args[:2]) + ([args[-1]] if prefix != ("container", "create") else []))
        return raw_run(args, **kwargs)

    def lose_create_ack(**kwargs):
        assert not created_ids, "Replacement repeated its create after an uncertain response"
        assert kwargs["expected_volume"] == evidence.volume
        identity = real_create(**kwargs)
        created_ids.append(identity)
        proof["injected_create_ack_loss"] = True
        raise KafkaStreamsDockerError("Test injected loss of successful create acknowledgement")

    with ExitStack() as stack:
        stack.enter_context(patch.object(runtime.docker, "_run", guarded_run))
        stack.enter_context(patch.object(runtime.docker, "create", lose_create_ack))
        stack.enter_context(patch.object(runtime.docker, "ensure_state_volume", forbidden))
        stack.enter_context(patch.object(runtime.progress, "initialize", forbidden))
        for name in ("alter_consumer_group_offsets", "create_topics", "delete_topics", "alter_configs",
                     "incremental_alter_configs", "create_partitions", "delete_consumer_groups"):
            stack.enter_context(patch.object(runtime.progress.admin, name, forbidden))
        yield


def verify_updated_records(journey, runtime, evidence):
    desired = evidence.desired_artifact.artifact
    input_topic, output_topic = desired.plan["input_topic"], desired.plan["output_topic"]
    producer = Producer({"bootstrap.servers": journey.bootstrap})
    delivered = []
    rows = [
        (b"below", {"id": "after-low", "amount": 150, "paid": True}),
        (b"\x01\xfe", {"id": "after-high", "amount": 250, "paid": True}),
        (None, {"id": "after-null-key", "amount": 300, "paid": True}),
    ]
    for key, row in rows:
        producer.produce(input_topic, key=key, value=json.dumps(row).encode(),
                         on_delivery=lambda error, _message: delivered.append(error))
    assert producer.flush(15) == 0
    assert delivered == [None] * len(rows)
    consumer = Consumer({"bootstrap.servers": journey.bootstrap, "group.id": "executor-proof-" + journey.token,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    consumer.assign([TopicPartition(output_topic, 0, 0)])
    outputs = []
    deadline = time.monotonic() + 45

    def append(message):
        assert message.error() is None
        outputs.append({"key_hex": message.key().hex() if message.key() is not None else None,
                        "value": json.loads(message.value()), "offset": message.offset()})

    try:
        while time.monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is not None:
                append(message)
            current = runtime.progress.observe(desired.application_id, input_topic, output_topic)
            current.require_resumable()
            if current.partitions[0].committed == 8 and len(outputs) >= 3:
                while (extra := consumer.poll(0.5)) is not None:
                    append(extra)
                break
        else:
            raise AssertionError("Replacement records or committed offset did not reach the expected result")
    finally:
        consumer.close()
    assert [(item["key_hex"], item["value"]) for item in outputs] == [
        ("00ff", {"id": "a", "amount": 120}),
        ("01fe", {"id": "after-high", "amount": 250}),
        (None, {"id": "after-null-key", "amount": 300}),
    ]
    assert (current.cluster_id, current.input_topic_id, current.output_topic_id) == (
        evidence.progress.cluster_id, evidence.progress.input_topic_id, evidence.progress.output_topic_id,
    )
    return outputs, current


def prove_executor(journey):
    directory = journey.root / "fresh"
    project = ProjectParser(directory).parse()
    changed = project.model_copy(deep=True)
    model = next(item for item in changed.models if item.name == "eligible_orders")
    assert "amount >= 100" in model.sql
    model.sql = model.sql.replace("amount >= 100", "amount >= 200", 1)
    manifest = Compiler(changed).compile(dry_run=True)
    desired = parse_compiled_kafka_streams_job_artifact(manifest.artifacts["kafka_streams_jobs"][0])
    runtime = KafkaStreamsDeployer(project.runtime.kafka_streams, project.runtime.kafka, state_dir=directory / ".streamt")
    observer = KafkaStreamsReplacementObserver(runtime)
    backend = LocalDeploymentStateBackend(directory)
    address = StateAddress("local", project.project.name, "default")
    proof = journey.evidence["executor_probe"] = {"operation_id": str(uuid.uuid4()), "checkpoints": []}
    try:
        with backend.operation(address) as operation:
            initial = operation.observe()
            operation.ensure_ready(initial)
            matches = [(name, record) for name, record in initial.state.state.resources.items()
                       if record.physical_name == desired.application_id]
            assert len(matches) == 1
            resource_id, record = matches[0]
            evidence = observer.prepare(desired, record)
            assert evidence.progress.partitions[0].committed == 5
            proof["prepared"] = evidence.to_dict()
            intent = OperationIntent(
                proof["operation_id"], "apply", operation_timestamp(), "executor-real-proof",
                initial.state.state_serial, state_checksum(initial.state.state), None,
                (OperationAction(0, resource_id, "update", kafka_streams_evidence=evidence),),
            )
            holder = ReplacementExecutionState(operation.begin_operation(initial, intent))
            initial_intent = holder.snapshot.control.control.intent.to_dict(control_version=4)
            real_observe, real_record = observer.observe, operation.record_progress

            def observed(*args, **kwargs):
                result = real_observe(*args, **kwargs)
                old = result.prior_container
                if old is not None and old.cleanly_closed and "close" not in proof:
                    raw = inspect_owned(journey, old.container_id, evidence.application_id)
                    status = runtime.docker.status_document(old.container_id)
                    assert old.exit_code == raw["State"]["ExitCode"] == 143
                    assert old.status_fresh
                    assert old.forced_exit is False
                    assert result.progress.active_members == 0
                    started, updated, finished = [parse_utc_timestamp(value) for value in (
                        raw["State"]["StartedAt"], status["updated_at"], raw["State"]["FinishedAt"],
                    )]
                    assert started <= updated <= finished
                    proof["close"] = {"raw_exit_code": old.exit_code, "status": status,
                                      "process_state": raw["State"], "progress": result.progress.to_dict()}
                return result

            def recorded(snapshot, progress):
                assert type(snapshot) is OperationSnapshot
                acknowledged = real_record(snapshot, progress)
                assert acknowledged.state == initial.state
                assert acknowledged.control.control.intent.to_dict(control_version=4) == initial_intent
                proof["checkpoints"].append(progress.to_dict(control_version=4))
                journey.save()
                return acknowledged

            with replacement_guard(runtime, operation, evidence, proof), patch.object(observer, "observe", observed), patch.object(operation, "record_progress", recorded):
                executor = KafkaStreamsReplacementExecutor(observer)
                try:
                    executor.run(operation, holder, operation_id=intent.operation_id, mode="execute", timeout_seconds=120)
                except KafkaStreamsReplacementExecutionError:
                    assert proof.get("injected_create_ack_loss") is True
                else:
                    raise AssertionError("Injected lost create acknowledgement was not surfaced")
                assert len(holder.snapshot.control.control.progress) == 3
                assert holder.snapshot.control.control.progress[-1].kafka_streams_checkpoint.phase == "old_removed"
                assert operation.observe() == holder.snapshot
                assert runtime.docker.inspect_exact(evidence.prior_container_id) is None
                assert runtime.docker.volume_witness(evidence.application_id, evidence.backend_identity) == evidence.volume.to_dict()
                created_id = proof["created_container_ids"][0]
                candidate = inspect_owned(journey, created_id, evidence.application_id)
                assert candidate["State"]["Status"] == "created"
                assert candidate["State"]["StartedAt"] == "0001-01-01T00:00:00Z"
                proof["after_lost_ack"] = {"durable_boundary": "old_removed", "container_id": created_id,
                                           "candidate_never_started": True, "prior_absent": True, "volume_retained": True}
                # Separate invocation, same persisted operation, still held lock.
                resumed = ReplacementExecutionState(operation.observe())
                final = executor.run(operation, resumed, operation_id=intent.operation_id, mode="resume", timeout_seconds=120)
                assert final == operation.observe()
                assert final.control.control.actions_completed
                assert final.control.control.status == "in_progress"
                assert final.state == initial.state
                assert final.control.control.intent.to_dict(control_version=4) == initial_intent
                checkpoint = final.control.control.progress[1].kafka_streams_checkpoint
                assert checkpoint.phase == "old_closed"
                assert checkpoint.exit_code == 143
                assert checkpoint.progress.partitions[0].committed == 5
                ready = observer.observe(evidence, record)
                assert ready.prior_container is None
                assert ready.candidate_container.ready
                assert ready.candidate_container.container_id == created_id
                assert ready.candidate_container.generation.operation_id == intent.operation_id
                assert ready.candidate_container.generation.action_index == 0
                assert ready.candidate_container.generation.evidence_fingerprint == evidence.immutable_fingerprint
                assert ready.volume == evidence.volume
                assert ready.progress.partitions[0].committed == 5
                outputs, progress = verify_updated_records(journey, runtime, evidence)
                assert operation.observe() == final
                proof.update({
                    "accepted": True, "same_application_id": evidence.application_id,
                    "prior_container_removed": evidence.prior_container_id,
                    "new_container_id": created_id, "new_generation": ready.candidate_container.generation.to_dict(),
                    "final_control": final.control.control.to_dict(), "protected_state_unchanged": True,
                    "pending_operation_not_cleared": True, "same_volume": ready.volume.to_dict(),
                    "outputs": outputs, "input_committed_before": 5, "input_committed_after": progress.partitions[0].committed,
                    "same_cluster_and_topic_uuids": True, "offset_initialization_or_reset": False,
                    "volume_creation_or_deletion_during_replacement": False,
                    "public_cli_replacement_or_resume_executed": False,
                })
                assert proof["forbidden_provider_write_attempts"] == []
                assert [entry[:2] for entry in proof["docker_commands"]] == [
                    ["container", "kill"], ["container", "rm"], ["container", "create"], ["container", "start"],
                ]
                journey.save()
        # Closing the local lock does not clear the completed pending operation.
        with backend.operation(address) as operation:
            assert operation.observe() == final
    finally:
        runtime.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--mode", choices=["source", "installed"], required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    utilities = runpy.run_path(str(Path(__file__).with_name("kafka_streams_journey.py")))
    journey = utilities["Journey"](args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence["scope"] = "Low-level inactive executor: test-internal v4 update, lost-create-ACK and same in_progress resume; no public replacement command"
    journey.evidence["process_umask"] = "0077"
    journey.evidence["librdkafka"] = libversion()[0]
    journey.evidence["harness_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    try:
        journey.setup()
        journey.exercise("fresh")
        prove_executor(journey)
        journey.evidence["accepted"] = True
    except BaseException as error:
        journey.evidence["accepted"] = False
        journey.evidence["failure"] = str(error)
        raise
    finally:
        journey.evidence["source_hashes_after"] = journey.source_hashes()
        journey.evidence["source_unchanged_during_run"] = journey.evidence["source_hashes_before"] == journey.evidence["source_hashes_after"]
        journey.save()
        cleanup_without_force(journey)
    assert journey.evidence["source_unchanged_during_run"]
    print(f"Real low-level executor proof passed; exact no-force cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
