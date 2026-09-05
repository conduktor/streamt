"""Real read-only replacement-observer proof; never execute a replacement.

Create a disposable public starter, prepare predicate-only evidence under the
real local state lock, TERM its exact owned runner, and observe full close
conditions. Provisioning/TERM/cleanup belong to this explicitly scoped test,
not to the read-only observer or a public replacement workflow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import runpy
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

from streamt.compiler.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_replacement_observer import (
    KafkaStreamsReplacementObservationError,
    KafkaStreamsReplacementObserver,
)
from streamt.deployer.kafka_streams_time import parse_utc_timestamp
from streamt.deployer.state_backend import LocalDeploymentStateBackend, StateAddress


@contextmanager
def readonly_observer(runtime: KafkaStreamsDeployer, calls: list[list[str]]):
    """Permit real provider reads while rejecting every exposed write boundary."""
    run = runtime.docker._run
    allowed = {("container", "ls"), ("container", "inspect"), ("container", "cp"),
               ("volume", "ls"), ("volume", "inspect"), ("network", "inspect"),
               ("image", "inspect"), ("context", "inspect")}

    def read_command(args, **kwargs):
        assert tuple(args[:2]) in allowed or args[0] == "info", "Observer attempted a Docker mutation"
        calls.append(args[:2])
        return run(args, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError("Observer attempted a provider mutation")

    with ExitStack() as stack:
        stack.enter_context(patch.object(runtime.docker, "_run", read_command))
        for name in ("create", "ensure_state_volume", "owned_command"):
            stack.enter_context(patch.object(runtime.docker, name, forbidden))
        stack.enter_context(patch.object(runtime.progress, "initialize", forbidden))
        for name in ("alter_consumer_group_offsets", "create_topics", "delete_topics", "alter_configs",
                     "incremental_alter_configs", "create_partitions", "delete_consumer_groups"):
            stack.enter_context(patch.object(runtime.progress.admin, name, forbidden))
        yield


def local_files(directory: Path) -> dict[str, str]:
    return {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.rglob("*")) if path.is_file()}


def inspect_fixture(journey, container_id: str, application_id: str) -> dict:
    data = json.loads(journey.docker("container", "inspect", "--format", "{{json .}}", container_id))
    assert data["Id"] == container_id
    assert data["Name"] == "/" + application_id
    assert data["Config"]["Labels"]["io.streamt.application-id"] == application_id
    assert data["Config"]["Labels"]["io.streamt.backend"] == journey.backend
    return data


def verify_renamed_container_refused(journey, runtime, observer, prepared, record, calls) -> None:
    """An extra app-labelled container stays visible after rename, without start."""
    application_id = prepared.application_id
    initial_name = application_id + "-probe-" + journey.token
    renamed_name = initial_name + "-renamed"
    owner_label = "io.streamt.close-probe-owner"
    assert json.loads(journey.docker("image", "inspect", "--format", "{{json .Config.Volumes}}", journey.image)) is None
    extra = journey.docker(
        "container", "create", "--pull=never", "--name", initial_name, "--network", "none",
        "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
        "--label", f"{owner_label}={journey.token}", "--label", f"io.streamt.application-id={application_id}",
        "--label", f"io.streamt.backend={prepared.backend_identity}", journey.image,
    )
    try:
        journey.docker("container", "rename", extra, renamed_name)
        observed = json.loads(journey.docker("container", "inspect", "--format", "{{json .}}", extra))
        assert observed["Name"] == "/" + renamed_name
        assert observed["State"]["Status"] == "created"
        assert observed["State"]["StartedAt"] == "0001-01-01T00:00:00Z"
        rejection = None
        with readonly_observer(runtime, calls):
            try:
                observer.observe(prepared, record)
            except KafkaStreamsReplacementObservationError as error:
                rejection = str(error)
            else:
                raise AssertionError("Renamed extra app-labelled container was not rejected")
        assert rejection is not None
        assert rejection == "Runner application has an unaccounted generation"
        journey.evidence["renamed_extra_container"] = {
            "id": extra, "never_started": True, "rejected_by_observer": True,
            "no_volume_mounts": True, "cleanup_complete": False,
        }
        journey.save()
    finally:
        observed = json.loads(journey.docker("container", "inspect", "--format", "{{json .}}", extra))
        assert observed["Id"] == extra
        assert observed["Name"] in ("/" + initial_name, "/" + renamed_name)
        assert observed["Config"]["Labels"][owner_label] == journey.token
        assert observed["Config"]["Labels"]["io.streamt.application-id"] == application_id
        assert observed["State"]["Status"] == "created"
        assert observed["State"]["StartedAt"] == "0001-01-01T00:00:00Z"
        assert not observed["Mounts"]
        journey.docker("container", "rm", extra)
        remaining = journey.docker("container", "ls", "--all", "--no-trunc", "--filter", f"id={extra}", "--format", "{{.ID}}")
        assert not remaining
        if "renamed_extra_container" in journey.evidence:
            journey.evidence["renamed_extra_container"]["cleanup_complete"] = True
            journey.save()


def prove_close(journey) -> None:
    directory = journey.root / "fresh"
    project = ProjectParser(directory).parse()
    desired_project = project.model_copy(deep=True)
    model = next(item for item in desired_project.models if item.name == "eligible_orders")
    assert "amount >= 100" in model.sql
    model.sql = model.sql.replace("amount >= 100", "amount >= 200", 1)
    desired_manifest = Compiler(desired_project).compile(dry_run=True)
    desired = parse_compiled_kafka_streams_job_artifact(desired_manifest.artifacts["kafka_streams_jobs"][0])
    assert project.runtime.kafka_streams is not None
    runtime = KafkaStreamsDeployer(project.runtime.kafka_streams, project.runtime.kafka,
                                   state_dir=directory / ".streamt")
    observer = KafkaStreamsReplacementObserver(runtime)
    backend = LocalDeploymentStateBackend(directory)
    address = StateAddress("local", project.project.name, "default")
    calls: list[list[str]] = []
    try:
        with backend.operation(address) as operation:
            snapshot = operation.observe()
            operation.ensure_ready(snapshot)
            resources = snapshot.state.state.resources
            matching = [item for item in resources.values() if item.physical_name == desired.application_id]
            assert len(matching) == 1
            record = matching[0]
            files_before = local_files(directory)
            with readonly_observer(runtime, calls):
                prepared = observer.prepare(desired, record)
                running = observer.observe(prepared, record)
            assert running.prior_container is not None
            assert running.prior_container.ready
            assert running.candidate_container is None
            assert running.progress.active_members == 1
            assert local_files(directory) == files_before
            container_id = prepared.prior_container_id
            before = inspect_fixture(journey, container_id, desired.application_id)
            assert before["State"]["Running"] is True
            journey.evidence["close_probe"] = {"prepared": prepared.to_dict(), "prepare_read_only": True}
            journey.save()
            verify_renamed_container_refused(journey, runtime, observer, prepared, record, calls)
            assert local_files(directory) == files_before
            # Test-owned lifecycle action: never a public replacement command.
            journey.docker("container", "kill", "--signal=TERM", container_id)
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                after = inspect_fixture(journey, container_id, desired.application_id)
                if after["State"]["Running"] is False:
                    break
                time.sleep(0.2)
            else:
                raise AssertionError("Exact fixture runner did not stop; no force removal authorized")
            with readonly_observer(runtime, calls):
                closed = observer.observe(prepared, record)
                status = runtime.docker.status_document(container_id)
                volume = runtime.docker.volume_witness(desired.application_id, prepared.backend_identity)
            surface = closed.prior_container
            assert surface is not None
            assert surface.cleanly_closed
            assert surface.exit_code == after["State"]["ExitCode"]
            assert surface.exit_code in (0, 143)
            assert surface.runner_state == "closed"
            assert surface.status_fresh
            assert surface.forced_exit is False
            assert closed.candidate_container is None
            assert closed.progress.active_members == 0
            closed.progress.require_at_least(prepared.progress)
            assert closed.progress == prepared.progress.__class__(
                prepared.progress.cluster_id, prepared.progress.input_topic_id, prepared.progress.output_topic_id,
                True, 0, prepared.progress.partitions,
            )
            assert surface.immutable_fingerprint == running.prior_container.immutable_fingerprint
            assert volume == prepared.volume.to_dict()
            assert before["Image"] == after["Image"]
            assert before["Config"] == after["Config"]
            assert before["HostConfig"] == after["HostConfig"]
            assert sorted(before["Mounts"], key=lambda item: item["Destination"]) == sorted(
                after["Mounts"], key=lambda item: item["Destination"],
            )
            started, updated, finished = (parse_utc_timestamp(value) for value in (
                after["State"]["StartedAt"], status["updated_at"], after["State"]["FinishedAt"],
            ))
            assert started <= updated <= finished
            assert before["State"]["StartedAt"] == after["State"]["StartedAt"]
            assert all(after["State"][key] is False for key in ("Running", "Paused", "Restarting", "OOMKilled", "Dead"))
            assert after["State"]["Error"] == ""
            assert after["RestartCount"] == 0
            operation.check_lock()
            assert operation.observe() == snapshot
            assert local_files(directory) == files_before
            journey.evidence["close_probe"].update({
                "accepted": True, "raw_exit_code": surface.exit_code,
                "process_state": after["State"], "status": status,
                "progress_before": prepared.progress.to_dict(), "progress_after": closed.progress.to_dict(),
                "volume_after": volume, "immutable_container_fingerprint": surface.immutable_fingerprint,
                "mounts_config_image_unchanged": True, "source_declarations_state_control_unchanged": True,
                "actual_ownership_record_under_lock": True, "observer_docker_read_commands": calls,
                "public_replacement_executed": False, "offset_reset_or_write_retry": False,
            })
            journey.save()
    finally:
        runtime.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--mode", choices=["source", "installed"], required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    utilities = runpy.run_path(str(Path(__file__).with_name("kafka_streams_journey.py")))
    journey = utilities["Journey"](args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence["scope"] = "Real public creation plus read-only observer clean-close proof; no replacement execution"
    journey.evidence["process_umask"] = "0077"
    try:
        journey.setup()
        journey.exercise("fresh")
        prove_close(journey)
        journey.evidence["accepted"] = True
    except BaseException as error:
        journey.evidence["accepted"] = False
        journey.evidence["failure"] = str(error)
        raise
    finally:
        journey.evidence["source_hashes_after"] = journey.source_hashes()
        journey.save()
        journey.cleanup()
    print(f"Real {args.mode} observer clean-close proof passed; cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
