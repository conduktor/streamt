"""Independent provider surfaces for preparing and re-observing replacement."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from streamt.core.runtime import KafkaConfig, KafkaStreamsConfig
from streamt.deployer.kafka_streams import KafkaStreamsDeployer, runner_plan_bytes
from streamt.deployer.kafka_streams_docker import (
    LABEL_ACTION_INDEX,
    LABEL_APP,
    LABEL_ARTIFACT,
    LABEL_BACKEND,
    LABEL_EVIDENCE,
    LABEL_INPUT,
    LABEL_OPERATION,
    LABEL_OUTPUT,
    LABEL_PLAN,
    KafkaStreamsPlanWitness,
    LocalDockerRunner,
)
from streamt.deployer.kafka_streams_progress import (
    ApplicationProgress,
    KafkaStreamsProgress,
    PartitionProgress,
)
from streamt.deployer.kafka_streams_replacement import decide_replacement
from streamt.deployer.kafka_streams_replacement_observer import (
    KafkaStreamsReplacementObservationError,
    KafkaStreamsReplacementObserver,
)
from tests.unit.test_kafka_streams_operation_evidence import (
    APP,
    BACKEND,
    IMAGE,
    NETWORK,
    NEW_ID,
    OLD_ID,
    OPERATION,
    RESOURCE,
    _boundaries,
    _control,
    _evidence,
    _state,
)

SECRET = "private-client-property-do-not-echo"
ZERO = "0001-01-01T00:00:00Z"
STARTED = "2026-09-05T12:00:02.123456789Z"
UPDATED = "2026-09-05T12:00:03.123456789Z"
FINISHED = "2026-09-05T12:00:04.123456789Z"


def _data(*, candidate=False, process="running", exit_code=143):
    evidence = _evidence()
    artifact = evidence.desired_artifact if candidate else evidence.prior_artifact
    labels = {
        LABEL_APP: APP, LABEL_BACKEND: BACKEND, LABEL_ARTIFACT: artifact.checksum,
        LABEL_PLAN: artifact.plan_hash, LABEL_INPUT: evidence.progress.input_topic_id,
        LABEL_OUTPUT: evidence.progress.output_topic_id,
    }
    if candidate:
        labels.update({LABEL_OPERATION: OPERATION, LABEL_ACTION_INDEX: "0", LABEL_EVIDENCE: evidence.immutable_fingerprint})
    return {
        "Id": NEW_ID if candidate else OLD_ID, "Name": "/" + APP, "Image": IMAGE,
        "Created": "2026-09-05T12:00:01Z", "RestartCount": 0,
        "Config": {
            "Labels": labels, "Image": IMAGE, "User": "10001:10001",
            "Env": ["PATH=/opt/java/openjdk/bin:/usr/bin", "JAVA_HOME=/opt/java/openjdk"],
            "WorkingDir": "/opt/streamt/runner",
            "Entrypoint": ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/opt/streamt/runner/runner.jar"],
            "Cmd": ["--plan", "/run/streamt/plan.json", "--client-properties", "/run/streamt/client.properties",
                    "--application-id", APP, "--state-dir", "/var/lib/streamt/state",
                    "--expected-cluster-id", evidence.progress.cluster_id,
                    "--expected-input-topic-id", evidence.progress.input_topic_id,
                    "--expected-output-topic-id", evidence.progress.output_topic_id],
        },
        "HostConfig": {
            "NetworkMode": NETWORK, "ReadonlyRootfs": True, "Privileged": False, "AutoRemove": False,
            "CapDrop": ["ALL"], "CapAdd": None, "SecurityOpt": ["no-new-privileges"],
            "RestartPolicy": {"Name": "no", "MaximumRetryCount": 0},
            "Memory": 536870912, "NanoCpus": 1000000000, "PidsLimit": 128,
            "Tmpfs": {"/tmp": "rw,nosuid,nodev,noexec,size=64m,mode=1777"},
        },
        "NetworkSettings": {"Networks": {NETWORK: {"NetworkID": NETWORK if process != "created" else ""}}},
        "Mounts": [
            {"Type": "bind", "Source": "/private/plan.json", "Destination": "/run/streamt/plan.json", "RW": False},
            {"Type": "bind", "Source": "/private/client.properties", "Destination": "/run/streamt/client.properties", "RW": False},
            {"Type": "volume", "Name": APP + "-state", "Destination": "/var/lib/streamt/state", "Driver": "local", "RW": True},
        ],
        "State": {
            "Status": process, "Running": process == "running", "Paused": False, "Restarting": False,
            "OOMKilled": False, "Dead": False, "Error": "", "ExitCode": exit_code if process == "exited" else 0,
            "StartedAt": ZERO if process == "created" else STARTED,
            "FinishedAt": FINISHED if process == "exited" else ZERO,
        },
    }


def _fixture(tmp_path, *, candidate=False, process="running", exit_code=143):
    evidence = _evidence()
    runtime = object.__new__(KafkaStreamsDeployer)
    runtime.config = KafkaStreamsConfig(image=IMAGE, network="bridge", initial_offset="earliest")
    runtime.kafka = KafkaConfig(bootstrap_servers="unit.invalid:9092")
    runtime.state_dir = tmp_path / ".streamt"
    runtime.cluster_id, runtime.backend_identity = evidence.progress.cluster_id, BACKEND
    runtime.image_id, runtime.network_id = IMAGE, NETWORK
    runtime.docker = MagicMock(spec=LocalDockerRunner)
    runtime.progress = MagicMock(spec=KafkaStreamsProgress)
    runtime.docker.backend_identity.return_value = BACKEND
    runtime.docker.image_id.return_value = IMAGE
    runtime.docker.network_id.return_value = NETWORK
    runtime.progress.cluster_id.return_value = runtime.cluster_id
    runtime.docker.volume_witness.return_value = evidence.volume.to_dict()
    runtime.docker.require_owned.side_effect = LocalDockerRunner.require_owned
    runtime.docker.generation.side_effect = LocalDockerRunner.generation
    runtime.docker.require_volume.side_effect = lambda expected: LocalDockerRunner.require_volume(runtime.docker, expected)
    runtime.docker.validate_mounts.side_effect = lambda data, expected: LocalDockerRunner.validate_mounts(runtime.docker, data, expected)
    runtime.docker.validate_process_environment.side_effect = lambda data, expected: LocalDockerRunner.validate_process_environment(runtime.docker, data, expected)
    runtime.docker._run.return_value = json.dumps({
        "Id": IMAGE, "Config": {
            "Env": ["PATH=/opt/java/openjdk/bin:/usr/bin", "JAVA_HOME=/opt/java/openjdk"],
            "WorkingDir": "/opt/streamt/runner",
        },
    }).encode()
    data = _data(candidate=candidate, process=process, exit_code=exit_code)
    runtime.docker.inspect_exact.return_value = None if candidate else data
    runtime.docker.inspect.return_value = data
    runtime.docker.application_containers.return_value = (data["Id"],)
    snapshot = evidence.desired_artifact if candidate else evidence.prior_artifact
    runtime.docker.plan_witness.return_value = KafkaStreamsPlanWitness(runner_plan_bytes(snapshot.artifact.plan))
    runtime.docker.status_document.return_value = {
        "application_id": APP, "runner_version": "0.1.1", "plan_version": 1, "plan_sha256": snapshot.plan_hash,
        "state": "closed" if process == "exited" else "running", "reason": None, "updated_at": UPDATED,
        "cluster_id": runtime.cluster_id, "input_topic_id": evidence.progress.input_topic_id,
        "output_topic_id": evidence.progress.output_topic_id,
    }
    runtime.progress.observe.return_value = ApplicationProgress(
        runtime.cluster_id, evidence.progress.input_topic_id, evidence.progress.output_topic_id,
        True, int(process == "running"), (PartitionProgress(0, 0, 100, 10),),
    )
    return KafkaStreamsReplacementObserver(runtime), runtime, data, _state().resources[RESOURCE]


def _no_mutation(runtime):
    runtime.docker.create.assert_not_called()
    runtime.docker.ensure_state_volume.assert_not_called()
    runtime.docker.owned_command.assert_not_called()
    runtime.progress.initialize.assert_not_called()
    assert not runtime.state_dir.exists()


def test_prepare_reconstructs_mounted_prior_and_keeps_reviewed_progress(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path)
    initial = runtime.progress.observe.return_value
    runtime.progress.observe.side_effect = [initial, replace(initial, partitions=(PartitionProgress(0, 0, 110, 11),))]
    result = observer.prepare(_evidence().desired_artifact.artifact, owner)
    assert result == _evidence()
    assert result.progress.partitions[0].committed == 10
    _no_mutation(runtime)


@pytest.mark.parametrize("exit_code", [0, 143])
def test_exact_term_close_preserves_raw_code_into_checkpoint(tmp_path, exit_code):
    observer, runtime, _data, owner = _fixture(tmp_path, process="exited", exit_code=exit_code)
    observed = observer.observe(_evidence(), owner)
    assert observed.prior_container.cleanly_closed
    assert observed.prior_container.exit_code == exit_code
    decision = decide_replacement(_control(_boundaries()[:1]), 0, observed)
    assert decision.step == "record_old_closed"
    assert decision.checkpoint.exit_code == exit_code
    _no_mutation(runtime)


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("stamp", ["2026-09-05T12:00:02.123456788Z", "2026-09-05T12:00:04.123456790Z"])
def test_status_outside_exact_start_finish_window_never_proves_close(tmp_path, candidate, stamp):
    observer, runtime, _data, owner = _fixture(tmp_path, candidate=candidate, process="exited")
    runtime.docker.status_document.return_value["updated_at"] = stamp
    observed = observer.observe(_evidence(), owner)
    surface = observed.candidate_container if candidate else observed.prior_container
    assert surface.status_fresh is False
    assert surface.cleanly_closed is False
    _no_mutation(runtime)


def test_unstarted_candidate_does_not_read_old_shared_status(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path, candidate=True, process="created")
    runtime.docker.status_document.side_effect = ValueError(SECRET)
    observed = observer.observe(_evidence(), owner)
    assert observed.candidate_container.runner_state is None
    assert not observed.candidate_container.status_fresh
    runtime.docker.status_document.assert_not_called()
    _no_mutation(runtime)


@pytest.mark.parametrize(("path", "value"), [
    (("Image",), "sha256:" + "f" * 64), (("Name",), "/foreign"),
    (("Created",), ZERO), (("Created",), FINISHED), (("RestartCount",), True), (("RestartCount",), 1),
    (("Config", "Image"), "latest"), (("Config", "Entrypoint"), ["sh"]), (("Config", "Cmd"), []),
    (("Config", "WorkingDir"), "/var/lib/streamt/state"), (("Config", "Env"), None),
    *(( ("Config", "Env"), [f"{key}={SECRET}"] ) for key in (
        "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "LD_PRELOAD", "PATH",
    )),
    (("Config", "User"), "0"), (("Config", "Labels", LABEL_PLAN), "sha256:" + "f" * 64),
    (("Config", "Labels", LABEL_ARTIFACT), "sha256:" + "f" * 64), (("Config", "Labels", LABEL_INPUT), SECRET),
    (("HostConfig", "ReadonlyRootfs"), False), (("HostConfig", "Privileged"), True),
    (("HostConfig", "CapAdd"), ["SYS_ADMIN"]), (("HostConfig", "CapDrop"), []),
    (("HostConfig", "SecurityOpt"), []), (("HostConfig", "AutoRemove"), True),
    (("HostConfig", "RestartPolicy", "MaximumRetryCount"), False),
    (("HostConfig", "NetworkMode"), "host"), (("HostConfig", "Memory"), True),
    (("HostConfig", "NanoCpus"), 0), (("HostConfig", "PidsLimit"), -1),
    (("HostConfig", "Tmpfs"), {}), (("Mounts",), []),
    (("NetworkSettings", "Networks"), {}),
    (("NetworkSettings", "Networks", NETWORK, "NetworkID"), "f" * 64),
    (("State", "Status"), "dead"), (("State", "Running"), True), (("State", "Paused"), True),
    (("State", "Restarting"), True), (("State", "OOMKilled"), True), (("State", "Dead"), True),
    (("State", "Error"), SECRET), (("State", "ExitCode"), True), (("State", "ExitCode"), -1),
    (("State", "ExitCode"), 256), (("State", "StartedAt"), ZERO), (("State", "FinishedAt"), ZERO),
])
def test_inconsistent_execution_surfaces_fail_before_any_mutation(tmp_path, path, value):
    observer, runtime, data, owner = _fixture(tmp_path, process="exited")
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(KafkaStreamsReplacementObservationError) as caught:
        observer.observe(_evidence(), owner)
    assert SECRET not in str(caught.value)
    _no_mutation(runtime)


@pytest.mark.parametrize("field", ["Status", "Running", "Paused", "Restarting", "OOMKilled", "Dead", "Error", "ExitCode", "StartedAt", "FinishedAt"])
def test_missing_process_evidence_is_not_a_clean_exit(tmp_path, field):
    observer, runtime, data, owner = _fixture(tmp_path, process="exited")
    del data["State"][field]
    with pytest.raises(KafkaStreamsReplacementObservationError):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


@pytest.mark.parametrize("field", ["application_id", "runner_version", "plan_version", "plan_sha256", "state", "reason", "updated_at", "cluster_id", "input_topic_id", "output_topic_id"])
def test_missing_status_evidence_is_not_a_clean_exit(tmp_path, field):
    observer, runtime, _data, owner = _fixture(tmp_path, process="exited")
    del runtime.docker.status_document.return_value[field]
    with pytest.raises(KafkaStreamsReplacementObservationError):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


@pytest.mark.parametrize("code", [1, 3, 130, 137, 255])
def test_non_clean_exit_code_is_observed_but_not_accepted(tmp_path, code):
    observer, runtime, _data, owner = _fixture(tmp_path, process="exited", exit_code=code)
    observed = observer.observe(_evidence(), owner)
    assert observed.prior_container.exit_code == code
    assert not observed.prior_container.cleanly_closed
    assert decide_replacement(_control(_boundaries()[:1]), 0, observed).step == "blocked"
    _no_mutation(runtime)


@pytest.mark.parametrize("method", ["verify_daemon", "inspect", "inspect_exact", "plan_witness", "volume_witness", "status_document", "application_containers", "validate_process_environment"])
def test_provider_errors_never_become_absence_or_echo_secrets(tmp_path, method):
    observer, runtime, _data, owner = _fixture(tmp_path)
    getattr(runtime.docker, method).side_effect = RuntimeError(SECRET)
    with pytest.raises(KafkaStreamsReplacementObservationError) as caught:
        observer.observe(_evidence(), owner)
    assert SECRET not in str(caught.value)
    _no_mutation(runtime)


def test_raw_plan_whitespace_cannot_be_normalized_into_prior_checksum(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path)
    raw = runner_plan_bytes(_evidence().prior_artifact.artifact.plan)
    runtime.docker.plan_witness.return_value = KafkaStreamsPlanWitness(b" " + raw)
    with pytest.raises(KafkaStreamsReplacementObservationError, match="Mounted prior plan"):
        observer.prepare(_evidence().desired_artifact.artifact, owner)
    _no_mutation(runtime)


def test_changed_volume_instance_is_not_recreated(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path)
    runtime.docker.volume_witness.return_value["token"] = "00000000-0000-4000-8000-000000000003"
    with pytest.raises(KafkaStreamsReplacementObservationError, match="volume instance"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


def test_external_ownership_rejects_before_provider_reads(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path)
    desired = _evidence().desired_artifact.artifact
    desired = replace(desired, ownership=replace(desired.ownership, mode="external"))
    with pytest.raises(KafkaStreamsReplacementObservationError, match="ownership"):
        observer.prepare(desired, owner)
    runtime.docker.verify_daemon.assert_not_called()
    runtime.progress.observe.assert_not_called()
    _no_mutation(runtime)


def test_same_name_cannot_hide_renamed_old_container(tmp_path):
    observer, runtime, data, owner = _fixture(tmp_path)
    old = copy.deepcopy(data)
    old["Name"] = "/renamed-old"
    runtime.docker.inspect_exact.return_value = old
    runtime.docker.inspect.return_value = _data(candidate=True)
    with pytest.raises(KafkaStreamsReplacementObservationError):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


def test_slot_changed_between_reads_cannot_be_used_for_a_transition(tmp_path):
    observer, runtime, data, owner = _fixture(tmp_path)
    runtime.docker.inspect.side_effect = [data, None]
    with pytest.raises(KafkaStreamsReplacementObservationError, match="slot changed"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("drift", ["exit", "restart", "status"])
def test_same_id_process_drift_cannot_survive_the_final_read(tmp_path, candidate, drift):
    observer, runtime, data, owner = _fixture(tmp_path, candidate=candidate)
    final = copy.deepcopy(data)
    initial_status = runtime.docker.status_document.return_value
    final_status = copy.deepcopy(initial_status)
    if drift == "exit":
        final["State"].update(Status="exited", Running=False, ExitCode=1, FinishedAt=FINISHED)
        final_status.update(state="failed", reason="runtime_error")
    elif drift == "restart":
        final["State"]["StartedAt"] = UPDATED
    else:
        final_status.update(state="failed", reason="runtime_error")
    runtime.docker.inspect.side_effect = [data, final]
    runtime.docker.status_document.side_effect = [initial_status, final_status]
    with pytest.raises(KafkaStreamsReplacementObservationError, match="process changed"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


@pytest.mark.parametrize("baseline", [None, {}, {"Env": None}, {"Env": ["PATH=a", "PATH=b"]}])
def test_missing_or_ambiguous_image_environment_is_not_trusted(tmp_path, baseline):
    observer, runtime, _data, owner = _fixture(tmp_path)
    runtime.docker._run.return_value = json.dumps({"Id": IMAGE, "Config": baseline}).encode()
    with pytest.raises(KafkaStreamsReplacementObservationError):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("inventory", [(), (OLD_ID, NEW_ID), [OLD_ID], None])
def test_incomplete_or_extra_generation_inventory_blocks_transition(tmp_path, candidate, inventory):
    observer, runtime, _data, owner = _fixture(tmp_path, candidate=candidate)
    runtime.docker.application_containers.return_value = inventory
    with pytest.raises(KafkaStreamsReplacementObservationError, match="unaccounted generation"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


def test_renamed_candidate_with_empty_name_slot_is_not_absence(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path, candidate=True, process="created")
    runtime.docker.inspect.return_value = None
    with pytest.raises(KafkaStreamsReplacementObservationError, match="unaccounted generation"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


def test_labelled_inventory_drift_at_final_read_blocks_transition(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path)
    runtime.docker.application_containers.side_effect = [(OLD_ID,), (OLD_ID, NEW_ID)]
    with pytest.raises(KafkaStreamsReplacementObservationError, match="inventory changed"):
        observer.observe(_evidence(), owner)
    _no_mutation(runtime)


def test_complete_absence_after_durable_removal_can_authorize_one_candidate(tmp_path):
    observer, runtime, _data, owner = _fixture(tmp_path, process="exited")
    runtime.docker.inspect_exact.return_value = None
    runtime.docker.inspect.return_value = None
    runtime.docker.application_containers.return_value = ()
    observed = observer.observe(_evidence(), owner)
    assert observed.prior_container is None
    assert observed.candidate_container is None
    runtime.docker.plan_witness.assert_not_called()
    runtime.docker.status_document.assert_not_called()
    runtime.progress.observe.return_value = replace(
        runtime.progress.observe.return_value, partitions=(PartitionProgress(0, 0, 100, 20),),
    )
    observed = observer.observe(_evidence(), owner)
    decision = decide_replacement(_control(_boundaries()[:3]), 0, observed)
    assert decision.step == "create_candidate"
    _no_mutation(runtime)


@pytest.mark.parametrize("payload", [b"short\n", b"\xff", (OLD_ID + "\n" + OLD_ID).encode()])
def test_docker_inventory_rejects_incomplete_malformed_or_duplicate_ids(payload):
    docker = object.__new__(LocalDockerRunner)
    docker.verify_daemon = MagicMock()
    docker._run = MagicMock(return_value=payload)
    with pytest.raises(ValueError, match="inventory"):
        docker.application_containers(APP)


@pytest.mark.parametrize("ids", [(), (OLD_ID,), (NEW_ID, OLD_ID)])
def test_docker_inventory_uses_all_full_ids_and_application_label(ids):
    docker = object.__new__(LocalDockerRunner)
    docker.verify_daemon = MagicMock()
    docker._run = MagicMock(return_value="\n".join(ids).encode())
    assert docker.application_containers(APP) == tuple(sorted(ids))
    docker.verify_daemon.assert_called_once_with()
    docker._run.assert_called_once_with([
        "container", "ls", "--all", "--no-trunc", "--filter", f"label={LABEL_APP}={APP}", "--format", "{{.ID}}",
    ])
