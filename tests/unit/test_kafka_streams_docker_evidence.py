"""Read-only Docker evidence and dormant replacement-generation arguments."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from streamt.deployer import kafka_streams_docker as docker
from streamt.deployer.kafka_streams_evidence import KafkaStreamsVolumeEvidence
from streamt.deployer.kafka_streams_replacement import ReplacementGeneration
from tests.unit import test_kafka_streams_docker as docker_fixtures
from tests.unit.test_kafka_streams_docker import (
    APP,
    BACKEND,
    CONTAINER,
    IMAGE,
    SECRET,
    _archive,
    _container,
    _witness_volume,
)

cli = docker_fixtures.cli
runner = docker_fixtures.runner

GENERATION = ReplacementGeneration("4c4733ad-3cf9-4158-bc90-2497761e7f1b", 2, "sha256:" + "e" * 64)
VOLUME_COMMAND = ["volume", "inspect", "--format", "{{json .}}", APP + "-state"]
EXACT_LIST = ["container", "ls", "--all", "--no-trunc", "--filter", f"id={CONTAINER}", "--format", "{{.ID}}"]


def volume() -> KafkaStreamsVolumeEvidence:
    data = _witness_volume()
    return KafkaStreamsVolumeEvidence(APP + "-state", "local", data["CreatedAt"], APP, BACKEND, data["Labels"][docker.LABEL_VOLUME])


def with_generation():
    data = _container()
    data["Config"]["Labels"].update({
        docker.LABEL_OPERATION: GENERATION.operation_id,
        docker.LABEL_ACTION_INDEX: str(GENERATION.action_index),
        docker.LABEL_EVIDENCE: GENERATION.evidence_fingerprint,
    })
    return data


def mounted():
    data = _container()
    data["HostConfig"] = {"Tmpfs": {"/tmp": "rw,nosuid,nodev,noexec,size=64m,mode=1777"}}
    data["Mounts"] = [
        {"Type": "bind", "Source": "/private/plan.json", "Destination": "/run/streamt/plan.json", "RW": False},
        {"Type": "bind", "Source": "/private/client.properties", "Destination": "/run/streamt/client.properties", "RW": False},
        {"Type": "volume", "Name": APP + "-state", "Driver": "local", "Destination": "/var/lib/streamt/state", "RW": True},
    ]
    return data


@pytest.mark.parametrize(("raw", "expected"), [
    ("2026-09-05T10:00:00Z", "2026-09-05T10:00:00Z"),
    ("2026-09-05T10:00:00+00:00", "2026-09-05T10:00:00Z"),
    ("2026-09-05T10:00:00.123456789+00:00", "2026-09-05T10:00:00.123456789Z"),
    ("2026-09-05T10:00:00.000000001-04:00", "2026-09-05T14:00:00.000000001Z"),
    ("2026-01-01T00:15:00.120000000+01:00", "2025-12-31T23:15:00.120000000Z"),
    ("2026-12-31T23:59:59.999999999-00:30", "2027-01-01T00:29:59.999999999Z"),
])
def test_docker_timestamp_offsets_normalize_without_losing_nanoseconds(raw, expected):
    assert docker.normalize_docker_timestamp(raw) == expected


@pytest.mark.parametrize("raw", [None, True, {}, "", "2026-09-05", "2026-09-05T10:00:00", "2026-02-30T10:00:00Z",
                                "2026-09-05T10:00:00.1234567890Z", "2026-09-05T10:00:00+00:60",
                                "2026-09-05T10:00:00+24:00", "2026-09-05T10:00:00-00:00", SECRET,
                                "2026-09-05T10:00:00Z\n", "0001-01-01T00:00:00+01:00"])
def test_timestamp_invalid_or_unknown_offset_is_secret_neutral(raw):
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        docker.normalize_docker_timestamp(raw)
    assert SECRET not in str(caught.value)


def test_normalized_volume_witness_satisfies_durable_schema(runner, cli):
    data = _witness_volume()
    data["CreatedAt"] = "2026-09-05T06:00:00.123456789-04:00"
    cli.reply(VOLUME_COMMAND, data)
    actual = KafkaStreamsVolumeEvidence.from_dict(runner.volume_witness(APP, BACKEND))
    assert actual.created_at == "2026-09-05T10:00:00.123456789Z"


def test_exact_container_absence_requires_successful_id_listing(runner, cli):
    cli.reply(EXACT_LIST, b"")
    assert runner.inspect_exact(CONTAINER) is None
    assert cli.calls[-1][0] == EXACT_LIST
    assert all("name=" not in " ".join(args) for args, _ in cli.calls)


def test_renamed_old_container_remains_present_by_exact_id(runner, cli):
    data = _container()
    data["Name"] = "/renamed-old-runner"
    cli.reply(EXACT_LIST, CONTAINER.encode())
    cli.reply(["container", "inspect", "--format", "{{json .}}", CONTAINER], data)
    assert runner.inspect_exact(CONTAINER) == data
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.require_owned(data, APP, BACKEND)


@pytest.mark.parametrize("response", [b"short", (CONTAINER + "\n" + CONTAINER).encode(), ("f" * 64).encode(), b"\xff"])
def test_exact_id_ambiguity_never_becomes_absence(runner, cli, response):
    cli.reply(EXACT_LIST, response)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.inspect_exact(CONTAINER)


@pytest.mark.parametrize("failure", [subprocess.TimeoutExpired(["docker", SECRET], 7),
                                    subprocess.CompletedProcess(["docker"], 1, SECRET.encode(), SECRET.encode())])
def test_exact_id_provider_failure_is_not_absence(runner, cli, failure):
    cli.reply(EXACT_LIST, b"unused")
    cli.responses[tuple(EXACT_LIST)] = failure
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        runner.inspect_exact(CONTAINER)
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("identity", [None, True, "short", "--all", "b" * 63, "B" * 64])
def test_exact_id_invalid_input_never_reaches_docker(runner, cli, identity):
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.inspect_exact(identity)
    assert cli.calls == []


def test_exact_id_inspection_cannot_return_another_physical_container(runner, cli):
    cli.reply(EXACT_LIST, CONTAINER.encode())
    data = _container()
    data["Id"] = "f" * 64
    cli.reply(["container", "inspect", "--format", "{{json .}}", CONTAINER], data)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.inspect_exact(CONTAINER)


def test_generation_is_exact_or_absent_never_inferred_from_owner_labels(runner, cli):
    assert runner.generation(_container()) is None
    assert runner.generation(with_generation()) == GENERATION
    assert cli.calls == []


@pytest.mark.parametrize(("label", "value"), [
    (docker.LABEL_OPERATION, SECRET), (docker.LABEL_OPERATION, "00000000-0000-0000-0000-000000000000"),
    (docker.LABEL_OPERATION, GENERATION.operation_id.upper()), (docker.LABEL_ACTION_INDEX, "02"),
    (docker.LABEL_ACTION_INDEX, 2), (docker.LABEL_ACTION_INDEX, True), (docker.LABEL_ACTION_INDEX, "-1"),
    (docker.LABEL_ACTION_INDEX, "9223372036854775808"), (docker.LABEL_ACTION_INDEX, "2\n"),
    (docker.LABEL_EVIDENCE, "e" * 64), (docker.LABEL_EVIDENCE, SECRET),
])
def test_invalid_generation_is_not_downgraded_to_legacy(runner, cli, label, value):
    data = with_generation()
    data["Config"]["Labels"][label] = value
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        runner.generation(data)
    assert SECRET not in str(caught.value)
    assert cli.calls == []


@pytest.mark.parametrize("label", [docker.LABEL_OPERATION, docker.LABEL_ACTION_INDEX, docker.LABEL_EVIDENCE])
def test_partial_generation_binding_is_invalid(runner, cli, label):
    data = with_generation()
    data["Config"]["Labels"].pop(label)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.generation(data)


def test_plan_witness_preserves_exact_bytes_and_defensive_json(runner, cli):
    payload = b'{ "version": 1, "nested": {"value":"caf\\u00e9"} }\n'
    command = ["container", "cp", f"{CONTAINER}:/run/streamt/plan.json", "-"]
    cli.reply(command, _archive(payload, name="plan.json"))
    witness = runner.plan_witness(CONTAINER)
    assert witness.raw_bytes == payload
    assert witness.sha256 == "sha256:" + hashlib.sha256(payload).hexdigest()
    decoded = witness.document
    decoded["nested"]["value"] = "changed"
    assert witness.document["nested"]["value"] == "café"
    assert witness.sha256 != "sha256:" + hashlib.sha256(json.dumps(witness.document).encode()).hexdigest()
    assert [args for args, _ in cli.calls] == [command]
    assert "café" not in repr(witness)


@pytest.mark.parametrize("payload", [b'{"x":NaN}', b'{"x":Infinity}', b'{"x":-Infinity}', b'{"x":1e999}', b'{"x":"\\ud800"}', b'{"x":1,"x":2}',
                                    b'{} {}', b'[]', b'\xff', '{}'.encode('utf-16'), b'\xef\xbb\xbf{}',
                                    b'{"x":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}'],
                         ids=lambda payload: hashlib.sha256(payload).hexdigest()[:8])
def test_plan_witness_rejects_non_strict_json(runner, cli, payload):
    cli.reply(["container", "cp", f"{CONTAINER}:/run/streamt/plan.json", "-"], _archive(payload, name="plan.json"))
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.plan_witness(CONTAINER)


def test_plan_witness_size_limit_and_fixed_mount_name(runner, cli):
    command = ["container", "cp", f"{CONTAINER}:/run/streamt/plan.json", "-"]
    for payload, name in [(b" " * (1024 * 1024) + b"{}", "plan.json"), (b"{}", "client.properties")]:
        cli.reply(command, _archive(payload, name=name))
        with pytest.raises(docker.KafkaStreamsDockerError):
            runner.plan_witness(CONTAINER)


@pytest.mark.parametrize("tmpfs_mount", [False, True])
def test_fixed_mount_layout_proves_existing_named_volume_without_reading_properties(runner, cli, tmpfs_mount):
    data = mounted()
    if tmpfs_mount:
        data["Mounts"].append({"Type": "tmpfs", "Destination": "/tmp", "RW": True})
    cli.reply(VOLUME_COMMAND, _witness_volume())
    runner.validate_mounts(data, volume())
    assert [args for args, _ in cli.calls] == [["info", "--format", "{{json .ID}}"], VOLUME_COMMAND]


@pytest.mark.parametrize("problem", ["missing", "duplicate", "anonymous", "overlay", "extra", "plan-writable", "props-writable",
                                    "plan-relative", "same-bind", "state-ro", "state-bind", "state-name", "state-driver",
                                    "tmpfs-missing", "tmpfs-extra", "tmpfs-changed", "tmp-overlay", "wrong-owner"])
def test_mount_layout_mismatches_fail_before_volume_or_other_provider_reads(runner, cli, problem):
    data = mounted()
    mounts = data["Mounts"]
    if problem == "missing":
        mounts.pop(0)
    elif problem == "duplicate":
        mounts.append(copy.deepcopy(mounts[0]))
    elif problem in {"anonymous", "overlay", "extra"}:
        mounts.append({"Type": "volume", "Name": "anonymous", "Destination": {
            "anonymous": "/extra", "overlay": "/var/lib/streamt/state/subdir", "extra": "/run/streamt",
        }[problem], "RW": True})
    elif problem in {"plan-writable", "props-writable"}:
        mounts[0 if problem == "plan-writable" else 1]["RW"] = True
    elif problem == "plan-relative":
        mounts[0]["Source"] = "relative"
    elif problem == "same-bind":
        mounts[1]["Source"] = mounts[0]["Source"]
    elif problem.startswith("state-"):
        field, value = {"state-ro": ("RW", False), "state-bind": ("Type", "bind"),
                        "state-name": ("Name", "foreign"), "state-driver": ("Driver", "remote")}[problem]
        mounts[2][field] = value
    elif problem == "tmpfs-missing":
        data["HostConfig"].pop("Tmpfs")
    elif problem == "tmpfs-extra":
        data["HostConfig"]["Tmpfs"]["/extra"] = "rw"
    elif problem == "tmpfs-changed":
        data["HostConfig"]["Tmpfs"]["/tmp"] = "rw,exec"
    elif problem == "tmp-overlay":
        mounts.append({"Type": "bind", "Destination": "/tmp", "RW": True})
    else:
        data["Config"]["Labels"][docker.LABEL_APP] = "foreign"
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.validate_mounts(data, volume())
    assert cli.calls == []


@pytest.mark.parametrize("changed", ["token", "created_at"])
def test_same_named_recreated_volume_is_not_same_instance(runner, cli, changed):
    expected = replace(volume(), **{changed: {"token": "cb31cd98-e4c2-468e-8c01-8ae5b7596477", "created_at": "2026-09-05T09:59:59Z"}[changed]})
    cli.reply(VOLUME_COMMAND, _witness_volume())
    with pytest.raises(docker.KafkaStreamsDockerError, match="identity changed"):
        runner.validate_mounts(mounted(), expected)
    assert not any(args[:2] == ["volume", "create"] for args, _ in cli.calls)


def create_args(path: Path):
    return {"application_id": APP, "image_id": IMAGE, "network": "bridge", "plan_file": path / "plan.json",
            "properties_file": path / "client.properties", "state_volume": APP + "-state", "artifact_hash": "sha256:" + "f" * 64,
            "plan_hash": "sha256:" + "e" * 64, "backend": BACKEND, "input_topic_id": "AAAAAAAAAAAAAAAAAAAAAQ",
            "output_topic_id": "AAAAAAAAAAAAAAAAAAAAAg", "cluster_id": "cluster-unit"}


def test_replacement_generation_labels_are_atomic_and_volume_is_never_created(runner, cli, tmp_path, monkeypatch):
    original = cli.run

    def allow_create(command, **kwargs):
        if command[1:3] == ["container", "create"]:
            cli.responses[tuple(command[1:])] = CONTAINER.encode()
        return original(command, **kwargs)

    monkeypatch.setattr(docker.subprocess, "run", allow_create)
    cli.reply(["network", "inspect", "--format", "{{json .}}", "bridge"], {
        "Id": "1" * 64, "Name": "bridge", "Driver": "bridge", "Scope": "local",
    })
    cli.reply(VOLUME_COMMAND, _witness_volume())
    assert runner.create(**create_args(tmp_path), generation=GENERATION, expected_volume=volume()) == CONTAINER
    commands = [args for args, _ in cli.calls]
    assert not any(args[:2] in (["volume", "create"], ["volume", "ls"]) for args in commands)
    creations = [args for args in commands if args[:2] == ["container", "create"]]
    assert len(creations) == 1
    creation = creations[0]
    for label in (f"{docker.LABEL_OPERATION}={GENERATION.operation_id}",
                  f"{docker.LABEL_ACTION_INDEX}={GENERATION.action_index}",
                  f"{docker.LABEL_EVIDENCE}={GENERATION.evidence_fingerprint}"):
        assert creation.count(label) == 1
        assert creation[creation.index(label) - 1] == "--label"
    assert not any(args[:2] in (["container", "start"], ["container", "kill"], ["container", "rm"]) for args in commands)


@pytest.mark.parametrize("problem", ["missing-generation", "missing-volume", "wrong-generation-type", "wrong-volume-type", "foreign-volume"])
def test_replacement_requires_both_exact_typed_generation_and_volume_before_docker(runner, cli, tmp_path, problem):
    generation, expected = GENERATION, volume()
    if problem == "missing-generation":
        generation = None
    elif problem == "missing-volume":
        expected = None
    elif problem == "wrong-generation-type":
        generation = {"operation_id": GENERATION.operation_id}
    elif problem == "wrong-volume-type":
        expected = volume().to_dict()
    else:
        expected = replace(volume(), backend_identity="kafka-streams-docker:v1:" + "f" * 64)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.create(**create_args(tmp_path), generation=generation, expected_volume=expected)
    assert cli.calls == []


def test_missing_replacement_volume_is_not_recreated(runner, cli, tmp_path):
    cli.reply(["network", "inspect", "--format", "{{json .}}", "bridge"], {
        "Id": "1" * 64, "Name": "bridge", "Driver": "bridge", "Scope": "local",
    })
    cli.responses[tuple(VOLUME_COMMAND)] = subprocess.CompletedProcess(["docker"], 1, b"", SECRET.encode())
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        runner.create(**create_args(tmp_path), generation=GENERATION, expected_volume=volume())
    assert SECRET not in str(caught.value)
    assert not any(args[:2] in (["volume", "create"], ["container", "create"], ["container", "start"])
                   for args, _ in cli.calls)


@pytest.mark.skipif(os.environ.get("STREAMT_DOCKER_EVIDENCE_PROBE") != "1", reason="Explicit local never-started Docker fixture only")
def test_real_never_started_container_evidence_and_exact_cleanup(tmp_path):
    """No Kafka client or container start; mutate only freshly owned fixture objects."""
    owner = str(uuid.uuid4())
    application = "streamt-" + uuid.UUID(owner).hex
    volume_name = application + "-state"
    local = docker.LocalDockerRunner(timeout=15)
    image = local.image_id(os.environ["STREAMT_DOCKER_EVIDENCE_IMAGE"])
    backend = local.backend_identity("offline-docker-evidence-fixture")
    generation = ReplacementGeneration(owner, 0, "sha256:" + "e" * 64)
    assert local.inspect(application) is None
    listing = ["volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{volume_name}$"]
    assert local._run(listing).strip() == b""
    payload = b'{ "version": 1 }\n'
    plan, properties = tmp_path / "plan.json", tmp_path / "client.properties"
    plan.write_bytes(payload)
    properties.write_bytes(b"bootstrap.servers=never-contacted.invalid:9092\n")
    container_id = None
    report = {"owner_uuid": owner, "image_id": image, "volume_name": volume_name, "container_ever_started": False}
    try:
        local._run(["volume", "create", "--label", f"{docker.LABEL_APP}={application}",
                    "--label", f"{docker.LABEL_BACKEND}={backend}", "--label", f"{docker.LABEL_OPERATION}={owner}",
                    "--label", f"{docker.LABEL_VOLUME}={uuid.uuid4()}", volume_name])
        raw_volume = json.loads(local._run(["volume", "inspect", "--format", "{{json .}}", volume_name]))
        assert raw_volume["Labels"][docker.LABEL_OPERATION] == owner
        expected = KafkaStreamsVolumeEvidence.from_dict(local.volume_witness(application, backend))
        report.update({"docker_created_at": raw_volume["CreatedAt"], "canonical_created_at": expected.created_at})
        container_id = local.create(
            application_id=application, image_id=image, network="bridge", plan_file=plan, properties_file=properties,
            state_volume=volume_name, artifact_hash="sha256:" + "f" * 64,
            plan_hash="sha256:" + hashlib.sha256(payload).hexdigest(), backend=backend,
            input_topic_id="AAAAAAAAAAAAAAAAAAAAAQ", output_topic_id="AAAAAAAAAAAAAAAAAAAAAg",
            cluster_id="never-contacted-fixture", generation=generation, expected_volume=expected,
        )
        report["container_id"] = container_id
        exact = local.inspect_exact(container_id)
        assert exact is not None
        assert exact["State"]["Status"] == "created"
        assert exact["State"]["Running"] is False
        assert exact["State"]["StartedAt"] == "0001-01-01T00:00:00Z"
        assert local.require_owned(exact, application, backend) == container_id
        assert local.generation(exact) == generation
        local.validate_mounts(exact, expected)
        witness = local.plan_witness(container_id)
        assert witness.raw_bytes == payload
        assert witness.sha256 == "sha256:" + hashlib.sha256(payload).hexdigest()
        report.update({"plan_bytes_verified": True, "generation_verified": True, "mount_layout_verified": True})
    finally:
        # A create response could be lost; inspect only this pre-checked random name.
        existing = local.inspect(application)
        if existing is not None:
            exact_id = local.require_owned(existing, application, backend)
            assert local.generation(existing) == generation
            assert existing["State"]["Status"] == "created"
            assert existing["State"]["Running"] is False
            if container_id is not None:
                assert exact_id == container_id
            local.owned_command("remove", application, backend, expected_id=exact_id)
            assert local.inspect_exact(exact_id) is None
        if local._run(listing).strip():
            raw_volume = json.loads(local._run(["volume", "inspect", "--format", "{{json .}}", volume_name]))
            assert raw_volume["Name"] == volume_name
            assert raw_volume["Labels"][docker.LABEL_OPERATION] == owner
            assert raw_volume["Labels"][docker.LABEL_APP] == application
            assert raw_volume["Labels"][docker.LABEL_BACKEND] == backend
            local._run(["volume", "rm", volume_name])
        assert local._run(listing).strip() == b""
        report["exact_owned_cleanup_complete"] = True
        print("DOCKER_EVIDENCE_PROBE " + json.dumps(report, sort_keys=True))
