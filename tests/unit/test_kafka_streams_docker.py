"""Adversarial unit checks for the local-only, owned-object Docker boundary."""

from __future__ import annotations

import io
import json
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pytest

from streamt.deployer import kafka_streams_docker as docker

APP = "streamt-" + "a" * 32
CONTAINER = "b" * 64
IMAGE = "sha256:" + "c" * 64
BACKEND = "kafka-streams-docker:v1:" + "d" * 64
SECRET = "provider-private-password-never-print"
ENDPOINT = "unix:///streamt-test-only/docker.sock"


class DockerCLI:
    """Never invokes a Docker binary or contacts a daemon."""

    def __init__(self) -> None:
        self.responses: dict[tuple[str, ...], object] = {
            ("info", "--format", "{{json .ID}}"): b'"daemon-unit-test"',
        }
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def run(self, command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        assert command[0] == "/test-only/docker"
        assert not kwargs.get("shell")
        assert kwargs["capture_output"] is True
        assert kwargs["check"] is False
        assert kwargs["timeout"] > 0
        args = command[1:]
        self.calls.append((args, kwargs))
        assert tuple(args) in self.responses, f"Unexpected Docker command: {args}"
        result = self.responses[tuple(args)]
        if isinstance(result, BaseException):
            raise result
        if isinstance(result, subprocess.CompletedProcess):
            return result
        assert isinstance(result, bytes)
        return subprocess.CompletedProcess(command, 0, result, b"")

    def reply(self, args: list[str], value: object) -> None:
        self.responses[tuple(args)] = value if isinstance(value, bytes) else json.dumps(value).encode()


@pytest.fixture
def cli(monkeypatch: pytest.MonkeyPatch) -> DockerCLI:
    fake = DockerCLI()
    monkeypatch.setattr(docker.shutil, "which", lambda _name: "/test-only/docker")
    monkeypatch.setattr(docker.subprocess, "run", fake.run)
    monkeypatch.setenv("DOCKER_HOST", ENDPOINT)
    monkeypatch.delenv("DOCKER_CONTEXT", raising=False)
    return fake


@pytest.fixture
def runner(cli: DockerCLI) -> docker.LocalDockerRunner:
    result = docker.LocalDockerRunner(timeout=7)
    cli.calls.clear()
    return result


def _container(*, running: object = False) -> dict[str, object]:
    return {
        "Id": CONTAINER,
        "Name": "/" + APP,
        "Config": {"Labels": {docker.LABEL_APP: APP, docker.LABEL_BACKEND: BACKEND}},
        "State": {"Running": running},
    }


def _inspection(cli: DockerCLI, data: dict[str, object] | None) -> None:
    cli.reply(["container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{APP}$",
               "--format", "{{.ID}}"], b"" if data is None else CONTAINER.encode())
    if data is not None:
        cli.reply(["container", "inspect", "--format", "{{json .}}", CONTAINER], data)


def _image() -> dict[str, object]:
    return {"Id": IMAGE, "Config": {"Volumes": None, "Labels": {
        "io.streamt.runner.version": docker.RUNNER_VERSION,
        "io.streamt.plan.version": docker.PLAN_VERSION,
    }}}


def _volume() -> dict[str, object]:
    return {"Name": APP + "-state", "Driver": "local", "Options": None,
            "Labels": {docker.LABEL_APP: APP, docker.LABEL_BACKEND: BACKEND}}


@pytest.mark.parametrize("endpoint", ["tcp://localhost:2375", "ssh://remote.invalid", "npipe://x",
                                      "https://remote.invalid", "unix://relative", "unix:///x\nsecret"])
def test_remote_or_malformed_daemon_refused_before_info(
    cli: DockerCLI, monkeypatch: pytest.MonkeyPatch, endpoint: str,
) -> None:
    monkeypatch.setenv("DOCKER_HOST", endpoint)
    with pytest.raises(docker.KafkaStreamsDockerError):
        docker.LocalDockerRunner()
    assert cli.calls == []


def test_context_endpoint_is_resolved_once_then_frozen(
    cli: DockerCLI, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DOCKER_HOST")
    monkeypatch.setenv("DOCKER_CONTEXT", "initial-context")
    cli.reply(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"], ENDPOINT)
    runner = docker.LocalDockerRunner()
    monkeypatch.setenv("DOCKER_HOST", "ssh://later-remote.invalid")
    monkeypatch.setenv("DOCKER_CONTEXT", "later-context")
    runner.verify_daemon()
    assert runner.endpoint == ENDPOINT
    for _args, kwargs in cli.calls[1:]:
        assert kwargs["env"]["DOCKER_HOST"] == ENDPOINT
        assert "DOCKER_CONTEXT" not in kwargs["env"]


@pytest.mark.parametrize("response", [b"invalid", b"null", b"{}", b'"tcp://remote.invalid"'])
def test_unusable_context_is_not_a_default_daemon_fallback(
    cli: DockerCLI, monkeypatch: pytest.MonkeyPatch, response: bytes,
) -> None:
    monkeypatch.delenv("DOCKER_HOST")
    cli.reply(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"], response)
    with pytest.raises(docker.KafkaStreamsDockerError):
        docker.LocalDockerRunner()
    assert len(cli.calls) == 1


def test_missing_docker_has_actionable_secret_neutral_error(
    cli: DockerCLI, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(docker.shutil, "which", lambda _name: None)
    with pytest.raises(docker.KafkaStreamsDockerError, match="Docker CLI is required"):
        docker.LocalDockerRunner()
    assert cli.calls == []


@pytest.mark.parametrize("failure", [
    OSError(SECRET), subprocess.TimeoutExpired(["docker", SECRET], 7),
    subprocess.CompletedProcess(["docker"], 1, SECRET.encode(), SECRET.encode()),
])
def test_docker_failures_never_echo_provider_output(
    runner: docker.LocalDockerRunner, cli: DockerCLI, failure: object,
) -> None:
    cli.responses[("info", "--format", "{{json .ID}}")] = failure
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        runner.verify_daemon()
    assert SECRET not in str(caught.value)


def test_backend_identity_is_secret_neutral_and_binds_both_systems(
    runner: docker.LocalDockerRunner, cli: DockerCLI,
) -> None:
    first = runner.backend_identity(SECRET)
    assert SECRET not in first
    assert ENDPOINT not in first
    assert first == runner.backend_identity(SECRET)
    assert first != runner.backend_identity("different-cluster")
    cli.reply(["info", "--format", "{{json .ID}}"], "changed-daemon")
    with pytest.raises(docker.KafkaStreamsDockerError, match="identity changed"):
        runner.verify_daemon()


@pytest.mark.parametrize("reference", [IMAGE, "example.invalid/runner@sha256:" + "e" * 64])
def test_inspects_only_already_present_immutable_runner_images(
    runner: docker.LocalDockerRunner, cli: DockerCLI, reference: str,
) -> None:
    cli.reply(["image", "inspect", "--format", "{{json .}}", reference], _image())
    assert runner.image_id(reference) == IMAGE
    assert len(cli.calls) == 1
    assert cli.calls[0][0][:2] == ["image", "inspect"]


@pytest.mark.parametrize("reference", ["runner:latest", "runner:v1", "sha256:abc", "--help",
                                       "sha256:" + "A" * 64, "runner@sha256:" + "f" * 63])
def test_mutable_or_malformed_image_ids_fail_without_docker(
    runner: docker.LocalDockerRunner, cli: DockerCLI, reference: str,
) -> None:
    with pytest.raises(docker.KafkaStreamsDockerError, match="immutable"):
        runner.image_id(reference)
    assert cli.calls == []


@pytest.mark.parametrize("data", [
    {}, {"Id": "short", "Config": {}}, {"Id": IMAGE, "Config": None},
    {"Id": IMAGE, "Config": {"Labels": {}}},
    {"Id": IMAGE, "Config": {"Volumes": {"/data": {}}, "Labels": {
        "io.streamt.runner.version": docker.RUNNER_VERSION,
        "io.streamt.plan.version": docker.PLAN_VERSION,
    }}},
])
def test_runner_contract_and_anonymous_volume_sources_are_checked(
    runner: docker.LocalDockerRunner, cli: DockerCLI, data: dict[str, object],
) -> None:
    cli.reply(["image", "inspect", "--format", "{{json .}}", IMAGE], data)
    with pytest.raises(docker.KafkaStreamsDockerError, match="compatible fixed"):
        runner.image_id(IMAGE)


def test_raw_image_id_cannot_resolve_to_another_image(
    runner: docker.LocalDockerRunner, cli: DockerCLI,
) -> None:
    data = _image()
    data["Id"] = "sha256:" + "e" * 64
    cli.reply(["image", "inspect", "--format", "{{json .}}", IMAGE], data)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.image_id(IMAGE)


@pytest.mark.parametrize("application_id", ["", "--all", "streamt-abc", "streamt-" + "A" * 32,
                                             APP + "\n", "foreign-container"])
def test_invalid_application_identity_never_reaches_docker(
    runner: docker.LocalDockerRunner, cli: DockerCLI, application_id: str,
) -> None:
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.inspect(application_id)
    assert cli.calls == []


def test_absence_requires_successful_exact_name_listing(
    runner: docker.LocalDockerRunner, cli: DockerCLI,
) -> None:
    _inspection(cli, None)
    assert runner.inspect(APP) is None
    assert len(cli.calls) == 1


@pytest.mark.parametrize("response", [b"short", (CONTAINER + "\n" + "d" * 64).encode(), b"\xff"])
def test_ambiguous_or_malformed_container_listing_is_not_absence(
    runner: docker.LocalDockerRunner, cli: DockerCLI, response: bytes,
) -> None:
    _inspection(cli, None)
    cli.reply(cli_args := ["container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{APP}$",
                           "--format", "{{.ID}}"], response)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.inspect(APP)
    assert [args for args, _kwargs in cli.calls] == [cli_args]


@pytest.mark.parametrize("mismatch", ["name", "id", "app-label", "backend-label", "labels"])
def test_lifecycle_refuses_foreign_or_replaced_containers(
    runner: docker.LocalDockerRunner, cli: DockerCLI, mismatch: str,
) -> None:
    data = _container()
    if mismatch == "name":
        data["Name"] = "/other"
    elif mismatch == "id":
        data["Id"] = "e" * 64
    else:
        labels = {docker.LABEL_APP: APP, docker.LABEL_BACKEND: BACKEND}
        if mismatch == "app-label":
            labels[docker.LABEL_APP] = "foreign-app"
        elif mismatch == "backend-label":
            labels[docker.LABEL_BACKEND] = "foreign-backend"
        data["Config"] = {"Labels": None if mismatch == "labels" else labels}
    _inspection(cli, data)
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.owned_command("remove", APP, BACKEND, expected_id=CONTAINER)
    assert not any(args[:2] == ["container", "rm"] for args, _kwargs in cli.calls)


@pytest.mark.parametrize(("operation", "mutation"), [
    ("start", ["container", "start", CONTAINER]),
    ("term", ["container", "kill", "--signal=TERM", CONTAINER]),
    ("remove", ["container", "rm", CONTAINER]),
])
def test_owned_lifecycle_uses_exact_id_without_force_or_volume_deletion(
    runner: docker.LocalDockerRunner, cli: DockerCLI, operation: str, mutation: list[str],
) -> None:
    _inspection(cli, _container())
    cli.reply(mutation, b"")
    runner.owned_command(operation, APP, BACKEND, expected_id=CONTAINER)
    assert cli.calls[-1][0] == mutation
    assert all("--force" not in args and "-f" not in args and "-v" not in args
               and args[:2] != ["volume", "rm"] for args, _kwargs in cli.calls)


@pytest.mark.parametrize("running", [True, None, 0, "false"])
def test_removal_never_forces_a_running_or_unknown_state(
    runner: docker.LocalDockerRunner, cli: DockerCLI, running: object,
) -> None:
    _inspection(cli, _container(running=running))
    with pytest.raises(docker.KafkaStreamsDockerError, match="running runner"):
        runner.owned_command("remove", APP, BACKEND, expected_id=CONTAINER)
    assert not any(args[:2] == ["container", "rm"] for args, _kwargs in cli.calls)


@pytest.mark.parametrize("foreign", [False, True])
def test_existing_volume_requires_exact_owner_and_is_never_deleted(
    runner: docker.LocalDockerRunner, cli: DockerCLI, foreign: bool,
) -> None:
    name = APP + "-state"
    cli.reply(["volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{name}$"], name.encode())
    data = _volume()
    if foreign:
        data["Labels"] = {docker.LABEL_APP: "other"}
    cli.reply(["volume", "inspect", "--format", "{{json .}}", name], data)
    if foreign:
        with pytest.raises(docker.KafkaStreamsDockerError, match="ownership binding"):
            runner.ensure_state_volume(APP, BACKEND)
    else:
        assert runner.ensure_state_volume(APP, BACKEND) == name
    assert all(args[:2] not in (["volume", "create"], ["volume", "rm"])
               for args, _kwargs in cli.calls)


def _archive(content: bytes, *, second: bool = False, symlink: bool = False, name: str = "status.json") -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        member = tarfile.TarInfo(name)
        member.size = len(content)
        if symlink:
            member.type = tarfile.SYMTYPE
            member.linkname = "/private/host-data"
        archive.addfile(member, io.BytesIO(content))
        if second:
            other = tarfile.TarInfo("extra")
            archive.addfile(other, io.BytesIO())
    return output.getvalue()


@pytest.mark.parametrize("payload", [
    b"not a tar", _archive(b"{}", second=True), _archive(b"{}", symlink=True),
    _archive(b"x" * 8193), _archive(b"[]"), _archive(SECRET.encode()),
])
def test_status_document_rejects_malformed_archives_without_echoing_payload(
    runner: docker.LocalDockerRunner, cli: DockerCLI, payload: bytes,
) -> None:
    cli.reply(["container", "cp", f"{CONTAINER}:/var/lib/streamt/state/status.json", "-"], payload)
    with pytest.raises(docker.KafkaStreamsDockerError) as caught:
        runner.status_document(CONTAINER)
    assert SECRET not in str(caught.value)


def test_status_is_read_in_memory_not_extracted_to_host(
    runner: docker.LocalDockerRunner, cli: DockerCLI,
) -> None:
    cli.reply(["container", "cp", f"{CONTAINER}:/var/lib/streamt/state/status.json", "-"],
              _archive(b'{"state":"RUNNING"}'))
    assert runner.status_document(CONTAINER) == {"state": "RUNNING"}


@pytest.mark.parametrize("payload", [
    b'{"state":"running","state":"closed"}',
    b'{"labels":{"owner":"streamt","owner":"foreign"}}',
])
def test_duplicate_json_fields_never_become_runtime_evidence(runner, cli, payload):
    cli.reply(["container", "cp", f"{CONTAINER}:/var/lib/streamt/state/status.json", "-"], _archive(payload))
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.status_document(CONTAINER)


@pytest.mark.parametrize("name", ["../status.json", "/status.json", "client.properties"])
def test_status_archive_requires_exact_fixed_document_name(runner, cli, name):
    cli.reply(["container", "cp", f"{CONTAINER}:/var/lib/streamt/state/status.json", "-"],
              _archive(b"{}", name=name))
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.status_document(CONTAINER)


def test_plan_reader_only_copies_the_fixed_nonsecret_mount(runner, cli):
    command = ["container", "cp", f"{CONTAINER}:/run/streamt/plan.json", "-"]
    cli.reply(command, _archive(b'{"version":1}', name="plan.json"))
    assert runner.plan_document(CONTAINER) == {"version": 1}
    assert [args for args, _ in cli.calls] == [command]


def _witness_volume():
    data = _volume()
    data["Labels"][docker.LABEL_VOLUME] = "93905888-7323-4c2e-a2a4-ddb8eac94c28"
    data["CreatedAt"] = "2026-09-05T10:00:00Z"
    return data


def test_volume_witness_is_read_only_and_freezes_generation(runner, cli):
    data = _witness_volume()
    command = ["volume", "inspect", "--format", "{{json .}}", APP + "-state"]
    cli.reply(command, data)
    witness = runner.volume_witness(APP, BACKEND)
    assert witness == {
        "name": APP + "-state", "driver": "local", "application_id": APP,
        "backend_identity": BACKEND, "created_at": data["CreatedAt"],
        "token": data["Labels"][docker.LABEL_VOLUME],
    }
    assert [args for args, _ in cli.calls] == [["info", "--format", "{{json .ID}}"], command]


@pytest.mark.parametrize("mismatch", [
    "absent-token", "empty-token", "uppercase-token", "zero-token", "short-token",
    "absent-time", "invalid-time", "naive-time", "driver", "options", "name", "owner", "backend",
])
def test_volume_without_exact_generation_blocks_without_relabel_or_recreation(runner, cli, mismatch):
    data = _witness_volume()
    if mismatch == "absent-token":
        data["Labels"].pop(docker.LABEL_VOLUME)
    elif mismatch.endswith("-token"):
        data["Labels"][docker.LABEL_VOLUME] = {
            "empty-token": "", "uppercase-token": "93905888-7323-4C2E-A2A4-DDB8EAC94C28",
            "zero-token": "00000000-0000-0000-0000-000000000000", "short-token": "abc",
        }[mismatch]
    elif mismatch.endswith("-time"):
        data["CreatedAt"] = {"absent-time": None, "invalid-time": "invalid", "naive-time": "2026-09-05T10:00:00"}[mismatch]
    elif mismatch == "driver":
        data["Driver"] = "remote"
    elif mismatch == "options":
        data["Options"] = {"device": "/foreign"}
    elif mismatch == "name":
        data["Name"] = "foreign"
    elif mismatch == "owner":
        data["Labels"][docker.LABEL_APP] = "foreign"
    else:
        data["Labels"][docker.LABEL_BACKEND] = "foreign"
    cli.reply(["volume", "inspect", "--format", "{{json .}}", APP + "-state"], data)
    with pytest.raises(docker.KafkaStreamsDockerError, match="generation witness"):
        runner.volume_witness(APP, BACKEND)
    assert all(args[:2] not in (["volume", "create"], ["volume", "rm"])
               for args, _ in cli.calls)


def test_new_state_volume_receives_a_generation_token(runner, cli, monkeypatch):
    data = _witness_volume()
    token = data["Labels"][docker.LABEL_VOLUME]
    monkeypatch.setattr(docker.uuid, "uuid4", lambda: token)
    name = APP + "-state"
    cli.reply(["volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{name}$"], b"")
    command = ["volume", "create", "--label", f"{docker.LABEL_APP}={APP}",
               "--label", f"{docker.LABEL_BACKEND}={BACKEND}", "--label", f"{docker.LABEL_VOLUME}={token}", name]
    cli.reply(command, name.encode())
    cli.reply(["volume", "inspect", "--format", "{{json .}}", name], data)
    assert runner.ensure_state_volume(APP, BACKEND) == name
    assert command in [args for args, _ in cli.calls]


def test_create_uses_fixed_hardened_arguments_and_never_pulls(
    runner: docker.LocalDockerRunner, cli: DockerCLI, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = cli.run

    def allow_create(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        if command[1:3] == ["container", "create"]:
            cli.responses[tuple(command[1:])] = CONTAINER.encode()
        return original(command, **kwargs)

    monkeypatch.setattr(docker.subprocess, "run", allow_create)
    cli.reply(["network", "inspect", "--format", "{{json .}}", "bridge"], {
        "Id": "1" * 64, "Name": "bridge", "Driver": "bridge", "Scope": "local",
    })
    volume = APP + "-state"
    cli.reply(["volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{volume}$"], volume.encode())
    cli.reply(["volume", "inspect", "--format", "{{json .}}", volume], {
        "Name": volume, "Driver": "local", "Options": None,
        "Labels": {docker.LABEL_APP: APP, docker.LABEL_BACKEND: BACKEND},
    })
    identity = runner.create(application_id=APP, image_id=IMAGE, network="bridge",
                             plan_file=tmp_path / "plan.json", properties_file=tmp_path / "private.properties",
                             state_volume=APP + "-state", artifact_hash="e" * 64,
                             plan_hash="f" * 64, backend=BACKEND,
                             input_topic_id="A" * 21 + "Q", output_topic_id="B" * 21 + "Q",
                             cluster_id="cluster-unit")
    assert identity == CONTAINER
    args = cli.calls[-1][0]
    for option in ("--pull=never", "--restart=no", "--read-only", "--cap-drop=ALL",
                   "--security-opt=no-new-privileges", "--user=10001:10001"):
        assert option in args
    assert args[args.index("--name") + 1] == APP
    assert IMAGE in args
    assert args[args.index("--network") + 1] == "1" * 64
    assert args[args.index("--expected-cluster-id") + 1] == "cluster-unit"
    assert args[args.index("--expected-input-topic-id") + 1] == "A" * 21 + "Q"
    assert args[args.index("--expected-output-topic-id") + 1] == "B" * 21 + "Q"
    assert not any(command[:2] == ["image", "pull"] for command, _kwargs in cli.calls)


@pytest.mark.parametrize("driver", ["host", "null", "overlay", "macvlan", None])
def test_network_must_be_local_bridge(runner: docker.LocalDockerRunner, cli: DockerCLI, driver: object) -> None:
    cli.reply(["network", "inspect", "--format", "{{json .}}", "example"], {
        "Id": "1" * 64, "Name": "example", "Driver": driver, "Scope": "local",
    })
    with pytest.raises(docker.KafkaStreamsDockerError, match="bridge"):
        runner.network_id("example")


def test_immutable_network_id_cannot_resolve_to_another_network(
    runner: docker.LocalDockerRunner, cli: DockerCLI,
) -> None:
    cli.reply(["network", "inspect", "--format", "{{json .}}", "1" * 64], {
        "Id": "2" * 64, "Name": "example", "Driver": "bridge", "Scope": "local",
    })
    with pytest.raises(docker.KafkaStreamsDockerError):
        runner.network_id("1" * 64)
