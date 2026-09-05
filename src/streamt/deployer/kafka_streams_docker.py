"""Bounded local Docker operations for the fixed Kafka Streams runner.

Commands never use a shell, pull an image, print provider responses, or accept
arbitrary container arguments. Docker daemon access is a trusted boundary.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import tarfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from streamt.deployer.kafka_streams_evidence import KafkaStreamsVolumeEvidence
from streamt.deployer.kafka_streams_time import parse_utc_timestamp

if TYPE_CHECKING:
    from streamt.deployer.kafka_streams_replacement import ReplacementGeneration


class KafkaStreamsDockerError(ValueError):
    """A secret-neutral local Docker boundary failure."""


_ID = re.compile(r"^[0-9a-f]{64}$")
_APP = re.compile(r"^streamt-[0-9a-f]{32}$")
_IMAGE = re.compile(r"^sha256:[0-9a-f]{64}$")
RUNNER_VERSION = "0.1.1"
PLAN_VERSION = "1"
LABEL_APP = "io.streamt.application-id"
LABEL_ARTIFACT = "io.streamt.artifact-sha256"
LABEL_PLAN = "io.streamt.plan-sha256"
LABEL_BACKEND = "io.streamt.backend"
LABEL_INPUT = "io.streamt.input-topic-id"
LABEL_OUTPUT = "io.streamt.output-topic-id"
LABEL_VOLUME = "io.streamt.state-volume-token"
LABEL_OPERATION = "io.streamt.operation-id"
LABEL_ACTION_INDEX = "io.streamt.operation-action-index"
LABEL_EVIDENCE = "io.streamt.replacement-fingerprint"
_TMPFS_SPEC = "rw,nosuid,nodev,noexec,size=64m,mode=1777"
_TMPFS_PATH = "/tmp"  # noqa: S108 - fixed path inside the container, not a host file
_PLAN_PATH = "/run/streamt/plan.json"
_PROPERTIES_PATH = "/run/streamt/client.properties"
_STATE_PATH = "/var/lib/streamt/state"


def normalize_docker_timestamp(value: object) -> str:
    """Normalize RFC3339 offsets to UTC without truncating nanosecond fractions."""
    if not isinstance(value, str):
        raise KafkaStreamsDockerError("Invalid Docker timestamp")
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})", value,
    )
    if match is None or match[3] == "-00:00":
        raise KafkaStreamsDockerError("Invalid Docker timestamp")
    if match[3] != "Z" and (int(match[3][1:3]) > 23 or int(match[3][4:6]) > 59):
        raise KafkaStreamsDockerError("Invalid Docker timestamp")
    try:
        # Parse only whole seconds; datetime would otherwise discard digits7-9.
        instant = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
        utc = instant.astimezone(timezone.utc).isoformat(timespec="seconds")
        normalized = utc.removesuffix("+00:00") + (match[2] or "") + "Z"
        parse_utc_timestamp(normalized)
        return normalized
    except (ValueError, OverflowError):
        raise KafkaStreamsDockerError("Invalid Docker timestamp") from None


def _json_object(raw: bytes, *, message: str) -> dict[str, object]:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    def invalid_constant(_value: str) -> object:
        raise ValueError

    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object, parse_constant=invalid_constant)
        pending = [(result, 0)]
        while pending:
            value, depth = pending.pop()
            if depth > 64 or (isinstance(value, float) and not math.isfinite(value)):
                raise ValueError
            if isinstance(value, str):
                value.encode("utf-8")
            elif isinstance(value, dict):
                pending.extend((item, depth + 1) for pair in value.items() for item in pair)
            elif isinstance(value, list):
                pending.extend((item, depth + 1) for item in value)
    except (UnicodeError, ValueError, RecursionError):
        raise KafkaStreamsDockerError(message) from None
    if not isinstance(result, dict):
        raise KafkaStreamsDockerError(message)
    return result


@dataclass(frozen=True)
class KafkaStreamsPlanWitness:
    """Exact bounded mounted bytes; decoding never exposes client properties."""

    raw_bytes: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.raw_bytes) is not bytes or len(self.raw_bytes) > 1024 * 1024:
            raise KafkaStreamsDockerError("Invalid runner plan witness")
        _json_object(self.raw_bytes, message="Invalid runner plan witness")

    @property
    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.raw_bytes).hexdigest()

    @property
    def document(self) -> dict[str, object]:
        # Give each caller a defensive decode; nested edits cannot change the witness.
        return _json_object(self.raw_bytes, message="Invalid runner plan witness")


def _text(raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeError:
        raise KafkaStreamsDockerError("Invalid Docker text response") from None


class LocalDockerRunner:
    """Freeze one local daemon endpoint and use immutable object identities."""

    def __init__(self, *, timeout: int = 30) -> None:
        self.timeout = timeout
        self._environment = dict(os.environ)
        executable = shutil.which("docker")
        if executable is None:
            raise KafkaStreamsDockerError("Docker CLI is required for the Kafka Streams executor")
        self._executable: str = executable
        endpoint = self._environment.get("DOCKER_HOST")
        if not endpoint:
            raw = self._run(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"])
            try:
                endpoint = json.loads(raw)
            except (ValueError, UnicodeError):
                raise KafkaStreamsDockerError("Cannot resolve local Docker endpoint") from None
        if not isinstance(endpoint, str) or not endpoint.startswith("unix:///"):
            raise KafkaStreamsDockerError("Kafka Streams requires a local Unix-socket Docker daemon")
        if any(character in endpoint for character in "\r\n\x00"):
            raise KafkaStreamsDockerError("Invalid local Docker endpoint")
        self._environment.pop("DOCKER_CONTEXT", None)
        self._environment["DOCKER_HOST"] = endpoint
        self.endpoint = endpoint
        self.daemon_id = self._daemon_id()

    def _run(self, args: list[str], *, timeout: int | None = None) -> bytes:
        try:
            result = subprocess.run(  # noqa: S603 - fixed CLI; callers validate each argument
                [self._executable, *args], capture_output=True, check=False,
                timeout=timeout or self.timeout, env=self._environment,
            )
        except (OSError, subprocess.SubprocessError):
            raise KafkaStreamsDockerError("Docker command failed or timed out") from None
        if result.returncode != 0:
            # Docker may echo credentials, host paths or container logs in errors.
            raise KafkaStreamsDockerError("Docker command failed; inspect the local daemon separately")
        return result.stdout

    def _daemon_id(self) -> str:
        raw = self._run(["info", "--format", "{{json .ID}}"])
        try:
            identity = json.loads(raw)
        except (ValueError, UnicodeError):
            raise KafkaStreamsDockerError("Cannot identify local Docker daemon") from None
        if not isinstance(identity, str) or not identity.strip():
            raise KafkaStreamsDockerError("Cannot identify local Docker daemon")
        return identity

    def verify_daemon(self) -> None:
        if self._daemon_id() != self.daemon_id:
            raise KafkaStreamsDockerError("Docker daemon identity changed; create a fresh plan")

    def backend_identity(self, kafka_cluster_id: str) -> str:
        if not isinstance(kafka_cluster_id, str) or not kafka_cluster_id:
            raise KafkaStreamsDockerError("Kafka cluster identity is required")
        digest = hashlib.sha256(json.dumps(
            [self.daemon_id, kafka_cluster_id], separators=(",", ":"),
        ).encode()).hexdigest()
        return f"kafka-streams-docker:v1:{digest}"

    def image_id(self, image: str) -> str:
        """Inspect an already present pinned image; never fetch it implicitly."""
        if not (_IMAGE.fullmatch(image) or re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", image)):
            raise KafkaStreamsDockerError("Runner image must use an immutable SHA-256 identity")
        data = _json_object(
            self._run(["image", "inspect", "--format", "{{json .}}", image]),
            message="Invalid runner image inspection",
        )
        identity = data.get("Id")
        config = data.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        if (
            not isinstance(identity, str) or not _IMAGE.fullmatch(identity)
            or (_IMAGE.fullmatch(image) and identity != image)
            or not isinstance(config, dict)
            or not isinstance(labels, dict)
            or labels.get("io.streamt.runner.version") != RUNNER_VERSION
            or labels.get("io.streamt.plan.version") != PLAN_VERSION
            or config.get("Volumes")
        ):
            raise KafkaStreamsDockerError("Image is not a compatible fixed streamt runner")
        return identity

    def network_id(self, network: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", network):
            raise KafkaStreamsDockerError("Invalid Docker network")
        data = _json_object(
            self._run(["network", "inspect", "--format", "{{json .}}", network]),
            message="Invalid Docker network inspection",
        )
        identity = data.get("Id")
        if (
            not isinstance(identity, str) or not _ID.fullmatch(identity)
            or data.get("Driver") != "bridge" or data.get("Scope") != "local"
            or data.get("Name") in {"host", "none"}
            or (not _ID.fullmatch(network) and data.get("Name") != network)
            or (_ID.fullmatch(network) and identity != network)
        ):
            raise KafkaStreamsDockerError("Runner requires an exact local Docker bridge network")
        return identity

    @staticmethod
    def container_name(application_id: str) -> str:
        if not _APP.fullmatch(application_id):
            raise KafkaStreamsDockerError("Invalid Kafka Streams application identity")
        return application_id

    def inspect(self, application_id: str) -> dict[str, object] | None:
        name = self.container_name(application_id)
        # An inspect error must not be mistaken for absence. Resolve absence by
        # a successful exact-name listing, then inspect the immutable ID.
        raw_ids = self._run([
            "container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{name}$",
            "--format", "{{.ID}}",
        ])
        try:
            ids = raw_ids.decode("ascii").split()
        except UnicodeError:
            raise KafkaStreamsDockerError("Invalid container identity response") from None
        if not ids:
            return None
        if len(ids) != 1 or not _ID.fullmatch(ids[0]):
            raise KafkaStreamsDockerError("Ambiguous container identity")
        data = _json_object(
            self._run(["container", "inspect", "--format", "{{json .}}", ids[0]]),
            message="Invalid container inspection",
        )
        if data.get("Id") != ids[0] or data.get("Name") != f"/{name}":
            raise KafkaStreamsDockerError("Container identity changed during observation")
        return data

    def inspect_exact(self, container_id: str) -> dict[str, object] | None:
        """Observe a physical container independently of the application name slot.

        A renamed old container remains present. The caller must also validate
        its ownership/name before deciding on a lifecycle action.
        """
        if not isinstance(container_id, str) or not _ID.fullmatch(container_id):
            raise KafkaStreamsDockerError("Invalid runner container ID")
        self.verify_daemon()
        raw = self._run([
            "container", "ls", "--all", "--no-trunc", "--filter", f"id={container_id}",
            "--format", "{{.ID}}",
        ])
        try:
            identities = raw.decode("ascii").split()
        except UnicodeError:
            raise KafkaStreamsDockerError("Invalid container identity response") from None
        if not identities:
            return None
        if identities != [container_id]:
            raise KafkaStreamsDockerError("Ambiguous container identity")
        data = _json_object(
            self._run(["container", "inspect", "--format", "{{json .}}", container_id]),
            message="Invalid container inspection",
        )
        if data.get("Id") != container_id:
            raise KafkaStreamsDockerError("Container identity changed during observation")
        return data

    @staticmethod
    def generation(data: dict[str, object]) -> ReplacementGeneration | None:
        """Read an all-or-none generation binding, never infer one from a name."""
        from streamt.deployer.kafka_streams_replacement import ReplacementGeneration

        config = data.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        if not isinstance(labels, dict):
            raise KafkaStreamsDockerError("Invalid runner generation labels")
        names = {LABEL_OPERATION, LABEL_ACTION_INDEX, LABEL_EVIDENCE}
        present = names.intersection(labels)
        if not present:
            return None
        if present != names:
            raise KafkaStreamsDockerError("Incomplete runner generation labels")
        index = labels[LABEL_ACTION_INDEX]
        if not isinstance(index, str) or re.fullmatch(r"0|[1-9][0-9]{0,18}", index) is None:
            raise KafkaStreamsDockerError("Invalid runner generation labels")
        try:
            return ReplacementGeneration(labels[LABEL_OPERATION], int(index), labels[LABEL_EVIDENCE])
        except (ValueError, TypeError):
            raise KafkaStreamsDockerError("Invalid runner generation labels") from None

    @staticmethod
    def require_owned(data: dict[str, object], application_id: str, backend: str) -> str:
        config = data.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
        identity = data.get("Id")
        if (
            not isinstance(identity, str) or not _ID.fullmatch(identity)
            or data.get("Name") != f"/{application_id}"
            or not isinstance(labels, dict)
            or labels.get(LABEL_APP) != application_id
            or labels.get(LABEL_BACKEND) != backend
        ):
            raise KafkaStreamsDockerError("Container has no exact streamt ownership binding")
        return identity

    def ensure_state_volume(self, application_id: str, backend: str) -> str:
        name = self.container_name(application_id) + "-state"
        raw = self._run(["volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{name}$"])
        names = _text(raw).split()
        if names and names != [name]:
            raise KafkaStreamsDockerError("Ambiguous runner state volume")
        if not names:
            self.verify_daemon()
            self._run([
                "volume", "create", "--label", f"{LABEL_APP}={application_id}",
                "--label", f"{LABEL_BACKEND}={backend}",
                "--label", f"{LABEL_VOLUME}={uuid.uuid4()}", name,
            ])
        data = _json_object(
            self._run(["volume", "inspect", "--format", "{{json .}}", name]),
            message="Invalid state volume inspection",
        )
        labels = data.get("Labels")
        if (
            data.get("Name") != name or data.get("Driver") != "local"
            or data.get("Options")
            or not isinstance(labels, dict) or labels.get(LABEL_APP) != application_id
            or labels.get(LABEL_BACKEND) != backend
        ):
            raise KafkaStreamsDockerError("State volume has no exact streamt ownership binding")
        return name

    def volume_witness(self, application_id: str, backend: str) -> dict[str, object]:
        """Read an exact generation witness; never create or upgrade a volume."""
        name = self.container_name(application_id) + "-state"
        self.verify_daemon()
        data = _json_object(
            self._run(["volume", "inspect", "--format", "{{json .}}", name]),
            message="Invalid state volume inspection",
        )
        labels = data.get("Labels")
        token = labels.get(LABEL_VOLUME) if isinstance(labels, dict) else None
        created = data.get("CreatedAt")
        try:
            created = normalize_docker_timestamp(created)
            valid_generation = (
                isinstance(token, str) and str(uuid.UUID(token)) == token
                and uuid.UUID(token).int != 0
            )
        except (ValueError, OverflowError):
            valid_generation = False
        if (
            not valid_generation or data.get("Name") != name or data.get("Driver") != "local"
            or data.get("Options") or not isinstance(labels, dict)
            or labels.get(LABEL_APP) != application_id or labels.get(LABEL_BACKEND) != backend
        ):
            raise KafkaStreamsDockerError("State volume has no exact owned generation witness")
        return {"name": name, "driver": "local", "created_at": created,
                "application_id": application_id, "backend_identity": backend, "token": token}

    def require_volume(self, expected: KafkaStreamsVolumeEvidence) -> None:
        """Require the reviewed same-instance witness without creating a volume."""
        if type(expected) is not KafkaStreamsVolumeEvidence:
            raise KafkaStreamsDockerError("An exact state volume witness is required")
        observed = self.volume_witness(expected.application_id, expected.backend_identity)
        if observed != expected.to_dict():
            raise KafkaStreamsDockerError("State volume identity changed after review")

    def validate_mounts(self, data: dict[str, object], expected: KafkaStreamsVolumeEvidence) -> None:
        """Check actual fixed mounts and the existing volume, without reading secrets."""
        if type(expected) is not KafkaStreamsVolumeEvidence:
            raise KafkaStreamsDockerError("An exact state volume witness is required")
        self.require_owned(data, expected.application_id, expected.backend_identity)
        mounts, host = data.get("Mounts"), data.get("HostConfig")
        if not isinstance(mounts, list) or not isinstance(host, dict):
            raise KafkaStreamsDockerError("Invalid runner mount layout")
        by_destination: dict[str, dict[str, object]] = {}
        for mount in mounts:
            if not isinstance(mount, dict) or not isinstance(mount.get("Destination"), str):
                raise KafkaStreamsDockerError("Invalid runner mount layout")
            destination = mount["Destination"]
            if destination in by_destination:
                raise KafkaStreamsDockerError("Invalid runner mount layout")
            by_destination[destination] = mount
        required = {_PLAN_PATH, _PROPERTIES_PATH, _STATE_PATH}
        if set(by_destination) not in (required, required | {_TMPFS_PATH}):
            raise KafkaStreamsDockerError("Invalid runner mount layout")
        for destination in (_PLAN_PATH, _PROPERTIES_PATH):
            mount = by_destination[destination]
            source = mount.get("Source")
            if (
                mount.get("Type") != "bind" or mount.get("RW") is not False
                or not isinstance(source, str) or not Path(source).is_absolute()
                or any(character in source for character in ",\r\n\x00")
            ):
                raise KafkaStreamsDockerError("Invalid runner mount layout")
        if by_destination[_PLAN_PATH]["Source"] == by_destination[_PROPERTIES_PATH]["Source"]:
            raise KafkaStreamsDockerError("Invalid runner mount layout")
        state = by_destination[_STATE_PATH]
        if (
            state.get("Type") != "volume" or state.get("Name") != expected.name
            or state.get("Driver") != "local" or state.get("RW") is not True
            or host.get("Tmpfs") != {_TMPFS_PATH: _TMPFS_SPEC}
        ):
            raise KafkaStreamsDockerError("Invalid runner mount layout")
        temporary = by_destination.get(_TMPFS_PATH)
        if temporary is not None and (temporary.get("Type") != "tmpfs" or temporary.get("RW") is not True):
            raise KafkaStreamsDockerError("Invalid runner mount layout")
        self.require_volume(expected)

    def create(
        self, *, application_id: str, image_id: str, network: str,
        plan_file: Path, properties_file: Path, state_volume: str,
        artifact_hash: str, plan_hash: str, backend: str,
        input_topic_id: str, output_topic_id: str, cluster_id: str,
        generation: ReplacementGeneration | None = None,
        expected_volume: KafkaStreamsVolumeEvidence | None = None,
    ) -> str:
        name = self.container_name(application_id)
        if not _IMAGE.fullmatch(image_id) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", network):
            raise KafkaStreamsDockerError("Invalid immutable image or Docker network")
        if network in {"host", "none"}:
            raise KafkaStreamsDockerError("Runner requires an isolated Docker bridge network")
        if state_volume != name + "-state":
            raise KafkaStreamsDockerError("Invalid runner state volume identity")
        generation_args: list[str] = []
        if generation is not None:
            from streamt.deployer.kafka_streams_replacement import ReplacementGeneration

            if (
                type(generation) is not ReplacementGeneration or type(expected_volume) is not KafkaStreamsVolumeEvidence
                or expected_volume.name != state_volume or expected_volume.application_id != application_id
                or expected_volume.backend_identity != backend
            ):
                raise KafkaStreamsDockerError("Replacement creation requires exact generation and existing volume evidence")
            generation_args = [
                "--label", f"{LABEL_OPERATION}={generation.operation_id}",
                "--label", f"{LABEL_ACTION_INDEX}={generation.action_index}",
                "--label", f"{LABEL_EVIDENCE}={generation.evidence_fingerprint}",
            ]
        elif expected_volume is not None:
            raise KafkaStreamsDockerError("Replacement volume evidence requires a generation")
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{22}", input_topic_id)
            or not re.fullmatch(r"[A-Za-z0-9_-]{22}", output_topic_id)
            or input_topic_id == output_topic_id
        ):
            raise KafkaStreamsDockerError("Invalid Kafka topic identity binding")
        if (
            not isinstance(cluster_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", cluster_id)
        ):
            raise KafkaStreamsDockerError("Invalid Kafka cluster identity binding")
        for path in (plan_file, properties_file):
            if not path.is_absolute() or any(character in str(path) for character in ",\n\r\x00"):
                raise KafkaStreamsDockerError("Invalid private runner input path")
        self.verify_daemon()
        network_id = self.network_id(network)
        if generation is None:
            self.ensure_state_volume(application_id, backend)
        else:
            assert expected_volume is not None
            self.require_volume(expected_volume)
        raw = self._run([
            "container", "create", "--pull=never", "--name", name,
            "--restart=no", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--user=10001:10001", "--network", network_id,
            "--pids-limit=128", "--memory=512m", "--cpus=1",
            # This path is inside the isolated container, not a host temp file.
            "--tmpfs", "/tmp:" + _TMPFS_SPEC,  # noqa: S108
            "--label", f"{LABEL_APP}={application_id}",
            "--label", f"{LABEL_BACKEND}={backend}",
            "--label", f"{LABEL_ARTIFACT}={artifact_hash}",
            "--label", f"{LABEL_PLAN}={plan_hash}",
            "--label", f"{LABEL_INPUT}={input_topic_id}",
            "--label", f"{LABEL_OUTPUT}={output_topic_id}",
            *generation_args,
            "--mount", f"type=bind,source={plan_file},target=/run/streamt/plan.json,readonly",
            "--mount", f"type=bind,source={properties_file},target=/run/streamt/client.properties,readonly",
            "--mount", f"type=volume,source={state_volume},target=/var/lib/streamt/state",
            image_id, "--plan", "/run/streamt/plan.json", "--client-properties",
            "/run/streamt/client.properties", "--application-id", application_id,
            "--state-dir", "/var/lib/streamt/state",
            "--expected-cluster-id", cluster_id,
            "--expected-input-topic-id", input_topic_id,
            "--expected-output-topic-id", output_topic_id,
        ])
        identity = _text(raw).strip()
        if not _ID.fullmatch(identity):
            raise KafkaStreamsDockerError("Docker did not return an exact created container ID")
        return identity

    def owned_command(self, command: str, application_id: str, backend: str, *, expected_id: str) -> None:
        if command not in {"start", "term", "remove"}:
            raise KafkaStreamsDockerError("Unsupported runner lifecycle operation")
        self.verify_daemon()
        data = self.inspect(application_id)
        if data is None or self.require_owned(data, application_id, backend) != expected_id:
            raise KafkaStreamsDockerError("Runner identity changed before lifecycle operation")
        if command == "start":
            self._run(["container", "start", expected_id])
        elif command == "term":
            self._run(["container", "kill", "--signal=TERM", expected_id])
        else:
            state = data.get("State")
            if not isinstance(state, dict) or state.get("Running") is not False:
                raise KafkaStreamsDockerError("Refusing to remove a running runner")
            # No -f or -v: lifecycle replacement cannot force a kill or erase state.
            self._run(["container", "rm", expected_id])

    def status_document(self, container_id: str) -> dict[str, object]:
        return self._container_document(container_id, "/var/lib/streamt/state/status.json", maximum=8192)

    def plan_document(self, container_id: str) -> dict[str, object]:
        """Read only the fixed SQL plan mount, never runtime client properties."""
        return self._container_document(container_id, "/run/streamt/plan.json", maximum=1024 * 1024)

    def plan_witness(self, container_id: str) -> KafkaStreamsPlanWitness:
        return KafkaStreamsPlanWitness(self._container_payload(container_id, _PLAN_PATH, maximum=1024 * 1024))

    def _container_document(self, container_id: str, path: str, *, maximum: int) -> dict[str, object]:
        return _json_object(self._container_payload(container_id, path, maximum=maximum), message="Invalid runner document")

    def _container_payload(self, container_id: str, path: str, *, maximum: int) -> bytes:
        if not isinstance(container_id, str) or not _ID.fullmatch(container_id):
            raise KafkaStreamsDockerError("Invalid runner container ID")
        raw = self._run(["container", "cp", f"{container_id}:{path}", "-"])
        try:
            if len(raw) > maximum + 10240:
                raise ValueError
            with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                members = archive.getmembers()
                if (
                    len(members) != 1 or not members[0].isfile()
                    or members[0].issparse()
                    or members[0].name != path.rsplit("/", 1)[-1]
                    or members[0].size > maximum or members[0].size < 0
                ):
                    raise ValueError
                handle = archive.extractfile(members[0])
                if handle is None:
                    raise ValueError
                content = handle.read(maximum + 1)
                if len(content) != members[0].size or len(content) > maximum:
                    raise ValueError
        except (OSError, ValueError, tarfile.TarError):
            raise KafkaStreamsDockerError("Invalid runner document") from None
        return content
