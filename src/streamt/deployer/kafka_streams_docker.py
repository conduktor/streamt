"""Bounded local Docker operations for the fixed Kafka Streams runner.

Commands never use a shell, pull an image, print provider responses, or accept
arbitrary container arguments. Docker daemon access is a trusted boundary.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import uuid
from datetime import datetime
from pathlib import Path


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


def _json_object(raw: bytes, *, message: str) -> dict[str, object]:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        result = json.loads(raw, object_pairs_hook=unique_object)
    except (UnicodeError, ValueError):
        raise KafkaStreamsDockerError(message) from None
    if not isinstance(result, dict):
        raise KafkaStreamsDockerError(message)
    return result


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
            valid_generation = (
                isinstance(token, str) and str(uuid.UUID(token)) == token
                and uuid.UUID(token).int != 0 and isinstance(created, str)
                and datetime.fromisoformat(created.replace("Z", "+00:00")).utcoffset() is not None
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

    def create(
        self, *, application_id: str, image_id: str, network: str,
        plan_file: Path, properties_file: Path, state_volume: str,
        artifact_hash: str, plan_hash: str, backend: str,
        input_topic_id: str, output_topic_id: str, cluster_id: str,
    ) -> str:
        name = self.container_name(application_id)
        if not _IMAGE.fullmatch(image_id) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", network):
            raise KafkaStreamsDockerError("Invalid immutable image or Docker network")
        if network in {"host", "none"}:
            raise KafkaStreamsDockerError("Runner requires an isolated Docker bridge network")
        if state_volume != name + "-state":
            raise KafkaStreamsDockerError("Invalid runner state volume identity")
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
        self.ensure_state_volume(application_id, backend)
        raw = self._run([
            "container", "create", "--pull=never", "--name", name,
            "--restart=no", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--user=10001:10001", "--network", network_id,
            "--pids-limit=128", "--memory=512m", "--cpus=1",
            # This path is inside the isolated container, not a host temp file.
            "--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777",  # noqa: S108
            "--label", f"{LABEL_APP}={application_id}",
            "--label", f"{LABEL_BACKEND}={backend}",
            "--label", f"{LABEL_ARTIFACT}={artifact_hash}",
            "--label", f"{LABEL_PLAN}={plan_hash}",
            "--label", f"{LABEL_INPUT}={input_topic_id}",
            "--label", f"{LABEL_OUTPUT}={output_topic_id}",
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

    def _container_document(self, container_id: str, path: str, *, maximum: int) -> dict[str, object]:
        if not _ID.fullmatch(container_id):
            raise KafkaStreamsDockerError("Invalid runner container ID")
        raw = self._run(["container", "cp", f"{container_id}:{path}", "-"])
        try:
            with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                members = archive.getmembers()
                if (
                    len(members) != 1 or not members[0].isfile()
                    or members[0].name != path.rsplit("/", 1)[-1]
                    or members[0].size > maximum or members[0].size < 0
                ):
                    raise ValueError
                handle = archive.extractfile(members[0])
                if handle is None:
                    raise ValueError
                content = handle.read(maximum + 1)
        except (OSError, ValueError, tarfile.TarError):
            raise KafkaStreamsDockerError("Invalid runner document") from None
        return _json_object(content, message="Invalid runner document")
