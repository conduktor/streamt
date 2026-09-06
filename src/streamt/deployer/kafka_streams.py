"""Owned Docker lifecycle for streamt's bounded Kafka Streams executor."""

from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from streamt.compiler.kafka_streams import MAX_RUNNER_INPUT_BYTES
from streamt.compiler.manifest import (
    KafkaStreamsJobArtifact,
    parse_compiled_kafka_streams_job_artifact,
)
from streamt.core.runtime import KafkaConfig, KafkaStreamsConfig
from streamt.deployer.kafka_streams_docker import (
    LABEL_ARTIFACT,
    LABEL_INPUT,
    LABEL_OUTPUT,
    LABEL_PLAN,
    RUNNER_VERSION,
    LocalDockerRunner,
)
from streamt.deployer.kafka_streams_evidence import KafkaStreamsActionEvidence
from streamt.deployer.kafka_streams_progress import ApplicationProgress, KafkaStreamsProgress
from streamt.deployer.kafka_streams_time import parse_utc_timestamp
from streamt.deployer.state import artifact_checksum


class KafkaStreamsLifecycleError(ValueError):
    """An unsupported, stale or incomplete runner lifecycle transition."""


def runner_plan_bytes(plan: dict[str, object]) -> bytes:
    return (json.dumps(plan, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode()


def runner_plan_hash(plan: dict[str, object]) -> str:
    return "sha256:" + hashlib.sha256(runner_plan_bytes(plan)).hexdigest()


def _status_time(value: object) -> tuple[datetime, int]:
    try:
        return parse_utc_timestamp(value)
    except ValueError:
        raise KafkaStreamsLifecycleError("Runner status has no valid UTC timestamp") from None


def _property(value: str) -> str:
    """Escape Java properties without allowing injected keys or line continuations."""
    output = ""
    for character in value:
        code = ord(character)
        if character in "\\:=#! ":
            output += "\\" + character
        elif character == "\n":
            output += "\\n"
        elif character == "\r":
            output += "\\r"
        elif character == "\t":
            output += "\\t"
        elif code < 32 or code > 126:
            if code > 0xFFFF:
                code -= 0x10000
                output += f"\\u{0xD800 + (code >> 10):04x}\\u{0xDC00 + (code & 0x3FF):04x}"
            else:
                output += f"\\u{code:04x}"
        else:
            output += character
    return output


def _jaas_quote(value: str) -> str:
    if any(ord(character) < 32 for character in value):
        raise KafkaStreamsLifecycleError("SASL credentials contain unsupported control characters")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def runner_client_properties(kafka: KafkaConfig) -> bytes:
    """Translate supported Kafka security settings into a private runtime file."""
    protocol = kafka.security_protocol or "PLAINTEXT"
    settings = {
        "bootstrap.servers": kafka.bootstrap_servers_internal or kafka.bootstrap_servers,
        "security.protocol": protocol,
    }
    # Match ClientProperties.java's closed address grammar without DNS or a
    # provider. The container endpoint must not fail local parsing after offsets
    # have been initialized on the host-side Kafka connection.
    for address in settings["bootstrap.servers"].split(","):
        if not re.fullmatch(r"(?:[A-Za-z0-9][A-Za-z0-9.-]*|\[[0-9A-Fa-f:]+\]):[0-9]{1,5}", address):
            raise KafkaStreamsLifecycleError("Runner bootstrap requires comma-separated host:port addresses")
        if not 0 < int(address.rsplit(":", 1)[1]) <= 65535:
            raise KafkaStreamsLifecycleError("Runner bootstrap port must be between 1 and 65535")
    if "SASL" in protocol:
        if (
            kafka.sasl_mechanism not in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
            or not kafka.sasl_username or kafka.sasl_password is None
        ):
            raise KafkaStreamsLifecycleError("Runner SASL requires explicit PLAIN/SCRAM mechanism and credentials")
        module = (
            "org.apache.kafka.common.security.plain.PlainLoginModule"
            if kafka.sasl_mechanism == "PLAIN"
            else "org.apache.kafka.common.security.scram.ScramLoginModule"
        )
        settings["sasl.mechanism"] = kafka.sasl_mechanism
        settings["sasl.jaas.config"] = (
            f"{module} required username={_jaas_quote(kafka.sasl_username)} "
            f"password={_jaas_quote(kafka.sasl_password.get_secret_value())};"
        )
    elif any((kafka.sasl_mechanism, kafka.sasl_username, kafka.sasl_password)):
        raise KafkaStreamsLifecycleError("SASL settings require a SASL security protocol")
    ssl_paths = (kafka.ssl_ca_location, kafka.ssl_certificate_location, kafka.ssl_key_location)
    if "SSL" not in protocol and (any(ssl_paths) or kafka.ssl_key_password):
        raise KafkaStreamsLifecycleError("TLS settings require an SSL security protocol")
    if "SSL" in protocol:
        settings["ssl.endpoint.identification.algorithm"] = "https"
        try:
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            if kafka.ssl_ca_location:
                settings["ssl.truststore.type"] = "PEM"
                settings["ssl.truststore.certificates"] = Path(kafka.ssl_ca_location).read_text()
                tls.load_verify_locations(cadata=settings["ssl.truststore.certificates"])
            if bool(kafka.ssl_certificate_location) != bool(kafka.ssl_key_location):
                raise KafkaStreamsLifecycleError("Mutual TLS requires both certificate and private key")
            if kafka.ssl_certificate_location and kafka.ssl_key_location:
                settings["ssl.keystore.type"] = "PEM"
                settings["ssl.keystore.certificate.chain"] = Path(kafka.ssl_certificate_location).read_text()
                settings["ssl.keystore.key"] = Path(kafka.ssl_key_location).read_text()
                if not any(marker in settings["ssl.keystore.key"] for marker in (
                    "-----BEGIN PRIVATE KEY-----", "-----BEGIN ENCRYPTED PRIVATE KEY-----",
                )):
                    raise KafkaStreamsLifecycleError("Runner mutual TLS requires a PEM PKCS#8 private key")
                # Supplying a password (even empty) avoids OpenSSL prompting on
                # encrypted keys. Validate local material without any network.
                tls.load_cert_chain(
                    kafka.ssl_certificate_location, kafka.ssl_key_location,
                    password=kafka.ssl_key_password.get_secret_value() if kafka.ssl_key_password else "",
                )
                if kafka.ssl_key_password:
                    settings["ssl.key.password"] = kafka.ssl_key_password.get_secret_value()
        except (OSError, UnicodeError, ValueError) as error:
            if isinstance(error, KafkaStreamsLifecycleError):
                raise
            raise KafkaStreamsLifecycleError("Cannot validate configured runner TLS material") from None
    content = "".join(f"{key}={_property(value)}\n" for key, value in sorted(settings.items())).encode("ascii")
    if len(content) > MAX_RUNNER_INPUT_BYTES:
        raise KafkaStreamsLifecycleError("Runner client properties exceed the fixed 1 MiB limit")
    return content


@dataclass
class KafkaStreamsJobState:
    name: str
    exists: bool
    container_id: str | None = None
    status: str | None = None
    artifact_hash: str | None = None
    plan_hash: str | None = None
    image_id: str | None = None
    input_topic_id: str | None = None
    output_topic_id: str | None = None
    network_id: str | None = None


@dataclass
class KafkaStreamsJobChange:
    job_name: str
    action: str
    current: KafkaStreamsJobState | None = None
    desired: KafkaStreamsJobArtifact | None = None
    changes: dict[str, object] = field(default_factory=dict)
    backend_identity: str | None = None
    blocker: str | None = None
    kafka_streams_evidence: KafkaStreamsActionEvidence | None = None


class KafkaStreamsDeployer:
    """Plan read-only; mutate only an exact owned application after preflight."""

    def __init__(self, config: KafkaStreamsConfig, kafka: KafkaConfig, *, state_dir: Path) -> None:
        # Validate security before constructing either provider. Keep bytes out
        # of artifact/state reprs and public plan payloads.
        self._client_properties = runner_client_properties(kafka)
        self.config = config
        self.kafka = kafka
        self.state_dir = state_dir
        self._validate_private_directory()
        self._planned_created_topics: set[str] = set()
        self._created_topic_ids: dict[str, str] = {}
        self.docker = LocalDockerRunner()
        self.progress = KafkaStreamsProgress(kafka.to_confluent_config())
        self.cluster_id = self.progress.cluster_id()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", self.cluster_id):
            raise KafkaStreamsLifecycleError("Kafka cluster identity is unsupported by the fixed runner")
        self.backend_identity = self.docker.backend_identity(self.cluster_id)
        self.image_id = self.docker.image_id(config.image)
        self.network_id = self.docker.network_id(config.network)

    def close(self) -> None:
        self._client_properties = b""

    def _artifact(self, desired: KafkaStreamsJobArtifact) -> KafkaStreamsJobArtifact:
        if type(desired) is not KafkaStreamsJobArtifact:
            raise KafkaStreamsLifecycleError("Runner requires an exact compiled artifact")
        artifact = parse_compiled_kafka_streams_job_artifact(desired.to_dict())
        if (
            artifact.image != self.config.image or artifact.network != self.config.network
            or artifact.initial_offset != self.config.initial_offset
        ):
            raise KafkaStreamsLifecycleError("Compiled runner does not match the configured Docker runtime")
        return artifact

    def get_job_state(self, artifact: KafkaStreamsJobArtifact) -> KafkaStreamsJobState:
        artifact = self._artifact(artifact)
        data = self.docker.inspect(artifact.application_id)
        if data is None:
            return KafkaStreamsJobState(artifact.name, False)
        container_id = self.docker.require_owned(data, artifact.application_id, self.backend_identity)
        config = data.get("Config")
        state = data.get("State")
        if not isinstance(config, dict) or not isinstance(state, dict):
            raise KafkaStreamsLifecycleError("Malformed runner container observation")
        if type(state.get("Running")) is not bool or type(state.get("ExitCode")) is not int:
            raise KafkaStreamsLifecycleError("Malformed runner process state")
        labels = config.get("Labels")
        if not isinstance(labels, dict):
            raise KafkaStreamsLifecycleError("Malformed runner ownership observation")
        artifact_hash, plan_hash = labels.get(LABEL_ARTIFACT), labels.get(LABEL_PLAN)
        if not all(
            isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
            for value in (artifact_hash, plan_hash)
        ):
            raise KafkaStreamsLifecycleError("Runner lacks versioned artifact identity")
        status = "stopped"
        if state.get("Running") is True:
            document = self.docker.status_document(container_id)
            if (
                document.get("application_id") != artifact.application_id
                or document.get("plan_sha256") != plan_hash
                or document.get("runner_version") != RUNNER_VERSION
                or type(document.get("plan_version")) is not int or document.get("plan_version") != 1
                or set(document) != {
                    "application_id", "plan_sha256", "runner_version", "plan_version",
                    "state", "reason", "updated_at",
                    "cluster_id", "input_topic_id", "output_topic_id",
                }
                or document.get("cluster_id") != self.cluster_id
                or document.get("input_topic_id") != labels.get(LABEL_INPUT)
                or document.get("output_topic_id") != labels.get(LABEL_OUTPUT)
                or not isinstance(document.get("state"), str)
                or document.get("state") not in {"starting", "running", "closing", "closed", "failed"}
                or (document.get("state") != "failed" and document.get("reason") is not None)
            ):
                raise KafkaStreamsLifecycleError("Runner status does not match its application/plan identity")
            # A retained state volume may still contain the previous process's
            # running document. It cannot prove this container is ready.
            if _status_time(document.get("updated_at")) < _status_time(state.get("StartedAt")):
                raise KafkaStreamsLifecycleError("Runner status belongs to a previous process start")
            status = str(document.get("state"))
        elif state.get("ExitCode") != 0:
            status = "failed"
        image_id = data.get("Image")
        if not isinstance(image_id, str):
            raise KafkaStreamsLifecycleError("Runner image identity is unavailable")
        input_id, output_id = labels.get(LABEL_INPUT), labels.get(LABEL_OUTPUT)
        host_config = data.get("HostConfig")
        network_id = host_config.get("NetworkMode") if isinstance(host_config, dict) else None
        if not all(isinstance(value, str) and value for value in (input_id, output_id, network_id)):
            raise KafkaStreamsLifecycleError("Runner lacks exact topic/network identity bindings")
        return KafkaStreamsJobState(
            artifact.name, True, container_id, status, artifact_hash, plan_hash, image_id,
            str(input_id), str(output_id), str(network_id),
        )

    def plan_job(
        self, artifact: KafkaStreamsJobArtifact, *, new_topics: frozenset[str] = frozenset(),
    ) -> KafkaStreamsJobChange:
        artifact = self._artifact(artifact)
        current = self.get_job_state(artifact)
        input_topic, output_topic = str(artifact.plan["input_topic"]), str(artifact.plan["output_topic"])
        bindings = {
            topic: None if topic in new_topics else self.progress.topic_id(topic)
            for topic in (input_topic, output_topic)
        }
        self._planned_created_topics.update(topic for topic, identity in bindings.items() if identity is None)
        if self.progress.cluster_id() != self.cluster_id:
            raise KafkaStreamsLifecycleError("Kafka cluster identity changed during planning")
        if current.exists and (
            bindings.get(input_topic) != current.input_topic_id
            or bindings.get(output_topic) != current.output_topic_id
            or current.network_id != self.network_id
        ):
            raise KafkaStreamsLifecycleError("Existing runner input/output/network identity changed; automatic replacement is blocked")
        desired_hash = artifact_checksum(artifact.to_dict())
        if not current.exists:
            self.progress.require_fresh_group(artifact.application_id)
            action, blocker = "create", None
        elif (
            current.artifact_hash == desired_hash and current.image_id == self.image_id
            and current.plan_hash == runner_plan_hash(artifact.plan) and current.status == "running"
        ):
            action, blocker = "none", None
        else:
            # This safety gate is removed only with same-identity replacement,
            # interrupted-operation and retention-loss acceptance coverage.
            action, blocker = "update", "kafka_streams_replacement_not_verified"
        change = KafkaStreamsJobChange(
            artifact.name, action, current, artifact,
            {"application_id": artifact.application_id, "image_id": self.image_id,
             "topic_bindings": bindings, "initial_offset": artifact.initial_offset,
             "network_id": self.network_id,
             "desired_artifact_hash": desired_hash}, self.backend_identity, blocker,
        )
        if action == "none":
            observed = self.progress.observe(artifact.application_id, input_topic, output_topic)
            observed.require_resumable()
            self._require_progress_binding(change, observed)
        return change

    def preflight(self, change: KafkaStreamsJobChange) -> None:
        """Check all runner actions before any schema/topic/provider mutation."""
        if type(change) is not KafkaStreamsJobChange or change.action not in {"create", "none"} or change.blocker:
            raise KafkaStreamsLifecycleError("Kafka Streams transition is not supported by the verified lifecycle")
        if change.desired is None or change.backend_identity != self.backend_identity:
            raise KafkaStreamsLifecycleError("Runner action lacks an exact desired/backend binding")
        artifact = self._artifact(change.desired)
        self._validate_private_directory()
        if (
            set(change.changes) != {"application_id", "image_id", "topic_bindings", "initial_offset",
                                    "network_id", "desired_artifact_hash"}
            or change.changes.get("application_id") != artifact.application_id
            or change.changes.get("image_id") != self.image_id
            or change.changes.get("initial_offset") != artifact.initial_offset
        ):
            raise KafkaStreamsLifecycleError("Runner action has inconsistent reviewed execution settings")
        self.docker.verify_daemon()
        if self.progress.cluster_id() != self.cluster_id or self.docker.image_id(artifact.image) != self.image_id:
            raise KafkaStreamsLifecycleError("Kafka or image identity changed after planning")
        if self.docker.network_id(artifact.network) != self.network_id or change.changes.get("network_id") != self.network_id:
            raise KafkaStreamsLifecycleError("Docker network identity changed after planning")
        if change.changes.get("desired_artifact_hash") != artifact_checksum(artifact.to_dict()):
            raise KafkaStreamsLifecycleError("Runner artifact changed after planning")
        current = self.get_job_state(artifact)
        if current != change.current:
            raise KafkaStreamsLifecycleError("Runner observation changed after planning")
        if (
            (change.action == "create" and current.exists)
            or (change.action == "none" and (
                not current.exists or current.status != "running"
                or current.artifact_hash != artifact_checksum(artifact.to_dict())
                or current.plan_hash != runner_plan_hash(artifact.plan) or current.image_id != self.image_id
            ))
        ):
            raise KafkaStreamsLifecycleError("Runner action does not match the supported current-to-desired transition")
        bindings = change.changes.get("topic_bindings")
        if not isinstance(bindings, dict) or set(bindings) != {
            artifact.plan["input_topic"], artifact.plan["output_topic"],
        }:
            raise KafkaStreamsLifecycleError("Runner action has incomplete topic bindings")
        for topic, identity in bindings.items():
            if identity is None and topic not in self._planned_created_topics:
                raise KafkaStreamsLifecycleError("Runner topic has no reviewed creation binding")
            if identity is not None and self.progress.topic_id(str(topic)) != identity:
                raise KafkaStreamsLifecycleError("Kafka topic was replaced after planning")
        if change.action == "create":
            self.progress.require_fresh_group(artifact.application_id)
        else:
            observed = self.progress.observe(
                artifact.application_id, str(artifact.plan["input_topic"]), str(artifact.plan["output_topic"]),
            )
            observed.require_resumable()
            self._require_progress_binding(change, observed)

    def record_created_topic(self, topic: str) -> None:
        """Capture a read-after-create receipt supplied by the same plan executor.

        This is not a Kafka CAS: an external writer can race the create response
        and this read. Later runner observations must retain this exact UUID.
        """
        if topic not in self._planned_created_topics or topic in self._created_topic_ids:
            raise KafkaStreamsLifecycleError("Unexpected or duplicate runner topic creation receipt")
        self.docker.verify_daemon()
        if self.progress.cluster_id() != self.cluster_id:
            raise KafkaStreamsLifecycleError("Kafka cluster changed before topic creation receipt")
        identity = self.progress.topic_id(topic)
        if self.progress.cluster_id() != self.cluster_id:
            raise KafkaStreamsLifecycleError("Kafka cluster changed during topic creation receipt")
        self._created_topic_ids[topic] = identity

    def _require_progress_binding(
        self, change: KafkaStreamsJobChange, progress: ApplicationProgress,
        *, created: ApplicationProgress | None = None,
    ) -> None:
        artifact = change.desired
        assert artifact is not None
        bindings = change.changes["topic_bindings"]
        assert isinstance(bindings, dict)
        if progress.cluster_id != self.cluster_id:
            raise KafkaStreamsLifecycleError("Kafka cluster changed after runner preflight")
        for topic, observed in (
            (str(artifact.plan["input_topic"]), progress.input_topic_id),
            (str(artifact.plan["output_topic"]), progress.output_topic_id),
        ):
            expected = bindings[topic]
            if expected is None:
                expected = self._created_topic_ids.get(topic)
            if expected is None or observed != expected:
                raise KafkaStreamsLifecycleError("Runner topic differs from its reviewed or created identity")
        if created is not None and (
            progress.input_topic_id != created.input_topic_id or progress.output_topic_id != created.output_topic_id
            or {item.partition for item in progress.partitions} != {item.partition for item in created.partitions}
        ):
            raise KafkaStreamsLifecycleError("Runner input/output identity or partitions changed during startup")

    def _validate_private_directory(self) -> Path:
        directory = self.state_dir / "kafka-streams"
        try:
            if directory.is_symlink():
                raise KafkaStreamsLifecycleError("Runner private directory cannot be a symlink")
            if any(character in str(directory.resolve()) for character in ",\n\r\x00"):
                raise KafkaStreamsLifecycleError("Runner private path cannot be represented as a Docker mount")
            if directory.exists() and (
                not directory.is_dir() or directory.stat().st_uid != os.getuid()
                or directory.stat().st_mode & 0o077
            ):
                raise KafkaStreamsLifecycleError("Runner private directory must be an owner-only directory")
            ancestor = directory
            while not ancestor.exists():
                ancestor = ancestor.parent
            if not ancestor.is_dir() or not os.access(ancestor, os.W_OK | os.X_OK):
                raise KafkaStreamsLifecycleError("Runner private directory is not writable")
        except (OSError, ValueError) as error:
            if isinstance(error, KafkaStreamsLifecycleError):
                raise
            raise KafkaStreamsLifecycleError("Cannot validate runner private directory") from None
        return directory

    def _private_inputs(self, artifact: KafkaStreamsJobArtifact) -> tuple[Path, Path]:
        directory = self._validate_private_directory()
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        if directory.stat().st_uid != os.getuid() or directory.stat().st_mode & 0o077:
            raise KafkaStreamsLifecycleError("Runner private directory must be owner-only")
        bundle = Path(tempfile.mkdtemp(prefix=artifact.application_id + "-", dir=directory)).resolve()
        plan_file, properties_file = bundle / "plan.json", bundle / "client.properties"
        # Individual read-only file mounts remain readable by container UID
        # 10001; the owner-only host parent prevents access by other host users.
        for path, content in ((plan_file, runner_plan_bytes(artifact.plan)), (properties_file, self._client_properties)):
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
            with os.fdopen(descriptor, "wb") as handle:
                # umask 077 must not turn this into a host-UID-only 0400 file;
                # the enclosing 0700 directory protects its host-side contents.
                os.fchmod(handle.fileno(), 0o444)
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
        return plan_file, properties_file

    def apply_job(self, change: KafkaStreamsJobChange) -> str:
        """Execute within the caller's durable operation and ownership lock."""
        self.preflight(change)
        if change.action == "none":
            return "unchanged"
        artifact = change.desired
        assert artifact is not None
        input_topic, output_topic = str(artifact.plan["input_topic"]), str(artifact.plan["output_topic"])
        progress = self.progress.observe(artifact.application_id, input_topic, output_topic)
        positions = progress.initial_positions(artifact.initial_offset)
        self._require_progress_binding(change, progress)
        plan_file, properties_file = self._private_inputs(artifact)
        volume = self.docker.ensure_state_volume(artifact.application_id, self.backend_identity)
        container_id = self.docker.create(
            application_id=artifact.application_id, image_id=self.image_id, network=self.network_id,
            plan_file=plan_file, properties_file=properties_file, state_volume=volume,
            artifact_hash=artifact_checksum(artifact.to_dict()), plan_hash=runner_plan_hash(artifact.plan),
            backend=self.backend_identity,
            input_topic_id=progress.input_topic_id, output_topic_id=progress.output_topic_id,
            cluster_id=progress.cluster_id,
        )
        self.progress.initialize(artifact.application_id, input_topic, output_topic, progress, positions)
        try:
            self.docker.owned_command("start", artifact.application_id, self.backend_identity, expected_id=container_id)
            return self._await_created_runner(change, container_id, progress)
        except Exception:
            # A failed start response may still have started the process. If
            # readiness/identity cannot be proved, request TERM only for this
            # exact owned generation. This is not proof of clean shutdown and
            # never clears its pending operation, removes state or forces a kill.
            try:
                self.docker.owned_command("term", artifact.application_id, self.backend_identity, expected_id=container_id)
            except ValueError:
                pass  # Unknown stop outcome remains covered by durable pending state.
            raise

    def _await_created_runner(
        self, change: KafkaStreamsJobChange, container_id: str, progress: ApplicationProgress,
    ) -> str:
        artifact = change.desired
        assert artifact is not None
        input_topic, output_topic = str(artifact.plan["input_topic"]), str(artifact.plan["output_topic"])
        initial_positions = progress.initial_positions(artifact.initial_offset)
        deadline = time.monotonic() + self.config.startup_timeout
        while time.monotonic() < deadline:
            try:
                state = self.get_job_state(artifact)
            except ValueError:
                # status.json may not exist while Java validates and starts.
                # Do not mistake this for readiness; the deadline is bounded.
                time.sleep(0.1)
                continue
            if state.container_id != container_id or state.status in {"stopped", "failed"}:
                raise KafkaStreamsLifecycleError("Runner did not start; inspect the durable operation")
            if state.status == "running":
                ready = self.progress.observe(artifact.application_id, input_topic, output_topic)
                ready.require_resumable()
                self._require_progress_binding(change, ready, created=progress)
                if any(item.committed is None or item.committed < initial_positions[item.partition] for item in ready.partitions):
                    raise KafkaStreamsLifecycleError("Runner progress moved behind its explicit initial positions")
                return "created"
            time.sleep(0.1)
        raise KafkaStreamsLifecycleError("Runner readiness timed out; inspect the durable operation")
