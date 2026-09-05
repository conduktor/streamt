"""Read-only bridge from Docker/Kafka to typed replacement evidence.

This module grants no operation authority and performs no runtime mutation.
Callers supply the ownership record read under their deployment-state lock.
"""

from __future__ import annotations

from typing import Literal, cast

from streamt.compiler.manifest import ArtifactOwnership, KafkaStreamsJobArtifact
from streamt.deployer.kafka_streams import KafkaStreamsDeployer
from streamt.deployer.kafka_streams_docker import (
    LABEL_ARTIFACT,
    LABEL_INPUT,
    LABEL_OUTPUT,
    LABEL_PLAN,
    RUNNER_VERSION,
)
from streamt.deployer.kafka_streams_evidence import (
    KafkaStreamsActionEvidence,
    KafkaStreamsArtifactSnapshot,
    KafkaStreamsPartitionEvidence,
    KafkaStreamsProgressEvidence,
    KafkaStreamsVolumeEvidence,
)
from streamt.deployer.kafka_streams_replacement import (
    ReplacementContainerObservation,
    ReplacementObservation,
)
from streamt.deployer.kafka_streams_time import parse_utc_timestamp
from streamt.deployer.state import ManagedResourceRecord

_ZERO = parse_utc_timestamp("0001-01-01T00:00:00Z")
_ENTRYPOINT = ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/opt/streamt/runner/runner.jar"]
_STATUS_FIELDS = {
    "application_id", "runner_version", "plan_version", "plan_sha256", "state", "reason",
    "updated_at", "cluster_id", "input_topic_id", "output_topic_id",
}
_ProcessState = Literal["created", "running", "exited"]
_RunnerState = Literal["starting", "running", "closing", "closed", "failed"]


class KafkaStreamsReplacementObservationError(ValueError):
    """An exact replacement surface cannot be established without guessing."""


def _object(value: object, message: str) -> dict[str, object]:
    if type(value) is not dict:
        raise KafkaStreamsReplacementObservationError(message)
    return cast(dict[str, object], value)


class KafkaStreamsReplacementObserver:
    """Prepare/re-observe one managed job; never stop, create, initialize or clear."""

    def __init__(self, deployer: KafkaStreamsDeployer) -> None:
        if type(deployer) is not KafkaStreamsDeployer:
            raise KafkaStreamsReplacementObservationError("An exact bound runner deployer is required")
        self.deployer = deployer

    def _bindings(self, desired: KafkaStreamsJobArtifact) -> tuple[str, str, str]:
        runtime = self.deployer
        runtime._artifact(desired)
        runtime._validate_private_directory()
        runtime.docker.verify_daemon()
        cluster = runtime.progress.cluster_id()
        image = runtime.docker.image_id(desired.image)
        network = runtime.docker.network_id(desired.network)
        if (
            cluster != runtime.cluster_id
            or runtime.docker.backend_identity(cluster) != runtime.backend_identity
            or image != runtime.image_id or network != runtime.network_id
        ):
            raise KafkaStreamsReplacementObservationError("Runner provider identity changed")
        return cluster, image, network

    @staticmethod
    def _ownership(record: ManagedResourceRecord, desired: KafkaStreamsJobArtifact, backend: str) -> None:
        ownership = ArtifactOwnership.from_dict(desired.ownership)
        if (
            type(record) is not ManagedResourceRecord or ownership is None
            or ownership.mode not in {"managed", "adopted"}
            or record.ownership != ownership.mode
            or record.physical_name != desired.application_id or record.backend != backend
        ):
            raise KafkaStreamsReplacementObservationError("Protected runner ownership does not match")

    def _progress(self, desired: KafkaStreamsJobArtifact) -> KafkaStreamsProgressEvidence:
        raw = self.deployer.progress.observe(
            desired.application_id, str(desired.plan["input_topic"]), str(desired.plan["output_topic"]),
        )
        raw.require_resumable()
        return KafkaStreamsProgressEvidence(
            raw.cluster_id, raw.input_topic_id, raw.output_topic_id, raw.group_exists, raw.active_members,
            tuple(KafkaStreamsPartitionEvidence(item.partition, item.low, item.high, cast(int, item.committed))
                  for item in sorted(raw.partitions, key=lambda item: item.partition)),
        )

    def prepare(
        self, desired: KafkaStreamsJobArtifact, ownership_record: ManagedResourceRecord,
    ) -> KafkaStreamsActionEvidence:
        """Reconstruct the old artifact from its mounted plan and protected checksum."""
        try:
            desired = self.deployer._artifact(desired)
            self._ownership(ownership_record, desired, self.deployer.backend_identity)
            cluster, image, network = self._bindings(desired)
            docker = self.deployer.docker
            data = docker.inspect(desired.application_id)
            if data is None:
                raise KafkaStreamsReplacementObservationError("Replacement requires an existing owned runner")
            old_id = docker.require_owned(data, desired.application_id, self.deployer.backend_identity)
            witness = docker.plan_witness(old_id)
            before = desired.to_dict()
            before["plan"] = witness.document
            prior = KafkaStreamsArtifactSnapshot.from_dict(before)
            if prior.checksum != ownership_record.artifact_checksum or prior.plan_hash != witness.sha256:
                raise KafkaStreamsReplacementObservationError("Mounted prior plan does not match protected ownership")
            volume = KafkaStreamsVolumeEvidence.from_dict(docker.volume_witness(
                desired.application_id, self.deployer.backend_identity,
            ))
            progress = self._progress(desired)
            if progress.cluster_id != cluster:
                raise KafkaStreamsReplacementObservationError("Kafka cluster changed during replacement preparation")
            evidence = KafkaStreamsActionEvidence(
                1, self.deployer.backend_identity, old_id, prior,
                KafkaStreamsArtifactSnapshot.from_artifact(desired), image, network, volume, progress,
            )
            observed = self.observe(evidence, ownership_record)
            observed.progress.require_at_least(progress)
            if (
                observed.prior_container is None or not observed.prior_container.ready
                or observed.candidate_container is not None or observed.progress.active_members != 1
            ):
                raise KafkaStreamsReplacementObservationError("Replacement requires one healthy prior runner")
            # Retain the initial reviewed lower bound, not the later observation.
            return evidence
        except KafkaStreamsReplacementObservationError:
            raise
        except Exception:
            raise KafkaStreamsReplacementObservationError("Cannot prepare exact runner replacement evidence") from None

    def observe(
        self, evidence: KafkaStreamsActionEvidence, ownership_record: ManagedResourceRecord,
    ) -> ReplacementObservation:
        """Read actual stopped/running generations, with no speculative recovery."""
        try:
            if type(evidence) is not KafkaStreamsActionEvidence:
                raise KafkaStreamsReplacementObservationError("Exact typed replacement evidence is required")
            desired = evidence.desired_artifact.artifact
            self._ownership(ownership_record, desired, evidence.backend_identity)
            if ownership_record.artifact_checksum not in {
                evidence.prior_artifact.checksum, evidence.desired_artifact.checksum,
            }:
                raise KafkaStreamsReplacementObservationError("Protected runner artifact changed")
            if evidence.backend_identity != self.deployer.backend_identity:
                raise KafkaStreamsReplacementObservationError("Replacement belongs to a different backend")
            cluster, image, network = self._bindings(desired)
            if (cluster, image, network) != (evidence.progress.cluster_id, evidence.image_id, evidence.network_id):
                raise KafkaStreamsReplacementObservationError("Reviewed runner provider identities changed")
            docker = self.deployer.docker
            volume = KafkaStreamsVolumeEvidence.from_dict(docker.volume_witness(
                desired.application_id, evidence.backend_identity,
            ))
            if volume != evidence.volume:
                raise KafkaStreamsReplacementObservationError("Reviewed runner volume instance changed")
            inventory = docker.application_containers(desired.application_id)
            old = docker.inspect_exact(evidence.prior_container_id)
            slot = docker.inspect(desired.application_id)
            expected_inventory = () if slot is None else (slot.get("Id"),)
            if type(inventory) is not tuple or inventory != expected_inventory:
                raise KafkaStreamsReplacementObservationError("Runner application has an unaccounted generation")
            if old is not None:
                old_id = docker.require_owned(old, desired.application_id, evidence.backend_identity)
                if old_id != evidence.prior_container_id or slot is None or slot.get("Id") != old_id:
                    raise KafkaStreamsReplacementObservationError("Runner name slot and exact prior identity disagree")
                candidate = None
            else:
                if slot is not None and slot.get("Id") == evidence.prior_container_id:
                    raise KafkaStreamsReplacementObservationError("Prior runner changed during absence observation")
                candidate = slot
            prior_surface = self._container(old, evidence, candidate=False) if old is not None else None
            candidate_surface = self._container(candidate, evidence, candidate=True) if candidate is not None else None
            progress = self._progress(desired)
            progress.require_at_least(evidence.progress)
            # Bracket observations with the same bindings/volume and exact slot.
            self._bindings(desired)
            docker.require_volume(evidence.volume)
            final_slot = docker.inspect(desired.application_id)
            if (None if final_slot is None else final_slot.get("Id")) != (None if slot is None else slot.get("Id")):
                raise KafkaStreamsReplacementObservationError("Runner name slot changed during observation")
            if slot is not None and final_slot is not None:
                final_surface = self._container(final_slot, evidence, candidate=candidate is not None)
                initial_surface = candidate_surface if candidate is not None else prior_surface
                initial_data = candidate if candidate is not None else old
                assert initial_data is not None
                if (
                    final_surface != initial_surface or final_slot.get("State") != initial_data.get("State")
                    or final_slot.get("Created") != initial_data.get("Created")
                ):
                    raise KafkaStreamsReplacementObservationError("Runner process changed during observation")
            if docker.application_containers(desired.application_id) != inventory:
                raise KafkaStreamsReplacementObservationError("Runner application inventory changed during observation")
            return ReplacementObservation(
                evidence.backend_identity, image, network, volume, progress,
                prior_surface, candidate_surface, ownership_record,
            )
        except KafkaStreamsReplacementObservationError:
            raise
        except Exception:
            raise KafkaStreamsReplacementObservationError("Cannot observe exact runner replacement evidence") from None

    def _container(
        self, data: dict[str, object], evidence: KafkaStreamsActionEvidence, *, candidate: bool,
    ) -> ReplacementContainerObservation:
        docker = self.deployer.docker
        artifact = evidence.desired_artifact if candidate else evidence.prior_artifact
        container_id = docker.require_owned(data, evidence.application_id, evidence.backend_identity)
        docker.validate_mounts(data, evidence.volume)
        docker.validate_process_environment(data, evidence.image_id)
        witness = docker.plan_witness(container_id)
        config = _object(data.get("Config"), "Runner configuration is incomplete")
        labels = _object(config.get("Labels"), "Runner labels are incomplete")
        host = _object(data.get("HostConfig"), "Runner host configuration is incomplete")
        state = _object(data.get("State"), "Runner process state is incomplete")
        restart = _object(host.get("RestartPolicy"), "Runner restart policy is incomplete")
        expected_args = [
            "--plan", "/run/streamt/plan.json", "--client-properties", "/run/streamt/client.properties",
            "--application-id", evidence.application_id, "--state-dir", "/var/lib/streamt/state",
            "--expected-cluster-id", evidence.progress.cluster_id,
            "--expected-input-topic-id", evidence.progress.input_topic_id,
            "--expected-output-topic-id", evidence.progress.output_topic_id,
        ]
        if (
            data.get("Image") != evidence.image_id or config.get("Image") != evidence.image_id
            or config.get("Entrypoint") != _ENTRYPOINT or config.get("Cmd") != expected_args
            or config.get("User") != "10001:10001" or host.get("NetworkMode") != evidence.network_id
            or host.get("ReadonlyRootfs") is not True or host.get("Privileged") is not False
            or host.get("AutoRemove") is not False or host.get("CapDrop") != ["ALL"]
            or "CapAdd" not in host or host["CapAdd"] not in (None, [])
            or host.get("SecurityOpt") != ["no-new-privileges"]
            or restart != {"Name": "no", "MaximumRetryCount": 0}
            or type(restart["MaximumRetryCount"]) is not int
            or any(type(host.get(key)) is not int or host[key] != value for key, value in (
                ("Memory", 536870912), ("NanoCpus", 1000000000), ("PidsLimit", 128),
            ))
            or type(data.get("RestartCount")) is not int or data.get("RestartCount") != 0
            or witness.sha256 != artifact.plan_hash or labels.get(LABEL_PLAN) != witness.sha256
            or labels.get(LABEL_ARTIFACT) != artifact.checksum
            or labels.get(LABEL_INPUT) != evidence.progress.input_topic_id
            or labels.get(LABEL_OUTPUT) != evidence.progress.output_topic_id
        ):
            raise KafkaStreamsReplacementObservationError("Runner execution surface does not match reviewed evidence")
        process = state.get("Status")
        if (
            type(process) is not str or process not in {"created", "running", "exited"}
            or type(state.get("Running")) is not bool or state["Running"] != (process == "running")
            or any(state.get(key) is not False for key in ("Paused", "Restarting", "OOMKilled", "Dead"))
            or state.get("Error") != "" or type(state.get("ExitCode")) is not int
            or not 0 <= cast(int, state["ExitCode"]) <= 255
        ):
            raise KafkaStreamsReplacementObservationError("Runner process outcome is incomplete or unsafe")
        networks = _object(_object(data.get("NetworkSettings"), "Runner network settings are incomplete").get("Networks"),
                           "Runner network attachments are incomplete")
        if len(networks) != 1:
            raise KafkaStreamsReplacementObservationError("Runner network attachments changed")
        attached = _object(next(iter(networks.values())), "Runner network attachment is incomplete").get("NetworkID")
        if attached != evidence.network_id and not (process != "running" and attached == ""):
            raise KafkaStreamsReplacementObservationError("Runner network attachment identity changed")
        created = parse_utc_timestamp(data.get("Created"))
        started = parse_utc_timestamp(state.get("StartedAt"))
        finished = parse_utc_timestamp(state.get("FinishedAt"))
        if created == _ZERO:
            raise KafkaStreamsReplacementObservationError("Runner creation timestamp is missing")
        runner_state = None
        fresh = False
        if process == "created":
            if started != _ZERO or finished != _ZERO or state["ExitCode"] != 0:
                raise KafkaStreamsReplacementObservationError("Candidate has already started or has an unknown outcome")
            # The retained volume still contains the old process's status.
        else:
            if started < created or (process == "exited" and finished < started):
                raise KafkaStreamsReplacementObservationError("Runner process timestamps are inconsistent")
            if process == "running" and (state["ExitCode"] != 0 or finished > started):
                raise KafkaStreamsReplacementObservationError("Running process carries an inconsistent exit outcome")
            document = docker.status_document(container_id)
            runner_state = document.get("state")
            if (
                set(document) != _STATUS_FIELDS or document.get("application_id") != evidence.application_id
                or document.get("runner_version") != RUNNER_VERSION
                or type(document.get("plan_version")) is not int or document.get("plan_version") != 1
                or document.get("plan_sha256") != witness.sha256
                or document.get("cluster_id") != evidence.progress.cluster_id
                or document.get("input_topic_id") != evidence.progress.input_topic_id
                or document.get("output_topic_id") != evidence.progress.output_topic_id
                or type(runner_state) is not str or runner_state not in {"starting", "running", "closing", "closed", "failed"}
                or (runner_state != "failed" and document.get("reason") is not None)
                or (runner_state == "failed" and type(document.get("reason")) is not str)
            ):
                raise KafkaStreamsReplacementObservationError("Runner status identity is incomplete or inconsistent")
            updated = parse_utc_timestamp(document.get("updated_at"))
            fresh = started <= updated and (process != "exited" or updated <= finished)
        return ReplacementContainerObservation(
            container_id, evidence.application_id, evidence.backend_identity, artifact.checksum,
            witness.sha256, evidence.image_id, evidence.network_id, evidence.volume,
            cast(_ProcessState, process), cast(int, state["ExitCode"]) if process == "exited" else None,
            False, cast(_RunnerState | None, runner_state), fresh, docker.generation(data),
        )
