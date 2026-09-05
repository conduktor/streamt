"""Direct runtime boundary tests, independent of planner/provider test doubles."""

from __future__ import annotations

import copy
import os
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from streamt.compiler import Compiler
from streamt.compiler.manifest import parse_compiled_kafka_streams_job_artifact
from streamt.core.models import StreamtProject
from streamt.core.runtime import KafkaConfig, KafkaStreamsConfig
from streamt.deployer import kafka_streams as lifecycle
from streamt.deployer.kafka_streams_docker import (
    LABEL_APP,
    LABEL_ARTIFACT,
    LABEL_BACKEND,
    LABEL_INPUT,
    LABEL_OUTPUT,
    LABEL_PLAN,
    RUNNER_VERSION,
    LocalDockerRunner,
)
from streamt.deployer.kafka_streams_progress import (
    ApplicationProgress,
    KafkaStreamsProgress,
    KafkaStreamsProgressError,
    PartitionProgress,
)
from streamt.deployer.state import artifact_checksum
from tests.unit.test_kafka_streams_compiler import _config

CONTAINER = "c" * 64
NETWORK = "d" * 64
BACKEND = "kafka-streams-docker:v1:" + "b" * 64
INPUT = "AAAAAAAAAAAAAAAAAAAAAQ"
OUTPUT = "AAAAAAAAAAAAAAAAAAAAAg"
SECRET = "private-runtime-password"


def _fixture(tmp_path: Path):
    project = StreamtProject.model_validate(_config())
    artifact = parse_compiled_kafka_streams_job_artifact(
        Compiler(project).compile(dry_run=True).artifacts["kafka_streams_jobs"][0],
    )
    deployer = object.__new__(lifecycle.KafkaStreamsDeployer)
    deployer.config = project.runtime.kafka_streams
    deployer.kafka = project.runtime.kafka
    deployer.state_dir = tmp_path / ".streamt"
    deployer._planned_created_topics = set()
    deployer._created_topic_ids = {}
    deployer._client_properties = b"bootstrap.servers=private.invalid\n"
    deployer.docker = MagicMock(spec=LocalDockerRunner)
    deployer.docker.require_owned.return_value = CONTAINER
    deployer.docker.inspect.return_value = None
    deployer.progress = MagicMock(spec=KafkaStreamsProgress)
    deployer.cluster_id = "cluster-unit"
    deployer.backend_identity = BACKEND
    deployer.image_id = artifact.image
    deployer.network_id = NETWORK
    deployer.docker.image_id.return_value = artifact.image
    deployer.docker.network_id.return_value = NETWORK
    deployer.progress.cluster_id.return_value = deployer.cluster_id
    deployer.progress.topic_id.side_effect = lambda topic: INPUT if topic == artifact.plan["input_topic"] else OUTPUT
    return deployer, artifact


def _progress(*, committed: int | None = None, exists: bool = False):
    return ApplicationProgress("cluster-unit", INPUT, OUTPUT, exists, int(exists),
                               (PartitionProgress(0, 0, 7, committed),))


def _running(deployer, artifact):
    deployer.docker.inspect.return_value = {
        "Id": CONTAINER, "Name": "/" + artifact.application_id,
        "Image": artifact.image, "HostConfig": {"NetworkMode": NETWORK},
        "Config": {"Labels": {
            LABEL_APP: artifact.application_id, LABEL_BACKEND: BACKEND,
            LABEL_ARTIFACT: artifact_checksum(artifact.to_dict()),
            LABEL_PLAN: lifecycle.runner_plan_hash(artifact.plan), LABEL_INPUT: INPUT, LABEL_OUTPUT: OUTPUT,
        }},
        "State": {"Running": True, "ExitCode": 0, "StartedAt": "2026-09-05T10:00:00.000000000Z"},
    }
    deployer.docker.status_document.return_value = {
        "application_id": artifact.application_id, "runner_version": RUNNER_VERSION, "plan_version": 1,
        "plan_sha256": lifecycle.runner_plan_hash(artifact.plan), "state": "running",
        "reason": None, "updated_at": "2026-09-05T10:00:01.123456789Z",
        "cluster_id": "cluster-unit", "input_topic_id": INPUT, "output_topic_id": OUTPUT,
    }
    deployer.progress.observe.return_value = _progress(committed=5, exists=True)


def _no_mutations(deployer):
    for name in ("create", "ensure_state_volume", "owned_command"):
        getattr(deployer.docker, name).assert_not_called()
    deployer.progress.initialize.assert_not_called()
    assert not deployer.state_dir.exists()


@pytest.mark.parametrize(("mechanism", "module"), [
    ("PLAIN", "plain.PlainLoginModule"), ("SCRAM-SHA-256", "scram.ScramLoginModule"),
    ("SCRAM-SHA-512", "scram.ScramLoginModule"),
])
def test_security_translation_uses_runner_endpoint_and_preserves_jaas_escaping(mechanism, module):
    kafka = KafkaConfig(bootstrap_servers="host.invalid:9092", bootstrap_servers_internal="broker:19092",
                        security_protocol="SASL_PLAINTEXT", sasl_mechanism=mechanism,
                        sasl_username='a"b\\c', sasl_password=SECRET)
    value = lifecycle.runner_client_properties(kafka).decode("ascii")
    assert "host.invalid" not in value
    assert "bootstrap.servers=broker\\:19092\n" in value
    assert module in value
    assert 'username\\="a\\\\"b\\\\\\\\c"' in value
    assert SECRET in value
    assert len(value.splitlines()) == 4


@pytest.mark.parametrize("fields", [
    {"security_protocol": "SASL_SSL", "sasl_mechanism": "GSSAPI"},
    {"security_protocol": "SASL_SSL", "sasl_mechanism": "OAUTHBEARER"},
    {"security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN"},
    {"security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN", "sasl_username": "user"},
    {"sasl_username": "user", "sasl_password": SECRET},
    {"ssl_key_password": SECRET},
    {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN",
     "sasl_username": "user", "sasl_password": SECRET + "\ninjected=value"},
])
def test_unsupported_security_fails_before_either_provider(tmp_path, monkeypatch, fields):
    docker = MagicMock(side_effect=AssertionError("Docker constructed"))
    progress = MagicMock(side_effect=AssertionError("Kafka constructed"))
    monkeypatch.setattr(lifecycle, "LocalDockerRunner", docker)
    monkeypatch.setattr(lifecycle, "KafkaStreamsProgress", progress)
    with pytest.raises(ValueError) as caught:
        lifecycle.KafkaStreamsDeployer(KafkaStreamsConfig(image="sha256:" + "a" * 64),
                                       KafkaConfig(bootstrap_servers="private.invalid", **fields), state_dir=tmp_path)
    assert SECRET not in str(caught.value)
    docker.assert_not_called()
    progress.assert_not_called()


def test_private_inputs_are_unique_owner_protected_and_never_enter_artifact(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    deployer._client_properties = SECRET.encode()
    original = copy.deepcopy(artifact.to_dict())
    plan, properties = deployer._private_inputs(artifact)
    next_plan, next_properties = deployer._private_inputs(artifact)
    assert plan.parent != next_plan.parent
    for path in (plan, properties, next_plan, next_properties):
        assert path.stat().st_mode & 0o777 == 0o444
        assert path.parent.stat().st_mode & 0o777 == 0o700
        assert path.parent.parent.stat().st_mode & 0o777 == 0o700
        assert path.stat().st_uid == os.getuid()
    assert plan.read_bytes() == lifecycle.runner_plan_bytes(artifact.plan)
    assert properties.read_bytes() == SECRET.encode()
    assert artifact.to_dict() == original
    assert SECRET not in str(artifact)
    deployer.close()
    assert deployer._client_properties == b""


@pytest.mark.parametrize("unsafe", ["symlink", "world-readable"])
def test_unsafe_private_directory_is_refused_without_writing_secret(tmp_path, unsafe):
    deployer, artifact = _fixture(tmp_path)
    deployer.state_dir.mkdir()
    directory = deployer.state_dir / "kafka-streams"
    if unsafe == "symlink":
        destination = tmp_path / "foreign"
        destination.mkdir()
        directory.symlink_to(destination, target_is_directory=True)
    else:
        directory.mkdir(mode=0o755)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer._private_inputs(artifact)
    assert list(directory.iterdir()) == []


@pytest.mark.parametrize(("field", "value"), [
    ("runner_version", "9.0.0"), ("plan_version", True), ("plan_version", 2),
    ("application_id", "other-app"), ("plan_sha256", "sha256:" + "f" * 64),
    ("state", "invented"), ("state", None), ("state", []), ("state", {}), ("reason", SECRET),
    ("updated_at", "2026-09-05T09:59:59Z"), ("updated_at", "2026-09-05T10:00:01"),
    ("updated_at", "invalid"), ("updated_at", None),
])
def test_running_status_requires_exact_current_container_evidence(tmp_path, field, value):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    deployer.docker.status_document.return_value[field] = value
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError) as caught:
        deployer.get_job_state(artifact)
    assert SECRET not in str(caught.value)
    _no_mutations(deployer)


@pytest.mark.parametrize("field", ["StartedAt", "Running", "ExitCode"])
def test_malformed_container_state_is_not_healthy(tmp_path, field):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    deployer.docker.inspect.return_value["State"].pop(field)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.get_job_state(artifact)


def test_current_runtime_is_noop_and_offsets_are_rechecked_without_initialization(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    change = deployer.plan_job(artifact)
    assert change.action == "none"
    assert deployer.apply_job(change) == "unchanged"
    assert deployer.progress.observe.call_count >= 2
    _no_mutations(deployer)


def test_offset_loss_after_noop_plan_is_not_reported_as_success(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    change = deployer.plan_job(artifact)
    deployer.progress.observe.return_value = _progress()
    with pytest.raises(KafkaStreamsProgressError):
        deployer.apply_job(change)
    _no_mutations(deployer)


def test_plan_label_drift_is_never_a_noop_even_with_matching_artifact_label(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    deployer.docker.inspect.return_value["Config"]["Labels"][LABEL_PLAN] = "sha256:" + "f" * 64
    deployer.docker.status_document.return_value["plan_sha256"] = "sha256:" + "f" * 64
    change = deployer.plan_job(artifact)
    assert change.blocker == "kafka_streams_replacement_not_verified"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.apply_job(change)
    _no_mutations(deployer)


@pytest.mark.parametrize("field", ["application_id", "image_id", "initial_offset", "unexpected"])
def test_tampered_reviewed_execution_settings_fail_before_mutation(tmp_path, field):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact)
    change.changes[field] = "tampered"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.apply_job(change)
    _no_mutations(deployer)


@pytest.mark.parametrize("material", ["ca", "pkcs1-key", "malformed-pkcs8", "missing-client-key"])
def test_invalid_local_tls_material_is_refused_before_providers(tmp_path, monkeypatch, material):
    certificate = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    certificate.write_text(SECRET)
    key.write_text("-----BEGIN PRIVATE KEY-----\n" + SECRET)
    fields = {"ssl_ca_location": str(certificate)} if material == "ca" else {
        "ssl_certificate_location": str(certificate), "ssl_key_location": str(key),
    }
    if material == "pkcs1-key":
        key.write_text("-----BEGIN RSA PRIVATE KEY-----\n" + SECRET)
    if material == "missing-client-key":
        fields.pop("ssl_key_location")
    kafka = KafkaConfig(bootstrap_servers="private.invalid", security_protocol="SSL", **fields)
    docker = MagicMock(side_effect=AssertionError("Docker constructed"))
    progress = MagicMock(side_effect=AssertionError("Kafka constructed"))
    monkeypatch.setattr(lifecycle, "LocalDockerRunner", docker)
    monkeypatch.setattr(lifecycle, "KafkaStreamsProgress", progress)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError) as caught:
        lifecycle.KafkaStreamsDeployer(KafkaStreamsConfig(image="sha256:" + "a" * 64), kafka, state_dir=tmp_path)
    assert SECRET not in str(caught.value)
    docker.assert_not_called()
    progress.assert_not_called()


@pytest.mark.parametrize("mismatch", ["cluster", "image", "network", "input", "artifact", "offset-policy", "container"])
def test_create_revalidates_all_reviewed_identities_before_mutation(tmp_path, mismatch):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact)
    if mismatch == "cluster":
        deployer.progress.cluster_id.return_value = "changed-cluster"
    elif mismatch == "image":
        deployer.docker.image_id.return_value = "sha256:" + "f" * 64
    elif mismatch == "network":
        deployer.docker.network_id.return_value = "f" * 64
    elif mismatch == "input":
        deployer.progress.topic_id.side_effect = None
        deployer.progress.topic_id.return_value = "AAAAAAAAAAAAAAAAAAAAAw"
    elif mismatch == "artifact":
        change.desired.plan["predicates"][0]["value"] = 99
    elif mismatch == "offset-policy":
        deployer.config = deployer.config.model_copy(update={"initial_offset": "latest"})
    else:
        _running(deployer, artifact)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.apply_job(change)
    _no_mutations(deployer)


def test_unverified_replacement_is_blocked_before_stop_or_offset_change(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    artifact.plan["predicates"][0]["value"] = 100
    change = deployer.plan_job(artifact)
    assert change.action == "update"
    assert change.blocker == "kafka_streams_replacement_not_verified"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.apply_job(change)
    _no_mutations(deployer)


def test_create_initializes_once_before_start_and_checks_committed_progress(tmp_path, monkeypatch):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact)
    events = []
    deployer.progress.observe.side_effect = [_progress(), _progress(committed=0, exists=True)]
    deployer.docker.ensure_state_volume.return_value = artifact.application_id + "-state"
    deployer.docker.create.side_effect = lambda **_: events.append("created-stopped") or CONTAINER
    deployer.progress.initialize.side_effect = lambda *_: events.append("initialize")
    deployer.docker.owned_command.side_effect = lambda *_args, **_kwargs: events.append("start")
    absent = deployer.get_job_state(artifact)
    _running(deployer, artifact)
    running = deployer.get_job_state(artifact)
    monkeypatch.setattr(deployer, "get_job_state", MagicMock(side_effect=[absent, running]))
    deployer.progress.observe.side_effect = [_progress(), _progress(committed=0, exists=True)]
    assert deployer.apply_job(change) == "created"
    assert events == ["created-stopped", "initialize", "start"]
    deployer.progress.initialize.assert_called_once_with(
        artifact.application_id, artifact.plan["input_topic"], artifact.plan["output_topic"], _progress(), {0: 0},
    )
    deployer.docker.owned_command.assert_called_once_with(
        "start", artifact.application_id, BACKEND, expected_id=CONTAINER,
    )


def test_failed_initialization_preserves_stopped_container_and_never_starts_or_resets(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact)
    deployer.progress.observe.return_value = _progress()
    deployer.docker.ensure_state_volume.return_value = artifact.application_id + "-state"
    deployer.docker.create.return_value = CONTAINER
    deployer.progress.initialize.side_effect = KafkaStreamsProgressError("coordinator not ready")
    with pytest.raises(KafkaStreamsProgressError):
        deployer.apply_job(change)
    deployer.docker.create.assert_called_once()
    deployer.progress.initialize.assert_called_once()
    deployer.docker.owned_command.assert_not_called()


@pytest.mark.parametrize("address", [
    "https://broker:9092", "broker", "broker:0", "broker:65536", "broker:9092,",
    "broker:9092, other:9092", "user:password@broker:9092", "broker_name:9092", "broker:123456",
])
def test_invalid_internal_bootstrap_fails_before_provider_construction(tmp_path, monkeypatch, address):
    docker = MagicMock(side_effect=AssertionError("Docker constructed"))
    progress = MagicMock(side_effect=AssertionError("Kafka constructed"))
    monkeypatch.setattr(lifecycle, "LocalDockerRunner", docker)
    monkeypatch.setattr(lifecycle, "KafkaStreamsProgress", progress)
    kafka = KafkaConfig(bootstrap_servers="host:9092", bootstrap_servers_internal=address)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match="bootstrap"):
        lifecycle.KafkaStreamsDeployer(KafkaStreamsConfig(image="sha256:" + "a" * 64), kafka, state_dir=tmp_path)
    docker.assert_not_called()
    progress.assert_not_called()


@pytest.mark.parametrize("address", ["broker:9092", "a.example:1,127.0.0.1:65535", "[::1]:9092"])
def test_supported_internal_address_grammar_remains_local(address):
    kafka = KafkaConfig(bootstrap_servers="host:9092", bootstrap_servers_internal=address)
    assert b"bootstrap.servers=" in lifecycle.runner_client_properties(kafka)


def test_serialized_properties_size_is_checked_before_providers(tmp_path, monkeypatch):
    docker = MagicMock(side_effect=AssertionError("Docker constructed"))
    progress = MagicMock(side_effect=AssertionError("Kafka constructed"))
    monkeypatch.setattr(lifecycle, "LocalDockerRunner", docker)
    monkeypatch.setattr(lifecycle, "KafkaStreamsProgress", progress)
    kafka = KafkaConfig(bootstrap_servers="host:9092", security_protocol="SASL_PLAINTEXT",
                        sasl_mechanism="PLAIN", sasl_username="user", sasl_password="x" * 1_048_576)
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match="1 MiB"):
        lifecycle.KafkaStreamsDeployer(KafkaStreamsConfig(image="sha256:" + "a" * 64), kafka, state_dir=tmp_path)
    docker.assert_not_called()
    progress.assert_not_called()


def test_restrictive_umask_keeps_private_container_inputs_readable(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    original = os.umask(0o077)
    try:
        plan, properties = deployer._private_inputs(artifact)
    finally:
        os.umask(original)
    assert plan.stat().st_mode & 0o777 == properties.stat().st_mode & 0o777 == 0o444
    assert plan.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("unsafe", ["comma", "world-readable", "symlink", "file"])
def test_private_path_preflight_precedes_every_topic_mutation(tmp_path, unsafe):
    from tests.unit.test_planner_kafka_streams import _planner

    planner, kafka, _runner, _change = _planner()
    deployer, _artifact = _fixture(tmp_path)
    planner.kafka_streams_deployer = deployer
    plan = planner.plan()
    if unsafe == "comma":
        deployer.state_dir = tmp_path / "invalid,mount"
    else:
        deployer.state_dir.mkdir()
        private = deployer.state_dir / "kafka-streams"
        if unsafe == "world-readable":
            private.mkdir(mode=0o755)
        elif unsafe == "symlink":
            private.symlink_to(tmp_path, target_is_directory=True)
        else:
            private.write_text("not a directory")
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        planner.apply(plan)
    kafka.apply_topic.assert_not_called()
    deployer.docker.create.assert_not_called()
    deployer.progress.initialize.assert_not_called()


@pytest.mark.parametrize("field", ["cluster_id", "input_topic_id", "output_topic_id"])
@pytest.mark.parametrize("action", ["create", "none"])
def test_observe_snapshot_cannot_rebase_a_reviewed_cluster_or_topic(tmp_path, field, action):
    deployer, artifact = _fixture(tmp_path)
    if action == "none":
        _running(deployer, artifact)
    change = deployer.plan_job(artifact)
    observed = _progress(committed=0, exists=True) if action == "none" else _progress()
    value = "other-cluster" if field == "cluster_id" else "AAAAAAAAAAAAAAAAAAAAAw"
    deployer.progress.observe.return_value = replace(observed, **{field: value})
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match=r"identity|cluster"):
        deployer.apply_job(change)
    _no_mutations(deployer)


def test_noop_planning_checks_the_joint_progress_snapshot(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    deployer.progress.observe.return_value = replace(_progress(committed=0, exists=True), cluster_id="other")
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match="cluster"):
        deployer.plan_job(artifact)
    _no_mutations(deployer)


def test_planned_new_topic_requires_its_acknowledged_creation_receipt(tmp_path):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact, new_topics=frozenset({artifact.plan["output_topic"]}))
    deployer.progress.observe.return_value = _progress()
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match="created identity"):
        deployer.apply_job(change)
    _no_mutations(deployer)
    deployer.record_created_topic(str(artifact.plan["output_topic"]))
    deployer._require_progress_binding(change, _progress())
    assert change.changes["topic_bindings"][artifact.plan["output_topic"]] is None
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.record_created_topic(str(artifact.plan["output_topic"]))
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.record_created_topic("not-a-planned-create")


@pytest.mark.parametrize("field", ["cluster_id", "input_topic_id", "output_topic_id"])
def test_ready_status_must_report_the_cluster_and_topic_ids_it_verified(tmp_path, field):
    deployer, artifact = _fixture(tmp_path)
    _running(deployer, artifact)
    deployer.docker.status_document.return_value[field] = "different"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.get_job_state(artifact)
    _no_mutations(deployer)


@pytest.mark.parametrize("field", ["cluster_id", "input_topic_id", "output_topic_id"])
def test_failed_ready_identity_requests_term_only_for_this_created_generation(tmp_path, monkeypatch, field):
    deployer, artifact = _fixture(tmp_path)
    change = deployer.plan_job(artifact)
    absent = deployer.get_job_state(artifact)
    _running(deployer, artifact)
    running = deployer.get_job_state(artifact)
    monkeypatch.setattr(deployer, "get_job_state", MagicMock(side_effect=[absent, running]))
    value = "other-cluster" if field == "cluster_id" else "AAAAAAAAAAAAAAAAAAAAAw"
    deployer.progress.observe.side_effect = [_progress(), replace(_progress(committed=0, exists=True), **{field: value})]
    deployer.docker.create.return_value = CONTAINER
    deployer.docker.ensure_state_volume.return_value = artifact.application_id + "-state"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError):
        deployer.apply_job(change)
    assert [call.args[0] for call in deployer.docker.owned_command.call_args_list] == ["start", "term"]
    for call in deployer.docker.owned_command.call_args_list:
        assert call.args[1:] == (artifact.application_id, BACKEND)
        assert call.kwargs == {"expected_id": CONTAINER}
    deployer.progress.initialize.assert_called_once()


def test_latest_initial_position_cannot_silently_regress_before_readiness(tmp_path, monkeypatch):
    deployer, artifact = _fixture(tmp_path)
    artifact = replace(artifact, initial_offset="latest")
    deployer.config = deployer.config.model_copy(update={"initial_offset": "latest"})
    change = deployer.plan_job(artifact)
    absent = deployer.get_job_state(artifact)
    _running(deployer, artifact)
    running = deployer.get_job_state(artifact)
    monkeypatch.setattr(deployer, "get_job_state", MagicMock(side_effect=[absent, running]))
    deployer.progress.observe.side_effect = [_progress(), _progress(committed=0, exists=True)]
    deployer.docker.create.return_value = CONTAINER
    deployer.docker.ensure_state_volume.return_value = artifact.application_id + "-state"
    with pytest.raises(lifecycle.KafkaStreamsLifecycleError, match="initial positions"):
        deployer.apply_job(change)
    deployer.progress.initialize.assert_called_once_with(
        artifact.application_id, artifact.plan["input_topic"], artifact.plan["output_topic"], _progress(), {0: 7},
    )
    assert [call.args[0] for call in deployer.docker.owned_command.call_args_list] == ["start", "term"]
