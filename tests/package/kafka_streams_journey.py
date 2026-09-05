"""Real source/installed CLI journeys on an exactly owned disposable Kafka broker.

Run with --mode source --checkout PATH or from an isolated installed-wheel
environment with --mode installed --checkout PATH. No image is pulled. This
gate proves create/no-op, not replacement, recovery, or custom-app scheduling.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka import __version__ as kafka_client_version
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource, NewTopic, ResourceType

import streamt
from streamt.compiler import Compiler
from streamt.compiler.manifest import TopicArtifact
from streamt.core.parser import ProjectParser
from streamt.deployer.kafka import KafkaDeployer
from streamt.deployer.kafka_streams_progress import KafkaStreamsProgress
from streamt.runtime_assets import load_runtime_build_inputs, runtime_build_contract

BROKER_IMAGE = "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837"
OWNER_LABEL = "io.streamt.journey.owner"
OFFLINE_GUARD = """
import runpy, socket, subprocess, sys
import confluent_kafka, confluent_kafka.admin, requests
def forbidden(*args, **kwargs):
    print('JOURNEY_OFFLINE_ACCESS_DENIED', file=sys.stderr, flush=True)
    raise RuntimeError('Offline journey attempted provider access')
socket.getaddrinfo = socket.create_connection = forbidden
socket.socket.connect = socket.socket.connect_ex = forbidden
subprocess.run = subprocess.Popen = forbidden
confluent_kafka.Consumer = confluent_kafka.Producer = forbidden
confluent_kafka.admin.AdminClient = forbidden
requests.Session.request = forbidden
runpy.run_module('streamt', run_name='__main__')
"""
COLUMNS = [
    {"name": "id", "type": "STRING", "required": True},
    {"name": "amount", "type": "BIGINT"},
    {"name": "paid", "type": "BOOLEAN", "required": True},
]


class Journey:
    def __init__(self, checkout: Path, mode: str, image: str, evidence_parent: Path | None = None) -> None:
        self.checkout, self.mode, self.image = checkout.resolve(), mode, image
        self.token = uuid.uuid4().hex[:12]
        self.broker_cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("ascii").rstrip("=")
        if evidence_parent is not None:
            evidence_parent = evidence_parent.resolve()
            if evidence_parent == self.checkout or self.checkout in evidence_parent.parents:
                raise ValueError("Journey evidence must be outside the checkout")
            evidence_parent.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=f"streamt-journey-{mode}-{self.token}-", dir=evidence_parent))
        self.network = f"streamt-journey-{self.token}"
        self.broker = self.network + "-broker"
        self.network_id: str | None = None
        self.broker_id: str | None = None
        self.backend: str | None = None
        self.applications: list[str] = []
        self.evidence: dict[str, object] = {
            "mode": mode, "token": self.token, "runner_image": image, "broker_image": BROKER_IMAGE,
            "package_path": str(Path(streamt.__file__).resolve()), "commands": [], "journeys": [],
            "scope": "create/no-op only; custom applications are declarations, not scheduled workloads",
            "source_hashes_before": self.source_hashes(),
            "python": sys.version, "python_prefix": sys.prefix, "python_base_prefix": sys.base_prefix,
            "kafka_client_version": kafka_client_version,
            "runtime_build_contract": runtime_build_contract(load_runtime_build_inputs()),
        }
        assert self.checkout not in self.root.resolve().parents
        imported = Path(streamt.__file__).resolve()
        assert ((self.checkout / "src") in imported.parents) == (mode == "source")
        self.environment = dict(os.environ)
        endpoint = self.environment.get("DOCKER_HOST")
        if not endpoint:
            endpoint = json.loads(self.run(["docker", "context", "inspect", "--format",
                                            "{{json .Endpoints.docker.Host}}"], env=self.environment).stdout)
        if not isinstance(endpoint, str) or not endpoint.startswith("unix:///"):
            raise RuntimeError("Journey requires an explicitly local Unix-socket Docker daemon")
        if any(character in endpoint for character in "\r\n\x00"):
            raise RuntimeError("Malformed local Docker endpoint")
        self.environment.pop("DOCKER_CONTEXT", None)
        self.environment["DOCKER_HOST"] = endpoint
        self.cli_environment = dict(self.environment)
        if mode == "source":
            self.cli_environment["PYTHONPATH"] = str(self.checkout / "src")
        else:
            self.cli_environment.pop("PYTHONPATH", None)
        self.save()
        print(f"Evidence: {self.root}", flush=True)

    def source_hashes(self) -> dict[str, str]:
        package = Path(streamt.__file__).resolve().parent
        return {str(path.relative_to(package)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted(package.rglob("*.py"))}

    def save(self) -> None:
        (self.root / "evidence.json").write_text(json.dumps(self.evidence, indent=2, sort_keys=True) + "\n")

    @staticmethod
    def run(args: list[str], *, env: dict[str, str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
        return subprocess.run(args, capture_output=True, text=True, env=env, timeout=timeout, check=True)

    def docker(self, *args: str, timeout: int = 30) -> str:
        return self.run(["docker", *args], env=self.environment, timeout=timeout).stdout.strip()

    def command(
        self, directory: Path, *args: str, offline: bool = False, expected_error: str | None = None,
    ) -> dict:
        executable = [sys.executable, *(["-I"] if self.mode == "installed" else [])]
        executable += ["-c", OFFLINE_GUARD] if offline else ["-m", "streamt"]
        command = [*executable, "-o", "json", *args, "--project-dir", str(directory)]
        result = subprocess.run(command, cwd=self.root, env=self.cli_environment,
                                capture_output=True, text=True, timeout=120, check=False)
        records = self.evidence["commands"]
        assert isinstance(records, list)
        index = len(records)
        (self.root / f"command-{index:02d}.stdout").write_text(result.stdout)
        (self.root / f"command-{index:02d}.stderr").write_text(result.stderr)
        records.append({"index": index, "journey": directory.name, "args": list(args),
                        "returncode": result.returncode, "offline_guard": offline,
                        "expected_error": expected_error})
        self.save()
        print(f"{directory.name}: {' '.join(args)} => {result.returncode}", flush=True)
        assert "JOURNEY_OFFLINE_ACCESS_DENIED" not in result.stdout + result.stderr
        assert result.returncode == (1 if expected_error else 0), result.stdout + result.stderr
        payload = json.loads(result.stdout)
        if expected_error:
            assert payload["status"] == "error", payload
            assert expected_error in result.stdout, payload
            return payload
        assert payload["status"] == "ok", payload
        return payload["data"]

    def setup(self) -> None:
        # Read-only prerequisites precede every infrastructure mutation.
        assert self.docker("image", "inspect", "--format", "{{.Id}}", self.image) == self.image
        self.docker("image", "inspect", "--format", "{{.Id}}", BROKER_IMAGE)
        self.network_id = self.docker("network", "create", "--driver", "bridge", "--label",
                                      f"{OWNER_LABEL}={self.token}", self.network)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        self.bootstrap = f"127.0.0.1:{port}"
        settings = {
            "CLUSTER_ID": self.broker_cluster_id,
            "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "EXTERNAL://:9092,INTERNAL://:19092,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"EXTERNAL://{self.bootstrap},INTERNAL://broker:19092",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
            "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false", "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
        }
        args = ["container", "create", "--pull=never", "--name", self.broker,
                "--label", f"{OWNER_LABEL}={self.token}", "--network", self.network_id,
                "--network-alias", "broker", "--publish", f"127.0.0.1:{port}:9092"]
        for key, value in settings.items():
            args.extend(["--env", f"{key}={value}"])
        self.broker_id = self.docker(*args, BROKER_IMAGE)
        self.docker("container", "start", self.broker_id)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = self.run(["docker", "logs", self.broker_id], env=self.environment)
            if "Kafka Server started" in logs.stdout + logs.stderr:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Owned Kafka broker readiness timed out")
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap})
        self.progress = KafkaStreamsProgress({"bootstrap.servers": self.bootstrap}, timeout=10)
        cluster = self.progress.cluster_id()
        assert cluster == self.broker_cluster_id, "Owned broker did not use its unique cluster identity"
        daemon = json.loads(self.docker("info", "--format", "{{json .ID}}"))
        digest = hashlib.sha256(json.dumps([daemon, cluster], separators=(",", ":")).encode()).hexdigest()
        self.backend = f"kafka-streams-docker:v1:{digest}"
        self.evidence["infrastructure"] = {"network": self.network, "network_id": self.network_id,
                                            "broker": self.broker, "broker_id": self.broker_id,
                                            "cluster_id": cluster, "bootstrap": self.bootstrap}
        self.save()

    def topic_config(self, topic: str) -> dict[str, str]:
        futures = self.admin.describe_configs([ConfigResource(ResourceType.TOPIC, topic)], request_timeout=10)
        return {key: value.value for key, value in next(iter(futures.values())).result(10).items()}

    def verify_topic_override_reconciliation(self) -> None:
        """Exercise real SDK config provenance and DELETE on a disposable topic."""
        topic = "config.probe"
        initial = TopicArtifact(name=topic, partitions=1, replication_factor=1,
                                config={"retention.ms": "600000"})
        empty = TopicArtifact(name=topic, partitions=1, replication_factor=1, config={})
        with KafkaDeployer(self.bootstrap) as deployer:
            deployer.create_topic(initial)
            described = self.admin.describe_configs([ConfigResource(ResourceType.TOPIC, topic)])
            entries = next(iter(described.values())).result(10)
            explicit_source = ConfigSource(entries["retention.ms"].source)
            inherited_source = ConfigSource(entries["min.insync.replicas"].source)
            assert explicit_source == ConfigSource.DYNAMIC_TOPIC_CONFIG
            assert inherited_source in {ConfigSource.STATIC_BROKER_CONFIG,
                                        ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG,
                                        ConfigSource.DYNAMIC_BROKER_CONFIG}
            assert entries["min.insync.replicas"].is_default is False
            assert deployer.plan_topic(initial).action == "none"
            removed = deployer.plan_topic(empty)
            assert removed.changes == {"config.retention.ms": {"from": "600000", "to": None}}
            deployer.update_topic(empty, removed.changes)
            assert deployer.plan_topic(empty).action == "none"
        self.evidence["topic_config_reconciliation"] = {
            "topic": topic, "explicit_source": explicit_source.name,
            "inherited_source": inherited_source.name,
            "inherited_is_default": entries["min.insync.replicas"].is_default,
            "removed_override": "retention.ms", "converged": True,
        }
        self.save()

    def verify_contract_rejection(self, directory: Path, config: dict) -> None:
        """A declared custom application's contract gates real plan/apply offline."""
        project_file = directory / "stream_project.yml"
        original = project_file.read_bytes()
        cases = [("missing_name", {"name": "missing", "type": "STRING"}, "consumes column 'missing' which is absent"),
                 ("incompatible_type", {"name": "amount", "type": "BOOLEAN"}, "consumes column 'amount' as 'BOOLEAN'")]
        try:
            for name, column, code in cases:
                broken = json.loads(json.dumps(config))
                consumer = next(exposure for exposure in broken["exposures"] if exposure["name"] == "fraud_app")
                consumer["columns"] = [column]
                project_file.write_text(yaml.safe_dump(broken, sort_keys=False))
                before = {str(path.relative_to(directory)): path.read_bytes()
                          for path in directory.rglob("*") if path.is_file()}
                for command in ("plan", "apply"):
                    self.command(directory, command, offline=True, expected_error=code)
                    after = {str(path.relative_to(directory)): path.read_bytes()
                             for path in directory.rglob("*") if path.is_file()}
                    assert after == before, f"{name}: failed {command} mutated project files"
        finally:
            project_file.write_bytes(original)

    def provider_snapshot(self) -> dict:
        """Observe only infrastructure isolated inside this run's owned broker/backend."""
        metadata = self.admin.list_topics(timeout=10)
        topics = {
            name: {"id": self.progress.topic_id(name), "config": self.topic_config(name),
                   "partitions": sorted(topic.partitions)}
            for name, topic in sorted(metadata.topics.items()) if not name.startswith("__")
        }
        groups = self.admin.list_consumer_groups(request_timeout=10).result(10)
        assert not groups.errors
        containers = self.docker("container", "ls", "--all", "--no-trunc", "--filter",
                                 f"label=io.streamt.backend={self.backend}", "--format", "{{.ID}}").split()
        volumes = self.docker("volume", "ls", "--filter", f"label=io.streamt.backend={self.backend}",
                              "--format", "{{.Name}}").split()
        container_snapshots = []
        for identity in sorted(containers):
            container = json.loads(self.docker("container", "inspect", "--format", "{{json .}}", identity))
            # Docker emits Mounts from a map: array order can change between
            # identical inspections. Preserve every field, sorted by binding.
            container["Mounts"] = sorted(container["Mounts"], key=lambda mount: mount["Destination"])
            container_snapshots.append(container)
        return {
            "topics": topics,
            "groups": sorted(group.group_id for group in groups.valid),
            "containers": container_snapshots,
            "volumes": [json.loads(self.docker("volume", "inspect", "--format", "{{json .}}", name))
                        for name in sorted(volumes)],
        }

    def verify_invalid_runtime_rejection(self, directory: Path, config: dict) -> None:
        """Invalid Java bootstrap syntax must fail before topic/offset/runtime mutation."""
        def changed_paths(before: object, after: object, prefix: str = "") -> list[str]:
            if type(before) is not type(after):
                return [prefix]
            if isinstance(before, dict) and isinstance(after, dict):
                if before.keys() != after.keys():
                    return [prefix + ".keys"]
                return [path for key in before
                        for path in changed_paths(before[key], after[key], f"{prefix}.{key}")]
            if isinstance(before, list) and isinstance(after, list):
                if len(before) != len(after):
                    return [prefix + ".length"]
                return [path for index, (left, right) in enumerate(zip(before, after, strict=True))
                        for path in changed_paths(left, right, f"{prefix}[{index}]")]
            return [] if before == after else [prefix]

        def declared_and_state_files() -> dict[str, bytes]:
            # Apply's established workflow may regenerate local compilation
            # artifacts before rejecting runtime settings. Declarations and all
            # .streamt state/control/private files must remain byte-identical.
            return {str(path.relative_to(directory)): path.read_bytes()
                    for path in directory.rglob("*") if path.is_file()
                    and path.relative_to(directory).parts[0] != "generated"}

        project_file = directory / "stream_project.yml"
        original = project_file.read_bytes()
        broken = json.loads(json.dumps(config))
        broken["runtime"]["kafka"]["bootstrap_servers_internal"] = "https://broker:19092"
        try:
            project_file.write_text(yaml.safe_dump(broken, sort_keys=False))
            files_before = declared_and_state_files()
            providers_before = self.provider_snapshot()
            for command in ("plan", "apply"):
                # These are online commands: read-only clients may already be
                # constructed. The invariant here is no provider/state mutation.
                self.command(directory, command, expected_error="bootstrap")
                differences = changed_paths(providers_before, self.provider_snapshot())
                if differences:
                    self.evidence["runtime_rejection_snapshot_changed"] = {
                        "journey": directory.name, "command": command, "fields": differences,
                    }
                    self.save()
                assert not differences, differences
                assert declared_and_state_files() == files_before
            self.evidence.setdefault("runtime_rejection", {})[directory.name] = {
                "provider_snapshot_unchanged": True, "declarations_and_state_unchanged": True,
                "local_compilation_artifacts_may_be_regenerated": True,
            }
            self.save()
        finally:
            project_file.write_bytes(original)

    def exercise(self, kind: str) -> None:
        directory = self.root / kind
        directory.mkdir()
        raw_topic, output_topic = f"{kind}.raw", f"{kind}.eligible"
        config: dict = {
            "apiVersion": "streamt.dev/v1alpha1", "project": {"name": f"journey-{kind}-{self.token}"},
            "runtime": {
                "kafka": {"bootstrap_servers": self.bootstrap, "bootstrap_servers_internal": "broker:19092"},
                "kafka_streams": {"image": self.image, "network": self.network,
                                  "initial_offset": "earliest", "startup_timeout": 60},
            },
        }
        project_file = directory / "stream_project.yml"
        external_id = external_config = None
        if kind == "existing":
            project_file.write_text(yaml.safe_dump(config, sort_keys=False))
            futures = self.admin.create_topics([NewTopic(raw_topic, 1, 1, config={"retention.ms": "600000"})])
            futures[raw_topic].result(10)
            external_id, external_config = self.progress.topic_id(raw_topic), self.topic_config(raw_topic)
            preview = self.command(directory, "import", "--include", raw_topic, "--no-schemas", "--dry-run")
            assert preview["written"] is False
            imported = self.command(directory, "import", "--include", raw_topic, "--no-schemas")
            assert imported["written"] is True
            parsed = ProjectParser(directory).parse()
            assert len(parsed.sources) == 1
            assert parsed.sources[0].ownership.mode.value == "external"
            source_name = parsed.sources[0].name
            assert imported["resources"][0]["completeness"]["columns"]["status"] == "not_inferred"
            declaration_path = directory / imported["output_file"]
            declaration = yaml.safe_load(declaration_path.read_text())
            declaration["sources"][0]["columns"] = COLUMNS
            declaration_path.write_text(yaml.safe_dump(declaration, sort_keys=False))
            edited_bytes = declaration_path.read_bytes()
            repeated = self.command(directory, "import", "--include", raw_topic, "--no-schemas", "--dry-run")
            assert repeated["written"] is False
            assert declaration_path.read_bytes() == edited_bytes
            dependency = {"source": source_name}
            sql_dependency = '{{ source("' + source_name + '") }}'
            config["models"] = [{
                "name": "eligible_orders", "materialized": "topic", "executor": "kafka_streams",
                "sql": f"SELECT id, amount FROM {sql_dependency} WHERE amount >= 100 AND paid = TRUE",
                "columns": COLUMNS[:2],
                "topic": {"name": output_topic, "partitions": 1, "replication_factor": 1},
            }]
            config["exposures"] = [
                {"name": "checkout_app", "type": "application", "role": "producer", "produces": [dependency],
                 "repo": "https://example.com/team/checkout-app", "tool": "java"},
                {"name": "fraud_app", "type": "application", "role": "consumer",
                 "consumes": [{"ref": "eligible_orders"}],
                 "columns": [{"name": "id", "type": "STRING"}, {"name": "amount", "type": "BIGINT"}],
                 "consumer_group": f"journey-fraud-{kind}-{self.token}", "tool": "python"},
            ]
            project_file.write_text(yaml.safe_dump(config, sort_keys=False))
        else:
            initialized = self.command(
                directory, "init", "--project-name", f"journey-{kind}-{self.token}",
                "--executor", "kafka_streams", "--runner-image", self.image,
                "--kafka", self.bootstrap, "--kafka-internal", "broker:19092",
                "--docker-network", self.network, "--initial-offset", "earliest", offline=True,
            )
            assert initialized["support"] == "create_noop_only"
            assert initialized["metadata_only_applications"] == ["fraud_app"]
            config = yaml.safe_load(project_file.read_text())
            raw_topic = next(model["topic"]["name"] for model in config["models"] if model["name"] == "raw_orders")
            output_topic = next(model["topic"]["name"] for model in config["models"] if model["name"] == "eligible_orders")
            dependency = {"ref": "raw_orders"}
        self.verify_contract_rejection(directory, config)
        self.command(directory, "validate", "--strict", offline=True)
        lineage = self.command(directory, "lineage", offline=True)
        edges = {(edge["from"], edge["to"]) for edge in lineage["edges"]}
        if kind == "existing":
            assert ("checkout_app", next(iter(dependency.values()))) in edges
        assert (next(iter(dependency.values())), "eligible_orders") in edges
        assert ("eligible_orders", "fraud_app") in edges
        self.command(directory, "compile", "--dry-run", offline=True)
        offline = self.command(directory, "plan", "--offline", offline=True)
        expected_creates = 2 if kind == "existing" else 3
        assert offline["creates"] == expected_creates
        project = ProjectParser(directory).parse()
        manifest = Compiler(project).compile(dry_run=True)
        assert not manifest.artifacts["flink_jobs"]
        assert not manifest.artifacts["schemas"]
        assert len(manifest.artifacts["kafka_streams_jobs"]) == 1
        job = manifest.artifacts["kafka_streams_jobs"][0]
        app_id = job["application_id"]
        self.applications.append(app_id)
        (directory / "expected-manifest.json").write_text(json.dumps(manifest.to_dict(), indent=2))
        if len(self.applications) == 1:
            # No fixture-side group describe/join/offset write may warm the
            # broker. The first online CLI plan must prepare its coordinator.
            assert "__consumer_offsets" not in self.admin.list_topics(timeout=10).topics
            self.evidence["cold_coordinator"] = {
                "fixture_group_probe": False,
                "consumer_offsets_topic_absent_before_first_online_plan": True,
                "first_journey": kind,
            }
            self.save()
        planned = self.command(directory, "plan")
        assert planned["creates"] == expected_creates
        assert not planned["is_apply_blocked"]
        self.verify_invalid_runtime_rejection(directory, config)
        applied = self.command(directory, "apply")
        assert applied["committed"] is True
        status = self.command(directory, "status", "--health")
        states = status["kafka_streams_jobs"]
        assert len(states) == 1
        assert states[0]["status"] == "running"
        container_id = states[0]["container_id"]
        if kind == "existing":
            assert self.progress.topic_id(raw_topic) == external_id
            assert self.topic_config(raw_topic) == external_config
            assert all(row["observation"] == "not_requested" and row["exists"] is None
                       for row in status["source_topics"])
        consumer_group = next(exposure["consumer_group"] for exposure in config["exposures"] if exposure["name"] == "fraud_app")
        self.verify_records(kind, app_id, raw_topic, output_topic, consumer_group)
        noop = self.command(directory, "plan")
        assert noop["has_changes"] is False
        self.command(directory, "apply")
        final_status = self.command(directory, "status", "--health")
        assert final_status["kafka_streams_jobs"][0]["container_id"] == container_id
        final_progress = self.progress.observe(app_id, raw_topic, output_topic)
        final_progress.require_resumable()
        assert final_progress.partitions[0].committed == 5
        states_path = directory / ".streamt/state/default.json"
        state = json.loads(states_path.read_text())
        assert len(state["resources"]) == expected_creates
        records = self.evidence["journeys"]
        assert isinstance(records, list)
        records.append({"kind": kind, "project": project.project.name, "application_id": app_id,
                        "container_id": container_id, "input_topic": raw_topic, "output_topic": output_topic,
                        "input_topic_id": final_progress.input_topic_id,
                        "output_topic_id": final_progress.output_topic_id,
                        "committed": final_progress.partitions[0].committed,
                        "expected_outputs": [{"id": "a", "amount": 120}], "state_resource_count": expected_creates,
                        "external_topic_unchanged": kind == "existing"})
        self.save()

    def verify_records(self, kind: str, app_id: str, input_topic: str, output_topic: str, consumer_group: str) -> None:
        delivered: list[object] = []
        producer = Producer({"bootstrap.servers": self.bootstrap})
        rows = [(b"\x00\xff", {"id": "a", "amount": 120, "paid": True}),
                (None, {"id": "b", "amount": 90, "paid": True}),
                (b"filtered", {"id": "c", "amount": 150, "paid": False}),
                (b"null", {"id": "d", "amount": None, "paid": True}), (b"deleted", None)]
        for key, row in rows:
            producer.produce(input_topic, key=key, value=json.dumps(row).encode() if row is not None else None,
                             on_delivery=lambda error, _message: delivered.append(error))
        assert producer.flush(15) == 0
        assert delivered == [None] * len(rows)
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": consumer_group,
                             "enable.auto.commit": False, "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
        consumer.assign([TopicPartition(output_topic, 0, 0)])
        output = []
        deadline = time.monotonic() + 45
        try:
            while time.monotonic() < deadline:
                message = consumer.poll(0.5)
                if message is not None:
                    assert message.error() is None, message.error()
                    output.append({"key_hex": message.key().hex() if message.key() is not None else None,
                                   "value": json.loads(message.value()), "offset": message.offset()})
                current = self.progress.observe(app_id, input_topic, output_topic)
                if current.partitions[0].committed == 5 and output:
                    # Drain any further committed output, including accidental duplicates.
                    while (extra := consumer.poll(0.5)) is not None:
                        assert extra.error() is None
                        output.append({"key_hex": extra.key().hex() if extra.key() is not None else None,
                                       "value": json.loads(extra.value()), "offset": extra.offset()})
                    break
            assert len(output) == 1, output
            assert output[0]["value"] == {"id": "a", "amount": 120}
            assert output[0]["key_hex"] == "00ff"
            (self.root / f"{kind}-records.json").write_text(json.dumps(output, indent=2))
        finally:
            consumer.close()

    def cleanup(self) -> None:
        removed: list[dict[str, object]] = []
        self.admin = None
        self.progress = None
        # Runtime replacement never deletes volumes. This disposable-test teardown
        # is separately authorized and only removes exact run-owned identities.
        for app_id in self.applications:
            ids = self.docker("container", "ls", "--all", "--no-trunc", "--filter", f"name=^/{app_id}$", "--format", "{{.ID}}").split()
            assert len(ids) <= 1
            if ids:
                container = json.loads(self.docker("container", "inspect", "--format", "{{json .}}", ids[0]))
                labels = container["Config"]["Labels"]
                assert container["Name"] == "/" + app_id
                assert labels["io.streamt.application-id"] == app_id
                assert labels["io.streamt.backend"] == self.backend
                if container["State"]["Running"]:
                    self.docker("container", "kill", "--signal=TERM", ids[0])
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        running = self.docker("container", "inspect", "--format", "{{.State.Running}}", ids[0])
                        if running == "false":
                            break
                        time.sleep(0.25)
                    else:
                        raise RuntimeError("Owned runner did not stop; retaining it and its state")
                stopped = json.loads(self.docker("container", "inspect", "--format", "{{json .}}", ids[0]))
                assert stopped["Id"] == ids[0]
                assert stopped["Name"] == "/" + app_id
                assert stopped["Config"]["Labels"]["io.streamt.application-id"] == app_id
                assert stopped["Config"]["Labels"]["io.streamt.backend"] == self.backend
                assert stopped["State"]["Running"] is False
                exit_code = stopped["State"]["ExitCode"]
                assert type(exit_code) is int
                self.docker("container", "rm", ids[0])
                removed.append({"kind": "runner", "id": ids[0], "application_id": app_id,
                                "exit_code_after_stop": exit_code})
            name = app_id + "-state"
            names = self.docker("volume", "ls", "--filter", f"name=^{name}$", "--format", "{{.Name}}").split()
            if names:
                assert names == [name]
                volume = json.loads(self.docker("volume", "inspect", "--format", "{{json .}}", name))
                assert volume["Labels"]["io.streamt.application-id"] == app_id
                assert volume["Labels"]["io.streamt.backend"] == self.backend
                self.docker("volume", "rm", name)
                removed.append({"kind": "runner-state-volume", "name": name})
        if self.broker_id:
            broker = json.loads(self.docker("container", "inspect", "--format", "{{json .}}", self.broker_id))
            assert broker["Name"] == "/" + self.broker
            assert broker["Config"]["Labels"][OWNER_LABEL] == self.token
            volumes = [mount["Name"] for mount in broker["Mounts"] if mount["Type"] == "volume"]
            logs = self.run(["docker", "logs", "--tail", "200", self.broker_id], env=self.environment)
            (self.root / "broker.log").write_text(logs.stdout + logs.stderr)
            self.docker("container", "stop", "--time", "15", self.broker_id)
            self.docker("container", "rm", "-v", self.broker_id)
            remaining = set(self.docker("volume", "ls", "--format", "{{.Name}}").split())
            assert not remaining.intersection(volumes)
            removed.append({"kind": "broker", "id": self.broker_id, "anonymous_volumes_removed": volumes})
        if self.network_id:
            network = json.loads(self.docker("network", "inspect", "--format", "{{json .}}", self.network_id))
            assert network["Name"] == self.network
            assert network["Labels"][OWNER_LABEL] == self.token
            self.docker("network", "rm", self.network_id)
            removed.append({"kind": "network", "id": self.network_id})
        self.evidence["cleanup"] = {"complete": True, "removed": removed}
        self.save()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--mode", choices=["source", "installed"], required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence-dir", type=Path, help="Outside-checkout parent for unique retained evidence directories")
    args = parser.parse_args()
    os.umask(0o077)
    journey = Journey(args.checkout, args.mode, args.image, args.evidence_dir)
    journey.evidence["process_umask"] = "0077"
    try:
        journey.setup()
        journey.verify_topic_override_reconciliation()
        for kind in ("existing", "fresh"):
            journey.exercise(kind)
        journey.evidence["accepted"] = True
    except BaseException as error:
        journey.evidence["accepted"] = False
        journey.evidence["failure"] = str(error)
        raise
    finally:
        journey.evidence["source_hashes_after"] = journey.source_hashes()
        journey.save()
        journey.cleanup()
    print(f"Both {args.mode} CLI create/no-op journeys passed; cleanup verified: {journey.root}", flush=True)


if __name__ == "__main__":
    main()
