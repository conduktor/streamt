#!/usr/bin/env python3
"""Standalone, fail-closed Strimzi 1.2.0 real-cluster acceptance gate.

This test-only program deliberately imports no ``streamt`` module.  It consumes
one caller-supplied wheel, validates the reviewed fixture and lock files, emits
the Strimzi stream twice with the installed executable while network and
deployment imports are denied, and only then creates its disposable kind
topology.

The real lane remains pilot-pending until ``images.lock.json`` contains the
exact runtime image IDs produced by the pinned kind/containerd combination.
``--self-test`` is intentionally local and side-effect free.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import platform as host_platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, TypeVar, cast

import yaml

STRIMZI_RELEASE = "1.2.0"
STRIMZI_COMMIT = "6c7b43c4af0db547c10463ba09d1dfa6f5e156a0"
KIND_VERSION = "v0.33.0"
KIND_ANNOTATED_TAG = "49aeee6b958d818ae881752fe5b09220b39b6f55"
KIND_COMMIT = "407a9675e6d9af1200b5f57f9ca52ec6cdacce74"
KUBERNETES_VERSION = "v1.35.8"
KAFKA_VERSION = "4.3.1"
OPERATOR_SOURCE_SHA256 = "a0b1ae3e375a7da0674eb3894df8aa955bc50abe8190bc9344e30b83bbee4775"
KAFKA_SINGLE_NODE_SOURCE_SHA256 = "2e7739e13dc250ccd00872bc6acf08dbf7fe768b9b76afcbef0dc733ede7b9ea"
KAFKA_EPHEMERAL_SOURCE_SHA256 = "dd12c1e217e7ff348f5be81f9289a6f8c809db5bf4d5bb6b14e24ef7156d4930"
NODE_INDEX_REFERENCE = (
    "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
)
OPERATOR_INDEX_REFERENCE = (
    "quay.io/strimzi/operator@"
    "sha256:77f8fa8121a67561c3418de985783d197f51b8931e9a47f793dc0437dc6bb21f"
)
KAFKA_INDEX_REFERENCE = (
    "quay.io/strimzi/kafka@sha256:e90a1a74af4226f3ca4d1ebef3ab13bdb09754ae17ca4c1444f7fcbb0ca8ea9a"
)
_EXPECTED_KIND_BINARIES = {
    "linux/amd64": (
        "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64",
        "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d",
    ),
    "linux/arm64": (
        "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-arm64",
        "20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2",
    ),
    "darwin/arm64": (
        "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-darwin-arm64",
        "0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f",
    ),
}
_EXPECTED_KUBECTL_BINARIES = {
    "linux/amd64": (
        "https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl",
        "874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf",
    ),
    "linux/arm64": (
        "https://dl.k8s.io/release/v1.35.8/bin/linux/arm64/kubectl",
        "cc749967b62f4422260bc9c0aa7a7c55f45175ae38cb8d95767b5d2b7e04c1fd",
    ),
    "darwin/arm64": (
        "https://dl.k8s.io/release/v1.35.8/bin/darwin/arm64/kubectl",
        "b8be50ae0c6665b646fb009f904a52cad30806deee19ab3b4fe5af2d68bd82eb",
    ),
}
_EXPECTED_IMAGE_PLATFORMS = {
    "node": {
        "linux/amd64": (
            "sha256:0c58cebbb66d7fa5fd497235dfae1e4e722ff84104e24a6f736ce8cd607cbe7c",
            "sha256:194068f84949f79dca8527c1e0578d9cd90f0bcd82a359bdb0d2d5bfe9d61185",
        ),
        "linux/arm64": (
            "sha256:b38a25576c835bfedc9d06368f87ec40863459a4d5dcbdbab2fd5f58ecf97466",
            "sha256:664b3989afaffcd2268ece28d6cf012b27700e6b8e81c3c7641cc167889075f5",
        ),
    },
    "operator": {
        "linux/amd64": (
            "sha256:6df3bf9f92d3d1907aca08ade8c6df6cdacd2e235756afad419ad582ce6a2c4e",
            "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3",
        ),
        "linux/arm64": (
            "sha256:ee8d9fb08ede3778120c33c42c70da16762b531d70e32790b9e2ff932e040927",
            "sha256:693db9e33a50f7cc1cd84cb763ee083c5209412b16f9f198ca013546da44f4f1",
        ),
    },
    "kafka": {
        "linux/amd64": (
            "sha256:63a2dd081b781951d1327626071760525734f4047acfb2d05b1a2878ad4135a5",
            "sha256:d6950337889e76dec427c5ce1ec9c9b8de79e024fc2743c10884027db58d69cd",
        ),
        "linux/arm64": (
            "sha256:b82defb185ed5f91542a678108af5cce08ecc704c98615d43175d113f9f1be4a",
            "sha256:2774fb129b66688c2d958e65a12349a8905e186466a124ccde1190742ba1454c",
        ),
    },
}
OPERATOR_SOURCE_URL = (
    "https://github.com/strimzi/strimzi-kafka-operator/releases/download/"
    "1.2.0/strimzi-cluster-operator-1.2.0.yaml"
)
KAFKA_SINGLE_NODE_SOURCE_URL = (
    "https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/"
    f"{STRIMZI_COMMIT}/examples/kafka/kafka-single-node.yaml"
)
KAFKA_EPHEMERAL_SOURCE_URL = (
    "https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/"
    f"{STRIMZI_COMMIT}/examples/kafka/kafka-ephemeral.yaml"
)

PROCESS_TIMEOUT_SECONDS = 120.0
DOWNLOAD_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 2.0
OPERATOR_TIMEOUT_SECONDS = 300.0
KAFKA_TIMEOUT_SECONDS = 600.0
TOPIC_TIMEOUT_SECONDS = 300.0
REPLAY_TIMEOUT_SECONDS = 300.0
GLOBAL_TIMEOUT_SECONDS = 1_500.0
CLEANUP_RESERVE_SECONDS = 180.0
MAX_PROCESS_OUTPUT_BYTES = 2 * 1024 * 1024
MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024
MAX_INPUT_BYTES = 8 * 1024 * 1024
MAX_EVIDENCE_FILE_BYTES = 2 * 1024 * 1024
MAX_EVIDENCE_TOTAL_BYTES = 12 * 1024 * 1024
MAX_CTR_IMAGE_REFERENCES = 4_096
MAX_CTR_IMAGE_REFERENCE_BYTES = 512
SCAN_FAILURE_MARKER_NAME = "EVIDENCE_REJECTED.txt"
SCAN_FAILURE_MARKER_BYTES = b"Strimzi gate evidence was rejected by the final safety scan.\n"
TOPIC_FIELD_MANAGER = "streamt-strimzi-gate"
PILOT_PENDING_EXIT_CODE = 2
DOCKER_NETWORK_DRIVER = "bridge"
DOCKER_NETWORK_OPTION_SETS = (
    {
        "com.docker.network.bridge.enable_ip_masquerade": "false",
    },
    {
        "com.docker.network.bridge.enable_ip_masquerade": "false",
        "com.docker.network.enable_ipv4": "true",
        "com.docker.network.enable_ipv6": "false",
    },
)

_FIXTURE_SHA256 = {
    "contract.json": "9e9a2aeb47a58a2a09f10d79c61b85c69eaa5e824194327ff6b0adffa2af3917",
    "images.lock.json": "3c583c589ae345da58abec1e0484320c68d5e4d307439d62c811a13a9f7af83c",
    "kafka.yaml": "f33e0b3255aaf1257603a981de79a51b8afc6882e58023841da1c5028770b445",
    "kind.yaml": "3c1649c94e244ede76e3631d2b08656c3ad8a9e23b26a8d98f8174a55a0c4575",
    "operator-contract.json": "7f9343e9aa04f70d91a39a4bf8db0ac6245810d1e2ac3e68309ef2014af19094",
    "stream_project.yml": "fb3b4758b007125b1f7997a2f283d43b30312ac09bec52863c54de21aa1ebdbc",
}
_KIND_NETWORK_WARNINGS = (
    "WARNING: Overriding docker network due to KIND_EXPERIMENTAL_DOCKER_NETWORK",
    "WARNING: Here be dragons! This is not supported currently.",
)

_FIXTURE_DIR = Path(__file__).with_name("strimzi") / STRIMZI_RELEASE
_FIXTURE_NAMES = frozenset(
    {
        "images.lock.json",
        "operator-contract.json",
        "kind.yaml",
        "kafka.yaml",
        "stream_project.yml",
        "contract.json",
    }
)
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_EVIDENCE_NAME = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_IMAGE_TAG = re.compile(r"(?:^|/)[^/@\s]+:[^/@\s]+$")
_STRIMZI_CHILD_REFERENCE = re.compile(r"^quay\.io/strimzi/(?:operator|kafka)@sha256:[0-9a-f]{64}$")
_CTR_IMAGE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/+:@-]{0,511}$")
_FORBIDDEN_IMPORTS = (
    "streamt.deployer",
    "streamt.deployment",
    "streamt.planner",
    "streamt.provider",
    "streamt.providers",
    "streamt.state",
    "streamt.state_backend",
    "streamt.cli.helpers",
    "streamt.integrations.openlineage.transport",
    "confluent_kafka",
)
_OFFLINE_GUARD_SOURCE = r'''"""Fail-closed guard for the installed exporter process."""
import builtins
import http.client
import importlib
import os
import socket
import subprocess
import sys
import urllib.request

_FORBIDDEN_IMPORTS = (
    "streamt.deployer",
    "streamt.deployment",
    "streamt.planner",
    "streamt.provider",
    "streamt.providers",
    "streamt.state",
    "streamt.state_backend",
    "streamt.cli.helpers",
    "streamt.integrations.openlineage.transport",
    "confluent_kafka",
)
_FORBIDDEN_EVENTS = {
    "socket.__new__",
    "socket.getaddrinfo",
    "socket.connect",
    "socket.connect_ex",
    "subprocess.Popen",
    "os.system",
    "os.fork",
    "os.forkpty",
    "os.posix_spawn",
    "os.posix_spawnp",
}

def _deny(*_args, **_kwargs):
    raise RuntimeError("offline Strimzi gate guard denied an operation")

def _forbidden_import(name):
    return isinstance(name, str) and any(
        name == prefix or name.startswith(prefix + ".")
        for prefix in _FORBIDDEN_IMPORTS
    )

def _audit(event, args):
    if event == "import" and args and _forbidden_import(args[0]):
        raise RuntimeError("offline Strimzi gate guard denied an import")
    if event in _FORBIDDEN_EVENTS:
        raise RuntimeError("offline Strimzi gate guard denied an operation")

sys.addaudithook(_audit)
socket.socket = _deny
socket.getaddrinfo = _deny
socket.create_connection = _deny
subprocess.Popen = _deny
subprocess.run = _deny
subprocess.call = _deny
subprocess.check_call = _deny
subprocess.check_output = _deny
urllib.request.urlopen = _deny
http.client.HTTPConnection.connect = _deny
http.client.HTTPSConnection.connect = _deny
for _name in ("fork", "forkpty", "posix_spawn", "posix_spawnp", "system"):
    if hasattr(os, _name):
        setattr(os, _name, _deny)

try:
    import requests.sessions
except ImportError:
    pass
else:
    requests.sessions.Session.__init__ = _deny
    requests.sessions.Session.request = _deny

_original_import = builtins.__import__
_original_import_module = importlib.import_module

def _guarded_import(name, *args, **kwargs):
    if _forbidden_import(name):
        raise RuntimeError("offline Strimzi gate guard denied an import")
    return _original_import(name, *args, **kwargs)

def _guarded_import_module(name, *args, **kwargs):
    if _forbidden_import(name):
        raise RuntimeError("offline Strimzi gate guard denied an import")
    return _original_import_module(name, *args, **kwargs)

builtins.__import__ = _guarded_import
importlib.import_module = _guarded_import_module

_marker = os.environ.get("STREAMT_STRIMZI_GUARD_MARKER")
if _marker is None:
    raise RuntimeError("offline Strimzi gate guard marker was not configured")
with open(_marker, "xb") as _stream:
    _stream.write(b"active\n")
'''
_OPERATOR_NAMESPACE_OBJECTS = frozenset(
    {
        ("RoleBinding", "strimzi-cluster-operator-watched"),
        ("RoleBinding", "strimzi-cluster-operator-entity-operator-delegation"),
        ("RoleBinding", "strimzi-cluster-operator"),
        ("RoleBinding", "strimzi-cluster-operator-leader-election"),
        ("ServiceAccount", "strimzi-cluster-operator"),
        ("Deployment", "strimzi-cluster-operator"),
        ("ConfigMap", "strimzi-cluster-operator"),
    }
)
_SUBJECT_NAMESPACE_OBJECTS = frozenset(
    {
        ("ClusterRoleBinding", "strimzi-cluster-operator"),
        ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-broker-delegation"),
        ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-client-delegation"),
        ("RoleBinding", "strimzi-cluster-operator-watched"),
        ("RoleBinding", "strimzi-cluster-operator-entity-operator-delegation"),
        ("RoleBinding", "strimzi-cluster-operator"),
        ("RoleBinding", "strimzi-cluster-operator-leader-election"),
    }
)
_OPERATOR_ENV_TO_OPERATOR = frozenset(
    {
        "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE",
        "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE",
        "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE",
    }
)
_OPERATOR_ENV_TO_KAFKA = frozenset(
    {
        "STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE",
        "STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE",
    }
)
_KAFKA_IMAGE_MAP_ENV = frozenset(
    {
        "STRIMZI_KAFKA_IMAGES",
        "STRIMZI_KAFKA_CONNECT_IMAGES",
        "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES",
    }
)
_KAFKA_IMAGE_MAP_VERSIONS = ("4.2.0", "4.2.1", "4.3.0", "4.3.1")
_REMOVED_IMAGE_ENV = frozenset(
    {
        "STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE",
        "STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE",
        "STRIMZI_DEFAULT_BUILDAH_IMAGE",
        "STRIMZI_DEFAULT_MAVEN_BUILDER",
    }
)
_ALLOWED_KAFKA_KINDS = frozenset({"Kafka", "KafkaNodePool"})
_FORBIDDEN_KAFKA_KINDS = frozenset(
    {
        "KafkaBridge",
        "KafkaConnect",
        "KafkaConnector",
        "KafkaMirrorMaker",
        "KafkaMirrorMaker2",
        "KafkaRebalance",
        "KafkaUser",
    }
)
_EXPECTED_OPERATOR_INVENTORY = (
    (0, "rbac.authorization.k8s.io/v1", "ClusterRoleBinding", "strimzi-cluster-operator"),
    (1, "rbac.authorization.k8s.io/v1", "RoleBinding", "strimzi-cluster-operator-watched"),
    (2, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkaconnects.kafka.strimzi.io"),
    (3, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "strimzipodsets.core.strimzi.io"),
    (4, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-entity-operator"),
    (5, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-kafka-broker"),
    (6, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-cluster-operator-watched"),
    (
        7,
        "rbac.authorization.k8s.io/v1",
        "RoleBinding",
        "strimzi-cluster-operator-entity-operator-delegation",
    ),
    (8, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkas.kafka.strimzi.io"),
    (9, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-cluster-operator-global"),
    (
        10,
        "rbac.authorization.k8s.io/v1",
        "ClusterRoleBinding",
        "strimzi-cluster-operator-kafka-broker-delegation",
    ),
    (11, "rbac.authorization.k8s.io/v1", "RoleBinding", "strimzi-cluster-operator"),
    (12, "v1", "ServiceAccount", "strimzi-cluster-operator"),
    (13, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkarebalances.kafka.strimzi.io"),
    (14, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkanodepools.kafka.strimzi.io"),
    (15, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkatopics.kafka.strimzi.io"),
    (
        16,
        "rbac.authorization.k8s.io/v1",
        "ClusterRoleBinding",
        "strimzi-cluster-operator-kafka-client-delegation",
    ),
    (17, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-kafka-client"),
    (18, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-cluster-operator-namespaced"),
    (19, "apps/v1", "Deployment", "strimzi-cluster-operator"),
    (
        20,
        "apiextensions.k8s.io/v1",
        "CustomResourceDefinition",
        "kafkamirrormaker2s.kafka.strimzi.io",
    ),
    (21, "v1", "ConfigMap", "strimzi-cluster-operator"),
    (22, "rbac.authorization.k8s.io/v1", "RoleBinding", "strimzi-cluster-operator-leader-election"),
    (23, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkabridges.kafka.strimzi.io"),
    (24, "rbac.authorization.k8s.io/v1", "ClusterRole", "strimzi-cluster-operator-leader-election"),
    (25, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkaconnectors.kafka.strimzi.io"),
    (26, "apiextensions.k8s.io/v1", "CustomResourceDefinition", "kafkausers.kafka.strimzi.io"),
)


class GateError(RuntimeError):
    """A bounded, secret-neutral gate failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class PollPendingError(RuntimeError):
    """A valid observation that has not reached the exact target state."""


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes


@dataclass(frozen=True)
class CtrImportPair:
    date: str
    child_source: str
    child_digest: str
    outer_source: str
    outer_digest: str


@dataclass(frozen=True)
class Download:
    url: str
    sha256: str


@dataclass(frozen=True)
class PlatformBinary:
    url: str
    sha256: str


@dataclass(frozen=True)
class ImagePlatformLock:
    manifest_digest: str
    config_digest: str
    runtime_image_id: str | None


@dataclass(frozen=True)
class ImageLock:
    repository: str
    tag: str
    index_reference: str
    platforms: Mapping[str, ImagePlatformLock]


@dataclass(frozen=True)
class ImagesLock:
    schema_version: int
    strimzi_release: str
    strimzi_commit: str
    kind_version: str
    kind_annotated_tag: str
    kind_commit: str
    kubernetes_version: str
    kafka_version: str
    operator_document_count: int
    operator_source: Download
    kafka_single_node_source: Download
    kafka_ephemeral_source: Download
    kind_binaries: Mapping[str, PlatformBinary]
    kubectl_binaries: Mapping[str, PlatformBinary]
    images: Mapping[str, ImageLock]


@dataclass(frozen=True)
class HostTools:
    platform: str
    kind: PlatformBinary
    kubectl: PlatformBinary


@dataclass(frozen=True)
class PlatformImages:
    platform: str
    node_index_reference: str
    node_manifest_digest: str
    node_config_digest: str
    operator_index_reference: str
    operator_child_reference: str
    operator_manifest_digest: str
    operator_config_digest: str
    operator_runtime_image_id: str | None
    kafka_index_reference: str
    kafka_child_reference: str
    kafka_manifest_digest: str
    kafka_config_digest: str
    kafka_runtime_image_id: str | None

    @property
    def node_child_reference(self) -> str:
        repository = self.node_index_reference.split("@", 1)[0].split(":", 1)[0]
        return f"{repository}@{self.node_manifest_digest}"

    @property
    def pilot_pending(self) -> bool:
        return not self.operator_runtime_image_id or not self.kafka_runtime_image_id


@dataclass(frozen=True)
class OperatorContract:
    schema_version: int
    document_count: int
    empty_document_count: int
    source_sha256: str
    source_url: str
    document_inventory: tuple[tuple[int, str, str, str], ...]
    deployment_image: Mapping[str, Any]
    image_environment: Mapping[str, Any]
    metadata_namespace_targets: tuple[Mapping[str, Any], ...]
    subject_namespace_targets: tuple[Mapping[str, Any], ...]
    forbidden_fixture_resources: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class TopicExpectation:
    metadata_name: str
    topic_name: str
    topic_name_sha256: str
    owner_name: str
    owner_type: str
    partitions: int
    replicas: int
    config: Mapping[str, str]


@dataclass(frozen=True)
class GateContract:
    schema_version: int
    target_release: str
    api_version: str
    kind: str
    namespace: str
    cluster_name: str
    namespace_pattern: str
    cluster_pattern: str
    kind_cluster_pattern: str
    docker_network_pattern: str
    project_name: str
    manifest_checksum: str
    counts: Mapping[str, int]
    warnings: tuple[Mapping[str, Any], ...]
    topics: tuple[TopicExpectation, ...]
    confidential_sentinels: tuple[str, ...]
    environment_sentinels: Mapping[str, str]


@dataclass(frozen=True)
class TopicSnapshot:
    namespace: str
    metadata_name: str
    uid: str
    generation: int
    topic_name: str
    topic_id: str
    spec: Mapping[str, Any]
    broker_description: Mapping[str, Any]
    broker_configs: Mapping[str, str]


@dataclass(frozen=True)
class CleanupTargets:
    prefix: str
    cluster_name: str
    network_name: str
    node_name: str
    runtime_dir: Path


@dataclass(frozen=True)
class CleanupReport:
    cluster_delete_returncode: int
    network_remove_returncode: int
    residue_containers: tuple[str, ...]
    residue_networks: tuple[str, ...]
    node_remove_returncode: int = 0


@dataclass(frozen=True)
class GateArguments:
    wheel: Path | None
    evidence_candidate: Path | None
    evidence_upload: Path | None
    pilot: bool
    self_test: bool


@dataclass(frozen=True)
class FixtureBundle:
    images_lock: ImagesLock
    operator_contract: OperatorContract
    gate_contract: GateContract
    kind_document: Mapping[str, Any]
    kafka_documents: tuple[Mapping[str, Any], ...]
    project_bytes: bytes


T = TypeVar("T")


def _require(condition: object, code: str, message: str) -> None:
    if not condition:
        raise GateError(code, message)


def _mapping(value: Any, *, keys: set[str], code: str) -> dict[str, Any]:
    _require(isinstance(value, dict) and set(value) == keys, code, "Gate input shape changed")
    return cast(dict[str, Any], value)


def _sequence(value: Any, *, code: str) -> list[Any]:
    _require(isinstance(value, list), code, "Gate input sequence changed")
    return cast(list[Any], value)


def _text(value: Any, *, code: str) -> str:
    _require(isinstance(value, str) and bool(value), code, "Gate input text is absent")
    return cast(str, value)


def _integer(value: Any, *, code: str, minimum: int = 0) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value >= minimum,
        code,
        "Gate input integer is invalid",
    )
    return cast(int, value)


def _digest(value: Any, *, code: str, prefixed: bool = False) -> str:
    text = _text(value, code=code)
    pattern = _SHA256 if prefixed else _HEX_SHA256
    _require(pattern.fullmatch(text) is not None, code, "Gate digest is invalid")
    return text


def _read_limited(path: Path, limit: int = MAX_INPUT_BYTES) -> bytes:
    try:
        with path.open("rb") as stream:
            value = stream.read(limit + 1)
    except OSError as error:
        raise GateError("input_unreadable", "A required gate input could not be read") from error
    _require(len(value) <= limit, "input_too_large", "A required gate input is too large")
    return value


def _reject_json_constant(_constant: str) -> None:
    raise ValueError("Non-finite JSON constants are forbidden")


def _load_json(path: Path) -> Any:
    raw = _read_limited(path)
    try:
        return json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeError, ValueError) as error:
        raise GateError("input_invalid", "A required JSON input is invalid") from error


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise GateError("input_invalid", "A required JSON input repeats a key")
        value[key] = item
    return value


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path, limit: int = MAX_INPUT_BYTES) -> str:
    return _sha256_bytes(_read_limited(path, limit))


def _validate_dns_label(value: str, *, code: str) -> str:
    _require(_DNS_LABEL.fullmatch(value) is not None, code, "Gate identity is invalid")
    return value


def _validate_https_url(value: str, *, hosts: frozenset[str]) -> str:
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as error:
        raise GateError("lock_invalid", "Locked download URL is invalid") from error
    _require(
        parsed.scheme == "https"
        and parsed.hostname in hosts
        and parsed.username is None
        and parsed.password is None
        and parsed.query == ""
        and parsed.fragment == ""
        and port in (None, 443)
        and bool(parsed.path),
        "lock_invalid",
        "Locked download URL is outside the reviewed boundary",
    )
    return value


def _platform_binary(value: Any) -> PlatformBinary:
    item = _mapping(value, keys={"url", "sha256"}, code="lock_invalid")
    url = _validate_https_url(
        _text(item["url"], code="lock_invalid"),
        hosts=frozenset({"github.com", "dl.k8s.io"}),
    )
    return PlatformBinary(url, _digest(item["sha256"], code="lock_invalid"))


def _download_lock(value: Any, *, documents: bool = False) -> tuple[Download, int | None]:
    keys = {"url", "sha256", "documents"} if documents else {"url", "sha256"}
    item = _mapping(value, keys=keys, code="lock_invalid")
    url = _validate_https_url(
        _text(item["url"], code="lock_invalid"),
        hosts=frozenset({"github.com", "raw.githubusercontent.com"}),
    )
    count = _integer(item["documents"], code="lock_invalid", minimum=1) if documents else None
    return Download(url, _digest(item["sha256"], code="lock_invalid")), count


def _image_lock(name: str, value: Any) -> ImageLock:
    item = _mapping(
        value,
        keys={"repository", "tag", "index", "platforms"},
        code="lock_invalid",
    )
    repository = _text(item["repository"], code="lock_invalid")
    tag = _text(item["tag"], code="lock_invalid")
    index = _text(item["index"], code="lock_invalid")
    _require(
        index.startswith(f"{repository}:{tag}@sha256:")
        or index.startswith(f"{repository}@sha256:"),
        "lock_invalid",
        "Image index identity changed",
    )
    _digest(index.rsplit("@", 1)[1], code="lock_invalid", prefixed=True)
    platforms_raw = _mapping(
        item["platforms"],
        keys={"linux/amd64", "linux/arm64"},
        code="lock_invalid",
    )
    platforms: dict[str, ImagePlatformLock] = {}
    for platform_name, raw in platforms_raw.items():
        keys = (
            {"manifest", "config"}
            if name == "node"
            else {
                "manifest",
                "config",
                "kubernetes_image_id",
            }
        )
        detail = _mapping(raw, keys=keys, code="lock_invalid")
        runtime = detail.get("kubernetes_image_id")
        _require(
            runtime is None or isinstance(runtime, str),
            "lock_invalid",
            "Runtime image ID is invalid",
        )
        if isinstance(runtime, str):
            _require(
                bool(runtime) and len(runtime) <= 512, "lock_invalid", "Runtime image ID is invalid"
            )
        platforms[platform_name] = ImagePlatformLock(
            _digest(detail["manifest"], code="lock_invalid", prefixed=True),
            _digest(detail["config"], code="lock_invalid", prefixed=True),
            runtime,
        )
    return ImageLock(repository, tag, index, platforms)


def _load_images_lock(path: Path) -> ImagesLock:
    """Load the complete immutable source/tool/image provenance boundary."""
    root = _mapping(
        _load_json(path),
        keys={
            "schema_version",
            "strimzi_release",
            "strimzi_commit",
            "assets",
            "kind",
            "kubectl",
            "images",
        },
        code="lock_invalid",
    )
    _require(root["schema_version"] == 1, "lock_invalid", "Image lock schema changed")
    _require(
        root["strimzi_release"] == STRIMZI_RELEASE and root["strimzi_commit"] == STRIMZI_COMMIT,
        "lock_invalid",
        "Strimzi source identity changed",
    )
    assets = _mapping(
        root["assets"],
        keys={"operator", "kafka_single_node", "kafka_ephemeral"},
        code="lock_invalid",
    )
    operator_source, document_count = _download_lock(assets["operator"], documents=True)
    single_source, _ = _download_lock(assets["kafka_single_node"])
    ephemeral_source, _ = _download_lock(assets["kafka_ephemeral"])
    _require(
        operator_source == Download(OPERATOR_SOURCE_URL, OPERATOR_SOURCE_SHA256)
        and document_count == 27
        and single_source == Download(KAFKA_SINGLE_NODE_SOURCE_URL, KAFKA_SINGLE_NODE_SOURCE_SHA256)
        and ephemeral_source == Download(KAFKA_EPHEMERAL_SOURCE_URL, KAFKA_EPHEMERAL_SOURCE_SHA256),
        "lock_invalid",
        "Locked Strimzi source asset changed",
    )
    assert document_count is not None
    kind = _mapping(
        root["kind"],
        keys={"version", "annotated_tag", "commit", "binaries"},
        code="lock_invalid",
    )
    _require(
        kind["version"] == KIND_VERSION
        and kind["annotated_tag"] == KIND_ANNOTATED_TAG
        and kind["commit"] == KIND_COMMIT,
        "lock_invalid",
        "kind provenance changed",
    )
    kubectl = _mapping(
        root["kubectl"],
        keys={"version", "binaries"},
        code="lock_invalid",
    )
    _require(
        kubectl["version"] == KUBERNETES_VERSION,
        "lock_invalid",
        "kubectl version changed",
    )
    kind_raw = kind["binaries"]
    kubectl_raw = kubectl["binaries"]
    _require(
        isinstance(kind_raw, dict)
        and isinstance(kubectl_raw, dict)
        and set(kind_raw) == set(kubectl_raw)
        and {"linux/amd64", "linux/arm64"} <= set(kind_raw)
        and set(kind_raw) <= {"linux/amd64", "linux/arm64", "darwin/arm64"},
        "lock_invalid",
        "Host tool platform closure changed",
    )
    kind_binaries = {name: _platform_binary(raw) for name, raw in kind_raw.items()}
    kubectl_binaries = {name: _platform_binary(raw) for name, raw in kubectl_raw.items()}
    _require(
        {name: (item.url, item.sha256) for name, item in kind_binaries.items()}
        == _EXPECTED_KIND_BINARIES
        and {name: (item.url, item.sha256) for name, item in kubectl_binaries.items()}
        == _EXPECTED_KUBECTL_BINARIES,
        "lock_invalid",
        "Locked host tool bytes changed",
    )
    images_raw = _mapping(root["images"], keys={"node", "operator", "kafka"}, code="lock_invalid")
    images = {name: _image_lock(name, raw) for name, raw in images_raw.items()}
    _require(
        images["node"].index_reference == NODE_INDEX_REFERENCE
        and (images["node"].repository, images["node"].tag) == ("kindest/node", "v1.35.8")
        and images["operator"].index_reference == OPERATOR_INDEX_REFERENCE
        and (images["operator"].repository, images["operator"].tag)
        == ("quay.io/strimzi/operator", "1.2.0")
        and images["kafka"].index_reference == KAFKA_INDEX_REFERENCE
        and (images["kafka"].repository, images["kafka"].tag)
        == ("quay.io/strimzi/kafka", "1.2.0-kafka-4.3.1")
        and all(
            (
                detail.manifest_digest,
                detail.config_digest,
            )
            == _EXPECTED_IMAGE_PLATFORMS[name][platform_name]
            for name, image in images.items()
            for platform_name, detail in image.platforms.items()
        ),
        "lock_invalid",
        "Image index provenance changed",
    )
    return ImagesLock(
        schema_version=1,
        strimzi_release=STRIMZI_RELEASE,
        strimzi_commit=STRIMZI_COMMIT,
        kind_version=KIND_VERSION,
        kind_annotated_tag=KIND_ANNOTATED_TAG,
        kind_commit=KIND_COMMIT,
        kubernetes_version=KUBERNETES_VERSION,
        kafka_version=KAFKA_VERSION,
        operator_document_count=document_count,
        operator_source=operator_source,
        kafka_single_node_source=single_source,
        kafka_ephemeral_source=ephemeral_source,
        kind_binaries=kind_binaries,
        kubectl_binaries=kubectl_binaries,
        images=images,
    )


def _normalize_machine(machine: str) -> str:
    normalized = machine.casefold()
    if normalized in {"x86_64", "amd64"}:
        return "amd64"
    if normalized in {"aarch64", "arm64"}:
        return "arm64"
    raise GateError("platform_unsupported", "Runner architecture is not locked")


def _select_host_tools(lock: ImagesLock, system: str, machine: str) -> HostTools:
    operating_system = system.casefold()
    if operating_system == "linux":
        os_name = "linux"
    elif operating_system == "darwin":
        os_name = "darwin"
    else:
        raise GateError("platform_unsupported", "Runner operating system is not locked")
    key = f"{os_name}/{_normalize_machine(machine)}"
    _require(
        key in lock.kind_binaries and key in lock.kubectl_binaries,
        "platform_unsupported",
        "Runner tool platform is not locked",
    )
    return HostTools(key, lock.kind_binaries[key], lock.kubectl_binaries[key])


def _select_platform(
    lock: ImagesLock,
    docker_os: str,
    docker_arch: str | None = None,
) -> PlatformImages:
    """Select the Docker server image platform, never the Python host platform."""
    if docker_arch is None:
        # Compatibility for pure contract callers; real orchestration always
        # supplies Docker OSType and Architecture independently.
        docker_arch = docker_os
        docker_os = "linux"
    _require(docker_os.casefold() == "linux", "platform_unsupported", "Docker OS is not locked")
    key = f"linux/{_normalize_machine(docker_arch)}"
    selected = {name: image.platforms.get(key) for name, image in lock.images.items()}
    _require(
        all(value is not None for value in selected.values()),
        "platform_unsupported",
        "Docker image platform is not locked",
    )
    node = selected["node"]
    operator = selected["operator"]
    kafka = selected["kafka"]
    assert node is not None
    assert operator is not None
    assert kafka is not None
    operator_image = lock.images["operator"]
    kafka_image = lock.images["kafka"]
    return PlatformImages(
        platform=key,
        node_index_reference=lock.images["node"].index_reference,
        node_manifest_digest=node.manifest_digest,
        node_config_digest=node.config_digest,
        operator_index_reference=operator_image.index_reference,
        operator_child_reference=f"{operator_image.repository}@{operator.manifest_digest}",
        operator_manifest_digest=operator.manifest_digest,
        operator_config_digest=operator.config_digest,
        operator_runtime_image_id=operator.runtime_image_id,
        kafka_index_reference=kafka_image.index_reference,
        kafka_child_reference=f"{kafka_image.repository}@{kafka.manifest_digest}",
        kafka_manifest_digest=kafka.manifest_digest,
        kafka_config_digest=kafka.config_digest,
        kafka_runtime_image_id=kafka.runtime_image_id,
    )


def _validate_platform_combination(tools: HostTools, images: PlatformImages) -> None:
    _require(
        tools.platform in {"linux/amd64", "linux/arm64", "darwin/arm64"}
        and images.platform in {"linux/amd64", "linux/arm64"}
        and tools.platform.rsplit("/", 1)[1] == images.platform.rsplit("/", 1)[1],
        "platform_mismatch",
        "Host tools and Docker image architecture differ",
    )


def _closed_named_items(
    value: Any,
    *,
    keys: set[str],
    code: str,
) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    names: set[str] = set()
    for raw in _sequence(value, code=code):
        item = _mapping(raw, keys=keys, code=code)
        name = _text(item["name"], code=code)
        _require(name not in names, code, "Gate contract repeats a named item")
        names.add(name)
        result.append(item)
    return tuple(result)


def _load_operator_contract(
    path: Path,
    images_lock: ImagesLock | None = None,
) -> OperatorContract:
    root = _mapping(
        _load_json(path),
        keys={
            "schema_version",
            "document_count",
            "empty_document_count",
            "document_inventory",
            "deployment_image",
            "image_environment",
            "metadata_namespace_targets",
            "subject_namespace_targets",
            "forbidden_fixture_resources",
        },
        code="operator_contract_invalid",
    )
    _require(
        root["schema_version"] == 1, "operator_contract_invalid", "Operator contract schema changed"
    )
    document_count = _integer(root["document_count"], code="operator_contract_invalid", minimum=1)
    empty_count = _integer(root["empty_document_count"], code="operator_contract_invalid")
    _require(
        document_count == 27 and empty_count == 1,
        "operator_contract_invalid",
        "Operator document count changed",
    )
    inventory: list[tuple[int, str, str, str]] = []
    for raw in _sequence(root["document_inventory"], code="operator_contract_invalid"):
        item = _mapping(
            raw,
            keys={"index", "api_version", "kind", "name"},
            code="operator_contract_invalid",
        )
        inventory.append(
            (
                _integer(item["index"], code="operator_contract_invalid"),
                _text(item["api_version"], code="operator_contract_invalid"),
                _text(item["kind"], code="operator_contract_invalid"),
                _text(item["name"], code="operator_contract_invalid"),
            )
        )
    _require(
        len(inventory) == document_count
        and [item[0] for item in inventory] == list(range(document_count))
        and len({item[1:] for item in inventory}) == document_count,
        "operator_contract_invalid",
        "Operator document inventory changed",
    )
    _require(
        tuple(inventory) == _EXPECTED_OPERATOR_INVENTORY,
        "operator_contract_invalid",
        "Pinned operator document inventory changed",
    )
    deployment = _mapping(
        root["deployment_image"],
        keys={
            "document_index",
            "container_name",
            "path",
            "source_value",
            "source_sha256",
            "rewrite_image",
            "image_pull_policy_path",
            "image_pull_policy_source",
            "image_pull_policy_value",
        },
        code="operator_contract_invalid",
    )
    _require(
        deployment
        == {
            "document_index": 19,
            "container_name": "strimzi-cluster-operator",
            "path": "/spec/template/spec/containers/0/image",
            "source_value": "quay.io/strimzi/operator:1.2.0",
            "source_sha256": hashlib.sha256(b"quay.io/strimzi/operator:1.2.0").hexdigest(),
            "rewrite_image": "operator",
            "image_pull_policy_path": "/spec/template/spec/containers/0/imagePullPolicy",
            "image_pull_policy_source": None,
            "image_pull_policy_value": "Never",
        },
        "operator_contract_invalid",
        "Operator deployment image contract changed",
    )
    environment = _mapping(
        root["image_environment"],
        keys={
            "document_index",
            "container_name",
            "path",
            "classified_source_count",
            "rewrite_operator",
            "rewrite_kafka",
            "rewrite_kafka_version_maps",
            "remove",
            "add",
        },
        code="operator_contract_invalid",
    )
    _require(
        environment["document_index"] == 19
        and environment["container_name"] == "strimzi-cluster-operator"
        and environment["path"] == "/spec/template/spec/containers/0/env"
        and environment["classified_source_count"] == 12,
        "operator_contract_invalid",
        "Operator environment location changed",
    )
    operator_items = _closed_named_items(
        environment["rewrite_operator"],
        keys={"name", "source_sha256"},
        code="operator_contract_invalid",
    )
    kafka_items = _closed_named_items(
        environment["rewrite_kafka"],
        keys={"name", "source_sha256"},
        code="operator_contract_invalid",
    )
    map_items = _closed_named_items(
        environment["rewrite_kafka_version_maps"],
        keys={"name", "source_sha256", "version_keys"},
        code="operator_contract_invalid",
    )
    removed_items = _closed_named_items(
        environment["remove"],
        keys={"name", "source_sha256"},
        code="operator_contract_invalid",
    )
    _require(
        {item["name"] for item in operator_items} == _OPERATOR_ENV_TO_OPERATOR
        and {item["name"] for item in kafka_items} == _OPERATOR_ENV_TO_KAFKA
        and {item["name"] for item in map_items} == _KAFKA_IMAGE_MAP_ENV
        and {item["name"] for item in removed_items} == _REMOVED_IMAGE_ENV
        and all(item["version_keys"] == list(_KAFKA_IMAGE_MAP_VERSIONS) for item in map_items),
        "operator_contract_invalid",
        "Operator image environment inventory changed",
    )
    for item in (*operator_items, *kafka_items, *map_items, *removed_items):
        _digest(item["source_sha256"], code="operator_contract_invalid")
    _require(
        environment["add"]
        == {"name": "STRIMZI_IMAGE_PULL_POLICY", "source_count": 0, "value": "Never"},
        "operator_contract_invalid",
        "Operator pull-policy contract changed",
    )

    metadata_targets: list[Mapping[str, Any]] = []
    for raw in _sequence(root["metadata_namespace_targets"], code="operator_contract_invalid"):
        item = _mapping(
            raw,
            keys={"document_index", "kind", "name", "path"},
            code="operator_contract_invalid",
        )
        _require(
            item["path"] == "/metadata/namespace",
            "operator_contract_invalid",
            "Namespace path changed",
        )
        metadata_targets.append(item)
    subject_targets: list[Mapping[str, Any]] = []
    for raw in _sequence(root["subject_namespace_targets"], code="operator_contract_invalid"):
        item = _mapping(
            raw,
            keys={"document_index", "kind", "name", "path", "source_value"},
            code="operator_contract_invalid",
        )
        _require(
            item["path"] == "/subjects/0/namespace" and item["source_value"] == "myproject",
            "operator_contract_invalid",
            "Subject namespace path changed",
        )
        subject_targets.append(item)
    _require(
        {(item["kind"], item["name"]) for item in metadata_targets} == _OPERATOR_NAMESPACE_OBJECTS
        and {(item["kind"], item["name"]) for item in subject_targets} == _SUBJECT_NAMESPACE_OBJECTS
        and len(metadata_targets) == 7
        and len(subject_targets) == 7,
        "operator_contract_invalid",
        "Operator namespace target inventory changed",
    )
    for target in (*metadata_targets, *subject_targets):
        index = _integer(target["document_index"], code="operator_contract_invalid")
        _require(
            inventory[index][2:] == (target["kind"], target["name"]),
            "operator_contract_invalid",
            "Operator namespace target index changed",
        )
    forbidden_raw = _mapping(
        root["forbidden_fixture_resources"],
        keys={"kinds", "kafka_spec_fields", "entity_operator_fields"},
        code="operator_contract_invalid",
    )
    forbidden = {
        key: tuple(
            _text(item, code="operator_contract_invalid")
            for item in _sequence(value, code="operator_contract_invalid")
        )
        for key, value in forbidden_raw.items()
    }
    _require(
        set(forbidden["kinds"]) == _FORBIDDEN_KAFKA_KINDS
        and set(forbidden["kafka_spec_fields"]) == {"cruiseControl", "kafkaExporter"}
        and set(forbidden["entity_operator_fields"]) == {"userOperator"},
        "operator_contract_invalid",
        "Forbidden Strimzi fixture closure changed",
    )
    lock = images_lock or _load_images_lock(path.with_name("images.lock.json"))
    return OperatorContract(
        schema_version=1,
        document_count=document_count,
        empty_document_count=empty_count,
        source_sha256=lock.operator_source.sha256,
        source_url=lock.operator_source.url,
        document_inventory=tuple(inventory),
        deployment_image=deployment,
        image_environment=environment,
        metadata_namespace_targets=tuple(metadata_targets),
        subject_namespace_targets=tuple(subject_targets),
        forbidden_fixture_resources=forbidden,
    )


def _load_gate_contract(path: Path) -> GateContract:
    root = _mapping(
        _load_json(path),
        keys={
            "schema_version",
            "target_release",
            "api_version",
            "kind",
            "placeholders",
            "export",
            "confidential_input_sentinels",
            "environment_sentinels",
        },
        code="contract_invalid",
    )
    _require(
        root["schema_version"] == 1
        and root["target_release"] == STRIMZI_RELEASE
        and root["api_version"] == "kafka.strimzi.io/v1"
        and root["kind"] == "KafkaTopic",
        "contract_invalid",
        "Gate export contract identity changed",
    )
    placeholders = _mapping(
        root["placeholders"],
        keys={"namespace", "kafka_cluster", "kind_cluster_pattern", "docker_network_pattern"},
        code="contract_invalid",
    )
    namespace = _mapping(
        placeholders["namespace"],
        keys={"value", "file", "occurrences", "replacement_pattern"},
        code="contract_invalid",
    )
    cluster = _mapping(
        placeholders["kafka_cluster"],
        keys={"value", "file", "occurrences", "replacement_pattern"},
        code="contract_invalid",
    )
    _require(
        namespace
        == {
            "value": "streamt-strimzi-namespace-placeholder",
            "file": "kafka.yaml",
            "occurrences": 2,
            "replacement_pattern": "^st-[a-z0-9]{12}$",
        }
        and cluster
        == {
            "value": "streamt-strimzi-cluster-placeholder",
            "file": "kafka.yaml",
            "occurrences": 2,
            "replacement_pattern": "^sk-[a-z0-9]{12}$",
        }
        and placeholders["kind_cluster_pattern"] == "^streamt-strimzi-[a-z0-9]{12}-kind$"
        and placeholders["docker_network_pattern"] == "^streamt-strimzi-[a-z0-9]{12}-net$",
        "contract_invalid",
        "Gate placeholder contract changed",
    )
    export = _mapping(
        root["export"],
        keys={
            "project_name",
            "manifest_checksum",
            "topic_count",
            "warnings",
            "counts",
            "topics",
        },
        code="contract_invalid",
    )
    counts = _mapping(
        export["counts"],
        keys={"emitted_topics", "external_topics_omitted", "other_artifacts_omitted"},
        code="contract_invalid",
    )
    _require(
        export["project_name"] == "strimzi-real-gate"
        and _SHA256.fullmatch(_text(export["manifest_checksum"], code="contract_invalid"))
        is not None
        and export["topic_count"] == 2
        and export["warnings"] == []
        and counts
        == {"emitted_topics": 2, "external_topics_omitted": 0, "other_artifacts_omitted": 0},
        "contract_invalid",
        "Gate export surface changed",
    )
    topics: list[TopicExpectation] = []
    for raw in _sequence(export["topics"], code="contract_invalid"):
        item = _mapping(
            raw,
            keys={
                "topic_name",
                "topic_name_sha256",
                "metadata_name",
                "owner_name",
                "owner_type",
                "partitions",
                "replicas",
                "config",
            },
            code="contract_invalid",
        )
        topic_name = _text(item["topic_name"], code="contract_invalid")
        topic_digest = _digest(item["topic_name_sha256"], code="contract_invalid")
        _require(
            hashlib.sha256(topic_name.encode("utf-8")).hexdigest() == topic_digest,
            "contract_invalid",
            "Topic identity digest changed",
        )
        config_raw = item["config"]
        _require(
            isinstance(config_raw, dict)
            and config_raw
            and all(
                isinstance(key, str) and isinstance(value, str) for key, value in config_raw.items()
            ),
            "contract_invalid",
            "Topic config contract changed",
        )
        topics.append(
            TopicExpectation(
                metadata_name=_text(item["metadata_name"], code="contract_invalid"),
                topic_name=topic_name,
                topic_name_sha256=topic_digest,
                owner_name=_text(item["owner_name"], code="contract_invalid"),
                owner_type=_text(item["owner_type"], code="contract_invalid"),
                partitions=_integer(item["partitions"], code="contract_invalid", minimum=1),
                replicas=_integer(item["replicas"], code="contract_invalid", minimum=1),
                config=dict(config_raw),
            )
        )
    _require(
        len(topics) == 2
        and len({topic.topic_name for topic in topics}) == 2
        and len({topic.metadata_name for topic in topics}) == 2,
        "contract_invalid",
        "Topic fixture coverage changed",
    )
    confidential = tuple(
        _text(value, code="contract_invalid")
        for value in _sequence(root["confidential_input_sentinels"], code="contract_invalid")
    )
    environment_raw = root["environment_sentinels"]
    _require(
        isinstance(environment_raw, dict)
        and set(environment_raw) == {"STREAMT_STRIMZI_GATE_STATE_DSN"}
        and all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in environment_raw.items()
        )
        and set(environment_raw.values()) <= set(confidential),
        "contract_invalid",
        "Gate environment sentinel inventory changed",
    )
    return GateContract(
        schema_version=1,
        target_release=STRIMZI_RELEASE,
        api_version="kafka.strimzi.io/v1",
        kind="KafkaTopic",
        namespace=_text(namespace["value"], code="contract_invalid"),
        cluster_name=_text(cluster["value"], code="contract_invalid"),
        namespace_pattern=_text(namespace["replacement_pattern"], code="contract_invalid"),
        cluster_pattern=_text(cluster["replacement_pattern"], code="contract_invalid"),
        kind_cluster_pattern=_text(placeholders["kind_cluster_pattern"], code="contract_invalid"),
        docker_network_pattern=_text(
            placeholders["docker_network_pattern"], code="contract_invalid"
        ),
        project_name=_text(export["project_name"], code="contract_invalid"),
        manifest_checksum=_text(export["manifest_checksum"], code="contract_invalid"),
        counts={key: _integer(value, code="contract_invalid") for key, value in counts.items()},
        warnings=(),
        topics=tuple(topics),
        confidential_sentinels=confidential,
        environment_sentinels=dict(environment_raw),
    )


class _StrictLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False) -> Any:
    loader.flatten_mapping(node)
    pairs = loader.construct_pairs(node, deep=deep)  # type: ignore[no-untyped-call]
    result: dict[Any, Any] = {}
    for key, value in pairs:
        _require(isinstance(key, str), "yaml_invalid", "YAML mapping key is not text")
        _require(key not in result, "yaml_invalid", "YAML mapping repeats a key")
        result[key] = value
    return result


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping,
)


def _load_yaml_documents(
    path: Path,
    expected_sha256: str,
    expected_count: int,
    *,
    expected_empty_count: int = 0,
) -> tuple[dict[str, Any], ...]:
    raw = _read_limited(path)
    _require(
        _sha256_bytes(raw) == expected_sha256,
        "fixture_digest_mismatch",
        "Reviewed YAML fixture digest changed",
    )
    try:
        events = tuple(yaml.parse(raw))
        _require(
            not any(isinstance(event, yaml.events.AliasEvent) for event in events),
            "yaml_invalid",
            "YAML aliases are forbidden in gate inputs",
        )
        loaded = list(yaml.load_all(raw, Loader=_StrictLoader))
    except GateError:
        raise
    except (UnicodeError, yaml.YAMLError) as error:
        raise GateError("yaml_invalid", "Reviewed YAML fixture is invalid") from error
    empty = sum(value is None for value in loaded)
    documents = tuple(value for value in loaded if value is not None)
    _require(
        empty == expected_empty_count
        and len(documents) == expected_count
        and all(isinstance(document, dict) for document in documents),
        "yaml_invalid",
        "Reviewed YAML document closure changed",
    )
    return cast(tuple[dict[str, Any], ...], documents)


def _validate_kind_fixture(document: Mapping[str, Any]) -> None:
    _require(
        document
        == {
            "kind": "Cluster",
            "apiVersion": "kind.x-k8s.io/v1alpha4",
            "networking": {"apiServerAddress": "127.0.0.1"},
            "nodes": [{"role": "control-plane"}],
        },
        "kind_fixture_invalid",
        "kind topology fixture changed",
    )


def _load_fixture_bundle(directory: Path = _FIXTURE_DIR) -> FixtureBundle:
    """Load the exact six-file reviewed fixture closure without checkout imports."""
    try:
        entries = tuple(directory.iterdir())
    except OSError as error:
        raise GateError(
            "fixture_unreadable", "Reviewed fixture directory is unavailable"
        ) from error
    _require(
        directory.is_dir()
        and not directory.is_symlink()
        and {entry.name for entry in entries} == _FIXTURE_NAMES
        and len(entries) == len(_FIXTURE_NAMES),
        "fixture_closure_changed",
        "Reviewed fixture file closure changed",
    )
    for entry in entries:
        try:
            mode = entry.lstat().st_mode
        except OSError as error:
            raise GateError(
                "fixture_unreadable", "Reviewed fixture entry is unavailable"
            ) from error
        _require(
            stat.S_ISREG(mode)
            and not stat.S_ISLNK(mode)
            and _sha256_file(entry) == _FIXTURE_SHA256[entry.name],
            "fixture_digest_mismatch",
            "Reviewed fixture bytes changed",
        )
    lock = _load_images_lock(directory / "images.lock.json")
    operator_contract = _load_operator_contract(
        directory / "operator-contract.json",
        lock,
    )
    gate_contract = _load_gate_contract(directory / "contract.json")
    kind_documents = _load_yaml_documents(
        directory / "kind.yaml",
        _FIXTURE_SHA256["kind.yaml"],
        1,
    )
    _validate_kind_fixture(kind_documents[0])
    kafka_documents = _load_yaml_documents(
        directory / "kafka.yaml",
        _FIXTURE_SHA256["kafka.yaml"],
        2,
    )
    _require(
        tuple(kafka_documents) == _source_kafka_documents(),
        "kafka_fixture_invalid",
        "Kafka fixture source closure changed",
    )
    project_bytes = _read_limited(directory / "stream_project.yml")
    _load_yaml_documents(
        directory / "stream_project.yml",
        _FIXTURE_SHA256["stream_project.yml"],
        1,
    )
    environment_values = set(gate_contract.environment_sentinels.values())
    project_sentinels = set(gate_contract.confidential_sentinels) - environment_values
    _require(
        project_sentinels
        and all(project_bytes.count(value.encode("utf-8")) == 1 for value in project_sentinels)
        and all(value.encode("utf-8") not in project_bytes for value in environment_values),
        "fixture_secret_contract_changed",
        "Fixture confidential sentinel closure changed",
    )
    return FixtureBundle(
        images_lock=lock,
        operator_contract=operator_contract,
        gate_contract=gate_contract,
        kind_document=kind_documents[0],
        kafka_documents=kafka_documents,
        project_bytes=project_bytes,
    )


def _document_identity(document: Mapping[str, Any], index: int) -> tuple[int, str, str, str]:
    metadata = document.get("metadata")
    _require(isinstance(metadata, dict), "operator_invalid", "Operator metadata is invalid")
    metadata = cast(dict[str, Any], metadata)
    return (
        index,
        _text(document.get("apiVersion"), code="operator_invalid"),
        _text(document.get("kind"), code="operator_invalid"),
        _text(metadata.get("name"), code="operator_invalid"),
    )


def _operator_container(documents: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    _require(len(documents) == 27, "operator_invalid", "Operator document count changed")
    document = documents[19]
    try:
        containers = document["spec"]["template"]["spec"]["containers"]
    except (KeyError, TypeError) as error:
        raise GateError("operator_invalid", "Operator Deployment structure changed") from error
    _require(
        isinstance(containers, list)
        and len(containers) == 1
        and isinstance(containers[0], dict)
        and containers[0].get("name") == "strimzi-cluster-operator",
        "operator_invalid",
        "Operator Deployment container closure changed",
    )
    template_spec = document["spec"]["template"]["spec"]
    _require(
        isinstance(template_spec, dict)
        and not template_spec.get("initContainers")
        and not template_spec.get("ephemeralContainers"),
        "operator_invalid",
        "Operator Deployment gained another executable image",
    )
    return cast(dict[str, Any], containers[0])


def _classified_image_environment(
    container: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    environment = container.get("env")
    _require(isinstance(environment, list), "operator_invalid", "Operator environment is invalid")
    environment = cast(list[Any], environment)
    all_items: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    for raw in environment:
        _require(isinstance(raw, dict), "operator_invalid", "Operator environment item is invalid")
        name = raw.get("name")
        _require(
            isinstance(name, str) and name not in by_name,
            "operator_invalid",
            "Operator environment contains a duplicate",
        )
        item = raw
        all_items.append(item)
        by_name[name] = item
    classified = {
        name: item
        for name, item in by_name.items()
        if name.endswith(("_IMAGE", "_IMAGES")) or name == "STRIMZI_DEFAULT_MAVEN_BUILDER"
    }
    return all_items, classified


def _contract_source_hashes(contract: OperatorContract | None) -> dict[str, str]:
    if contract is None:
        return {}
    environment = contract.image_environment
    result: dict[str, str] = {}
    for group in ("rewrite_operator", "rewrite_kafka", "rewrite_kafka_version_maps", "remove"):
        for item in environment[group]:
            result[item["name"]] = item["source_sha256"]
    return result


def _validate_source_kafka_version_map(value: str) -> None:
    _require(
        value.endswith("\n") and "\r" not in value,
        "operator_invalid",
        "Operator Kafka version map formatting changed",
    )
    lines = value[:-1].split("\n")
    pairs: list[tuple[str, str]] = []
    for line in lines:
        _require(
            line.count("=") == 1 and line == line.strip(),
            "operator_invalid",
            "Operator Kafka version map entry is malformed",
        )
        version, image = line.split("=", 1)
        pairs.append((version, image))
    _require(
        tuple(version for version, _image in pairs) == _KAFKA_IMAGE_MAP_VERSIONS
        and tuple(image for _version, image in pairs)
        == tuple(
            f"quay.io/strimzi/kafka:1.2.0-kafka-{version}" for version in _KAFKA_IMAGE_MAP_VERSIONS
        ),
        "operator_invalid",
        "Operator Kafka version map source closure changed",
    )


def _rewritten_kafka_version_map(child_reference: str) -> str:
    _require(
        _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None,
        "operator_invalid",
        "Rewritten Kafka child reference is invalid",
    )
    _require(
        child_reference.startswith("quay.io/strimzi/kafka@"),
        "operator_invalid",
        "Rewritten Kafka child reference is invalid",
    )
    return "".join(f"{version}={child_reference}\n" for version in _KAFKA_IMAGE_MAP_VERSIONS)


def _rewrite_operator_documents(
    documents: Sequence[Mapping[str, Any]],
    namespace: str,
    images: PlatformImages,
    contract: OperatorContract | None = None,
) -> tuple[dict[str, Any], ...]:
    """Perform the exact reviewed image and namespace transformation."""
    _validate_dns_label(namespace, code="operator_invalid")
    source = tuple(documents)
    expected_inventory = contract.document_inventory if contract else _EXPECTED_OPERATOR_INVENTORY
    actual_inventory = tuple(
        _document_identity(document, index) for index, document in enumerate(source)
    )
    _require(
        actual_inventory == expected_inventory,
        "operator_invalid",
        "Operator document inventory changed",
    )
    rewritten = copy.deepcopy(source)
    container = _operator_container(rewritten)
    _require(
        container.get("image") == "quay.io/strimzi/operator:1.2.0"
        and "imagePullPolicy" not in container,
        "operator_invalid",
        "Operator Deployment image source changed",
    )
    all_environment, classified = _classified_image_environment(container)
    expected_source_names = (
        _OPERATOR_ENV_TO_OPERATOR
        | _OPERATOR_ENV_TO_KAFKA
        | _KAFKA_IMAGE_MAP_ENV
        | _REMOVED_IMAGE_ENV
    )
    _require(
        set(classified) == expected_source_names and len(classified) == 12,
        "operator_invalid",
        "Operator image environment closure changed",
    )
    source_hashes = _contract_source_hashes(contract)
    for name, item in classified.items():
        _require(
            set(item) == {"name", "value"} and isinstance(item.get("value"), str),
            "operator_invalid",
            "Operator image environment became indirect",
        )
        if name in source_hashes:
            _require(
                hashlib.sha256(item["value"].encode("utf-8")).hexdigest() == source_hashes[name],
                "operator_invalid",
                "Operator image environment source changed",
            )
        if name in _KAFKA_IMAGE_MAP_ENV:
            _validate_source_kafka_version_map(item["value"])

    container["image"] = images.operator_child_reference
    container["imagePullPolicy"] = "Never"
    retained = [item for item in all_environment if item["name"] not in _REMOVED_IMAGE_ENV]
    for item in retained:
        name = item["name"]
        if name in _OPERATOR_ENV_TO_OPERATOR:
            item.clear()
            item.update({"name": name, "value": images.operator_child_reference})
        elif name in _OPERATOR_ENV_TO_KAFKA:
            item.clear()
            item.update({"name": name, "value": images.kafka_child_reference})
        elif name in _KAFKA_IMAGE_MAP_ENV:
            item.clear()
            item.update(
                {
                    "name": name,
                    "value": _rewritten_kafka_version_map(images.kafka_child_reference),
                }
            )
    retained.append({"name": "STRIMZI_IMAGE_PULL_POLICY", "value": "Never"})
    container["env"] = retained

    metadata_seen: set[tuple[str, str]] = set()
    subject_seen: set[tuple[str, str]] = set()
    for document in rewritten:
        kind = _text(document.get("kind"), code="operator_invalid")
        metadata = document.get("metadata")
        assert isinstance(metadata, dict)
        name = _text(metadata.get("name"), code="operator_invalid")
        identity = (kind, name)
        if identity in _OPERATOR_NAMESPACE_OBJECTS:
            _require(
                "namespace" not in metadata,
                "operator_invalid",
                "Operator namespace source diverged",
            )
            metadata["namespace"] = namespace
            metadata_seen.add(identity)
        subjects = document.get("subjects")
        if identity in _SUBJECT_NAMESPACE_OBJECTS:
            _require(
                isinstance(subjects, list)
                and len(subjects) == 1
                and isinstance(subjects[0], dict)
                and subjects[0].get("kind") == "ServiceAccount"
                and subjects[0].get("name") == "strimzi-cluster-operator"
                and subjects[0].get("namespace") == "myproject",
                "operator_invalid",
                "Operator subject namespace source diverged",
            )
            subject_items = cast(list[dict[str, Any]], subjects)
            subject_items[0]["namespace"] = namespace
            subject_seen.add(identity)
        elif isinstance(subjects, list):
            _require(
                not any(
                    isinstance(item, dict) and item.get("namespace") == "myproject"
                    for item in subjects
                ),
                "operator_invalid",
                "Operator gained an unreviewed subject namespace",
            )
    _require(
        metadata_seen == _OPERATOR_NAMESPACE_OBJECTS and subject_seen == _SUBJECT_NAMESPACE_OBJECTS,
        "operator_invalid",
        "Operator namespace rewrite closure changed",
    )
    _validate_operator_closure(rewritten, namespace, images, contract)
    return cast(tuple[dict[str, Any], ...], rewritten)


def _validate_operator_closure(
    documents: Sequence[Mapping[str, Any]],
    namespace: str,
    images: PlatformImages,
    contract: OperatorContract | None = None,
) -> None:
    expected_inventory = contract.document_inventory if contract else _EXPECTED_OPERATOR_INVENTORY
    actual_inventory = tuple(
        _document_identity(document, index) for index, document in enumerate(documents)
    )
    _require(
        actual_inventory == expected_inventory,
        "operator_invalid",
        "Rewritten operator inventory changed",
    )
    container = _operator_container(documents)
    _require(
        container.get("image") == images.operator_child_reference
        and container.get("imagePullPolicy") == "Never",
        "operator_invalid",
        "Rewritten operator image closure changed",
    )
    _all_environment, classified = _classified_image_environment(container)
    _require(
        set(classified)
        == (_OPERATOR_ENV_TO_OPERATOR | _OPERATOR_ENV_TO_KAFKA | _KAFKA_IMAGE_MAP_ENV),
        "operator_invalid",
        "Rewritten operator environment closure changed",
    )
    for name in _OPERATOR_ENV_TO_OPERATOR:
        _require(
            classified[name] == {"name": name, "value": images.operator_child_reference},
            "operator_invalid",
            "Rewritten operator child identity changed",
        )
    for name in _OPERATOR_ENV_TO_KAFKA:
        _require(
            classified[name] == {"name": name, "value": images.kafka_child_reference},
            "operator_invalid",
            "Rewritten Kafka child identity changed",
        )
    for name in _KAFKA_IMAGE_MAP_ENV:
        _require(
            classified[name]
            == {
                "name": name,
                "value": _rewritten_kafka_version_map(images.kafka_child_reference),
            },
            "operator_invalid",
            "Rewritten Kafka version map changed",
        )
    environment = container.get("env")
    assert isinstance(environment, list)
    policy = [
        item
        for item in environment
        if isinstance(item, dict) and item.get("name") == "STRIMZI_IMAGE_PULL_POLICY"
    ]
    _require(
        policy == [{"name": "STRIMZI_IMAGE_PULL_POLICY", "value": "Never"}],
        "operator_invalid",
        "Operator pull policy environment changed",
    )
    metadata_targets: set[tuple[str, str]] = set()
    subject_targets: set[tuple[str, str]] = set()
    for document in documents:
        metadata = document["metadata"]
        assert isinstance(metadata, dict)
        identity = (str(document["kind"]), str(metadata["name"]))
        if metadata.get("namespace") == namespace:
            metadata_targets.add(identity)
        elif "namespace" in metadata:
            raise GateError("operator_invalid", "Operator object escaped the namespace closure")
        subjects = document.get("subjects", [])
        _require(isinstance(subjects, list), "operator_invalid", "Operator subjects are invalid")
        for subject in subjects:
            _require(isinstance(subject, dict), "operator_invalid", "Operator subject is invalid")
            if "namespace" in subject:
                _require(
                    subject.get("namespace") == namespace,
                    "operator_invalid",
                    "Operator subject escaped namespace closure",
                )
                subject_targets.add(identity)
    _require(
        metadata_targets == _OPERATOR_NAMESPACE_OBJECTS
        and subject_targets == _SUBJECT_NAMESPACE_OBJECTS,
        "operator_invalid",
        "Operator namespace closure changed",
    )


def _docker_network_create_command(docker: Path, network_name: str) -> tuple[str, ...]:
    _validate_dns_label(network_name, code="docker_network_invalid")
    return (
        str(docker),
        "network",
        "create",
        "--driver",
        DOCKER_NETWORK_DRIVER,
        "--opt",
        "com.docker.network.bridge.enable_ip_masquerade=false",
        network_name,
    )


def _validate_docker_network(
    value: Any,
    expected_name: str,
    expected_id: str | None = None,
    *,
    expected_node: str | None = None,
) -> str:
    _validate_dns_label(expected_name, code="docker_network_invalid")
    if expected_node is not None:
        _validate_dns_label(expected_node, code="docker_network_invalid")
    _require(
        isinstance(value, list) and len(value) == 1,
        "docker_network_invalid",
        "Docker network inspection changed",
    )
    network = value[0]
    _require(
        isinstance(network, dict), "docker_network_invalid", "Docker network inspection is invalid"
    )
    identity = network.get("Id")
    containers = network.get("Containers")
    attachments_valid = isinstance(containers, dict)
    if attachments_valid and expected_node is None:
        attachments_valid = not containers
    elif attachments_valid:
        assert expected_node is not None
        attachments_valid = len(containers) == 1 and all(
            isinstance(container_id, str)
            and _HEX_SHA256.fullmatch(container_id) is not None
            and isinstance(attachment, dict)
            and attachment.get("Name") == expected_node
            for container_id, attachment in containers.items()
        )
    _require(
        network.get("Name") == expected_name
        and isinstance(identity, str)
        and _HEX_SHA256.fullmatch(identity) is not None
        and network.get("Driver") == DOCKER_NETWORK_DRIVER
        and network.get("Internal") is False
        and network.get("Options") in DOCKER_NETWORK_OPTION_SETS
        and attachments_valid
        and (expected_id is None or identity == expected_id),
        "docker_network_invalid",
        "Docker isolated bridge identity changed",
    )
    return cast(str, identity)


def _default_route_delete_command(docker: Path, node_name: str) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="node_route_invalid")
    return (str(docker), "exec", node_name, "ip", "route", "del", "default")


def _default_route_inventory_command(
    docker: Path,
    node_name: str,
    *,
    family: int,
) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="node_route_invalid")
    _require(family in {4, 6}, "node_route_invalid", "IP route family is invalid")
    return (
        str(docker),
        "exec",
        node_name,
        "ip",
        f"-{family}",
        "route",
        "show",
        "default",
    )


def _validate_empty_default_routes(ipv4: ProcessResult, ipv6: ProcessResult) -> None:
    _require(
        ipv4.returncode == 0
        and ipv4.stdout == b""
        and ipv4.stderr == b""
        and ipv6.returncode == 0
        and ipv6.stdout == b""
        and ipv6.stderr == b"",
        "node_route_invalid",
        "Node default-route inventory is not empty",
    )


def _validate_docker_nodes(
    value: Any,
    *,
    prefix: str,
    network_name: str,
    network_id: str,
    images: PlatformImages,
) -> tuple[str, ...]:
    cluster_name = f"{prefix}-kind"
    expected_node = f"{cluster_name}-control-plane"
    _validate_dns_label(prefix, code="docker_node_invalid")
    _require(
        isinstance(value, list) and len(value) == 1,
        "docker_node_invalid",
        "Docker node closure changed",
    )
    node = value[0]
    _require(isinstance(node, dict), "docker_node_invalid", "Docker node inspection is invalid")
    try:
        networks = node["NetworkSettings"]["Networks"]
    except (KeyError, TypeError) as error:
        raise GateError(
            "docker_node_invalid", "Docker node network inspection is invalid"
        ) from error
    _require(
        node.get("Name") == f"/{expected_node}"
        and isinstance(node.get("Config"), dict)
        and node["Config"].get("Image") == images.node_child_reference
        and node.get("Image") in {images.node_config_digest, images.node_manifest_digest}
        and isinstance(networks, dict)
        and set(networks) == {network_name}
        and isinstance(networks[network_name], dict)
        and networks[network_name].get("NetworkID") == network_id,
        "docker_node_invalid",
        "Docker node escaped its exact isolated bridge",
    )
    return (expected_node,)


def _validate_node_ports(value: Any) -> int:
    _require(
        isinstance(value, list) and len(value) == 1,
        "docker_port_invalid",
        "Docker node port closure changed",
    )
    node = value[0]
    try:
        ports = node["NetworkSettings"]["Ports"]
    except (KeyError, TypeError) as error:
        raise GateError("docker_port_invalid", "Docker node port inspection is invalid") from error
    _require(
        isinstance(ports, dict) and set(ports) == {"6443/tcp"},
        "docker_port_invalid",
        "Docker node published an unexpected port",
    )
    binding = ports["6443/tcp"]
    _require(
        isinstance(binding, list)
        and len(binding) == 1
        and isinstance(binding[0], dict)
        and set(binding[0]) == {"HostIp", "HostPort"}
        and binding[0]["HostIp"] == "127.0.0.1"
        and isinstance(binding[0]["HostPort"], str)
        and binding[0]["HostPort"].isdigit(),
        "docker_port_invalid",
        "Kubernetes API publication is not exact loopback",
    )
    port = int(binding[0]["HostPort"])
    _require(1 <= port <= 65535, "docker_port_invalid", "Kubernetes API port is invalid")
    return port


def _validate_cri_mappings(value: Any, images: PlatformImages) -> None:
    root = _mapping(value, keys={"images"}, code="cri_invalid")
    entries = _sequence(root["images"], code="cri_invalid")
    selected = {
        images.operator_child_reference: images.operator_config_digest,
        images.kafka_child_reference: images.kafka_config_digest,
    }
    observed: dict[str, str] = {}
    for raw in entries:
        _require(isinstance(raw, dict), "cri_invalid", "CRI image record is invalid")
        image_id = raw.get("id")
        digests = raw.get("repoDigests")
        _require(
            isinstance(image_id, str)
            and isinstance(digests, list)
            and all(isinstance(item, str) for item in digests),
            "cri_invalid",
            "CRI image identity is invalid",
        )
        for reference in selected:
            if reference in digests:
                _require(
                    raw.get("repoTags") == []
                    and _valid_cri_repo_digests(digests, reference)
                    and reference not in observed,
                    "cri_invalid",
                    "CRI image mapping has an unsupported identity closure",
                )
                observed[reference] = image_id
    _require(observed == selected, "cri_invalid", "CRI image-to-config mapping changed")


def _valid_cri_repo_digests(digests: Sequence[str], child_reference: str) -> bool:
    return _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None and list(digests) == [
        child_reference
    ]


def _ctr_images_list_command(docker: Path, node_name: str) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="ctr_inventory_invalid")
    return (
        str(docker),
        "exec",
        node_name,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "list",
        "-q",
    )


def _parse_ctr_image_names(output: bytes) -> frozenset[str]:
    _require(
        all(byte == 0x0A or 0x20 <= byte <= 0x7E for byte in output),
        "ctr_inventory_invalid",
        "ctr image inventory contains a control byte",
    )
    try:
        decoded = output.decode("ascii")
    except UnicodeError as error:
        raise GateError("ctr_inventory_invalid", "ctr image inventory is invalid") from error
    lines = decoded.splitlines()
    _require(
        "\r" not in decoded
        and all(line and line == line.strip() for line in lines)
        and len(lines) == len(set(lines))
        and len(lines) <= MAX_CTR_IMAGE_REFERENCES,
        "ctr_inventory_invalid",
        "ctr image inventory is ambiguous",
    )
    for line in lines:
        _require(
            len(line.encode("ascii")) <= MAX_CTR_IMAGE_REFERENCE_BYTES
            and _CTR_IMAGE_REFERENCE.fullmatch(line) is not None
            and "://" not in line,
            "ctr_inventory_invalid",
            "ctr image inventory contains an unsafe reference",
        )
        if "@" in line:
            _require(
                line.count("@") == 1 and _SHA256.fullmatch(line.rsplit("@", 1)[1]) is not None,
                "ctr_inventory_invalid",
                "ctr image inventory digest suffix is invalid",
            )
    return frozenset(lines)


def _canonical_ctr_image_names(names: Iterable[str]) -> bytes:
    return "".join(f"{name}\n" for name in sorted(names)).encode("ascii")


def _parse_ctr_import_source(source: str) -> tuple[str, str] | None:
    matched = re.fullmatch(
        r"import-(\d{4}-\d{2}-\d{2})@(sha256:[0-9a-f]{64})",
        source,
    )
    if matched is None:
        return None
    try:
        observed = date.fromisoformat(matched.group(1))
    except ValueError:
        return None
    if observed.isoformat() != matched.group(1):
        return None
    return matched.group(1), matched.group(2)


def _valid_ctr_import_source(source: str, child_reference: str) -> bool:
    if _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is None:
        return False
    parsed = _parse_ctr_import_source(source)
    return parsed is not None and parsed[1] == child_reference.rsplit("@", 1)[1]


def _classify_ctr_image_load(
    before: frozenset[str],
    after: frozenset[str],
    child_reference: str,
    config_digest: str,
) -> CtrImportPair | None:
    _require(
        _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None,
        "ctr_inventory_invalid",
        "Selected child reference is invalid",
    )
    _require(
        _SHA256.fullmatch(config_digest) is not None,
        "ctr_inventory_invalid",
        "Selected config digest is invalid",
    )
    child_digest = child_reference.rsplit("@", 1)[1]
    _require(
        child_reference not in before and config_digest not in before and before <= after,
        "ctr_inventory_invalid",
        "ctr image load changed a pre-existing identity",
    )
    added = after - before
    if added == {child_reference, config_digest}:
        return None
    _require(
        config_digest in added and len(added) == 3,
        "ctr_inventory_invalid",
        "ctr image load delta has an unsupported identity closure",
    )
    imports: list[tuple[str, str, str]] = []
    for name in added - {config_digest}:
        parsed = _parse_ctr_import_source(name)
        _require(
            parsed is not None,
            "ctr_inventory_invalid",
            "ctr image load created an unsupported identity",
        )
        assert parsed is not None
        imports.append((name, parsed[0], parsed[1]))
    _require(
        len(imports) == 2
        and imports[0][1] == imports[1][1]
        and len({item[2] for item in imports}) == 2
        and child_digest in {item[2] for item in imports}
        and config_digest not in {item[2] for item in imports},
        "ctr_inventory_invalid",
        "ctr import identities are not the exact paired representation",
    )
    child = next(item for item in imports if item[2] == child_digest)
    outer = next(item for item in imports if item[2] != child_digest)
    return CtrImportPair(
        date=child[1],
        child_source=child[0],
        child_digest=child[2],
        outer_source=outer[0],
        outer_digest=outer[2],
    )


def _ctr_content_get_command(
    docker: Path,
    node_name: str,
    digest: str,
) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="ctr_content_invalid")
    _require(
        _SHA256.fullmatch(digest) is not None,
        "ctr_content_invalid",
        "ctr content digest is invalid",
    )
    return (
        str(docker),
        "exec",
        node_name,
        "ctr",
        "--namespace=k8s.io",
        "content",
        "get",
        digest,
    )


def _validate_ctr_outer_content(
    result: ProcessResult,
    pair: CtrImportPair,
    child_reference: str,
) -> None:
    _require(
        _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None
        and child_reference.rsplit("@", 1)[1] == pair.child_digest,
        "ctr_content_invalid",
        "ctr child identity is invalid",
    )
    _require(
        result.returncode == 0
        and result.stderr == b""
        and f"sha256:{hashlib.sha256(result.stdout).hexdigest()}" == pair.outer_digest,
        "ctr_content_invalid",
        "ctr outer content identity changed",
    )
    root = _mapping(
        _parse_json_bytes(result.stdout, code="ctr_content_invalid"),
        keys={"schemaVersion", "mediaType", "manifests"},
        code="ctr_content_invalid",
    )
    manifests = _sequence(root["manifests"], code="ctr_content_invalid")
    _require(
        root["schemaVersion"] == 2
        and not isinstance(root["schemaVersion"], bool)
        and root["mediaType"] == "application/vnd.oci.image.index.v1+json"
        and len(manifests) == 1,
        "ctr_content_invalid",
        "ctr outer content is not the exact OCI index shape",
    )
    descriptor = _mapping(
        manifests[0],
        keys={"mediaType", "digest", "size", "annotations"},
        code="ctr_content_invalid",
    )
    repository = child_reference.split("@", 1)[0].removeprefix("quay.io/")
    _require(
        descriptor["mediaType"] == "application/vnd.docker.distribution.manifest.v2+json"
        and descriptor["digest"] == pair.child_digest
        and isinstance(descriptor["size"], int)
        and not isinstance(descriptor["size"], bool)
        and descriptor["size"] > 0,
        "ctr_content_invalid",
        "ctr outer descriptor does not identify the selected child",
    )
    _require(
        descriptor["annotations"] == {"containerd.io/distribution.source.quay.io": repository},
        "ctr_content_invalid",
        "ctr outer descriptor annotation changed",
    )


def _ctr_image_tag_command(
    docker: Path,
    node_name: str,
    source: str,
    child_reference: str,
) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="ctr_tag_invalid")
    _require(
        _valid_ctr_import_source(source, child_reference),
        "ctr_tag_invalid",
        "ctr tag source is invalid",
    )
    return (
        str(docker),
        "exec",
        node_name,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "tag",
        source,
        child_reference,
    )


def _validate_ctr_tag_result(result: ProcessResult, child_reference: str) -> None:
    _require(
        _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None
        and result.returncode == 0
        and result.stdout == f"{child_reference}\n".encode()
        and result.stderr == b"",
        "ctr_tag_invalid",
        "ctr tag result changed",
    )


def _ctr_images_remove_command(
    docker: Path,
    node_name: str,
    pair: CtrImportPair,
) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="ctr_remove_invalid")
    _require(
        _parse_ctr_import_source(pair.child_source) == (pair.date, pair.child_digest)
        and _parse_ctr_import_source(pair.outer_source) == (pair.date, pair.outer_digest)
        and pair.child_source != pair.outer_source
        and pair.child_digest != pair.outer_digest,
        "ctr_remove_invalid",
        "ctr removal identities are invalid",
    )
    return (
        str(docker),
        "exec",
        node_name,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "rm",
        pair.child_source,
        pair.outer_source,
    )


def _validate_ctr_remove_result(result: ProcessResult, pair: CtrImportPair) -> None:
    _require(
        result.returncode == 0
        and result.stderr == b""
        and result.stdout == f"{pair.child_source}\n{pair.outer_source}\n".encode(),
        "ctr_remove_invalid",
        "ctr removal result changed",
    )


def _validate_ctr_pre_remove_names(
    post_load: frozenset[str],
    current: frozenset[str],
    child_reference: str,
    pair: CtrImportPair,
) -> None:
    _require(
        current == post_load | {child_reference}
        and pair.child_source in current
        and pair.outer_source in current,
        "ctr_inventory_invalid",
        "ctr identities changed before exact removal",
    )


def _validate_ctr_normalized_names(
    before: frozenset[str],
    after: frozenset[str],
    child_reference: str,
    config_digest: str,
) -> None:
    _require(
        after == before | {child_reference, config_digest},
        "ctr_inventory_invalid",
        "ctr image identities were not normalized exactly",
    )


def _containerd_restart_command(docker: Path, node_name: str) -> tuple[str, ...]:
    _validate_dns_label(node_name, code="containerd_restart_failed")
    return (str(docker), "exec", node_name, "systemctl", "restart", "containerd")


def _validate_containerd_restart_result(result: ProcessResult) -> None:
    _require(
        result.returncode == 0 and result.stdout == b"" and result.stderr == b"",
        "containerd_restart_failed",
        "containerd restart result changed",
    )


def _container_pairs(pod: Mapping[str, Any], category: str) -> tuple[tuple[str, str], ...]:
    spec = pod.get("spec")
    _require(isinstance(spec, dict), "workload_image_invalid", "Pod spec is invalid")
    spec = cast(dict[str, Any], spec)
    raw = spec.get(category, [])
    _require(isinstance(raw, list), "workload_image_invalid", "Pod container list is invalid")
    result: list[tuple[str, str]] = []
    for item in raw:
        _require(
            isinstance(item, dict)
            and isinstance(item.get("name"), str)
            and isinstance(item.get("image"), str),
            "workload_image_invalid",
            "Pod container identity is invalid",
        )
        result.append((item["name"], item["image"]))
    _require(
        len(result) == len({name for name, _image in result}),
        "workload_image_invalid",
        "Pod container name repeats",
    )
    return tuple(result)


def _status_by_name(pod: Mapping[str, Any], category: str) -> dict[str, Mapping[str, Any]]:
    status = pod.get("status")
    _require(isinstance(status, dict), "workload_image_invalid", "Pod status is invalid")
    status = cast(dict[str, Any], status)
    raw = status.get(category, [])
    _require(isinstance(raw, list), "workload_image_invalid", "Pod container status is invalid")
    result: dict[str, Mapping[str, Any]] = {}
    for item in raw:
        _require(
            isinstance(item, dict) and isinstance(item.get("name"), str),
            "workload_image_invalid",
            "Pod container status is invalid",
        )
        name = item["name"]
        _require(name not in result, "workload_image_invalid", "Pod container status repeats")
        result[name] = item
    return result


def _validate_status_image(
    value: Any,
    *,
    child_reference: str,
    config_digest: str,
    code: str,
) -> None:
    _require(
        _STRIMZI_CHILD_REFERENCE.fullmatch(child_reference) is not None
        and _SHA256.fullmatch(config_digest) is not None,
        code,
        "Expected workload image identity is invalid",
    )
    _require(
        isinstance(value, str) and value in {child_reference, config_digest},
        code,
        "Pod runtime status image has an unsupported backend representation",
    )


def _validate_workload_images(pods: Any, images: PlatformImages) -> None:
    _require(
        not images.pilot_pending,
        "pilot_pending",
        "Runtime Kubernetes image IDs await a reviewed pilot",
    )
    root = _mapping(
        pods, keys={"apiVersion", "kind", "metadata", "items"}, code="workload_image_invalid"
    )
    _require(
        root["apiVersion"] == "v1" and root["kind"] == "PodList",
        "workload_image_invalid",
        "Pod list identity changed",
    )
    expected = {
        images.operator_child_reference: (
            images.operator_config_digest,
            images.operator_runtime_image_id,
        ),
        images.kafka_child_reference: (
            images.kafka_config_digest,
            images.kafka_runtime_image_id,
        ),
    }
    counts = dict.fromkeys(expected, 0)
    items = _sequence(root["items"], code="workload_image_invalid")
    _require(bool(items), "workload_image_invalid", "Strimzi workload Pod list is empty")
    for raw_pod in items:
        _require(isinstance(raw_pod, dict), "workload_image_invalid", "Pod item is invalid")
        for spec_key, status_key in (
            ("initContainers", "initContainerStatuses"),
            ("containers", "containerStatuses"),
        ):
            specifications = _container_pairs(raw_pod, spec_key)
            statuses = _status_by_name(raw_pod, status_key)
            _require(
                set(statuses) == {name for name, _image in specifications},
                "workload_image_invalid",
                "Pod status coverage changed",
            )
            for name, reference in specifications:
                _require(
                    reference in expected,
                    "workload_image_invalid",
                    "Pod uses an unlocked image",
                )
                status = statuses[name]
                config_digest, expected_id = expected[reference]
                _validate_status_image(
                    status.get("image"),
                    child_reference=reference,
                    config_digest=config_digest,
                    code="workload_image_invalid",
                )
                _require(
                    status.get("imageID") == expected_id,
                    "workload_image_invalid",
                    "Pod runtime image identity changed",
                )
                counts[reference] += 1
    _require(
        all(count > 0 for count in counts.values()),
        "workload_image_invalid",
        "Strimzi workload image closure is incomplete",
    )


def _validate_workload_exposure(
    pods: Any,
    services: Any,
    *,
    cluster_name: str,
) -> None:
    """Prove selected workloads and their services publish no host endpoint."""
    pod_list = _mapping(
        pods,
        keys={"apiVersion", "kind", "metadata", "items"},
        code="workload_exposure_invalid",
    )
    _require(
        pod_list["apiVersion"] == "v1" and pod_list["kind"] == "PodList",
        "workload_exposure_invalid",
        "Workload Pod list identity changed",
    )
    for pod in _sequence(pod_list["items"], code="workload_exposure_invalid"):
        _require(isinstance(pod, dict), "workload_exposure_invalid", "Workload Pod is invalid")
        spec = pod.get("spec")
        _require(
            isinstance(spec, dict)
            and spec.get("hostNetwork") in (None, False)
            and spec.get("hostPID") in (None, False)
            and spec.get("hostIPC") in (None, False)
            and spec.get("ephemeralContainers") in (None, []),
            "workload_exposure_invalid",
            "Workload Pod entered a host namespace",
        )
        for category in ("initContainers", "containers"):
            containers = spec.get(category, [])
            _require(
                isinstance(containers, list),
                "workload_exposure_invalid",
                "Pod container list is invalid",
            )
            for container in containers:
                _require(
                    isinstance(container, dict),
                    "workload_exposure_invalid",
                    "Pod container is invalid",
                )
                ports = container.get("ports", [])
                _require(
                    isinstance(ports, list),
                    "workload_exposure_invalid",
                    "Container ports are invalid",
                )
                for port in ports:
                    _require(
                        isinstance(port, dict)
                        and port.get("hostPort") in (None, 0)
                        and "hostIP" not in port,
                        "workload_exposure_invalid",
                        "Workload container published a host port",
                    )

    service_items = _kubectl_collection_items(
        services,
        item_kind="Service",
        code="workload_exposure_invalid",
    )
    expected_names = {
        f"{cluster_name}-kafka-bootstrap",
        f"{cluster_name}-kafka-brokers",
    }
    observed_names: set[str] = set()
    for service in service_items:
        metadata = service.get("metadata")
        spec = service.get("spec")
        _require(
            isinstance(metadata, dict) and isinstance(spec, dict),
            "workload_exposure_invalid",
            "Workload Service structure changed",
        )
        metadata = cast(dict[str, Any], metadata)
        spec = cast(dict[str, Any], spec)
        name = _text(metadata.get("name"), code="workload_exposure_invalid")
        _require(
            name not in observed_names, "workload_exposure_invalid", "Workload Service repeats"
        )
        observed_names.add(name)
        ports = spec.get("ports")
        _require(
            spec.get("type") == "ClusterIP"
            and spec.get("externalIPs") in (None, [])
            and "externalName" not in spec
            and "externalTrafficPolicy" not in spec
            and "healthCheckNodePort" not in spec
            and "loadBalancerClass" not in spec
            and isinstance(ports, list)
            and bool(ports),
            "workload_exposure_invalid",
            "Workload Service escaped the internal-only closure",
        )
        assert isinstance(ports, list)
        for port in ports:
            _require(
                isinstance(port, dict) and "nodePort" not in port,
                "workload_exposure_invalid",
                "Workload Service published a node port",
            )
    _require(
        observed_names == expected_names,
        "workload_exposure_invalid",
        "Workload Service inventory changed",
    )


def _validate_pilot_mode(images: PlatformImages, pilot: bool) -> None:
    pending = (
        images.operator_runtime_image_id is None,
        images.kafka_runtime_image_id is None,
    )
    _require(
        pending[0] == pending[1],
        "image_lock_invalid",
        "Runtime image ID lock is partially populated",
    )
    if pilot:
        _require(
            all(pending), "pilot_forbidden", "Pilot mode is forbidden after image IDs are frozen"
        )
    else:
        _require(not any(pending), "pilot_pending", "Runtime image IDs require a reviewed pilot")


def _collect_pilot_image_ids(pods: Any, images: PlatformImages) -> dict[str, Any]:
    _validate_pilot_mode(images, True)
    root = _mapping(pods, keys={"apiVersion", "kind", "metadata", "items"}, code="pilot_invalid")
    _require(
        root["apiVersion"] == "v1" and root["kind"] == "PodList",
        "pilot_invalid",
        "Pilot Pod list identity changed",
    )
    selected = {
        images.operator_child_reference: (
            "operator",
            images.operator_config_digest,
        ),
        images.kafka_child_reference: ("kafka", images.kafka_config_digest),
    }
    observed: dict[str, set[str]] = {reference: set() for reference in selected}
    for raw_pod in _sequence(root["items"], code="pilot_invalid"):
        _require(isinstance(raw_pod, dict), "pilot_invalid", "Pilot Pod item is invalid")
        for spec_key, status_key in (
            ("initContainers", "initContainerStatuses"),
            ("containers", "containerStatuses"),
        ):
            specifications = _container_pairs(raw_pod, spec_key)
            statuses = _status_by_name(raw_pod, status_key)
            _require(
                set(statuses) == {name for name, _image in specifications},
                "pilot_invalid",
                "Pilot Pod status coverage changed",
            )
            for name, reference in specifications:
                _require(reference in selected, "pilot_invalid", "Pilot Pod uses an unlocked image")
                status = statuses[name]
                image_id = status.get("imageID")
                _validate_status_image(
                    status.get("image"),
                    child_reference=reference,
                    config_digest=selected[reference][1],
                    code="pilot_invalid",
                )
                _require(
                    isinstance(image_id, str)
                    and 1 <= len(image_id) <= 512
                    and "@sha256:" in image_id
                    and not any(character.isspace() for character in image_id),
                    "pilot_invalid",
                    "Pilot runtime image identity is invalid",
                )
                image_id = cast(str, image_id)
                digest = image_id.rsplit("@", 1)[1]
                _digest(digest, code="pilot_invalid", prefixed=True)
                observed[reference].add(image_id)
    _require(
        all(len(values) == 1 for values in observed.values()),
        "pilot_invalid",
        "Pilot runtime image IDs are absent or inconsistent",
    )
    records: dict[str, Any] = {}
    for reference, (name, config) in selected.items():
        records[name] = {
            "child_reference": reference,
            "config_digest": config,
            "kubernetes_image_id": next(iter(observed[reference])),
        }
    return {"schema_version": 1, "platform": images.platform, "images": records}


def _source_kafka_documents() -> tuple[dict[str, Any], dict[str, Any]]:
    namespace = "streamt-strimzi-namespace-placeholder"
    cluster = "streamt-strimzi-cluster-placeholder"
    pool = {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": "dual-role",
            "namespace": namespace,
            "labels": {"strimzi.io/cluster": cluster},
        },
        "spec": {
            "replicas": 1,
            "roles": ["controller", "broker"],
            "resources": {
                "requests": {"cpu": "250m", "memory": "512Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"},
            },
            "storage": {
                "type": "jbod",
                "volumes": [{"id": 0, "type": "ephemeral", "kraftMetadata": "shared"}],
            },
        },
    }
    kafka = {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "Kafka",
        "metadata": {"name": cluster, "namespace": namespace},
        "spec": {
            "kafka": {
                "version": KAFKA_VERSION,
                "metadataVersion": "4.3-IV0",
                "listeners": [{"name": "plain", "port": 9092, "type": "internal", "tls": False}],
                "config": {
                    "offsets.topic.replication.factor": 1,
                    "transaction.state.log.replication.factor": 1,
                    "transaction.state.log.min.isr": 1,
                    "default.replication.factor": 1,
                    "min.insync.replicas": 1,
                },
            },
            "entityOperator": {
                "topicOperator": {
                    "resources": {
                        "requests": {"cpu": "100m", "memory": "128Mi"},
                        "limits": {"cpu": "500m", "memory": "512Mi"},
                    }
                }
            },
        },
    }
    return pool, kafka


def _render_kafka_documents(
    documents: Sequence[Mapping[str, Any]],
    namespace: str,
    cluster_name: str,
    images: PlatformImages,
) -> tuple[dict[str, Any], ...]:
    _validate_dns_label(namespace, code="kafka_fixture_invalid")
    _validate_dns_label(cluster_name, code="kafka_fixture_invalid")
    _require(
        tuple(documents) == _source_kafka_documents(),
        "kafka_fixture_invalid",
        "Kafka fixture source closure changed",
    )
    rendered = copy.deepcopy(tuple(documents))
    pool, kafka = rendered
    pool["metadata"]["namespace"] = namespace
    pool["metadata"]["labels"]["strimzi.io/cluster"] = cluster_name
    kafka["metadata"]["namespace"] = namespace
    kafka["metadata"]["name"] = cluster_name
    kafka["spec"]["kafka"]["image"] = images.kafka_child_reference
    _validate_kafka_fixture_closure(rendered, namespace, cluster_name, images)
    return cast(tuple[dict[str, Any], ...], rendered)


def _validate_kafka_fixture_closure(
    documents: Sequence[Mapping[str, Any]],
    namespace: str,
    cluster_name: str,
    images: PlatformImages,
) -> None:
    _require(len(documents) == 2, "kafka_fixture_invalid", "Kafka fixture document count changed")
    pool, kafka = documents
    _require(
        pool.get("apiVersion") == "kafka.strimzi.io/v1"
        and pool.get("kind") == "KafkaNodePool"
        and kafka.get("apiVersion") == "kafka.strimzi.io/v1"
        and kafka.get("kind") == "Kafka",
        "kafka_fixture_invalid",
        "Kafka fixture kinds changed",
    )
    expected_pool, expected_kafka = _source_kafka_documents()
    expected_pool["metadata"]["namespace"] = namespace
    expected_pool["metadata"]["labels"]["strimzi.io/cluster"] = cluster_name
    expected_kafka["metadata"]["namespace"] = namespace
    expected_kafka["metadata"]["name"] = cluster_name
    expected_kafka["spec"]["kafka"]["image"] = images.kafka_child_reference
    _require(
        pool == expected_pool and kafka == expected_kafka,
        "kafka_fixture_invalid",
        "Kafka fixture structural closure changed",
    )
    _require(
        not ({str(document.get("kind")) for document in documents} & _FORBIDDEN_KAFKA_KINDS),
        "kafka_fixture_invalid",
        "Kafka fixture contains a forbidden resource",
    )


def _validate_export_documents(
    value: Sequence[Mapping[str, Any]],
    contract: GateContract,
    namespace: str,
    cluster_name: str,
) -> tuple[dict[str, Any], ...]:
    _require(
        re.fullmatch(contract.namespace_pattern, namespace) is not None
        and re.fullmatch(contract.cluster_pattern, cluster_name) is not None,
        "export_invalid",
        "Generated export identities are outside the fixture contract",
    )
    documents = tuple(copy.deepcopy(value))
    _require(len(documents) == len(contract.topics), "export_invalid", "Export topic count changed")
    for document, topic in zip(documents, contract.topics, strict=True):
        expected = {
            "apiVersion": contract.api_version,
            "kind": contract.kind,
            "metadata": {
                "name": topic.metadata_name,
                "namespace": namespace,
                "labels": {
                    "strimzi.io/cluster": cluster_name,
                    "app.kubernetes.io/managed-by": "streamt",
                },
                "annotations": {
                    "streamt.dev/manifest-checksum": contract.manifest_checksum,
                    "streamt.dev/owner-name": topic.owner_name,
                    "streamt.dev/owner-type": topic.owner_type,
                    "streamt.dev/ownership-mode": "managed",
                    "streamt.dev/project": contract.project_name,
                    "streamt.dev/strimzi-release": contract.target_release,
                },
            },
            "spec": {
                "topicName": topic.topic_name,
                "partitions": topic.partitions,
                "replicas": topic.replicas,
                "config": dict(topic.config),
            },
        }
        _require(document == expected, "export_invalid", "Installed export document changed")
    return cast(tuple[dict[str, Any], ...], documents)


def _topic_apply_command(
    kubectl: Path,
    kubeconfig: Path,
    manifest: Path,
    *,
    dry_run: bool,
) -> tuple[str, ...]:
    command = [
        str(kubectl),
        "--kubeconfig",
        str(kubeconfig),
        "apply",
        "--server-side=true",
        f"--field-manager={TOPIC_FIELD_MANAGER}",
        "--filename",
        str(manifest),
    ]
    if dry_run:
        command.append("--dry-run=server")
    return tuple(command)


def _parse_topic_describe(text: bytes | str, expected_topic: str) -> Mapping[str, Any]:
    try:
        decoded = text.decode("utf-8") if isinstance(text, bytes) else text
    except UnicodeError as error:
        raise GateError("broker_readback_invalid", "Kafka topic description is invalid") from error
    lines = [line for line in decoded.replace("\r", "").splitlines() if line]
    _require(bool(lines), "broker_readback_invalid", "Kafka topic description is empty")

    def fields(line: str) -> dict[str, str]:
        result: dict[str, str] = {}
        parts = line.split("\t")
        if parts and parts[0] == "":
            parts.pop(0)
        _require(
            bool(parts) and all(parts),
            "broker_readback_invalid",
            "Kafka topic description shape changed",
        )
        for part in parts:
            key, separator, item = part.partition(": ")
            _require(
                bool(separator) and key not in result,
                "broker_readback_invalid",
                "Kafka topic description shape changed",
            )
            result[key] = item
        return result

    header = fields(lines[0])
    _require(
        set(header) == {"Topic", "TopicId", "PartitionCount", "ReplicationFactor", "Configs"}
        and header["Topic"] == expected_topic
        and bool(header["TopicId"])
        and header["PartitionCount"].isdigit()
        and header["ReplicationFactor"].isdigit(),
        "broker_readback_invalid",
        "Kafka topic description header changed",
    )
    partitions: list[Mapping[str, Any]] = []
    for line in lines[1:]:
        row = fields(line)
        _require(
            set(row) == {"Topic", "Partition", "Leader", "Replicas", "Isr", "Elr", "LastKnownElr"}
            and row["Topic"] == expected_topic
            and row["Partition"].isdigit()
            and row["Leader"].lstrip("-").isdigit(),
            "broker_readback_invalid",
            "Kafka partition description changed",
        )
        replica_text = tuple(item for item in row["Replicas"].split(",") if item)
        isr_text = tuple(item for item in row["Isr"].split(",") if item)
        _require(
            replica_text
            and all(item.isdigit() for item in (*replica_text, *isr_text))
            and int(row["Leader"]) >= 0,
            "broker_readback_invalid",
            "Kafka replica description changed",
        )
        replica_ids = tuple(int(item) for item in replica_text)
        isr_ids = tuple(int(item) for item in isr_text)
        _require(
            len(replica_ids) == int(header["ReplicationFactor"])
            and int(row["Leader"]) in replica_ids
            and isr_ids == replica_ids
            and row["Elr"] == ""
            and row["LastKnownElr"] == "",
            "broker_readback_invalid",
            "Kafka replica set is not exactly healthy",
        )
        partitions.append(
            {
                "partition": int(row["Partition"]),
                "leader": int(row["Leader"]),
                "replicas": replica_ids,
                "isr": isr_ids,
            }
        )
    partition_count = int(header["PartitionCount"])
    _require(
        len(partitions) == partition_count
        and [row["partition"] for row in partitions] == list(range(partition_count)),
        "broker_readback_invalid",
        "Kafka partition coverage changed",
    )
    return {
        "topic_name": expected_topic,
        "topic_id": header["TopicId"],
        "partition_count": partition_count,
        "replication_factor": int(header["ReplicationFactor"]),
        "partitions": tuple(partitions),
    }


_CONFIG_LINE = re.compile(
    r"^(?P<key>[^=\s]+)=(?P<value>.*?) sensitive=(?P<sensitive>true|false) "
    r"synonyms=(?P<synonyms>\{.*\})$"
)


def _parse_topic_configs(text: bytes | str, expected_topic: str) -> Mapping[str, str]:
    try:
        decoded = text.decode("utf-8") if isinstance(text, bytes) else text
    except UnicodeError as error:
        raise GateError("broker_readback_invalid", "Kafka topic configs are invalid") from error
    lines = [line.strip() for line in decoded.replace("\r", "").splitlines() if line.strip()]
    _require(
        bool(lines) and lines[0] == f"Dynamic configs for topic {expected_topic} are:",
        "broker_readback_invalid",
        "Kafka topic config header changed",
    )
    configs: dict[str, str] = {}
    for line in lines[1:]:
        match = _CONFIG_LINE.fullmatch(line)
        _require(match is not None, "broker_readback_invalid", "Kafka topic config row changed")
        assert match is not None
        key = match.group("key")
        _require(
            key not in configs and match.group("sensitive") == "false",
            "broker_readback_invalid",
            "Kafka topic config identity changed",
        )
        configs[key] = match.group("value")
    _require(bool(configs), "broker_readback_invalid", "Kafka topic configs are empty")
    return configs


def _topic_snapshot(
    topic_object: Mapping[str, Any],
    broker_description: Mapping[str, Any],
    broker_configs: Mapping[str, str],
) -> TopicSnapshot:
    metadata = topic_object.get("metadata")
    spec = topic_object.get("spec")
    status_value = topic_object.get("status")
    _require(
        topic_object.get("apiVersion") == "kafka.strimzi.io/v1"
        and topic_object.get("kind") == "KafkaTopic"
        and isinstance(metadata, dict)
        and isinstance(spec, dict)
        and isinstance(status_value, dict),
        "topic_readback_invalid",
        "KafkaTopic read-back shape changed",
    )
    metadata = cast(dict[str, Any], metadata)
    spec = cast(dict[str, Any], spec)
    status_value = cast(dict[str, Any], status_value)
    generation = metadata.get("generation")
    observed = status_value.get("observedGeneration")
    conditions = status_value.get("conditions")
    _require(
        isinstance(generation, int)
        and not isinstance(generation, bool)
        and generation >= 1
        and observed == generation
        and isinstance(conditions, list)
        and sum(
            isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        )
        == 1,
        "topic_readback_invalid",
        "KafkaTopic is not exactly Ready",
    )
    generation = cast(int, generation)
    topic_name = _text(status_value.get("topicName"), code="topic_readback_invalid")
    topic_id = _text(status_value.get("topicId"), code="topic_readback_invalid")
    _require(
        broker_description.get("topic_name") == topic_name
        and broker_description.get("topic_id") == topic_id,
        "topic_readback_invalid",
        "KafkaTopic and broker identity differ",
    )
    _require(
        spec.get("topicName") == topic_name
        and spec.get("partitions") == broker_description.get("partition_count")
        and spec.get("replicas") == broker_description.get("replication_factor")
        and isinstance(spec.get("config"), dict)
        and spec.get("config") == dict(broker_configs),
        "topic_readback_invalid",
        "KafkaTopic spec and broker state differ",
    )
    return TopicSnapshot(
        namespace=_text(metadata.get("namespace"), code="topic_readback_invalid"),
        metadata_name=_text(metadata.get("name"), code="topic_readback_invalid"),
        uid=_text(metadata.get("uid"), code="topic_readback_invalid"),
        generation=generation,
        topic_name=topic_name,
        topic_id=topic_id,
        spec=copy.deepcopy(spec),
        broker_description=copy.deepcopy(broker_description),
        broker_configs=dict(broker_configs),
    )


def _validate_replay(first: TopicSnapshot, second: TopicSnapshot) -> None:
    _require(first == second, "replay_changed", "Server-side replay changed exact topic state")


def _node_ready(value: Any, *, node_name: str) -> Mapping[str, Any]:
    root = _require_kubernetes_object(value, "v1", "Node", "node_not_ready")
    metadata = root.get("metadata")
    status_value = root.get("status")
    _require(
        isinstance(metadata, dict)
        and metadata.get("name") == node_name
        and isinstance(status_value, dict),
        "node_not_ready",
        "Node readiness identity changed",
    )
    assert isinstance(status_value, dict)
    conditions = status_value.get("conditions")
    _require(
        isinstance(conditions, list) and all(isinstance(item, dict) for item in conditions),
        "node_not_ready",
        "Node readiness shape changed",
    )
    assert isinstance(conditions, list)
    conditions = cast(list[dict[str, Any]], conditions)
    ready = [item for item in conditions if item.get("type") == "Ready"]
    _require(
        len(ready) == 1 and isinstance(ready[0].get("status"), str),
        "node_not_ready",
        "Node Ready condition changed",
    )
    if ready[0]["status"] != "True":
        raise PollPendingError
    return root


def _operator_ready(value: Any, *, namespace: str) -> Mapping[str, Any]:
    root = _require_kubernetes_object(value, "apps/v1", "Deployment", "operator_not_ready")
    metadata = root.get("metadata")
    spec = root.get("spec")
    status_value = root.get("status")
    _require(
        isinstance(metadata, dict)
        and metadata.get("name") == "strimzi-cluster-operator"
        and metadata.get("namespace") == namespace
        and isinstance(metadata.get("generation"), int)
        and not isinstance(metadata.get("generation"), bool)
        and metadata["generation"] >= 1
        and isinstance(spec, dict)
        and isinstance(spec.get("replicas", 1), int)
        and not isinstance(spec.get("replicas", 1), bool)
        and spec.get("replicas", 1) >= 1
        and isinstance(status_value, dict),
        "operator_not_ready",
        "Operator Deployment readiness shape changed",
    )
    metadata = cast(dict[str, Any], metadata)
    spec = cast(dict[str, Any], spec)
    status_value = cast(dict[str, Any], status_value)
    replicas = spec.get("replicas", 1)
    conditions = status_value.get("conditions")
    ready = (
        status_value.get("observedGeneration") == metadata["generation"]
        and status_value.get("availableReplicas") == replicas
        and status_value.get("readyReplicas") == replicas
        and isinstance(conditions, list)
        and sum(
            isinstance(item, dict)
            and item.get("type") == "Available"
            and item.get("status") == "True"
            for item in conditions
        )
        == 1
    )
    if not ready:
        raise PollPendingError
    return root


def _kafka_ready(value: Any, *, namespace: str, cluster_name: str) -> Mapping[str, Any]:
    root = _require_kubernetes_object(value, "kafka.strimzi.io/v1", "Kafka", "kafka_not_ready")
    metadata = root.get("metadata")
    status_value = root.get("status")
    _require(
        isinstance(metadata, dict)
        and metadata.get("name") == cluster_name
        and metadata.get("namespace") == namespace
        and isinstance(metadata.get("generation"), int)
        and not isinstance(metadata.get("generation"), bool)
        and metadata["generation"] >= 1,
        "kafka_not_ready",
        "Kafka readiness identity changed",
    )
    metadata = cast(dict[str, Any], metadata)
    if status_value is None:
        raise PollPendingError
    _require(
        isinstance(status_value, dict),
        "kafka_not_ready",
        "Kafka readiness status shape changed",
    )
    status_value = cast(dict[str, Any], status_value)
    conditions = status_value.get("conditions")
    ready = (
        status_value.get("observedGeneration") == metadata["generation"]
        and isinstance(conditions, list)
        and sum(
            isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        )
        == 1
    )
    if not ready:
        raise PollPendingError
    return root


def _require_kubernetes_object(
    value: Any,
    api_version: str,
    kind: str,
    code: str,
) -> dict[str, Any]:
    _require(isinstance(value, dict), code, "Kubernetes object output is invalid")
    _require(
        value.get("apiVersion") == api_version and value.get("kind") == kind,
        code,
        "Kubernetes object identity changed",
    )
    return cast(dict[str, Any], value)


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (AttributeError, OSError):
        try:
            process.kill()
        except OSError:
            pass


def run_process(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: float = PROCESS_TIMEOUT_SECONDS,
    max_output_bytes: int = MAX_PROCESS_OUTPUT_BYTES,
) -> ProcessResult:
    """Run one absolute executable in a new process group with hard bounds."""
    _require(
        bool(command) and Path(command[0]).is_absolute(),
        "unsafe_executable",
        "Gate subprocess executable must be absolute",
    )
    _require(
        0 < timeout_seconds <= GLOBAL_TIMEOUT_SECONDS
        and 0 < max_output_bytes <= MAX_PROCESS_OUTPUT_BYTES,
        "subprocess_bound_invalid",
        "Gate subprocess bounds are invalid",
    )
    try:
        resolved_cwd = cwd.resolve(strict=True)
    except OSError as error:
        raise GateError("subprocess_failed", "Gate subprocess directory is unavailable") from error
    _require(resolved_cwd.is_dir(), "subprocess_failed", "Gate subprocess directory is invalid")
    try:
        process = subprocess.Popen(
            list(command),
            cwd=resolved_cwd,
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as error:
        raise GateError("subprocess_failed", "A gate subprocess could not start") from error

    stdout = bytearray()
    stderr = bytearray()
    lock = threading.Lock()
    overflow = threading.Event()

    def drain(stream: Any, destination: bytearray) -> None:
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                return
            with lock:
                remaining = max_output_bytes - len(stdout) - len(stderr)
                if remaining > 0:
                    destination.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    overflow.set()
                    _terminate_process(process)
                    return

    assert process.stdout is not None
    assert process.stderr is not None
    readers = (
        threading.Thread(
            target=drain,
            args=(process.stdout, stdout),
            name="strimzi-gate-stdout",
            daemon=True,
        ),
        threading.Thread(
            target=drain,
            args=(process.stderr, stderr),
            name="strimzi-gate-stderr",
            daemon=True,
        ),
    )
    for reader in readers:
        reader.start()
    try:
        returncode = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as error:
        _terminate_process(process)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        for reader in readers:
            reader.join(timeout=5)
        raise GateError("subprocess_timeout", "A gate subprocess exceeded its timeout") from error
    for reader in readers:
        reader.join(timeout=5)
    if any(reader.is_alive() for reader in readers):
        _terminate_process(process)
        raise GateError("subprocess_failed", "A gate subprocess output reader did not stop")
    _require(
        not overflow.is_set(),
        "subprocess_output_limit",
        "A gate subprocess exceeded its output bound",
    )
    return ProcessResult(returncode, bytes(stdout), bytes(stderr))


def _checked(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    timeout_seconds: float = PROCESS_TIMEOUT_SECONDS,
    code: str = "subprocess_failed",
) -> ProcessResult:
    result = run_process(
        command,
        cwd=cwd,
        environment=environment,
        timeout_seconds=timeout_seconds,
    )
    _require(result.returncode == 0, code, "A required gate subprocess failed")
    return result


def poll_exact(
    reader: Callable[[], T],
    *,
    timeout_seconds: float,
    timeout_code: str,
    global_deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Poll only explicitly pending observations against monotonic deadlines."""
    _require(
        0 < timeout_seconds <= GLOBAL_TIMEOUT_SECONDS,
        "poll_bound_invalid",
        "Gate poll bound is invalid",
    )
    deadline = monotonic() + timeout_seconds
    if global_deadline is not None:
        deadline = min(deadline, global_deadline)
    while True:
        try:
            return reader()
        except PollPendingError:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise GateError(timeout_code, "Gate observation did not converge") from None
            sleep(min(POLL_INTERVAL_SECONDS, remaining))


class _LockedRedirect(urllib.request.HTTPRedirectHandler):
    """Allow the one redirect used by immutable GitHub release assets."""

    def __init__(self, expected_source_url: str | None = None) -> None:
        super().__init__()
        self.count = 0
        self.expected_source_url = expected_source_url

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request:
        from urllib.parse import urlsplit

        source = urlsplit(req.full_url)
        target = urlsplit(newurl)
        self.count += 1
        frozen_sources = {
            OPERATOR_SOURCE_URL,
            *(
                url
                for url, _digest_value in _EXPECTED_KIND_BINARIES.values()
                if url.startswith("https://github.com/")
            ),
        }
        expected_source = self.expected_source_url or req.full_url
        _require(
            self.count == 1
            and expected_source in frozen_sources
            and req.full_url == expected_source
            and source.scheme == "https"
            and source.hostname == "github.com"
            and source.username is None
            and source.password is None
            and source.fragment == ""
            and target.scheme == "https"
            and target.hostname == "release-assets.githubusercontent.com"
            and target.username is None
            and target.password is None
            and target.fragment == ""
            and target.port in (None, 443),
            "download_redirect",
            "A locked download redirect escaped its reviewed boundary",
        )
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        _require(redirected is not None, "download_redirect", "A locked redirect was rejected")
        return cast(urllib.request.Request, redirected)


def _download_verified(
    asset: Download | PlatformBinary,
    destination: Path,
    *,
    executable: bool = False,
    opener: Any | None = None,
    deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Download one immutable HTTPS asset directly into a new bounded file."""
    _validate_https_url(
        asset.url,
        hosts=frozenset({"github.com", "raw.githubusercontent.com", "dl.k8s.io"}),
    )
    _require(not destination.exists(), "download_target_exists", "Download target already exists")
    redirect = _LockedRedirect(asset.url)
    client = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), redirect)
    request = urllib.request.Request(asset.url, method="GET", headers={"Accept": "*/*"})
    digest = hashlib.sha256()
    total = 0
    open_timeout = DOWNLOAD_TIMEOUT_SECONDS
    if deadline is not None:
        open_timeout = min(open_timeout, deadline - monotonic())
    _require(open_timeout > 0, "global_timeout", "The Strimzi gate exceeded its global deadline")
    try:
        with client.open(request, timeout=open_timeout) as response:
            status = response.getcode()
            _require(status == 200, "download_failed", "A locked download failed")
            if opener is None:
                from urllib.parse import urlsplit

                initial_host = urlsplit(asset.url).hostname
                final = urlsplit(response.geturl())
                expected_redirects = 1 if initial_host == "github.com" else 0
                expected_host = (
                    "release-assets.githubusercontent.com"
                    if initial_host == "github.com"
                    else initial_host
                )
                _require(
                    redirect.count == expected_redirects
                    and final.scheme == "https"
                    and final.hostname == expected_host
                    and final.username is None
                    and final.password is None
                    and final.fragment == "",
                    "download_redirect",
                    "A locked download final URL changed",
                )
            with destination.open("xb", buffering=0) as stream:
                reader = getattr(response, "read1", response.read)
                while True:
                    if deadline is not None:
                        _require(
                            monotonic() < deadline,
                            "global_timeout",
                            "The Strimzi gate exceeded its global deadline",
                        )
                    chunk = reader(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    _require(
                        total <= MAX_DOWNLOAD_BYTES,
                        "download_too_large",
                        "A locked download exceeded its size bound",
                    )
                    digest.update(chunk)
                    stream.write(chunk)
                os.fsync(stream.fileno())
    except GateError:
        destination.unlink(missing_ok=True)
        raise
    except (OSError, urllib.error.URLError) as error:
        destination.unlink(missing_ok=True)
        raise GateError("download_failed", "A locked download failed") from error
    _require(total > 0, "download_failed", "A locked download was empty")
    if digest.hexdigest() != asset.sha256:
        destination.unlink(missing_ok=True)
        raise GateError("download_digest_mismatch", "A locked download digest changed")
    mode = stat.S_IRUSR | stat.S_IWUSR
    if executable:
        mode |= stat.S_IXUSR
    destination.chmod(mode)


def _minimal_environment(home: Path, tools: Path | None = None) -> dict[str, str]:
    environment: dict[str, str] = {}
    for key in ("LANG", "LC_ALL", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR"):
        if key in os.environ:
            environment[key] = os.environ[key]
    environment.update(
        {
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(home / ".config"),
            "PYTHONNOUSERSITE": "1",
            "PIP_DISABLE_PIP_VERSION_CHECK": "1",
            "NO_COLOR": "1",
        }
    )
    if tools is not None:
        environment["PATH"] = str(tools)
    return environment


def _scan_and_stage_evidence(
    candidate_dir: Path,
    upload_dir: Path,
    confidential: Iterable[str],
) -> tuple[str, ...]:
    """Atomically decide the complete bounded candidate set before copying any."""
    secrets = tuple(item.encode("utf-8") for item in confidential if item)
    _require(
        candidate_dir.is_dir() and not candidate_dir.is_symlink(),
        "evidence_scan_failed",
        "Evidence candidate directory is invalid",
    )
    accepted: list[tuple[str, bytes]] = []
    total = 0
    failure = not secrets or len(secrets) != len(set(secrets))
    _require(
        not upload_dir.exists() and not upload_dir.is_symlink(),
        "evidence_stage_exists",
        "Evidence upload directory must be fresh",
    )
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    candidate_fd = -1
    try:
        candidate_fd = os.open(candidate_dir, directory_flags)
        names = sorted(os.listdir(candidate_fd), key=lambda item: item.encode("utf-8"))
        for name in names:
            if _SAFE_EVIDENCE_NAME.fullmatch(name) is None or name == SCAN_FAILURE_MARKER_NAME:
                failure = True
                break
            file_flags = os.O_RDONLY
            if hasattr(os, "O_NOFOLLOW"):
                file_flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(name, file_flags, dir_fd=candidate_fd)
            except OSError:
                failure = True
                break
            try:
                before = os.fstat(descriptor)
                if not stat.S_ISREG(before.st_mode):
                    failure = True
                    break
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    payload = stream.read(MAX_EVIDENCE_FILE_BYTES + 1)
                after = os.fstat(descriptor)
            except OSError:
                failure = True
                break
            finally:
                os.close(descriptor)
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or len(payload) > MAX_EVIDENCE_FILE_BYTES
                or len(payload) != before.st_size
            ):
                failure = True
                break
            total += len(payload)
            if total > MAX_EVIDENCE_TOTAL_BYTES or any(secret in payload for secret in secrets):
                failure = True
                break
            accepted.append((name, payload))
        if tuple(names) != tuple(
            sorted(os.listdir(candidate_fd), key=lambda item: item.encode("utf-8"))
        ):
            failure = True
    except OSError:
        failure = True
    finally:
        if candidate_fd >= 0:
            os.close(candidate_fd)

    def marker_only() -> None:
        try:
            if upload_dir.exists() and upload_dir.is_dir() and not upload_dir.is_symlink():
                shutil.rmtree(upload_dir)
            elif upload_dir.exists() or upload_dir.is_symlink():
                upload_dir.unlink()
            upload_dir.mkdir(mode=0o700, parents=False)
            marker = upload_dir / SCAN_FAILURE_MARKER_NAME
            marker.write_bytes(SCAN_FAILURE_MARKER_BYTES)
            marker.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            descriptor = os.open(marker, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            descriptor = os.open(upload_dir, directory_flags)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            if upload_dir.exists() and upload_dir.is_dir() and not upload_dir.is_symlink():
                shutil.rmtree(upload_dir)
            raise GateError("evidence_marker_failed", "Evidence rejection marker failed") from error

    if failure:
        marker_only()
        raise GateError("evidence_rejected", "Gate evidence failed its final safety scan")

    staging = upload_dir.with_name(f".{upload_dir.name}.stage-{uuid.uuid4().hex}")
    try:
        staging.mkdir(mode=0o700, parents=False)
        for name, payload in accepted:
            target = staging / name
            target.write_bytes(payload)
            target.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            descriptor = os.open(target, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        descriptor = os.open(staging, directory_flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(staging, upload_dir)
    except (OSError, GateError):
        if staging.exists() and staging.is_dir() and not staging.is_symlink():
            shutil.rmtree(staging)
        marker_only()
        raise GateError("evidence_rejected", "Gate evidence staging failed") from None
    return tuple(name for name, _payload in accepted)


def _derive_cleanup_targets(
    prefix: str,
    cluster_name: str,
    network_name: str,
    runtime_dir: Path,
) -> CleanupTargets:
    _require(
        re.fullmatch(r"streamt-strimzi-[a-z0-9]{12}", prefix) is not None,
        "cleanup_target_invalid",
        "Cleanup prefix is outside the unique gate contract",
    )
    _require(
        cluster_name == f"{prefix}-kind"
        and network_name == f"{prefix}-net"
        and runtime_dir.name == prefix
        and runtime_dir.is_absolute(),
        "cleanup_target_invalid",
        "Cleanup targets are not derived from the unique gate prefix",
    )
    return CleanupTargets(
        prefix=prefix,
        cluster_name=cluster_name,
        network_name=network_name,
        node_name=f"{cluster_name}-control-plane",
        runtime_dir=runtime_dir,
    )


def _output_lines(result: ProcessResult) -> tuple[str, ...]:
    try:
        value = result.stdout.decode("utf-8")
    except UnicodeError as error:
        raise GateError("subprocess_output_invalid", "Gate subprocess output is invalid") from error
    return tuple(line for line in value.splitlines() if line)


def _cleanup(
    targets: CleanupTargets,
    *,
    kind: Path,
    docker: Path,
    cwd: Path,
    environment: Mapping[str, str],
    runner: Callable[..., ProcessResult] = run_process,
    remover: Callable[[Path], None] = shutil.rmtree,
    cleanup_deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> CleanupReport:
    """Delete only exact derived resources, then enumerate prefix residue."""
    checked = _derive_cleanup_targets(
        targets.prefix,
        targets.cluster_name,
        targets.network_name,
        targets.runtime_dir,
    )

    def attempt(command: list[str], nominal_timeout: float) -> ProcessResult:
        timeout = nominal_timeout
        if cleanup_deadline is not None:
            timeout = min(timeout, max(0.0, cleanup_deadline - monotonic()))
        if timeout <= 0:
            return ProcessResult(-1, b"", b"")
        try:
            return runner(
                command,
                cwd=cwd,
                environment=environment,
                timeout_seconds=timeout,
            )
        except Exception:
            return ProcessResult(-1, b"", b"")

    delete = attempt(
        [str(kind), "delete", "cluster", "--name", checked.cluster_name],
        60,
    )
    node_remove = ProcessResult(0, b"", b"")
    if delete.returncode != 0:
        node_remove = attempt(
            [str(docker), "rm", "--force", checked.node_name],
            20,
        )
    network = attempt([str(docker), "network", "rm", checked.network_name], 20)
    containers = attempt(
        [str(docker), "ps", "-a", "--format", "{{.Names}}"],
        10,
    )
    networks = attempt(
        [str(docker), "network", "ls", "--format", "{{.Name}}"],
        10,
    )
    if containers.returncode == 0:
        try:
            residue_containers = tuple(
                item for item in _output_lines(containers) if item.startswith(checked.prefix)
            )
        except GateError:
            residue_containers = (f"{checked.prefix}-container-enumeration-failed",)
    else:
        residue_containers = (f"{checked.prefix}-container-enumeration-failed",)
    if networks.returncode == 0:
        try:
            residue_networks = tuple(
                item for item in _output_lines(networks) if item.startswith(checked.prefix)
            )
        except GateError:
            residue_networks = (f"{checked.prefix}-network-enumeration-failed",)
    else:
        residue_networks = (f"{checked.prefix}-network-enumeration-failed",)
    if checked.runtime_dir.exists():
        try:
            remover(checked.runtime_dir)
        except Exception:
            residue_containers = (
                *residue_containers,
                f"{checked.prefix}-runtime-removal-failed",
            )
    return CleanupReport(
        delete.returncode,
        network.returncode,
        residue_containers,
        residue_networks,
        node_remove.returncode,
    )


def _parse_json_bytes(raw: bytes, *, code: str) -> Any:
    _require(len(raw) <= MAX_PROCESS_OUTPUT_BYTES, code, "JSON subprocess output is too large")
    try:
        return json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except GateError:
        raise
    except (UnicodeError, ValueError) as error:
        raise GateError(code, "JSON subprocess output is invalid") from error


def _parse_yaml_bytes(
    raw: bytes,
    *,
    expected_count: int,
    expected_empty_count: int = 0,
) -> tuple[dict[str, Any], ...]:
    _require(len(raw) <= MAX_INPUT_BYTES, "yaml_invalid", "YAML output is too large")
    try:
        events = tuple(yaml.parse(raw))
        _require(
            not any(isinstance(event, yaml.events.AliasEvent) for event in events),
            "yaml_invalid",
            "YAML aliases are forbidden in gate output",
        )
        loaded = list(yaml.load_all(raw, Loader=_StrictLoader))
    except GateError:
        raise
    except (UnicodeError, yaml.YAMLError) as error:
        raise GateError("yaml_invalid", "YAML output is invalid") from error
    empty_count = sum(item is None for item in loaded)
    documents = tuple(item for item in loaded if item is not None)
    _require(
        empty_count == expected_empty_count
        and len(documents) == expected_count
        and all(isinstance(item, dict) for item in documents),
        "yaml_invalid",
        "YAML output document closure changed",
    )
    return cast(tuple[dict[str, Any], ...], documents)


def _yaml_stream_bytes(documents: Sequence[Mapping[str, Any]]) -> bytes:
    try:
        rendered = yaml.safe_dump_all(
            list(documents),
            allow_unicode=True,
            default_flow_style=False,
            explicit_start=True,
            sort_keys=False,
            width=4096,
        ).encode("utf-8")
    except (UnicodeError, yaml.YAMLError) as error:
        raise GateError("yaml_invalid", "Gate YAML rendering failed") from error
    _require(len(rendered) <= MAX_INPUT_BYTES, "yaml_invalid", "Gate YAML rendering is too large")
    _parse_yaml_bytes(rendered, expected_count=len(documents))
    return rendered


def _write_new_file(path: Path, payload: bytes, *, executable: bool = False) -> None:
    _require(
        not path.exists() and not path.is_symlink() and len(payload) <= MAX_INPUT_BYTES,
        "file_write_invalid",
        "Gate output target or size is invalid",
    )
    mode = stat.S_IRUSR | stat.S_IWUSR
    if executable:
        mode |= stat.S_IXUSR
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(descriptor)
        finally:
            os.close(descriptor)
        path.chmod(mode)
    except OSError as error:
        path.unlink(missing_ok=True)
        raise GateError("file_write_failed", "A gate output could not be written") from error


def _evidence_write(
    candidate_dir: Path,
    name: str,
    payload: bytes,
    confidential: Iterable[str],
) -> None:
    secrets = tuple(value.encode("utf-8") for value in confidential if value)
    _require(
        _SAFE_EVIDENCE_NAME.fullmatch(name) is not None
        and name != SCAN_FAILURE_MARKER_NAME
        and secrets
        and len(secrets) == len(set(secrets))
        and len(payload) <= MAX_EVIDENCE_FILE_BYTES
        and not any(secret in payload for secret in secrets),
        "evidence_candidate_invalid",
        "Evidence candidate bytes are unsafe",
    )
    _write_new_file(candidate_dir / name, payload)


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        raw = (
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise GateError("json_encode_failed", "Gate JSON evidence could not be encoded") from error
    return raw


def _ctr_import_content_evidence(source: str, result: ProcessResult) -> bytes:
    parsed_source = _parse_ctr_import_source(source)
    _require(
        parsed_source is not None and result.returncode == 0 and result.stderr == b"",
        "ctr_content_invalid",
        "ctr import content result changed",
    )
    content = _parse_json_bytes(result.stdout, code="ctr_content_invalid")
    _require(
        isinstance(content, dict),
        "ctr_content_invalid",
        "ctr import content is not a JSON object",
    )
    return _canonical_json_bytes(
        {
            "content": content,
            "raw_content_sha256": f"sha256:{_sha256_bytes(result.stdout)}",
            "source": source,
        }
    )


def _validate_index_manifest(
    value: Any,
    *,
    platform: str,
    expected_child_digest: str,
) -> None:
    _require(isinstance(value, dict), "image_index_invalid", "Image index output is invalid")
    manifests = value.get("manifests")
    _require(
        isinstance(manifests, list), "image_index_invalid", "Image index manifest list is invalid"
    )
    operating_system, architecture = platform.split("/", 1)
    selected: list[str] = []
    seen: set[tuple[str, str, str]] = set()
    for item in manifests:
        _require(isinstance(item, dict), "image_index_invalid", "Image index member is invalid")
        digest = item.get("digest")
        member_platform = item.get("platform")
        _require(
            isinstance(digest, str)
            and _SHA256.fullmatch(digest) is not None
            and isinstance(member_platform, dict)
            and isinstance(member_platform.get("os"), str)
            and isinstance(member_platform.get("architecture"), str),
            "image_index_invalid",
            "Image index member identity is invalid",
        )
        identity = (
            member_platform["os"],
            member_platform["architecture"],
            str(member_platform.get("variant", "")),
        )
        _require(identity not in seen, "image_index_invalid", "Image index repeats a platform")
        seen.add(identity)
        expected_variants = {"", "v8"} if architecture == "arm64" else {""}
        if identity[:2] == (operating_system, architecture) and identity[2] in expected_variants:
            selected.append(digest)
    _require(
        selected == [expected_child_digest],
        "image_index_invalid",
        "Image index-to-child relationship changed",
    )


def _validate_child_manifest(value: Any, *, expected_config_digest: str) -> None:
    _require(isinstance(value, dict), "child_manifest_invalid", "Child manifest is invalid")
    config = value.get("config")
    layers = value.get("layers")
    _require(
        set(value) == {"schemaVersion", "mediaType", "config", "layers"}
        and value.get("schemaVersion") == 2
        and value.get("mediaType")
        in {
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
        }
        and isinstance(config, dict)
        and config.get("digest") == expected_config_digest
        and isinstance(layers, list)
        and all(
            isinstance(layer, dict)
            and isinstance(layer.get("digest"), str)
            and _SHA256.fullmatch(layer["digest"]) is not None
            for layer in layers
        ),
        "child_manifest_invalid",
        "Child manifest-to-config identity changed",
    )


def _validate_local_image(
    value: Any,
    *,
    child_reference: str,
    child_digest: str,
    config_digest: str,
    platform: str,
) -> str:
    _require(
        isinstance(value, list) and len(value) == 1,
        "local_image_invalid",
        "Local image inspection changed",
    )
    image = value[0]
    _require(isinstance(image, dict), "local_image_invalid", "Local image record is invalid")
    image = cast(dict[str, Any], image)
    operating_system, architecture = platform.split("/", 1)
    classic = image.get("Id") == config_digest and "Descriptor" not in image
    descriptor = image.get("Descriptor")
    containerd = (
        image.get("Id") == child_digest
        and isinstance(descriptor, dict)
        and descriptor.get("digest") == child_digest
    )
    _require(
        isinstance(image, dict)
        and isinstance(image.get("RepoDigests"), list)
        and image["RepoDigests"] == [child_reference]
        and image.get("Os") == operating_system
        and image.get("Architecture") == architecture
        and (classic or containerd),
        "local_image_invalid",
        "Local image-store representation changed",
    )
    return "classic" if classic else "containerd"


def _validate_kind_network_warnings(result: ProcessResult) -> None:
    try:
        lines = (result.stdout + b"\n" + result.stderr).decode("utf-8").splitlines()
    except UnicodeError as error:
        raise GateError("kind_warning_invalid", "kind output is invalid") from error
    _require(
        all(lines.count(warning) == 1 for warning in _KIND_NETWORK_WARNINGS),
        "kind_warning_invalid",
        "kind selected-network warning contract changed",
    )


def _deadline_timeout(deadline: float, cap: float, monotonic: Callable[[], float]) -> float:
    timeout = min(cap, deadline - monotonic())
    _require(timeout > 0, "global_timeout", "The Strimzi gate exceeded its global deadline")
    return timeout


def _run_checked_before(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    deadline: float,
    cap: float = PROCESS_TIMEOUT_SECONDS,
    runner: Callable[..., ProcessResult] = run_process,
    monotonic: Callable[[], float] = time.monotonic,
    code: str = "subprocess_failed",
) -> ProcessResult:
    result = runner(
        command,
        cwd=cwd,
        environment=environment,
        timeout_seconds=_deadline_timeout(deadline, cap, monotonic),
    )
    _require(result.returncode == 0, code, "A required gate subprocess failed")
    return result


def _resolve_real_inputs(arguments: GateArguments) -> tuple[Path, Path, Path]:
    _require(
        arguments.wheel is not None
        and arguments.evidence_candidate is not None
        and arguments.evidence_upload is not None,
        "argument_invalid",
        "Real gate inputs are absent",
    )
    wheel_argument = cast(Path, arguments.wheel)
    _require(not wheel_argument.is_symlink(), "wheel_invalid", "Wheel input must not be a symlink")
    try:
        wheel = wheel_argument.resolve(strict=True)
        wheel_mode = wheel.stat().st_mode
    except OSError as error:
        raise GateError("wheel_invalid", "Wheel input is unavailable") from error
    _require(
        wheel.is_absolute()
        and stat.S_ISREG(wheel_mode)
        and wheel.suffix == ".whl"
        and 0 < wheel.stat().st_size <= MAX_DOWNLOAD_BYTES,
        "wheel_invalid",
        "Wheel input is outside the bounded artifact contract",
    )

    paths: list[Path] = []
    for raw_value in (arguments.evidence_candidate, arguments.evidence_upload):
        value = cast(Path, raw_value)
        _require(value.is_absolute(), "argument_invalid", "Evidence paths must be absolute")
        try:
            parent = value.parent.resolve(strict=True)
        except OSError as error:
            raise GateError("argument_invalid", "Evidence parent is unavailable") from error
        resolved = parent / value.name
        _require(
            parent.is_dir()
            and not parent.is_symlink()
            and not resolved.exists()
            and not resolved.is_symlink(),
            "argument_invalid",
            "Evidence target must be fresh",
        )
        paths.append(resolved)
    _require(paths[0] != paths[1], "argument_invalid", "Evidence targets must be distinct")
    return wheel, paths[0], paths[1]


def _resolve_docker() -> Path:
    discovered = shutil.which("docker")
    _require(discovered is not None, "docker_unavailable", "Docker CLI is unavailable")
    discovered = cast(str, discovered)
    try:
        docker = Path(discovered).resolve(strict=True)
        mode = docker.stat().st_mode
    except OSError as error:
        raise GateError("docker_unavailable", "Docker CLI is unavailable") from error
    _require(
        docker.is_absolute() and stat.S_ISREG(mode) and os.access(docker, os.X_OK),
        "docker_unavailable",
        "Docker CLI is invalid",
    )
    return docker


def _docker_server_images(
    lock: ImagesLock,
    docker: Path,
    *,
    runner: Callable[..., ProcessResult],
) -> PlatformImages:
    result = runner(
        [str(docker), "info", "--format", "{{json .}}"],
        cwd=_FIXTURE_DIR,
        environment=_minimal_environment(_FIXTURE_DIR),
        timeout_seconds=30,
    )
    _require(
        result.returncode == 0 and not result.stderr,
        "docker_unavailable",
        "Docker server inspection failed",
    )
    value = _parse_json_bytes(result.stdout, code="docker_info_invalid")
    _require(isinstance(value, dict), "docker_info_invalid", "Docker server inspection is invalid")
    return _select_platform(
        lock,
        _text(value.get("OSType"), code="docker_info_invalid"),
        _text(value.get("Architecture"), code="docker_info_invalid"),
    )


def _prepare_runtime(prefix: str, candidate: Path) -> tuple[Path, Path, Path]:
    raw_root = os.environ.get("RUNNER_TEMP")
    try:
        root = (
            Path(raw_root).resolve(strict=True)
            if raw_root
            else Path(tempfile.gettempdir()).resolve(strict=True)
        )
    except OSError as error:
        raise GateError("runtime_invalid", "Runner temporary root is unavailable") from error
    runtime = root / prefix
    _require(
        root.is_dir()
        and not root.is_symlink()
        and not runtime.exists()
        and not runtime.is_symlink()
        and not candidate.exists(),
        "runtime_invalid",
        "Runtime or evidence candidate target is not fresh",
    )
    try:
        runtime.mkdir(mode=0o700)
        candidate.mkdir(mode=0o700)
        home = runtime / "home"
        tools = runtime / "tools"
        home.mkdir(mode=0o700)
        (home / ".docker").mkdir(mode=0o700)
        tools.mkdir(mode=0o700)
    except OSError as error:
        for partial in (runtime, candidate):
            if partial.exists() and partial.is_dir() and not partial.is_symlink():
                shutil.rmtree(partial, ignore_errors=True)
        raise GateError("runtime_invalid", "Runtime directories could not be created") from error
    return runtime, home, tools


def _guarded_environment(
    base: Mapping[str, str],
    *,
    guard_dir: Path,
    marker: Path,
    contract: GateContract,
) -> dict[str, str]:
    environment = dict(base)
    environment.update(contract.environment_sentinels)
    environment["PYTHONPATH"] = str(guard_dir)
    environment["STREAMT_STRIMZI_GUARD_MARKER"] = str(marker)
    environment["COLUMNS"] = "160"
    return environment


def _invoke_guarded(
    command: Sequence[str],
    *,
    cwd: Path,
    base_environment: Mapping[str, str],
    guard_dir: Path,
    marker: Path,
    contract: GateContract,
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
    cap: float = 60,
) -> ProcessResult:
    _require(not marker.exists(), "guard_invalid", "Offline guard marker already exists")
    result = _run_checked_before(
        command,
        cwd=cwd,
        environment=_guarded_environment(
            base_environment,
            guard_dir=guard_dir,
            marker=marker,
            contract=contract,
        ),
        deadline=deadline,
        cap=cap,
        runner=runner,
        monotonic=monotonic,
        code="guarded_process_failed",
    )
    try:
        marker_bytes = marker.read_bytes()
    except OSError as error:
        raise GateError("guard_inactive", "Offline exporter guard did not activate") from error
    _require(
        marker_bytes == b"active\n", "guard_inactive", "Offline exporter guard did not activate"
    )
    surface = result.stdout + result.stderr + repr(result).encode("utf-8")
    _require(
        not any(secret.encode("utf-8") in surface for secret in contract.confidential_sentinels),
        "guard_secret_leak",
        "A confidential sentinel reached a guarded process surface",
    )
    return result


def _verify_installed_wheel(
    python: Path,
    *,
    venv: Path,
    checkout: Path,
    cwd: Path,
    base_environment: Mapping[str, str],
    guard_dir: Path,
    marker: Path,
    contract: GateContract,
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> None:
    probe = "from pathlib import Path\nimport streamt\nprint(Path(streamt.__file__).resolve())\n"
    result = _invoke_guarded(
        [str(python), "-c", probe],
        cwd=cwd,
        base_environment=base_environment,
        guard_dir=guard_dir,
        marker=marker,
        contract=contract,
        deadline=deadline,
        runner=runner,
        monotonic=monotonic,
    )
    try:
        module_path = Path(result.stdout.decode("utf-8").strip()).resolve(strict=True)
        venv_root = venv.resolve(strict=True)
        checkout_root = checkout.resolve(strict=True)
    except (OSError, UnicodeError) as error:
        raise GateError(
            "wheel_import_invalid", "Installed wheel import identity is invalid"
        ) from error
    _require(
        result.stderr == b""
        and module_path.is_file()
        and module_path.is_relative_to(venv_root)
        and not module_path.is_relative_to(checkout_root),
        "wheel_import_invalid",
        "Installed exporter resolved to the checkout or outside its venv",
    )


def _export_twice(
    executable: Path,
    *,
    project: Path,
    namespace: str,
    cluster_name: str,
    output_one: Path,
    output_two: Path,
    cwd: Path,
    base_environment: Mapping[str, str],
    guard_dir: Path,
    contract: GateContract,
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> tuple[bytes, tuple[dict[str, Any], ...]]:
    for index, output in enumerate((output_one, output_two), start=1):
        marker = guard_dir / f"export-{index}.marker"
        result = _invoke_guarded(
            [
                str(executable),
                "--quiet",
                "export",
                "strimzi",
                "--namespace",
                namespace,
                "--cluster-name",
                cluster_name,
                "--project-dir",
                str(project),
                "--output-file",
                str(output),
            ],
            cwd=cwd,
            base_environment=base_environment,
            guard_dir=guard_dir,
            marker=marker,
            contract=contract,
            deadline=deadline,
            runner=runner,
            monotonic=monotonic,
        )
        _require(
            result.stdout == b"" and result.stderr == b"",
            "export_surface_invalid",
            "Quiet file export wrote a process surface",
        )
    first = _read_limited(output_one)
    second = _read_limited(output_two)
    _require(
        first == second, "export_nondeterministic", "Installed exporter bytes differ across runs"
    )
    _require(
        not any(secret.encode("utf-8") in first for secret in contract.confidential_sentinels),
        "export_secret_leak",
        "A confidential sentinel reached generated YAML",
    )
    documents = _parse_yaml_bytes(first, expected_count=len(contract.topics))
    validated = _validate_export_documents(documents, contract, namespace, cluster_name)
    output_one.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    return first, validated


def _kubectl_json(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> Any:
    result = _run_checked_before(
        command,
        cwd=cwd,
        environment=environment,
        deadline=deadline,
        cap=30,
        runner=runner,
        monotonic=monotonic,
        code="kubectl_failed",
    )
    _require(result.stderr == b"", "kubectl_output_invalid", "kubectl JSON wrote stderr")
    return _parse_json_bytes(result.stdout, code="kubectl_output_invalid")


def _validate_topic_projection(
    topic_object: Mapping[str, Any],
    expected_document: Mapping[str, Any],
) -> None:
    metadata = topic_object.get("metadata")
    expected_metadata = expected_document.get("metadata")
    _require(
        topic_object.get("apiVersion") == expected_document.get("apiVersion")
        and topic_object.get("kind") == expected_document.get("kind")
        and isinstance(metadata, dict)
        and isinstance(expected_metadata, dict)
        and metadata.get("name") == expected_metadata.get("name")
        and metadata.get("namespace") == expected_metadata.get("namespace")
        and metadata.get("labels") == expected_metadata.get("labels")
        and metadata.get("annotations") == expected_metadata.get("annotations")
        and topic_object.get("spec") == expected_document.get("spec"),
        "topic_readback_invalid",
        "KafkaTopic contract projection changed",
    )


def _topic_object_ready(
    value: Any,
    expected_document: Mapping[str, Any],
) -> Mapping[str, Any]:
    _require(isinstance(value, dict), "topic_readback_invalid", "KafkaTopic output is invalid")
    _validate_topic_projection(value, expected_document)
    metadata = value.get("metadata")
    status_value = value.get("status")
    assert isinstance(metadata, dict)
    generation = metadata.get("generation")
    if status_value is None:
        raise PollPendingError
    _require(
        isinstance(generation, int)
        and not isinstance(generation, bool)
        and generation >= 1
        and isinstance(status_value, dict),
        "topic_readback_invalid",
        "KafkaTopic readiness shape changed",
    )
    status_value = cast(dict[str, Any], status_value)
    conditions = status_value.get("conditions")
    if (
        status_value.get("observedGeneration") != generation
        or not isinstance(conditions, list)
        or not any(
            isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        )
    ):
        raise PollPendingError
    _require(
        sum(
            isinstance(item, dict) and item.get("type") == "Ready" and item.get("status") == "True"
            for item in conditions
        )
        == 1,
        "topic_readback_invalid",
        "KafkaTopic readiness condition repeats",
    )
    _text(status_value.get("topicName"), code="topic_readback_invalid")
    _text(status_value.get("topicId"), code="topic_readback_invalid")
    return cast(Mapping[str, Any], value)


def _kubectl_collection_items(
    value: Any,
    *,
    item_kind: str,
    code: str,
) -> tuple[Mapping[str, Any], ...]:
    _require(item_kind in {"Pod", "Service"}, code, "Collection item kind is invalid")
    root = _mapping(
        value,
        keys={"apiVersion", "kind", "metadata", "items"},
        code=code,
    )
    _require(
        root["apiVersion"] == "v1"
        and root["kind"] == "List"
        and root["metadata"] == {"resourceVersion": ""},
        code,
        "kubectl collection envelope changed",
    )
    result: list[Mapping[str, Any]] = []
    for item in _sequence(root["items"], code=code):
        _require(
            isinstance(item, dict)
            and item.get("apiVersion") == "v1"
            and item.get("kind") == item_kind,
            code,
            "kubectl collection item identity changed",
        )
        result.append(item)
    return tuple(result)


def _combine_pod_lists(*values: Any) -> dict[str, Any]:
    items: list[Any] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        for item in _kubectl_collection_items(
            value,
            item_kind="Pod",
            code="workload_image_invalid",
        ):
            metadata = item.get("metadata")
            _require(
                isinstance(metadata, dict), "workload_image_invalid", "Pod metadata is invalid"
            )
            metadata = cast(dict[str, Any], metadata)
            identity = (
                _text(metadata.get("namespace"), code="workload_image_invalid"),
                _text(metadata.get("name"), code="workload_image_invalid"),
            )
            _require(identity not in seen, "workload_image_invalid", "Pod inventory repeats")
            seen.add(identity)
            items.append(item)
    _require(bool(items), "workload_image_invalid", "Strimzi workload Pod inventory is empty")
    return {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items}


def _select_broker_pod(pods: Mapping[str, Any], cluster_name: str) -> str:
    matches: list[str] = []
    for item in _sequence(pods.get("items"), code="broker_pod_invalid"):
        _require(isinstance(item, dict), "broker_pod_invalid", "Broker Pod item is invalid")
        metadata = item.get("metadata")
        status_value = item.get("status")
        containers = _container_pairs(item, "containers")
        if any(name == "kafka" for name, _reference in containers):
            _require(
                isinstance(metadata, dict)
                and isinstance(status_value, dict)
                and status_value.get("phase") == "Running"
                and isinstance(status_value.get("conditions"), list)
                and any(
                    isinstance(condition, dict)
                    and condition.get("type") == "Ready"
                    and condition.get("status") == "True"
                    for condition in status_value["conditions"]
                ),
                "broker_pod_invalid",
                "Broker Pod is not Ready",
            )
            name = _text(metadata.get("name"), code="broker_pod_invalid")
            _require(
                name.startswith(f"{cluster_name}-"),
                "broker_pod_invalid",
                "Broker Pod identity changed",
            )
            matches.append(name)
    _require(len(matches) == 1, "broker_pod_invalid", "Broker Pod closure changed")
    return matches[0]


def _observe_topics(
    *,
    kubectl: Path,
    kubeconfig: Path,
    namespace: str,
    broker_pod: str,
    expected_documents: Sequence[Mapping[str, Any]],
    contract: GateContract,
    cwd: Path,
    environment: Mapping[str, str],
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> tuple[TopicSnapshot, ...]:
    by_name = {topic.metadata_name: topic for topic in contract.topics}

    def reader() -> tuple[TopicSnapshot, ...]:
        snapshots: list[TopicSnapshot] = []
        for expected in expected_documents:
            metadata = expected.get("metadata")
            _require(
                isinstance(metadata, dict),
                "topic_readback_invalid",
                "Expected topic metadata is invalid",
            )
            metadata = cast(dict[str, Any], metadata)
            metadata_name = _text(metadata.get("name"), code="topic_readback_invalid")
            expectation = by_name.get(metadata_name)
            _require(
                expectation is not None,
                "topic_readback_invalid",
                "Expected topic contract is absent",
            )
            expectation = cast(TopicExpectation, expectation)
            topic_value = _kubectl_json(
                [
                    str(kubectl),
                    "--kubeconfig",
                    str(kubeconfig),
                    "--namespace",
                    namespace,
                    "get",
                    "kafkatopic",
                    metadata_name,
                    "--output=json",
                ],
                cwd=cwd,
                environment=environment,
                deadline=deadline,
                runner=runner,
                monotonic=monotonic,
            )
            _require(
                isinstance(topic_value, dict),
                "topic_readback_invalid",
                "KafkaTopic JSON is invalid",
            )
            topic_object = _topic_object_ready(topic_value, expected)
            describe = runner(
                [
                    str(kubectl),
                    "--kubeconfig",
                    str(kubeconfig),
                    "--namespace",
                    namespace,
                    "exec",
                    broker_pod,
                    "--container=kafka",
                    "--",
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--describe",
                    "--topic",
                    expectation.topic_name,
                ],
                cwd=cwd,
                environment=environment,
                timeout_seconds=_deadline_timeout(deadline, 30, monotonic),
            )
            configs = runner(
                [
                    str(kubectl),
                    "--kubeconfig",
                    str(kubeconfig),
                    "--namespace",
                    namespace,
                    "exec",
                    broker_pod,
                    "--container=kafka",
                    "--",
                    "/opt/kafka/bin/kafka-configs.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--entity-type",
                    "topics",
                    "--entity-name",
                    expectation.topic_name,
                    "--describe",
                ],
                cwd=cwd,
                environment=environment,
                timeout_seconds=_deadline_timeout(deadline, 30, monotonic),
            )
            if describe.returncode != 0 or configs.returncode != 0:
                raise PollPendingError
            description = _parse_topic_describe(describe.stdout, expectation.topic_name)
            configuration = _parse_topic_configs(configs.stdout, expectation.topic_name)
            _require(
                description["partition_count"] == expectation.partitions
                and description["replication_factor"] == expectation.replicas
                and configuration == expectation.config,
                "broker_readback_invalid",
                "Broker state differs from the exact topic contract",
            )
            snapshots.append(_topic_snapshot(topic_object, description, configuration))
        return tuple(snapshots)

    return poll_exact(
        reader,
        timeout_seconds=TOPIC_TIMEOUT_SECONDS,
        timeout_code="topic_timeout",
        global_deadline=deadline,
        monotonic=monotonic,
    )


def _validate_downloaded_yaml(path: Path) -> None:
    raw = _read_limited(path)
    try:
        events = tuple(yaml.parse(raw))
        _require(
            not any(isinstance(event, yaml.events.AliasEvent) for event in events),
            "yaml_invalid",
            "Downloaded YAML aliases are forbidden",
        )
        loaded = list(yaml.load_all(raw, Loader=_StrictLoader))
    except GateError:
        raise
    except (UnicodeError, yaml.YAMLError) as error:
        raise GateError("yaml_invalid", "Downloaded YAML is invalid") from error
    documents = [item for item in loaded if item is not None]
    _require(
        documents and len(documents) <= 64 and all(isinstance(item, dict) for item in documents),
        "yaml_invalid",
        "Downloaded YAML document closure is invalid",
    )


def _verify_image_chain(
    docker: Path,
    images: PlatformImages,
    *,
    cwd: Path,
    environment: Mapping[str, str],
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> None:
    identities = (
        (
            images.node_index_reference,
            images.node_child_reference,
            images.node_manifest_digest,
            images.node_config_digest,
        ),
        (
            images.operator_index_reference,
            images.operator_child_reference,
            images.operator_manifest_digest,
            images.operator_config_digest,
        ),
        (
            images.kafka_index_reference,
            images.kafka_child_reference,
            images.kafka_manifest_digest,
            images.kafka_config_digest,
        ),
    )
    for index_reference, child_reference, child_digest, config_digest in identities:
        raw_index = _run_checked_before(
            [str(docker), "manifest", "inspect", index_reference],
            cwd=cwd,
            environment=environment,
            deadline=deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="image_index_failed",
        )
        _require(
            raw_index.stderr == b"", "image_index_invalid", "Image index inspection wrote stderr"
        )
        _validate_index_manifest(
            _parse_json_bytes(raw_index.stdout, code="image_index_invalid"),
            platform=images.platform,
            expected_child_digest=child_digest,
        )
        _run_checked_before(
            [str(docker), "pull", "--platform", images.platform, child_reference],
            cwd=cwd,
            environment=environment,
            deadline=deadline,
            cap=180,
            runner=runner,
            monotonic=monotonic,
            code="image_pull_failed",
        )
        child_manifest = _run_checked_before(
            [str(docker), "manifest", "inspect", child_reference],
            cwd=cwd,
            environment=environment,
            deadline=deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="child_manifest_failed",
        )
        _require(
            child_manifest.stderr == b"",
            "child_manifest_invalid",
            "Child manifest inspection wrote stderr",
        )
        _validate_child_manifest(
            _parse_json_bytes(child_manifest.stdout, code="child_manifest_invalid"),
            expected_config_digest=config_digest,
        )
        inspected = _run_checked_before(
            [str(docker), "image", "inspect", child_reference],
            cwd=cwd,
            environment=environment,
            deadline=deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
            code="local_image_invalid",
        )
        _require(
            inspected.stderr == b"", "local_image_invalid", "Local image inspection wrote stderr"
        )
        _validate_local_image(
            _parse_json_bytes(inspected.stdout, code="local_image_invalid"),
            child_reference=child_reference,
            child_digest=child_digest,
            config_digest=config_digest,
            platform=images.platform,
        )


def _capture_cluster_evidence(
    *,
    candidate: Path,
    confidential: Iterable[str],
    docker: Path,
    node_name: str,
    kubectl: Path,
    kubeconfig: Path,
    namespace: str,
    cluster_name: str,
    broker_pod: str | None,
    topics: Sequence[TopicExpectation],
    cwd: Path,
    environment: Mapping[str, str],
    deadline: float,
    runner: Callable[..., ProcessResult],
    monotonic: Callable[[], float],
) -> tuple[tuple[str, ...], bool]:
    commands: list[tuple[str, list[str]]] = [
        (
            "ctr-images.txt",
            list(_ctr_images_list_command(docker, node_name)),
        ),
    ]
    if kubeconfig.is_file() and not kubeconfig.is_symlink():
        commands.extend(
            [
                (
                    "kubernetes-version.json",
                    [str(kubectl), "--kubeconfig", str(kubeconfig), "version", "--output=json"],
                ),
                (
                    "kafkatopic-crd.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "get",
                        "customresourcedefinition/kafkatopics.kafka.strimzi.io",
                        "--output=json",
                    ],
                ),
                (
                    "nodes.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "get",
                        "nodes",
                        "--output=json",
                    ],
                ),
                (
                    "operator-deployment.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "deployment/strimzi-cluster-operator",
                        "--output=json",
                    ],
                ),
                (
                    "workload-pods.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "pods",
                        "--output=json",
                    ],
                ),
                (
                    "strimzi-resources.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "kafka,kafkanodepool,kafkatopic",
                        "--output=json",
                    ],
                ),
                (
                    "events.json",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "events",
                        "--output=json",
                    ],
                ),
                (
                    "operator.log",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "logs",
                        "deployment/strimzi-cluster-operator",
                        "--tail=500",
                        "--limit-bytes=524288",
                    ],
                ),
                (
                    "topic-operator.log",
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "logs",
                        f"deployment/{cluster_name}-entity-operator",
                        "--container=topic-operator",
                        "--tail=500",
                        "--limit-bytes=524288",
                    ],
                ),
            ]
        )
    if broker_pod is not None and kubeconfig.is_file() and not kubeconfig.is_symlink():
        for index, topic in enumerate(topics):
            commands.extend(
                [
                    (
                        f"broker-{index}-describe.txt",
                        [
                            str(kubectl),
                            "--kubeconfig",
                            str(kubeconfig),
                            "--namespace",
                            namespace,
                            "exec",
                            broker_pod,
                            "--container=kafka",
                            "--",
                            "/opt/kafka/bin/kafka-topics.sh",
                            "--bootstrap-server",
                            "localhost:9092",
                            "--describe",
                            "--topic",
                            topic.topic_name,
                        ],
                    ),
                    (
                        f"broker-{index}-configs.txt",
                        [
                            str(kubectl),
                            "--kubeconfig",
                            str(kubeconfig),
                            "--namespace",
                            namespace,
                            "exec",
                            broker_pod,
                            "--container=kafka",
                            "--",
                            "/opt/kafka/bin/kafka-configs.sh",
                            "--bootstrap-server",
                            "localhost:9092",
                            "--entity-type",
                            "topics",
                            "--entity-name",
                            topic.topic_name,
                            "--describe",
                        ],
                    ),
                ]
            )
    captured: list[str] = []
    rejected = False
    for name, command in commands:
        remaining = deadline - monotonic()
        if remaining <= 0:
            rejected = True
            break
        try:
            result = runner(
                command,
                cwd=cwd,
                environment=environment,
                timeout_seconds=min(5, remaining),
                max_output_bytes=512 * 1024,
            )
        except Exception:
            failure_name = f"{name}.failed"
            try:
                _evidence_write(
                    candidate,
                    failure_name,
                    b'{"status":"capture-failed"}\n',
                    confidential,
                )
            except Exception:
                rejected = True
                break
            captured.append(failure_name)
            continue
        if result.returncode != 0 or (name == "ctr-images.txt" and result.stderr != b""):
            try:
                _evidence_write(
                    candidate,
                    name,
                    _canonical_json_bytes(
                        {
                            "returncode": result.returncode,
                            "status": "capture-failed",
                        }
                    ),
                    confidential,
                )
            except Exception:
                rejected = True
                break
            captured.append(name)
            continue
        try:
            payload = result.stdout
            if name == "ctr-images.txt":
                names = _parse_ctr_image_names(payload)
                payload = "".join(f"{item}\n" for item in sorted(names)).encode("utf-8")
            _evidence_write(candidate, name, payload, confidential)
            captured.append(name)
            if name == "kafkatopic-crd.json":
                digest_name = "kafkatopic-crd-sha256.json"
                _evidence_write(
                    candidate,
                    digest_name,
                    _canonical_json_bytes({"sha256": _sha256_bytes(payload)}),
                    confidential,
                )
                captured.append(digest_name)
        except Exception:
            rejected = True
            break
    return tuple(captured), rejected


def _cleanup_succeeded(report: CleanupReport) -> bool:
    return (
        report.cluster_delete_returncode == 0
        and report.node_remove_returncode == 0
        and report.network_remove_returncode == 0
        and not report.residue_containers
        and not report.residue_networks
    )


def _execute_gate(
    arguments: GateArguments,
    *,
    runner: Callable[..., ProcessResult] = run_process,
    downloader: Callable[..., None] = _download_verified,
    monotonic: Callable[[], float] = time.monotonic,
) -> int:
    """Run the full pinned gate; every created identity derives from one token."""
    bundle = _load_fixture_bundle()
    wheel, candidate, upload = _resolve_real_inputs(arguments)
    docker = _resolve_docker()
    images = _docker_server_images(bundle.images_lock, docker, runner=runner)
    host_tools = _select_host_tools(
        bundle.images_lock,
        host_platform.system(),
        host_platform.machine(),
    )
    _validate_platform_combination(host_tools, images)
    # This is intentionally before any filesystem, image-store, network, or
    # deployment mutation. Docker server inspection above is read-only.
    _validate_pilot_mode(images, arguments.pilot)

    token = uuid.uuid4().hex[:12]
    _require(
        re.fullmatch(r"[a-z0-9]{12}", token) is not None,
        "identity_invalid",
        "Unique token is invalid",
    )
    prefix = f"streamt-strimzi-{token}"
    cluster_name = f"{prefix}-kind"
    network_name = f"{prefix}-net"
    namespace = f"st-{token}"
    kafka_cluster = f"sk-{token}"
    _require(
        re.fullmatch(bundle.gate_contract.kind_cluster_pattern, cluster_name) is not None
        and re.fullmatch(bundle.gate_contract.docker_network_pattern, network_name) is not None
        and re.fullmatch(bundle.gate_contract.namespace_pattern, namespace) is not None
        and re.fullmatch(bundle.gate_contract.cluster_pattern, kafka_cluster) is not None,
        "identity_invalid",
        "Generated gate identities escaped the contract",
    )

    runtime, home, tools_dir = _prepare_runtime(prefix, candidate)
    targets = _derive_cleanup_targets(prefix, cluster_name, network_name, runtime)
    kind = tools_dir / "kind"
    kubectl = tools_dir / "kubectl"
    docker_link = tools_dir / "docker"
    kubeconfig = runtime / "kubeconfig"
    operator_source = runtime / "operator-source.yaml"
    single_source = runtime / "kafka-single-node-source.yaml"
    ephemeral_source = runtime / "kafka-ephemeral-source.yaml"
    operator_manifest = runtime / "operator.yaml"
    kafka_manifest = runtime / "kafka.yaml"
    project = runtime / "project"
    guard_dir = runtime / "guard"
    venv = runtime / "venv"
    output_one = runtime / "topics-one.yaml"
    output_two = runtime / "topics-two.yaml"
    execution_deadline = monotonic() + GLOBAL_TIMEOUT_SECONDS
    cleanup_deadline = execution_deadline + CLEANUP_RESERVE_SECONDS
    environment = _minimal_environment(home, tools_dir)
    environment["DOCKER_CONFIG"] = str(home / ".docker")
    primary: GateError | None = None
    cleanup_report = CleanupReport(-1, -1, (), (), -1)
    capture_rejected = False
    timeline: list[dict[str, Any]] = []
    first_snapshots: tuple[TopicSnapshot, ...] = ()
    second_snapshots: tuple[TopicSnapshot, ...] = ()
    pilot_record: Mapping[str, Any] | None = None
    broker_pod: str | None = None

    def phase(name: str) -> None:
        timeline.append(
            {
                "phase": name,
                "elapsed_ms": int(
                    (monotonic() - (execution_deadline - GLOBAL_TIMEOUT_SECONDS)) * 1000
                ),
            }
        )

    try:
        try:
            os.symlink(docker, docker_link)
        except OSError as error:
            raise GateError(
                "tool_setup_failed", "Validated Docker shim could not be created"
            ) from error
        _require(
            docker_link.is_symlink() and docker_link.resolve(strict=True) == docker,
            "tool_setup_failed",
            "Validated Docker shim identity changed",
        )
        downloader(
            host_tools.kind,
            kind,
            executable=True,
            deadline=execution_deadline,
            monotonic=monotonic,
        )
        downloader(
            host_tools.kubectl,
            kubectl,
            executable=True,
            deadline=execution_deadline,
            monotonic=monotonic,
        )
        downloader(
            bundle.images_lock.operator_source,
            operator_source,
            executable=False,
            deadline=execution_deadline,
            monotonic=monotonic,
        )
        downloader(
            bundle.images_lock.kafka_single_node_source,
            single_source,
            executable=False,
            deadline=execution_deadline,
            monotonic=monotonic,
        )
        downloader(
            bundle.images_lock.kafka_ephemeral_source,
            ephemeral_source,
            executable=False,
            deadline=execution_deadline,
            monotonic=monotonic,
        )
        _validate_downloaded_yaml(single_source)
        _validate_downloaded_yaml(ephemeral_source)
        source_documents = _load_yaml_documents(
            operator_source,
            bundle.operator_contract.source_sha256,
            bundle.operator_contract.document_count,
            expected_empty_count=bundle.operator_contract.empty_document_count,
        )
        phase("downloads-verified")

        _verify_image_chain(
            docker,
            images,
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        phase("image-chains-verified")

        project.mkdir(mode=0o700)
        _write_new_file(project / "stream_project.yml", bundle.project_bytes)
        guard_dir.mkdir(mode=0o700)
        guard_source = _OFFLINE_GUARD_SOURCE.encode("utf-8")
        compile(_OFFLINE_GUARD_SOURCE, "sitecustomize.py", "exec")
        _write_new_file(guard_dir / "sitecustomize.py", guard_source)
        python = Path(sys.executable).resolve(strict=True)
        _run_checked_before(
            [str(python), "-m", "venv", str(venv)],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=120,
            runner=runner,
            monotonic=monotonic,
            code="venv_failed",
        )
        venv_python = venv / "bin" / "python"
        streamt = venv / "bin" / "streamt"
        _run_checked_before(
            [
                str(venv_python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-input",
                "--no-cache-dir",
                str(wheel),
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=300,
            runner=runner,
            monotonic=monotonic,
            code="wheel_install_failed",
        )
        _require(
            venv_python.is_file()
            and streamt.is_file()
            and not streamt.is_symlink()
            and os.access(streamt, os.X_OK),
            "wheel_install_failed",
            "Installed wheel entry point is invalid",
        )
        _verify_installed_wheel(
            venv_python,
            venv=venv,
            checkout=Path(__file__).resolve().parents[2],
            cwd=runtime,
            base_environment=environment,
            guard_dir=guard_dir,
            marker=guard_dir / "import.marker",
            contract=bundle.gate_contract,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        topic_bytes, topic_documents = _export_twice(
            streamt,
            project=project,
            namespace=namespace,
            cluster_name=kafka_cluster,
            output_one=output_one,
            output_two=output_two,
            cwd=runtime,
            base_environment=environment,
            guard_dir=guard_dir,
            contract=bundle.gate_contract,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        phase("offline-export-verified")

        rewritten_operator = _rewrite_operator_documents(
            source_documents,
            namespace,
            images,
            bundle.operator_contract,
        )
        _validate_operator_closure(
            rewritten_operator,
            namespace,
            images,
            bundle.operator_contract,
        )
        rendered_kafka = _render_kafka_documents(
            bundle.kafka_documents,
            namespace,
            kafka_cluster,
            images,
        )
        _write_new_file(operator_manifest, _yaml_stream_bytes(rewritten_operator))
        _write_new_file(kafka_manifest, _yaml_stream_bytes(rendered_kafka))
        _evidence_write(
            candidate,
            "generated-checksums.json",
            _canonical_json_bytes(
                {
                    "export_sha256": _sha256_bytes(topic_bytes),
                    "fixtures": _FIXTURE_SHA256,
                    "operator_manifest_sha256": _sha256_file(operator_manifest),
                    "kafka_manifest_sha256": _sha256_file(kafka_manifest),
                    "wheel_sha256": _sha256_file(wheel, MAX_DOWNLOAD_BYTES),
                }
            ),
            bundle.gate_contract.confidential_sentinels,
        )

        network = _run_checked_before(
            _docker_network_create_command(docker, network_name),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
            code="docker_network_failed",
        )
        try:
            created_network_id = network.stdout.decode("ascii").strip()
        except UnicodeError as error:
            raise GateError(
                "docker_network_invalid", "Docker network ID output is invalid"
            ) from error
        _require(
            _HEX_SHA256.fullmatch(created_network_id) is not None and network.stderr == b"",
            "docker_network_invalid",
            "Docker network creation output changed",
        )
        network_inspect = _run_checked_before(
            [str(docker), "network", "inspect", network_name],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
        )
        network_id = _validate_docker_network(
            _parse_json_bytes(network_inspect.stdout, code="docker_network_invalid"),
            network_name,
            created_network_id,
        )
        phase("isolated-bridge-created")

        kind_environment = dict(environment)
        kind_environment["KIND_EXPERIMENTAL_DOCKER_NETWORK"] = network_name
        created = _run_checked_before(
            [
                str(kind),
                "create",
                "cluster",
                "--name",
                cluster_name,
                "--config",
                str(_FIXTURE_DIR / "kind.yaml"),
                "--image",
                images.node_child_reference,
                "--kubeconfig",
                str(kubeconfig),
                "--wait",
                "300s",
            ],
            cwd=runtime,
            environment=kind_environment,
            deadline=execution_deadline,
            cap=330,
            runner=runner,
            monotonic=monotonic,
            code="kind_create_failed",
        )
        _validate_kind_network_warnings(created)
        _require(
            kubeconfig.is_file()
            and not kubeconfig.is_symlink()
            and 0 < kubeconfig.stat().st_size <= MAX_INPUT_BYTES,
            "kubeconfig_invalid",
            "kind kubeconfig is invalid",
        )
        kubeconfig.chmod(stat.S_IRUSR | stat.S_IWUSR)
        phase("kind-created")

        route_delete = _run_checked_before(
            _default_route_delete_command(docker, targets.node_name),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=15,
            runner=runner,
            monotonic=monotonic,
            code="node_route_delete_failed",
        )
        _require(
            route_delete.stdout == b"" and route_delete.stderr == b"",
            "node_route_delete_failed",
            "Node default-route deletion output changed",
        )

        node_inspect = _run_checked_before(
            [str(docker), "inspect", targets.node_name],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
        )
        node_value = _parse_json_bytes(node_inspect.stdout, code="docker_node_invalid")
        _validate_docker_nodes(
            node_value,
            prefix=prefix,
            network_name=network_name,
            network_id=network_id,
            images=images,
        )
        _validate_node_ports(node_value)
        attached_network_inspect = _run_checked_before(
            [str(docker), "network", "inspect", network_name],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
        )
        _validate_docker_network(
            _parse_json_bytes(
                attached_network_inspect.stdout,
                code="docker_network_invalid",
            ),
            network_name,
            network_id,
            expected_node=targets.node_name,
        )
        initial_routes = tuple(
            _run_checked_before(
                _default_route_inventory_command(
                    docker,
                    targets.node_name,
                    family=family,
                ),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=10,
                runner=runner,
                monotonic=monotonic,
                code="node_route_invalid",
            )
            for family in (4, 6)
        )
        _validate_empty_default_routes(*initial_routes)
        node_record = cast(list[dict[str, Any]], node_value)[0]
        _evidence_write(
            candidate,
            "node-image.json",
            _canonical_json_bytes(
                {
                    "platform": images.platform,
                    "index": images.node_index_reference,
                    "child": images.node_child_reference,
                    "config": images.node_config_digest,
                    "docker_node_image_id": node_record["Image"],
                }
            ),
            bundle.gate_contract.confidential_sentinels,
        )
        egress = _run_checked_before(
            [
                str(docker),
                "exec",
                targets.node_name,
                "bash",
                "-ceu",
                (
                    "command -v bash >/dev/null; command -v timeout >/dev/null; "
                    "timeout 5 bash -c '</dev/tcp/127.0.0.1/6443' 2>/dev/null || exit 41; "
                    "if timeout 5 bash -c '</dev/tcp/1.1.1.1/443' 2>/dev/null; then exit 42; "
                    'else code=$?; case "$code" in 1|124) exit 0;; *) exit 43;; esac; fi'
                ),
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=15,
            runner=runner,
            monotonic=monotonic,
            code="egress_probe_failed",
        )
        _require(
            egress.stdout == b"" and egress.stderr == b"",
            "egress_probe_failed",
            "Node egress probe output changed",
        )
        phase("node-isolation-verified")

        ctr_inventory = _run_checked_before(
            _ctr_images_list_command(docker, targets.node_name),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
            code="ctr_inventory_invalid",
        )
        _require(
            ctr_inventory.stderr == b"",
            "ctr_inventory_invalid",
            "ctr image inventory wrote stderr",
        )
        ctr_names = _parse_ctr_image_names(ctr_inventory.stdout)
        import_pairs: list[CtrImportPair] = []
        load_modes: list[bool] = []
        selected_digests = {
            images.operator_manifest_digest,
            images.operator_config_digest,
            images.kafka_manifest_digest,
            images.kafka_config_digest,
        }
        for load_index, (reference, config_digest) in enumerate(
            (
                (images.operator_child_reference, images.operator_config_digest),
                (images.kafka_child_reference, images.kafka_config_digest),
            )
        ):
            before_names = ctr_names
            _run_checked_before(
                [str(kind), "load", "docker-image", reference, "--name", cluster_name],
                cwd=runtime,
                environment=kind_environment,
                deadline=execution_deadline,
                cap=120,
                runner=runner,
                monotonic=monotonic,
                code="kind_image_load_failed",
            )
            ctr_inventory = _run_checked_before(
                _ctr_images_list_command(docker, targets.node_name),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=30,
                runner=runner,
                monotonic=monotonic,
                code="ctr_inventory_invalid",
            )
            _require(
                ctr_inventory.stderr == b"",
                "ctr_inventory_invalid",
                "ctr image inventory wrote stderr",
            )
            post_load_names = _parse_ctr_image_names(ctr_inventory.stdout)
            try:
                _evidence_write(
                    candidate,
                    f"ctr-load-{load_index}-before.txt",
                    _canonical_ctr_image_names(before_names),
                    bundle.gate_contract.confidential_sentinels,
                )
                _evidence_write(
                    candidate,
                    f"ctr-load-{load_index}-after.txt",
                    _canonical_ctr_image_names(post_load_names),
                    bundle.gate_contract.confidential_sentinels,
                )
            except Exception:
                capture_rejected = True
                raise
            import_sources = tuple(
                sorted(
                    name
                    for name in post_load_names - before_names
                    if _parse_ctr_import_source(name) is not None
                )
            )
            _require(
                len(import_sources) <= 2,
                "ctr_inventory_invalid",
                "ctr image load added too many import identities",
            )
            for import_index, source in enumerate(import_sources):
                try:
                    parsed_source = _parse_ctr_import_source(source)
                    assert parsed_source is not None
                    import_content = _run_checked_before(
                        _ctr_content_get_command(
                            docker,
                            targets.node_name,
                            parsed_source[1],
                        ),
                        cwd=runtime,
                        environment=environment,
                        deadline=execution_deadline,
                        cap=30,
                        runner=runner,
                        monotonic=monotonic,
                        code="ctr_content_invalid",
                    )
                    _evidence_write(
                        candidate,
                        f"ctr-load-{load_index}-import-{import_index}.json",
                        _ctr_import_content_evidence(source, import_content),
                        bundle.gate_contract.confidential_sentinels,
                    )
                except Exception:
                    capture_rejected = True
                    raise
            import_pair = _classify_ctr_image_load(
                before_names,
                post_load_names,
                reference,
                config_digest,
            )
            load_modes.append(import_pair is not None)
            if import_pair is not None:
                _require(
                    import_pair.outer_digest not in selected_digests
                    and all(
                        import_pair.child_source not in {prior.child_source, prior.outer_source}
                        and import_pair.outer_source not in {prior.child_source, prior.outer_source}
                        and import_pair.outer_digest != prior.outer_digest
                        for prior in import_pairs
                    ),
                    "ctr_inventory_invalid",
                    "ctr import pairs overlap selected image identities",
                )
                outer_content = _run_checked_before(
                    _ctr_content_get_command(
                        docker,
                        targets.node_name,
                        import_pair.outer_digest,
                    ),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=30,
                    runner=runner,
                    monotonic=monotonic,
                    code="ctr_content_invalid",
                )
                _validate_ctr_outer_content(outer_content, import_pair, reference)
                tagged = _run_checked_before(
                    _ctr_image_tag_command(
                        docker,
                        targets.node_name,
                        import_pair.child_source,
                        reference,
                    ),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=30,
                    runner=runner,
                    monotonic=monotonic,
                    code="ctr_tag_invalid",
                )
                _validate_ctr_tag_result(tagged, reference)
                pre_remove = _run_checked_before(
                    _ctr_images_list_command(docker, targets.node_name),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=30,
                    runner=runner,
                    monotonic=monotonic,
                    code="ctr_inventory_invalid",
                )
                _require(
                    pre_remove.stderr == b"",
                    "ctr_inventory_invalid",
                    "ctr image inventory wrote stderr",
                )
                _validate_ctr_pre_remove_names(
                    post_load_names,
                    _parse_ctr_image_names(pre_remove.stdout),
                    reference,
                    import_pair,
                )
                removed = _run_checked_before(
                    _ctr_images_remove_command(docker, targets.node_name, import_pair),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=30,
                    runner=runner,
                    monotonic=monotonic,
                    code="ctr_remove_invalid",
                )
                _validate_ctr_remove_result(removed, import_pair)
                normalized = _run_checked_before(
                    _ctr_images_list_command(docker, targets.node_name),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=30,
                    runner=runner,
                    monotonic=monotonic,
                    code="ctr_inventory_invalid",
                )
                _require(
                    normalized.stderr == b"",
                    "ctr_inventory_invalid",
                    "ctr image inventory wrote stderr",
                )
                ctr_names = _parse_ctr_image_names(normalized.stdout)
                _validate_ctr_normalized_names(
                    before_names,
                    ctr_names,
                    reference,
                    config_digest,
                )
                import_pairs.append(import_pair)
            else:
                ctr_names = post_load_names

        _require(
            load_modes in ([False, False], [True, True]),
            "ctr_inventory_invalid",
            "Pinned images used mixed ctr storage representations",
        )
        if import_pairs:
            restarted = _run_checked_before(
                _containerd_restart_command(docker, targets.node_name),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=60,
                runner=runner,
                monotonic=monotonic,
                code="containerd_restart_failed",
            )
            _validate_containerd_restart_result(restarted)
            post_restart_inventory = _run_checked_before(
                _ctr_images_list_command(docker, targets.node_name),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=30,
                runner=runner,
                monotonic=monotonic,
                code="ctr_inventory_invalid",
            )
            _require(
                post_restart_inventory.stderr == b""
                and _parse_ctr_image_names(post_restart_inventory.stdout) == ctr_names,
                "ctr_inventory_invalid",
                "containerd restart changed normalized image identities",
            )
            poll_exact(
                lambda: _node_ready(
                    _kubectl_json(
                        [
                            str(kubectl),
                            "--kubeconfig",
                            str(kubeconfig),
                            "get",
                            "node",
                            targets.node_name,
                            "--output=json",
                        ],
                        cwd=runtime,
                        environment=environment,
                        deadline=execution_deadline,
                        runner=runner,
                        monotonic=monotonic,
                    ),
                    node_name=targets.node_name,
                ),
                timeout_seconds=120,
                timeout_code="node_restart_timeout",
                global_deadline=execution_deadline,
                monotonic=monotonic,
            )
            restarted_node_inspect = _run_checked_before(
                [str(docker), "inspect", targets.node_name],
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=30,
                runner=runner,
                monotonic=monotonic,
            )
            restarted_node_value = _parse_json_bytes(
                restarted_node_inspect.stdout,
                code="docker_node_invalid",
            )
            _validate_docker_nodes(
                restarted_node_value,
                prefix=prefix,
                network_name=network_name,
                network_id=network_id,
                images=images,
            )
            _validate_node_ports(restarted_node_value)
            restarted_network_inspect = _run_checked_before(
                [str(docker), "network", "inspect", network_name],
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=30,
                runner=runner,
                monotonic=monotonic,
            )
            _validate_docker_network(
                _parse_json_bytes(
                    restarted_network_inspect.stdout,
                    code="docker_network_invalid",
                ),
                network_name,
                network_id,
                expected_node=targets.node_name,
            )
            restarted_routes = tuple(
                _run_checked_before(
                    _default_route_inventory_command(
                        docker,
                        targets.node_name,
                        family=family,
                    ),
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    cap=10,
                    runner=runner,
                    monotonic=monotonic,
                    code="node_route_invalid",
                )
                for family in (4, 6)
            )
            _validate_empty_default_routes(*restarted_routes)
            restarted_egress = _run_checked_before(
                [
                    str(docker),
                    "exec",
                    targets.node_name,
                    "bash",
                    "-ceu",
                    (
                        "command -v bash >/dev/null; command -v timeout >/dev/null; "
                        "timeout 5 bash -c '</dev/tcp/127.0.0.1/6443' 2>/dev/null || exit 41; "
                        "if timeout 5 bash -c '</dev/tcp/1.1.1.1/443' 2>/dev/null; then exit 42; "
                        'else code=$?; case "$code" in 1|124) exit 0;; *) exit 43;; esac; fi'
                    ),
                ],
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=15,
                runner=runner,
                monotonic=monotonic,
                code="egress_probe_failed",
            )
            _require(
                restarted_egress.stdout == b"" and restarted_egress.stderr == b"",
                "egress_probe_failed",
                "Node egress probe output changed after containerd restart",
            )
            recovered_inventory = _run_checked_before(
                _ctr_images_list_command(docker, targets.node_name),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=30,
                runner=runner,
                monotonic=monotonic,
                code="ctr_inventory_invalid",
            )
            _require(
                recovered_inventory.stderr == b""
                and _parse_ctr_image_names(recovered_inventory.stdout) == ctr_names,
                "ctr_inventory_invalid",
                "Node recovery changed normalized image identities",
            )
        cri = _run_checked_before(
            [str(docker), "exec", targets.node_name, "crictl", "images", "-o", "json"],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
            code="cri_invalid",
        )
        _require(cri.stderr == b"", "cri_invalid", "CRI image inventory wrote stderr")
        _validate_cri_mappings(_parse_json_bytes(cri.stdout, code="cri_invalid"), images)
        phase("node-image-store-verified")

        _run_checked_before(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "create",
                "namespace",
                namespace,
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
            code="namespace_create_failed",
        )
        _run_checked_before(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "apply",
                "--server-side=true",
                "--field-manager=streamt-strimzi-gate-operator",
                "--validate=strict",
                "--filename",
                str(operator_manifest),
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=120,
            runner=runner,
            monotonic=monotonic,
            code="operator_apply_failed",
        )

        def operator_reader() -> Mapping[str, Any]:
            return _operator_ready(
                _kubectl_json(
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "deployment/strimzi-cluster-operator",
                        "--output=json",
                    ],
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    runner=runner,
                    monotonic=monotonic,
                ),
                namespace=namespace,
            )

        poll_exact(
            operator_reader,
            timeout_seconds=OPERATOR_TIMEOUT_SECONDS,
            timeout_code="operator_timeout",
            global_deadline=execution_deadline,
            monotonic=monotonic,
        )
        phase("operator-ready")

        _run_checked_before(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "apply",
                "--server-side=true",
                "--field-manager=streamt-strimzi-gate-fixture",
                "--validate=strict",
                "--filename",
                str(kafka_manifest),
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="kafka_apply_failed",
        )

        def kafka_reader() -> Mapping[str, Any]:
            return _kafka_ready(
                _kubectl_json(
                    [
                        str(kubectl),
                        "--kubeconfig",
                        str(kubeconfig),
                        "--namespace",
                        namespace,
                        "get",
                        "kafka",
                        kafka_cluster,
                        "--output=json",
                    ],
                    cwd=runtime,
                    environment=environment,
                    deadline=execution_deadline,
                    runner=runner,
                    monotonic=monotonic,
                ),
                namespace=namespace,
                cluster_name=kafka_cluster,
            )

        poll_exact(
            kafka_reader,
            timeout_seconds=KAFKA_TIMEOUT_SECONDS,
            timeout_code="kafka_timeout",
            global_deadline=execution_deadline,
            monotonic=monotonic,
        )
        phase("kafka-ready")

        cluster_pods = _kubectl_json(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "--namespace",
                namespace,
                "get",
                "pods",
                "--output=json",
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        _require(
            isinstance(cluster_pods, dict), "broker_pod_invalid", "Cluster Pod list is invalid"
        )
        broker_pod = _select_broker_pod(_combine_pod_lists(cluster_pods), kafka_cluster)

        _run_checked_before(
            _topic_apply_command(kubectl, kubeconfig, output_one, dry_run=True),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="topic_dry_run_failed",
        )
        _run_checked_before(
            _topic_apply_command(kubectl, kubeconfig, output_one, dry_run=False),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="topic_apply_failed",
        )
        first_snapshots = _observe_topics(
            kubectl=kubectl,
            kubeconfig=kubeconfig,
            namespace=namespace,
            broker_pod=broker_pod,
            expected_documents=topic_documents,
            contract=bundle.gate_contract,
            cwd=runtime,
            environment=environment,
            deadline=min(execution_deadline, monotonic() + TOPIC_TIMEOUT_SECONDS),
            runner=runner,
            monotonic=monotonic,
        )
        _evidence_write(
            candidate,
            "topic-first.json",
            _canonical_json_bytes([asdict(item) for item in first_snapshots]),
            bundle.gate_contract.confidential_sentinels,
        )
        phase("topics-ready")

        namespace_pods = _kubectl_json(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "--namespace",
                namespace,
                "get",
                "pods",
                "--output=json",
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        workload_pods = _combine_pod_lists(namespace_pods)
        services = _kubectl_json(
            [
                str(kubectl),
                "--kubeconfig",
                str(kubeconfig),
                "--namespace",
                namespace,
                "get",
                "services",
                "--output=json",
            ],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            runner=runner,
            monotonic=monotonic,
        )
        _validate_workload_exposure(
            workload_pods,
            services,
            cluster_name=kafka_cluster,
        )
        if arguments.pilot:
            pilot_record = _collect_pilot_image_ids(workload_pods, images)
        else:
            _validate_workload_images(workload_pods, images)
        phase("workload-images-verified")

        _run_checked_before(
            _topic_apply_command(kubectl, kubeconfig, output_one, dry_run=False),
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=60,
            runner=runner,
            monotonic=monotonic,
            code="topic_replay_failed",
        )
        second_snapshots = _observe_topics(
            kubectl=kubectl,
            kubeconfig=kubeconfig,
            namespace=namespace,
            broker_pod=broker_pod,
            expected_documents=topic_documents,
            contract=bundle.gate_contract,
            cwd=runtime,
            environment=environment,
            deadline=min(execution_deadline, monotonic() + REPLAY_TIMEOUT_SECONDS),
            runner=runner,
            monotonic=monotonic,
        )
        _require(
            len(first_snapshots) == len(second_snapshots) == len(bundle.gate_contract.topics),
            "replay_changed",
            "Replay topic coverage changed",
        )
        for first, second in zip(first_snapshots, second_snapshots, strict=True):
            _validate_replay(first, second)
        _evidence_write(
            candidate,
            "topic-replay.json",
            _canonical_json_bytes([asdict(item) for item in second_snapshots]),
            bundle.gate_contract.confidential_sentinels,
        )
        phase("replay-verified")
        late_routes = tuple(
            _run_checked_before(
                _default_route_inventory_command(
                    docker,
                    targets.node_name,
                    family=family,
                ),
                cwd=runtime,
                environment=environment,
                deadline=execution_deadline,
                cap=10,
                runner=runner,
                monotonic=monotonic,
                code="node_route_invalid",
            )
            for family in (4, 6)
        )
        _validate_empty_default_routes(*late_routes)
        late_network_inspect = _run_checked_before(
            [str(docker), "network", "inspect", network_name],
            cwd=runtime,
            environment=environment,
            deadline=execution_deadline,
            cap=30,
            runner=runner,
            monotonic=monotonic,
        )
        _validate_docker_network(
            _parse_json_bytes(late_network_inspect.stdout, code="docker_network_invalid"),
            network_name,
            network_id,
            expected_node=targets.node_name,
        )
        phase("node-isolation-reverified")
        if pilot_record is not None:
            _evidence_write(
                candidate,
                "pilot-image-ids.json",
                _canonical_json_bytes(pilot_record),
                bundle.gate_contract.confidential_sentinels,
            )
    except GateError as error:
        primary = error
    except Exception:
        primary = GateError("internal_error", "The Strimzi gate failed internally")
    finally:
        capture_end = min(cleanup_deadline - 125, monotonic() + 40)
        _captured, rejected = _capture_cluster_evidence(
            candidate=candidate,
            confidential=bundle.gate_contract.confidential_sentinels,
            docker=docker,
            node_name=targets.node_name,
            kubectl=kubectl,
            kubeconfig=kubeconfig,
            namespace=namespace,
            cluster_name=kafka_cluster,
            broker_pod=broker_pod,
            topics=bundle.gate_contract.topics,
            cwd=runtime,
            environment=environment,
            deadline=capture_end,
            runner=runner,
            monotonic=monotonic,
        )
        capture_rejected = capture_rejected or rejected
        cleanup_environment = dict(environment)
        cleanup_environment["KIND_EXPERIMENTAL_DOCKER_NETWORK"] = network_name
        cleanup_report = _cleanup(
            targets,
            kind=kind,
            docker=docker,
            cwd=runtime,
            environment=cleanup_environment,
            runner=runner,
            cleanup_deadline=cleanup_deadline,
            monotonic=monotonic,
        )
        cleanup_ok = _cleanup_succeeded(cleanup_report)
        if not cleanup_ok and primary is None:
            primary = GateError("cleanup_failed", "Disposable gate cleanup was incomplete")
        timeline.append(
            {
                "phase": "cleanup-complete",
                "elapsed_ms": int(
                    (monotonic() - (execution_deadline - GLOBAL_TIMEOUT_SECONDS)) * 1000
                ),
            }
        )
        required_evidence = (
            (
                "summary.json",
                _canonical_json_bytes(
                    {
                        "status": "failed"
                        if primary is not None
                        else ("pilot-pending" if arguments.pilot else "passed"),
                        "failure_code": primary.code if primary is not None else None,
                        "platform": images.platform,
                        "release": STRIMZI_RELEASE,
                    }
                ),
            ),
            ("cleanup.json", _canonical_json_bytes(asdict(cleanup_report))),
            ("timeline.json", _canonical_json_bytes(timeline)),
        )
        for name, payload in required_evidence:
            try:
                _evidence_write(
                    candidate,
                    name,
                    payload,
                    bundle.gate_contract.confidential_sentinels,
                )
            except Exception:
                capture_rejected = True

    stage_error: GateError | None = None
    try:
        _scan_and_stage_evidence(
            candidate,
            upload,
            () if capture_rejected else bundle.gate_contract.confidential_sentinels,
        )
    except GateError as error:
        stage_error = error
    try:
        shutil.rmtree(candidate)
    except OSError:
        if primary is None:
            primary = GateError("evidence_cleanup_failed", "Evidence candidate cleanup failed")
    if stage_error is not None:
        raise stage_error
    if primary is not None:
        raise primary
    _require(
        _cleanup_succeeded(cleanup_report),
        "cleanup_failed",
        "Disposable gate cleanup was incomplete",
    )
    return PILOT_PENDING_EXIT_CODE if arguments.pilot else 0


def _parse_args(argv: Sequence[str] | None = None) -> GateArguments:
    parser = argparse.ArgumentParser(description="Pinned Strimzi 1.2.0 acceptance gate")
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--evidence-candidate", type=Path)
    parser.add_argument("--evidence-upload", type=Path)
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parsed = parser.parse_args(argv)
    supplied = (parsed.wheel, parsed.evidence_candidate, parsed.evidence_upload)
    if parsed.self_test:
        _require(
            not parsed.pilot and all(value is None for value in supplied),
            "argument_invalid",
            "Self-test mode accepts no real-gate arguments",
        )
    else:
        _require(
            all(value is not None for value in supplied),
            "argument_invalid",
            "The wheel and both evidence directories are required",
        )
    return GateArguments(
        wheel=parsed.wheel,
        evidence_candidate=parsed.evidence_candidate,
        evidence_upload=parsed.evidence_upload,
        pilot=bool(parsed.pilot),
        self_test=bool(parsed.self_test),
    )


def _self_test() -> None:
    try:
        compile(_OFFLINE_GUARD_SOURCE, "sitecustomize.py", "exec")
    except SyntaxError as error:
        raise GateError("self_test_failed", "Offline guard source is invalid") from error
    _require(
        frozenset(_FIXTURE_SHA256) == _FIXTURE_NAMES
        and GLOBAL_TIMEOUT_SECONDS + CLEANUP_RESERVE_SECONDS < 30 * 60
        and CLEANUP_RESERVE_SECONDS >= 60 + 20 + 20 + 10 + 10
        and _DNS_LABEL.fullmatch("st-123456789abc") is not None,
        "self_test_failed",
        "Strimzi gate self-test failed",
    )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = _parse_args(argv)
        if arguments.self_test:
            _self_test()
            return 0
        return _execute_gate(arguments)
    except Exception:
        sys.stderr.write("ERROR: Strimzi acceptance gate failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
