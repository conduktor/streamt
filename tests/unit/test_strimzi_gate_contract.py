"""Docker-free, fail-closed contracts for the pinned Strimzi acceptance gate."""

from __future__ import annotations

import ast
import copy
import dataclasses
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_GATE_PATH = _ROOT / "tests" / "integration" / "strimzi_gate.py"
_TOPOLOGY_DIR = _ROOT / "tests" / "integration" / "strimzi" / "1.2.0"
_IMAGES_LOCK_PATH = _TOPOLOGY_DIR / "images.lock.json"
_OPERATOR_CONTRACT_PATH = _TOPOLOGY_DIR / "operator-contract.json"
_CONTRACT_PATH = _TOPOLOGY_DIR / "contract.json"
_KIND_PATH = _TOPOLOGY_DIR / "kind.yaml"
_KAFKA_PATH = _TOPOLOGY_DIR / "kafka.yaml"
_PROJECT_PATH = _TOPOLOGY_DIR / "stream_project.yml"

_EXPECTED_FILES = {
    "contract.json",
    "images.lock.json",
    "kafka.yaml",
    "kind.yaml",
    "operator-contract.json",
    "stream_project.yml",
}
_NAMESPACE_PLACEHOLDER = "streamt-strimzi-namespace-placeholder"
_CLUSTER_PLACEHOLDER = "streamt-strimzi-cluster-placeholder"
_OPERATOR_INDEX = "sha256:77f8fa8121a67561c3418de985783d197f51b8931e9a47f793dc0437dc6bb21f"
_KAFKA_INDEX = "sha256:e90a1a74af4226f3ca4d1ebef3ab13bdb09754ae17ca4c1444f7fcbb0ca8ea9a"
_NODE_INDEX = "sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
_PLATFORMS = {
    "linux/amd64": {
        "machine": "x86_64",
        "kind_sha256": "aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d",
        "kubectl_sha256": "874d5e72dbb819f43cff16bcd1e4f8bac5b7f2361fe1e55049b0a6c676fb0cbf",
        "node_manifest": "sha256:0c58cebbb66d7fa5fd497235dfae1e4e722ff84104e24a6f736ce8cd607cbe7c",
        "node_config": "sha256:194068f84949f79dca8527c1e0578d9cd90f0bcd82a359bdb0d2d5bfe9d61185",
        "operator_manifest": "sha256:6df3bf9f92d3d1907aca08ade8c6df6cdacd2e235756afad419ad582ce6a2c4e",
        "operator_config": "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3",
        "kafka_manifest": "sha256:63a2dd081b781951d1327626071760525734f4047acfb2d05b1a2878ad4135a5",
        "kafka_config": "sha256:d6950337889e76dec427c5ce1ec9c9b8de79e024fc2743c10884027db58d69cd",
    },
    "linux/arm64": {
        "machine": "aarch64",
        "kind_sha256": "20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2",
        "kubectl_sha256": "cc749967b62f4422260bc9c0aa7a7c55f45175ae38cb8d95767b5d2b7e04c1fd",
        "node_manifest": "sha256:b38a25576c835bfedc9d06368f87ec40863459a4d5dcbdbab2fd5f58ecf97466",
        "node_config": "sha256:664b3989afaffcd2268ece28d6cf012b27700e6b8e81c3c7641cc167889075f5",
        "operator_manifest": "sha256:ee8d9fb08ede3778120c33c42c70da16762b531d70e32790b9e2ff932e040927",
        "operator_config": "sha256:693db9e33a50f7cc1cd84cb763ee083c5209412b16f9f198ca013546da44f4f1",
        "kafka_manifest": "sha256:b82defb185ed5f91542a678108af5cce08ecc704c98615d43175d113f9f1be4a",
        "kafka_config": "sha256:2774fb129b66688c2d958e65a12349a8905e186466a124ccde1190742ba1454c",
    },
}
_OPERATOR_ASSET_SHA256 = "a0b1ae3e375a7da0674eb3894df8aa955bc50abe8190bc9344e30b83bbee4775"
_SINGLE_NODE_SHA256 = "2e7739e13dc250ccd00872bc6acf08dbf7fe768b9b76afcbef0dc733ede7b9ea"
_EPHEMERAL_SHA256 = "dd12c1e217e7ff348f5be81f9289a6f8c809db5bf4d5bb6b14e24ef7156d4930"
_SUBJECT_TARGETS = {
    ("ClusterRoleBinding", "strimzi-cluster-operator"),
    ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-broker-delegation"),
    ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-client-delegation"),
    ("RoleBinding", "strimzi-cluster-operator-watched"),
    ("RoleBinding", "strimzi-cluster-operator-entity-operator-delegation"),
    ("RoleBinding", "strimzi-cluster-operator"),
    ("RoleBinding", "strimzi-cluster-operator-leader-election"),
}
_NAMESPACED_TARGETS = {
    ("RoleBinding", "strimzi-cluster-operator-watched"),
    ("RoleBinding", "strimzi-cluster-operator-entity-operator-delegation"),
    ("RoleBinding", "strimzi-cluster-operator"),
    ("RoleBinding", "strimzi-cluster-operator-leader-election"),
    ("ServiceAccount", "strimzi-cluster-operator"),
    ("Deployment", "strimzi-cluster-operator"),
    ("ConfigMap", "strimzi-cluster-operator"),
}
_REWRITTEN_OPERATOR_ENVS = {
    "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE",
    "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE",
    "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE",
}
_REWRITTEN_KAFKA_ENVS = {
    "STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE",
    "STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE",
}
_REWRITTEN_KAFKA_MAPS = {
    "STRIMZI_KAFKA_IMAGES",
    "STRIMZI_KAFKA_CONNECT_IMAGES",
    "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES",
}
_REMOVED_IMAGE_ENVS = {
    "STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE",
    "STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE",
    "STRIMZI_DEFAULT_BUILDAH_IMAGE",
    "STRIMZI_DEFAULT_MAVEN_BUILDER",
}
_SAFE_SENTINELS = ("CONFIDENTIAL_SENTINEL",)


def _load_gate() -> ModuleType:
    spec = importlib.util.spec_from_file_location("streamt_test_strimzi_gate", _GATE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


gate = _load_gate()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _platform(machine: str = "x86_64", docker_os: str = "linux") -> Any:
    return gate._select_platform(gate._load_images_lock(_IMAGES_LOCK_PATH), docker_os, machine)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def _deployment(documents: tuple[dict[str, Any], ...] | list[dict[str, Any]]) -> dict[str, Any]:
    matches = [document for document in documents if document.get("kind") == "Deployment"]
    assert len(matches) == 1
    return matches[0]


def _operator_documents() -> tuple[dict[str, Any], ...]:
    """Small but structurally complete representation of the pinned 27 documents."""
    inventory = (
        ("ClusterRoleBinding", "strimzi-cluster-operator"),
        ("RoleBinding", "strimzi-cluster-operator-watched"),
        ("CustomResourceDefinition", "kafkaconnects.kafka.strimzi.io"),
        ("CustomResourceDefinition", "strimzipodsets.core.strimzi.io"),
        ("ClusterRole", "strimzi-entity-operator"),
        ("ClusterRole", "strimzi-kafka-broker"),
        ("ClusterRole", "strimzi-cluster-operator-watched"),
        ("RoleBinding", "strimzi-cluster-operator-entity-operator-delegation"),
        ("CustomResourceDefinition", "kafkas.kafka.strimzi.io"),
        ("ClusterRole", "strimzi-cluster-operator-global"),
        ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-broker-delegation"),
        ("RoleBinding", "strimzi-cluster-operator"),
        ("ServiceAccount", "strimzi-cluster-operator"),
        ("CustomResourceDefinition", "kafkarebalances.kafka.strimzi.io"),
        ("CustomResourceDefinition", "kafkanodepools.kafka.strimzi.io"),
        ("CustomResourceDefinition", "kafkatopics.kafka.strimzi.io"),
        ("ClusterRoleBinding", "strimzi-cluster-operator-kafka-client-delegation"),
        ("ClusterRole", "strimzi-kafka-client"),
        ("ClusterRole", "strimzi-cluster-operator-namespaced"),
        ("Deployment", "strimzi-cluster-operator"),
        ("CustomResourceDefinition", "kafkamirrormaker2s.kafka.strimzi.io"),
        ("ConfigMap", "strimzi-cluster-operator"),
        ("RoleBinding", "strimzi-cluster-operator-leader-election"),
        ("CustomResourceDefinition", "kafkabridges.kafka.strimzi.io"),
        ("ClusterRole", "strimzi-cluster-operator-leader-election"),
        ("CustomResourceDefinition", "kafkaconnectors.kafka.strimzi.io"),
        ("CustomResourceDefinition", "kafkausers.kafka.strimzi.io"),
    )
    documents: list[dict[str, Any]] = []
    for kind, name in inventory:
        if kind == "CustomResourceDefinition":
            api_version = "apiextensions.k8s.io/v1"
        elif kind == "Deployment":
            api_version = "apps/v1"
        elif kind in {"ServiceAccount", "ConfigMap"}:
            api_version = "v1"
        else:
            api_version = "rbac.authorization.k8s.io/v1"
        document: dict[str, Any] = {
            "apiVersion": api_version,
            "kind": kind,
            "metadata": {"name": name},
        }
        if (kind, name) in _SUBJECT_TARGETS:
            document["subjects"] = [
                {
                    "kind": "ServiceAccount",
                    "name": "strimzi-cluster-operator",
                    "namespace": "myproject",
                }
            ]
        if kind == "CustomResourceDefinition":
            document["spec"] = {
                "versions": [
                    {
                        "name": "v1",
                        "schema": {
                            "openAPIV3Schema": {
                                "type": "object",
                                "properties": {
                                    "spec": {
                                        "type": "object",
                                        "properties": {
                                            "image": {
                                                "type": "string",
                                                "description": "schema-image:latest",
                                            }
                                        },
                                    }
                                },
                            }
                        },
                    }
                ]
            }
        documents.append(document)

    deployment = _deployment(documents)
    deployment["apiVersion"] = "apps/v1"
    deployment["spec"] = {
        "template": {
            "spec": {
                "containers": [
                    {
                        "name": "strimzi-cluster-operator",
                        "image": "quay.io/strimzi/operator:1.2.0",
                        "env": [
                            {
                                "name": "STRIMZI_NAMESPACE",
                                "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}},
                            },
                            {
                                "name": "STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE",
                                "value": "quay.io/strimzi/kafka:1.2.0-kafka-4.3.1",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_CRUISE_CONTROL_IMAGE",
                                "value": "quay.io/strimzi/kafka:1.2.0-kafka-4.3.1",
                            },
                            {
                                "name": "STRIMZI_KAFKA_IMAGES",
                                "value": (
                                    "4.2.0=quay.io/strimzi/kafka:1.2.0-kafka-4.2.0\n"
                                    "4.2.1=quay.io/strimzi/kafka:1.2.0-kafka-4.2.1\n"
                                    "4.3.0=quay.io/strimzi/kafka:1.2.0-kafka-4.3.0\n"
                                    "4.3.1=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1\n"
                                ),
                            },
                            {
                                "name": "STRIMZI_KAFKA_CONNECT_IMAGES",
                                "value": (
                                    "4.2.0=quay.io/strimzi/kafka:1.2.0-kafka-4.2.0\n"
                                    "4.2.1=quay.io/strimzi/kafka:1.2.0-kafka-4.2.1\n"
                                    "4.3.0=quay.io/strimzi/kafka:1.2.0-kafka-4.3.0\n"
                                    "4.3.1=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1\n"
                                ),
                            },
                            {
                                "name": "STRIMZI_KAFKA_MIRROR_MAKER_2_IMAGES",
                                "value": (
                                    "4.2.0=quay.io/strimzi/kafka:1.2.0-kafka-4.2.0\n"
                                    "4.2.1=quay.io/strimzi/kafka:1.2.0-kafka-4.2.1\n"
                                    "4.3.0=quay.io/strimzi/kafka:1.2.0-kafka-4.3.0\n"
                                    "4.3.1=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1\n"
                                ),
                            },
                            {
                                "name": "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE",
                                "value": "quay.io/strimzi/operator:1.2.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_USER_OPERATOR_IMAGE",
                                "value": "quay.io/strimzi/operator:1.2.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_KAFKA_INIT_IMAGE",
                                "value": "quay.io/strimzi/operator:1.2.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE",
                                "value": "quay.io/strimzi/kafka-bridge:1.1.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE",
                                "value": "quay.io/strimzi/kaniko-executor:1.2.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_BUILDAH_IMAGE",
                                "value": "quay.io/strimzi/buildah:1.2.0",
                            },
                            {
                                "name": "STRIMZI_DEFAULT_MAVEN_BUILDER",
                                "value": "quay.io/strimzi/maven-builder:1.2.0",
                            },
                        ],
                    }
                ]
            }
        }
    }
    return tuple(documents)


def _container(document: Mapping[str, Any]) -> dict[str, Any]:
    return document["spec"]["template"]["spec"]["containers"][0]


def _assert_marker_only(upload_dir: Path) -> None:
    assert sorted(path.name for path in upload_dir.iterdir()) == [gate.SCAN_FAILURE_MARKER_NAME]
    assert (upload_dir / gate.SCAN_FAILURE_MARKER_NAME).read_bytes() == (
        gate.SCAN_FAILURE_MARKER_BYTES
    )
    assert upload_dir.stat().st_mode & 0o777 == 0o700
    assert upload_dir.joinpath(gate.SCAN_FAILURE_MARKER_NAME).stat().st_mode & 0o777 == 0o444
    assert list(upload_dir.parent.glob(f".{upload_dir.name}.stage-*")) == []


def test_gate_module_exists_and_topology_owns_exactly_six_files() -> None:
    assert _GATE_PATH.is_file()
    assert {path.name for path in _TOPOLOGY_DIR.iterdir() if path.is_file()} == _EXPECTED_FILES
    assert {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in _TOPOLOGY_DIR.iterdir()
        if path.is_file()
    } == {
        "contract.json": "9e9a2aeb47a58a2a09f10d79c61b85c69eaa5e824194327ff6b0adffa2af3917",
        "images.lock.json": "3c583c589ae345da58abec1e0484320c68d5e4d307439d62c811a13a9f7af83c",
        "kafka.yaml": "f33e0b3255aaf1257603a981de79a51b8afc6882e58023841da1c5028770b445",
        "kind.yaml": "3c1649c94e244ede76e3631d2b08656c3ad8a9e23b26a8d98f8174a55a0c4575",
        "operator-contract.json": (
            "7f9343e9aa04f70d91a39a4bf8db0ac6245810d1e2ac3e68309ef2014af19094"
        ),
        "stream_project.yml": ("fb3b4758b007125b1f7997a2f283d43b30312ac09bec52863c54de21aa1ebdbc"),
    }


def test_image_lock_has_exact_closed_provenance_and_platform_chains() -> None:
    value = _read_json(_IMAGES_LOCK_PATH)
    assert set(value) == {
        "schema_version",
        "strimzi_release",
        "strimzi_commit",
        "assets",
        "kind",
        "kubectl",
        "images",
    }
    assert value["schema_version"] == 1
    assert value["strimzi_release"] == "1.2.0"
    assert value["strimzi_commit"] == "6c7b43c4af0db547c10463ba09d1dfa6f5e156a0"

    assets = value["assets"]
    assert set(assets) == {"operator", "kafka_single_node", "kafka_ephemeral"}
    assert set(assets["operator"]) == {"url", "sha256", "documents"}
    assert assets["operator"] == {
        "url": (
            "https://github.com/strimzi/strimzi-kafka-operator/releases/download/"
            "1.2.0/strimzi-cluster-operator-1.2.0.yaml"
        ),
        "sha256": _OPERATOR_ASSET_SHA256,
        "documents": 27,
    }
    assert set(assets["kafka_single_node"]) == {"url", "sha256"}
    assert assets["kafka_single_node"] == {
        "url": (
            "https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/"
            "6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/"
            "examples/kafka/kafka-single-node.yaml"
        ),
        "sha256": _SINGLE_NODE_SHA256,
    }
    assert set(assets["kafka_ephemeral"]) == {"url", "sha256"}
    assert assets["kafka_ephemeral"] == {
        "url": (
            "https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/"
            "6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/"
            "examples/kafka/kafka-ephemeral.yaml"
        ),
        "sha256": _EPHEMERAL_SHA256,
    }

    kind = value["kind"]
    assert set(kind) == {"version", "annotated_tag", "commit", "binaries"}
    assert kind["version"] == "v0.33.0"
    assert kind["annotated_tag"] == "49aeee6b958d818ae881752fe5b09220b39b6f55"
    assert kind["commit"] == "407a9675e6d9af1200b5f57f9ca52ec6cdacce74"
    kubectl = value["kubectl"]
    assert set(kubectl) == {"version", "binaries"}
    assert kubectl["version"] == "v1.35.8"
    assert set(kind["binaries"]) == {"linux/amd64", "linux/arm64", "darwin/arm64"}
    assert set(kubectl["binaries"]) == {"linux/amd64", "linux/arm64", "darwin/arm64"}
    for platform_key in kind["binaries"]:
        assert set(kind["binaries"][platform_key]) == {"url", "sha256"}
        assert set(kubectl["binaries"][platform_key]) == {"url", "sha256"}
        operating_system, architecture = platform_key.split("/")
        assert kind["binaries"][platform_key]["url"] == (
            "https://github.com/kubernetes-sigs/kind/releases/download/"
            f"v0.33.0/kind-{operating_system}-{architecture}"
        )
        assert kubectl["binaries"][platform_key]["url"] == (
            f"https://dl.k8s.io/release/v1.35.8/bin/{operating_system}/{architecture}/kubectl"
        )
    assert kind["binaries"]["linux/amd64"]["sha256"] == _PLATFORMS["linux/amd64"]["kind_sha256"]
    assert kind["binaries"]["linux/arm64"]["sha256"] == _PLATFORMS["linux/arm64"]["kind_sha256"]
    assert kind["binaries"]["darwin/arm64"] == {
        "url": (
            "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-darwin-arm64"
        ),
        "sha256": "0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f",
    }
    assert (
        kubectl["binaries"]["linux/amd64"]["sha256"] == _PLATFORMS["linux/amd64"]["kubectl_sha256"]
    )
    assert (
        kubectl["binaries"]["linux/arm64"]["sha256"] == _PLATFORMS["linux/arm64"]["kubectl_sha256"]
    )
    assert kubectl["binaries"]["darwin/arm64"] == {
        "url": "https://dl.k8s.io/release/v1.35.8/bin/darwin/arm64/kubectl",
        "sha256": "b8be50ae0c6665b646fb009f904a52cad30806deee19ab3b4fe5af2d68bd82eb",
    }

    images = value["images"]
    assert set(images) == {"node", "operator", "kafka"}
    expected_names = {
        "node": ("kindest/node", "v1.35.8"),
        "operator": ("quay.io/strimzi/operator", "1.2.0"),
        "kafka": ("quay.io/strimzi/kafka", "1.2.0-kafka-4.3.1"),
    }
    expected_indexes = {"node": _NODE_INDEX, "operator": _OPERATOR_INDEX, "kafka": _KAFKA_INDEX}
    for image_name, image in images.items():
        assert set(image) == {"repository", "tag", "index", "platforms"}
        repository, tag = expected_names[image_name]
        assert (image["repository"], image["tag"]) == (repository, tag)
        expected_separator = ":v1.35.8@" if image_name == "node" else "@"
        assert image["index"] == (repository + expected_separator + expected_indexes[image_name])
        assert set(image["platforms"]) == set(_PLATFORMS)
        for platform_key, platform_value in image["platforms"].items():
            expected = _PLATFORMS[platform_key]
            expected_keys = {"manifest", "config"}
            if image_name != "node":
                expected_keys.add("kubernetes_image_id")
                expected_runtime_id = (
                    None
                    if platform_key == "linux/amd64"
                    else f"{repository}@{expected[f'{image_name}_manifest']}"
                )
                assert platform_value["kubernetes_image_id"] == expected_runtime_id
            assert set(platform_value) == expected_keys
            assert platform_value["manifest"] == expected[f"{image_name}_manifest"]
            assert platform_value["config"] == expected[f"{image_name}_config"]


def test_operator_contract_closes_all_27_documents_and_rewrite_targets() -> None:
    value = _read_json(_OPERATOR_CONTRACT_PATH)
    assert set(value) == {
        "schema_version",
        "document_count",
        "empty_document_count",
        "document_inventory",
        "deployment_image",
        "image_environment",
        "metadata_namespace_targets",
        "subject_namespace_targets",
        "forbidden_fixture_resources",
    }
    assert value["schema_version"] == 1
    assert value["document_count"] == 27
    assert value["empty_document_count"] == 1
    inventory = value["document_inventory"]
    assert len(inventory) == 27
    assert all(set(item) == {"index", "api_version", "kind", "name"} for item in inventory)
    expected_documents = _operator_documents()
    assert [
        (item["index"], item["api_version"], item["kind"], item["name"]) for item in inventory
    ] == [
        (
            index,
            document["apiVersion"],
            document["kind"],
            document["metadata"]["name"],
        )
        for index, document in enumerate(expected_documents)
    ]

    deployment = value["deployment_image"]
    assert set(deployment) == {
        "document_index",
        "container_name",
        "path",
        "source_value",
        "source_sha256",
        "rewrite_image",
        "image_pull_policy_path",
        "image_pull_policy_source",
        "image_pull_policy_value",
    }
    assert deployment["document_index"] == 19
    assert deployment["path"] == "/spec/template/spec/containers/0/image"
    assert deployment["source_value"] == "quay.io/strimzi/operator:1.2.0"
    assert (
        deployment["source_sha256"]
        == hashlib.sha256(deployment["source_value"].encode()).hexdigest()
    )
    assert deployment["rewrite_image"] == "operator"
    assert deployment["image_pull_policy_path"] == (
        "/spec/template/spec/containers/0/imagePullPolicy"
    )
    assert deployment["image_pull_policy_source"] is None
    assert deployment["image_pull_policy_value"] == "Never"

    environment = value["image_environment"]
    assert set(environment) == {
        "document_index",
        "container_name",
        "path",
        "classified_source_count",
        "rewrite_operator",
        "rewrite_kafka",
        "rewrite_kafka_version_maps",
        "remove",
        "add",
    }
    assert environment["classified_source_count"] == 12
    assert environment["document_index"] == 19
    assert environment["container_name"] == "strimzi-cluster-operator"
    assert environment["path"] == "/spec/template/spec/containers/0/env"
    operator_value = "quay.io/strimzi/operator:1.2.0"
    kafka_value = "quay.io/strimzi/kafka:1.2.0-kafka-4.3.1"
    kafka_map = (
        "4.2.0=quay.io/strimzi/kafka:1.2.0-kafka-4.2.0\n"
        "4.2.1=quay.io/strimzi/kafka:1.2.0-kafka-4.2.1\n"
        "4.3.0=quay.io/strimzi/kafka:1.2.0-kafka-4.3.0\n"
        "4.3.1=quay.io/strimzi/kafka:1.2.0-kafka-4.3.1\n"
    )
    removed_values = {
        "STRIMZI_DEFAULT_KAFKA_BRIDGE_IMAGE": "quay.io/strimzi/kafka-bridge:1.1.0",
        "STRIMZI_DEFAULT_KANIKO_EXECUTOR_IMAGE": ("quay.io/strimzi/kaniko-executor:1.2.0"),
        "STRIMZI_DEFAULT_BUILDAH_IMAGE": "quay.io/strimzi/buildah:1.2.0",
        "STRIMZI_DEFAULT_MAVEN_BUILDER": "quay.io/strimzi/maven-builder:1.2.0",
    }
    for item in environment["rewrite_operator"]:
        assert set(item) == {"name", "source_sha256"}
        assert item["source_sha256"] == hashlib.sha256(operator_value.encode()).hexdigest()
    for item in environment["rewrite_kafka"]:
        assert set(item) == {"name", "source_sha256"}
        assert item["source_sha256"] == hashlib.sha256(kafka_value.encode()).hexdigest()
    for item in environment["rewrite_kafka_version_maps"]:
        assert set(item) == {"name", "source_sha256", "version_keys"}
        assert item["source_sha256"] == hashlib.sha256(kafka_map.encode()).hexdigest()
        assert item["version_keys"] == ["4.2.0", "4.2.1", "4.3.0", "4.3.1"]
    for item in environment["remove"]:
        assert set(item) == {"name", "source_sha256"}
        assert (
            item["source_sha256"]
            == hashlib.sha256(removed_values[item["name"]].encode()).hexdigest()
        )
    assert {item["name"] for item in environment["rewrite_operator"]} == (_REWRITTEN_OPERATOR_ENVS)
    assert {item["name"] for item in environment["rewrite_kafka"]} == _REWRITTEN_KAFKA_ENVS
    assert {item["name"] for item in environment["rewrite_kafka_version_maps"]} == (
        _REWRITTEN_KAFKA_MAPS
    )
    assert {item["name"] for item in environment["remove"]} == _REMOVED_IMAGE_ENVS
    assert environment["add"] == {
        "name": "STRIMZI_IMAGE_PULL_POLICY",
        "source_count": 0,
        "value": "Never",
    }

    metadata_targets = value["metadata_namespace_targets"]
    assert len(metadata_targets) == 7
    assert all(set(item) == {"document_index", "kind", "name", "path"} for item in metadata_targets)
    assert [(item["document_index"], item["kind"], item["name"]) for item in metadata_targets] == [
        (index, document["kind"], document["metadata"]["name"])
        for index, document in enumerate(expected_documents)
        if (document["kind"], document["metadata"]["name"]) in _NAMESPACED_TARGETS
    ]
    assert {item["path"] for item in metadata_targets} == {"/metadata/namespace"}
    subject_targets = value["subject_namespace_targets"]
    assert len(subject_targets) == 7
    assert all(
        set(item) == {"document_index", "kind", "name", "path", "source_value"}
        for item in subject_targets
    )
    assert [(item["document_index"], item["kind"], item["name"]) for item in subject_targets] == [
        (index, document["kind"], document["metadata"]["name"])
        for index, document in enumerate(expected_documents)
        if (document["kind"], document["metadata"]["name"]) in _SUBJECT_TARGETS
    ]
    assert {item["path"] for item in subject_targets} == {"/subjects/0/namespace"}
    assert {item["source_value"] for item in subject_targets} == {"myproject"}

    forbidden = value["forbidden_fixture_resources"]
    assert set(forbidden) == {"kinds", "kafka_spec_fields", "entity_operator_fields"}
    assert set(forbidden["kinds"]) == {
        "KafkaBridge",
        "KafkaConnect",
        "KafkaConnector",
        "KafkaMirrorMaker",
        "KafkaMirrorMaker2",
        "KafkaRebalance",
        "KafkaUser",
    }
    assert forbidden["kafka_spec_fields"] == ["cruiseControl", "kafkaExporter"]
    assert forbidden["entity_operator_fields"] == ["userOperator"]


def test_kind_fixture_is_exactly_one_loopback_control_plane() -> None:
    value = yaml.safe_load(_KIND_PATH.read_text(encoding="utf-8"))
    assert value == {
        "kind": "Cluster",
        "apiVersion": "kind.x-k8s.io/v1alpha4",
        "networking": {"apiServerAddress": "127.0.0.1"},
        "nodes": [{"role": "control-plane"}],
    }


def test_kafka_fixture_is_exactly_single_node_dual_role_ephemeral_topic_only() -> None:
    raw = _KAFKA_PATH.read_text(encoding="utf-8")
    documents = tuple(yaml.safe_load_all(raw))
    assert len(documents) == 2
    pool, kafka = documents
    assert set(pool) == {"apiVersion", "kind", "metadata", "spec"}
    assert pool == {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "KafkaNodePool",
        "metadata": {
            "name": "dual-role",
            "namespace": _NAMESPACE_PLACEHOLDER,
            "labels": {"strimzi.io/cluster": _CLUSTER_PLACEHOLDER},
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
    assert set(kafka) == {"apiVersion", "kind", "metadata", "spec"}
    assert kafka["apiVersion"] == "kafka.strimzi.io/v1"
    assert kafka["kind"] == "Kafka"
    assert kafka["metadata"] == {
        "name": _CLUSTER_PLACEHOLDER,
        "namespace": _NAMESPACE_PLACEHOLDER,
    }
    assert set(kafka["spec"]) == {"kafka", "entityOperator"}
    assert kafka["spec"]["entityOperator"] == {
        "topicOperator": {
            "resources": {
                "requests": {"cpu": "100m", "memory": "128Mi"},
                "limits": {"cpu": "500m", "memory": "512Mi"},
            }
        }
    }
    broker = kafka["spec"]["kafka"]
    assert broker == {
        "version": "4.3.1",
        "metadataVersion": "4.3-IV0",
        "listeners": [{"name": "plain", "port": 9092, "type": "internal", "tls": False}],
        "config": {
            "offsets.topic.replication.factor": 1,
            "transaction.state.log.replication.factor": 1,
            "transaction.state.log.min.isr": 1,
            "default.replication.factor": 1,
            "min.insync.replicas": 1,
        },
    }
    assert "resources" not in broker
    assert raw.count(_NAMESPACE_PLACEHOLDER) == 2
    assert raw.count(_CLUSTER_PLACEHOLDER) == 2
    assert (
        gate._load_yaml_documents(
            _KAFKA_PATH,
            "f33e0b3255aaf1257603a981de79a51b8afc6882e58023841da1c5028770b445",
            2,
        )
        == documents
    )


def test_kind_fixture_passes_the_same_strict_digest_bound_yaml_loader() -> None:
    assert gate._load_yaml_documents(
        _KIND_PATH,
        "3c1649c94e244ede76e3631d2b08656c3ad8a9e23b26a8d98f8174a55a0c4575",
        1,
    ) == (yaml.safe_load(_KIND_PATH.read_text(encoding="utf-8")),)


def test_project_fixture_has_exact_two_topic_inputs_and_distinct_secrets() -> None:
    value = yaml.safe_load(_PROJECT_PATH.read_text(encoding="utf-8"))
    assert set(value) == {
        "apiVersion",
        "project",
        "runtime",
        "connections",
        "deployment_state",
        "models",
    }
    assert value["apiVersion"] == "streamt.dev/v1alpha1"
    assert value["project"] == {
        "name": "strimzi-real-gate",
        "version": "1.0.0",
        "description": "Deterministic Strimzi 1.2.0 real-cluster acceptance fixture",
    }
    assert set(value["runtime"]) == {"kafka"}
    assert value["runtime"]["kafka"] == {
        "bootstrap_servers": "strimzi-runtime-not-contacted.invalid:19092",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "strimzi-gate-user",
        "sasl_password": "STRIMZI_GATE_RUNTIME_PASSWORD_SECRET_91d4",
    }
    assert value["connections"] == {
        "unused": {
            "type": "opaque",
            "config": {
                "password": "STRIMZI_GATE_CONNECTION_PASSWORD_SECRET_29af",
                "sql": "SELECT 'STRIMZI_GATE_SQL_SECRET_36c8' AS private_marker",
            },
        }
    }
    assert value["deployment_state"] == {
        "backend": "postgres",
        "namespace": "strimzi-real-gate",
        "postgres": {"dsn_env": "STREAMT_STRIMZI_GATE_STATE_DSN"},
    }
    assert value["models"] == [
        {
            "name": "hashed-topic-owner",
            "materialized": "topic",
            "topic": {
                "name": "Orders_READY_v1",
                "partitions": 2,
                "replication_factor": 1,
                "config": {
                    "compression.type": "lz4",
                    "min.insync.replicas": 1,
                    "unclean.leader.election.enable": True,
                },
            },
        },
        {
            "name": "direct-topic-owner",
            "materialized": "topic",
            "tags": ["STRIMZI_GATE_TAG_SECRET_d84a"],
            "topic": {
                "name": "orders-ready-v1",
                "partitions": 3,
                "replication_factor": 1,
                "config": {
                    "cleanup.policy": "compact",
                    "delete.retention.ms": 86400000,
                    "remote.storage.enable": False,
                },
            },
        },
    ]


def test_gate_contract_is_closed_and_freezes_both_topic_identities() -> None:
    value = _read_json(_CONTRACT_PATH)
    assert set(value) == {
        "schema_version",
        "target_release",
        "api_version",
        "kind",
        "placeholders",
        "export",
        "confidential_input_sentinels",
        "environment_sentinels",
    }
    assert value["schema_version"] == 1
    assert value["target_release"] == "1.2.0"
    assert value["api_version"] == "kafka.strimzi.io/v1"
    assert value["kind"] == "KafkaTopic"
    assert value["placeholders"] == {
        "namespace": {
            "value": _NAMESPACE_PLACEHOLDER,
            "file": "kafka.yaml",
            "occurrences": 2,
            "replacement_pattern": "^st-[a-z0-9]{12}$",
        },
        "kafka_cluster": {
            "value": _CLUSTER_PLACEHOLDER,
            "file": "kafka.yaml",
            "occurrences": 2,
            "replacement_pattern": "^sk-[a-z0-9]{12}$",
        },
        "kind_cluster_pattern": "^streamt-strimzi-[a-z0-9]{12}-kind$",
        "docker_network_pattern": "^streamt-strimzi-[a-z0-9]{12}-net$",
    }
    export = value["export"]
    assert set(export) == {
        "project_name",
        "manifest_checksum",
        "topic_count",
        "warnings",
        "counts",
        "topics",
    }
    assert export["project_name"] == "strimzi-real-gate"
    assert export["manifest_checksum"] == (
        "sha256:589e61b12ea586cf0db22870eeed5ed3b6ee35fad7e0df042b769b0c55ae1c56"
    )
    assert export["topic_count"] == 2
    assert export["warnings"] == []
    assert export["counts"] == {
        "emitted_topics": 2,
        "external_topics_omitted": 0,
        "other_artifacts_omitted": 0,
    }
    assert export["topics"] == [
        {
            "topic_name": "Orders_READY_v1",
            "topic_name_sha256": (
                "a48756b0f1cc6f4a99afdf5092fd5dd5877abf2f0bb001dcc5d3348d2fb10214"
            ),
            "metadata_name": (
                "streamt-topic-a48756b0f1cc6f4a99afdf5092fd5dd5877abf2f0bb001dcc5d3348d2fb10214"
            ),
            "owner_name": "hashed-topic-owner",
            "owner_type": "model",
            "partitions": 2,
            "replicas": 1,
            "config": {
                "compression.type": "lz4",
                "min.insync.replicas": "1",
                "unclean.leader.election.enable": "true",
            },
        },
        {
            "topic_name": "orders-ready-v1",
            "topic_name_sha256": (
                "e97ad7095b74ba4a3f86b40183235028166073cbc28bbf5e324746696e1dfe74"
            ),
            "metadata_name": "orders-ready-v1",
            "owner_name": "direct-topic-owner",
            "owner_type": "model",
            "partitions": 3,
            "replicas": 1,
            "config": {
                "cleanup.policy": "compact",
                "delete.retention.ms": "86400000",
                "remote.storage.enable": "false",
            },
        },
    ]
    assert value["confidential_input_sentinels"] == [
        "strimzi-runtime-not-contacted.invalid:19092",
        "STRIMZI_GATE_RUNTIME_PASSWORD_SECRET_91d4",
        "STRIMZI_GATE_CONNECTION_PASSWORD_SECRET_29af",
        "STRIMZI_GATE_SQL_SECRET_36c8",
        "STRIMZI_GATE_TAG_SECRET_d84a",
        (
            "postgresql://STRIMZI_GATE_STATE_PASSWORD_SECRET_663b@"
            "state-not-contacted.invalid/streamt"
        ),
    ]
    assert value["environment_sentinels"] == {
        "STREAMT_STRIMZI_GATE_STATE_DSN": (
            "postgresql://STRIMZI_GATE_STATE_PASSWORD_SECRET_663b@"
            "state-not-contacted.invalid/streamt"
        )
    }
    assert len(set(value["confidential_input_sentinels"])) == 6


def test_json_loaders_reject_unknown_missing_and_duplicate_keys(tmp_path: Path) -> None:
    images = _read_json(_IMAGES_LOCK_PATH)
    extra = copy.deepcopy(images)
    extra["unexpected"] = True
    missing = copy.deepcopy(images)
    missing.pop("assets")
    for index, mutated in enumerate((extra, missing)):
        path = tmp_path / f"images-{index}.json"
        _write_json(path, mutated)
        with pytest.raises(gate.GateError):
            gate._load_images_lock(path)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    with pytest.raises(gate.GateError):
        gate._load_images_lock(duplicate)

    for loader, fixture in (
        (gate._load_operator_contract, _OPERATOR_CONTRACT_PATH),
        (gate._load_gate_contract, _CONTRACT_PATH),
    ):
        value = _read_json(fixture)
        value["unexpected"] = True
        path = tmp_path / f"extra-{fixture.name}"
        _write_json(path, value)
        with pytest.raises(gate.GateError):
            loader(path)


def test_contract_loaders_accept_only_the_frozen_live_contracts() -> None:
    images = gate._load_images_lock(_IMAGES_LOCK_PATH)
    operator = gate._load_operator_contract(_OPERATOR_CONTRACT_PATH, images)
    contract = gate._load_gate_contract(_CONTRACT_PATH)

    assert operator.document_count == 27
    assert operator.empty_document_count == 1
    assert operator.source_url == images.operator_source.url
    assert operator.source_sha256 == images.operator_source.sha256
    assert operator.document_inventory == tuple(
        (
            index,
            document["apiVersion"],
            document["kind"],
            document["metadata"]["name"],
        )
        for index, document in enumerate(_operator_documents())
    )
    assert contract.namespace == _NAMESPACE_PLACEHOLDER
    assert contract.cluster_name == _CLUSTER_PLACEHOLDER
    assert contract.manifest_checksum == (
        "sha256:589e61b12ea586cf0db22870eeed5ed3b6ee35fad7e0df042b769b0c55ae1c56"
    )
    assert [topic.topic_name for topic in contract.topics] == [
        "Orders_READY_v1",
        "orders-ready-v1",
    ]


@pytest.mark.parametrize(
    "mutation",
    ["missing", "extra", "duplicate", "reordered", "malformed"],
)
def test_operator_contract_rejects_nonexact_kafka_version_keys(
    tmp_path: Path,
    mutation: str,
) -> None:
    value = _read_json(_OPERATOR_CONTRACT_PATH)
    keys = value["image_environment"]["rewrite_kafka_version_maps"][0]["version_keys"]
    if mutation == "missing":
        keys.pop(0)
    elif mutation == "extra":
        keys.append("4.4.0")
    elif mutation == "duplicate":
        keys[1] = keys[0]
    elif mutation == "reordered":
        keys[0], keys[1] = keys[1], keys[0]
    else:
        keys[0] = 420
    path = tmp_path / f"operator-contract-{mutation}.json"
    _write_json(path, value)
    with pytest.raises(gate.GateError):
        gate._load_operator_contract(path, gate._load_images_lock(_IMAGES_LOCK_PATH))


def test_fixture_bundle_loader_binds_all_six_files_without_checkout_imports() -> None:
    bundle = gate._load_fixture_bundle(_TOPOLOGY_DIR)
    assert bundle.images_lock.strimzi_release == "1.2.0"
    assert bundle.operator_contract.document_count == 27
    assert bundle.gate_contract.project_name == "strimzi-real-gate"
    assert bundle.kind_document == yaml.safe_load(_KIND_PATH.read_bytes())
    assert bundle.kafka_documents == tuple(yaml.safe_load_all(_KAFKA_PATH.read_bytes()))
    assert bundle.project_bytes == _PROJECT_PATH.read_bytes()


def test_yaml_loader_checks_digest_count_and_trailing_empty_document(
    tmp_path: Path,
) -> None:
    content = (
        "\n---\n".join(
            f"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: item-{index}\n"
            for index in range(27)
        )
        + "---\n"
    )
    path = tmp_path / "operator.yaml"
    path.write_text(content, encoding="utf-8")
    digest = hashlib.sha256(content.encode()).hexdigest()

    assert len(gate._load_yaml_documents(path, digest, 27, expected_empty_count=1)) == 27
    with pytest.raises(gate.GateError):
        gate._load_yaml_documents(path, "0" * 64, 27, expected_empty_count=1)
    with pytest.raises(gate.GateError):
        gate._load_yaml_documents(path, digest, 26, expected_empty_count=1)
    with pytest.raises(gate.GateError):
        gate._load_yaml_documents(path, digest, 27, expected_empty_count=0)


@pytest.mark.parametrize(
    ("name", "content", "count", "empty_count"),
    [
        (
            "kind.yaml",
            "apiVersion: kind.x-k8s.io/v1alpha4\nkind: Cluster\nkind: Other\n",
            1,
            0,
        ),
        (
            "kafka.yaml",
            (
                "apiVersion: kafka.strimzi.io/v1\nkind: KafkaNodePool\n"
                "metadata:\n  name: pool\n---\n"
                "apiVersion: kafka.strimzi.io/v1\nkind: Kafka\nkind: Other\n"
                "metadata:\n  name: kafka\n"
            ),
            2,
            0,
        ),
        (
            "operator.yaml",
            (
                "apiVersion: apps/v1\nkind: Deployment\n"
                "metadata:\n  name: operator\n  name: changed\n---\n"
            )
            + "---\n".join(
                f"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: item-{index}\n"
                for index in range(26)
            )
            + "---\n",
            27,
            1,
        ),
        (
            "export.yaml",
            (
                "apiVersion: kafka.strimzi.io/v1\nkind: KafkaTopic\nmetadata:\n"
                "  name: topic\n  name: changed\nspec:\n  partitions: 1\n"
            ),
            1,
            0,
        ),
    ],
)
def test_yaml_loader_rejects_duplicate_keys_for_every_gate_stream(
    tmp_path: Path, name: str, content: str, count: int, empty_count: int
) -> None:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    with pytest.raises(gate.GateError):
        gate._load_yaml_documents(
            path,
            hashlib.sha256(content.encode()).hexdigest(),
            count,
            expected_empty_count=empty_count,
        )


def test_gate_source_has_no_streamt_import_or_import_time_entrypoint() -> None:
    source = _GATE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported.update(
        node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    )
    assert not {name for name in imported if name == "streamt" or name.startswith("streamt.")}
    assert 'if __name__ == "__main__":' in source


def test_embedded_offline_guard_is_valid_python() -> None:
    compile(gate._OFFLINE_GUARD_SOURCE, "sitecustomize.py", "exec")


def test_fresh_gate_import_loads_no_streamt_module_or_performs_work() -> None:
    program = f"""
import importlib.util
import json
import sys

path = {str(_GATE_PATH)!r}
before = set(sys.modules)
spec = importlib.util.spec_from_file_location("isolated_strimzi_gate", path)
assert spec is not None and spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
loaded = sorted(name for name in set(sys.modules) - before if name == "streamt" or name.startswith("streamt."))
print(json.dumps(loaded))
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-c", program],
        cwd=_ROOT.parent,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "[]\n"
    assert completed.stderr == ""


@pytest.mark.parametrize("machine", ["x86_64", "AMD64", "aarch64", "arm64"])
def test_platform_selection_is_closed_and_exact(machine: str) -> None:
    expected_key = "linux/amd64" if machine.lower() in {"x86_64", "amd64"} else "linux/arm64"
    expected = _PLATFORMS[expected_key]
    selected = _platform(machine)

    assert selected.platform == expected_key
    assert selected.node_index_reference.endswith(_NODE_INDEX)
    assert selected.node_manifest_digest == expected["node_manifest"]
    assert selected.node_config_digest == expected["node_config"]
    assert selected.operator_index_reference.endswith(_OPERATOR_INDEX)
    assert selected.operator_manifest_digest == expected["operator_manifest"]
    assert selected.operator_config_digest == expected["operator_config"]
    assert selected.operator_child_reference.endswith(expected["operator_manifest"])
    assert selected.kafka_index_reference.endswith(_KAFKA_INDEX)
    assert selected.kafka_manifest_digest == expected["kafka_manifest"]
    assert selected.kafka_config_digest == expected["kafka_config"]
    assert selected.kafka_child_reference.endswith(expected["kafka_manifest"])


@pytest.mark.parametrize("machine", ["", "i386", "ppc64le", "s390x", "amd64/evil"])
def test_platform_selection_rejects_every_unlocked_machine(machine: str) -> None:
    with pytest.raises(gate.GateError):
        _platform(machine)


def test_host_tool_and_docker_image_platforms_are_selected_independently() -> None:
    lock = gate._load_images_lock(_IMAGES_LOCK_PATH)

    local_tools = gate._select_host_tools(lock, "Darwin", "arm64")
    local_images = gate._select_platform(lock, "linux", "arm64")
    gate._validate_platform_combination(local_tools, local_images)
    assert local_tools.platform == "darwin/arm64"
    assert local_tools.kind.url.endswith("kind-darwin-arm64")
    assert "/darwin/arm64/kubectl" in local_tools.kubectl.url
    assert local_images.platform == "linux/arm64"

    ci_tools = gate._select_host_tools(lock, "Linux", "x86_64")
    ci_images = gate._select_platform(lock, "linux", "amd64")
    gate._validate_platform_combination(ci_tools, ci_images)
    assert ci_tools.platform == ci_images.platform == "linux/amd64"


@pytest.mark.parametrize(
    ("system", "machine", "docker_os", "docker_arch"),
    [
        ("windows", "amd64", "linux", "amd64"),
        ("darwin", "x86_64", "linux", "amd64"),
        ("linux", "amd64", "windows", "amd64"),
        ("linux", "amd64", "linux", "ppc64le"),
    ],
)
def test_unsupported_host_or_docker_platforms_fail_closed(
    system: str, machine: str, docker_os: str, docker_arch: str
) -> None:
    lock = gate._load_images_lock(_IMAGES_LOCK_PATH)

    def select() -> None:
        tools = gate._select_host_tools(lock, system, machine)
        images = gate._select_platform(lock, docker_os, docker_arch)
        gate._validate_platform_combination(tools, images)

    with pytest.raises(gate.GateError):
        select()


def test_host_and_docker_architecture_mismatch_fails_closed() -> None:
    lock = gate._load_images_lock(_IMAGES_LOCK_PATH)
    tools = gate._select_host_tools(lock, "darwin", "arm64")
    images = gate._select_platform(lock, "linux", "amd64")
    with pytest.raises(gate.GateError):
        gate._validate_platform_combination(tools, images)


def _frozen_platform() -> Any:
    images = _platform()
    return dataclasses.replace(
        images,
        operator_runtime_image_id=(
            "quay.io/strimzi/operator@"
            "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3"
        ),
        kafka_runtime_image_id=(
            "quay.io/strimzi/kafka@"
            "sha256:d6950337889e76dec427c5ce1ec9c9b8de79e024fc2743c10884027db58d69cd"
        ),
    )


def _cri_inventory(images: Any) -> dict[str, Any]:
    return {
        "images": [
            {
                "id": images.operator_config_digest,
                "repoTags": [],
                "repoDigests": [images.operator_child_reference],
                "size": "1",
            },
            {
                "id": images.kafka_config_digest,
                "repoTags": [],
                "repoDigests": [images.kafka_child_reference],
                "size": "1",
            },
        ]
    }


def _desktop_cri_inventory(images: Any) -> dict[str, Any]:
    inventory = _cri_inventory(images)
    for index, (entry, child_reference) in enumerate(
        zip(
            inventory["images"],
            (images.operator_child_reference, images.kafka_child_reference),
            strict=True,
        )
    ):
        child_digest = child_reference.rsplit("@", 1)[1]
        other_digest = "sha256:" + ("a" if index == 0 else "b") * 64
        entry["repoDigests"] = [
            f"docker.io/library/import-2026-09-04@{other_digest}",
            child_reference,
            f"docker.io/library/import-2026-09-04@{child_digest}",
        ]
    return inventory


def _ctr_import_key(child_reference: str, date: str = "2026-09-04") -> str:
    child_digest = child_reference.rsplit("@", 1)[1]
    return f"import-{date}@{child_digest}"


def _workload_pods(images: Any) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": [
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "operator"},
                "spec": {
                    "containers": [{"name": "operator", "image": images.operator_child_reference}]
                },
                "status": {
                    "containerStatuses": [
                        {
                            "name": "operator",
                            "image": images.operator_child_reference,
                            "imageID": images.operator_runtime_image_id,
                        }
                    ]
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "kafka"},
                "spec": {
                    "initContainers": [{"name": "init", "image": images.operator_child_reference}],
                    "containers": [{"name": "kafka", "image": images.kafka_child_reference}],
                },
                "status": {
                    "initContainerStatuses": [
                        {
                            "name": "init",
                            "image": images.operator_child_reference,
                            "imageID": images.operator_runtime_image_id,
                        }
                    ],
                    "containerStatuses": [
                        {
                            "name": "kafka",
                            "image": images.kafka_child_reference,
                            "imageID": images.kafka_runtime_image_id,
                        }
                    ],
                },
            },
        ],
    }


def _set_status_image_form(pods: dict[str, Any], images: Any, form: str) -> None:
    replacements = {
        images.operator_child_reference: images.operator_config_digest,
        images.kafka_child_reference: images.kafka_config_digest,
    }
    observed = 0
    for pod in pods["items"]:
        for status_key in ("initContainerStatuses", "containerStatuses"):
            for status in pod["status"].get(status_key, []):
                if form == "config" or (form == "mixed" and observed % 2 == 0):
                    status["image"] = replacements[status["image"]]
                else:
                    assert form in {"child", "mixed"}
                observed += 1


def test_ctr_image_normalization_commands_have_an_exact_bounded_surface() -> None:
    docker = Path("/locked/docker")
    node = "streamt-strimzi-123456789abc-kind-control-plane"
    images = _platform()
    source = _ctr_import_key(images.kafka_child_reference)
    outer = "import-2026-09-04@sha256:" + "a" * 64
    pair = gate.CtrImportPair(
        date="2026-09-04",
        child_source=source,
        child_digest=images.kafka_manifest_digest,
        outer_source=outer,
        outer_digest="sha256:" + "a" * 64,
    )

    assert gate._ctr_images_list_command(docker, node) == (
        str(docker),
        "exec",
        node,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "list",
        "-q",
    )
    assert gate._ctr_image_tag_command(docker, node, source, images.kafka_child_reference) == (
        str(docker),
        "exec",
        node,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "tag",
        source,
        images.kafka_child_reference,
    )
    assert gate._ctr_content_get_command(docker, node, pair.outer_digest) == (
        str(docker),
        "exec",
        node,
        "ctr",
        "--namespace=k8s.io",
        "content",
        "get",
        pair.outer_digest,
    )
    assert gate._ctr_images_remove_command(docker, node, pair) == (
        str(docker),
        "exec",
        node,
        "ctr",
        "--namespace=k8s.io",
        "images",
        "rm",
        pair.child_source,
        pair.outer_source,
    )
    assert gate._containerd_restart_command(docker, node) == (
        str(docker),
        "exec",
        node,
        "systemctl",
        "restart",
        "containerd",
    )
    assert "--sync" not in gate._ctr_images_remove_command(docker, node, pair)
    for invalid_source in (
        images.kafka_child_reference,
        f"docker.io/library/{source}",
        _ctr_import_key(images.operator_child_reference),
        _ctr_import_key(images.kafka_child_reference, "2026-02-30"),
    ):
        with pytest.raises(gate.GateError):
            gate._ctr_image_tag_command(
                docker,
                node,
                invalid_source,
                images.kafka_child_reference,
            )


def test_ctr_image_load_delta_accepts_only_frozen_classic_or_desktop_shapes() -> None:
    images = _platform()
    child = images.operator_child_reference
    unrelated = "registry.k8s.io/pause@sha256:" + "a" * 64
    before = gate._parse_ctr_image_names(f"{unrelated}\n".encode())
    classic = before | {child, images.operator_config_digest}
    assert (
        gate._classify_ctr_image_load(
            before,
            classic,
            child,
            images.operator_config_digest,
        )
        is None
    )

    imported = _ctr_import_key(child)
    outer = "import-2026-09-04@sha256:" + "b" * 64
    desktop = before | {imported, outer, images.operator_config_digest}
    assert gate._classify_ctr_image_load(
        before,
        desktop,
        child,
        images.operator_config_digest,
    ) == gate.CtrImportPair(
        date="2026-09-04",
        child_source=imported,
        child_digest=images.operator_manifest_digest,
        outer_source=outer,
        outer_digest="sha256:" + "b" * 64,
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "missing-config",
        "extra",
        "removed-baseline",
        "preexisting-target",
        "direct-with-import",
        "wrong-digest",
        "different-dates",
        "impossible-date",
        "docker-alias",
        "full-ref-import",
        "outer-is-config",
    ],
)
def test_ctr_image_load_delta_rejects_every_partial_or_ambiguous_shape(mutation: str) -> None:
    images = _platform()
    child = images.operator_child_reference
    config = images.operator_config_digest
    baseline = frozenset({"registry.k8s.io/pause@sha256:" + "a" * 64})
    child_import = _ctr_import_key(child)
    outer = "import-2026-09-04@sha256:" + "b" * 64
    before = baseline
    after = baseline | {child_import, outer, config}
    if mutation == "missing-config":
        after -= {config}
    elif mutation == "extra":
        after |= {"arbitrary"}
    elif mutation == "removed-baseline":
        after -= baseline
    elif mutation == "preexisting-target":
        before |= {child}
        after |= {child}
    elif mutation == "direct-with-import":
        after |= {child}
    elif mutation == "wrong-digest":
        after = baseline | {_ctr_import_key(child[:-64] + "f" * 64), outer, config}
    elif mutation == "different-dates":
        after = baseline | {child_import, outer.replace("2026-09-04", "2026-09-03"), config}
    elif mutation == "impossible-date":
        after = baseline | {
            _ctr_import_key(child, "2026-02-30"),
            outer.replace("2026-09-04", "2026-02-30"),
            config,
        }
    elif mutation == "docker-alias":
        after = baseline | {f"docker.io/library/{child_import}", outer, config}
    elif mutation == "full-ref-import":
        after = baseline | {f"import-2026-09-04@{child}", outer, config}
    else:
        assert mutation == "outer-is-config"
        after = baseline | {child_import, f"import-2026-09-04@{config}", config}
    with pytest.raises(gate.GateError):
        gate._classify_ctr_image_load(before, after, child, config)


@pytest.mark.parametrize(
    "output",
    [
        b"a\na\n",
        b"a\n\n",
        b" a\n",
        b"a\r\n",
        b"\xff\n",
        b"name\x00escape\n",
        b"name\vother\n",
        b"name\fother\n",
        b"name\x1cother\n",
        b"name\x1dother\n",
        b"name\x1eother\n",
        b"name\x1b[31m\n",
        b"name\tvalue\n",
        b"https://registry.invalid/image:tag\n",
        b"user:password@registry.invalid/image\n",
        b"registry.invalid/image?query\n",
        b"registry.invalid/image#fragment\n",
        b"registry.invalid\\image:tag\n",
        b"registry.invalid/image@latest\n",
        b"a" * 513 + b"\n",
    ],
)
def test_ctr_q_inventory_parser_rejects_ambiguous_bytes(output: bytes) -> None:
    with pytest.raises(gate.GateError):
        gate._parse_ctr_image_names(output)


def test_ctr_q_inventory_parser_keeps_unexpected_safe_references_for_diagnosis() -> None:
    digest = "sha256:" + "a" * 64
    assert gate._parse_ctr_image_names(
        f"unexpected.registry/example/image:tag\n{digest}\n".encode()
    ) == frozenset({"unexpected.registry/example/image:tag", digest})


def test_ctr_q_inventory_parser_enforces_reference_count_bound() -> None:
    output = "".join(f"registry.invalid/image-{index}:tag\n" for index in range(4_097)).encode()
    with pytest.raises(gate.GateError):
        gate._parse_ctr_image_names(output)


def test_ctr_load_diagnostic_canonicalizes_inventories_and_import_content() -> None:
    names = frozenset(
        {
            "quay.io/strimzi/operator@sha256:" + "b" * 64,
            "import-2026-09-04@sha256:" + "a" * 64,
        }
    )
    assert (
        gate._canonical_ctr_image_names(names)
        == (
            "import-2026-09-04@sha256:"
            + "a" * 64
            + "\nquay.io/strimzi/operator@sha256:"
            + "b" * 64
            + "\n"
        ).encode()
    )

    source = "import-2026-09-04@sha256:" + "a" * 64
    raw = b'{"z":1, "a":{"value":2}}\n'
    evidence = gate._ctr_import_content_evidence(
        source,
        gate.ProcessResult(0, raw, b""),
    )
    assert evidence == gate._canonical_json_bytes(
        {
            "content": {"a": {"value": 2}, "z": 1},
            "raw_content_sha256": f"sha256:{hashlib.sha256(raw).hexdigest()}",
            "source": source,
        }
    )


def test_ctr_load_diagnostic_records_a_content_addressed_manifest_index_chain() -> None:
    images = _platform()
    inner_raw = gate._canonical_json_bytes(
        {
            "config": {
                "digest": images.operator_config_digest,
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "size": 321,
            },
            "layers": [],
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "schemaVersion": 2,
        }
    )
    inner_digest = f"sha256:{hashlib.sha256(inner_raw).hexdigest()}"
    child_reference = f"quay.io/strimzi/operator@{inner_digest}"
    inner_source = f"import-2026-09-04@{inner_digest}"
    outer_raw = _ctr_outer_payload(child_reference)
    outer_digest = f"sha256:{hashlib.sha256(outer_raw).hexdigest()}"
    outer_source = f"import-2026-09-04@{outer_digest}"

    inner = json.loads(
        gate._ctr_import_content_evidence(
            inner_source,
            gate.ProcessResult(0, inner_raw, b""),
        )
    )
    outer = json.loads(
        gate._ctr_import_content_evidence(
            outer_source,
            gate.ProcessResult(0, outer_raw, b""),
        )
    )

    assert inner["source"].rsplit("@", 1)[1] == inner["raw_content_sha256"]
    assert inner["content"]["config"]["digest"] == images.operator_config_digest
    assert outer["source"].rsplit("@", 1)[1] == outer["raw_content_sha256"]
    assert outer["content"]["manifests"] == [
        {
            "annotations": {"containerd.io/distribution.source.quay.io": "strimzi/operator"},
            "digest": inner["raw_content_sha256"],
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "size": 123,
        }
    ]


def test_ctr_load_diagnostic_rejects_unsafe_or_ambiguous_import_content() -> None:
    source = "import-2026-09-04@sha256:" + "a" * 64
    invalid = (
        (source, gate.ProcessResult(9, b"{}", b"")),
        (source, gate.ProcessResult(0, b"{}", b"warning")),
        (source, gate.ProcessResult(0, b"not-json", b"")),
        (source, gate.ProcessResult(0, b'{"duplicate":1,"duplicate":2}', b"")),
        (source, gate.ProcessResult(0, b"[]", b"")),
        ("import-2026-02-30@sha256:" + "a" * 64, gate.ProcessResult(0, b"{}", b"")),
        (
            source,
            gate.ProcessResult(0, b" " * (gate.MAX_PROCESS_OUTPUT_BYTES + 1), b""),
        ),
    )
    for import_source, result in invalid:
        with pytest.raises(gate.GateError):
            gate._ctr_import_content_evidence(import_source, result)


def test_ctr_tag_result_requires_exact_direct_child_acknowledgement() -> None:
    child = _platform().kafka_child_reference
    gate._validate_ctr_tag_result(gate.ProcessResult(0, f"{child}\n".encode(), b""), child)

    for result in (
        gate.ProcessResult(9, f"{child}\n".encode(), b""),
        gate.ProcessResult(0, f"{child}\n".encode(), b"warning"),
        gate.ProcessResult(0, b"", b""),
        gate.ProcessResult(0, f"{child} extra\n".encode(), b""),
        gate.ProcessResult(0, f"{child}\n{child}\n".encode(), b""),
        gate.ProcessResult(0, b"\xff\n", b""),
    ):
        with pytest.raises(gate.GateError):
            gate._validate_ctr_tag_result(result, child)


def _ctr_outer_payload(child_reference: str, **descriptor_changes: Any) -> bytes:
    repository = child_reference.split("@", 1)[0].removeprefix("quay.io/")
    descriptor: dict[str, Any] = {
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "digest": child_reference.rsplit("@", 1)[1],
        "size": 123,
        "annotations": {"containerd.io/distribution.source.quay.io": repository},
    }
    descriptor.update(descriptor_changes)
    return json.dumps(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [descriptor],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def _ctr_pair_for_payload(child_reference: str, payload: bytes) -> Any:
    child_digest = child_reference.rsplit("@", 1)[1]
    outer_digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    return gate.CtrImportPair(
        date="2026-09-04",
        child_source=f"import-2026-09-04@{child_digest}",
        child_digest=child_digest,
        outer_source=f"import-2026-09-04@{outer_digest}",
        outer_digest=outer_digest,
    )


def test_ctr_outer_content_is_hash_bound_to_one_exact_child_descriptor() -> None:
    child = _platform().operator_child_reference
    payload = _ctr_outer_payload(child)
    pair = _ctr_pair_for_payload(child, payload)
    gate._validate_ctr_outer_content(gate.ProcessResult(0, payload, b""), pair, child)

    for changed in (
        _ctr_outer_payload(child, digest="sha256:" + "f" * 64),
        _ctr_outer_payload(child, size=True),
        _ctr_outer_payload(child, size=0),
        _ctr_outer_payload(child, annotations={}),
        _ctr_outer_payload(child, annotations={"extra": "value"}),
    ):
        with pytest.raises(gate.GateError):
            gate._validate_ctr_outer_content(
                gate.ProcessResult(0, changed, b""),
                _ctr_pair_for_payload(child, changed),
                child,
            )
    with pytest.raises(gate.GateError):
        gate._validate_ctr_outer_content(
            gate.ProcessResult(0, payload + b" ", b""),
            pair,
            child,
        )


def test_ctr_removal_and_normalized_inventory_require_exact_results() -> None:
    images = _platform()
    child = images.kafka_child_reference
    config = images.kafka_config_digest
    payload = _ctr_outer_payload(child)
    pair = _ctr_pair_for_payload(child, payload)
    baseline = frozenset({"system"})
    post_load = baseline | {pair.child_source, pair.outer_source, config}
    tagged = post_load | {child}
    gate._validate_ctr_pre_remove_names(post_load, tagged, child, pair)
    gate._validate_ctr_remove_result(
        gate.ProcessResult(0, f"{pair.child_source}\n{pair.outer_source}\n".encode(), b""),
        pair,
    )
    gate._validate_ctr_normalized_names(baseline, baseline | {child, config}, child, config)
    gate._validate_containerd_restart_result(gate.ProcessResult(0, b"", b""))

    with pytest.raises(gate.GateError):
        gate._validate_ctr_pre_remove_names(post_load, tagged | {"extra"}, child, pair)
    with pytest.raises(gate.GateError):
        gate._validate_ctr_remove_result(gate.ProcessResult(0, b"", b""), pair)
    with pytest.raises(gate.GateError):
        gate._validate_ctr_normalized_names(
            baseline,
            baseline | {child, config, pair.outer_source},
            child,
            config,
        )
    with pytest.raises(gate.GateError):
        gate._validate_containerd_restart_result(gate.ProcessResult(0, b"warning", b""))


def test_cri_mapping_accepts_only_normalized_target_only_records() -> None:
    images = _platform()
    gate._validate_cri_mappings(_cri_inventory(images), images)

    for mutation in (
        "wrong-config",
        "missing",
        "duplicate",
        "missing-repo-tags",
        "extra-tag",
        "extra-digest",
        "desktop-missing-child-alias",
        "desktop-missing-target",
        "desktop-different-dates",
        "desktop-invalid-date",
        "desktop-other-is-child",
        "docker-alias",
        "index-ref",
        "tag-only",
        "desktop-aliases",
    ):
        inventory = _cri_inventory(images)
        operator = inventory["images"][0]
        if mutation == "wrong-config":
            operator["id"] = images.operator_manifest_digest
        elif mutation == "missing":
            inventory["images"].pop(0)
        elif mutation == "duplicate":
            inventory["images"].append(copy.deepcopy(operator))
        elif mutation == "missing-repo-tags":
            del operator["repoTags"]
        elif mutation == "extra-tag":
            operator["repoTags"] = ["quay.io/strimzi/operator:latest"]
        elif mutation == "extra-digest":
            operator["repoDigests"].append("quay.io/strimzi/operator@sha256:" + "f" * 64)
        elif mutation == "desktop-aliases":
            operator["repoDigests"] = _desktop_cri_inventory(images)["images"][0]["repoDigests"]
        elif mutation == "docker-alias":
            operator["repoDigests"] = [f"docker.io/{images.operator_child_reference}"]
        elif mutation == "index-ref":
            operator["repoDigests"] = [images.operator_index_reference]
        else:
            operator["repoDigests"] = []
            operator["repoTags"] = ["arbitrary.local/operator:latest"]
        with pytest.raises(gate.GateError):
            gate._validate_cri_mappings(inventory, images)


@pytest.mark.parametrize("status_image_form", ["child", "config", "mixed"])
def test_workload_image_validation_accepts_both_exact_backend_status_forms(
    status_image_form: str,
) -> None:
    images = _frozen_platform()
    pods = _workload_pods(images)
    _set_status_image_form(pods, images, status_image_form)
    gate._validate_workload_images(pods, images)


def test_workload_image_validation_rejects_pilot_pending_and_nonexact_ids() -> None:
    pending = _platform()
    assert pending.pilot_pending
    with pytest.raises(gate.GateError):
        gate._validate_workload_images(_workload_pods(pending), pending)

    images = _frozen_platform()
    assert not images.pilot_pending

    for mutation in ("index", "wrong-id", "missing-status", "extra-image"):
        pods = _workload_pods(images)
        if mutation == "index":
            pods["items"][0]["spec"]["containers"][0]["image"] = images.operator_index_reference
        elif mutation == "wrong-id":
            pods["items"][1]["status"]["containerStatuses"][0]["imageID"] = (
                images.kafka_config_digest
            )
        elif mutation == "missing-status":
            pods["items"][1]["status"]["containerStatuses"] = []
        else:
            pods["items"][1]["spec"]["containers"].append(
                {"name": "extra", "image": "registry.invalid/extra:latest"}
            )
        with pytest.raises(gate.GateError):
            gate._validate_workload_images(pods, images)


@pytest.mark.parametrize("consumer", ["normal", "pilot"])
@pytest.mark.parametrize(
    "mutation",
    ["manifest", "index", "other-config", "tag", "missing", "non-string"],
)
def test_workload_status_image_rejects_every_unreviewed_backend_form(
    consumer: str,
    mutation: str,
) -> None:
    images = _frozen_platform() if consumer == "normal" else _platform()
    pods = _workload_pods(images) if consumer == "normal" else _pilot_pods(images)
    status = pods["items"][0]["status"]["containerStatuses"][0]
    if mutation == "manifest":
        status["image"] = images.operator_manifest_digest
    elif mutation == "index":
        status["image"] = images.operator_index_reference
    elif mutation == "other-config":
        status["image"] = images.kafka_config_digest
    elif mutation == "tag":
        status["image"] = "quay.io/strimzi/operator:1.2.0"
    elif mutation == "missing":
        status.pop("image")
    else:
        status["image"] = None

    validate = (
        gate._validate_workload_images if consumer == "normal" else gate._collect_pilot_image_ids
    )
    with pytest.raises(gate.GateError):
        validate(pods, images)


def _internal_services(cluster_name: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": [
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": f"{cluster_name}-kafka-bootstrap"},
                "spec": {"type": "ClusterIP", "ports": [{"port": 9092}]},
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": f"{cluster_name}-kafka-brokers"},
                "spec": {"type": "ClusterIP", "ports": [{"port": 9092}]},
            },
        ],
    }


def _raw_pod_collection(images: Any) -> dict[str, Any]:
    pods = _workload_pods(images)
    pods["kind"] = "List"
    pods["metadata"] = {"resourceVersion": ""}
    for pod in pods["items"]:
        pod["metadata"]["namespace"] = "st-123456789abc"
    return pods


def test_raw_kubectl_collections_normalize_only_after_exact_identity_validation() -> None:
    raw = _raw_pod_collection(_frozen_platform())
    assert len(gate._kubectl_collection_items(raw, item_kind="Pod", code="test")) == 2
    normalized = gate._combine_pod_lists(raw)
    assert normalized["apiVersion"] == "v1"
    assert normalized["kind"] == "PodList"
    assert normalized["metadata"] == {}
    assert normalized["items"] == raw["items"]

    services = _internal_services("sk-123456789abc")
    assert len(gate._kubectl_collection_items(services, item_kind="Service", code="test")) == 2


@pytest.mark.parametrize(
    "mutation",
    [
        "typed-list",
        "api-version",
        "metadata-empty",
        "metadata-nonempty",
        "metadata-nonstring",
        "metadata-extra",
        "root-extra",
        "root-missing",
        "items-not-list",
        "item-api-missing",
        "item-api-wrong",
        "item-kind-missing",
        "item-kind-wrong",
        "item-nonmapping",
    ],
)
def test_raw_kubectl_pod_collection_rejects_every_nonexact_envelope_or_item(
    mutation: str,
) -> None:
    raw = _raw_pod_collection(_frozen_platform())
    if mutation == "typed-list":
        raw["kind"] = "PodList"
    elif mutation == "api-version":
        raw["apiVersion"] = "v1beta1"
    elif mutation == "metadata-empty":
        raw["metadata"] = {}
    elif mutation == "metadata-nonempty":
        raw["metadata"]["resourceVersion"] = "123"
    elif mutation == "metadata-nonstring":
        raw["metadata"]["resourceVersion"] = 0
    elif mutation == "metadata-extra":
        raw["metadata"]["continue"] = "token"
    elif mutation == "root-extra":
        raw["continue"] = "token"
    elif mutation == "root-missing":
        raw.pop("metadata")
    elif mutation == "items-not-list":
        raw["items"] = {}
    elif mutation == "item-api-missing":
        raw["items"][0].pop("apiVersion")
    elif mutation == "item-api-wrong":
        raw["items"][0]["apiVersion"] = "apps/v1"
    elif mutation == "item-kind-missing":
        raw["items"][0].pop("kind")
    elif mutation == "item-kind-wrong":
        raw["items"][0]["kind"] = "Service"
    else:
        raw["items"][0] = "pod"

    with pytest.raises(gate.GateError):
        gate._combine_pod_lists(raw)


@pytest.mark.parametrize(
    "mutation",
    ["typed-list", "metadata", "item-api", "item-kind", "mixed-kind"],
)
def test_workload_exposure_rejects_nonexact_raw_service_collections(mutation: str) -> None:
    services = _internal_services("sk-123456789abc")
    if mutation == "typed-list":
        services["kind"] = "ServiceList"
    elif mutation == "metadata":
        services["metadata"] = {}
    elif mutation == "item-api":
        services["items"][0]["apiVersion"] = "apps/v1"
    elif mutation == "item-kind":
        services["items"][0]["kind"] = "Pod"
    else:
        services["items"][1]["kind"] = "Pod"

    with pytest.raises(gate.GateError):
        gate._validate_workload_exposure(
            _workload_pods(_frozen_platform()),
            services,
            cluster_name="sk-123456789abc",
        )


def test_workload_exposure_accepts_only_internal_pods_and_exact_services() -> None:
    gate._validate_workload_exposure(
        _workload_pods(_frozen_platform()),
        _internal_services("sk-123456789abc"),
        cluster_name="sk-123456789abc",
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "host-network",
        "host-pid",
        "host-ipc",
        "ephemeral-container",
        "container-host-port",
        "container-host-ip",
        "init-host-port",
        "missing-service",
        "duplicate-service",
        "extra-service",
        "node-port-service",
        "load-balancer-service",
        "external-name-service",
        "external-ip",
        "external-traffic-policy",
        "health-check-node-port",
        "load-balancer-class",
        "service-node-port",
        "empty-service-ports",
    ],
)
def test_workload_exposure_rejects_every_host_or_service_escape(mutation: str) -> None:
    pods = _workload_pods(_frozen_platform())
    services = _internal_services("sk-123456789abc")
    if mutation == "host-network":
        pods["items"][0]["spec"]["hostNetwork"] = True
    elif mutation == "host-pid":
        pods["items"][0]["spec"]["hostPID"] = True
    elif mutation == "host-ipc":
        pods["items"][0]["spec"]["hostIPC"] = True
    elif mutation == "ephemeral-container":
        pods["items"][0]["spec"]["ephemeralContainers"] = [
            {"name": "debug", "image": "busybox:latest"}
        ]
    elif mutation == "container-host-port":
        pods["items"][0]["spec"]["containers"][0]["ports"] = [
            {"containerPort": 8080, "hostPort": 8080}
        ]
    elif mutation == "container-host-ip":
        pods["items"][0]["spec"]["containers"][0]["ports"] = [
            {"containerPort": 8080, "hostIP": "127.0.0.1"}
        ]
    elif mutation == "init-host-port":
        pods["items"][1]["spec"]["initContainers"][0]["ports"] = [
            {"containerPort": 8080, "hostPort": 8080}
        ]
    elif mutation == "missing-service":
        services["items"].pop()
    elif mutation == "duplicate-service":
        services["items"].append(copy.deepcopy(services["items"][0]))
    elif mutation == "extra-service":
        extra = copy.deepcopy(services["items"][0])
        extra["metadata"]["name"] = "sk-123456789abc-extra"
        services["items"].append(extra)
    elif mutation == "node-port-service":
        services["items"][0]["spec"]["type"] = "NodePort"
    elif mutation == "load-balancer-service":
        services["items"][0]["spec"]["type"] = "LoadBalancer"
    elif mutation == "external-name-service":
        services["items"][0]["spec"]["type"] = "ExternalName"
    elif mutation == "external-ip":
        services["items"][0]["spec"]["externalIPs"] = ["203.0.113.1"]
    elif mutation == "external-traffic-policy":
        services["items"][0]["spec"]["externalTrafficPolicy"] = "Local"
    elif mutation == "health-check-node-port":
        services["items"][0]["spec"]["healthCheckNodePort"] = 30001
    elif mutation == "load-balancer-class":
        services["items"][0]["spec"]["loadBalancerClass"] = "example.invalid/lb"
    elif mutation == "service-node-port":
        services["items"][0]["spec"]["ports"][0]["nodePort"] = 30001
    else:
        services["items"][0]["spec"]["ports"] = []

    with pytest.raises(gate.GateError) as captured:
        gate._validate_workload_exposure(
            pods,
            services,
            cluster_name="sk-123456789abc",
        )
    assert captured.value.code == "workload_exposure_invalid"


def test_pilot_mode_is_allowed_only_for_a_fully_pending_runtime_id_lock() -> None:
    pending = _platform()
    frozen = _frozen_platform()
    gate._validate_pilot_mode(pending, True)
    gate._validate_pilot_mode(frozen, False)
    with pytest.raises(gate.GateError):
        gate._validate_pilot_mode(pending, False)
    with pytest.raises(gate.GateError):
        gate._validate_pilot_mode(frozen, True)

    partial = dataclasses.replace(
        pending,
        operator_runtime_image_id=(
            "quay.io/strimzi/operator@"
            "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3"
        ),
    )
    with pytest.raises(gate.GateError):
        gate._validate_pilot_mode(partial, False)
    with pytest.raises(gate.GateError):
        gate._validate_pilot_mode(partial, True)


def _pilot_pods(images: Any) -> dict[str, Any]:
    pods = _workload_pods(images)
    operator_id = (
        "quay.io/strimzi/operator@"
        "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3"
    )
    kafka_id = (
        "quay.io/strimzi/kafka@"
        "sha256:d6950337889e76dec427c5ce1ec9c9b8de79e024fc2743c10884027db58d69cd"
    )
    pods["items"][0]["status"]["containerStatuses"][0]["imageID"] = operator_id
    pods["items"][1]["status"]["initContainerStatuses"][0]["imageID"] = operator_id
    pods["items"][1]["status"]["containerStatuses"][0]["imageID"] = kafka_id
    return pods


@pytest.mark.parametrize("status_image_form", ["child", "config", "mixed"])
def test_pilot_collector_accepts_both_exact_backend_status_forms(
    status_image_form: str,
) -> None:
    images = _platform()
    pods = _pilot_pods(images)
    _set_status_image_form(pods, images, status_image_form)
    collected = gate._collect_pilot_image_ids(pods, images)
    assert set(collected["images"]) == {"operator", "kafka"}
    assert collected == gate._collect_pilot_image_ids(_pilot_pods(images), images)


def test_pilot_collector_requires_one_consistent_observed_id_per_child() -> None:
    images = _platform()
    collected = gate._collect_pilot_image_ids(_pilot_pods(images), images)
    assert collected == {
        "schema_version": 1,
        "platform": "linux/amd64",
        "images": {
            "operator": {
                "child_reference": images.operator_child_reference,
                "config_digest": images.operator_config_digest,
                "kubernetes_image_id": (
                    "quay.io/strimzi/operator@"
                    "sha256:307ebd6e0fd9121e0775b1cf0f06a5658cece38c58d46082512b910a7d095ce3"
                ),
            },
            "kafka": {
                "child_reference": images.kafka_child_reference,
                "config_digest": images.kafka_config_digest,
                "kubernetes_image_id": (
                    "quay.io/strimzi/kafka@"
                    "sha256:d6950337889e76dec427c5ce1ec9c9b8de79e024fc2743c10884027db58d69cd"
                ),
            },
        },
    }
    encoded = (json.dumps(collected, sort_keys=True, separators=(",", ":")) + "\n").encode()
    assert len(encoded) < gate.MAX_EVIDENCE_FILE_BYTES
    assert not any(sentinel.encode() in encoded for sentinel in _SAFE_SENTINELS)

    inconsistent = _pilot_pods(images)
    inconsistent["items"][1]["status"]["initContainerStatuses"][0]["imageID"] = (
        "quay.io/strimzi/operator@" + "sha256:" + "f" * 64
    )
    with pytest.raises(gate.GateError):
        gate._collect_pilot_image_ids(inconsistent, images)

    missing = _pilot_pods(images)
    missing["items"] = missing["items"][:1]
    with pytest.raises(gate.GateError):
        gate._collect_pilot_image_ids(missing, images)

    with pytest.raises(gate.GateError):
        gate._collect_pilot_image_ids(_workload_pods(_frozen_platform()), _frozen_platform())


def _network_inspect(prefix: str, *, attached: bool = False) -> list[dict[str, Any]]:
    containers: dict[str, Any] = {}
    if attached:
        containers["b" * 64] = {"Name": f"{prefix}-kind-control-plane"}
    return [
        {
            "Name": f"{prefix}-net",
            "Id": "a" * 64,
            "Driver": "bridge",
            "Internal": False,
            "Options": {"com.docker.network.bridge.enable_ip_masquerade": "false"},
            "Containers": containers,
        }
    ]


def _node_inspect(prefix: str, images: Any) -> list[dict[str, Any]]:
    return [
        {
            "Name": f"/{prefix}-kind-control-plane",
            "Config": {"Image": images.node_child_reference},
            "Image": images.node_config_digest,
            "NetworkSettings": {
                "Networks": {f"{prefix}-net": {"NetworkID": "a" * 64}},
                "Ports": {"6443/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49153"}]},
            },
        }
    ]


def test_docker_network_node_and_port_parsers_require_exact_isolation() -> None:
    prefix = "streamt-strimzi-123456789abc"
    network = f"{prefix}-net"
    images = _platform()
    assert gate._validate_docker_network(_network_inspect(prefix), network) == "a" * 64
    assert gate._validate_docker_network(_network_inspect(prefix), network, "a" * 64) == "a" * 64
    assert (
        gate._validate_docker_network(
            _network_inspect(prefix, attached=True),
            network,
            "a" * 64,
            expected_node=f"{prefix}-kind-control-plane",
        )
        == "a" * 64
    )
    assert gate._validate_docker_nodes(
        _node_inspect(prefix, images),
        prefix=prefix,
        network_name=network,
        network_id="a" * 64,
        images=images,
    ) == (f"{prefix}-kind-control-plane",)
    assert gate._validate_node_ports(_node_inspect(prefix, images)) == 49153
    containerd_node = _node_inspect(prefix, images)
    containerd_node[0]["Image"] = images.node_manifest_digest
    assert gate._validate_docker_nodes(
        containerd_node,
        prefix=prefix,
        network_name=network,
        network_id="a" * 64,
        images=images,
    ) == (f"{prefix}-kind-control-plane",)

    mutations = []
    internal = _network_inspect(prefix)
    internal[0]["Internal"] = True
    mutations.append((gate._validate_docker_network, (internal, network), {}))
    wrong_name = _network_inspect(prefix)
    wrong_name[0]["Name"] = "bridge"
    mutations.append((gate._validate_docker_network, (wrong_name, network), {}))
    wrong_driver = _network_inspect(prefix)
    wrong_driver[0]["Driver"] = "overlay"
    mutations.append((gate._validate_docker_network, (wrong_driver, network), {}))
    masquerading = _network_inspect(prefix)
    masquerading[0]["Options"]["com.docker.network.bridge.enable_ip_masquerade"] = "true"
    mutations.append((gate._validate_docker_network, (masquerading, network), {}))
    unknown_option = _network_inspect(prefix)
    unknown_option[0]["Options"]["com.docker.network.bridge.host_binding_ipv4"] = "127.0.0.1"
    mutations.append((gate._validate_docker_network, (unknown_option, network), {}))
    unexpected_attachment = _network_inspect(prefix)
    unexpected_attachment[0]["Containers"]["b" * 64] = {"Name": f"{prefix}-kind-control-plane"}
    mutations.append((gate._validate_docker_network, (unexpected_attachment, network), {}))
    wrong_attachment = _network_inspect(prefix, attached=True)
    wrong_attachment[0]["Containers"]["b" * 64]["Name"] = "other-node"
    mutations.append(
        (
            gate._validate_docker_network,
            (wrong_attachment, network, "a" * 64),
            {"expected_node": f"{prefix}-kind-control-plane"},
        )
    )
    missing_attachment = _network_inspect(prefix)
    mutations.append(
        (
            gate._validate_docker_network,
            (missing_attachment, network, "a" * 64),
            {"expected_node": f"{prefix}-kind-control-plane"},
        )
    )
    extra_attachment = _network_inspect(prefix, attached=True)
    extra_attachment[0]["Containers"]["c" * 64] = {"Name": f"{prefix}-kind-control-plane"}
    mutations.append(
        (
            gate._validate_docker_network,
            (extra_attachment, network, "a" * 64),
            {"expected_node": f"{prefix}-kind-control-plane"},
        )
    )
    invalid_attachment_id = _network_inspect(prefix, attached=True)
    attachment = invalid_attachment_id[0]["Containers"].pop("b" * 64)
    invalid_attachment_id[0]["Containers"]["not-a-container-id"] = attachment
    mutations.append(
        (
            gate._validate_docker_network,
            (invalid_attachment_id, network, "a" * 64),
            {"expected_node": f"{prefix}-kind-control-plane"},
        )
    )
    wrong_network = _node_inspect(prefix, images)
    wrong_network[0]["NetworkSettings"]["Networks"]["bridge"] = {"NetworkID": "b" * 64}
    mutations.append(
        (
            gate._validate_docker_nodes,
            (wrong_network,),
            {
                "prefix": prefix,
                "network_name": network,
                "network_id": "a" * 64,
                "images": images,
            },
        )
    )
    arbitrary_node_id = _node_inspect(prefix, images)
    arbitrary_node_id[0]["Image"] = "sha256:" + "f" * 64
    mutations.append(
        (
            gate._validate_docker_nodes,
            (arbitrary_node_id,),
            {
                "prefix": prefix,
                "network_name": network,
                "network_id": "a" * 64,
                "images": images,
            },
        )
    )
    wrong_child = _node_inspect(prefix, images)
    wrong_child[0]["Config"]["Image"] = images.node_index_reference
    mutations.append(
        (
            gate._validate_docker_nodes,
            (wrong_child,),
            {
                "prefix": prefix,
                "network_name": network,
                "network_id": "a" * 64,
                "images": images,
            },
        )
    )
    wrong_config = _node_inspect(prefix, images)
    wrong_config[0]["Image"] = _NODE_INDEX
    mutations.append(
        (
            gate._validate_docker_nodes,
            (wrong_config,),
            {
                "prefix": prefix,
                "network_name": network,
                "network_id": "a" * 64,
                "images": images,
            },
        )
    )
    wildcard = _node_inspect(prefix, images)
    wildcard[0]["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostIp"] = "0.0.0.0"
    mutations.append((gate._validate_node_ports, (wildcard,), {}))
    extra_port = _node_inspect(prefix, images)
    extra_port[0]["NetworkSettings"]["Ports"]["9092/tcp"] = [
        {"HostIp": "127.0.0.1", "HostPort": "9092"}
    ]
    mutations.append((gate._validate_node_ports, (extra_port,), {}))

    for validator, args, kwargs in mutations:
        with pytest.raises(gate.GateError):
            validator(*args, **kwargs)


def test_docker_network_options_accept_only_classic_or_desktop_exact_shapes() -> None:
    prefix = "streamt-strimzi-123456789abc"
    network_name = f"{prefix}-net"
    classic = {
        "com.docker.network.bridge.enable_ip_masquerade": "false",
    }
    desktop = {
        **classic,
        "com.docker.network.enable_ipv4": "true",
        "com.docker.network.enable_ipv6": "false",
    }
    assert (classic, desktop) == gate.DOCKER_NETWORK_OPTION_SETS
    for options in (classic, desktop):
        inspected = _network_inspect(prefix)
        inspected[0]["Options"] = options
        assert gate._validate_docker_network(inspected, network_name) == "a" * 64


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"com.docker.network.enable_ipv4": "true"},
        {"com.docker.network.enable_ipv6": "false"},
        {
            "com.docker.network.bridge.enable_ip_masquerade": "false",
            "com.docker.network.enable_ipv4": "true",
        },
        {
            "com.docker.network.bridge.enable_ip_masquerade": "false",
            "com.docker.network.enable_ipv6": "false",
        },
        {"com.docker.network.bridge.enable_ip_masquerade": "true"},
        {
            "com.docker.network.bridge.enable_ip_masquerade": "false",
            "com.docker.network.enable_ipv4": "false",
            "com.docker.network.enable_ipv6": "false",
        },
        {
            "com.docker.network.bridge.enable_ip_masquerade": "false",
            "com.docker.network.enable_ipv4": "true",
            "com.docker.network.enable_ipv6": "true",
        },
        {
            "com.docker.network.bridge.enable_ip_masquerade": False,
        },
        {
            "com.docker.network.bridge.enable_ip_masquerade": "false",
            "com.docker.network.enable_ipv4": "true",
            "com.docker.network.enable_ipv6": "false",
            "com.docker.network.bridge.host_binding_ipv4": "127.0.0.1",
        },
    ],
)
def test_docker_network_options_reject_partial_wrong_or_unknown_shapes(
    options: dict[str, Any],
) -> None:
    prefix = "streamt-strimzi-123456789abc"
    inspected = _network_inspect(prefix)
    inspected[0]["Options"] = options
    with pytest.raises(gate.GateError) as captured:
        gate._validate_docker_network(inspected, f"{prefix}-net")
    assert captured.value.code == "docker_network_invalid"


def test_non_masquerading_bridge_and_default_route_commands_are_exact() -> None:
    docker = Path("/usr/bin/docker")
    network = "streamt-strimzi-123456789abc-net"
    node = "streamt-strimzi-123456789abc-kind-control-plane"
    assert gate._docker_network_create_command(docker, network) == (
        "/usr/bin/docker",
        "network",
        "create",
        "--driver",
        "bridge",
        "--opt",
        "com.docker.network.bridge.enable_ip_masquerade=false",
        network,
    )
    assert gate._default_route_delete_command(docker, node) == (
        "/usr/bin/docker",
        "exec",
        node,
        "ip",
        "route",
        "del",
        "default",
    )
    assert gate._default_route_inventory_command(docker, node, family=4) == (
        "/usr/bin/docker",
        "exec",
        node,
        "ip",
        "-4",
        "route",
        "show",
        "default",
    )
    assert gate._default_route_inventory_command(docker, node, family=6) == (
        "/usr/bin/docker",
        "exec",
        node,
        "ip",
        "-6",
        "route",
        "show",
        "default",
    )


@pytest.mark.parametrize(
    ("ipv4", "ipv6"),
    [
        (gate.ProcessResult(1, b"", b""), gate.ProcessResult(0, b"", b"")),
        (gate.ProcessResult(0, b"default via 172.18.0.1\n", b""), gate.ProcessResult(0, b"", b"")),
        (gate.ProcessResult(0, b"", b""), gate.ProcessResult(0, b"default via fe80::1\n", b"")),
        (gate.ProcessResult(0, b"", b"warning"), gate.ProcessResult(0, b"", b"")),
        (gate.ProcessResult(0, b"\n", b""), gate.ProcessResult(0, b"", b"")),
    ],
)
def test_default_route_inventory_requires_exact_empty_dual_stack_results(
    ipv4: Any, ipv6: Any
) -> None:
    with pytest.raises(gate.GateError):
        gate._validate_empty_default_routes(ipv4, ipv6)

    gate._validate_empty_default_routes(
        gate.ProcessResult(0, b"", b""),
        gate.ProcessResult(0, b"", b""),
    )


def test_cleanup_targets_and_commands_are_derived_only_from_unique_prefix(
    tmp_path: Path,
) -> None:
    prefix = "streamt-strimzi-123456789abc"
    runtime = tmp_path / prefix
    runtime.mkdir()
    targets = gate._derive_cleanup_targets(prefix, f"{prefix}-kind", f"{prefix}-net", runtime)
    assert targets == gate.CleanupTargets(
        prefix=prefix,
        cluster_name=f"{prefix}-kind",
        network_name=f"{prefix}-net",
        node_name=f"{prefix}-kind-control-plane",
        runtime_dir=runtime,
    )
    commands: list[tuple[str, ...]] = []

    def runner(command: list[str], **_kwargs: Any) -> Any:
        commands.append(tuple(command))
        if command[1:3] == ["ps", "-a"]:
            return gate.ProcessResult(
                0,
                f"unrelated\n{prefix}-kind-control-plane\n".encode(),
                b"",
            )
        if command[1:3] == ["network", "ls"]:
            return gate.ProcessResult(0, f"{prefix}-net-leftover\n".encode(), b"")
        return gate.ProcessResult(0, b"", b"")

    report = gate._cleanup(
        targets,
        kind=Path("/locked/kind"),
        docker=Path("/usr/bin/docker"),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        runner=runner,
    )
    assert commands == [
        ("/locked/kind", "delete", "cluster", "--name", f"{prefix}-kind"),
        ("/usr/bin/docker", "network", "rm", f"{prefix}-net"),
        ("/usr/bin/docker", "ps", "-a", "--format", "{{.Names}}"),
        ("/usr/bin/docker", "network", "ls", "--format", "{{.Name}}"),
    ]
    assert report == gate.CleanupReport(
        cluster_delete_returncode=0,
        network_remove_returncode=0,
        residue_containers=(f"{prefix}-kind-control-plane",),
        residue_networks=(f"{prefix}-net-leftover",),
    )
    assert not runtime.exists()


@pytest.mark.parametrize(
    ("prefix", "cluster", "network", "runtime_name"),
    [
        ("short", "short-kind", "short-net", "short"),
        (
            "streamt-strimzi-123456789abc",
            "other-kind",
            "streamt-strimzi-123456789abc-net",
            "streamt-strimzi-123456789abc",
        ),
        (
            "streamt-strimzi-123456789abc",
            "streamt-strimzi-123456789abc-kind",
            "other-net",
            "streamt-strimzi-123456789abc",
        ),
        (
            "streamt-strimzi-123456789abc",
            "streamt-strimzi-123456789abc-kind",
            "streamt-strimzi-123456789abc-net",
            "other-runtime",
        ),
    ],
)
def test_cleanup_target_derivation_rejects_nonexact_names(
    tmp_path: Path, prefix: str, cluster: str, network: str, runtime_name: str
) -> None:
    with pytest.raises(gate.GateError):
        gate._derive_cleanup_targets(prefix, cluster, network, tmp_path / runtime_name)


def test_cleanup_aggregates_early_failure_and_clamps_every_later_step(
    tmp_path: Path,
) -> None:
    prefix = "streamt-strimzi-123456789abc"
    runtime = tmp_path / prefix
    runtime.mkdir()
    targets = gate._derive_cleanup_targets(prefix, f"{prefix}-kind", f"{prefix}-net", runtime)
    now = 0.0
    calls: list[tuple[tuple[str, ...], float]] = []

    def monotonic() -> float:
        return now

    def runner(command: list[str], **kwargs: Any) -> Any:
        nonlocal now
        timeout = kwargs["timeout_seconds"]
        calls.append((tuple(command), timeout))
        now += 5
        if len(calls) in {1, 2}:
            raise gate.GateError("subprocess_timeout", "bounded injected failure")
        if command[1:3] == ["ps", "-a"]:
            return gate.ProcessResult(0, f"{prefix}-kind-control-plane\n".encode(), b"")
        if command[1:3] == ["network", "ls"]:
            return gate.ProcessResult(0, f"{prefix}-net\n".encode(), b"")
        return gate.ProcessResult(7 if command[1:3] == ["network", "rm"] else 0, b"", b"")

    report = gate._cleanup(
        targets,
        kind=Path("/locked/kind"),
        docker=Path("/usr/bin/docker"),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        runner=runner,
        cleanup_deadline=25,
        monotonic=monotonic,
    )

    assert [call[0][1:3] for call in calls] == [
        ("delete", "cluster"),
        ("rm", "--force"),
        ("network", "rm"),
        ("ps", "-a"),
        ("network", "ls"),
    ]
    assert calls[1][0] == (
        "/usr/bin/docker",
        "rm",
        "--force",
        f"{prefix}-kind-control-plane",
    )
    assert [call[1] for call in calls] == [25, 20, 15, 10, 5]
    assert report.cluster_delete_returncode != 0
    assert report.node_remove_returncode != 0
    assert report.network_remove_returncode == 7
    assert report.residue_containers == (f"{prefix}-kind-control-plane",)
    assert report.residue_networks == (f"{prefix}-net",)
    assert not runtime.exists()


def test_cleanup_and_gate_time_budgets_leave_failure_upload_margin() -> None:
    assert gate.CLEANUP_RESERVE_SECONDS >= 60 + 20 + 20 + 10 + 10
    assert gate.GLOBAL_TIMEOUT_SECONDS + gate.CLEANUP_RESERVE_SECONDS < 30 * 60


def test_cleanup_enumeration_and_runtime_failures_are_aggregated_before_staging(
    tmp_path: Path,
) -> None:
    prefix = "streamt-strimzi-123456789abc"
    runtime = tmp_path / prefix
    runtime.mkdir()
    targets = gate._derive_cleanup_targets(prefix, f"{prefix}-kind", f"{prefix}-net", runtime)
    calls = 0

    def runner(command: list[str], **_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if command[1:3] == ["ps", "-a"]:
            raise gate.GateError("subprocess_timeout", "bounded enumeration failure")
        if command[1:3] == ["network", "ls"]:
            return gate.ProcessResult(4, b"", b"")
        return gate.ProcessResult(0, b"", b"")

    def remover(_path: Path) -> None:
        raise OSError("bounded removal failure")

    report = gate._cleanup(
        targets,
        kind=Path("/locked/kind"),
        docker=Path("/usr/bin/docker"),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        runner=runner,
        remover=remover,
        cleanup_deadline=time.monotonic() + 30,
    )
    assert calls == 4
    assert report.residue_containers == (
        f"{prefix}-container-enumeration-failed",
        f"{prefix}-runtime-removal-failed",
    )
    assert report.residue_networks == (f"{prefix}-network-enumeration-failed",)

    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "cleanup.json").write_text(
        json.dumps(dataclasses.asdict(report), sort_keys=True), encoding="utf-8"
    )
    assert gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS) == ("cleanup.json",)


def test_operator_rewriter_makes_the_exact_closed_transformation() -> None:
    source = _operator_documents()
    schema_before = copy.deepcopy(source[2]["spec"])
    images = _platform()
    contract = gate._load_operator_contract(
        _OPERATOR_CONTRACT_PATH, gate._load_images_lock(_IMAGES_LOCK_PATH)
    )

    rewritten = gate._rewrite_operator_documents(source, "strimzi-gate-123", images, contract)
    gate._validate_operator_closure(rewritten, "strimzi-gate-123", images, contract)

    assert len(rewritten) == 27
    assert source[19]["spec"]["template"]["spec"]["containers"][0]["image"] == (
        "quay.io/strimzi/operator:1.2.0"
    )
    assert rewritten[2]["spec"] == schema_before
    container = _container(_deployment(rewritten))
    assert container["image"] == images.operator_child_reference
    assert container["imagePullPolicy"] == "Never"
    environment = {item["name"]: item for item in container["env"]}
    assert not (_REMOVED_IMAGE_ENVS & environment.keys())
    for name in _REWRITTEN_OPERATOR_ENVS:
        assert environment[name] == {"name": name, "value": images.operator_child_reference}
    for name in _REWRITTEN_KAFKA_ENVS:
        assert environment[name] == {"name": name, "value": images.kafka_child_reference}
    for name in _REWRITTEN_KAFKA_MAPS:
        assert environment[name] == {
            "name": name,
            "value": "".join(
                f"{version}={images.kafka_child_reference}\n"
                for version in ("4.2.0", "4.2.1", "4.3.0", "4.3.1")
            ),
        }
    assert environment["STRIMZI_IMAGE_PULL_POLICY"] == {
        "name": "STRIMZI_IMAGE_PULL_POLICY",
        "value": "Never",
    }
    assert environment["STRIMZI_NAMESPACE"] == {
        "name": "STRIMZI_NAMESPACE",
        "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}},
    }

    identities = {(doc["kind"], doc["metadata"]["name"]): doc for doc in rewritten}
    assert {
        identity
        for identity, document in identities.items()
        if document["metadata"].get("namespace") == "strimzi-gate-123"
    } == _NAMESPACED_TARGETS
    assert {
        identity
        for identity, document in identities.items()
        if any(
            subject.get("namespace") == "strimzi-gate-123"
            for subject in document.get("subjects", [])
        )
    } == _SUBJECT_TARGETS


def _mutate_missing_env(documents: list[dict[str, Any]]) -> None:
    environment = _container(_deployment(documents))["env"]
    environment[:] = [
        item for item in environment if item["name"] != "STRIMZI_DEFAULT_MAVEN_BUILDER"
    ]


def _mutate_duplicate_env(documents: list[dict[str, Any]]) -> None:
    environment = _container(_deployment(documents))["env"]
    environment.append(copy.deepcopy(environment[-1]))


def _mutate_extra_env(documents: list[dict[str, Any]]) -> None:
    _container(_deployment(documents))["env"].append(
        {"name": "STRIMZI_UNEXPECTED_IMAGE", "value": "registry.invalid/extra:latest"}
    )


def _mutate_extra_container(documents: list[dict[str, Any]]) -> None:
    _deployment(documents)["spec"]["template"]["spec"]["initContainers"] = [
        {"name": "unexpected", "image": "registry.invalid/extra:latest"}
    ]


def _mutate_value_from_image(documents: list[dict[str, Any]]) -> None:
    environment = _container(_deployment(documents))["env"]
    target = next(
        item for item in environment if item["name"] == "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE"
    )
    target.pop("value")
    target["valueFrom"] = {"secretKeyRef": {"name": "bad", "key": "image"}}


@pytest.mark.parametrize(
    "mutator",
    [
        _mutate_missing_env,
        _mutate_duplicate_env,
        _mutate_extra_env,
        _mutate_extra_container,
        _mutate_value_from_image,
    ],
)
def test_operator_rewriter_rejects_missing_duplicate_extra_or_indirect_images(
    mutator: Callable[[list[dict[str, Any]]], None],
) -> None:
    documents = copy.deepcopy(list(_operator_documents()))
    mutator(documents)
    with pytest.raises(gate.GateError):
        gate._rewrite_operator_documents(documents, "strimzi-gate-123", _platform())


def test_operator_rewriter_rejects_divergent_namespace_and_does_not_mutate_input() -> None:
    documents = copy.deepcopy(list(_operator_documents()))
    target = next(
        document
        for document in documents
        if document["kind"] == "ServiceAccount"
        and document["metadata"]["name"] == "strimzi-cluster-operator"
    )
    target["metadata"]["namespace"] = "somewhere-else"
    before = copy.deepcopy(documents)

    with pytest.raises(gate.GateError):
        gate._rewrite_operator_documents(documents, "strimzi-gate-123", _platform())
    assert documents == before


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "extra", "identity"])
def test_operator_rewriter_rejects_nonexact_document_inventory(mutation: str) -> None:
    documents = copy.deepcopy(list(_operator_documents()))
    if mutation == "missing":
        documents.pop(4)
    elif mutation == "duplicate":
        documents.insert(5, copy.deepcopy(documents[4]))
    elif mutation == "extra":
        documents.append({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "extra"}})
    else:
        documents[4]["metadata"]["name"] = "changed"

    with pytest.raises(gate.GateError):
        gate._rewrite_operator_documents(documents, "st-123456789abc", _platform())


@pytest.mark.parametrize(
    "environment_name",
    [
        "STRIMZI_DEFAULT_TOPIC_OPERATOR_IMAGE",
        "STRIMZI_DEFAULT_KAFKA_EXPORTER_IMAGE",
        "STRIMZI_KAFKA_IMAGES",
        "STRIMZI_DEFAULT_MAVEN_BUILDER",
    ],
)
def test_operator_rewriter_checks_each_class_source_hash_before_rewriting(
    environment_name: str,
) -> None:
    documents = copy.deepcopy(list(_operator_documents()))
    environment = _container(_deployment(documents))["env"]
    target = next(item for item in environment if item["name"] == environment_name)
    target["value"] += "-changed"
    contract = gate._load_operator_contract(
        _OPERATOR_CONTRACT_PATH, gate._load_images_lock(_IMAGES_LOCK_PATH)
    )

    with pytest.raises(gate.GateError):
        gate._rewrite_operator_documents(documents, "st-123456789abc", _platform(), contract)


def test_source_kafka_version_map_parser_rejects_noncanonical_formatting() -> None:
    documents = _operator_documents()
    environment = _container(_deployment(documents))["env"]
    value = next(item for item in environment if item["name"] == "STRIMZI_KAFKA_IMAGES")["value"]
    gate._validate_source_kafka_version_map(value)
    for changed in (
        value.removesuffix("\n"),
        value.replace("\n", "\r\n"),
        value.replace("4.2.0=", " 4.2.0="),
        value.replace("\n4.2.1", "\n\n4.2.1"),
        value.replace("4.2.0=", "4.2.0=="),
    ):
        with pytest.raises(gate.GateError):
            gate._validate_source_kafka_version_map(changed)


@pytest.mark.parametrize("environment_name", sorted(_REWRITTEN_KAFKA_MAPS))
@pytest.mark.parametrize(
    "mutation",
    ["missing", "extra", "duplicate", "reordered", "malformed", "mutable"],
)
def test_operator_rewriter_rejects_nonexact_source_version_maps(
    environment_name: str,
    mutation: str,
) -> None:
    documents = copy.deepcopy(list(_operator_documents()))
    environment = _container(_deployment(documents))["env"]
    target = next(item for item in environment if item["name"] == environment_name)
    lines = target["value"].splitlines()
    if mutation == "missing":
        lines.pop(0)
    elif mutation == "extra":
        lines.append("4.4.0=quay.io/strimzi/kafka:1.2.0-kafka-4.4.0")
    elif mutation == "duplicate":
        lines[1] = lines[0]
    elif mutation == "reordered":
        lines[0], lines[1] = lines[1], lines[0]
    elif mutation == "malformed":
        lines[0] += "=extra"
    else:
        lines[0] = lines[0].replace("1.2.0", "latest")
    target["value"] = "\n".join(lines) + "\n"
    contract = gate._load_operator_contract(
        _OPERATOR_CONTRACT_PATH,
        gate._load_images_lock(_IMAGES_LOCK_PATH),
    )

    with pytest.raises(gate.GateError):
        gate._rewrite_operator_documents(
            documents,
            "st-123456789abc",
            _platform(),
            contract,
        )


@pytest.mark.parametrize("environment_name", sorted(_REWRITTEN_KAFKA_MAPS))
@pytest.mark.parametrize(
    "mutation",
    ["missing", "extra", "duplicate", "reordered", "malformed", "mutable"],
)
def test_operator_closure_rejects_nonexact_rewritten_version_maps(
    environment_name: str,
    mutation: str,
) -> None:
    images = _platform()
    contract = gate._load_operator_contract(
        _OPERATOR_CONTRACT_PATH,
        gate._load_images_lock(_IMAGES_LOCK_PATH),
    )
    rewritten = list(
        gate._rewrite_operator_documents(
            _operator_documents(),
            "st-123456789abc",
            images,
            contract,
        )
    )
    environment = _container(_deployment(rewritten))["env"]
    target = next(item for item in environment if item["name"] == environment_name)
    lines = target["value"].splitlines()
    if mutation == "missing":
        lines.pop(0)
    elif mutation == "extra":
        lines.append(f"4.4.0={images.kafka_child_reference}")
    elif mutation == "duplicate":
        lines[1] = lines[0]
    elif mutation == "reordered":
        lines[0], lines[1] = lines[1], lines[0]
    elif mutation == "malformed":
        lines[0] += "=extra"
    else:
        lines[0] = lines[0].replace(images.kafka_child_reference, images.kafka_index_reference)
    target["value"] = "\n".join(lines) + "\n"

    with pytest.raises(gate.GateError):
        gate._validate_operator_closure(
            rewritten,
            "st-123456789abc",
            images,
            contract,
        )


def test_kafka_fixture_renderer_makes_only_the_reviewed_transformations() -> None:
    source = tuple(yaml.safe_load_all(_KAFKA_PATH.read_text(encoding="utf-8")))
    before = copy.deepcopy(source)
    images = _platform()
    namespace = "st-123456789abc"
    cluster_name = "sk-123456789abc"

    rendered = gate._render_kafka_documents(source, namespace, cluster_name, images)
    gate._validate_kafka_fixture_closure(rendered, namespace, cluster_name, images)

    assert source == before
    assert len(rendered) == 2
    pool, kafka = rendered
    assert pool["metadata"] == {
        "name": "dual-role",
        "namespace": namespace,
        "labels": {"strimzi.io/cluster": cluster_name},
    }
    assert kafka["metadata"] == {"name": cluster_name, "namespace": namespace}
    assert kafka["spec"]["kafka"]["image"] == images.kafka_child_reference
    assert kafka["spec"]["entityOperator"] == before[1]["spec"]["entityOperator"]
    assert _NAMESPACE_PLACEHOLDER not in json.dumps(rendered)
    assert _CLUSTER_PLACEHOLDER not in json.dumps(rendered)


@pytest.mark.parametrize(
    "mutation",
    [
        "extra-kind",
        "cruise-control",
        "user-operator",
        "extra-image",
        "misplaced-resources",
        "missing-placeholder",
        "duplicate-placeholder",
        "kafka-version",
    ],
)
def test_kafka_fixture_renderer_rejects_every_unreviewed_resource_or_field(
    mutation: str,
) -> None:
    source = copy.deepcopy(list(yaml.safe_load_all(_KAFKA_PATH.read_text(encoding="utf-8"))))
    if mutation == "extra-kind":
        source.append(
            {
                "apiVersion": "kafka.strimzi.io/v1",
                "kind": "KafkaUser",
                "metadata": {"name": "user"},
                "spec": {},
            }
        )
    elif mutation == "cruise-control":
        source[1]["spec"]["cruiseControl"] = {}
    elif mutation == "user-operator":
        source[1]["spec"]["entityOperator"]["userOperator"] = {}
    elif mutation == "extra-image":
        source[0]["spec"]["template"] = {"pod": {"imagePullSecrets": [{"name": "unexpected"}]}}
    elif mutation == "misplaced-resources":
        source[1]["spec"]["kafka"]["resources"] = source[0]["spec"].pop("resources")
    elif mutation == "missing-placeholder":
        source[0]["metadata"]["namespace"] = "already-rendered"
    elif mutation == "kafka-version":
        source[1]["spec"]["kafka"]["version"] = "4.2.0"
    else:
        source[0]["metadata"]["extra"] = _NAMESPACE_PLACEHOLDER

    with pytest.raises(gate.GateError):
        gate._render_kafka_documents(source, "st-123456789abc", "sk-123456789abc", _platform())


def _expected_export_documents(
    contract: Any, namespace: str, cluster_name: str
) -> tuple[dict[str, Any], ...]:
    documents = []
    for topic in contract.topics:
        documents.append(
            {
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
        )
    return tuple(documents)


def test_export_document_validator_closes_direct_and_hashed_topic_shapes() -> None:
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    namespace = "st-123456789abc"
    cluster_name = "sk-123456789abc"
    documents = _expected_export_documents(contract, namespace, cluster_name)

    assert (
        gate._validate_export_documents(documents, contract, namespace, cluster_name) == documents
    )
    assert documents[0]["spec"]["topicName"] == "Orders_READY_v1"
    assert documents[0]["metadata"]["name"].startswith("streamt-topic-a48756")
    assert documents[1]["spec"]["topicName"] == documents[1]["metadata"]["name"]


@pytest.mark.parametrize(
    "mutation",
    ["missing", "duplicate", "extra-field", "reordered", "wrong-config", "wrong-hash"],
)
def test_export_document_validator_rejects_nonexact_stream(mutation: str) -> None:
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    namespace = "st-123456789abc"
    cluster_name = "sk-123456789abc"
    documents = copy.deepcopy(list(_expected_export_documents(contract, namespace, cluster_name)))
    if mutation == "missing":
        documents.pop()
    elif mutation == "duplicate":
        documents.append(copy.deepcopy(documents[0]))
    elif mutation == "extra-field":
        documents[0]["metadata"]["annotations"]["unexpected"] = "value"
    elif mutation == "reordered":
        documents.reverse()
    elif mutation == "wrong-config":
        documents[1]["spec"]["config"]["cleanup.policy"] = "delete"
    else:
        documents[0]["metadata"]["name"] = "streamt-topic-" + "0" * 64

    with pytest.raises(gate.GateError):
        gate._validate_export_documents(documents, contract, namespace, cluster_name)


def test_both_topic_applications_use_the_same_fixed_server_side_manager() -> None:
    kubectl = Path("/locked/kubectl")
    kubeconfig = Path("/private/runtime/kubeconfig")
    manifest = Path("/private/runtime/topics.yaml")
    base = (
        "/locked/kubectl",
        "--kubeconfig",
        "/private/runtime/kubeconfig",
        "apply",
        "--server-side=true",
        "--field-manager=streamt-strimzi-gate",
        "--filename",
        "/private/runtime/topics.yaml",
    )
    dry_run = gate._topic_apply_command(kubectl, kubeconfig, manifest, dry_run=True)
    first_apply = gate._topic_apply_command(kubectl, kubeconfig, manifest, dry_run=False)
    replay_apply = gate._topic_apply_command(kubectl, kubeconfig, manifest, dry_run=False)

    assert gate.TOPIC_FIELD_MANAGER == "streamt-strimzi-gate"
    assert dry_run == (*base, "--dry-run=server")
    assert first_apply == replay_apply == base
    assert all("save-config" not in argument for argument in (*dry_run, *first_apply))


def test_process_runner_preserves_channels_and_bounds_output(tmp_path: Path) -> None:
    result = gate.run_process(
        [sys.executable, "-c", "import sys; sys.stdout.write('out'); sys.stderr.write('err')"],
        cwd=tmp_path,
        environment={"PATH": os.environ.get("PATH", "")},
        timeout_seconds=5,
        max_output_bytes=32,
    )
    assert result.returncode == 0
    assert result.stdout == b"out"
    assert result.stderr == b"err"

    with pytest.raises(gate.GateError):
        gate.run_process(
            [sys.executable, "-c", "print('x' * 65)"],
            cwd=tmp_path,
            environment={"PATH": os.environ.get("PATH", "")},
            timeout_seconds=5,
            max_output_bytes=64,
        )


@pytest.mark.parametrize(
    ("command", "timeout", "limit"),
    [
        (("python", "-V"), 1.0, 64),
        ((sys.executable, "-V"), 0.0, 64),
        ((sys.executable, "-V"), gate.GLOBAL_TIMEOUT_SECONDS + 1, 64),
        ((sys.executable, "-V"), 1.0, 0),
        ((sys.executable, "-V"), 1.0, gate.MAX_PROCESS_OUTPUT_BYTES + 1),
    ],
)
def test_process_runner_rejects_unbounded_or_relative_invocations(
    tmp_path: Path, command: tuple[str, ...], timeout: float, limit: int
) -> None:
    with pytest.raises(gate.GateError):
        gate.run_process(
            command,
            cwd=tmp_path,
            environment={"PATH": os.environ.get("PATH", "")},
            timeout_seconds=timeout,
            max_output_bytes=limit,
        )


def test_locked_redirect_allows_exactly_one_github_release_asset_hop() -> None:
    source_url = (
        "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64"
    )
    handler = gate._LockedRedirect(source_url)
    source = urllib.request.Request(source_url)
    target = (
        "https://release-assets.githubusercontent.com/github-production-release-asset/"
        "123/kind-linux-amd64?sp=r&sig=opaque"
    )
    redirected = handler.redirect_request(source, None, 302, "Found", {}, target)
    assert redirected.full_url == target
    assert handler.count == 1

    with pytest.raises(gate.GateError):
        handler.redirect_request(redirected, None, 302, "Found", {}, target)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        (
            "https://raw.githubusercontent.com/owner/repo/commit/file",
            "https://release-assets.githubusercontent.com/file",
        ),
        (
            "https://github.com/owner/repo/releases/download/v1/file",
            "https://evil.invalid/file",
        ),
        (
            "https://github.com/owner/repo/releases/download/v1/file",
            "http://release-assets.githubusercontent.com/file",
        ),
        (
            "https://github.com/owner/repo/releases/download/v1/file",
            "https://user:password@release-assets.githubusercontent.com/file",
        ),
        (
            "https://github.com/owner/repo/releases/download/v1/file",
            "https://release-assets.githubusercontent.com/file#fragment",
        ),
    ],
)
def test_locked_redirect_rejects_every_other_hop(source: str, target: str) -> None:
    handler = gate._LockedRedirect(source)
    with pytest.raises(gate.GateError):
        handler.redirect_request(urllib.request.Request(source), None, 302, "Found", {}, target)


def test_locked_redirect_rejects_a_different_github_source_path() -> None:
    expected = "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-amd64"
    handler = gate._LockedRedirect(expected)
    with pytest.raises(gate.GateError):
        handler.redirect_request(
            urllib.request.Request(
                "https://github.com/kubernetes-sigs/kind/releases/download/v0.33.0/kind-linux-arm64"
            ),
            None,
            302,
            "Found",
            {},
            "https://release-assets.githubusercontent.com/file",
        )


class _FakeDownloadResponse:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._read = False

    def __enter__(self) -> _FakeDownloadResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def getcode(self) -> int:
        return 200

    def read(self, _size: int) -> bytes:
        if self._read:
            return b""
        self._read = True
        return self._payload


class _FakeDownloadOpener:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def open(self, *_args: object, **_kwargs: object) -> _FakeDownloadResponse:
        return _FakeDownloadResponse(self._payload)


def test_download_verifier_checks_exact_digest_bounds_cleanup_and_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"locked payload"
    asset = gate.Download(
        "https://dl.k8s.io/release/v1.35.8/bin/linux/amd64/kubectl",
        hashlib.sha256(payload).hexdigest(),
    )
    destination = tmp_path / "kubectl"
    gate._download_verified(
        asset, destination, executable=True, opener=_FakeDownloadOpener(payload)
    )
    assert destination.read_bytes() == payload
    assert destination.stat().st_mode & 0o777 == 0o700

    wrong = tmp_path / "wrong"
    with pytest.raises(gate.GateError):
        gate._download_verified(
            dataclasses.replace(asset, sha256="0" * 64),
            wrong,
            opener=_FakeDownloadOpener(payload),
        )
    assert not wrong.exists()

    monkeypatch.setattr(gate, "MAX_DOWNLOAD_BYTES", len(payload) - 1)
    oversized = tmp_path / "oversized"
    with pytest.raises(gate.GateError):
        gate._download_verified(asset, oversized, opener=_FakeDownloadOpener(payload))
    assert not oversized.exists()


def test_json_and_yaml_subprocess_parsers_reject_duplicates_and_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert gate._parse_json_bytes(b'{"ok":true}', code="probe_invalid") == {"ok": True}
    with pytest.raises(gate.GateError):
        gate._parse_json_bytes(b'{"ok":true,"ok":false}', code="probe_invalid")
    with pytest.raises(gate.GateError):
        gate._parse_yaml_bytes(b"name: first\nname: second\n", expected_count=1)
    with pytest.raises(gate.GateError):
        gate._parse_yaml_bytes(b"base: &base value\ncopy: *base\n", expected_count=1)
    for index, constant in enumerate((b"NaN", b"Infinity", b"-Infinity")):
        payload = b'{"value":' + constant + b"}"
        with pytest.raises(gate.GateError):
            gate._parse_json_bytes(payload, code="probe_invalid")
        path = tmp_path / f"non-finite-{index}.json"
        path.write_bytes(payload)
        with pytest.raises(gate.GateError):
            gate._load_json(path)
    with pytest.raises(gate.GateError):
        gate._canonical_json_bytes({"value": float("nan")})

    monkeypatch.setattr(gate, "MAX_PROCESS_OUTPUT_BYTES", 2)
    with pytest.raises(gate.GateError):
        gate._parse_json_bytes(b"{}\n", code="probe_invalid")


def test_index_child_and_local_config_validators_close_image_identity_chain() -> None:
    images = _platform()
    index = {
        "manifests": [
            {
                "digest": images.node_manifest_digest,
                "platform": {"os": "linux", "architecture": "amd64"},
            },
            {
                "digest": _PLATFORMS["linux/arm64"]["node_manifest"],
                "platform": {"os": "linux", "architecture": "arm64"},
            },
        ]
    }
    gate._validate_index_manifest(
        index,
        platform="linux/amd64",
        expected_child_digest=images.node_manifest_digest,
    )
    for manifest_media_type, config_media_type in (
        (
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.docker.container.image.v1+json",
        ),
        (
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.oci.image.config.v1+json",
        ),
    ):
        gate._validate_child_manifest(
            {
                "schemaVersion": 2,
                "mediaType": manifest_media_type,
                "config": {
                    "digest": images.node_config_digest,
                    "mediaType": config_media_type,
                    "size": 123,
                },
                "layers": [],
            },
            expected_config_digest=images.node_config_digest,
        )
    assert (
        gate._validate_local_image(
            [
                {
                    "Id": images.node_config_digest,
                    "RepoDigests": [images.node_child_reference],
                    "Os": "linux",
                    "Architecture": "amd64",
                }
            ],
            child_reference=images.node_child_reference,
            child_digest=images.node_manifest_digest,
            config_digest=images.node_config_digest,
            platform="linux/amd64",
        )
        == "classic"
    )
    assert (
        gate._validate_local_image(
            [
                {
                    "Id": images.node_manifest_digest,
                    "RepoDigests": [images.node_child_reference],
                    "Os": "linux",
                    "Architecture": "amd64",
                    "Descriptor": {
                        "digest": images.node_manifest_digest,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "size": 456,
                    },
                }
            ],
            child_reference=images.node_child_reference,
            child_digest=images.node_manifest_digest,
            config_digest=images.node_config_digest,
            platform="linux/amd64",
        )
        == "containerd"
    )

    for changed in ("missing", "wrong-child", "duplicate-platform"):
        mutated = copy.deepcopy(index)
        if changed == "missing":
            mutated["manifests"] = mutated["manifests"][1:]
        elif changed == "wrong-child":
            mutated["manifests"][0]["digest"] = images.node_config_digest
        else:
            mutated["manifests"].append(copy.deepcopy(mutated["manifests"][0]))
        with pytest.raises(gate.GateError):
            gate._validate_index_manifest(
                mutated,
                platform="linux/amd64",
                expected_child_digest=images.node_manifest_digest,
            )

    with pytest.raises(gate.GateError):
        gate._validate_local_image(
            [
                {
                    "Id": images.node_manifest_digest,
                    "RepoDigests": [images.node_child_reference],
                    "Os": "linux",
                    "Architecture": "amd64",
                }
            ],
            child_reference=images.node_child_reference,
            child_digest=images.node_manifest_digest,
            config_digest=images.node_config_digest,
            platform="linux/amd64",
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "schema",
        "index-body",
        "missing-config",
        "index-config",
        "arbitrary-config",
        "config-shape",
    ],
)
def test_child_manifest_rejects_every_nonexact_config_identity(mutation: str) -> None:
    images = _platform()
    manifest: Any = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "digest": images.node_config_digest,
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 123,
        },
        "layers": [],
    }
    if mutation == "schema":
        manifest["schemaVersion"] = 1
    elif mutation == "index-body":
        manifest["mediaType"] = "application/vnd.oci.image.index.v1+json"
        manifest["manifests"] = []
    elif mutation == "missing-config":
        manifest.pop("config")
    elif mutation == "index-config":
        manifest["config"]["digest"] = _NODE_INDEX
    elif mutation == "arbitrary-config":
        manifest["config"]["digest"] = "sha256:" + "f" * 64
    else:
        manifest["config"] = images.node_config_digest

    with pytest.raises(gate.GateError):
        gate._validate_child_manifest(
            manifest,
            expected_config_digest=images.node_config_digest,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "index-id",
        "index-backend",
        "arbitrary-id",
        "child-without-descriptor",
        "child-wrong-descriptor",
        "child-config-descriptor",
        "classic-with-descriptor",
        "missing-repodigest",
        "index-repodigest",
        "extra-repodigest",
        "wrong-os",
        "wrong-architecture",
        "duplicate-record",
    ],
)
def test_local_image_rejects_every_backend_identity_mismatch(mutation: str) -> None:
    images = _platform()
    value: list[dict[str, Any]] = [
        {
            "Id": images.node_config_digest,
            "RepoDigests": [images.node_child_reference],
            "Os": "linux",
            "Architecture": "amd64",
        }
    ]
    image = value[0]
    if mutation == "index-id":
        image["Id"] = _NODE_INDEX
    elif mutation == "index-backend":
        image["Id"] = _NODE_INDEX
        image["Descriptor"] = {"digest": _NODE_INDEX}
    elif mutation == "arbitrary-id":
        image["Id"] = "sha256:" + "f" * 64
    elif mutation == "child-without-descriptor":
        image["Id"] = images.node_manifest_digest
    elif mutation == "child-wrong-descriptor":
        image["Id"] = images.node_manifest_digest
        image["Descriptor"] = {"digest": "sha256:" + "f" * 64}
    elif mutation == "child-config-descriptor":
        image["Id"] = images.node_manifest_digest
        image["Descriptor"] = {"digest": images.node_config_digest}
    elif mutation == "classic-with-descriptor":
        image["Descriptor"] = {"digest": images.node_manifest_digest}
    elif mutation == "missing-repodigest":
        image["RepoDigests"] = []
    elif mutation == "index-repodigest":
        image["RepoDigests"] = [images.node_index_reference]
    elif mutation == "extra-repodigest":
        image["RepoDigests"].append("registry.invalid/extra@" + "sha256:" + "f" * 64)
    elif mutation == "wrong-os":
        image["Os"] = "darwin"
    elif mutation == "wrong-architecture":
        image["Architecture"] = "arm64"
    else:
        value.append(copy.deepcopy(image))

    with pytest.raises(gate.GateError):
        gate._validate_local_image(
            value,
            child_reference=images.node_child_reference,
            child_digest=images.node_manifest_digest,
            config_digest=images.node_config_digest,
            platform="linux/amd64",
        )


@pytest.mark.parametrize("backend", ["classic", "containerd"])
def test_image_chain_uses_only_builtin_docker_commands_and_exact_locked_identities(
    tmp_path: Path, backend: str
) -> None:
    images = _platform()
    identities = {
        images.node_index_reference: (
            images.node_manifest_digest,
            images.node_child_reference,
            images.node_config_digest,
        ),
        images.operator_index_reference: (
            images.operator_manifest_digest,
            images.operator_child_reference,
            images.operator_config_digest,
        ),
        images.kafka_index_reference: (
            images.kafka_manifest_digest,
            images.kafka_child_reference,
            images.kafka_config_digest,
        ),
    }
    by_child = {child: (manifest, config) for manifest, child, config in identities.values()}
    commands: list[tuple[str, ...]] = []
    timeouts: list[float] = []

    def runner(command: list[str], **kwargs: Any) -> Any:
        encoded = tuple(command)
        commands.append(encoded)
        timeouts.append(kwargs["timeout_seconds"])
        if encoded[1:3] == ("manifest", "inspect"):
            if encoded[3] in identities:
                manifest, _child, _config = identities[encoded[3]]
                payload = {
                    "manifests": [
                        {
                            "digest": manifest,
                            "platform": {"os": "linux", "architecture": "amd64"},
                        }
                    ]
                }
            else:
                _manifest, config = by_child[encoded[3]]
                payload = {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "config": {
                        "digest": config,
                        "mediaType": "application/vnd.oci.image.config.v1+json",
                        "size": 123,
                    },
                    "layers": [],
                }
            return gate.ProcessResult(
                0,
                json.dumps(payload).encode(),
                b"",
            )
        if encoded[1:3] == ("image", "inspect"):
            manifest, config = by_child[encoded[3]]
            assert encoded[3].endswith(manifest)
            image = {
                "Id": config if backend == "classic" else manifest,
                "RepoDigests": [encoded[3]],
                "Os": "linux",
                "Architecture": "amd64",
            }
            if backend == "containerd":
                image["Descriptor"] = {
                    "digest": manifest,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "size": 456,
                }
            return gate.ProcessResult(
                0,
                json.dumps([image]).encode(),
                b"",
            )
        return gate.ProcessResult(0, b"", b"")

    gate._verify_image_chain(
        Path("/usr/bin/docker"),
        images,
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=1_000,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert len(commands) == 12
    for index, (index_reference, (_manifest, child, _config)) in enumerate(identities.items()):
        assert commands[index * 4 : index * 4 + 4] == [
            ("/usr/bin/docker", "manifest", "inspect", index_reference),
            ("/usr/bin/docker", "pull", "--platform", "linux/amd64", child),
            ("/usr/bin/docker", "manifest", "inspect", child),
            ("/usr/bin/docker", "image", "inspect", child),
        ]
    assert timeouts == [60, 180, 60, 30] * 3
    assert not any("buildx" in command for command in commands)


def test_kind_experimental_network_warnings_are_present_exactly_once() -> None:
    first, second = gate._KIND_NETWORK_WARNINGS
    gate._validate_kind_network_warnings(
        gate.ProcessResult(0, f"{first}\n".encode(), f"{second}\n".encode())
    )
    for result in (
        gate.ProcessResult(0, f"{first}\n".encode(), b""),
        gate.ProcessResult(0, f"{first}\n{first}\n".encode(), f"{second}\n".encode()),
        gate.ProcessResult(0, b"\xff", f"{second}\n".encode()),
    ):
        with pytest.raises(gate.GateError):
            gate._validate_kind_network_warnings(result)


def test_real_input_resolution_requires_one_regular_wheel_and_fresh_distinct_paths(
    tmp_path: Path,
) -> None:
    wheel = tmp_path / "streamt.whl"
    wheel.write_bytes(b"wheel")
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    arguments = gate.GateArguments(wheel, candidate, upload, False, False)
    assert gate._resolve_real_inputs(arguments) == (wheel, candidate, upload)

    symlink = tmp_path / "linked.whl"
    symlink.symlink_to(wheel)
    invalid = (
        dataclasses.replace(arguments, wheel=symlink),
        dataclasses.replace(arguments, wheel=tmp_path / "missing.whl"),
        dataclasses.replace(arguments, wheel=tmp_path / "wrong.txt"),
        dataclasses.replace(arguments, evidence_upload=candidate),
    )
    (tmp_path / "wrong.txt").write_bytes(b"not-wheel")
    for value in invalid:
        with pytest.raises(gate.GateError):
            gate._resolve_real_inputs(value)

    upload.mkdir()
    with pytest.raises(gate.GateError):
        gate._resolve_real_inputs(arguments)


def test_docker_server_platform_is_read_from_server_not_python_host() -> None:
    lock = gate._load_images_lock(_IMAGES_LOCK_PATH)
    commands: list[tuple[str, ...]] = []

    def runner(command: list[str], **_kwargs: Any) -> Any:
        commands.append(tuple(command))
        return gate.ProcessResult(0, b'{"OSType":"linux","Architecture":"arm64"}', b"")

    selected = gate._docker_server_images(lock, Path("/usr/bin/docker"), runner=runner)
    assert selected.platform == "linux/arm64"
    assert commands == [
        ("/usr/bin/docker", "info", "--format", "{{json .}}"),
    ]


def test_deadline_helpers_clamp_every_process_and_fail_after_deadline(
    tmp_path: Path,
) -> None:
    assert gate._deadline_timeout(15, 10, lambda: 3) == 10
    assert gate._deadline_timeout(8, 10, lambda: 3) == 5
    with pytest.raises(gate.GateError):
        gate._deadline_timeout(3, 10, lambda: 3)

    seen: list[float] = []

    def runner(_command: list[str], **kwargs: Any) -> Any:
        seen.append(kwargs["timeout_seconds"])
        return gate.ProcessResult(0, b"", b"")

    gate._run_checked_before(
        [sys.executable, "-V"],
        cwd=tmp_path,
        environment={},
        deadline=8,
        cap=10,
        runner=runner,
        monotonic=lambda: 3,
    )
    assert seen == [5]


def test_guarded_export_runs_twice_with_exact_installed_cli_surface(
    tmp_path: Path,
) -> None:
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    documents = _expected_export_documents(contract, "st-123456789abc", "sk-123456789abc")
    payload = gate._yaml_stream_bytes(documents)
    guard_dir = tmp_path / "guard"
    guard_dir.mkdir()
    project = tmp_path / "stream_project.yml"
    project.write_bytes(_PROJECT_PATH.read_bytes())
    output_one = tmp_path / "one.yaml"
    output_two = tmp_path / "two.yaml"
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def runner(command: list[str], **kwargs: Any) -> Any:
        environment = kwargs["environment"]
        calls.append((tuple(command), dict(environment)))
        Path(environment["STREAMT_STRIMZI_GUARD_MARKER"]).write_bytes(b"active\n")
        output = Path(command[command.index("--output-file") + 1])
        output.write_bytes(payload)
        return gate.ProcessResult(0, b"", b"")

    first, validated = gate._export_twice(
        Path("/private/venv/bin/streamt"),
        project=project,
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        output_one=output_one,
        output_two=output_two,
        cwd=tmp_path,
        base_environment={"HOME": str(tmp_path)},
        guard_dir=guard_dir,
        contract=contract,
        deadline=100,
        runner=runner,
        monotonic=lambda: 0,
    )
    assert first == payload
    assert validated == documents
    assert len(calls) == 2
    for index, (command, environment) in enumerate(calls, start=1):
        assert command == (
            "/private/venv/bin/streamt",
            "--quiet",
            "export",
            "strimzi",
            "--namespace",
            "st-123456789abc",
            "--cluster-name",
            "sk-123456789abc",
            "--project-dir",
            str(project),
            "--output-file",
            str(output_one if index == 1 else output_two),
        )
        assert environment["PYTHONPATH"] == str(guard_dir)
        assert environment["STREAMT_STRIMZI_GUARD_MARKER"] == str(
            guard_dir / f"export-{index}.marker"
        )
        assert environment["STREAMT_STRIMZI_GATE_STATE_DSN"].startswith(
            "postgresql://STRIMZI_GATE_STATE_PASSWORD_SECRET_663b@"
        )


@pytest.mark.parametrize("mutation", ["different", "secret", "guard", "surface"])
def test_guarded_double_export_fails_closed_on_any_boundary_change(
    tmp_path: Path, mutation: str
) -> None:
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    documents = _expected_export_documents(contract, "st-123456789abc", "sk-123456789abc")
    payload = gate._yaml_stream_bytes(documents)
    guard_dir = tmp_path / "guard"
    guard_dir.mkdir()
    project = tmp_path / "stream_project.yml"
    project.write_bytes(_PROJECT_PATH.read_bytes())
    calls = 0

    def runner(command: list[str], **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        environment = kwargs["environment"]
        if mutation != "guard":
            Path(environment["STREAMT_STRIMZI_GUARD_MARKER"]).write_bytes(b"active\n")
        output = Path(command[command.index("--output-file") + 1])
        written = payload
        if mutation == "different" and calls == 2:
            written += b"\n"
        elif mutation == "secret":
            written += b"# STRIMZI_GATE_TAG_SECRET_d84a\n"
        output.write_bytes(written)
        stderr = b"STRIMZI_GATE_SQL_SECRET_36c8" if mutation == "surface" else b""
        return gate.ProcessResult(0, b"", stderr)

    with pytest.raises(gate.GateError):
        gate._export_twice(
            Path("/private/venv/bin/streamt"),
            project=project,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
            output_one=tmp_path / "one.yaml",
            output_two=tmp_path / "two.yaml",
            cwd=tmp_path,
            base_environment={"HOME": str(tmp_path)},
            guard_dir=guard_dir,
            contract=contract,
            deadline=100,
            runner=runner,
            monotonic=lambda: 0,
        )


def test_process_runner_enforces_timeout_without_leaking_child_text(tmp_path: Path) -> None:
    started = time.monotonic()
    with pytest.raises(gate.GateError) as captured:
        gate.run_process(
            [
                sys.executable,
                "-c",
                "import sys,time; sys.stderr.write('CONFIDENTIAL_CHILD'); sys.stderr.flush(); time.sleep(30)",
            ],
            cwd=tmp_path,
            environment={"PATH": os.environ.get("PATH", "")},
            timeout_seconds=0.05,
            max_output_bytes=128,
        )
    assert time.monotonic() - started < 3
    assert "CONFIDENTIAL_CHILD" not in str(captured.value)
    assert "CONFIDENTIAL_CHILD" not in repr(captured.value)


def test_poll_exact_retries_only_pending_and_obeys_deadlines() -> None:
    attempts = 0
    now = 0.0

    def reader() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise gate.PollPendingError("not-ready")
        return "ready"

    def monotonic() -> float:
        return now

    def sleep(delay: float) -> None:
        nonlocal now
        now += delay

    assert (
        gate.poll_exact(
            reader,
            timeout_seconds=5,
            timeout_code="topic_timeout",
            monotonic=monotonic,
            sleep=sleep,
        )
        == "ready"
    )
    assert attempts == 3

    now = 0.0
    with pytest.raises(gate.GateError) as captured:
        gate.poll_exact(
            lambda: (_ for _ in ()).throw(gate.PollPendingError("not-ready")),
            timeout_seconds=3,
            timeout_code="topic_timeout",
            monotonic=monotonic,
            sleep=sleep,
        )
    assert captured.value.code == "topic_timeout"

    with pytest.raises(RuntimeError, match="unexpected"):
        gate.poll_exact(
            lambda: (_ for _ in ()).throw(RuntimeError("unexpected")),
            timeout_seconds=1,
            timeout_code="topic_timeout",
            monotonic=monotonic,
            sleep=sleep,
        )


def _operator_deployment() -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "strimzi-cluster-operator",
            "namespace": "st-123456789abc",
            "generation": 1,
        },
        "spec": {"replicas": 1},
        "status": {
            "observedGeneration": 1,
            "availableReplicas": 1,
            "readyReplicas": 1,
            "conditions": [{"type": "Available", "status": "True"}],
        },
    }


def _kafka_cluster() -> dict[str, Any]:
    return {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "Kafka",
        "metadata": {
            "name": "sk-123456789abc",
            "namespace": "st-123456789abc",
            "generation": 1,
        },
        "status": {
            "observedGeneration": 1,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def test_operator_and_kafka_readiness_are_exact_and_identity_bound() -> None:
    operator = _operator_deployment()
    kafka = _kafka_cluster()
    assert gate._operator_ready(operator, namespace="st-123456789abc") is operator
    assert (
        gate._kafka_ready(
            kafka,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
        )
        is kafka
    )


@pytest.mark.parametrize("status_mode", ["absent", "null"])
def test_kafka_readiness_treats_missing_initial_status_as_pending(status_mode: str) -> None:
    kafka = _kafka_cluster()
    if status_mode == "absent":
        kafka.pop("status")
    else:
        kafka["status"] = None
    with pytest.raises(gate.PollPendingError):
        gate._kafka_ready(
            kafka,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
        )


@pytest.mark.parametrize("status", [False, 0, "pending", [], ["Ready"]])
def test_kafka_readiness_rejects_present_nonmapping_status(status: Any) -> None:
    kafka = _kafka_cluster()
    kafka["status"] = status
    with pytest.raises(gate.GateError):
        gate._kafka_ready(
            kafka,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
        )


def test_kafka_readiness_validates_identity_before_missing_status_pending() -> None:
    kafka = _kafka_cluster()
    kafka.pop("status")
    kafka["metadata"]["namespace"] = "wrong"
    with pytest.raises(gate.GateError):
        gate._kafka_ready(
            kafka,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
        )


def test_kafka_readiness_treats_real_initial_creating_status_as_pending() -> None:
    kafka = _kafka_cluster()
    kafka["status"] = {
        "observedGeneration": 0,
        "conditions": [
            {
                "type": "NotReady",
                "status": "True",
                "reason": "Creating",
                "message": "Kafka cluster is being deployed",
            }
        ],
    }
    with pytest.raises(gate.PollPendingError):
        gate._kafka_ready(
            kafka,
            namespace="st-123456789abc",
            cluster_name="sk-123456789abc",
        )


@pytest.mark.parametrize(
    ("resource", "mutation", "pending"),
    [
        ("operator", "api", False),
        ("operator", "name", False),
        ("operator", "namespace", False),
        ("operator", "generation-bool", False),
        ("operator", "generation-zero", False),
        ("operator", "replicas-bool", False),
        ("operator", "replicas-zero", False),
        ("operator", "observed", True),
        ("operator", "available", True),
        ("operator", "condition", True),
        ("kafka", "api", False),
        ("kafka", "name", False),
        ("kafka", "namespace", False),
        ("kafka", "generation-bool", False),
        ("kafka", "generation-zero", False),
        ("kafka", "observed", True),
        ("kafka", "condition", True),
    ],
)
def test_operator_and_kafka_readiness_reject_invalid_or_pending_observations(
    resource: str, mutation: str, pending: bool
) -> None:
    value = _operator_deployment() if resource == "operator" else _kafka_cluster()
    if mutation == "api":
        value["apiVersion"] = "v1"
    elif mutation == "name":
        value["metadata"]["name"] = "other"
    elif mutation == "namespace":
        value["metadata"]["namespace"] = "other"
    elif mutation == "generation-bool":
        value["metadata"]["generation"] = True
        value["status"]["observedGeneration"] = True
    elif mutation == "generation-zero":
        value["metadata"]["generation"] = 0
        value["status"]["observedGeneration"] = 0
    elif mutation == "replicas-bool":
        value["spec"]["replicas"] = True
        value["status"]["availableReplicas"] = True
        value["status"]["readyReplicas"] = True
    elif mutation == "replicas-zero":
        value["spec"]["replicas"] = 0
        value["status"]["availableReplicas"] = 0
        value["status"]["readyReplicas"] = 0
    elif mutation == "observed":
        value["status"]["observedGeneration"] = 0
    elif mutation == "available":
        value["status"]["availableReplicas"] = 0
    else:
        value["status"]["conditions"][0]["status"] = "False"

    expected_error = gate.PollPendingError if pending else gate.GateError

    def observe() -> None:
        if resource == "operator":
            gate._operator_ready(value, namespace="st-123456789abc")
        else:
            gate._kafka_ready(
                value,
                namespace="st-123456789abc",
                cluster_name="sk-123456789abc",
            )

    with pytest.raises(expected_error):
        observe()


def _topic_description(topic_name: str = "orders-ready-v1") -> str:
    return (
        f"Topic: {topic_name}\tTopicId: Mk3M5vQbQ42nKPfWlK9l0Q\t"
        "PartitionCount: 3\tReplicationFactor: 1\tConfigs: cleanup.policy=compact\n"
        f"\tTopic: {topic_name}\tPartition: 0\tLeader: 0\tReplicas: 0\t"
        "Isr: 0\tElr: \tLastKnownElr: \n"
        f"\tTopic: {topic_name}\tPartition: 1\tLeader: 0\tReplicas: 0\t"
        "Isr: 0\tElr: \tLastKnownElr: \n"
        f"\tTopic: {topic_name}\tPartition: 2\tLeader: 0\tReplicas: 0\t"
        "Isr: 0\tElr: \tLastKnownElr: \n"
    )


def _topic_configs(topic_name: str = "orders-ready-v1") -> str:
    return (
        f"Dynamic configs for topic {topic_name} are:\n"
        "  cleanup.policy=compact sensitive=false "
        "synonyms={DYNAMIC_TOPIC_CONFIG:cleanup.policy=compact}\n"
        "  delete.retention.ms=86400000 sensitive=false "
        "synonyms={DYNAMIC_TOPIC_CONFIG:delete.retention.ms=86400000}\n"
        "  remote.storage.enable=false sensitive=false "
        "synonyms={DYNAMIC_TOPIC_CONFIG:remote.storage.enable=false}\n"
    )


def test_broker_topic_parsers_accept_only_exact_single_topic_output() -> None:
    description = gate._parse_topic_describe(_topic_description().encode(), "orders-ready-v1")
    assert description == {
        "topic_name": "orders-ready-v1",
        "topic_id": "Mk3M5vQbQ42nKPfWlK9l0Q",
        "partition_count": 3,
        "replication_factor": 1,
        "partitions": (
            {"partition": 0, "leader": 0, "replicas": (0,), "isr": (0,)},
            {"partition": 1, "leader": 0, "replicas": (0,), "isr": (0,)},
            {"partition": 2, "leader": 0, "replicas": (0,), "isr": (0,)},
        ),
    }
    assert gate._parse_topic_configs(_topic_configs(), "orders-ready-v1") == {
        "cleanup.policy": "compact",
        "delete.retention.ms": "86400000",
        "remote.storage.enable": "false",
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong-topic",
        "duplicate-header",
        "missing-row",
        "duplicate-row",
        "unordered-row",
        "wrong-replica-count",
        "unhealthy-isr",
        "extra-field",
        "invalid-utf8",
    ],
)
def test_topic_describe_parser_rejects_ambiguous_or_unhealthy_output(
    mutation: str,
) -> None:
    output: bytes | str = _topic_description()
    if mutation == "wrong-topic":
        output = str(output).replace("orders-ready-v1", "other-topic")
    elif mutation == "duplicate-header":
        output = str(output) + str(output).splitlines(keepends=True)[0]
    elif mutation == "missing-row":
        output = "".join(str(output).splitlines(keepends=True)[:-1])
    elif mutation == "duplicate-row":
        rows = str(output).splitlines(keepends=True)
        output = "".join([*rows, rows[-1]])
    elif mutation == "unordered-row":
        output = str(output).replace("Partition: 1", "Partition: 2", 1)
    elif mutation == "wrong-replica-count":
        output = str(output).replace("Replicas: 0", "Replicas: 0,1", 1)
    elif mutation == "unhealthy-isr":
        output = str(output).replace("Isr: 0", "Isr: ", 1)
    elif mutation == "extra-field":
        output = str(output).replace("\tElr: ", "\tUnexpected: x\tElr: ", 1)
    else:
        output = b"\xff"

    with pytest.raises(gate.GateError):
        gate._parse_topic_describe(output, "orders-ready-v1")


@pytest.mark.parametrize(
    "mutation",
    ["wrong-topic", "duplicate-header", "duplicate-key", "sensitive", "missing-suffix"],
)
def test_topic_config_parser_rejects_ambiguous_or_sensitive_output(
    mutation: str,
) -> None:
    output = _topic_configs()
    if mutation == "wrong-topic":
        output = output.replace("orders-ready-v1", "other-topic")
    elif mutation == "duplicate-header":
        output += "Dynamic configs for topic orders-ready-v1 are:\n"
    elif mutation == "duplicate-key":
        output += output.splitlines(keepends=True)[1]
    elif mutation == "sensitive":
        output = output.replace("sensitive=false", "sensitive=true", 1)
    else:
        output = output.replace(" synonyms={", " ", 1)

    with pytest.raises(gate.GateError):
        gate._parse_topic_configs(output, "orders-ready-v1")


def _topic_object(
    *,
    namespace: str = "st-123456789abc",
    name: str = "orders-ready-v1",
    topic_name: str = "orders-ready-v1",
) -> dict[str, Any]:
    return {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "KafkaTopic",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": "67b6c7a2-ef1a-4c21-9ce2-9db969e12830",
            "generation": 1,
            "resourceVersion": "12",
        },
        "spec": {
            "topicName": topic_name,
            "partitions": 3,
            "replicas": 1,
            "config": {
                "cleanup.policy": "compact",
                "delete.retention.ms": "86400000",
                "remote.storage.enable": "false",
            },
        },
        "status": {
            "topicName": topic_name,
            "topicId": "Mk3M5vQbQ42nKPfWlK9l0Q",
            "observedGeneration": 1,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }


def test_topic_snapshot_and_replay_require_all_stable_identities() -> None:
    broker = gate._parse_topic_describe(_topic_description(), "orders-ready-v1")
    configs = gate._parse_topic_configs(_topic_configs(), "orders-ready-v1")
    first = gate._topic_snapshot(_topic_object(), broker, configs)
    assert first == gate.TopicSnapshot(
        namespace="st-123456789abc",
        metadata_name="orders-ready-v1",
        uid="67b6c7a2-ef1a-4c21-9ce2-9db969e12830",
        generation=1,
        topic_name="orders-ready-v1",
        topic_id="Mk3M5vQbQ42nKPfWlK9l0Q",
        spec=_topic_object()["spec"],
        broker_description=broker,
        broker_configs=configs,
    )
    second_object = _topic_object()
    second_object["metadata"]["resourceVersion"] = "99"
    second = gate._topic_snapshot(second_object, copy.deepcopy(broker), dict(configs))
    gate._validate_replay(first, second)

    for field, value in (
        ("uid", "different"),
        ("generation", 2),
        ("topic_id", "different"),
        ("spec", {**first.spec, "partitions": 4}),
        (
            "broker_description",
            {**first.broker_description, "partition_count": 4},
        ),
        ("broker_configs", {**first.broker_configs, "cleanup.policy": "delete"}),
    ):
        with pytest.raises(gate.GateError):
            gate._validate_replay(first, dataclasses.replace(second, **{field: value}))


@pytest.mark.parametrize(
    "mutation",
    [
        "api-version",
        "kind",
        "status-name",
        "observed-generation",
        "not-ready",
        "missing-id",
        "broker-name",
        "spec-topic",
        "spec-partitions",
        "spec-config",
        "broker-replicas",
    ],
)
def test_topic_snapshot_rejects_unready_or_cross_topic_state(mutation: str) -> None:
    topic = _topic_object()
    broker = gate._parse_topic_describe(_topic_description(), "orders-ready-v1")
    configs = gate._parse_topic_configs(_topic_configs(), "orders-ready-v1")
    if mutation == "api-version":
        topic["apiVersion"] = "kafka.strimzi.io/v1beta2"
    elif mutation == "kind":
        topic["kind"] = "Kafka"
    elif mutation == "status-name":
        topic["status"]["topicName"] = "other"
    elif mutation == "observed-generation":
        topic["status"]["observedGeneration"] = 0
    elif mutation == "not-ready":
        topic["status"]["conditions"][0]["status"] = "False"
    elif mutation == "missing-id":
        topic["status"]["topicId"] = ""
    elif mutation == "broker-name":
        broker = {**broker, "topic_name": "other"}
    elif mutation == "spec-topic":
        topic["spec"]["topicName"] = "other"
    elif mutation == "spec-partitions":
        topic["spec"]["partitions"] = 4
    elif mutation == "spec-config":
        topic["spec"]["config"]["cleanup.policy"] = "delete"
    else:
        broker = {**broker, "replication_factor": 2}
    with pytest.raises((gate.GateError, gate.PollPendingError)):
        gate._topic_snapshot(topic, broker, configs)


def test_cluster_evidence_capture_is_exact_bounded_and_broker_complete(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_bytes(b"private kubeconfig")
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    commands: list[tuple[str, ...]] = []
    bounds: list[tuple[float, int]] = []

    def runner(command: list[str], **kwargs: Any) -> Any:
        commands.append(tuple(command))
        bounds.append((kwargs["timeout_seconds"], kwargs["max_output_bytes"]))
        if "ctr" in command:
            return gate.ProcessResult(0, b"quay.io/z\nquay.io/a\n", b"")
        return gate.ProcessResult(0, b"bounded evidence\n", b"")

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=contract.confidential_sentinels,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=kubeconfig,
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod="sk-123456789abc-dual-role-0",
        topics=contract.topics,
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert rejected is False
    assert len(commands) == 10 + 2 * len(contract.topics)
    assert len(captured) == len(commands) + 1
    assert set(captured) == {
        "ctr-images.txt",
        "kubernetes-version.json",
        "kafkatopic-crd.json",
        "kafkatopic-crd-sha256.json",
        "nodes.json",
        "operator-deployment.json",
        "workload-pods.json",
        "strimzi-resources.json",
        "events.json",
        "operator.log",
        "topic-operator.log",
        "broker-0-describe.txt",
        "broker-0-configs.txt",
        "broker-1-describe.txt",
        "broker-1-configs.txt",
    }
    assert commands[0] == gate._ctr_images_list_command(
        Path("/locked/docker"),
        "streamt-strimzi-123456789abc-kind-control-plane",
    )
    assert (candidate / "ctr-images.txt").read_bytes() == b"quay.io/a\nquay.io/z\n"
    assert bounds == [(5, 512 * 1024)] * len(commands)
    for index, topic in enumerate(contract.topics):
        describe = commands[10 + index * 2]
        configs = commands[11 + index * 2]
        assert describe[-3:] == ("--describe", "--topic", topic.topic_name)
        assert configs[-3:] == ("--entity-name", topic.topic_name, "--describe")
        assert "localhost:9092" in describe
        assert "localhost:9092" in configs
    assert not any("private kubeconfig" in path.read_text() for path in candidate.iterdir())


def test_ctr_inventory_capture_survives_absent_node_and_kubeconfig(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    commands: list[tuple[str, ...]] = []

    def runner(command: list[str], **kwargs: Any) -> Any:
        commands.append(tuple(command))
        assert kwargs["timeout_seconds"] == 5
        assert kwargs["max_output_bytes"] == 512 * 1024
        return gate.ProcessResult(1, b"untrusted stdout", b"untrusted stderr")

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=tmp_path / "missing-kubeconfig",
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod=None,
        topics=(),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert commands == [
        gate._ctr_images_list_command(
            Path("/locked/docker"),
            "streamt-strimzi-123456789abc-kind-control-plane",
        )
    ]
    assert captured == ("ctr-images.txt",)
    assert rejected is False
    assert (candidate / "ctr-images.txt").read_bytes() == (
        b'{"returncode":1,"status":"capture-failed"}\n'
    )
    assert b"untrusted" not in (candidate / "ctr-images.txt").read_bytes()


def test_ctr_inventory_capture_neutralizes_success_with_stderr(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()

    def runner(_command: list[str], **_kwargs: Any) -> Any:
        return gate.ProcessResult(
            0,
            b"quay.io/strimzi/operator@sha256:" + b"a" * 64 + b"\n",
            b"untrusted warning",
        )

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=tmp_path / "missing-kubeconfig",
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod=None,
        topics=(),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert captured == ("ctr-images.txt",)
    assert rejected is False
    assert (candidate / "ctr-images.txt").read_bytes() == (
        b'{"returncode":0,"status":"capture-failed"}\n'
    )


def test_unsafe_ctr_inventory_capture_forces_marker_only_staging(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()

    def runner(_command: list[str], **_kwargs: Any) -> Any:
        return gate.ProcessResult(0, b"safe\x1b[31munsafe\n", b"")

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=tmp_path / "missing-kubeconfig",
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod=None,
        topics=(),
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert captured == ()
    assert rejected is True
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, ())
    _assert_marker_only(upload)


@pytest.mark.parametrize(
    ("failure", "failure_name", "failure_payload"),
    [
        (
            "nonzero",
            "kafkatopic-crd.json",
            b'{"returncode":7,"status":"capture-failed"}\n',
        ),
        (
            "exception",
            "kafkatopic-crd.json.failed",
            b'{"status":"capture-failed"}\n',
        ),
    ],
)
def test_ordinary_capture_failure_stages_only_neutral_placeholder_and_summary(
    tmp_path: Path,
    failure: str,
    failure_name: str,
    failure_payload: bytes,
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_bytes(b"private kubeconfig")
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    calls = 0

    def runner(_command: list[str], **_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            return gate.ProcessResult(0, b"sha256:" + b"a" * 64 + b"\n", b"")
        if calls == 3 and failure == "nonzero":
            return gate.ProcessResult(
                7,
                b"CONFIDENTIAL_SENTINEL raw stdout",
                b"CONFIDENTIAL_SENTINEL raw stderr",
            )
        if calls == 3 and failure == "exception":
            raise RuntimeError("CONFIDENTIAL_SENTINEL raw exception")
        return gate.ProcessResult(0, b"bounded evidence\n", b"")

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=kubeconfig,
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod="sk-123456789abc-dual-role-0",
        topics=contract.topics,
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert calls == 10 + 2 * len(contract.topics)
    assert rejected is False
    assert failure_name in captured
    assert (candidate / failure_name).read_bytes() == failure_payload
    candidate_bytes = b"".join(path.read_bytes() for path in candidate.iterdir())
    assert b"raw stdout" not in candidate_bytes
    assert b"raw stderr" not in candidate_bytes
    assert b"raw exception" not in candidate_bytes
    gate._evidence_write(
        candidate,
        "summary.json",
        b'{"failure_code":"primary-failure","status":"failed"}\n',
        _SAFE_SENTINELS,
    )

    staged = gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert "summary.json" in staged
    assert failure_name in staged
    upload_bytes = b"".join(path.read_bytes() for path in upload.iterdir())
    assert b"raw stdout" not in upload_bytes
    assert b"raw stderr" not in upload_bytes
    assert b"raw exception" not in upload_bytes
    assert b"CONFIDENTIAL_SENTINEL" not in upload_bytes


def test_capture_deadline_incompleteness_forces_marker_only_staging(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_bytes(b"private kubeconfig")
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    calls = 0
    monotonic_calls = 0

    def runner(_command: list[str], **_kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        return gate.ProcessResult(0, b"sha256:" + b"a" * 64 + b"\n", b"")

    def monotonic() -> float:
        nonlocal monotonic_calls
        monotonic_calls += 1
        return 31 if monotonic_calls >= 3 else 0

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=kubeconfig,
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod="sk-123456789abc-dual-role-0",
        topics=contract.topics,
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=monotonic,
    )

    assert captured
    assert calls == 2
    assert rejected is True
    with pytest.raises(gate.GateError) as staged:
        gate._scan_and_stage_evidence(candidate, upload, ())
    assert staged.value.code == "evidence_rejected"
    _assert_marker_only(upload)


@pytest.mark.parametrize("failure", ["secret", "write"])
def test_capture_evidence_safety_failure_forces_marker_only_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_bytes(b"private kubeconfig")
    contract = gate._load_gate_contract(_CONTRACT_PATH)
    original_evidence_write = gate._evidence_write

    if failure == "write":

        def failed_write(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("CONFIDENTIAL_SENTINEL write failure")

        monkeypatch.setattr(gate, "_evidence_write", failed_write)

    def runner(_command: list[str], **_kwargs: Any) -> Any:
        payload = (
            b"CONFIDENTIAL_SENTINEL" if failure == "secret" else b"sha256:" + b"a" * 64 + b"\n"
        )
        return gate.ProcessResult(0, payload, b"")

    captured, rejected = gate._capture_cluster_evidence(
        candidate=candidate,
        confidential=_SAFE_SENTINELS,
        docker=Path("/locked/docker"),
        node_name="streamt-strimzi-123456789abc-kind-control-plane",
        kubectl=Path("/locked/kubectl"),
        kubeconfig=kubeconfig,
        namespace="st-123456789abc",
        cluster_name="sk-123456789abc",
        broker_pod="sk-123456789abc-dual-role-0",
        topics=contract.topics,
        cwd=tmp_path,
        environment={"PATH": "/locked"},
        deadline=30,
        runner=runner,
        monotonic=lambda: 0,
    )

    assert captured == ()
    assert rejected is True
    monkeypatch.setattr(gate, "_evidence_write", original_evidence_write)
    with pytest.raises(gate.GateError) as staged:
        gate._scan_and_stage_evidence(candidate, upload, ())
    assert staged.value.code == "evidence_rejected"
    _assert_marker_only(upload)


def test_evidence_staging_is_all_or_nothing_into_fresh_output(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "summary.json").write_text('{"status":"failed"}\n', encoding="utf-8")
    (candidate / "cleanup.txt").write_text("no residue\n", encoding="utf-8")

    staged = gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert staged == ("cleanup.txt", "summary.json")
    assert sorted(path.name for path in upload.iterdir()) == ["cleanup.txt", "summary.json"]
    assert upload.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o444 for path in upload.iterdir())
    assert list(tmp_path.glob(".upload.stage-*")) == []


def test_evidence_staging_rejects_preexisting_upload_without_touching_it(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    upload.mkdir()
    (candidate / "summary.json").write_text('{"status":"failed"}\n', encoding="utf-8")
    stale = upload / "stale-secret.txt"
    stale.write_text("must remain caller-owned", encoding="utf-8")

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert stale.read_text(encoding="utf-8") == "must remain caller-owned"
    assert sorted(path.name for path in upload.iterdir()) == ["stale-secret.txt"]


def test_evidence_secret_match_stages_only_fixed_marker(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "safe.txt").write_text("safe", encoding="utf-8")
    (candidate / "unsafe.txt").write_text("prefix CONFIDENTIAL_SENTINEL suffix", encoding="utf-8")

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)
    assert b"CONFIDENTIAL_SENTINEL" not in (upload / gate.SCAN_FAILURE_MARKER_NAME).read_bytes()


@pytest.mark.parametrize("sentinels", [(), ("same", "same")])
def test_evidence_staging_rejects_incomplete_sentinel_inventory_with_marker(
    tmp_path: Path, sentinels: tuple[str, ...]
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "safe.txt").write_bytes(b"safe")

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, sentinels)

    _assert_marker_only(upload)


def test_evidence_size_violation_stages_only_fixed_marker(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "first-safe.txt").write_bytes(b"safe")
    (candidate / "oversized.bin").write_bytes(b"x" * (gate.MAX_EVIDENCE_FILE_BYTES + 1))

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)


def test_evidence_total_size_violation_stages_only_fixed_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "a.bin").write_bytes(b"a" * 8)
    (candidate / "b.bin").write_bytes(b"b" * 8)
    monkeypatch.setattr(gate, "MAX_EVIDENCE_FILE_BYTES", 10)
    monkeypatch.setattr(gate, "MAX_EVIDENCE_TOTAL_BYTES", 12)

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)


def test_evidence_read_failure_stages_only_fixed_marker(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "safe.txt").write_bytes(b"safe")
    (candidate / "not-a-regular-file").mkdir()

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)


def test_evidence_symlink_is_rejected_without_following_target(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    victim = tmp_path / "outside.txt"
    candidate.mkdir()
    victim.write_text("outside data", encoding="utf-8")
    (candidate / "linked.txt").symlink_to(victim)

    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)
    assert victim.read_text(encoding="utf-8") == "outside data"


def test_evidence_regular_file_swap_to_symlink_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    victim = tmp_path / "outside.txt"
    candidate.mkdir()
    swapped = candidate / "swapped.txt"
    swapped.write_text("initial", encoding="utf-8")
    victim.write_text("outside data", encoding="utf-8")
    original_open = gate.os.open
    did_swap = False

    def swapping_open(path: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal did_swap
        if path == swapped.name and kwargs.get("dir_fd") is not None and not did_swap:
            did_swap = True
            swapped.unlink()
            swapped.symlink_to(victim)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(gate.os, "open", swapping_open)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)
    assert victim.read_text(encoding="utf-8") == "outside data"


def test_evidence_directory_membership_change_during_scan_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "initial.txt").write_bytes(b"initial")
    original_listdir = gate.os.listdir
    calls = 0

    def changing_listdir(path: Any) -> list[str]:
        nonlocal calls
        calls += 1
        if calls == 2:
            (candidate / "late.txt").write_bytes(b"late")
        return original_listdir(path)

    monkeypatch.setattr(gate.os, "listdir", changing_listdir)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)


def test_evidence_stage_write_failure_leaves_marker_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "a.txt").write_bytes(b"a")
    (candidate / "b.txt").write_bytes(b"b")
    original_write = Path.write_bytes
    failed = False

    def failing_write(path: Path, data: bytes) -> int:
        nonlocal failed
        if path.name == "b.txt" and not failed:
            failed = True
            raise OSError("injected stage write failure")
        return original_write(path, data)

    monkeypatch.setattr(Path, "write_bytes", failing_write)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    _assert_marker_only(upload)


def test_evidence_stage_fsync_failure_leaves_marker_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "a.txt").write_bytes(b"a")
    original_fsync = gate.os.fsync
    failed = False

    def failing_fsync(fd: int) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected fsync failure")
        original_fsync(fd)

    monkeypatch.setattr(gate.os, "fsync", failing_fsync)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert failed
    _assert_marker_only(upload)


def test_evidence_publish_failure_leaves_marker_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "a.txt").write_bytes(b"a")
    original_replace = gate.os.replace
    failed = False

    def failing_replace(source: Any, destination: Any) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected publish failure")
        original_replace(source, destination)

    monkeypatch.setattr(gate.os, "replace", failing_replace)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert failed
    _assert_marker_only(upload)


def test_evidence_marker_persistence_failure_never_publishes_candidate_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    upload = tmp_path / "upload"
    candidate.mkdir()
    (candidate / "unsafe.txt").write_bytes(b"CONFIDENTIAL_SENTINEL")
    original_write = Path.write_bytes

    def failing_marker(path: Path, data: bytes) -> int:
        if path.name == gate.SCAN_FAILURE_MARKER_NAME:
            raise OSError("injected marker failure")
        return original_write(path, data)

    monkeypatch.setattr(Path, "write_bytes", failing_marker)
    with pytest.raises(gate.GateError):
        gate._scan_and_stage_evidence(candidate, upload, _SAFE_SENTINELS)

    assert not upload.exists()


def _lifecycle_arguments(tmp_path: Path, *, pilot: bool) -> Any:
    wheel = tmp_path / "streamt-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"bounded wheel artifact")
    return gate.GateArguments(
        wheel=wheel,
        evidence_candidate=tmp_path / "candidate",
        evidence_upload=tmp_path / "upload",
        pilot=pilot,
        self_test=False,
    )


def _install_lifecycle_fakes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    images: Any,
    *,
    failure: str | None = None,
    cleanup_failure: bool = False,
    ctr_mode: str = "desktop",
) -> dict[str, Any]:
    bundle = gate._load_fixture_bundle()
    docker = Path("/bin/echo").resolve(strict=True)
    prefix = "streamt-strimzi-123456789abc"
    namespace = "st-123456789abc"
    kafka_cluster = "sk-123456789abc"
    network_name = f"{prefix}-net"
    network_id = "a" * 64
    commands: list[tuple[str, ...]] = []
    observations: list[tuple[Any, ...]] = []
    events: list[str] = []

    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    monkeypatch.setattr(gate, "_load_fixture_bundle", lambda: bundle)
    monkeypatch.setattr(gate, "_resolve_docker", lambda: docker)
    monkeypatch.setattr(
        gate,
        "_docker_server_images",
        lambda _lock, _docker, *, runner: images,
    )
    monkeypatch.setattr(gate.host_platform, "system", lambda: "Linux")
    monkeypatch.setattr(gate.host_platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(
        gate.uuid,
        "uuid4",
        lambda: gate.uuid.UUID("12345678-9abc-def0-1234-56789abcdef0"),
    )

    def downloader(
        _download: Any,
        destination: Path,
        *,
        executable: bool,
        **_kwargs: Any,
    ) -> None:
        events.append(f"download:{destination.name}")
        payload = (
            b"#!/bin/sh\n"
            if executable
            else b"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: source\n"
        )
        destination.write_bytes(payload)
        destination.chmod(0o700 if executable else 0o600)

    original_yaml_loader = gate._load_yaml_documents

    def load_yaml_documents(path: Path, *args: Any, **kwargs: Any) -> Any:
        if path.name == "operator-source.yaml":
            return _operator_documents()
        return original_yaml_loader(path, *args, **kwargs)

    monkeypatch.setattr(gate, "_load_yaml_documents", load_yaml_documents)

    def verify_image_chain(*_args: Any, **_kwargs: Any) -> None:
        events.append("image-chain")

    monkeypatch.setattr(gate, "_verify_image_chain", verify_image_chain)

    def verify_installed_wheel(*_args: Any, **_kwargs: Any) -> None:
        events.append("wheel-import")

    monkeypatch.setattr(gate, "_verify_installed_wheel", verify_installed_wheel)

    def export_twice(_executable: Path, **kwargs: Any) -> tuple[bytes, tuple[dict[str, Any], ...]]:
        documents = _expected_export_documents(
            bundle.gate_contract,
            kwargs["namespace"],
            kwargs["cluster_name"],
        )
        payload = gate._yaml_stream_bytes(documents)
        kwargs["output_one"].write_bytes(payload)
        kwargs["output_two"].write_bytes(payload)
        events.append("offline-export")
        return payload, documents

    monkeypatch.setattr(gate, "_export_twice", export_twice)

    def observe_topics(**kwargs: Any) -> tuple[Any, ...]:
        snapshots = tuple(
            gate.TopicSnapshot(
                namespace=namespace,
                metadata_name=document["metadata"]["name"],
                uid=f"uid-{index}",
                generation=1,
                topic_name=document["spec"]["topicName"],
                topic_id=f"topic-id-{index}",
                spec=copy.deepcopy(document["spec"]),
                broker_description={
                    "topic_name": document["spec"]["topicName"],
                    "topic_id": f"topic-id-{index}",
                    "partition_count": document["spec"]["partitions"],
                    "replication_factor": document["spec"]["replicas"],
                    "rows": (),
                },
                broker_configs=dict(document["spec"]["config"]),
            )
            for index, document in enumerate(kwargs["expected_documents"])
        )
        observations.append(snapshots)
        events.append("observe-topics")
        return snapshots

    monkeypatch.setattr(gate, "_observe_topics", observe_topics)
    original_evidence_write = gate._evidence_write

    def evidence_write(
        candidate: Path,
        name: str,
        payload: bytes,
        confidential: Any,
    ) -> None:
        events.append(f"evidence:{name}")
        original_evidence_write(candidate, name, payload, confidential)

    monkeypatch.setattr(gate, "_evidence_write", evidence_write)

    operator_pods = _pilot_pods(images) if images.pilot_pending else _workload_pods(images)
    operator_item = operator_pods["items"][0]
    operator_item["metadata"].update({"name": "strimzi-cluster-operator-0", "namespace": namespace})
    cluster_item = operator_pods["items"][1]
    cluster_item["metadata"].update(
        {"name": f"{kafka_cluster}-dual-role-0", "namespace": namespace}
    )
    cluster_item["status"].update(
        {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }
    )
    operator_list = {
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": [operator_item],
    }
    cluster_list = {
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": [cluster_item],
    }
    services = {
        "apiVersion": "v1",
        "kind": "List",
        "metadata": {"resourceVersion": ""},
        "items": [
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": f"{kafka_cluster}-kafka-bootstrap"},
                "spec": {"type": "ClusterIP", "ports": [{"port": 9092}]},
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": f"{kafka_cluster}-kafka-brokers"},
                "spec": {"type": "ClusterIP", "ports": [{"port": 9092}]},
            },
        ],
    }
    operator_ready = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "strimzi-cluster-operator",
            "namespace": namespace,
            "generation": 1,
        },
        "spec": {"replicas": 1},
        "status": {
            "observedGeneration": 1,
            "availableReplicas": 1,
            "readyReplicas": 1,
            "conditions": [{"type": "Available", "status": "True"}],
        },
    }
    kafka_ready = {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "Kafka",
        "metadata": {
            "name": kafka_cluster,
            "namespace": namespace,
            "generation": 1,
        },
        "status": {
            "observedGeneration": 1,
            "conditions": [{"type": "Ready", "status": "True"}],
        },
    }
    network_inspects = 0
    route_inventories = 0
    ctr_inventories = 0
    egress_probes = 0
    ctr_names = {"registry.k8s.io/pause@sha256:" + "a" * 64}
    content_payloads: dict[str, bytes] = {}
    import_pairs: dict[str, Any] = {}
    for reference in (images.operator_child_reference, images.kafka_child_reference):
        child_digest = reference.rsplit("@", 1)[1]
        content_payloads[child_digest] = gate._canonical_json_bytes(
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "schemaVersion": 2,
            }
        )
        payload = _ctr_outer_payload(reference)
        pair = _ctr_pair_for_payload(reference, payload)
        content_payloads[pair.outer_digest] = payload
        import_pairs[reference] = pair

    def runner(command: list[str], **_kwargs: Any) -> Any:
        nonlocal ctr_inventories, egress_probes, network_inspects, route_inventories
        encoded = tuple(str(item) for item in command)
        commands.append(encoded)
        if len(encoded) >= 3 and encoded[1:3] == ("-m", "venv"):
            venv = Path(encoded[-1])
            (venv / "bin").mkdir(parents=True)
            for name in ("python", "streamt"):
                executable = venv / "bin" / name
                executable.write_bytes(b"#!/bin/sh\n")
                executable.chmod(0o700)
        if encoded[1:3] == ("network", "create"):
            events.append("network-create")
            return gate.ProcessResult(0, f"{network_id}\n".encode(), b"")
        if encoded[1:3] == ("network", "inspect"):
            network_inspects += 1
            events.append(
                "network-inspect-pre" if network_inspects == 1 else "network-inspect-attached"
            )
            inspected = _network_inspect(prefix, attached=network_inspects > 1)
            if failure == "network-masquerade" and network_inspects == 1:
                inspected[0]["Options"]["com.docker.network.bridge.enable_ip_masquerade"] = "true"
            if failure == "attachment-drift" and network_inspects == 2:
                inspected[0]["Containers"] = {}
            if failure == "restart-attachment-drift" and network_inspects == 3:
                inspected[0]["Containers"] = {}
            if failure == "late-network-drift" and network_inspects >= 4:
                inspected[0]["Options"]["com.docker.network.bridge.enable_ip_masquerade"] = "true"
            return gate.ProcessResult(0, json.dumps(inspected).encode(), b"")
        if encoded[1:3] == ("create", "cluster"):
            Path(encoded[encoded.index("--kubeconfig") + 1]).write_bytes(b"bounded kubeconfig")
            events.append("kind-create")
            warnings = ("\n".join(gate._KIND_NETWORK_WARNINGS) + "\n").encode()
            return gate.ProcessResult(0, b"", warnings)
        if encoded[1:3] == ("inspect", f"{prefix}-kind-control-plane"):
            inspected_node = _node_inspect(prefix, images)
            if failure == "host-publish":
                inspected_node[0]["NetworkSettings"]["Ports"]["6443/tcp"][0]["HostIp"] = "0.0.0.0"
            return gate.ProcessResult(0, json.dumps(inspected_node).encode(), b"")
        if encoded[1:3] == ("load", "docker-image"):
            reference = encoded[3]
            pair = import_pairs[reference]
            config = (
                images.operator_config_digest
                if reference == images.operator_child_reference
                else images.kafka_config_digest
            )
            use_desktop = ctr_mode == "desktop" or (
                ctr_mode == "mixed" and reference == images.operator_child_reference
            )
            if use_desktop:
                outer_source = (
                    import_pairs[images.operator_child_reference].outer_source
                    if failure == "ctr-pair-overlap" and reference == images.kafka_child_reference
                    else pair.outer_source
                )
                ctr_names.update({pair.child_source, outer_source, config})
                if (
                    failure == "ctr-too-many-imports"
                    and reference == images.operator_child_reference
                ):
                    ctr_names.add("import-2026-09-04@sha256:" + "f" * 64)
            else:
                assert ctr_mode in {"classic", "mixed"}
                ctr_names.update({reference, config})
            events.append(f"kind-load:{reference}")
        if encoded[1:] == gate._ctr_images_list_command(docker, f"{prefix}-kind-control-plane")[1:]:
            ctr_inventories += 1
            events.append("ctr-list")
            if failure == "restart-ctr-drift" and ctr_inventories >= 9:
                ctr_names.add("unexpected-after-recovery")
            return gate.ProcessResult(0, ("\n".join(sorted(ctr_names)) + "\n").encode(), b"")
        if (
            len(encoded) == 8
            and encoded[1:6]
            == (
                "exec",
                f"{prefix}-kind-control-plane",
                "ctr",
                "--namespace=k8s.io",
                "content",
            )
            and encoded[6] == "get"
        ):
            digest = encoded[-1]
            events.append(f"ctr-content:{digest}")
            if digest in {
                images.operator_manifest_digest,
                images.kafka_manifest_digest,
            }:
                if failure == "ctr-diagnostic-nonzero":
                    return gate.ProcessResult(19, b"", b"")
                if failure == "ctr-diagnostic-exception":
                    raise RuntimeError("CONFIDENTIAL_CTR_DIAGNOSTIC_EXCEPTION")
                if failure == "ctr-diagnostic-timeout":
                    raise gate.GateError("subprocess_timeout", "Bounded diagnostic timeout")
            payload = content_payloads[digest]
            if failure == "ctr-diagnostic-duplicate-json" and digest in {
                images.operator_manifest_digest,
                images.kafka_manifest_digest,
            }:
                payload = b'{"duplicate":1,"duplicate":2}'
            if failure == "ctr-content-hash":
                payload += b" "
            return gate.ProcessResult(0, payload, b"")
        if (
            len(encoded) == 9
            and encoded[1:6]
            == (
                "exec",
                f"{prefix}-kind-control-plane",
                "ctr",
                "--namespace=k8s.io",
                "images",
            )
            and encoded[6] == "tag"
        ):
            source, target = encoded[-2:]
            assert source == import_pairs[target].child_source
            ctr_names.add(target)
            events.append(f"ctr-tag:{target}")
            if failure == "ctr-tag-output":
                return gate.ProcessResult(0, b"arbitrary tagged name\n", b"")
            return gate.ProcessResult(0, f"{target}\n".encode(), b"")
        if (
            len(encoded) == 9
            and encoded[1:6]
            == (
                "exec",
                f"{prefix}-kind-control-plane",
                "ctr",
                "--namespace=k8s.io",
                "images",
            )
            and encoded[6] == "rm"
        ):
            child_source, outer_source = encoded[-2:]
            ctr_names.remove(child_source)
            ctr_names.remove(outer_source)
            events.append(f"ctr-rm:{child_source}")
            if failure == "ctr-remove-output":
                return gate.ProcessResult(0, b"arbitrary\n", b"")
            return gate.ProcessResult(0, f"{child_source}\n{outer_source}\n".encode(), b"")
        if encoded[1:] == (
            "exec",
            f"{prefix}-kind-control-plane",
            "systemctl",
            "restart",
            "containerd",
        ):
            events.append("containerd-restart")
            if failure == "containerd-restart":
                return gate.ProcessResult(9, b"", b"")
            return gate.ProcessResult(0, b"", b"")
        if "crictl" in encoded:
            events.append("cri-final")
            inventory = _cri_inventory(images)
            if failure == "cri-wrong-config":
                inventory["images"][0]["id"] = images.operator_manifest_digest
            if failure == "cri-wrong-ref":
                inventory["images"][0]["repoDigests"] = [images.operator_index_reference]
            return gate.ProcessResult(0, json.dumps(inventory).encode(), b"")
        if encoded[1:4] == (
            "exec",
            f"{prefix}-kind-control-plane",
            "ip",
        ) and encoded[4:] == ("route", "del", "default"):
            events.append("route-delete")
            if failure == "route-delete":
                return gate.ProcessResult(8, b"", b"")
            if failure == "route-delete-output":
                return gate.ProcessResult(0, b"deleted default route\n", b"")
            return gate.ProcessResult(0, b"", b"")
        if encoded[1:4] == (
            "exec",
            f"{prefix}-kind-control-plane",
            "ip",
        ) and encoded[4:] in {
            ("-4", "route", "show", "default"),
            ("-6", "route", "show", "default"),
        }:
            route_inventories += 1
            family = encoded[4]
            events.append(f"route-inventory-{family}")
            drift = (
                failure == "route-drift"
                or (failure == "late-route-drift" and route_inventories > 4)
                or (failure == "restart-route-drift" and route_inventories in {3, 4})
            )
            return gate.ProcessResult(
                0,
                b"default via 172.18.0.1\n" if drift and family == "-4" else b"",
                b"",
            )
        if encoded[1:3] == ("exec", f"{prefix}-kind-control-plane"):
            egress_probes += 1
            events.append("egress-probe")
            if failure == "restart-egress-output" and egress_probes == 2:
                return gate.ProcessResult(0, b"changed\n", b"")
            return gate.ProcessResult(0, b"", b"")
        if "create" in encoded and "namespace" in encoded:
            events.append("namespace-create")
            if failure == "returncode":
                return gate.ProcessResult(17, b"", b"bounded failure")
            if failure == "timeout":
                raise gate.GateError("subprocess_timeout", "bounded timeout")
            if failure == "unexpected":
                raise RuntimeError("CONFIDENTIAL_INTERNAL_FAILURE")
        if "deployment/strimzi-cluster-operator" in encoded:
            return gate.ProcessResult(0, json.dumps(operator_ready).encode(), b"")
        if "get" in encoded and "node" in encoded:
            node_ready = {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": f"{prefix}-kind-control-plane"},
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
            if failure == "node-identity":
                node_ready["metadata"]["name"] = "wrong-node"
            events.append("node-ready")
            return gate.ProcessResult(0, json.dumps(node_ready).encode(), b"")
        if "get" in encoded and "kafka" in encoded:
            return gate.ProcessResult(0, json.dumps(kafka_ready).encode(), b"")
        if "get" in encoded and "pods" in encoded:
            value = copy.deepcopy(operator_list)
            value["items"].extend(copy.deepcopy(cluster_list["items"]))
            if failure == "pod-collection-envelope":
                value["kind"] = "PodList"
            return gate.ProcessResult(0, json.dumps(value).encode(), b"")
        if "get" in encoded and "services" in encoded:
            value = copy.deepcopy(services)
            if failure == "service-collection-envelope":
                value["kind"] = "ServiceList"
            return gate.ProcessResult(0, json.dumps(value).encode(), b"")
        if "--dry-run=server" in encoded:
            events.append("topic-dry-run")
        elif "--field-manager=streamt-strimzi-gate" in encoded:
            events.append("topic-apply")
        if encoded[1:4] == ("delete", "cluster", "--name"):
            events.append("cleanup-kind")
            return gate.ProcessResult(9 if cleanup_failure else 0, b"", b"")
        if encoded[1:3] == ("rm", "--force"):
            events.append("cleanup-node-fallback")
            return gate.ProcessResult(0, b"", b"")
        if encoded[1:3] == ("network", "rm"):
            events.append("cleanup-network")
            return gate.ProcessResult(0, b"", b"")
        if encoded[1:3] in {("ps", "-a"), ("network", "ls")}:
            events.append("cleanup-residue")
            return gate.ProcessResult(0, b"", b"")
        if "version" in encoded and "--output=json" in encoded:
            events.append("capture")
            if failure == "capture-secret":
                return gate.ProcessResult(
                    0,
                    bundle.gate_contract.confidential_sentinels[0].encode(),
                    b"",
                )
        return gate.ProcessResult(0, b"{}\n", b"")

    original_stage = gate._scan_and_stage_evidence

    def stage(candidate: Path, upload: Path, confidential: Any) -> Any:
        events.append("stage")
        return original_stage(candidate, upload, confidential)

    monkeypatch.setattr(gate, "_scan_and_stage_evidence", stage)
    return {
        "bundle": bundle,
        "commands": commands,
        "events": events,
        "observations": observations,
        "runner": runner,
        "downloader": downloader,
        "prefix": prefix,
        "namespace": namespace,
        "kafka_cluster": kafka_cluster,
        "network_name": network_name,
    }


@pytest.mark.parametrize(
    ("pilot", "images", "expected_code"),
    [
        (False, _platform(), "pilot_pending"),
        (True, _frozen_platform(), "pilot_forbidden"),
    ],
)
def test_lifecycle_rejects_invalid_pilot_mode_before_any_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot: bool,
    images: Any,
    expected_code: str,
) -> None:
    arguments = _lifecycle_arguments(tmp_path, pilot=pilot)
    bundle = gate._load_fixture_bundle()
    monkeypatch.setattr(gate, "_load_fixture_bundle", lambda: bundle)
    monkeypatch.setattr(gate, "_resolve_docker", lambda: Path("/bin/echo"))
    monkeypatch.setattr(
        gate,
        "_docker_server_images",
        lambda _lock, _docker, *, runner: images,
    )
    monkeypatch.setattr(gate.host_platform, "system", lambda: "Linux")
    monkeypatch.setattr(gate.host_platform, "machine", lambda: "x86_64")

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("pilot lock failure crossed the mutation boundary")

    monkeypatch.setattr(gate, "_prepare_runtime", forbidden)
    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(arguments, runner=forbidden, downloader=forbidden)
    assert captured.value.code == expected_code
    assert not arguments.evidence_candidate.exists()
    assert not arguments.evidence_upload.exists()


@pytest.mark.parametrize("pilot", [False, True])
def test_full_mocked_lifecycle_reconciles_replays_cleans_and_stages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot: bool,
) -> None:
    images = _platform() if pilot else _frozen_platform()
    harness = _install_lifecycle_fakes(monkeypatch, tmp_path, images)
    arguments = _lifecycle_arguments(tmp_path, pilot=pilot)

    result = gate._execute_gate(
        arguments,
        runner=harness["runner"],
        downloader=harness["downloader"],
        monotonic=lambda: 100.0,
    )

    assert result == (gate.PILOT_PENDING_EXIT_CODE if pilot else 0)
    assert len(harness["observations"]) == 2
    assert harness["observations"][0] == harness["observations"][1]
    assert harness["events"].count("topic-dry-run") == 1
    assert harness["events"].count("topic-apply") == 2
    assert harness["events"].index("offline-export") < harness["events"].index("network-create")
    assert harness["events"].index("evidence:generated-checksums.json") < harness["events"].index(
        "network-create"
    )
    assert harness["events"].index("evidence:node-image.json") < harness["events"].index(
        "topic-dry-run"
    )
    image_store_events = [
        event
        for event in harness["events"]
        if event.startswith(("kind-load:", "ctr-", "cri-final"))
    ]
    operator_pair = _ctr_pair_for_payload(
        images.operator_child_reference,
        _ctr_outer_payload(images.operator_child_reference),
    )
    kafka_pair = _ctr_pair_for_payload(
        images.kafka_child_reference,
        _ctr_outer_payload(images.kafka_child_reference),
    )
    operator_diagnostic_digests = [
        source.rsplit("@", 1)[1]
        for source in sorted((operator_pair.child_source, operator_pair.outer_source))
    ]
    kafka_diagnostic_digests = [
        source.rsplit("@", 1)[1]
        for source in sorted((kafka_pair.child_source, kafka_pair.outer_source))
    ]
    assert image_store_events == [
        "ctr-list",
        f"kind-load:{images.operator_child_reference}",
        "ctr-list",
        *(f"ctr-content:{digest}" for digest in operator_diagnostic_digests),
        f"ctr-content:{operator_pair.outer_digest}",
        f"ctr-tag:{images.operator_child_reference}",
        "ctr-list",
        f"ctr-rm:{operator_pair.child_source}",
        "ctr-list",
        f"kind-load:{images.kafka_child_reference}",
        "ctr-list",
        *(f"ctr-content:{digest}" for digest in kafka_diagnostic_digests),
        f"ctr-content:{kafka_pair.outer_digest}",
        f"ctr-tag:{images.kafka_child_reference}",
        "ctr-list",
        f"ctr-rm:{kafka_pair.child_source}",
        "ctr-list",
        "ctr-list",
        "ctr-list",
        "cri-final",
        "ctr-list",
    ]
    assert harness["events"].index("route-delete") == harness["events"].index("kind-create") + 1
    first_ipv4 = harness["events"].index("route-inventory--4")
    first_ipv6 = harness["events"].index("route-inventory--6")
    assert harness["events"].index("route-delete") < first_ipv4 < first_ipv6
    assert (
        first_ipv6
        < harness["events"].index("egress-probe")
        < harness["events"].index("namespace-create")
    )
    assert harness["events"].index("evidence:topic-first.json") < harness["events"].index(
        "topic-apply", harness["events"].index("topic-apply") + 1
    )
    assert harness["events"].index("evidence:topic-replay.json") < harness["events"].index(
        "capture"
    )
    assert harness["events"].count("route-inventory--4") == 3
    assert harness["events"].count("route-inventory--6") == 3
    ipv4_indexes = [
        index for index, event in enumerate(harness["events"]) if event == "route-inventory--4"
    ]
    assert ipv4_indexes[1] < harness["events"].index("evidence:topic-replay.json")
    assert ipv4_indexes[2] > harness["events"].index("evidence:topic-replay.json")
    assert harness["events"].index(
        "network-inspect-attached",
        harness["events"].index("network-inspect-attached") + 1,
    ) < harness["events"].index("capture")
    assert harness["events"].index("capture") < harness["events"].index("cleanup-kind")
    assert harness["events"].index("cleanup-network") < harness["events"].index("stage")
    assert not arguments.evidence_candidate.exists()
    assert arguments.evidence_upload.is_dir()
    ctr_diagnostic_names = {path.name for path in arguments.evidence_upload.glob("ctr-load-*")}
    assert ctr_diagnostic_names == {
        "ctr-load-0-before.txt",
        "ctr-load-0-after.txt",
        "ctr-load-0-import-0.json",
        "ctr-load-0-import-1.json",
        "ctr-load-1-before.txt",
        "ctr-load-1-after.txt",
        "ctr-load-1-import-0.json",
        "ctr-load-1-import-1.json",
    }
    baseline = frozenset({"registry.k8s.io/pause@sha256:" + "a" * 64})
    operator_after = baseline | {
        operator_pair.child_source,
        operator_pair.outer_source,
        images.operator_config_digest,
    }
    operator_normalized = baseline | {
        images.operator_child_reference,
        images.operator_config_digest,
    }
    kafka_after = operator_normalized | {
        kafka_pair.child_source,
        kafka_pair.outer_source,
        images.kafka_config_digest,
    }
    for name, names in {
        "ctr-load-0-before.txt": baseline,
        "ctr-load-0-after.txt": operator_after,
        "ctr-load-1-before.txt": operator_normalized,
        "ctr-load-1-after.txt": kafka_after,
    }.items():
        assert (arguments.evidence_upload / name).read_bytes() == gate._canonical_ctr_image_names(
            names
        )
    for load_index, (reference, pair) in enumerate(
        (
            (images.operator_child_reference, operator_pair),
            (images.kafka_child_reference, kafka_pair),
        )
    ):
        child_payload = gate._canonical_json_bytes(
            {
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "schemaVersion": 2,
            }
        )
        outer_payload = _ctr_outer_payload(reference)
        payloads = {
            pair.child_source: child_payload,
            pair.outer_source: outer_payload,
        }
        for import_index, source in enumerate(sorted(payloads)):
            artifact = json.loads(
                (
                    arguments.evidence_upload / f"ctr-load-{load_index}-import-{import_index}.json"
                ).read_bytes()
            )
            raw = payloads[source]
            assert artifact == {
                "content": json.loads(raw),
                "raw_content_sha256": f"sha256:{hashlib.sha256(raw).hexdigest()}",
                "source": source,
            }
    summary = json.loads((arguments.evidence_upload / "summary.json").read_bytes())
    assert summary == {
        "failure_code": None,
        "platform": "linux/amd64",
        "release": "1.2.0",
        "status": "pilot-pending" if pilot else "passed",
    }
    pilot_path = arguments.evidence_upload / "pilot-image-ids.json"
    assert pilot_path.exists() is pilot
    if pilot:
        pilot_value = json.loads(pilot_path.read_bytes())
        assert pilot_value["platform"] == "linux/amd64"
        assert {
            name: value["child_reference"] for name, value in pilot_value["images"].items()
        } == {
            "operator": images.operator_child_reference,
            "kafka": images.kafka_child_reference,
        }
    apply_commands = [
        command
        for command in harness["commands"]
        if "--field-manager=streamt-strimzi-gate" in command
    ]
    assert len(apply_commands) == 3
    assert all("--server-side=true" in command for command in apply_commands)
    assert sum("--dry-run=server" in command for command in apply_commands) == 1
    assert all(
        command[command.index("--filename") + 1].endswith("topics-one.yaml")
        for command in apply_commands
    )
    namespace_inventory = [
        command
        for command in harness["commands"]
        if "--namespace" in command
        and "get" in command
        and ("pods" in command or "services" in command)
    ]
    assert namespace_inventory
    assert all(
        "--selector" not in command and "-l" not in command for command in namespace_inventory
    )
    egress_commands = [
        command for command in harness["commands"] if "/dev/tcp/127.0.0.1/6443" in " ".join(command)
    ]
    assert len(egress_commands) == 2
    egress_script = egress_commands[0][-1]
    assert egress_script.index("/dev/tcp/127.0.0.1/6443") < egress_script.index(
        "/dev/tcp/1.1.1.1/443"
    )
    assert "exit 41" in egress_script
    assert "exit 42" in egress_script
    assert 'case "$code" in 1|124)' in egress_script
    assert "exit 43" in egress_script
    network_create_commands = [
        command for command in harness["commands"] if command[1:3] == ("network", "create")
    ]
    assert network_create_commands == [
        gate._docker_network_create_command(
            Path(network_create_commands[0][0]), harness["network_name"]
        )
    ]
    assert "--internal" not in network_create_commands[0]
    route_delete_commands = [
        command
        for command in harness["commands"]
        if command[1:4] == ("exec", f"{harness['prefix']}-kind-control-plane", "ip")
        and command[4:] == ("route", "del", "default")
    ]
    assert len(route_delete_commands) == 1
    kind_create_index = next(
        index
        for index, command in enumerate(harness["commands"])
        if command[1:3] == ("create", "cluster")
    )
    route_delete_index = harness["commands"].index(route_delete_commands[0])
    assert route_delete_index == kind_create_index + 1
    initial_route_indexes = [
        index
        for index, command in enumerate(harness["commands"])
        if command[1:4] == ("exec", f"{harness['prefix']}-kind-control-plane", "ip")
        and command[4:]
        in {
            ("-4", "route", "show", "default"),
            ("-6", "route", "show", "default"),
        }
    ][:2]
    egress_index = harness["commands"].index(egress_commands[0])
    first_strimzi_apply = next(
        index
        for index, command in enumerate(harness["commands"])
        if "apply" in command and "--filename" in command
    )
    final_cri_index = next(
        index for index, command in enumerate(harness["commands"]) if "crictl" in command
    )
    ctr_tag_commands = [
        command
        for command in harness["commands"]
        if len(command) == 9
        and command[1:6]
        == (
            "exec",
            f"{harness['prefix']}-kind-control-plane",
            "ctr",
            "--namespace=k8s.io",
            "images",
        )
        and command[6] == "tag"
    ]
    assert ctr_tag_commands == [
        gate._ctr_image_tag_command(
            Path(ctr_tag_commands[0][0]),
            f"{harness['prefix']}-kind-control-plane",
            _ctr_import_key(images.operator_child_reference),
            images.operator_child_reference,
        ),
        gate._ctr_image_tag_command(
            Path(ctr_tag_commands[0][0]),
            f"{harness['prefix']}-kind-control-plane",
            _ctr_import_key(images.kafka_child_reference),
            images.kafka_child_reference,
        ),
    ]
    assert route_delete_index < min(initial_route_indexes)
    assert max(initial_route_indexes) < egress_index < first_strimzi_apply
    assert final_cri_index < first_strimzi_apply


def test_classic_lifecycle_never_tags_removes_or_restarts_containerd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    images = _frozen_platform()
    harness = _install_lifecycle_fakes(
        monkeypatch,
        tmp_path,
        images,
        ctr_mode="classic",
    )
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    assert (
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )
        == 0
    )
    assert not any(
        event.startswith(("ctr-content:", "ctr-tag:", "ctr-rm:")) for event in harness["events"]
    )
    assert "containerd-restart" not in harness["events"]
    assert harness["events"].count("route-inventory--4") == 2
    assert harness["events"].count("route-inventory--6") == 2
    assert harness["events"].count("egress-probe") == 1
    assert harness["events"].index("cri-final") < harness["events"].index("namespace-create")


def test_lifecycle_rejects_mixed_ctr_modes_before_restart_or_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    images = _frozen_platform()
    harness = _install_lifecycle_fakes(
        monkeypatch,
        tmp_path,
        images,
        ctr_mode="mixed",
    )
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )
    assert captured.value.code == "ctr_inventory_invalid"
    assert "containerd-restart" not in harness["events"]
    assert "namespace-create" not in harness["events"]
    assert "cleanup-kind" in harness["events"]
    assert "stage" in harness["events"]


@pytest.mark.parametrize(
    ("failure", "failure_code"),
    [
        ("returncode", "namespace_create_failed"),
        ("timeout", "subprocess_timeout"),
        ("unexpected", "internal_error"),
        ("route-delete", "node_route_delete_failed"),
        ("route-delete-output", "node_route_delete_failed"),
        ("route-drift", "node_route_invalid"),
        ("late-route-drift", "node_route_invalid"),
        ("network-masquerade", "docker_network_invalid"),
        ("attachment-drift", "docker_network_invalid"),
        ("late-network-drift", "docker_network_invalid"),
        ("host-publish", "docker_port_invalid"),
        ("ctr-content-hash", "ctr_content_invalid"),
        ("ctr-too-many-imports", "ctr_inventory_invalid"),
        ("ctr-pair-overlap", "ctr_inventory_invalid"),
        ("ctr-tag-output", "ctr_tag_invalid"),
        ("ctr-remove-output", "ctr_remove_invalid"),
        ("containerd-restart", "containerd_restart_failed"),
        ("node-identity", "node_not_ready"),
        ("restart-attachment-drift", "docker_network_invalid"),
        ("restart-route-drift", "node_route_invalid"),
        ("restart-egress-output", "egress_probe_failed"),
        ("restart-ctr-drift", "ctr_inventory_invalid"),
        ("cri-wrong-config", "cri_invalid"),
        ("cri-wrong-ref", "cri_invalid"),
        ("pod-collection-envelope", "workload_image_invalid"),
        ("service-collection-envelope", "workload_exposure_invalid"),
    ],
)
def test_lifecycle_failure_still_captures_cleans_and_stages_safe_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    failure_code: str,
) -> None:
    harness = _install_lifecycle_fakes(monkeypatch, tmp_path, _frozen_platform(), failure=failure)
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )

    assert captured.value.code == failure_code
    if failure == "network-masquerade":
        assert "kind-create" not in harness["events"]
        assert "capture" not in harness["events"]
    else:
        assert harness["events"].index("capture") < harness["events"].index("cleanup-kind")
    if failure.startswith(("ctr-", "cri-")):
        assert not any(
            "apply" in command and "--filename" in command for command in harness["commands"]
        )
    assert harness["events"].index("cleanup-network") < harness["events"].index("stage")
    assert not arguments.evidence_candidate.exists()
    assert arguments.evidence_upload.is_dir()
    summary = json.loads((arguments.evidence_upload / "summary.json").read_bytes())
    assert summary["status"] == "failed"
    assert summary["failure_code"] == failure_code
    assert (arguments.evidence_upload / "ctr-images.txt").is_file()
    upload_bytes = b"".join(path.read_bytes() for path in arguments.evidence_upload.iterdir())
    assert b"CONFIDENTIAL_INTERNAL_FAILURE" not in upload_bytes


@pytest.mark.parametrize(
    "failure",
    [
        "ctr-diagnostic-duplicate-json",
        "ctr-diagnostic-nonzero",
        "ctr-diagnostic-exception",
        "ctr-diagnostic-timeout",
        "ctr-diagnostic-write",
    ],
)
def test_ctr_load_diagnostic_safety_failure_stages_only_the_fixed_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    runner_failure = None if failure == "ctr-diagnostic-write" else failure
    harness = _install_lifecycle_fakes(
        monkeypatch,
        tmp_path,
        _frozen_platform(),
        failure=runner_failure,
    )
    if failure == "ctr-diagnostic-write":
        evidence_writer = gate._evidence_write

        def reject_diagnostic(
            candidate: Path,
            name: str,
            payload: bytes,
            confidential: Any,
        ) -> None:
            if name == "ctr-load-0-before.txt":
                raise gate.GateError(
                    "evidence_candidate_invalid",
                    "Synthetic diagnostic write rejection",
                )
            evidence_writer(candidate, name, payload, confidential)

        monkeypatch.setattr(gate, "_evidence_write", reject_diagnostic)
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )

    assert captured.value.code == "evidence_rejected"
    assert not any("apply" in command for command in harness["commands"])
    assert "cleanup-kind" in harness["events"]
    assert "stage" in harness["events"]
    _assert_marker_only(arguments.evidence_upload)


def test_cleanup_failure_is_aggregated_only_after_capture_and_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _install_lifecycle_fakes(
        monkeypatch, tmp_path, _frozen_platform(), cleanup_failure=True
    )
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )

    assert captured.value.code == "cleanup_failed"
    assert harness["events"].index("capture") < harness["events"].index("cleanup-kind")
    assert harness["events"].index("cleanup-kind") < harness["events"].index(
        "cleanup-node-fallback"
    )
    assert harness["events"].index("cleanup-network") < harness["events"].index("stage")
    cleanup = json.loads((arguments.evidence_upload / "cleanup.json").read_bytes())
    assert cleanup["cluster_delete_returncode"] == 9
    assert cleanup["node_remove_returncode"] == 0
    assert cleanup["network_remove_returncode"] == 0
    summary = json.loads((arguments.evidence_upload / "summary.json").read_bytes())
    assert summary["status"] == "failed"
    assert summary["failure_code"] == "cleanup_failed"


def test_capture_secret_fails_closed_to_marker_only_after_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _install_lifecycle_fakes(
        monkeypatch,
        tmp_path,
        _frozen_platform(),
        failure="capture-secret",
    )
    arguments = _lifecycle_arguments(tmp_path, pilot=False)

    with pytest.raises(gate.GateError) as captured:
        gate._execute_gate(
            arguments,
            runner=harness["runner"],
            downloader=harness["downloader"],
            monotonic=lambda: 100.0,
        )

    assert captured.value.code == "evidence_rejected"
    assert harness["events"].index("capture") < harness["events"].index("cleanup-kind")
    assert harness["events"].index("cleanup-network") < harness["events"].index("stage")
    _assert_marker_only(arguments.evidence_upload)
    assert not arguments.evidence_candidate.exists()


def test_self_test_is_network_process_and_streamt_free(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("self-test crossed an external boundary")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr("socket.socket", forbidden)
    monkeypatch.setattr("socket.create_connection", forbidden)
    before = {name for name in sys.modules if name == "streamt" or name.startswith("streamt.")}

    assert gate.main(["--self-test"]) == 0

    after = {name for name in sys.modules if name == "streamt" or name.startswith("streamt.")}
    assert after == before
    assert capsys.readouterr() == ("", "")


def test_argument_parser_keeps_self_test_real_and_pilot_modes_disjoint() -> None:
    assert gate._parse_args(["--self-test"]) == gate.GateArguments(
        wheel=None,
        evidence_candidate=None,
        evidence_upload=None,
        pilot=False,
        self_test=True,
    )
    real = gate._parse_args(
        [
            "--wheel",
            "/artifacts/streamt.whl",
            "--evidence-candidate",
            "/private/candidate",
            "--evidence-upload",
            "/private/upload",
        ]
    )
    assert real == gate.GateArguments(
        wheel=Path("/artifacts/streamt.whl"),
        evidence_candidate=Path("/private/candidate"),
        evidence_upload=Path("/private/upload"),
        pilot=False,
        self_test=False,
    )
    assert (
        gate._parse_args(
            [
                "--wheel",
                "/artifacts/streamt.whl",
                "--evidence-candidate",
                "/private/candidate",
                "--evidence-upload",
                "/private/upload",
                "--pilot",
            ]
        ).pilot
        is True
    )

    invalid = (
        [],
        ["--self-test", "--pilot"],
        ["--self-test", "--wheel", "/artifacts/streamt.whl"],
        ["--wheel", "/artifacts/streamt.whl"],
    )
    for arguments in invalid:
        with pytest.raises(gate.GateError):
            gate._parse_args(arguments)


def test_main_contains_expected_and_unexpected_lifecycle_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    arguments = [
        "--wheel",
        "/artifacts/streamt.whl",
        "--evidence-candidate",
        "/private/candidate",
        "--evidence-upload",
        "/private/upload",
    ]

    def expected_failure(_arguments: Any) -> None:
        raise gate.GateError("injected", "CONFIDENTIAL_EXPECTED")

    monkeypatch.setattr(gate, "_execute_gate", expected_failure)
    assert gate.main(arguments) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "ERROR: Strimzi acceptance gate failed safely\n"
    assert "CONFIDENTIAL_EXPECTED" not in output.err

    def unexpected_failure(_arguments: Any) -> None:
        raise RuntimeError("CONFIDENTIAL_UNEXPECTED")

    monkeypatch.setattr(gate, "_execute_gate", unexpected_failure)
    assert gate.main(arguments) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "ERROR: Strimzi acceptance gate failed safely\n"
    assert "CONFIDENTIAL_UNEXPECTED" not in output.err


def test_temporary_strimzi_amd64_pilot_ci_job_is_manual_and_deliberately_red() -> None:
    workflow_path = _ROOT / ".github" / "workflows" / "ci.yml"
    workflow = yaml.load(workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    assert workflow["on"] == {
        "push": {"branches": ["main"]},
        "pull_request": {"branches": ["main"]},
        "workflow_dispatch": {
            "inputs": {
                "strimzi_amd64_pilot": {
                    "description": ("Run the temporary non-acceptance Strimzi amd64 imageID pilot"),
                    "required": "true",
                    "default": "false",
                    "type": "boolean",
                }
            }
        },
    }

    job = workflow["jobs"]["strimzi-amd64-pilot"]
    assert job["name"] == (
        "TEMPORARY NON-ACCEPTANCE - Strimzi 1.2.0 amd64 imageID pilot (expected red)"
    )
    assert job["if"] == (
        "${{ github.event_name == 'workflow_dispatch' && inputs.strimzi_amd64_pilot == true }}"
    )
    assert job["needs"] == ["package", "strimzi-package-parity"]
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["timeout-minutes"] == "30"
    assert job["permissions"] == {"contents": "read"}
    assert "continue-on-error" not in job

    steps = job["steps"]
    checkout = next(step for step in steps if step["name"] == "Checkout")
    assert checkout["uses"] == ("actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803")
    setup = next(step for step in steps if step["name"] == "Setup Python")
    assert setup["uses"] == ("actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1")
    assert setup["with"] == {"python-version": "3.12"}
    download = next(
        step for step in steps if step["name"] == "Download exact release-candidate distributions"
    )
    assert download["uses"] == (
        "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert download["with"] == {
        "name": "python-distributions-${{ github.run_attempt }}",
        "path": "${{ runner.temp }}/streamt-strimzi-wheel",
    }

    self_test = next(step for step in steps if step["name"] == "Self-test standalone gate")
    assert self_test["run"] == (
        "env -u PYTHONPATH python -I tests/integration/strimzi_gate.py --self-test"
    )
    install = next(step for step in steps if step["name"] == "Install standalone gate dependency")
    assert install["run"] == (
        "python -m pip install --disable-pip-version-check --no-input 'PyYAML==6.0.3'"
    )
    pilot = next(
        step
        for step in steps
        if step["name"] == "Run amd64 pilot, validate staged evidence, and remain deliberately red"
    )
    assert pilot["shell"] == "bash"
    script = pilot["run"]
    assert "set -euo pipefail" in script
    assert "--pilot" in script
    assert "env -u PYTHONPATH python -I" in script
    assert script.count("env -u PYTHONPATH python -I -") == 2
    assert "import sys" in script
    assert "Path(sys.argv[1])" in script
    assert 'python -I - "${upload}"' in script
    assert '--wheel "${wheel}"' in script
    assert "if (( pilot_status != 2 )); then" in script
    assert "Expected pilot exit 2" in script
    assert "summary != expected_summary" in script
    assert '"status": "pilot-pending"' in script
    assert '"platform": "linux/amd64"' in script
    assert '"pilot-image-ids.json"' in script
    assert '"linux/arm64"]["kubernetes_image_id"]' in script
    assert '"linux/amd64"]["kubernetes_image_id"]' in script
    assert script.rstrip().endswith("exit 2")
    assert "continue-on-error" not in pilot

    uploads = [
        step for step in steps if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(uploads) == 1
    upload = uploads[0]
    assert upload["name"] == "Upload only scanned amd64 pilot evidence"
    assert upload["if"] == "failure()"
    assert upload["uses"] == ("actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a")
    assert upload["with"] == {
        "name": (
            "strimzi-1.2.0-amd64-pilot-evidence-${{ github.run_id }}-${{ github.run_attempt }}"
        ),
        "path": (
            "${{ runner.temp }}/streamt-strimzi-evidence-upload-"
            "${{ github.run_id }}-${{ github.run_attempt }}/"
        ),
        "if-no-files-found": "error",
        "retention-days": "7",
    }
    assert "candidate" not in upload["with"]["path"]


def test_strimzi_ci_job_is_exact_once_wiring_is_present() -> None:
    workflow_path = _ROOT / ".github" / "workflows" / "ci.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    jobs = workflow["jobs"]
    if "strimzi-real-acceptance" not in jobs:
        return

    job = jobs["strimzi-real-acceptance"]
    assert job["name"] == "Strimzi 1.2.0 real KafkaTopic acceptance"
    assert job["needs"] == ["package", "strimzi-package-parity"]
    assert job["runs-on"] == "ubuntu-24.04"
    assert job["timeout-minutes"] == 30
    assert job["permissions"] == {"contents": "read"}
    encoded = json.dumps(job, sort_keys=True)
    assert "${RUNNER_TEMP}/streamt-strimzi-wheel" in encoded
    assert (
        "${RUNNER_TEMP}/streamt-strimzi-evidence-candidate-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    ) in encoded
    assert (
        "${RUNNER_TEMP}/streamt-strimzi-evidence-upload-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
    ) in encoded
    assert "strimzi-1.2.0-evidence-${{ github.run_id }}-${{ github.run_attempt }}" in encoded
    assert '"retention-days": 7' in encoded
    assert '"if-no-files-found": "error"' in encoded
    assert "--pilot" not in encoded
    assert "PYTHONPATH" not in encoded

    uploads = [
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(uploads) == 1
    assert uploads[0]["if"] == "failure()"
