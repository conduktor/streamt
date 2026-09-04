"""Frozen-source and distribution checks for the Strimzi KafkaTopic CRD."""

from __future__ import annotations

import ast
import base64
import gzip
import hashlib
import importlib.util
import os
import re
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path, PurePosixPath

import pytest

_SCHEMA_PACKAGE = "streamt.integrations.gitops.schemas"
_RESOURCE_PREFIX = "streamt/integrations/gitops/schemas"
_SCHEMA_RESOURCE = "strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64"
_LICENSE_RESOURCE = "strimzi-1.2.0-LICENSE.txt"
_NOTICE_RESOURCE = "strimzi-1.2.0-NOTICE.txt"
_SOURCE_SHA256 = "36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e"
_RESOURCE_EXPECTATIONS = {
    "README.md": (
        1_621,
        "b52b11dc90ec2bba8b745dc7a4673637727889d10a81bef0eb73e4e02575795f",
    ),
    "__init__.py": (
        80,
        "14a9c9a4fc868cfd8a2a0c28948b0a97d6ecdd899ceff7be1825a57a9523b046",
    ),
    _SCHEMA_RESOURCE: (
        2_598,
        "f1e20b00cf206d10d65bec699f6966ad5df535102360f1975bd2d6c2a2d04587",
    ),
    _LICENSE_RESOURCE: (
        11_357,
        "b40930bbcf80744c86c46a12bc9da056641d722716c378f5659b9e555ef833e1",
    ),
    _NOTICE_RESOURCE: (
        1_169,
        "3ceda40a278d56f94646305b1fc5dfb3e437b6b2663f48fefe10544df15b1053",
    ),
}
_COMPRESSED_SHA256 = "2c3773847d5f83277940551ad1805b522c79d79441a3f3d423fe6b71d0766b92"
_FORBIDDEN_NAME = re.compile(
    r"(?:^|[^a-z0-9])(?:kubernetes|openshift|strimzi|pyhelm|helm)"
    r"(?:[^a-z0-9]|$)",
    re.IGNORECASE,
)
_FORBIDDEN_TOP_LEVEL_PACKAGES = {
    "helm",
    "kubernetes",
    "kubernetes_asyncio",
    "openshift",
    "pyhelm",
    "strimzi",
}


def _resource_member(
    resource_name: str,
    member_names: set[str],
    *,
    source_distribution: bool,
) -> str:
    expected = f"{_RESOURCE_PREFIX}/{resource_name}"
    if not source_distribution:
        assert expected in member_names
        return expected

    suffix = f"/src/{expected}"
    matches = [name for name in member_names if name.endswith(suffix)]
    assert len(matches) == 1, matches
    return matches[0]


def _assert_frozen_resources(
    *,
    member_names: set[str],
    read_member: Callable[[str], bytes],
    source_distribution: bool,
) -> None:
    resolved: dict[str, bytes] = {}
    for resource_name, (expected_size, expected_sha256) in _RESOURCE_EXPECTATIONS.items():
        member = _resource_member(
            resource_name,
            member_names,
            source_distribution=source_distribution,
        )
        payload = read_member(member)
        assert len(payload) == expected_size
        assert hashlib.sha256(payload).hexdigest() == expected_sha256
        resolved[resource_name] = payload

    encoded = b"".join(resolved[_SCHEMA_RESOURCE].split())
    compressed = base64.b64decode(encoded, validate=True)
    assert compressed[:10] == bytes.fromhex("1f8b0800000000000003")
    assert hashlib.sha256(compressed).hexdigest() == _COMPRESSED_SHA256
    decoded = gzip.decompress(compressed)
    assert len(decoded) == 6_329
    assert hashlib.sha256(decoded).hexdigest() == _SOURCE_SHA256


def _assert_no_target_sdk_requirements(metadata: bytes) -> None:
    for line in metadata.decode("utf-8").splitlines():
        if line.startswith(("Requires-Dist:", "Provides-Extra:")):
            assert _FORBIDDEN_NAME.search(line) is None, line


def _assert_no_target_sdk_imports(
    *,
    member_names: set[str],
    read_member: Callable[[str], bytes],
    source_distribution: bool,
) -> None:
    if source_distribution:
        production_python = [
            name
            for name in member_names
            if "/src/streamt/" in name and name.endswith(".py")
        ]
    else:
        production_python = [
            name
            for name in member_names
            if name.startswith("streamt/") and name.endswith(".py")
        ]
    assert production_python
    for name in production_python:
        tree = ast.parse(read_member(name), filename=name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imported_modules = [node.module]
            else:
                continue
            for module in imported_modules:
                top_level = module.partition(".")[0].casefold()
                assert top_level not in _FORBIDDEN_TOP_LEVEL_PACKAGES, name


def _assert_no_vendored_target_sdk(
    member_names: set[str],
    *,
    source_distribution: bool,
) -> None:
    for name in member_names:
        parts = PurePosixPath(name).parts
        if source_distribution:
            if len(parts) < 3 or parts[1] != "src":
                continue
            top_level = parts[2]
        else:
            if not parts:
                continue
            top_level = parts[0]
        assert top_level.casefold() not in _FORBIDDEN_TOP_LEVEL_PACKAGES, name


def _assert_wheel_contract(archive: zipfile.ZipFile) -> None:
    names = set(archive.namelist())
    _assert_frozen_resources(
        member_names=names,
        read_member=archive.read,
        source_distribution=False,
    )
    metadata = [name for name in names if name.endswith(".dist-info/METADATA")]
    assert len(metadata) == 1, metadata
    _assert_no_target_sdk_requirements(archive.read(metadata[0]))
    _assert_no_target_sdk_imports(
        member_names=names,
        read_member=archive.read,
        source_distribution=False,
    )
    _assert_no_vendored_target_sdk(names, source_distribution=False)


def _assert_source_distribution_contract(archive: tarfile.TarFile) -> None:
    names = set(archive.getnames())

    def read_member(name: str) -> bytes:
        extracted = archive.extractfile(name)
        assert extracted is not None
        return extracted.read()

    _assert_frozen_resources(
        member_names=names,
        read_member=read_member,
        source_distribution=True,
    )
    metadata = [name for name in names if name.endswith("/PKG-INFO")]
    assert len(metadata) == 1, metadata
    _assert_no_target_sdk_requirements(read_member(metadata[0]))
    _assert_no_target_sdk_imports(
        member_names=names,
        read_member=read_member,
        source_distribution=True,
    )
    _assert_no_vendored_target_sdk(names, source_distribution=True)


def test_source_tree_contains_exact_frozen_schema_license_and_notice() -> None:
    package = files(_SCHEMA_PACKAGE)
    names = set(_RESOURCE_EXPECTATIONS)
    _assert_frozen_resources(
        member_names={f"{_RESOURCE_PREFIX}/{name}" for name in names},
        read_member=lambda name: package.joinpath(Path(name).name).read_bytes(),
        source_distribution=False,
    )


def test_notice_records_complete_immutable_provenance() -> None:
    notice = files(_SCHEMA_PACKAGE).joinpath(_NOTICE_RESOURCE).read_text(encoding="utf-8")
    for expected in (
        "Upstream release: 1.2.0 (released 2026-08-20)",
        "Upstream commit: 6c7b43c4af0db547c10463ba09d1dfa6f5e156a0",
        "Source URL: https://raw.githubusercontent.com/strimzi/",
        f"Source SHA-256: {_SOURCE_SHA256}",
        "Transformation: The exact source bytes were gzip-compressed",
        "Retrieval date: 2026-09-03",
        "License: Apache License, Version 2.0.",
    ):
        assert expected in notice


@pytest.mark.parametrize("missing_resource", sorted(_RESOURCE_EXPECTATIONS))
def test_resource_contract_rejects_any_missing_frozen_resource(
    missing_resource: str,
) -> None:
    package = files(_SCHEMA_PACKAGE)
    names = set(_RESOURCE_EXPECTATIONS) - {missing_resource}
    with pytest.raises(AssertionError):
        _assert_frozen_resources(
            member_names={f"{_RESOURCE_PREFIX}/{name}" for name in names},
            read_member=lambda name: package.joinpath(Path(name).name).read_bytes(),
            source_distribution=False,
        )


@pytest.mark.parametrize("changed_resource", sorted(_RESOURCE_EXPECTATIONS))
def test_resource_contract_rejects_any_changed_frozen_resource(
    changed_resource: str,
) -> None:
    package = files(_SCHEMA_PACKAGE)

    def read_member(name: str) -> bytes:
        resource_name = Path(name).name
        payload = package.joinpath(resource_name).read_bytes()
        return payload + b"changed" if resource_name == changed_resource else payload

    with pytest.raises(AssertionError):
        _assert_frozen_resources(
            member_names={f"{_RESOURCE_PREFIX}/{name}" for name in _RESOURCE_EXPECTATIONS},
            read_member=read_member,
            source_distribution=False,
        )


@pytest.mark.parametrize(
    "metadata",
    [
        b"Requires-Dist: kubernetes>=35\n",
        b"Requires-Dist: openshift\n",
        b"Requires-Dist: strimzi-kafka\n",
        b"Requires-Dist: pyhelm\n",
        b"Requires-Dist: helm\n",
        b"Provides-Extra: kubernetes\n",
    ],
)
def test_dependency_contract_rejects_target_sdks(metadata: bytes) -> None:
    with pytest.raises(AssertionError):
        _assert_no_target_sdk_requirements(metadata)


@pytest.mark.parametrize(
    "source",
    [
        b"import kubernetes\n",
        b"import kubernetes_asyncio\n",
        b"from openshift.client import ApiClient\n",
        b"import strimzi\n",
        b"from pyhelm import chart\n",
        b"import yaml, kubernetes\n",
        b"import os, helm as charts\n",
    ],
)
def test_import_contract_rejects_target_sdks(source: bytes) -> None:
    with pytest.raises(AssertionError):
        _assert_no_target_sdk_imports(
            member_names={"streamt/example.py"},
            read_member=lambda _name: source,
            source_distribution=False,
        )


def test_built_wheel_and_source_distribution_preserve_frozen_boundary(
    tmp_path: Path,
) -> None:
    configured_dist = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if configured_dist is None:
        if importlib.util.find_spec("build") is None:
            pytest.skip("the build frontend is required for distribution inspection")
        repository = Path(__file__).parents[2]
        distribution_dir = tmp_path / "dist"
        subprocess.run(
            [
                sys.executable,
                "-m",
                "build",
                "--wheel",
                "--sdist",
                "--outdir",
                str(distribution_dir),
            ],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )
    else:
        distribution_dir = Path(configured_dist)

    wheels = list(distribution_dir.glob("streamt-*.whl"))
    source_distributions = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(source_distributions) == 1, source_distributions

    with zipfile.ZipFile(wheels[0]) as wheel:
        _assert_wheel_contract(wheel)
    with tarfile.open(source_distributions[0], "r:gz") as source_distribution:
        _assert_source_distribution_contract(source_distribution)
