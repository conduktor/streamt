"""Package-version coherence across source, metadata, CLI, and distributions."""

from __future__ import annotations

import importlib.metadata
import os
import re
import subprocess
import sys
import tarfile
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path

import pytest
from click.testing import CliRunner

from streamt import __version__
from streamt.cli import main

PROJECT_ROOT = Path(__file__).parents[2]


def _project_version() -> str:
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project_section = pyproject.split("[project]", maxsplit=1)[1].split("\n[", maxsplit=1)[0]
    match = re.search(r'^version = "([^"]+)"$', project_section, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def _metadata_version(payload: bytes) -> str:
    message = BytesParser(policy=policy.default).parsebytes(payload)
    version = message["Version"]
    assert version is not None
    return version


def test_declared_runtime_installed_and_cli_versions_match() -> None:
    expected = _project_version()

    assert expected == "0.1.0a1"
    assert __version__ == expected
    assert importlib.metadata.version("streamt") == expected

    result = CliRunner().invoke(main, ["--version"], prog_name="streamt")
    assert result.exit_code == 0
    assert result.output == f"streamt, version {expected}\n"

    completed = subprocess.run(
        [sys.executable, "-I", "-m", "streamt", "--version"],
        cwd=PROJECT_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout == f"python -m streamt, version {expected}\n"
    assert completed.stderr == ""


def test_built_distribution_metadata_matches_project_version() -> None:
    distribution_dir_value = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if distribution_dir_value is None:
        pytest.skip("built distributions are checked by the package and release jobs")

    distribution_dir = Path(distribution_dir_value)
    wheels = list(distribution_dir.glob("streamt-*.whl"))
    source_distributions = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1, wheels
    assert len(source_distributions) == 1, source_distributions

    expected = _project_version()
    with zipfile.ZipFile(wheels[0]) as wheel:
        metadata_members = [
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        ]
        assert len(metadata_members) == 1, metadata_members
        wheel_version = _metadata_version(wheel.read(metadata_members[0]))

    with tarfile.open(source_distributions[0], "r:gz") as source_distribution:
        metadata_members = [
            name for name in source_distribution.getnames() if name.endswith("/PKG-INFO")
        ]
        assert len(metadata_members) == 1, metadata_members
        extracted = source_distribution.extractfile(metadata_members[0])
        assert extracted is not None
        source_version = _metadata_version(extracted.read())

    assert wheel_version == source_version == expected


def test_built_distributions_exclude_repository_only_runtime_proof() -> None:
    distribution_dir_value = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if distribution_dir_value is None:
        pytest.skip("built distributions are checked by the package and release jobs")

    distribution_dir = Path(distribution_dir_value)
    wheels = list(distribution_dir.glob("streamt-*.whl"))
    source_distributions = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == len(source_distributions) == 1
    with zipfile.ZipFile(wheels[0]) as wheel:
        wheel_members = wheel.namelist()
    with tarfile.open(source_distributions[0], "r:gz") as source_distribution:
        source_members = source_distribution.getnames()

    # Exclude source as well as generated JARs so even a clean checkout tests
    # the repository-only boundary; local target/ outputs must not leak either.
    for names in (wheel_members, source_members):
        leaked = [name for name in names
                  if "/experiments/kafka-streams/" in f"/{name.rstrip('/')}/"]
        assert not leaked, f"Repository-only runtime proof leaked into distribution: {leaked}"
