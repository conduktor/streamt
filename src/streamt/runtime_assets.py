"""Packaged, pinned build inputs for the maintained Kafka Streams runner."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from importlib.abc import Traversable

RUNNER_VERSION = "0.1.1"
PLAN_VERSION = 1
_ROOT_FILES = ("Dockerfile", "pom.xml", "LICENSE", "images.lock.json")
_SOURCE_TREES = ("src/main", "src/test")
_PINNED_IMAGE = re.compile(r"^[a-z0-9][a-zA-Z0-9._/:\-]*@sha256:[0-9a-f]{64}$")


class RuntimeAssetsError(ValueError):
    """A fixed, secret-neutral package resource error."""


def runtime_build_assets() -> Traversable:
    """Read installed resources, or the exact module-relative editable source tree.

    Never search the working directory or the historical experiment. Missing
    resources in an installed distribution are an error, not a source fallback.
    """
    packaged = files("streamt").joinpath("_runtime_assets").joinpath("kafka_streams")
    if packaged.is_dir():
        return packaged
    module = Path(__file__).resolve()
    repository = module.parents[2]
    if (
        module.parent.name == "streamt"
        and module.parent.parent.name == "src"
        and (repository / "pyproject.toml").is_file()
    ):
        source = repository / "runtimes" / "kafka-streams"
        if source.is_dir() and not source.is_symlink():
            return source
    raise RuntimeAssetsError("Kafka Streams build assets are missing; reinstall streamt")


def _read_tree(root: Traversable, prefix: str) -> Iterator[tuple[str, bytes]]:
    for member in sorted(root.iterdir(), key=lambda item: item.name):
        if member.name in {"", ".", ".."} or "/" in member.name or "\\" in member.name:
            raise RuntimeAssetsError("Invalid Kafka Streams build resource")
        if isinstance(member, Path) and member.is_symlink():
            raise RuntimeAssetsError("Symlinks are not accepted in Kafka Streams build inputs")
        name = f"{prefix}/{member.name}"
        if member.is_dir():
            yield from _read_tree(member, name)
        elif member.is_file():
            yield name, member.read_bytes()
        else:
            raise RuntimeAssetsError("Invalid Kafka Streams build resource")


def load_runtime_build_inputs() -> dict[str, bytes]:
    """Snapshot the same allowlisted source bytes in editable and wheel installs."""
    try:
        root = runtime_build_assets()
        inputs: dict[str, bytes] = {}
        for name in _ROOT_FILES:
            member = root.joinpath(name)
            if isinstance(member, Path) and member.is_symlink():
                raise RuntimeAssetsError("Symlinks are not accepted in Kafka Streams build inputs")
            inputs[name] = member.read_bytes()
        for prefix in _SOURCE_TREES:
            directory = root
            for part in prefix.split("/"):
                directory = directory.joinpath(part)
                if isinstance(directory, Path) and directory.is_symlink():
                    raise RuntimeAssetsError(
                        "Symlinks are not accepted in Kafka Streams build inputs"
                    )
            members = dict(_read_tree(directory, prefix))
            if not members:
                raise RuntimeAssetsError("Kafka Streams Java build sources are missing")
            inputs.update(members)
        runtime_build_contract(inputs)
        return dict(sorted(inputs.items()))
    except (OSError, UnicodeError):
        raise RuntimeAssetsError("Cannot read Kafka Streams build assets") from None


def runtime_build_contract(inputs: Mapping[str, bytes]) -> dict[str, object]:
    """Describe the local build without creating files, processes, or clients."""
    try:
        lock = json.loads(inputs["images.lock.json"])
        images = [lock["builder"], lock["runtime"]]
        if any(not isinstance(ref, str) or not _PINNED_IMAGE.fullmatch(ref) for ref in images):
            raise ValueError
        declared = re.findall(r"^FROM (\S+)", inputs["Dockerfile"].decode(), re.MULTILINE)
        if declared != images:
            raise ValueError
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise RuntimeAssetsError(
            "Kafka Streams base-image pins are missing or inconsistent"
        ) from None
    digest = hashlib.sha256()
    for name, payload in sorted(inputs.items()):
        # Length framing makes filenames and contents unambiguous.
        encoded = name.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return {
        "executor": "kafka_streams",
        "runner_version": RUNNER_VERSION,
        "plan_version": PLAN_VERSION,
        "build_context_sha256": f"sha256:{digest.hexdigest()}",
        "build_context_files": len(inputs),
        "base_images": images,
        "builder": "local-docker-default",
        "publishes_image": False,
        "tags_image": False,
        "network_access_on_build": True,
    }


@contextmanager
def materialize_runtime_build_context(inputs: Mapping[str, bytes]) -> Iterator[Path]:
    """Write only the supplied package inputs to an owned temporary directory."""
    with TemporaryDirectory(prefix="streamt-runtime-build-") as temporary:
        root = Path(temporary) / "context"
        root.mkdir(mode=0o700)
        for name, payload in inputs.items():
            parts = PurePosixPath(name).parts
            if (
                not parts
                or any(part in {"", ".", ".."} for part in parts)
                or name.startswith("/")
                or "\\" in name
                or (name not in _ROOT_FILES and not name.startswith(("src/main/", "src/test/")))
            ):
                raise RuntimeAssetsError("Invalid Kafka Streams build resource path")
            destination = root.joinpath(*parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
        yield root
