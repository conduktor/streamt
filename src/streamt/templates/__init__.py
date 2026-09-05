"""Installed starter resources for the existing init command."""

from __future__ import annotations

import re
from importlib.resources import files
from pathlib import Path

from jinja2 import Environment, StrictUndefined
from pydantic import ValidationError

from streamt.core.runtime import KafkaStreamsConfig

_RESOURCES = {
    "stream_project.yml": "stream_project.yml.j2",
    "README.md": "README.md.j2",
    "sample_events.jsonl": "sample_events.jsonl",
}
_DIRECTORIES = ("sources", "models", "tests")


def kafka_streams_starter_resources() -> dict[str, str]:
    """Read package resources without searching a checkout or the working directory."""
    root = files("streamt").joinpath("templates").joinpath("kafka_streams")
    try:
        return {
            destination: root.joinpath(source).read_text(encoding="utf-8")
            for destination, source in _RESOURCES.items()
        }
    except (OSError, UnicodeError):
        raise ValueError(
            "Kafka Streams starter resources are missing; reinstall streamt."
        ) from None


def create_kafka_streams_starter(
    project_path: Path,
    *,
    name: str,
    runner_image: str | None,
    kafka: str | None,
    kafka_internal: str | None,
    docker_network: str | None,
    initial_offset: str | None,
    dry_run: bool,
    force: bool,
) -> list[str]:
    """Render a create-only topology, preflight collisions, then write owned destinations."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,100}", name):
        raise ValueError(
            "Use --project-name with 1-101 letters, digits, dots, hyphens or underscores, starting with a letter or digit."
        )
    if not runner_image:
        raise ValueError(
            "--runner-image is required with --executor kafka_streams. Build it explicitly with streamt runtime build, then use its immutable image ID."
        )
    try:
        runtime = KafkaStreamsConfig(
            image=runner_image,
            network=docker_network if docker_network is not None else "bridge",
            initial_offset=initial_offset or "earliest",  # type: ignore[arg-type]
        )
    except ValidationError:
        raise ValueError(
            "Kafka Streams starter requires an immutable SHA-256 image, one local Docker network name, and earliest/latest initial offsets."
        ) from None
    for address in (kafka, kafka_internal):
        if address is not None and (
            not address.strip()
            or any(c in address for c in "\r\n\x00")
            or "://" in address
            or "@" in address
        ):
            raise ValueError(
                "Kafka starter addresses must be broker addresses without URLs or embedded credentials."
            )
    context = {
        "project_name": name,
        "runner_image": runtime.image,
        "docker_network": runtime.network,
        "initial_offset": runtime.initial_offset,
        "kafka_bootstrap": kafka or "${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}",
        "kafka_internal": kafka_internal or "${KAFKA_BOOTSTRAP_SERVERS_INTERNAL:-broker:19092}",
        "raw_topic": f"{name}.raw.orders.v1",
        "output_topic": f"{name}.eligible.orders.v1",
        "consumer_group": f"{name}-fraud-app",
    }
    environment = Environment(
        undefined=StrictUndefined, autoescape=False, keep_trailing_newline=True
    )
    rendered = {
        destination: environment.from_string(payload).render(context)
        for destination, payload in kafka_streams_starter_resources().items()
    }
    created = [*rendered, *(f"{directory}/" for directory in _DIRECTORIES)]
    if dry_run:
        return created
    # All known conflicts are checked before any file is written. Force applies
    # only to the named starter files, never directory contents or symlink targets.
    for destination in rendered:
        path = project_path / destination
        if path.is_symlink() or path.is_dir():
            raise ValueError(
                "A starter destination is a directory or symlink; no files were written."
            )
        if path.exists() and not force:
            raise ValueError(
                "A starter file already exists; use a new directory or --force to replace the named starter files."
            )
    for directory in _DIRECTORIES:
        path = project_path / directory
        if path.is_symlink() or (path.exists() and not path.is_dir()):
            raise ValueError(
                "A starter directory is a symlink or non-directory; no files were written."
            )
    project_path.mkdir(parents=True, exist_ok=True)
    for destination, payload in rendered.items():
        (project_path / destination).write_text(payload, encoding="utf-8")
    for directory in _DIRECTORIES:
        path = project_path / directory
        path.mkdir(exist_ok=True)
        marker = path / ".gitkeep"
        if not marker.exists() and not marker.is_symlink():
            marker.touch()
    return created
