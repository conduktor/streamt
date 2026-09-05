"""Build the supplied fixed runner on the explicitly selected local Docker daemon."""

from __future__ import annotations

import re
from pathlib import Path

import click

from streamt.cli.helpers import make_formatter
from streamt.deployer.kafka_streams_docker import KafkaStreamsDockerError, LocalDockerRunner
from streamt.output import StructuredError
from streamt.runtime_assets import (
    RuntimeAssetsError,
    load_runtime_build_inputs,
    materialize_runtime_build_context,
    runtime_build_contract,
)


class _RuntimeImageBuilder(LocalDockerRunner):
    """Explicit build is the only command that authorizes fetching pinned bases."""

    def build(self, context: Path, *, timeout: int) -> str:
        # An inherited custom Buildx builder/source policy must not turn a local
        # Docker endpoint into a remote build or replace the packaged image pins.
        for key in list(self._environment):
            if key.startswith(("BUILDX_", "BUILDKIT_")) or key in {
                "EXPERIMENTAL_BUILDKIT_SOURCE_POLICY",
                "DOCKER_DEFAULT_PLATFORM",
            }:
                self._environment.pop(key)
        self._environment["DOCKER_BUILDKIT"] = "1"
        self.verify_daemon()
        # Keep the output identifier outside the directory transmitted to Docker.
        image_file = context.parent / f"{context.name}.image-id"
        try:
            self._run(
                [
                    "build",
                    "--builder",
                    "default",
                    "--iidfile",
                    str(image_file),
                    str(context),
                ],
                timeout=timeout,
            )
            image_id = image_file.read_text(encoding="ascii").strip()
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
                raise KafkaStreamsDockerError("Docker build did not return an immutable image ID")
            self.verify_daemon()
            return self.image_id(image_id)
        except (OSError, UnicodeError):
            raise KafkaStreamsDockerError("Cannot read the built Docker image identity") from None
        finally:
            image_file.unlink(missing_ok=True)


@click.group()
def runtime() -> None:
    """Build the maintained Kafka Streams runner image locally."""


@runtime.command("build")
@click.option("--dry-run", is_flag=True, help="Describe packaged inputs without Docker or writes.")
@click.option(
    "--timeout",
    type=click.IntRange(30, 1800),
    default=600,
    show_default=True,
    help="Maximum seconds for the Docker build (daemon checks have separate short bounds).",
)
@click.pass_context
def build_runtime(ctx: click.Context, dry_run: bool, timeout: int) -> None:
    """Build packaged sources; download pinned bases/dependencies, never push or tag.

    No project or Kafka connection is needed. The result is a local immutable
    image ID for runtime.kafka_streams.image. Docker Engine and Buildx are required.
    """
    fmt = make_formatter(ctx, "runtime build")
    try:
        inputs = load_runtime_build_inputs()
        data = runtime_build_contract(inputs)
        data.update({"dry_run": dry_run, "timeout_seconds": timeout, "image": None})
        if dry_run:
            fmt.print("Dry run: packaged Kafka Streams runner; no Docker access or file writes.")
            fmt.print(f"Runner {data['runner_version']}; plan version {data['plan_version']}.")
            fmt.print(
                "An actual build downloads pinned base images and Maven dependencies locally."
            )
        else:
            builder = _RuntimeImageBuilder()
            fmt.progress("Building the packaged Kafka Streams runner on the local Docker daemon.")
            with materialize_runtime_build_context(inputs) as context:
                image_id = builder.build(context, timeout=timeout)
            data["image"] = image_id
            data["configuration"] = {"runtime": {"kafka_streams": {"image": image_id}}}
            fmt.print(image_id)
            fmt.print("Set runtime.kafka_streams.image to this immutable local image ID.")
        fmt.set_data(data)
    except (RuntimeAssetsError, KafkaStreamsDockerError, OSError):
        # Provider errors, temp paths and subprocess output are not public diagnostics.
        message = (
            "Kafka Streams runtime build failed. Check the installed assets, local Docker "
            "Engine/Buildx, available disk space, and build network access. No image was published."
        )
        fmt.add_error(StructuredError(code="RUNTIME_BUILD_FAILED", message=message))
        fmt.print_error(message)
        fmt.flush()
        ctx.exit(1)
    fmt.flush()
