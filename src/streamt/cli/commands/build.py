"""streamt build command — generate self-contained deployable artifacts."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import click

from streamt.cli.helpers import get_project_path, handle_parse_error, make_formatter


@click.command("build")
@click.option("--project-dir", "-p", type=click.Path(exists=True), help="Path to project directory")
@click.option(
    "--env", "-e", "environment", help="Target environment (reads from STREAMT_ENV if not set)"
)
@click.option(
    "--output-dir", type=click.Path(), default=None, help="Output directory (default: ./build)"
)
@click.pass_context
def build(
    ctx: click.Context,
    project_dir: Optional[str],
    environment: Optional[str],
    output_dir: Optional[str],
) -> None:
    """Compile and package deployable artifacts with manifest and checksums."""
    from streamt.compiler import Compiler
    from streamt.core.parser import EnvVarError, ParseError, ProjectParser

    fmt = make_formatter(ctx, "build")

    try:
        project_path = get_project_path(project_dir)
        parser = ProjectParser(project_path, environment=environment)
        project = parser.parse()
    except (EnvVarError, ParseError) as e:
        handle_parse_error(fmt, e, "PARSE_ERROR")
        return

    out = Path(output_dir) if output_dir else project_path / "build"
    compiler = Compiler(project, out)
    manifest = compiler.compile(dry_run=False)

    # Write manifest.json
    manifest_data = manifest.to_dict()
    manifest_path = out / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f, indent=2)

    # Compute checksums for all generated files
    checksums: dict[str, str] = {}
    for path in sorted(out.rglob("*")):
        if path.is_file() and path.name != "checksums.sha256":
            rel = str(path.relative_to(out))
            checksums[rel] = hashlib.sha256(path.read_bytes()).hexdigest()

    checksum_path = out / "checksums.sha256"
    with open(checksum_path, "w") as f:
        for name, digest in checksums.items():
            f.write(f"{digest}  {name}\n")

    # Summary
    artifacts = manifest.artifacts
    summary = {
        "output_dir": str(out),
        "schemas": len(artifacts.get("schemas", [])),
        "topics": len(artifacts.get("topics", [])),
        "flink_jobs": len(artifacts.get("flink_jobs", [])),
        "connectors": len(artifacts.get("connectors", [])),
        "gateway_rules": len(artifacts.get("gateway_rules", [])),
        "files": len(checksums),
    }

    fmt.print(f"Build output: {out}")
    fmt.print(f"  Schemas: {summary['schemas']}")
    fmt.print(f"  Topics: {summary['topics']}")
    fmt.print(f"  Flink jobs: {summary['flink_jobs']}")
    if artifacts.get("kafka_streams_jobs"):
        summary["kafka_streams_jobs"] = len(artifacts["kafka_streams_jobs"])
        fmt.print(f"  Kafka Streams jobs: {summary['kafka_streams_jobs']}")
    fmt.print(f"  Connectors: {summary['connectors']}")
    fmt.print(f"  Gateway rules: {summary['gateway_rules']}")
    fmt.print(f"  Total files: {summary['files']}")

    fmt.set_data(summary)
    fmt.flush()
