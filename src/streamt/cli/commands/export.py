"""Deterministic, offline GitOps exports."""

from __future__ import annotations

import logging
import os
import re
import stat
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, NoReturn

import click

from streamt.core.errors import ErrorCode
from streamt.output import OutputFormatter, StructuredError, StructuredWarning

if TYPE_CHECKING:
    from streamt.integrations.gitops.strimzi import (
        StrimziExportWarning,
        StrimziKafkaTopicExport,
    )

_DNS1123_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.ASCII)
_FAILURE_MESSAGE = "Strimzi export failed safely"
_MAPPER_LOCATIONS = frozenset(
    {
        "project",
        "manifest_checksum",
        "target",
        "artifacts",
        "artifacts/schemas",
        "artifacts/topics",
        "artifacts/flink_jobs",
        "artifacts/test_jobs",
        "artifacts/connectors",
        "artifacts/connector_removals",
        "artifacts/gateway_rules",
        "artifacts/gateway_rule_removals",
        "documents",
        "export.manifest_checksum",
        "export.counts",
        "export.documents",
        "export.warnings",
        "export.yaml",
    }
)


@click.group()
def export() -> None:
    """Export deterministic offline GitOps artifacts."""


class _StrimziCommandError(ValueError):
    """A fixed, secret-neutral command failure."""

    def __init__(self, location: str) -> None:
        super().__init__(_FAILURE_MESSAGE)
        self.location = location


class _StdoutCommandError(_StrimziCommandError):
    """A stdout failure carrying only whether the stream may be tainted."""

    def __init__(self, *, may_have_output: bool) -> None:
        super().__init__("stdout")
        self.may_have_output = may_have_output


def _formatter(ctx: click.Context) -> OutputFormatter:
    root = ctx
    while root.parent is not None:
        root = root.parent
    configured = root.obj if isinstance(root.obj, dict) else {}
    output = configured.get("output", "text")
    quiet = configured.get("quiet", False)
    fmt = OutputFormatter(
        output if isinstance(output, str) else "text",
        quiet=quiet if isinstance(quiet, bool) else False,
    )
    fmt.set_command("export strimzi")
    return fmt


def _require_target(value: str | None, *, location: str) -> str:
    if value is None or _DNS1123_LABEL.fullmatch(value) is None:
        raise _StrimziCommandError(location)
    return value


def _mapper_location(value: object) -> str:
    return value if type(value) is str and value in _MAPPER_LOCATIONS else "export"


@contextmanager
def _suppress_project_logging() -> Iterator[None]:
    """Suppress parser/compiler logs even when the root command is verbose."""
    previous = logging.root.manager.disable
    logging.disable(sys.maxsize)
    try:
        yield
    finally:
        logging.disable(previous)


def _project_path(project_dir: str | None) -> Path:
    try:
        return Path(project_dir).resolve() if project_dir is not None else Path.cwd()
    except Exception:
        raise _StrimziCommandError("project") from None


def _destination_state(path: Path) -> tuple[int, int, int] | None:
    try:
        status = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(status.st_mode):
        raise _StrimziCommandError("output_file")
    return (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode))


def _staging_state(path: Path) -> tuple[int, int, int]:
    try:
        status = path.lstat()
    except OSError:
        raise _StrimziCommandError("output_file") from None
    if not stat.S_ISREG(status.st_mode) or stat.S_IMODE(status.st_mode) != 0o600:
        raise _StrimziCommandError("output_file")
    return (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode))


def _close_stream_quietly(stream: BinaryIO) -> None:
    try:
        stream.close()
    except BaseException:
        pass


def _close_fd_quietly(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except BaseException:
        pass


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except BaseException:
        pass


def _atomic_write(path: Path, content: bytes) -> None:
    """Durably replace one regular destination via a private same-dir stage."""
    stage: Path | None = None
    descriptor = -1
    stream: BinaryIO | None = None
    stage_identity: tuple[int, int, int] | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        initial = _destination_state(path)
        descriptor, stage_name = tempfile.mkstemp(
            prefix=f".{path.name}.streamt-strimzi-",
            suffix=".tmp",
            dir=path.parent,
        )
        stage = Path(stage_name)
        os.fchmod(descriptor, 0o600)
        staged = os.fstat(descriptor)
        if not stat.S_ISREG(staged.st_mode) or stat.S_IMODE(staged.st_mode) != 0o600:
            raise _StrimziCommandError("output_file")
        stage_identity = (staged.st_dev, staged.st_ino, stat.S_IFMT(staged.st_mode))
        stream = os.fdopen(descriptor, "wb", closefd=False)
        written = stream.write(content)
        if written != len(content):
            raise OSError("short staging write")
        stream.flush()
        os.fsync(descriptor)
        stream.close()
        stream = None
        os.close(descriptor)
        descriptor = -1

        current = _destination_state(path)
        if current != initial:
            raise _StrimziCommandError("output_file")
        if _staging_state(stage) != stage_identity:
            raise _StrimziCommandError("output_file")
        os.replace(stage, path)
        stage = None
    except BaseException:
        if stream is not None:
            _close_stream_quietly(stream)
        if descriptor >= 0:
            _close_fd_quietly(descriptor)
        if stage is not None:
            _unlink_quietly(stage)
        raise


def _result_data(
    result: StrimziKafkaTopicExport,
    *,
    output_file: str | None,
) -> dict[str, object]:
    return {
        "target_release": result.target_release,
        "api_version": result.api_version,
        "kind": result.kind,
        "manifest_checksum": result.manifest_checksum,
        "documents": list(result.documents),
        "counts": result.counts.to_dict(),
        "output_file": output_file,
    }


def _capture_warnings(
    fmt: OutputFormatter,
    warnings: tuple[StrimziExportWarning, ...],
) -> tuple[str, ...]:
    messages: list[str] = []
    for warning in warnings:
        fmt.add_warning(
            StructuredWarning(
                code=warning.code,
                message=warning.message,
                location=warning.location,
            )
        )
        messages.append(warning.message)
    return tuple(messages)


def _write_stdout(content: bytes) -> None:
    try:
        output = sys.stdout.buffer
    except Exception:
        raise _StdoutCommandError(may_have_output=False) from None

    try:
        written = output.write(content)
    except Exception:
        # A stream may accept a prefix before reporting an error. Never append
        # a JSON error envelope to a channel that might already contain bytes.
        raise _StdoutCommandError(may_have_output=True) from None
    if type(written) is not int:
        raise _StdoutCommandError(may_have_output=True) from None
    if written != len(content):
        raise _StdoutCommandError(may_have_output=written != 0) from None
    try:
        output.flush()
    except Exception:
        raise _StdoutCommandError(may_have_output=bool(content)) from None


def _flush_success(fmt: OutputFormatter) -> None:
    if fmt.format != "json":
        fmt.flush()
        return
    try:
        payload = (fmt.get_result().to_json() + "\n").encode("utf-8")
    except Exception:
        raise _StdoutCommandError(may_have_output=False) from None
    _write_stdout(payload)


def _fail(
    fmt: OutputFormatter,
    *,
    location: str,
    stdout_may_have_output: bool = False,
) -> NoReturn:
    result = fmt.get_result()
    result.data.clear()
    result.errors.clear()
    result.warnings.clear()
    result.status = "ok"
    fmt.add_error(
        StructuredError(
            code=ErrorCode.STRIMZI_INVALID,
            message=_FAILURE_MESSAGE,
            location=location,
        )
    )
    try:
        fmt.print_error(_FAILURE_MESSAGE)
    except Exception:
        pass
    # Retrying a JSON envelope is safe only before the original stream could
    # have accepted bytes. Otherwise it would concatenate success/partial data
    # with an error document and make the channel unparseable.
    if fmt.format != "json" or not stdout_may_have_output:
        try:
            fmt.flush()
        except Exception:
            pass
    raise click.exceptions.Exit(1)


@export.command("strimzi")
@click.option("--namespace", help="Kubernetes namespace for KafkaTopic resources")
@click.option("--cluster-name", help="Strimzi Kafka cluster label value")
@click.option("--output-file", type=click.Path(), help="Atomically write canonical YAML")
@click.option("--project-dir", type=click.Path(), help="Path to project directory")
@click.option("--env", "environment", help="Target environment")
@click.pass_context
def strimzi(
    ctx: click.Context,
    namespace: str | None,
    cluster_name: str | None,
    output_file: str | None,
    project_dir: str | None,
    environment: str | None,
) -> None:
    """Export managed topics as validated Strimzi KafkaTopic YAML."""
    fmt = _formatter(ctx)
    failure_location: str | None = None
    stdout_may_have_output = False
    try:
        exact_namespace = _require_target(namespace, location="target.namespace")
        exact_cluster = _require_target(cluster_name, location="target.cluster_name")
        if fmt.quiet and output_file is None:
            raise _StrimziCommandError("output")

        # Imports stay behind the command boundary so command discovery and
        # completion remain free of compiler, runtime, and deployment layers.
        try:
            from streamt.compiler import Compiler
            from streamt.core.environment import EnvironmentError
            from streamt.core.manifest_identity import (
                ManifestIdentityError,
                manifest_checksum,
            )
            from streamt.core.parser import EnvVarError, ParseError, ProjectParser
            from streamt.core.validator import ProjectValidator
            from streamt.integrations.gitops.strimzi import (
                StrimziExportError,
                StrimziExportTarget,
                generate_strimzi_export,
            )
        except Exception:
            raise _StrimziCommandError("export") from None

        with _suppress_project_logging():
            project_path = _project_path(project_dir)
            try:
                parser = ProjectParser(
                    project_path,
                    environment=environment,
                    warn_callback=lambda _message: None,
                )
                project = parser.parse()
                validation = ProjectValidator(project).validate()
                if not validation.is_valid:
                    raise _StrimziCommandError("project")
            except _StrimziCommandError:
                raise
            except (EnvVarError, EnvironmentError, ParseError):
                raise _StrimziCommandError("project") from None
            except Exception:
                raise _StrimziCommandError("project") from None

            try:
                manifest = Compiler(project).compile(dry_run=True)
            except Exception:
                raise _StrimziCommandError("manifest") from None

        try:
            checksum = manifest_checksum(manifest)
        except ManifestIdentityError:
            raise _StrimziCommandError("manifest_checksum") from None
        except Exception:
            raise _StrimziCommandError("manifest_checksum") from None

        try:
            target = StrimziExportTarget(
                namespace=exact_namespace,
                cluster_name=exact_cluster,
            )
            generated = generate_strimzi_export(
                manifest.artifacts,
                project_name=manifest.project_name,
                manifest_checksum=checksum,
                target=target,
            )
        except StrimziExportError as error:
            raise _StrimziCommandError(_mapper_location(error.location)) from None
        except Exception:
            raise _StrimziCommandError("export") from None

        try:
            data = _result_data(generated, output_file=output_file)
            warning_messages = _capture_warnings(fmt, generated.warnings)
            yaml_bytes = generated.yaml_bytes
            fmt.set_data(data)
        except Exception:
            raise _StrimziCommandError("export") from None

        if output_file is not None:
            try:
                _atomic_write(Path(output_file), yaml_bytes)
            except _StrimziCommandError:
                raise
            except Exception:
                raise _StrimziCommandError("output_file") from None
        elif fmt.format == "text" and not fmt.quiet:
            _write_stdout(yaml_bytes)

        if fmt.format == "text" and not fmt.quiet:
            for message in warning_messages:
                fmt.stderr.print(f"[yellow]WARNING[/yellow]: {message}")

        if not fmt.quiet:
            try:
                _flush_success(fmt)
            except _StdoutCommandError:
                raise
            except Exception:
                raise _StdoutCommandError(may_have_output=False) from None
    except _StdoutCommandError as error:
        failure_location = error.location
        stdout_may_have_output = error.may_have_output
    except _StrimziCommandError as error:
        # Retain only the closed structural location. In particular, do not
        # carry Python's implicit exception context into Click's returned
        # SystemExit chain, where parser/configuration exception text would
        # otherwise remain inspectable even though it was not printed.
        failure_location = error.location
    except Exception:
        failure_location = "export"

    if failure_location is not None:
        _fail(
            fmt,
            location=failure_location,
            stdout_may_have_output=stdout_may_have_output,
        )


__all__ = ["export", "strimzi"]
