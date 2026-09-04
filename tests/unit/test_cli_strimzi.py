"""CLI contract tests for deterministic offline Strimzi export."""

from __future__ import annotations

import json
import logging
import os
import stat
import subprocess
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.testing import CliRunner

import streamt.cli.commands.export as export_command
from streamt.cli import main
from streamt.integrations.gitops.strimzi import StrimziExportError

FIXTURES = Path(__file__).parents[1] / "fixtures" / "strimzi" / "1.2.0"
CONTRACT = json.loads((FIXTURES / "contract.json").read_text(encoding="utf-8"))
MANIFEST = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
EXPECTED_YAML = (FIXTURES / "expected.yaml").read_bytes()
EXPECTED_DOCUMENTS = json.loads(
    (FIXTURES / "expected-documents.json").read_text(encoding="utf-8")
)


class _Manifest:
    def __init__(self, value: dict[str, object]) -> None:
        self._value = value
        self.artifacts = value["artifacts"]
        self.project_name = value["project"]

    def to_dict(self) -> dict[str, object]:
        return self._value


class _Validation:
    def __init__(self, valid: bool = True) -> None:
        self.is_valid = valid


@contextmanager
def _pipeline(
    manifest_data: dict[str, object] = MANIFEST,
    *,
    parsed_project_name: object | None = None,
    valid: bool = True,
    parser_error: BaseException | None = None,
    compile_error: BaseException | None = None,
) -> Iterator[SimpleNamespace]:
    calls = SimpleNamespace(parse=0, validate=0, compile=0, dry_runs=[])
    project_name = (
        manifest_data["project"] if parsed_project_name is None else parsed_project_name
    )
    project = SimpleNamespace(project=SimpleNamespace(name=project_name))
    manifest = _Manifest(manifest_data)

    class Parser:
        def __init__(self, *_args: object, **kwargs: object) -> None:
            self.env_config = None
            warning = kwargs.get("warn_callback")
            if callable(warning):
                warning("PARSER_WARNING_SECRET_MUST_NOT_APPEAR")

        def parse(self) -> object:
            calls.parse += 1
            logging.getLogger("streamt.test.parser").warning(
                "PARSER_LOG_SECRET_MUST_NOT_APPEAR"
            )
            if parser_error is not None:
                raise parser_error
            return project

    class Validator:
        def __init__(self, _project: object) -> None:
            pass

        def validate(self) -> _Validation:
            calls.validate += 1
            return _Validation(valid)

    class Compiler:
        def __init__(self, _project: object) -> None:
            pass

        def compile(self, *, dry_run: bool) -> _Manifest:
            calls.compile += 1
            calls.dry_runs.append(dry_run)
            logging.getLogger("streamt.test.compiler").warning(
                "COMPILER_LOG_SECRET_MUST_NOT_APPEAR"
            )
            if compile_error is not None:
                raise compile_error
            return manifest

    with ExitStack() as stack:
        stack.enter_context(patch("streamt.core.parser.ProjectParser", Parser))
        stack.enter_context(patch("streamt.core.validator.ProjectValidator", Validator))
        stack.enter_context(patch("streamt.compiler.Compiler", Compiler))
        yield calls


def _args(*extra: str) -> list[str]:
    return [
        "export",
        "strimzi",
        "--namespace",
        CONTRACT["namespace"],
        "--cluster-name",
        CONTRACT["cluster_name"],
        *extra,
    ]


def _error_envelope(result: object) -> dict[str, object]:
    return json.loads(result.stdout)  # type: ignore[attr-defined]


def _exception_representations(error: BaseException | None) -> str:
    """Render every reachable exception without trusting suppress-context."""
    pending = [error] if error is not None else []
    seen: set[int] = set()
    rendered: list[str] = []
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        rendered.extend((repr(current), str(current)))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return "\n".join(rendered)


def test_help_exposes_only_the_frozen_surface() -> None:
    top = CliRunner().invoke(main, ["--help"])
    group = CliRunner().invoke(main, ["export", "--help"])
    command = CliRunner().invoke(main, ["export", "strimzi", "--help"])
    assert top.exit_code == group.exit_code == command.exit_code == 0
    assert "export" in top.output
    assert "strimzi" in group.output
    for option in ("--namespace", "--cluster-name", "--output-file", "--project-dir", "--env"):
        assert option in command.output
    for forbidden in ("--apply", "--context", "--bootstrap-servers", "--force", "--prune"):
        assert forbidden not in command.output


def test_fresh_cli_and_export_imports_do_not_load_forbidden_layers() -> None:
    code = """
import sys
import streamt.cli
for prefix in ('streamt.deployer', 'streamt.planner', 'streamt.providers',
               'streamt.state', 'streamt.core.runtime',
               'streamt.core.deployment_state', 'streamt.cli.commands.'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
import streamt.cli.commands.export
for prefix in ('streamt.deployer', 'streamt.planner', 'streamt.providers',
               'streamt.state', 'streamt.core.runtime',
               'streamt.core.deployment_state', 'streamt.compiler',
               'streamt.integrations.gitops'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
for module in ('subprocess', 'socket', 'requests', 'urllib.request'):
    assert module not in sys.modules, module
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.parametrize(
    ("name", "module", "attribute"),
    [
        ("adopt", "streamt.cli.commands.adopt", "adopt"),
        ("list", "streamt.cli.commands.list_cmd", "list_resources"),
        ("show", "streamt.cli.commands.show", "show_resource"),
        ("diff", "streamt.cli.commands.diff", "diff_resources"),
        ("import", "streamt.cli.commands.import_cmd", "import_resources"),
        ("export", "streamt.cli.commands.export", "export"),
    ],
)
def test_lazy_registry_preserves_resolved_command_identity(
    name: str, module: str, attribute: str
) -> None:
    import importlib

    resolved = main.get_command(main.make_context("streamt", [], resilient_parsing=True), name)
    assert resolved is getattr(importlib.import_module(module), attribute)
    assert main.commands[name] is resolved


def test_text_success_is_exact_yaml_with_stderr_only_warnings() -> None:
    with _pipeline() as calls:
        first = CliRunner().invoke(main, _args())
        second = CliRunner().invoke(main, _args())
    assert first.exit_code == second.exit_code == 0, first.output
    assert first.stdout.encode() == second.stdout.encode() == EXPECTED_YAML
    assert first.stderr.count("WARNING") == second.stderr.count("WARNING") == 2
    positions = [
        first.stderr.index("Non-topic artifacts omitted from Strimzi export"),
        first.stderr.index("External topic artifact omitted from Strimzi export"),
    ]
    assert positions == sorted(positions)
    assert calls.parse == calls.validate == calls.compile == 2
    assert calls.dry_runs == [True, True]
    for secret in (
        "fixture-private-token-alpha",
        "External_Audit_v1",
        "external_audit",
        "PARSER_WARNING_SECRET",
        "PARSER_LOG_SECRET",
        "COMPILER_LOG_SECRET",
    ):
        assert secret not in first.stdout + first.stderr


def test_verbose_still_suppresses_parser_and_compiler_logging() -> None:
    with _pipeline():
        result = CliRunner().invoke(main, ["--verbose", *_args()])
    assert result.exit_code == 0
    assert "PARSER_LOG_SECRET" not in result.stdout + result.stderr
    assert "COMPILER_LOG_SECRET" not in result.stdout + result.stderr


def test_json_success_is_one_exact_envelope_and_stderr_is_empty() -> None:
    with _pipeline() as calls:
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    assert result.exit_code == 0, result.output
    assert result.stderr == ""
    assert json.loads(result.stdout) == {
        "status": "ok",
        "command": "export strimzi",
        "data": {
            "target_release": CONTRACT["target_release"],
            "api_version": CONTRACT["api_version"],
            "kind": CONTRACT["kind"],
            "manifest_checksum": CONTRACT["manifest_checksums"]["baseline"],
            "documents": EXPECTED_DOCUMENTS,
            "counts": CONTRACT["counts"],
            "output_file": None,
        },
        "errors": [],
        "warnings": CONTRACT["warnings"],
    }
    assert calls.compile == 1


def test_mapper_uses_the_compiled_manifest_project_identity() -> None:
    with _pipeline(parsed_project_name="different-parser-project"):
        result = CliRunner().invoke(main, _args())
    assert result.exit_code == 0, result.output
    assert result.stdout.encode() == EXPECTED_YAML
    assert "different-parser-project" not in result.stdout + result.stderr


@pytest.mark.parametrize("json_mode", [False, True])
def test_file_success_is_atomic_and_retains_exact_lexical_argument(
    tmp_path: Path, json_mode: bool
) -> None:
    lexical = str(tmp_path / "new" / ".." / "new" / "topics.yaml")
    arguments = (["--output", "json"] if json_mode else []) + _args(
        "--output-file", lexical
    )
    with _pipeline():
        result = CliRunner().invoke(main, arguments)
    assert result.exit_code == 0, result.output
    assert Path(lexical).read_bytes() == EXPECTED_YAML
    assert stat.S_IMODE(Path(lexical).stat().st_mode) == 0o600
    if json_mode:
        assert json.loads(result.stdout)["data"]["output_file"] == lexical
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert result.stderr.count("WARNING") == 2
    assert not list(Path(lexical).parent.glob("*.tmp"))


@pytest.mark.parametrize("json_mode", [False, True])
def test_quiet_file_success_has_no_output(tmp_path: Path, json_mode: bool) -> None:
    target = tmp_path / "topics.yaml"
    prefix = ["--quiet"] + (["--output", "json"] if json_mode else [])
    with _pipeline():
        result = CliRunner().invoke(
            main, [*prefix, *_args("--output-file", str(target))]
        )
    assert result.exit_code == 0
    assert result.stdout == result.stderr == ""
    assert target.read_bytes() == EXPECTED_YAML


def test_empty_export_is_exactly_zero_bytes() -> None:
    empty = json.loads((FIXTURES / "empty-manifest.json").read_text(encoding="utf-8"))
    with _pipeline(empty):
        text = CliRunner().invoke(main, _args())
        structured = CliRunner().invoke(main, ["--output", "json", *_args()])
    assert text.exit_code == structured.exit_code == 0
    assert text.stdout == text.stderr == ""
    data = json.loads(structured.stdout)
    assert data["data"]["documents"] == []
    assert data["data"]["counts"] == CONTRACT["empty_counts"]
    assert data["warnings"] == []


@pytest.mark.parametrize(
    ("arguments", "location"),
    [
        (["export", "strimzi"], "target.namespace"),
        (["export", "strimzi", "--namespace", "", "--cluster-name", "valid"], "target.namespace"),
        (["export", "strimzi", "--namespace", "Upper", "--cluster-name", "valid"], "target.namespace"),
        (["export", "strimzi", "--namespace", "valid"], "target.cluster_name"),
        (["export", "strimzi", "--namespace", "valid", "--cluster-name", "bad_name"], "target.cluster_name"),
        (["--quiet", "export", "strimzi", "--namespace", "valid", "--cluster-name", "cluster"], "output"),
    ],
)
def test_primitive_failures_precede_all_project_work(
    arguments: list[str], location: str
) -> None:
    with patch.object(export_command, "_project_path", side_effect=AssertionError("PROJECT_SECRET")):
        result = CliRunner().invoke(main, ["--output", "json", *arguments])
    assert result.exit_code == 1
    envelope = _error_envelope(result)
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert envelope["errors"] == [
        {
            "code": "E509_STRIMZI_INVALID",
            "message": "Strimzi export failed safely",
            "location": location,
        }
    ]
    assert result.stderr == "ERROR: Strimzi export failed safely\n"
    assert "PROJECT_SECRET" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    ("valid", "parser_error", "compile_error", "location"),
    [
        (False, None, None, "project"),
        (True, ValueError("PARSE_EXCEPTION_SECRET"), None, "project"),
        (True, None, ValueError("COMPILE_EXCEPTION_SECRET"), "manifest"),
    ],
)
def test_project_and_manifest_failures_are_fixed_and_secret_neutral(
    valid: bool,
    parser_error: BaseException | None,
    compile_error: BaseException | None,
    location: str,
) -> None:
    with _pipeline(valid=valid, parser_error=parser_error, compile_error=compile_error) as calls:
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    assert result.exit_code == 1
    envelope = _error_envelope(result)
    assert envelope["errors"][0]["location"] == location
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert "EXCEPTION_SECRET" not in result.stdout + result.stderr
    assert calls.compile == (1 if location == "manifest" else 0)


def test_checksum_failure_uses_checksum_phase_and_clears_all_material() -> None:
    from streamt.core import manifest_identity

    with _pipeline(), patch.object(
        manifest_identity,
        "manifest_checksum",
        side_effect=ValueError("CHECKSUM_SECRET_MUST_NOT_APPEAR"),
    ):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    envelope = _error_envelope(result)
    assert result.exit_code == 1
    assert envelope["errors"][0]["location"] == "manifest_checksum"
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert "CHECKSUM_SECRET" not in result.stdout + result.stderr


@pytest.mark.parametrize(
    ("mapper_location", "expected"),
    [
        ("artifacts/topics", "artifacts/topics"),
        ("export.documents", "export.documents"),
        ("target.namespace", "export"),
        ("PRIVATE_LOCATION_SECRET", "export"),
        (None, "export"),
    ],
)
def test_mapper_location_is_closed_and_safe(mapper_location: object, expected: str) -> None:
    import streamt.integrations.gitops.strimzi as mapper

    error = StrimziExportError("MAPPER_SECRET_MUST_NOT_APPEAR", location="unused")
    error.location = mapper_location  # type: ignore[assignment]
    with _pipeline(), patch.object(mapper, "generate_strimzi_export", side_effect=error):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    envelope = _error_envelope(result)
    assert result.exit_code == 1
    assert envelope["errors"][0]["location"] == expected
    assert "MAPPER_SECRET" not in result.stdout + result.stderr
    assert "PRIVATE_LOCATION_SECRET" not in result.stdout + result.stderr


def test_late_file_failure_clears_documents_and_warnings(tmp_path: Path) -> None:
    existing = tmp_path / "topics.yaml"
    existing.write_bytes(b"ORIGINAL")
    with _pipeline(), patch.object(
        export_command,
        "_atomic_write",
        side_effect=OSError("ATOMIC_SECRET_MUST_NOT_APPEAR"),
    ):
        result = CliRunner().invoke(
            main,
            ["--output", "json", *_args("--output-file", str(existing))],
        )
    envelope = _error_envelope(result)
    assert result.exit_code == 1
    assert existing.read_bytes() == b"ORIGINAL"
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert envelope["errors"][0]["location"] == "output_file"
    for forbidden in ("ATOMIC_SECRET", "orders-ready-v1", "payments-streaming"):
        assert forbidden not in result.stdout + result.stderr


def test_late_stdout_failure_clears_documents_and_warnings() -> None:
    with _pipeline(), patch.object(
        export_command,
        "_write_stdout",
        side_effect=export_command._StrimziCommandError("stdout"),
    ):
        result = CliRunner().invoke(main, _args())
    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr == "ERROR: Strimzi export failed safely\n"
    for forbidden in ("orders-ready-v1", "payments-streaming", "WARNING"):
        assert forbidden not in result.stdout + result.stderr


def test_json_prewrite_failure_maps_to_stdout_and_emits_clean_error() -> None:
    with _pipeline(), patch.object(
        export_command,
        "_write_stdout",
        side_effect=export_command._StdoutCommandError(may_have_output=False),
    ):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    envelope = _error_envelope(result)
    assert result.exit_code == 1
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert envelope["errors"][0]["location"] == "stdout"


def test_json_postwrite_failure_does_not_append_a_second_envelope() -> None:
    def write_then_fail(content: bytes) -> None:
        sys.stdout.buffer.write(content)
        raise export_command._StdoutCommandError(may_have_output=True)

    with _pipeline(), patch.object(
        export_command,
        "_write_stdout",
        side_effect=write_then_fail,
    ):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    envelope = json.loads(result.stdout)
    assert result.exit_code == 1
    assert envelope["status"] == "ok"
    assert envelope["data"]["documents"] == EXPECTED_DOCUMENTS
    assert result.stdout.count('"command": "export strimzi"') == 1
    assert result.stderr == "ERROR: Strimzi export failed safely\n"
    assert "E509_STRIMZI_INVALID" not in result.stdout


def test_unexpected_exception_is_contained_without_exception_text() -> None:
    with _pipeline(), patch.object(
        export_command,
        "_result_data",
        side_effect=RuntimeError("UNEXPECTED_SECRET_MUST_NOT_APPEAR"),
    ):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    envelope = _error_envelope(result)
    assert result.exit_code == 1
    assert envelope["errors"][0]["location"] == "export"
    assert envelope["data"] == {}
    assert envelope["warnings"] == []
    assert "UNEXPECTED_SECRET" not in result.stdout + result.stderr
    assert "UNEXPECTED_SECRET" not in _exception_representations(result.exception)


def test_project_exception_chain_does_not_retain_confidential_context() -> None:
    sentinel = "PROJECT_EXCEPTION_CHAIN_CONFIDENTIAL_SENTINEL"
    with _pipeline(parser_error=RuntimeError(sentinel)):
        result = CliRunner().invoke(main, ["--output", "json", *_args()])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["errors"][0]["location"] == "project"
    assert sentinel not in result.stdout + result.stderr
    assert sentinel not in _exception_representations(result.exception)


def test_command_does_not_catch_base_exception() -> None:
    with _pipeline(parser_error=KeyboardInterrupt("BASE_SECRET_MUST_NOT_APPEAR")):
        result = CliRunner().invoke(main, _args())
    # Click itself converts KeyboardInterrupt to its ordinary abort exit, but
    # the callback's fixed E509 conversion must not claim the exception.
    assert result.exit_code == 1
    assert "Strimzi export failed safely" not in result.stdout + result.stderr


@pytest.mark.parametrize("kind", ["directory", "symlink", "fifo"])
def test_atomic_writer_rejects_nonregular_destinations(tmp_path: Path, kind: str) -> None:
    target = tmp_path / "topics.yaml"
    if kind == "directory":
        target.mkdir()
    elif kind == "symlink":
        original = tmp_path / "original"
        original.write_bytes(b"ORIGINAL")
        target.symlink_to(original)
    else:
        os.mkfifo(target)
    with pytest.raises(export_command._StrimziCommandError) as raised:
        export_command._atomic_write(target, b"NEW")
    assert raised.value.location == "output_file"
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_writer_replaces_regular_file_via_private_same_dir_stage(
    tmp_path: Path,
) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    observed: dict[str, object] = {}
    real_replace = os.replace

    def inspect_replace(source: os.PathLike[str], destination: os.PathLike[str]) -> None:
        stage = Path(source)
        observed["parent"] = stage.parent
        observed["mode"] = stat.S_IMODE(stage.stat().st_mode)
        observed["bytes"] = stage.read_bytes()
        real_replace(source, destination)

    with patch.object(export_command.os, "replace", side_effect=inspect_replace):
        export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"NEW"
    assert observed == {"parent": tmp_path, "mode": 0o600, "bytes": b"NEW"}
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_writer_rejects_destination_swap_and_preserves_current_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    real_state = export_command._destination_state
    calls = 0

    def swap_before_second_check(path: Path) -> tuple[int, int, int] | None:
        nonlocal calls
        calls += 1
        if calls == 2:
            path.unlink()
            path.write_bytes(b"SWAPPED")
        return real_state(path)

    with (
        patch.object(export_command, "_destination_state", side_effect=swap_before_second_check),
        pytest.raises(export_command._StrimziCommandError),
    ):
        export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"SWAPPED"
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_replace_never_follows_a_post_sample_destination_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "topics.yaml"
    victim = tmp_path / "victim"
    target.write_bytes(b"ORIGINAL")
    victim.write_bytes(b"VICTIM")
    real_state = export_command._destination_state
    calls = 0

    def swap_after_second_sample(path: Path) -> tuple[int, int, int] | None:
        nonlocal calls
        calls += 1
        sampled = real_state(path)
        if calls == 2:
            path.unlink()
            path.symlink_to(victim)
        return sampled

    with patch.object(
        export_command,
        "_destination_state",
        side_effect=swap_after_second_sample,
    ):
        export_command._atomic_write(target, b"NEW")
    assert not target.is_symlink()
    assert target.read_bytes() == b"NEW"
    assert victim.read_bytes() == b"VICTIM"
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_writer_rejects_a_swapped_staging_path(tmp_path: Path) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    with (
        patch.object(
            export_command,
            "_staging_state",
            return_value=(999, 999, stat.S_IFREG),
        ),
        pytest.raises(export_command._StrimziCommandError),
    ):
        export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"ORIGINAL"
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize(
    "failure", ["write", "flush", "fsync", "close", "close_after", "replace"]
)
def test_atomic_writer_cleans_stage_and_preserves_existing_on_every_exception(
    tmp_path: Path, failure: str
) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    real_fdopen = os.fdopen
    opened_descriptors: list[int] = []

    class FailingStream:
        def __init__(self, wrapped: object) -> None:
            self.wrapped = wrapped

        def write(self, content: bytes) -> int:
            if failure == "write":
                raise OSError("WRITE_SECRET")
            return self.wrapped.write(content)  # type: ignore[no-any-return,union-attr]

        def flush(self) -> None:
            if failure == "flush":
                raise OSError("FLUSH_SECRET")
            self.wrapped.flush()  # type: ignore[union-attr]

        def fileno(self) -> int:
            return self.wrapped.fileno()  # type: ignore[no-any-return,union-attr]

        def close(self) -> None:
            if failure == "close":
                raise OSError("CLOSE_SECRET")
            self.wrapped.close()  # type: ignore[union-attr]
            if failure == "close_after":
                raise OSError("CLOSE_AFTER_SECRET")

    def fdopen(descriptor: int, mode: str, *, closefd: bool) -> FailingStream:
        opened_descriptors.append(descriptor)
        assert closefd is False
        return FailingStream(real_fdopen(descriptor, mode, closefd=closefd))

    contexts = [patch.object(export_command.os, "fdopen", side_effect=fdopen)]
    if failure == "fsync":
        contexts.append(patch.object(export_command.os, "fsync", side_effect=OSError("FSYNC_SECRET")))
    if failure == "replace":
        contexts.append(
            patch.object(export_command.os, "replace", side_effect=OSError("REPLACE_SECRET"))
        )
    with ExitStack() as stack:
        for context in contexts:
            stack.enter_context(context)
        with pytest.raises(OSError):
            export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"ORIGINAL"
    assert not list(tmp_path.glob("*.tmp"))
    assert len(opened_descriptors) == 1
    with pytest.raises(OSError):
        os.fstat(opened_descriptors[0])


def test_atomic_writer_cleans_descriptor_when_fdopen_fails(tmp_path: Path) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    opened: list[int] = []

    def fail_fdopen(descriptor: int, _mode: str, *, closefd: bool) -> object:
        assert closefd is False
        opened.append(descriptor)
        raise OSError("FDOPEN_SECRET")

    with (
        patch.object(export_command.os, "fdopen", side_effect=fail_fdopen),
        pytest.raises(OSError),
    ):
        export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"ORIGINAL"
    assert not list(tmp_path.glob("*.tmp"))
    assert len(opened) == 1
    with pytest.raises(OSError):
        os.fstat(opened[0])


def test_atomic_writer_cleanup_covers_base_exception(tmp_path: Path) -> None:
    target = tmp_path / "topics.yaml"
    target.write_bytes(b"ORIGINAL")
    with (
        patch.object(export_command.os, "fsync", side_effect=KeyboardInterrupt("BASE_SECRET")),
        pytest.raises(KeyboardInterrupt),
    ):
        export_command._atomic_write(target, b"NEW")
    assert target.read_bytes() == b"ORIGINAL"
    assert not list(tmp_path.glob("*.tmp"))
