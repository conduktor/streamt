"""Installed runner build is explicit, local, pinned, and secret-neutral."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from streamt import runtime_assets
from streamt.cli import main
from streamt.cli.commands.runtime import _RuntimeImageBuilder
from streamt.deployer import kafka_streams_docker

IMAGE = "sha256:" + "a" * 64


def _success_docker(monkeypatch, *, fail=None):
    calls = []
    staged = []

    def run(args, **kwargs):
        calls.append((args, kwargs))
        assert kwargs["capture_output"] is True
        assert kwargs["check"] is False
        assert "shell" not in kwargs
        command = args[1:]
        if command[0] == "context":
            stdout = b'"unix:///test/local.sock"'
        elif command[0] == "info":
            stdout = b'"local-daemon-id"'
        elif command[0] == "build":
            if fail:
                raise fail
            assert command[1:3] == ["--builder", "default"]
            assert "--tag" not in command
            assert "-t" not in command
            context = Path(command[-1])
            staged.append(context)
            assert context.is_dir()
            assert (context.parent.stat().st_mode & 0o777) == 0o700
            assert (
                context / "Dockerfile"
            ).read_bytes() == runtime_assets.load_runtime_build_inputs()["Dockerfile"]
            assert sorted(
                path.relative_to(context).as_posix()
                for path in context.rglob("*")
                if path.is_file()
            ) == sorted(runtime_assets.load_runtime_build_inputs())
            image_file = Path(command[command.index("--iidfile") + 1])
            assert image_file.parent == context.parent
            image_file.write_text(IMAGE)
            stdout = b"sensitive-build-output-must-not-be-printed"
        elif command[:2] == ["image", "inspect"]:
            assert command[-1] == IMAGE
            stdout = json.dumps(
                {
                    "Id": IMAGE,
                    "Config": {
                        "Labels": {
                            "io.streamt.runner.version": "0.1.1",
                            "io.streamt.plan.version": "1",
                        }
                    },
                }
            ).encode()
        else:
            pytest.fail(f"Unexpected Docker command: {command}")
        return subprocess.CompletedProcess(args, 0, stdout, b"secret-provider-stderr")

    monkeypatch.delenv("DOCKER_HOST", raising=False)
    monkeypatch.setattr(kafka_streams_docker.shutil, "which", lambda _: "/test/docker")
    monkeypatch.setattr(kafka_streams_docker.subprocess, "run", run)
    return calls, staged


def test_runtime_build_dry_run_needs_no_project_docker_network_or_writes(monkeypatch, tmp_path):
    def forbidden(*args, **kwargs):
        pytest.fail("Dry run attempted a side effect")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(_RuntimeImageBuilder, "__init__", forbidden)
    monkeypatch.setattr(runtime_assets, "TemporaryDirectory", forbidden)
    monkeypatch.setattr(Path, "write_bytes", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    result = CliRunner().invoke(main, ["-o", "json", "runtime", "build", "--dry-run"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["command"] == "runtime build"
    assert data["status"] == "ok"
    assert data["data"] == {
        **runtime_assets.runtime_build_contract(runtime_assets.load_runtime_build_inputs()),
        "dry_run": True,
        "timeout_seconds": 600,
        "image": None,
    }
    assert not list(tmp_path.iterdir())


def test_runtime_build_freezes_local_builder_and_reports_verified_immutable_image(monkeypatch):
    calls, staged = _success_docker(monkeypatch)
    monkeypatch.setenv("DOCKER_CONTEXT", "selected-context")
    monkeypatch.setenv("BUILDX_BUILDER", "remote-builder")
    monkeypatch.setenv("BUILDKIT_HOST", "tcp://remote-builder:1234")
    monkeypatch.setenv("EXPERIMENTAL_BUILDKIT_SOURCE_POLICY", "/sensitive/rewrite-pins.json")
    monkeypatch.setenv("DOCKER_DEFAULT_PLATFORM", "linux/surprise")
    result = CliRunner().invoke(main, ["-o", "json", "runtime", "build", "--timeout", "120"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["image"] == IMAGE
    assert data["runner_version"] == "0.1.1"
    assert data["plan_version"] == 1
    assert data["configuration"]["runtime"]["kafka_streams"]["image"] == IMAGE
    assert data["publishes_image"] is False
    assert data["tags_image"] is False
    assert "sensitive-build" not in result.output
    assert "secret-provider" not in result.output
    build_calls = [(args, kwargs) for args, kwargs in calls if args[1] == "build"]
    assert len(build_calls) == 1
    env = build_calls[0][1]["env"]
    assert env["DOCKER_HOST"] == "unix:///test/local.sock"
    assert env["DOCKER_BUILDKIT"] == "1"
    assert not any(key.startswith(("BUILDX_", "BUILDKIT_")) for key in env)
    assert "DOCKER_CONTEXT" not in env
    assert "EXPERIMENTAL_BUILDKIT_SOURCE_POLICY" not in env
    assert "DOCKER_DEFAULT_PLATFORM" not in env
    assert build_calls[0][1]["timeout"] == 120
    assert len(staged) == 1
    assert not staged[0].parent.exists()
    assert [args[1] for args, _ in calls] == ["context", "info", "info", "build", "info", "image"]


@pytest.mark.parametrize("value", ["0", "29", "1801", "999999", "not-a-number"])
def test_runtime_build_rejects_unbounded_timeout_before_docker(monkeypatch, value):
    monkeypatch.setattr(_RuntimeImageBuilder, "__init__", lambda _: pytest.fail("Docker accessed"))
    result = CliRunner().invoke(main, ["runtime", "build", "--timeout", value])
    assert result.exit_code == 2


@pytest.mark.parametrize(
    "failure",
    [
        OSError("sensitive-host-path"),
        subprocess.TimeoutExpired("secret-command", 5, b"secret-output"),
        subprocess.CalledProcessError(1, "secret-command", b"secret-output"),
    ],
)
def test_runtime_build_failures_are_neutral_and_cleanup_staging(monkeypatch, failure):
    calls, _ = _success_docker(monkeypatch, fail=failure)
    result = CliRunner().invoke(main, ["-o", "json", "runtime", "build"])
    assert result.exit_code == 1
    data = json.loads(result.stdout)
    assert data["status"] == "error"
    assert data["errors"][0]["code"] == "RUNTIME_BUILD_FAILED"
    assert "secret" not in result.output
    assert "sensitive" not in result.output
    contexts = [Path(args[-1]) for args, _ in calls if args[1] == "build"]
    assert len(contexts) == 1
    assert not contexts[0].parent.exists()


def test_runtime_build_remote_daemon_rejected_before_staging(monkeypatch):
    _success_docker(monkeypatch)
    monkeypatch.setenv("DOCKER_HOST", "tcp://secret-daemon:2376")
    monkeypatch.setattr(
        runtime_assets, "TemporaryDirectory", lambda **_: pytest.fail("Staging created")
    )
    result = CliRunner().invoke(main, ["-o", "json", "runtime", "build"])
    assert result.exit_code == 1
    assert "secret-daemon" not in result.output


def test_runtime_assets_ignore_current_working_directory(monkeypatch, tmp_path):
    expected = runtime_assets.load_runtime_build_inputs()
    (tmp_path / "Dockerfile").write_text("FROM unrelated")
    monkeypatch.chdir(tmp_path)
    assert runtime_assets.load_runtime_build_inputs() == expected


def test_runtime_assets_installed_path_precedes_editable_fallback(monkeypatch, tmp_path):
    packaged = tmp_path / "_runtime_assets" / "kafka_streams"
    packaged.mkdir(parents=True)
    monkeypatch.setattr(runtime_assets, "files", lambda _: tmp_path)
    assert runtime_assets.runtime_build_assets() == packaged


def test_runtime_assets_missing_install_does_not_search_current_directory(monkeypatch, tmp_path):
    monkeypatch.setattr(runtime_assets, "files", lambda _: tmp_path)
    monkeypatch.setattr(
        runtime_assets,
        "__file__",
        str(tmp_path / "site-packages" / "streamt" / "runtime_assets.py"),
    )
    with pytest.raises(runtime_assets.RuntimeAssetsError, match="missing"):
        runtime_assets.runtime_build_assets()


@pytest.mark.parametrize("filename", ["Dockerfile", "images.lock.json"])
def test_runtime_assets_reject_missing_or_changed_base_pins(filename):
    inputs = runtime_assets.load_runtime_build_inputs()
    inputs[filename] = b"{}" if filename.endswith("json") else b"FROM maven:latest\n"
    with pytest.raises(runtime_assets.RuntimeAssetsError, match="pins"):
        runtime_assets.runtime_build_contract(inputs)


def test_runtime_assets_context_hash_covers_all_bytes_and_names():
    inputs = runtime_assets.load_runtime_build_inputs()
    original = runtime_assets.runtime_build_contract(inputs)
    inputs["LICENSE"] += b"\n"
    assert (
        runtime_assets.runtime_build_contract(inputs)["build_context_sha256"]
        != original["build_context_sha256"]
    )
    assert runtime_assets.runtime_build_contract(
        dict(reversed(list(inputs.items())))
    ) == runtime_assets.runtime_build_contract(inputs)


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "/tmp/escape",
        "src/main/../../escape",
        "src\\main\\escape",
        "target/evidence.log",
    ],
)
def test_runtime_assets_materialization_rejects_untrusted_paths(path):
    with (
        pytest.raises(runtime_assets.RuntimeAssetsError, match="path"),
        runtime_assets.materialize_runtime_build_context({path: b"invalid"}),
    ):
        pytest.fail("Untrusted path materialized")


def test_runtime_assets_reject_symlinked_sources(monkeypatch, tmp_path):
    inputs = runtime_assets.load_runtime_build_inputs()
    with runtime_assets.materialize_runtime_build_context(inputs) as root:
        source = root / "src" / "main" / "outside.java"
        source.symlink_to(tmp_path / "outside.java")
        monkeypatch.setattr(runtime_assets, "runtime_build_assets", lambda: root)
        with pytest.raises(runtime_assets.RuntimeAssetsError, match="Symlinks"):
            runtime_assets.load_runtime_build_inputs()
