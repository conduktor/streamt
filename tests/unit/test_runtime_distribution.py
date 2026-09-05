"""The installed wheel contains exactly the maintained runner build inputs."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path, PurePosixPath

import pytest

from streamt.runtime_assets import load_runtime_build_inputs, runtime_build_contract

REPOSITORY = Path(__file__).parents[2]
PREFIX = "streamt/_runtime_assets/kafka_streams/"


def test_editable_assets_are_the_exact_maintained_build_inputs():
    inputs = load_runtime_build_inputs()
    root = REPOSITORY / "runtimes" / "kafka-streams"
    expected = {
        name: (root / name).read_bytes()
        for name in (
            "Dockerfile",
            "pom.xml",
            "LICENSE",
            "images.lock.json",
        )
    }
    for tree in ("src/main", "src/test"):
        expected.update(
            {
                path.relative_to(root).as_posix(): path.read_bytes()
                for path in (root / tree).rglob("*")
                if path.is_file()
            }
        )
    assert inputs == expected
    assert len(inputs) >= 12
    assert all("target" not in PurePosixPath(name).parts for name in inputs)
    assert not any(name.endswith((".jar", ".log", ".py")) for name in inputs)


def test_wheel_and_sdist_runtime_assets_have_byte_parity_and_no_build_outputs(tmp_path):
    configured = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if configured:
        distribution_dir = Path(configured)
    else:
        if importlib.util.find_spec("build") is None:
            pytest.skip("package/release jobs supply built distributions")
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
            cwd=REPOSITORY,
            check=True,
            capture_output=True,
            timeout=180,
        )
    wheels = list(distribution_dir.glob("streamt-*.whl"))
    sdists = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1
    expected = load_runtime_build_inputs()
    with zipfile.ZipFile(wheels[0]) as archive:
        actual = {
            name.removeprefix(PREFIX): archive.read(name)
            for name in archive.namelist()
            if name.startswith(PREFIX) and not name.endswith("/")
        }
        assert actual == expected
        assert not any("experiments/kafka-streams" in name for name in archive.namelist())
    with tarfile.open(sdists[0]) as archive:
        members = {member.name.partition("/")[2]: member for member in archive.getmembers()}
        assert not any(name.startswith("experiments/kafka-streams/") for name in members)
        assert not any(name.startswith("runtimes/kafka-streams/target/") for name in members)
        for name, payload in expected.items():
            member = members[f"runtimes/kafka-streams/{name}"]
            assert member.isfile()
            stream = archive.extractfile(member)
            assert stream is not None
            with stream:
                assert stream.read() == payload
    # Load the wheel as an isolated zip resource, away from the checkout. No
    # editable source fallback exists inside the wheel's synthetic module path.
    code = """
import json, sys
sys.path.insert(0, sys.argv[1])
from streamt.runtime_assets import load_runtime_build_inputs, runtime_build_contract
print(json.dumps(runtime_build_contract(load_runtime_build_inputs()), sort_keys=True))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            code,
            str(wheels[0].resolve()),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert result.stderr == ""
    assert json.loads(result.stdout) == runtime_build_contract(expected)
    dry_run_code = """
import os, sys
sys.path.insert(0, sys.argv[1])
from streamt.cli import main
def reject_side_effects(event, args):
    if event.startswith(('socket.', 'subprocess.', 'os.exec', 'os.spawn')) or event in {
        'os.mkdir', 'os.remove', 'os.rmdir', 'os.rename', 'os.fork', 'os.system'
    }:
        raise RuntimeError('dry_run_side_effect')
    if event == 'open':
        mode = args[1] or ''
        flags = args[2] or 0
        if any(character in mode for character in 'wax+') or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            raise RuntimeError('dry_run_write')
sys.addaudithook(reject_side_effects)
main(args=['-o', 'json', 'runtime', 'build', '--dry-run'])
"""
    completed = subprocess.run(
        [sys.executable, "-I", "-B", "-c", dry_run_code, str(wheels[0].resolve())],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert completed.stderr == ""
    envelope = json.loads(completed.stdout)
    assert envelope["status"] == "ok"
    assert envelope["data"]["dry_run"] is True
    assert (
        envelope["data"]["build_context_sha256"]
        == runtime_build_contract(expected)["build_context_sha256"]
    )
