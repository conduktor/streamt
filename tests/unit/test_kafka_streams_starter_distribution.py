"""Fresh wheel/sdist starter resources and installed offline init behavior."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

from streamt.templates import create_kafka_streams_starter, kafka_streams_starter_resources

PREFIX = "streamt/templates/kafka_streams/"
RESOURCES = {
    "stream_project.yml": "stream_project.yml.j2",
    "README.md": "README.md.j2",
    "sample_events.jsonl": "sample_events.jsonl",
}


def test_installed_kafka_streams_starter_resources_and_offline_journey(tmp_path):
    configured = os.environ.get("STREAMT_TEST_DISTRIBUTIONS_DIR")
    if configured:
        distribution_dir = Path(configured)
    else:
        if importlib.util.find_spec("build") is None:
            pytest.skip("package/release jobs supply fresh built distributions")
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
            cwd=Path(__file__).parents[2],
            check=True,
            capture_output=True,
            timeout=180,
        )
    wheels = list(distribution_dir.glob("streamt-*.whl"))
    sdists = list(distribution_dir.glob("streamt-*.tar.gz"))
    assert len(wheels) == 1
    assert len(sdists) == 1
    resources = kafka_streams_starter_resources()
    with zipfile.ZipFile(wheels[0]) as archive:
        actual = {
            name.removeprefix(PREFIX): archive.read(name)
            for name in archive.namelist()
            if name.startswith(PREFIX) and not name.endswith("/")
        }
        assert actual == {RESOURCES[name]: payload.encode() for name, payload in resources.items()}
    with tarfile.open(sdists[0]) as archive:
        members = {member.name.partition("/")[2]: member for member in archive.getmembers()}
        for destination, source in RESOURCES.items():
            resource = archive.extractfile(members[f"src/{PREFIX}{source}"])
            assert resource is not None
            with resource:
                assert resource.read() == resources[destination].encode()
    expected_directory = tmp_path / "expected"
    create_kafka_streams_starter(
        expected_directory,
        name="starter-distribution",
        runner_image="sha256:" + "a" * 64,
        kafka="localhost:9092",
        kafka_internal="broker:19092",
        docker_network="local-streaming",
        initial_offset="earliest",
        dry_run=False,
        force=False,
    )
    code = """
import hashlib, json, socket, subprocess, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from click.testing import CliRunner
from streamt.cli import main
import confluent_kafka, confluent_kafka.admin
def forbidden(*args, **kwargs):
    raise RuntimeError('offline_starter_provider_access')
socket.getaddrinfo = socket.create_connection = forbidden
socket.socket.connect = socket.socket.connect_ex = forbidden
subprocess.run = subprocess.Popen = forbidden
confluent_kafka.Consumer = confluent_kafka.Producer = forbidden
confluent_kafka.admin.AdminClient = forbidden
directory = Path(sys.argv[2])
commands = [
    ['init', '--project-name', 'starter-distribution', '--executor', 'kafka_streams',
     '--runner-image', 'sha256:' + 'a' * 64, '--kafka', 'localhost:9092',
     '--kafka-internal', 'broker:19092', '--docker-network', 'local-streaming',
     '--initial-offset', 'earliest'],
    ['validate', '--strict'], ['lineage'], ['compile', '--dry-run'], ['plan', '--offline'],
]
results = []
for command in commands:
    result = CliRunner().invoke(main, ['-o', 'json', *command, '-p', str(directory)])
    if result.exit_code != 0:
        raise RuntimeError(result.output)
    results.append(json.loads(result.stdout))
hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
          for name in ['stream_project.yml', 'README.md', 'sample_events.jsonl']}
print(json.dumps({'hashes': hashes, 'results': results}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            code,
            str(wheels[0].resolve()),
            str(tmp_path / "installed"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    assert result.stderr == ""
    data = json.loads(result.stdout)
    assert data["hashes"] == {
        name: hashlib.sha256((expected_directory / name).read_bytes()).hexdigest()
        for name in RESOURCES
    }
    assert [item["command"] for item in data["results"]] == [
        "init",
        "validate",
        "lineage",
        "compile",
        "plan",
    ]
    assert data["results"][0]["data"]["support"] == "create_noop_only"
    assert data["results"][1]["warnings"] == []
    assert data["results"][-1]["data"]["creates"] == 3
    assert not (tmp_path / "installed" / "target").exists()
