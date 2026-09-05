"""Run the maintained image on an owned broker/network; remove only owned fixture resources."""

from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LABEL = "io.streamt.acceptance.owner"


def run(
    args: list[str], *, timeout: int = 60, capture: bool = True, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed commands and generated resource names, no shell
        args,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=capture,
        timeout=timeout,
        env=env,
    )


def cleanup(token: str, evidence: Path) -> None:
    selector = f"label={LABEL}={token}"
    containers = run(["docker", "ps", "-aq", "--filter", selector]).stdout.splitlines()
    mounted_volumes: list[str] = []
    for container in containers:
        info = json.loads(run(["docker", "inspect", container]).stdout)[0]
        if info["Config"]["Labels"].get(LABEL) != token:
            raise RuntimeError("Refusing cleanup: container ownership changed")
        mounted_volumes.extend(
            mount["Name"] for mount in info["Mounts"] if mount["Type"] == "volume"
        )
        name = info["Name"].lstrip("/")
        try:
            logs = run(["docker", "logs", container])
            (evidence / f"{name}.log").write_text(logs.stdout + logs.stderr)
        finally:
            run(["docker", "rm", "-f", "-v", container])
    volumes = run(["docker", "volume", "ls", "-q", "--filter", selector]).stdout.splitlines()
    for volume in volumes:
        info = json.loads(run(["docker", "volume", "inspect", volume]).stdout)[0]
        if info["Labels"].get(LABEL) != token:
            raise RuntimeError("Refusing cleanup: volume ownership changed")
        run(["docker", "volume", "rm", volume])
    remaining = set(run(["docker", "volume", "ls", "--format", "{{.Name}}"]).stdout.splitlines())
    if remaining.intersection(mounted_volumes + volumes):
        raise RuntimeError("Owned fixture volume remains after cleanup")
    networks = run(["docker", "network", "ls", "-q", "--filter", selector]).stdout.splitlines()
    for network in networks:
        info = json.loads(run(["docker", "network", "inspect", network]).stdout)[0]
        if info["Labels"].get(LABEL) != token:
            raise RuntimeError("Refusing cleanup: network ownership changed")
        run(["docker", "network", "rm", network])
    (evidence / "cleanup.json").write_text(
        json.dumps(
            {
                "owner_token": token,
                "removed_containers": containers,
                "recorded_mount_volume_names_verified_absent": mounted_volumes,
                "removed_labelled_volumes": volumes,
                "removed_networks": networks,
            },
            indent=2,
        )
        + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image", help="Use an existing immutable local sha256 image ID; otherwise build"
    )
    args = parser.parse_args()
    if args.image and not re.fullmatch(r"sha256:[a-f0-9]{64}", args.image):
        parser.error("--image requires a local immutable sha256 image ID")
    token = uuid.uuid4().hex[:12]
    prefix = f"streamt-runtime-accept-{token}"
    evidence = ROOT / "target" / "acceptance" / token
    evidence.mkdir(parents=True, mode=0o700)
    run(["mvn", "-q", "package"], timeout=300, capture=False)
    if args.image:
        image = args.image
    else:
        run(
            ["docker", "build", "--iidfile", str(evidence / "image.id"), "."],
            timeout=600,
            capture=False,
        )
        image = (evidence / "image.id").read_text().strip()
    network, broker = prefix + "-network", prefix + "-broker"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    bootstrap = f"127.0.0.1:{port}"
    lock = json.loads((ROOT / "images.lock.json").read_text())
    broker_image = lock["acceptance_broker"]
    environment = {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://{broker}:9092,EXTERNAL://{bootstrap}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
        "KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m",
    }
    try:
        run(["docker", "network", "create", "--label", f"{LABEL}={token}", network])
        command = [
            "docker",
            "create",
            "--name",
            broker,
            "--network",
            network,
            "--label",
            f"{LABEL}={token}",
            "-p",
            f"127.0.0.1:{port}:19092",
        ]
        for key, value in environment.items():
            command.extend(["-e", f"{key}={value}"])
        run([*command, broker_image], timeout=120)
        run(["docker", "start", broker])
        version = run(
            ["docker", "exec", broker, "/opt/kafka/bin/kafka-topics.sh", "--version"]
        ).stdout.strip()
        if version != "4.3.1":
            raise RuntimeError("Broker version mismatch before topic creation")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            logs = run(["docker", "logs", broker])
            if "Kafka Server started" in logs.stdout + logs.stderr:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("Owned broker startup timed out")
        (evidence / "fixture.json").write_text(
            json.dumps(
                {
                    "image_id": image,
                    "broker_image": broker_image,
                    "broker_version": version,
                    "broker": broker,
                    "network": network,
                    "bootstrap": bootstrap,
                },
                indent=2,
            )
            + "\n"
        )
        test_env = dict(
            os.environ,
            STREAMT_RUNTIME_ACCEPTANCE_IMAGE=image,
            STREAMT_RUNTIME_ACCEPTANCE_TOKEN=token,
            STREAMT_RUNTIME_ACCEPTANCE_NETWORK=network,
            STREAMT_RUNTIME_ACCEPTANCE_BROKER=broker,
            STREAMT_RUNTIME_ACCEPTANCE_BOOTSTRAP=bootstrap,
            STREAMT_RUNTIME_ACCEPTANCE_EVIDENCE=str(evidence),
        )
        run(
            ["mvn", "-q", "-Dtest=DockerAcceptanceTest", "test"],
            timeout=280,
            capture=False,
            env=test_env,
        )
        sys.stdout.write((evidence / "result.json").read_text() + "\n")
    finally:
        cleanup(token, evidence)
        sys.stdout.write(f"Owned Docker fixture removed; evidence: {evidence}\n")


if __name__ == "__main__":
    main()
